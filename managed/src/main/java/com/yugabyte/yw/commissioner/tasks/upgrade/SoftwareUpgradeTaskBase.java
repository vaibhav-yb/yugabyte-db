// Copyright (c) YugaByte, Inc.

package com.yugabyte.yw.commissioner.tasks.upgrade;

import com.yugabyte.yw.commissioner.BaseTaskDependencies;
import com.yugabyte.yw.commissioner.TaskExecutor.SubTaskGroup;
import com.yugabyte.yw.commissioner.UpgradeTaskBase;
import com.yugabyte.yw.commissioner.UserTaskDetails.SubTaskGroupType;
import com.yugabyte.yw.commissioner.tasks.XClusterConfigTaskBase;
import com.yugabyte.yw.common.Util;
import com.yugabyte.yw.common.config.UniverseConfKeys;
import com.yugabyte.yw.common.gflags.GFlagsUtil;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams;
import com.yugabyte.yw.forms.UpgradeTaskParams.UpgradeTaskSubType;
import com.yugabyte.yw.models.Universe;
import com.yugabyte.yw.models.XClusterConfig;
import com.yugabyte.yw.models.helpers.NodeDetails;
import com.yugabyte.yw.models.helpers.NodeDetails.NodeState;
import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public abstract class SoftwareUpgradeTaskBase extends UpgradeTaskBase {

  @Inject
  protected SoftwareUpgradeTaskBase(BaseTaskDependencies baseTaskDependencies) {
    super(baseTaskDependencies);
  }

  @Override
  public SubTaskGroupType getTaskSubGroupType() {
    return SubTaskGroupType.UpgradingSoftware;
  }

  @Override
  public NodeState getNodeState() {
    return NodeState.UpgradeSoftware;
  }

  protected UpgradeContext getUpgradeContext(String targetSoftwareVersion) {
    return UpgradeContext.builder()
        .reconfigureMaster(false)
        .runBeforeStopping(false)
        .processInactiveMaster(true)
        .targetSoftwareVersion(targetSoftwareVersion)
        .build();
  }

  protected UpgradeContext getRollbackUpgradeContext(
      Set<NodeDetails> nodesToSkipMasterActions,
      Set<NodeDetails> nodesToSkipTServerActions,
      String targetSoftwareVersion) {
    return UpgradeContext.builder()
        .reconfigureMaster(false)
        .runBeforeStopping(false)
        .processInactiveMaster(true)
        .processTServersFirst(true)
        .targetSoftwareVersion(targetSoftwareVersion)
        .nodesToSkipMasterActions(nodesToSkipMasterActions)
        .nodesToSkipTServerActions(nodesToSkipTServerActions)
        .build();
  }

  protected void createDownloadTasks(Collection<NodeDetails> nodes, String softwareVersion) {
    String subGroupDescription =
        String.format(
            "AnsibleConfigureServers (%s) for: %s",
            SubTaskGroupType.DownloadingSoftware, taskParams().nodePrefix);

    SubTaskGroup downloadTaskGroup = createSubTaskGroup(subGroupDescription);
    for (NodeDetails node : nodes) {
      downloadTaskGroup.addSubTask(
          getAnsibleConfigureServerTask(
              node, ServerType.TSERVER, UpgradeTaskSubType.Download, softwareVersion));
    }
    downloadTaskGroup.setSubTaskGroupType(SubTaskGroupType.DownloadingSoftware);
    getRunnableTask().addSubTaskGroup(downloadTaskGroup);
  }

  protected void createUpgradeTaskFlowTasks(
      Pair<List<NodeDetails>, List<NodeDetails>> nodes,
      String newVersion,
      UpgradeContext upgradeContext,
      boolean reProvision) {
    createUpgradeTaskFlow(
        (nodes1, processTypes) -> {
          // Re-provisioning the nodes if ybc needs to be installed and systemd is already
          // enabled
          // to register newly introduced ybc service if it is missing in case old universes.
          // We would skip ybc installation in case of manually provisioned systemd enabled
          // on-prem
          // universes as we may not have sudo permissions.
          if (reProvision) {
            createSetupServerTasks(nodes1, param -> param.isSystemdUpgrade = true);
          }
          createSoftwareInstallTasks(
              nodes1, getSingle(processTypes), newVersion, getTaskSubGroupType());
        },
        nodes,
        upgradeContext,
        false);
  }

  protected void createYbcInstallTask(
      Universe universe, List<NodeDetails> nodes, String newVersion) {
    createYbcSoftwareInstallTasks(nodes, newVersion, getTaskSubGroupType());
    // Start yb-controller process and wait for it to get responsive.
    createStartYbcProcessTasks(
        new HashSet<>(nodes),
        universe.getUniverseDetails().getPrimaryCluster().userIntent.useSystemd);
    createUpdateYbcTask(taskParams().getYbcSoftwareVersion())
        .setSubTaskGroupType(getTaskSubGroupType());
  }

  protected void createXClusterSourceRootCertDirPathGFlagTasks() {
    Universe targetUniverse = getUniverse();
    UniverseDefinitionTaskParams targetUniverseDetails = targetUniverse.getUniverseDetails();
    UniverseDefinitionTaskParams.Cluster targetPrimaryCluster =
        targetUniverseDetails.getPrimaryCluster();
    UniverseDefinitionTaskParams.UserIntent targetPrimaryUserIntent =
        targetPrimaryCluster.userIntent;
    List<XClusterConfig> xClusterConfigsAsTarget =
        XClusterConfig.getByTargetUniverseUUID(targetUniverse.getUniverseUUID());

    // If the gflag was set manually before, use its old value.
    File manualSourceRootCertDirPath = targetUniverseDetails.getSourceRootCertDirPath();
    if (manualSourceRootCertDirPath != null) {
      log.debug(
          "{} gflag has already been set manually",
          XClusterConfigTaskBase.SOURCE_ROOT_CERTS_DIR_GFLAG);
      targetUniverseDetails.xClusterInfo.sourceRootCertDirPath =
          manualSourceRootCertDirPath.toString();
      GFlagsUtil.removeGFlag(
          targetPrimaryUserIntent,
          XClusterConfigTaskBase.SOURCE_ROOT_CERTS_DIR_GFLAG,
          ServerType.TSERVER,
          ServerType.MASTER);
    } else {
      targetUniverseDetails.xClusterInfo.sourceRootCertDirPath =
          XClusterConfigTaskBase.getProducerCertsDir(targetPrimaryUserIntent.provider);
    }
    log.debug(
        "sourceRootCertDirPath={} will be used", targetUniverseDetails.getSourceRootCertDirPath());

    // Copy the source certs to the corresponding directory on the target universe.
    Map<UUID, List<XClusterConfig>> sourceUniverseUuidToXClusterConfigsMap =
        xClusterConfigsAsTarget.stream()
            .collect(
                Collectors.groupingBy(xClusterConfig -> xClusterConfig.getSourceUniverseUUID()));

    // Put all the universes in the locked list. The unlock operation is a no-op if the universe
    // does not get locked by this task.
    lockedXClusterUniversesUuidSet = sourceUniverseUuidToXClusterConfigsMap.keySet();

    sourceUniverseUuidToXClusterConfigsMap.forEach(
        (sourceUniverseUuid, xClusterConfigs) -> {
          try {
            // Lock the source universe.
            Universe sourceUniverse =
                lockUniverseIfExist(sourceUniverseUuid, -1 /* expectedUniverseVersion */);
            if (sourceUniverse == null) {
              log.info("Other universe is deleted; No further action is needed");
              return;
            }

            // Create the subtasks to transfer all source universe root certificates to the target
            // universe if required.
            String sourceCertificatePath = sourceUniverse.getCertificateNodetoNode();
            if (sourceCertificatePath != null) {
              File sourceCertificate = new File(sourceCertificatePath);
              if (!sourceCertificate.exists()) {
                throw new IllegalStateException(
                    String.format(
                        "sourceCertificate file \"%s\" for universe \"%s\" does not exist",
                        sourceCertificate, sourceUniverse.getUniverseUUID()));
              }
              xClusterConfigs.forEach(
                  xClusterConfig ->
                      createTransferXClusterCertsCopyTasks(
                          xClusterConfig,
                          targetUniverse.getNodes(),
                          xClusterConfig.getReplicationGroupName(),
                          sourceCertificate,
                          targetUniverseDetails.getSourceRootCertDirPath()));
            }
            log.debug(
                "Subtasks created to transfer all source universe root certificates to "
                    + "the target universe for these xCluster configs: {}",
                xClusterConfigs);
          } catch (Exception e) {
            log.error(
                "{} hit error while creating subtasks for transferring source universe TLS "
                    + "certificates : {}",
                getName(),
                e.getMessage());
            throw new RuntimeException(e);
          }
        });

    // If the gflags were manually set, persist the new target universe user intent without those
    // gflags. Otherwise, it means the gflags do not already exist in the conf files on the DB
    // nodes, and it should regenerate the conf files.
    if (manualSourceRootCertDirPath != null) {
      updateGFlagsPersistTasks(
              targetPrimaryCluster,
              targetPrimaryUserIntent.masterGFlags,
              targetPrimaryUserIntent.tserverGFlags,
              targetPrimaryUserIntent.specificGFlags)
          .setSubTaskGroupType(SubTaskGroupType.UpdatingGFlags);
    } else {
      createGFlagsOverrideTasks(targetUniverse.getMasters(), ServerType.MASTER);
      createGFlagsOverrideTasks(targetUniverse.getTServersInPrimaryCluster(), ServerType.TSERVER);
    }

    // Put the gflag into xCluster info of the universe and persist it.
    createXClusterInfoPersistTask();
  }

  protected void createPrecheckTasks(Universe universe, String newVersion) {
    Pair<List<NodeDetails>, List<NodeDetails>> nodes = fetchNodes(taskParams().upgradeOption);
    Set<NodeDetails> allNodes = toOrderedSet(nodes);

    // Preliminary checks for upgrades.
    createCheckUpgradeTask(newVersion).setSubTaskGroupType(SubTaskGroupType.PreflightChecks);

    // PreCheck for Available Memory on tserver nodes.
    long memAvailableLimit =
        confGetter.getConfForScope(universe, UniverseConfKeys.dbMemAvailableLimit);
    // No need to run the check if the minimum allowed is 0.
    if (memAvailableLimit > 0) {
      createAvailableMemoryCheck(allNodes, Util.AVAILABLE_MEMORY, memAvailableLimit)
          .setSubTaskGroupType(SubTaskGroupType.PreflightChecks);
    }
  }
}
