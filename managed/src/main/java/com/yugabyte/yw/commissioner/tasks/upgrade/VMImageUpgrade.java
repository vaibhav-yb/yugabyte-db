// Copyright (c) YugaByte, Inc.

package com.yugabyte.yw.commissioner.tasks.upgrade;

import com.yugabyte.yw.commissioner.BaseTaskDependencies;
import com.yugabyte.yw.commissioner.TaskExecutor.SubTaskGroup;
import com.yugabyte.yw.commissioner.UpgradeTaskBase;
import com.yugabyte.yw.commissioner.UserTaskDetails.SubTaskGroupType;
import com.yugabyte.yw.commissioner.tasks.UniverseTaskBase;
import com.yugabyte.yw.commissioner.tasks.subtasks.CreateRootVolumes;
import com.yugabyte.yw.commissioner.tasks.subtasks.ReplaceRootVolume;
import com.yugabyte.yw.common.config.RuntimeConfGetter;
import com.yugabyte.yw.common.config.UniverseConfKeys;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams.Cluster;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams.UserIntent;
import com.yugabyte.yw.forms.VMImageUpgradeParams;
import com.yugabyte.yw.forms.VMImageUpgradeParams.VmUpgradeTaskType;
import com.yugabyte.yw.models.helpers.CommonUtils;
import com.yugabyte.yw.models.helpers.NodeDetails;
import com.yugabyte.yw.models.helpers.NodeDetails.NodeState;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class VMImageUpgrade extends UpgradeTaskBase {

  private final Map<UUID, List<String>> replacementRootVolumes = new ConcurrentHashMap<>();

  private final RuntimeConfGetter confGetter;

  @Inject
  protected VMImageUpgrade(
      BaseTaskDependencies baseTaskDependencies, RuntimeConfGetter confGetter) {
    super(baseTaskDependencies);
    this.confGetter = confGetter;
  }

  @Override
  protected VMImageUpgradeParams taskParams() {
    return (VMImageUpgradeParams) taskParams;
  }

  @Override
  public SubTaskGroupType getTaskSubGroupType() {
    return SubTaskGroupType.Invalid;
  }

  @Override
  public NodeState getNodeState() {
    return null;
  }

  @Override
  public void run() {
    runUpgrade(
        () -> {
          Set<NodeDetails> nodeSet = fetchAllNodes(taskParams().upgradeOption);
          // Verify the request params and fail if invalid
          taskParams().verifyParams(getUniverse());

          String newVersion = taskParams().ybSoftwareVersion;
          if (taskParams().isSoftwareUpdateViaVm) {
            createCheckUpgradeTask(newVersion).setSubTaskGroupType(getTaskSubGroupType());
          }

          // Create task sequence for VM Image upgrade
          createVMImageUpgradeTasks(nodeSet);

          if (taskParams().isSoftwareUpdateViaVm) {
            // Promote Auto flags on compatible versions.
            if (confGetter.getConfForScope(getUniverse(), UniverseConfKeys.promoteAutoFlag)
                && CommonUtils.isAutoFlagSupported(newVersion)) {
              createCheckSoftwareVersionTask(nodeSet, newVersion)
                  .setSubTaskGroupType(getTaskSubGroupType());
              createPromoteAutoFlagTask().setSubTaskGroupType(getTaskSubGroupType());
            }

            // Update software version in the universe metadata.
            createUpdateSoftwareVersionTask(newVersion, true /*isSoftwareUpdateViaVm*/)
                .setSubTaskGroupType(getTaskSubGroupType());
          }

          createMarkUniverseForHealthScriptReUploadTask();
        });
  }

  private void createVMImageUpgradeTasks(Set<NodeDetails> nodes) {
    createRootVolumeCreationTasks(nodes).setSubTaskGroupType(getTaskSubGroupType());

    for (NodeDetails node : nodes) {
      UUID region = taskParams().nodeToRegion.get(node.nodeUuid);
      String machineImage = taskParams().machineImages.get(region);
      String sshUserOverride = taskParams().sshUserOverrideMap.get(region);
      if (!taskParams().forceVMImageUpgrade && machineImage.equals(node.machineImage)) {
        log.info(
            "Skipping node {} as it's already running on {} and force flag is not set",
            node.nodeName,
            machineImage);
        continue;
      }

      List<UniverseTaskBase.ServerType> processTypes = new ArrayList<>();
      if (node.isMaster) processTypes.add(ServerType.MASTER);
      if (node.isTserver) processTypes.add(ServerType.TSERVER);
      if (getUniverse().isYbcEnabled()) processTypes.add(ServerType.CONTROLLER);

      // The node is going to be stopped. Ignore error because of previous error due to
      // possibly detached root volume.
      processTypes.forEach(
          processType ->
              createServerControlTask(
                      node, processType, "stop", params -> params.isIgnoreError = true)
                  .setSubTaskGroupType(getTaskSubGroupType()));

      createRootVolumeReplacementTask(node).setSubTaskGroupType(getTaskSubGroupType());

      node.machineImage = machineImage;
      if (StringUtils.isNotBlank(sshUserOverride)) {
        node.sshUserOverride = sshUserOverride;
      }

      node.ybPrebuiltAmi =
          taskParams().vmUpgradeTaskType == VmUpgradeTaskType.VmUpgradeWithCustomImages;
      List<NodeDetails> nodeList = Collections.singletonList(node);
      createInstallNodeAgentTasks(nodeList).setSubTaskGroupType(SubTaskGroupType.Provisioning);
      createWaitForNodeAgentTasks(nodeList).setSubTaskGroupType(SubTaskGroupType.Provisioning);
      createSetupServerTasks(nodeList, p -> p.vmUpgradeTaskType = taskParams().vmUpgradeTaskType)
          .setSubTaskGroupType(SubTaskGroupType.InstallingSoftware);
      createConfigureServerTasks(
              nodeList, params -> params.vmUpgradeTaskType = taskParams().vmUpgradeTaskType)
          .setSubTaskGroupType(SubTaskGroupType.InstallingSoftware);

      // Copy the source root certificate to the node.
      createTransferXClusterCertsCopyTasks(
          Collections.singleton(node), getUniverse(), SubTaskGroupType.InstallingSoftware);

      processTypes.forEach(
          processType -> {
            if (processType.equals(ServerType.CONTROLLER)) {
              createStartYbcTasks(Arrays.asList(node)).setSubTaskGroupType(getTaskSubGroupType());

              // Wait for yb-controller to be responsive on each node.
              createWaitForYbcServerTask(new HashSet<>(Arrays.asList(node)))
                  .setSubTaskGroupType(SubTaskGroupType.ConfigureUniverse);
            } else {
              createGFlagsOverrideTasks(
                  nodeList,
                  processType,
                  false /*isMasterInShellMode*/,
                  taskParams().vmUpgradeTaskType,
                  false /*ignoreUseCustomImageConfig*/);
              createServerControlTask(node, processType, "start")
                  .setSubTaskGroupType(getTaskSubGroupType());
              createWaitForServersTasks(new HashSet<>(nodeList), processType);
              createWaitForServerReady(node, processType, getSleepTimeForProcess(processType))
                  .setSubTaskGroupType(getTaskSubGroupType());
            }
          });

      createWaitForKeyInMemoryTask(node);
      createNodeDetailsUpdateTask(node, !taskParams().isSoftwareUpdateViaVm)
          .setSubTaskGroupType(getTaskSubGroupType());
    }
    // Delete after all the disks are replaced.
    createDeleteRootVolumesTasks(getUniverse(), nodes, null /* volume Ids */)
        .setSubTaskGroupType(getTaskSubGroupType());
  }

  private SubTaskGroup createRootVolumeCreationTasks(Collection<NodeDetails> nodes) {
    Map<UUID, List<NodeDetails>> rootVolumesPerAZ =
        nodes.stream().collect(Collectors.groupingBy(n -> n.azUuid));
    SubTaskGroup subTaskGroup = createSubTaskGroup("CreateRootVolumes");

    rootVolumesPerAZ.forEach(
        (key, value) -> {
          NodeDetails node = value.get(0);
          UUID region = taskParams().nodeToRegion.get(node.nodeUuid);
          String machineImage = taskParams().machineImages.get(region);
          int numVolumes = value.size();

          if (!taskParams().forceVMImageUpgrade) {
            numVolumes =
                (int) value.stream().filter(n -> !machineImage.equals(n.machineImage)).count();
          }

          if (numVolumes == 0) {
            log.info("Nothing to upgrade in AZ {}", node.cloudInfo.az);
            return;
          }

          CreateRootVolumes.Params params = new CreateRootVolumes.Params();
          Cluster cluster = taskParams().getClusterByUuid(node.placementUuid);
          if (cluster == null) {
            throw new IllegalArgumentException(
                "No cluster available with UUID: " + node.placementUuid);
          }
          UserIntent userIntent = cluster.userIntent;
          fillCreateParamsForNode(params, userIntent, node);
          params.numVolumes = numVolumes;
          params.machineImage = machineImage;
          params.bootDisksPerZone = this.replacementRootVolumes;

          log.info(
              "Creating {} root volumes using {} in AZ {}",
              params.numVolumes,
              params.machineImage,
              node.cloudInfo.az);

          CreateRootVolumes task = createTask(CreateRootVolumes.class);
          task.initialize(params);
          subTaskGroup.addSubTask(task);
        });

    getRunnableTask().addSubTaskGroup(subTaskGroup);
    return subTaskGroup;
  }

  private SubTaskGroup createRootVolumeReplacementTask(NodeDetails node) {
    SubTaskGroup subTaskGroup = createSubTaskGroup("ReplaceRootVolume");
    ReplaceRootVolume.Params replaceParams = new ReplaceRootVolume.Params();
    replaceParams.nodeName = node.nodeName;
    replaceParams.azUuid = node.azUuid;
    replaceParams.universeUUID = taskParams().universeUUID;
    replaceParams.bootDisksPerZone = this.replacementRootVolumes;

    ReplaceRootVolume replaceDiskTask = createTask(ReplaceRootVolume.class);
    replaceDiskTask.initialize(replaceParams);
    subTaskGroup.addSubTask(replaceDiskTask);

    getRunnableTask().addSubTaskGroup(subTaskGroup);
    return subTaskGroup;
  }
}
