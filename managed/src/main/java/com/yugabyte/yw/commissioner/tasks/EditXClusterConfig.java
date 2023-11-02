// Copyright (c) YugaByte, Inc.
package com.yugabyte.yw.commissioner.tasks;

import com.yugabyte.yw.commissioner.BaseTaskDependencies;
import com.yugabyte.yw.commissioner.UserTaskDetails;
import com.yugabyte.yw.common.DrConfigStates.SourceUniverseState;
import com.yugabyte.yw.common.DrConfigStates.State;
import com.yugabyte.yw.common.DrConfigStates.TargetUniverseState;
import com.yugabyte.yw.common.XClusterUniverseService;
import com.yugabyte.yw.forms.XClusterConfigEditFormData;
import com.yugabyte.yw.models.Restore;
import com.yugabyte.yw.models.Universe;
import com.yugabyte.yw.models.XClusterConfig;
import com.yugabyte.yw.models.XClusterConfig.XClusterConfigStatusType;
import com.yugabyte.yw.models.XClusterTableConfig;
import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.yb.master.MasterDdlOuterClass;

@Slf4j
public class EditXClusterConfig extends CreateXClusterConfig {

  @Inject
  protected EditXClusterConfig(
      BaseTaskDependencies baseTaskDependencies, XClusterUniverseService xClusterUniverseService) {
    super(baseTaskDependencies, xClusterUniverseService);
  }

  @Override
  public void run() {
    log.info("Running {}", getName());

    XClusterConfig xClusterConfig = getXClusterConfigFromTaskParams();
    Universe sourceUniverse = Universe.getOrBadRequest(xClusterConfig.getSourceUniverseUUID());
    Universe targetUniverse = Universe.getOrBadRequest(xClusterConfig.getTargetUniverseUUID());
    XClusterConfigEditFormData editFormData = taskParams().getEditFormData();

    // Lock the source universe.
    lockUniverseForUpdate(sourceUniverse.getUniverseUUID(), sourceUniverse.getVersion());
    try {
      // Lock the target universe.
      lockUniverseForUpdate(targetUniverse.getUniverseUUID(), targetUniverse.getVersion());
      try {

        // Check Auto flags on source and target universes while resuming xCluster.
        if (editFormData.status != null && editFormData.status.equals("Running")) {
          createCheckXUniverseAutoFlag(sourceUniverse, targetUniverse)
              .setSubTaskGroupType(UserTaskDetails.SubTaskGroupType.PreflightChecks);
        }

        createXClusterConfigSetStatusTask(xClusterConfig, XClusterConfigStatusType.Updating)
            .setSubTaskGroupType(UserTaskDetails.SubTaskGroupType.ConfigureUniverse);

        if (editFormData.name != null) {
          String oldReplicationGroupName = xClusterConfig.getReplicationGroupName();
          // If TLS root certificates are different, create a directory containing the source
          // universe root certs with the new name.
          Optional<File> sourceCertificate =
              getSourceCertificateIfNecessary(sourceUniverse, targetUniverse);
          sourceCertificate.ifPresent(
              cert ->
                  createTransferXClusterCertsCopyTasks(
                      xClusterConfig,
                      targetUniverse.getNodes(),
                      xClusterConfig.getNewReplicationGroupName(
                          xClusterConfig.getSourceUniverseUUID(), editFormData.name),
                      cert,
                      targetUniverse.getUniverseDetails().getSourceRootCertDirPath()));

          createXClusterConfigRenameTask(xClusterConfig, editFormData.name)
              .setSubTaskGroupType(UserTaskDetails.SubTaskGroupType.ConfigureUniverse);

          // Delete the old directory if it created a new one. When the old directory is removed
          // because of renaming, the directory for transactional replication must not be deleted.
          sourceCertificate.ifPresent(
              cert ->
                  createTransferXClusterCertsRemoveTasks(
                      xClusterConfig,
                      oldReplicationGroupName,
                      targetUniverse.getUniverseDetails().getSourceRootCertDirPath(),
                      false /* ignoreErrors */));
        } else if (editFormData.status != null) {
          createSetReplicationPausedTask(xClusterConfig, editFormData.status)
              .setSubTaskGroupType(UserTaskDetails.SubTaskGroupType.ConfigureUniverse);
        } else if (editFormData.sourceRole != null || editFormData.targetRole != null) {
          createChangeXClusterRoleTask(
                  taskParams().getXClusterConfig(),
                  editFormData.sourceRole,
                  editFormData.targetRole)
              .setSubTaskGroupType(UserTaskDetails.SubTaskGroupType.ConfigureUniverse);
        } else if (editFormData.tables != null) {
          if (!CollectionUtils.isEmpty(taskParams().getTableInfoList())) {
            createXClusterConfigSetStatusForTablesTask(
                xClusterConfig,
                taskParams().getTableIdsToAdd(),
                XClusterTableConfig.Status.Updating);

            addSubtasksToAddTablesToXClusterConfig(
                xClusterConfig,
                taskParams().getTableInfoList(),
                taskParams().getMainTableIndexTablesMap(),
                Collections.emptySet() /* tableIdsScheduledForBeingRemoved */);
          }

          if (!CollectionUtils.isEmpty(taskParams().getTableIdsToRemove())) {
            createXClusterConfigSetStatusForTablesTask(
                xClusterConfig,
                taskParams().getTableIdsToRemove(),
                XClusterTableConfig.Status.Updating);

            createRemoveTableFromXClusterConfigSubtasks(
                xClusterConfig, taskParams().getTableIdsToRemove(), false /* keepEntry */);
          }
        } else {
          throw new RuntimeException("No edit operation was specified in editFormData");
        }

        createXClusterConfigSetStatusTask(xClusterConfig, XClusterConfigStatusType.Running)
            .setSubTaskGroupType(UserTaskDetails.SubTaskGroupType.ConfigureUniverse);

        createMarkUniverseUpdateSuccessTasks(targetUniverse.getUniverseUUID())
            .setSubTaskGroupType(UserTaskDetails.SubTaskGroupType.ConfigureUniverse);

        createMarkUniverseUpdateSuccessTasks(sourceUniverse.getUniverseUUID())
            .setSubTaskGroupType(UserTaskDetails.SubTaskGroupType.ConfigureUniverse);

        getRunnableTask().runSubTasks();
      } finally {
        // Unlock the target universe.
        unlockUniverseForUpdate(targetUniverse.getUniverseUUID());
      }
    } catch (Exception e) {
      log.error("{} hit error : {}", getName(), e.getMessage());
      xClusterConfig.updateStatus(XClusterConfigStatusType.Running);
      if (editFormData.tables != null) {
        // Set tables in updating status to failed.
        Set<String> tablesInPendingStatus =
            xClusterConfig.getTableIdsInStatus(
                Stream.concat(
                        getTableIds(taskParams().getTableInfoList()).stream(),
                        taskParams().getTableIdsToRemove().stream())
                    .collect(Collectors.toSet()),
                X_CLUSTER_TABLE_CONFIG_PENDING_STATUS_LIST);
        xClusterConfig.updateStatusForTables(
            tablesInPendingStatus, XClusterTableConfig.Status.Failed);
      }
      // Set backup and restore status to failed and alter load balanced.
      boolean isLoadBalancerAltered = false;
      for (Restore restore : restoreList) {
        isLoadBalancerAltered = isLoadBalancerAltered || restore.isAlterLoadBalancer();
      }
      handleFailedBackupAndRestore(
          backupList, restoreList, false /* isAbort */, isLoadBalancerAltered);
      throw new RuntimeException(e);
    } finally {
      // Unlock the source universe.
      unlockUniverseForUpdate(sourceUniverse.getUniverseUUID());
    }

    log.info("Completed {}", getName());
  }

  protected void addSubtasksToAddTablesToXClusterConfig(
      XClusterConfig xClusterConfig,
      List<MasterDdlOuterClass.ListTablesResponsePB.TableInfo> requestedTableInfoList,
      Map<String, List<String>> mainTableIndexTablesMap,
      Set<String> tableIdsScheduledForBeingRemoved) {
    Map<String, List<MasterDdlOuterClass.ListTablesResponsePB.TableInfo>>
        dbToTablesInfoMapNeedBootstrap =
            getDbToTablesInfoMapNeedBootstrap(
                xClusterConfig,
                taskParams().getTableIdsToAdd(),
                requestedTableInfoList,
                mainTableIndexTablesMap,
                taskParams().getSourceTableIdsWithNoTableOnTargetUniverse());

    // Add the subtasks to set up replication for tables that do not need bootstrapping.
    Set<String> tableIdsNotNeedBootstrap =
        getTableIdsNotNeedBootstrap(taskParams().getTableIdsToAdd());
    if (!tableIdsNotNeedBootstrap.isEmpty()) {
      addSubtasksForTablesNotNeedBootstrap(
          xClusterConfig,
          tableIdsNotNeedBootstrap,
          requestedTableInfoList,
          true /* isReplicationConfigCreated */,
          taskParams().getPitrParams());
    }

    // Add the subtasks to set up replication for tables that need bootstrapping if any.
    if (!dbToTablesInfoMapNeedBootstrap.isEmpty()) {
      // YSQL tables replication with bootstrapping can only be set up with DB granularity. The
      // following subtasks remove tables in replication, so the replication can be set up again
      // for all the tables in the DB including the new tables.
      Set<String> tableIdsDeleteReplication = new HashSet<>();
      dbToTablesInfoMapNeedBootstrap.forEach(
          (namespaceName, tablesInfo) -> {
            Set<String> tableIdsNeedBootstrap = getTableIds(tablesInfo);
            Set<String> tableIdsNeedBootstrapInReplication =
                xClusterConfig.getTableIdsWithReplicationSetup(
                    tableIdsNeedBootstrap, true /* done */);
            tableIdsDeleteReplication.addAll(
                tableIdsNeedBootstrapInReplication.stream()
                    .filter(tableId -> !tableIdsScheduledForBeingRemoved.contains(tableId))
                    .collect(Collectors.toSet()));
          });

      // A replication group with no tables in it cannot exist in YBDB. If all the tables must be
      // removed from the replication group, remove the replication group.
      boolean isRestartWholeConfig =
          tableIdsDeleteReplication.size() + tableIdsScheduledForBeingRemoved.size()
              >= xClusterConfig.getTableIdsWithReplicationSetup().size()
                  + tableIdsNotNeedBootstrap.size();
      log.info(
          "tableIdsDeleteReplication is {} and isRestartWholeConfig is {}",
          tableIdsDeleteReplication,
          isRestartWholeConfig);
      if (isRestartWholeConfig) {
        createXClusterConfigSetStatusForTablesTask(
            xClusterConfig,
            getTableIds(requestedTableInfoList),
            XClusterTableConfig.Status.Updating);

        // Delete the xCluster config.
        createDeleteXClusterConfigSubtasks(
            xClusterConfig, true /* keepEntry */, taskParams().isForced());

        if (xClusterConfig.isUsedForDr()) {
          createSetDrStatesTask(
                  xClusterConfig,
                  State.Initializing,
                  SourceUniverseState.Unconfigured,
                  TargetUniverseState.Unconfigured,
                  null /* keyspacePending */)
              .setSubTaskGroupType(UserTaskDetails.SubTaskGroupType.ConfigureUniverse);
        }

        createXClusterConfigSetStatusTask(
            xClusterConfig, XClusterConfig.XClusterConfigStatusType.Updating);

        createXClusterConfigSetStatusForTablesTask(
            xClusterConfig,
            getTableIds(requestedTableInfoList),
            XClusterTableConfig.Status.Updating);

        // Recreate the config including the new tables.
        addSubtasksToCreateXClusterConfig(
            xClusterConfig,
            requestedTableInfoList,
            mainTableIndexTablesMap,
            taskParams().getSourceTableIdsWithNoTableOnTargetUniverse(),
            taskParams().getPitrParams());
      } else {
        createRemoveTableFromXClusterConfigSubtasks(
            xClusterConfig, tableIdsDeleteReplication, true /* keepEntry */);

        if (xClusterConfig.isUsedForDr()) {
          createSetDrStatesTask(
                  xClusterConfig,
                  State.Initializing,
                  SourceUniverseState.Unconfigured,
                  TargetUniverseState.Unconfigured,
                  null /* keyspacePending */)
              .setSubTaskGroupType(UserTaskDetails.SubTaskGroupType.ConfigureUniverse);
        }

        createXClusterConfigSetStatusForTablesTask(
            xClusterConfig, tableIdsDeleteReplication, XClusterTableConfig.Status.Updating);

        // Add the subtasks to set up replication for tables that need bootstrapping.
        addSubtasksForTablesNeedBootstrap(
            xClusterConfig,
            taskParams().getBootstrapParams(),
            dbToTablesInfoMapNeedBootstrap,
            true /* isReplicationConfigCreated */,
            taskParams().getPitrParams());
      }
    }
  }
}
