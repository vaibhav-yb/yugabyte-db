/*
 * Copyright 2019 YugaByte, Inc. and Contributors
 *
 * Licensed under the Polyform Free Trial License 1.0.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 *     https://github.com/YugaByte/yugabyte-db/blob/master/licenses/POLYFORM-FREE-TRIAL-LICENSE-1.0.0.txt
 */

package com.yugabyte.yw.commissioner.tasks;

import com.yugabyte.yw.commissioner.BaseTaskDependencies;
import com.yugabyte.yw.commissioner.UserTaskDetails.SubTaskGroupType;
import com.yugabyte.yw.commissioner.tasks.subtasks.KubernetesCommandExecutor.CommandType;
import com.yugabyte.yw.common.PlacementInfoUtil;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams.UserIntent;
import com.yugabyte.yw.forms.UpgradeParams;
import com.yugabyte.yw.forms.UpgradeTaskParams;
import com.yugabyte.yw.models.Provider;
import com.yugabyte.yw.models.Universe;
import com.yugabyte.yw.models.helpers.PlacementInfo;
import java.util.UUID;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpgradeKubernetesUniverse extends KubernetesTaskBase {

  @Inject
  protected UpgradeKubernetesUniverse(BaseTaskDependencies baseTaskDependencies) {
    super(baseTaskDependencies);
  }

  public static class Params extends UpgradeParams {}

  @Override
  protected UpgradeParams taskParams() {
    return (UpgradeParams) taskParams;
  }

  @Override
  public void run() {
    try {
      checkUniverseVersion();

      // Update the universe DB with the update to be performed and set the 'updateInProgress' flag
      // to prevent other updates from happening.
      Universe universe = lockUniverseForUpdate(taskParams().expectedUniverseVersion);

      taskParams().rootCA = universe.getUniverseDetails().rootCA;

      // This value is used by subsequent calls to helper methods for
      // creating KubernetesCommandExecutor tasks. This value cannot
      // be changed once set during the Universe creation, so we don't
      // allow users to modify it later during edit, upgrade, etc.
      taskParams().useNewHelmNamingStyle = universe.getUniverseDetails().useNewHelmNamingStyle;

      UserIntent userIntent = universe.getUniverseDetails().getPrimaryCluster().userIntent;
      PlacementInfo pi = universe.getUniverseDetails().getPrimaryCluster().placementInfo;

      if (taskParams().taskType == UpgradeTaskParams.UpgradeTaskType.Software) {
        if (taskParams().ybSoftwareVersion == null || taskParams().ybSoftwareVersion.isEmpty()) {
          throw new IllegalArgumentException(
              "Invalid yugabyte software version: " + taskParams().ybSoftwareVersion);
        }
        if (taskParams().ybSoftwareVersion.equals(userIntent.ybSoftwareVersion)) {
          throw new IllegalArgumentException(
              "Cluster is already on yugabyte software version: " + taskParams().ybSoftwareVersion);
        }
      }

      preTaskActions();

      switch (taskParams().taskType) {
        case Software:
          log.info(
              "Upgrading software version to {} in universe {}",
              taskParams().ybSoftwareVersion,
              universe.name);

          createUpgradeTask(userIntent, universe, pi);

          createRunYsqlUpgradeTask(taskParams().ybSoftwareVersion)
              .setSubTaskGroupType(getTaskSubGroupType());

          createUpdateSoftwareVersionTask(taskParams().ybSoftwareVersion)
              .setSubTaskGroupType(getTaskSubGroupType());
          break;
        case GFlags:
          log.info("Upgrading GFlags in universe {}", universe.name);
          updateGFlagsPersistTasks(taskParams().masterGFlags, taskParams().tserverGFlags)
              .setSubTaskGroupType(getTaskSubGroupType());

          createUpgradeTask(userIntent, universe, pi);
          break;
      }

      // Marks update of this universe as a success only if all the tasks before it succeeded.
      createMarkUniverseUpdateSuccessTasks()
          .setSubTaskGroupType(SubTaskGroupType.ConfigureUniverse);

      // Run all the tasks.
      getRunnableTask().runSubTasks();
    } catch (Throwable t) {
      log.error("Error executing task {} with error={}.", getName(), t);

      // Clear previous subtasks if any.
      getRunnableTask().reset();
      // If the task failed, we don't want the loadbalancer to be disabled,
      // so we enable it again in case of errors.
      createLoadBalancerStateChangeTask(true /*enable*/).setSubTaskGroupType(getTaskSubGroupType());

      getRunnableTask().runSubTasks();

      throw t;
    } finally {
      unlockUniverseForUpdate();
    }
    log.info("Finished {} task.", getName());
  }

  private SubTaskGroupType getTaskSubGroupType() {
    switch (taskParams().taskType) {
      case Software:
        return SubTaskGroupType.UpgradingSoftware;
      case GFlags:
        return SubTaskGroupType.UpdatingGFlags;
      default:
        return SubTaskGroupType.Invalid;
    }
  }

  private void createUpgradeTask(UserIntent userIntent, Universe universe, PlacementInfo pi) {
    String ybSoftwareVersion = null;
    boolean masterChanged = false;
    boolean tserverChanged = false;
    if (taskParams().taskType == UpgradeTaskParams.UpgradeTaskType.Software) {
      ybSoftwareVersion = taskParams().ybSoftwareVersion;
      masterChanged = true;
      tserverChanged = true;
    } else {
      ybSoftwareVersion = userIntent.ybSoftwareVersion;
      if (!taskParams().masterGFlags.equals(userIntent.masterGFlags)) {
        masterChanged = true;
      }
      if (!taskParams().tserverGFlags.equals(userIntent.tserverGFlags)) {
        tserverChanged = true;
      }
    }

    createSingleKubernetesExecutorTask(CommandType.POD_INFO, pi, false);

    KubernetesPlacement placement = new KubernetesPlacement(pi);

    Provider provider =
        Provider.get(UUID.fromString(taskParams().getPrimaryCluster().userIntent.provider));

    UniverseDefinitionTaskParams universeDetails = universe.getUniverseDetails();

    boolean newNamingStyle = taskParams().useNewHelmNamingStyle;

    String masterAddresses =
        PlacementInfoUtil.computeMasterAddresses(
            pi,
            placement.masters,
            taskParams().nodePrefix,
            provider,
            universeDetails.communicationPorts.masterRpcPort,
            newNamingStyle);

    if (masterChanged) {
      userIntent.masterGFlags = taskParams().masterGFlags;
      upgradePodsTask(
          placement,
          masterAddresses,
          null,
          ServerType.MASTER,
          ybSoftwareVersion,
          taskParams().sleepAfterMasterRestartMillis,
          masterChanged,
          tserverChanged,
          newNamingStyle);
    }
    if (tserverChanged) {
      createLoadBalancerStateChangeTask(false /*enable*/)
          .setSubTaskGroupType(getTaskSubGroupType());

      userIntent.tserverGFlags = taskParams().tserverGFlags;
      upgradePodsTask(
          placement,
          masterAddresses,
          null,
          ServerType.TSERVER,
          ybSoftwareVersion,
          taskParams().sleepAfterTServerRestartMillis,
          false /* master change is false since it has already been upgraded.*/,
          tserverChanged,
          newNamingStyle);

      createLoadBalancerStateChangeTask(true /*enable*/).setSubTaskGroupType(getTaskSubGroupType());
    }
  }
}
