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
import com.yugabyte.yw.commissioner.TaskExecutor.SubTaskGroup;
import com.yugabyte.yw.commissioner.UserTaskDetails;
import com.yugabyte.yw.commissioner.tasks.subtasks.KubernetesCommandExecutor;
import com.yugabyte.yw.common.PlacementInfoUtil;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams;
import com.yugabyte.yw.models.AvailabilityZone;
import com.yugabyte.yw.models.Provider;
import com.yugabyte.yw.models.Universe;
import com.yugabyte.yw.models.helpers.PlacementInfo;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DestroyKubernetesUniverse extends DestroyUniverse {

  @Inject
  public DestroyKubernetesUniverse(BaseTaskDependencies baseTaskDependencies) {
    super(baseTaskDependencies);
  }

  @Override
  public void run() {
    try {

      // Update the universe DB with the update to be performed and set the 'updateInProgress' flag
      // to prevent other updates from happening.
      Universe universe = null;
      if (params().isForceDelete) {
        universe = forceLockUniverseForUpdate(-1);
      } else {
        universe = lockUniverseForUpdate(-1 /* expectedUniverseVersion */);
      }

      preTaskActions();

      UniverseDefinitionTaskParams.UserIntent userIntent =
          universe.getUniverseDetails().getPrimaryCluster().userIntent;
      UUID providerUUID = UUID.fromString(userIntent.provider);

      Map<String, String> universeConfig = universe.getConfig();
      boolean runHelmDelete = universeConfig.containsKey(Universe.HELM2_LEGACY);

      PlacementInfo pi = universe.getUniverseDetails().getPrimaryCluster().placementInfo;

      Provider provider = Provider.get(UUID.fromString(userIntent.provider));

      Map<UUID, Map<String, String>> azToConfig = PlacementInfoUtil.getConfigPerAZ(pi);
      boolean isMultiAz = PlacementInfoUtil.isMultiAZ(provider);

      // Cleanup the kms_history table
      createDestroyEncryptionAtRestTask()
          .setSubTaskGroupType(UserTaskDetails.SubTaskGroupType.RemovingUnusedServers);

      // Try to unify this with the edit remove pods/deployments flow. Currently delete is
      // tied down to a different base class which makes params porting not straight-forward.
      SubTaskGroup helmDeletes =
          getTaskExecutor()
              .createSubTaskGroup(
                  KubernetesCommandExecutor.CommandType.HELM_DELETE.getSubTaskGroupName(),
                  executor);
      helmDeletes.setSubTaskGroupType(UserTaskDetails.SubTaskGroupType.RemovingUnusedServers);

      SubTaskGroup volumeDeletes =
          getTaskExecutor()
              .createSubTaskGroup(
                  KubernetesCommandExecutor.CommandType.VOLUME_DELETE.getSubTaskGroupName(),
                  executor);
      volumeDeletes.setSubTaskGroupType(UserTaskDetails.SubTaskGroupType.RemovingUnusedServers);

      SubTaskGroup namespaceDeletes =
          getTaskExecutor()
              .createSubTaskGroup(
                  KubernetesCommandExecutor.CommandType.NAMESPACE_DELETE.getSubTaskGroupName(),
                  executor);
      namespaceDeletes.setSubTaskGroupType(UserTaskDetails.SubTaskGroupType.RemovingUnusedServers);

      for (Entry<UUID, Map<String, String>> entry : azToConfig.entrySet()) {
        UUID azUUID = entry.getKey();
        String azName = isMultiAz ? AvailabilityZone.get(azUUID).code : null;

        Map<String, String> config = entry.getValue();

        String namespace = config.get("KUBENAMESPACE");

        if (runHelmDelete || namespace != null) {
          // Delete the helm deployments.
          helmDeletes.addSubTask(
              createDestroyKubernetesTask(
                  universe.getUniverseDetails().nodePrefix,
                  universe.getUniverseDetails().useNewHelmNamingStyle,
                  azName,
                  config,
                  KubernetesCommandExecutor.CommandType.HELM_DELETE,
                  providerUUID));
        }

        // Delete the PVCs created for this AZ.
        volumeDeletes.addSubTask(
            createDestroyKubernetesTask(
                universe.getUniverseDetails().nodePrefix,
                universe.getUniverseDetails().useNewHelmNamingStyle,
                azName,
                config,
                KubernetesCommandExecutor.CommandType.VOLUME_DELETE,
                providerUUID));

        // TODO(bhavin192): delete the pull secret as well? As of now,
        // we depend on the fact that, deleting the namespace will
        // delete the pull secret. That won't be the case with
        // providers which have KUBENAMESPACE paramter in the AZ
        // config. How to find the pull secret name? Should we delete
        // it when we have multiple releases in one namespace?. It is
        // possible that same provider is creating multiple releases
        // in one namespace. Tracked here:
        // https://github.com/yugabyte/yugabyte-db/issues/7012

        // Delete the namespaces of the deployments only if those were
        // created by us.
        if (namespace == null) {
          namespaceDeletes.addSubTask(
              createDestroyKubernetesTask(
                  universe.getUniverseDetails().nodePrefix,
                  universe.getUniverseDetails().useNewHelmNamingStyle,
                  azName,
                  config,
                  KubernetesCommandExecutor.CommandType.NAMESPACE_DELETE,
                  providerUUID));
        }
      }

      getRunnableTask().addSubTaskGroup(helmDeletes);
      getRunnableTask().addSubTaskGroup(volumeDeletes);
      getRunnableTask().addSubTaskGroup(namespaceDeletes);

      // Create tasks to remove the universe entry from the Universe table.
      createRemoveUniverseEntryTask()
          .setSubTaskGroupType(UserTaskDetails.SubTaskGroupType.RemovingUnusedServers);

      // Update the swamper target file.
      createSwamperTargetUpdateTask(true /* removeFile */);

      // Run all the tasks.
      getRunnableTask().runSubTasks();
    } catch (Throwable t) {
      // If for any reason destroy fails we would just unlock the universe for update
      try {
        unlockUniverseForUpdate();
      } catch (Throwable t1) {
        // Ignore the error
      }
      log.error("Error executing task {} with error='{}'.", getName(), t.getMessage(), t);
      throw t;
    }
    log.info("Finished {} task.", getName());
  }

  protected KubernetesCommandExecutor createDestroyKubernetesTask(
      String nodePrefix,
      boolean newNamingStyle,
      String az,
      Map<String, String> config,
      KubernetesCommandExecutor.CommandType commandType,
      UUID providerUUID) {
    KubernetesCommandExecutor.Params params = new KubernetesCommandExecutor.Params();
    params.commandType = commandType;
    params.nodePrefix = nodePrefix;
    params.providerUUID = providerUUID;
    if (az != null) {
      params.nodePrefix = String.format("%s-%s", nodePrefix, az);
    }
    if (config != null) {
      params.config = config;
      // This assumes that the config is az config. It is true in this
      // particular case, all callers just pass az config.
      // params.namespace remains null if config is not passed.
      params.namespace =
          PlacementInfoUtil.getKubernetesNamespace(
              nodePrefix, az, config, newNamingStyle, /*isReadOnlyCluster*/ false);
    }
    params.universeUUID = taskParams().universeUUID;
    KubernetesCommandExecutor task = createTask(KubernetesCommandExecutor.class);
    task.initialize(params);
    return task;
  }
}
