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
import com.yugabyte.yw.commissioner.ITask.Retryable;
import com.yugabyte.yw.commissioner.UserTaskDetails.SubTaskGroupType;
import com.yugabyte.yw.commissioner.tasks.params.NodeTaskParams;
import com.yugabyte.yw.common.DnsManager;
import com.yugabyte.yw.common.NodeActionType;
import com.yugabyte.yw.common.PlacementInfoUtil;
import com.yugabyte.yw.common.config.GlobalConfKeys;
import com.yugabyte.yw.common.config.UniverseConfKeys;
import com.yugabyte.yw.forms.NodeActionFormData;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams.Cluster;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams.UserIntent;
import com.yugabyte.yw.models.Universe;
import com.yugabyte.yw.models.helpers.CommonUtils;
import com.yugabyte.yw.models.helpers.NodeDetails;
import com.yugabyte.yw.models.helpers.NodeDetails.MasterState;
import com.yugabyte.yw.models.helpers.NodeDetails.NodeState;
import com.yugabyte.yw.models.helpers.PlacementInfo;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.yb.util.TabletServerInfo;

// Allows the removal of a node from a universe. Ensures the task waits for the right set of
// server data move primitives. And stops using the underlying instance, though YW still owns it.

@Slf4j
@Retryable
public class RemoveNodeFromUniverse extends UniverseDefinitionTaskBase {

  private String replacementMasterName;

  @Inject
  protected RemoveNodeFromUniverse(BaseTaskDependencies baseTaskDependencies) {
    super(baseTaskDependencies);
  }

  @Override
  protected NodeTaskParams taskParams() {
    return (NodeTaskParams) taskParams;
  }

  private String findReplacementMasterIfApplicable(
      Universe universe, NodeDetails currentNode, boolean pickNewNode) {
    boolean startMasterOnRemoveNode =
        confGetter.getGlobalConf(GlobalConfKeys.startMasterOnRemoveNode);
    if (startMasterOnRemoveNode && NodeActionFormData.startMasterOnRemoveNode) {
      return super.findReplacementMaster(universe, currentNode, pickNewNode);
    }
    return null;
  }

  private NodeDetails runBasicChecks(Universe universe) {
    NodeDetails currentNode = universe.getNode(taskParams().nodeName);
    if (currentNode == null) {
      String msg = "No node " + taskParams().nodeName + " found in universe " + universe.getName();
      log.error(msg);
      throw new RuntimeException(msg);
    }

    if (isFirstTry()) {
      currentNode.validateActionOnState(NodeActionType.REMOVE);
    }
    return currentNode;
  }

  @Override
  public void validateParams(boolean isFirstTry) {
    super.validateParams(isFirstTry);
    runBasicChecks(getUniverse());
  }

  @Override
  protected void createPrecheckTasks(Universe universe) {
    // Check again after locking.
    NodeDetails currentNode = runBasicChecks(getUniverse());
    boolean alwaysWaitForDataMove =
        confGetter.getConfForScope(getUniverse(), UniverseConfKeys.alwaysWaitForDataMove);
    if (alwaysWaitForDataMove) {
      performPrecheck();
    }
    addBasicPrecheckTasks();
    // Pick new only on first try.
    replacementMasterName = findReplacementMasterIfApplicable(universe, currentNode, isFirstTry());
  }

  private void freezeUniverseInTxn(Universe universe) {
    NodeDetails node = universe.getNode(taskParams().nodeName);
    if (node == null) {
      String msg = "No node " + taskParams().nodeName + " found in universe " + universe.getName();
      log.error(msg);
      throw new RuntimeException(msg);
    }
    if (node.isMaster) {
      if (replacementMasterName != null) {
        NodeDetails replacementMaster = universe.getNode(replacementMasterName);
        if (replacementMaster.masterState == null) {
          replacementMaster.masterState = MasterState.ToStart;
        }
      }
      node.masterState = MasterState.ToStop;
    }
  }

  @Override
  public void run() {
    log.info(
        "Started {} task for node {} in univ uuid={}",
        getName(),
        taskParams().nodeName,
        taskParams().getUniverseUUID());
    checkUniverseVersion();
    boolean alwaysWaitForDataMove =
        confGetter.getConfForScope(getUniverse(), UniverseConfKeys.alwaysWaitForDataMove);

    Universe universe =
        lockAndFreezeUniverseForUpdate(
            taskParams().expectedUniverseVersion, this::freezeUniverseInTxn);
    try {
      preTaskActions();
      NodeDetails currentNode = universe.getNode(taskParams().nodeName);
      taskParams().azUuid = currentNode.azUuid;
      taskParams().placementUuid = currentNode.placementUuid;

      Cluster currCluster =
          universe.getUniverseDetails().getClusterByUuid(taskParams().placementUuid);
      boolean masterReachable = false;
      // Update Node State to being removed.
      createSetNodeStateTask(currentNode, NodeState.Removing)
          .setSubTaskGroupType(SubTaskGroupType.RemovingNode);

      // Mark the tserver as blacklisted on the master leader.
      createPlacementInfoTask(
              Collections.singleton(currentNode), universe.getUniverseDetails().clusters)
          .setSubTaskGroupType(SubTaskGroupType.WaitForDataMigration);

      if (alwaysWaitForDataMove || isTabletMovementAvailable()) {
        createWaitForDataMoveTask().setSubTaskGroupType(SubTaskGroupType.WaitForDataMigration);
      }

      // Remove node from load balancer.
      createManageLoadBalancerTasks(
          createLoadBalancerMap(
              universe.getUniverseDetails(),
              Arrays.asList(currCluster),
              Collections.singleton(currentNode),
              null));
      createStopServerTasks(
              Collections.singleton(currentNode),
              ServerType.TSERVER,
              params -> {
                params.isIgnoreError = true;
                params.deconfigure = true;
              })
          .setSubTaskGroupType(SubTaskGroupType.StoppingNodeProcesses);
      if (universe.isYbcEnabled()) {
        createStopYbControllerTasks(Collections.singleton(currentNode), true /*isIgnoreErrors*/)
            .setSubTaskGroupType(SubTaskGroupType.StoppingNodeProcesses);
      }
      masterReachable = isMasterAliveOnNode(currentNode, universe.getMasterAddresses());

      // Remove its tserver status in YBA DB.
      createUpdateNodeProcessTask(taskParams().nodeName, ServerType.TSERVER, false)
          .setSubTaskGroupType(SubTaskGroupType.StoppingNodeProcesses);

      // Create the master replacement tasks.
      createMasterReplacementTasks(
          universe,
          currentNode,
          () -> replacementMasterName == null ? null : universe.getNode(replacementMasterName),
          masterReachable,
          true /* ignore stop error */);

      // Update the DNS entry for this universe.
      createDnsManipulationTask(DnsManager.DnsCommandType.Edit, false, universe)
          .setSubTaskGroupType(SubTaskGroupType.RemovingNode);

      // Update Node State to Removed
      createSetNodeStateTask(currentNode, NodeState.Removed)
          .setSubTaskGroupType(SubTaskGroupType.RemovingNode);

      // Update the swamper target file.
      createSwamperTargetUpdateTask(false /* removeFile */);

      // Mark universe task state to success
      createMarkUniverseUpdateSuccessTasks().setSubTaskGroupType(SubTaskGroupType.RemovingNode);

      // Run all the tasks.
      getRunnableTask().runSubTasks();
    } catch (Throwable t) {
      log.error("Error executing task {} with error='{}'.", getName(), t.getMessage(), t);
      throw t;
    } finally {
      // Mark the update of the universe as done. This will allow future edits/updates to the
      // universe to happen.
      unlockUniverseForUpdate();
    }
    log.info("Finished {} task.", getName());
  }

  private boolean isTabletMovementAvailable() {
    Universe universe = getUniverse();
    NodeDetails currentNode = universe.getNode(taskParams().nodeName);
    String softwareVersion =
        universe.getUniverseDetails().getPrimaryCluster().userIntent.ybSoftwareVersion;
    if (CommonUtils.isReleaseBefore(CommonUtils.MIN_LIVE_TABLET_SERVERS_RELEASE, softwareVersion)) {
      log.debug("ListLiveTabletServers is not supported for {} version", softwareVersion);
      return true;
    }

    // taskParams().placementUuid is not used because it will be null for RR.
    Cluster currCluster = universe.getUniverseDetails().getClusterByUuid(currentNode.placementUuid);
    UserIntent userIntent = currCluster.userIntent;
    PlacementInfo pi = currCluster.placementInfo;

    Collection<NodeDetails> nodesExcludingCurrentNode =
        new HashSet<>(universe.getNodesByCluster(currCluster.uuid));
    nodesExcludingCurrentNode.remove(currentNode);
    int rfInZone =
        PlacementInfoUtil.getZoneRF(
            pi,
            currentNode.cloudInfo.cloud,
            currentNode.cloudInfo.region,
            currentNode.cloudInfo.az);

    if (rfInZone == -1) {
      log.error(
          "Unexpected placement info in universe: {} rfInZone: {}", universe.getName(), rfInZone);
      throw new RuntimeException(
          "Error getting placement info for cluster with node: " + currentNode.nodeName);
    }

    // We do not get isActive() tservers due to new masters starting up changing
    //   nodeStates to not-active node states which will cause retry to fail.
    // Note: On master leader failover, if a tserver was already down, it will not be reported as a
    //    "live" tserver even though it has been less than
    //    "follower_unavailable_considered_failed_sec" secs since the tserver was down. This is
    //    fine because we do not take into account the current node and if it is not the current
    //    node that is down we may prematurely fail, which is expected.
    List<TabletServerInfo> liveTabletServers = getLiveTabletServers(universe);

    List<TabletServerInfo> tserversActiveInAZExcludingCurrentNode =
        liveTabletServers.stream()
            .filter(
                tserverInfo ->
                    currentNode.cloudInfo.cloud.equals(tserverInfo.getCloudInfo().getCloud())
                        && currentNode.cloudInfo.region.equals(
                            tserverInfo.getCloudInfo().getRegion())
                        && currentNode.cloudInfo.az.equals(tserverInfo.getCloudInfo().getZone())
                        && currCluster.uuid.equals(tserverInfo.getPlacementUuid())
                        && !currentNode.cloudInfo.private_ip.equals(
                            tserverInfo.getPrivateAddress().getHost()))
            .collect(Collectors.toList());

    long numActiveTservers = tserversActiveInAZExcludingCurrentNode.size();

    // We have replication number of copies a tablet so we need more than the replication
    //   factor number of nodes for tablets to move off.
    // We only want to move data if the number of nodes in the zone are more than or equal
    //   the RF of the zone.
    log.debug(
        "Cluster: {}, numNodes in cluster: {}, number of active tservers excluding current node"
            + " removing: {}, RF in az: {}",
        currCluster.uuid,
        userIntent.numNodes,
        numActiveTservers,
        rfInZone);
    return userIntent.numNodes > userIntent.replicationFactor && numActiveTservers >= rfInZone;
  }

  // Check that there is a place to move the tablets and if not, make sure there are no tablets
  // assigned to this tserver. Otherwise, do not allow the remove node task to succeed.
  public void performPrecheck() {
    Universe universe = getUniverse();
    NodeDetails currentNode = universe.getNode(taskParams().nodeName);

    if (!isTabletMovementAvailable()) {
      log.debug(
          "Tablets have nowhere to move off of tserver on node: {}. Checking if there are still"
              + " tablets assigned to it. A healthy tserver should not be removed.",
          currentNode.getNodeName());
      // TODO: Move this into a subtask.
      checkNoTabletsOnNode(universe, currentNode);
    }
    log.debug("Pre-check succeeded");
  }
}
