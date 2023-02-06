// Copyright (c) YugaByte, Inc.

package com.yugabyte.yw.commissioner.tasks;

import static com.yugabyte.yw.common.AssertHelper.assertJsonEqual;
import static com.yugabyte.yw.common.ModelFactory.createUniverse;
import static com.yugabyte.yw.models.TaskInfo.State.Failure;
import static com.yugabyte.yw.models.TaskInfo.State.Success;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.yugabyte.yw.commissioner.tasks.params.NodeTaskParams;
import com.yugabyte.yw.common.ApiUtils;
import com.yugabyte.yw.common.NodeActionType;
import com.yugabyte.yw.common.NodeManager;
import com.yugabyte.yw.common.ShellResponse;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams.UserIntent;
import com.yugabyte.yw.models.AvailabilityZone;
import com.yugabyte.yw.models.Region;
import com.yugabyte.yw.models.TaskInfo;
import com.yugabyte.yw.models.Universe;
import com.yugabyte.yw.models.helpers.NodeDetails;
import com.yugabyte.yw.models.helpers.NodeDetails.NodeState;
import com.yugabyte.yw.models.helpers.TaskType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.yb.client.ChangeMasterClusterConfigResponse;
import org.yb.client.GetLoadMovePercentResponse;
import org.yb.client.ListMastersResponse;
import org.yb.client.YBClient;
import play.libs.Json;

@RunWith(MockitoJUnitRunner.class)
public class RemoveNodeFromUniverseTest extends CommissionerBaseTest {

  private Universe defaultUniverse;

  public void setUp(boolean withMaster, int numNodes, int replicationFactor, boolean multiZone) {
    super.setUp();

    Region region = Region.create(defaultProvider, "test-region", "Region 1", "yb-image-1");
    AvailabilityZone.createOrThrow(region, "az-1", "az-1", "subnet-1");
    if (multiZone) {
      AvailabilityZone.createOrThrow(region, "az-2", "az-2", "subnet-2");
      AvailabilityZone.createOrThrow(region, "az-3", "az-3", "subnet-3");
    }
    // create default universe
    UserIntent userIntent = new UserIntent();
    userIntent.numNodes = numNodes;
    userIntent.provider = defaultProvider.uuid.toString();
    userIntent.ybSoftwareVersion = "yb-version";
    userIntent.accessKeyCode = "demo-access";
    userIntent.replicationFactor = replicationFactor;
    userIntent.regionList = ImmutableList.of(region.uuid);
    defaultUniverse = createUniverse(defaultCustomer.getCustomerId());
    Universe.saveDetails(
        defaultUniverse.universeUUID,
        ApiUtils.mockUniverseUpdater(userIntent, withMaster /* setMasters */));
    defaultUniverse = Universe.getOrBadRequest(defaultUniverse.universeUUID);

    Universe.UniverseUpdater updater =
        universe -> {
          UniverseDefinitionTaskParams universeDetails = universe.getUniverseDetails();
          Set<NodeDetails> nodes = universeDetails.nodeDetailsSet;
          int count = 0;
          int numZones = 1;
          if (multiZone) {
            numZones = 3;
          }
          for (NodeDetails node : nodes) {
            node.cloudInfo.az = "az-" + (count % numZones + 1);
            nodes.add(node);
          }
          universeDetails.nodeDetailsSet = nodes;
          universe.setUniverseDetails(universeDetails);
        };
    Universe.saveDetails(defaultUniverse.universeUUID, updater);
    defaultUniverse = Universe.getOrBadRequest(defaultUniverse.universeUUID);

    YBClient mockClient = mock(YBClient.class);
    when(mockNodeManager.nodeCommand(any(), any()))
        .then(
            invocation -> {
              if (invocation.getArgument(0).equals(NodeManager.NodeCommandType.List)) {
                ShellResponse listResponse = new ShellResponse();
                NodeTaskParams params = invocation.getArgument(1);
                if (params.nodeUuid == null) {
                  listResponse.message = "{\"universe_uuid\":\"" + params.universeUUID + "\"}";
                } else {
                  listResponse.message =
                      "{\"universe_uuid\":\""
                          + params.universeUUID
                          + "\", "
                          + "\"node_uuid\": \""
                          + params.nodeUuid
                          + "\"}";
                }
                return listResponse;
              }
              return ShellResponse.create(ShellResponse.ERROR_CODE_SUCCESS, "true");
            });
    when(mockClient.waitForServer(any(), anyLong())).thenReturn(true);

    ChangeMasterClusterConfigResponse ccr = new ChangeMasterClusterConfigResponse(1111, "", null);
    GetLoadMovePercentResponse gpr = new GetLoadMovePercentResponse(0, "", 100.0, 0, 0, null);
    try {
      // WaitForTServerHeartBeats mock.
      doNothing().when(mockClient).waitForMasterLeader(anyLong());
      when(mockClient.changeMasterClusterConfig(any())).thenReturn(ccr);
      when(mockClient.getLoadMoveCompletion()).thenReturn(gpr);
      ListMastersResponse listMastersResponse = mock(ListMastersResponse.class);
      when(listMastersResponse.getMasters()).thenReturn(Collections.emptyList());
      when(mockClient.listMasters()).thenReturn(listMastersResponse);
    } catch (Exception e) {
    }

    when(mockYBClient.getClient(any(), any())).thenReturn(mockClient);
    mockWaits(mockClient, 3);
  }

  private TaskInfo submitTask(NodeTaskParams taskParams, String nodeName) {
    taskParams.nodeName = nodeName;
    try {
      UUID taskUUID = commissioner.submit(TaskType.RemoveNodeFromUniverse, taskParams);
      return waitForTask(taskUUID);
    } catch (InterruptedException e) {
      assertNull(e.getMessage());
    }
    return null;
  }

  private static final List<TaskType> REMOVE_NODE_TASK_SEQUENCE =
      ImmutableList.of(
          TaskType.SetNodeState,
          TaskType.UpdatePlacementInfo,
          TaskType.WaitForDataMove,
          TaskType.AnsibleClusterServerCtl,
          TaskType.UpdateNodeProcess,
          TaskType.UpdateNodeProcess,
          TaskType.SetNodeState,
          TaskType.UniverseUpdateSucceeded);

  private static final List<JsonNode> REMOVE_NODE_TASK_EXPECTED_RESULTS =
      ImmutableList.of(
          Json.toJson(ImmutableMap.of("state", "Removing")),
          Json.toJson(ImmutableMap.of()),
          Json.toJson(ImmutableMap.of()),
          Json.toJson(ImmutableMap.of("process", "tserver", "command", "stop")),
          Json.toJson(ImmutableMap.of("processType", "MASTER", "isAdd", false)),
          Json.toJson(ImmutableMap.of("processType", "TSERVER", "isAdd", false)),
          Json.toJson(ImmutableMap.of("state", "Removed")),
          Json.toJson(ImmutableMap.of()));

  private static final List<TaskType> REMOVE_NODE_WITH_MASTER =
      ImmutableList.of(
          TaskType.SetNodeState,
          TaskType.WaitForMasterLeader,
          TaskType.ChangeMasterConfig,
          TaskType.AnsibleClusterServerCtl,
          TaskType.WaitForMasterLeader,
          TaskType.UpdatePlacementInfo,
          TaskType.WaitForDataMove,
          TaskType.AnsibleClusterServerCtl,
          TaskType.UpdateNodeProcess,
          TaskType.UpdateNodeProcess,
          TaskType.SetNodeState,
          TaskType.UniverseUpdateSucceeded);

  private static final List<JsonNode> REMOVE_NODE_WITH_MASTER_RESULTS =
      ImmutableList.of(
          Json.toJson(ImmutableMap.of("state", "Removing")),
          Json.toJson(ImmutableMap.of()),
          Json.toJson(ImmutableMap.of()),
          Json.toJson(ImmutableMap.of("process", "master", "command", "stop")),
          Json.toJson(ImmutableMap.of()),
          Json.toJson(ImmutableMap.of()),
          Json.toJson(ImmutableMap.of()),
          Json.toJson(ImmutableMap.of("process", "tserver", "command", "stop")),
          Json.toJson(ImmutableMap.of("processType", "MASTER", "isAdd", false)),
          Json.toJson(ImmutableMap.of("processType", "TSERVER", "isAdd", false)),
          Json.toJson(ImmutableMap.of("state", "Removed")),
          Json.toJson(ImmutableMap.of()));

  private static final List<TaskType> REMOVE_NOT_EXISTS_NODE_TASK_SEQUENCE =
      ImmutableList.of(
          TaskType.SetNodeState,
          TaskType.UpdatePlacementInfo,
          TaskType.UpdateNodeProcess,
          TaskType.UpdateNodeProcess,
          TaskType.SetNodeState,
          TaskType.UniverseUpdateSucceeded);

  private static final List<JsonNode> REMOVE_NOT_EXISTS_NODE_TASK_EXPECTED_RESULTS =
      ImmutableList.of(
          Json.toJson(ImmutableMap.of("state", "Removing")),
          Json.toJson(ImmutableMap.of()),
          Json.toJson(ImmutableMap.of("processType", "MASTER", "isAdd", false)),
          Json.toJson(ImmutableMap.of("processType", "TSERVER", "isAdd", false)),
          Json.toJson(ImmutableMap.of("state", "Removed")),
          Json.toJson(ImmutableMap.of()));

  private enum RemoveType {
    WITH_MASTER,
    ONLY_TSERVER,
    NOT_EXISTS
  }

  private void assertRemoveNodeSequence(
      Map<Integer, List<TaskInfo>> subTasksByPosition, RemoveType type, boolean moveData) {
    int position = 0;
    int taskPosition = 0;
    switch (type) {
      case WITH_MASTER:
        for (TaskType taskType : REMOVE_NODE_WITH_MASTER) {
          if (taskType.equals(TaskType.WaitForDataMove) && !moveData) {
            position++;
            continue;
          }

          List<TaskInfo> tasks = subTasksByPosition.get(taskPosition);
          assertEquals(1, tasks.size());
          assertEquals(taskType, tasks.get(0).getTaskType());
          JsonNode expectedResults = REMOVE_NODE_WITH_MASTER_RESULTS.get(position);
          List<JsonNode> taskDetails =
              tasks.stream().map(TaskInfo::getTaskDetails).collect(Collectors.toList());
          assertJsonEqual(expectedResults, taskDetails.get(0));
          position++;
          taskPosition++;
        }
        break;
      case ONLY_TSERVER:
        for (TaskType taskType : REMOVE_NODE_TASK_SEQUENCE) {
          List<TaskInfo> tasks = subTasksByPosition.get(position);
          assertEquals(1, tasks.size());
          assertEquals(taskType, tasks.get(0).getTaskType());
          JsonNode expectedResults = REMOVE_NODE_TASK_EXPECTED_RESULTS.get(position);
          List<JsonNode> taskDetails =
              tasks.stream().map(TaskInfo::getTaskDetails).collect(Collectors.toList());
          assertJsonEqual(expectedResults, taskDetails.get(0));
          position++;
          taskPosition++;
        }
        break;
      case NOT_EXISTS:
        for (TaskType taskType : REMOVE_NOT_EXISTS_NODE_TASK_SEQUENCE) {
          List<TaskInfo> tasks = subTasksByPosition.get(position);
          assertEquals(1, tasks.size());
          assertEquals(taskType, tasks.get(0).getTaskType());
          JsonNode expectedResults = REMOVE_NOT_EXISTS_NODE_TASK_EXPECTED_RESULTS.get(position);
          List<JsonNode> taskDetails =
              tasks.stream().map(TaskInfo::getTaskDetails).collect(Collectors.toList());
          assertJsonEqual(expectedResults, taskDetails.get(0));
          position++;
          taskPosition++;
        }
        break;
    }
  }

  @Test
  public void testRemoveNodeSuccess() {
    setUp(false, 4, 3, false);
    NodeTaskParams taskParams = new NodeTaskParams();
    taskParams.universeUUID = defaultUniverse.universeUUID;
    taskParams.expectedUniverseVersion = 3;

    TaskInfo taskInfo = submitTask(taskParams, "host-n1");
    assertEquals(Success, taskInfo.getTaskState());

    List<TaskInfo> subTasks = taskInfo.getSubTasks();
    Map<Integer, List<TaskInfo>> subTasksByPosition =
        subTasks.stream().collect(Collectors.groupingBy(TaskInfo::getPosition));
    assertRemoveNodeSequence(subTasksByPosition, RemoveType.ONLY_TSERVER, true);
  }

  @Test
  public void testRemoveNodeWithMaster() {
    setUp(true, 4, 3, false);
    NodeTaskParams taskParams = new NodeTaskParams();
    taskParams.universeUUID = defaultUniverse.universeUUID;
    taskParams.expectedUniverseVersion = 3;

    TaskInfo taskInfo = submitTask(taskParams, "host-n1");
    assertEquals(Success, taskInfo.getTaskState());

    List<TaskInfo> subTasks = taskInfo.getSubTasks();
    Map<Integer, List<TaskInfo>> subTasksByPosition =
        subTasks.stream().collect(Collectors.groupingBy(TaskInfo::getPosition));
    assertRemoveNodeSequence(subTasksByPosition, RemoveType.WITH_MASTER, true);
  }

  @Test
  public void testRemoveUnknownNode() {
    setUp(false, 4, 3, false);
    NodeTaskParams taskParams = new NodeTaskParams();
    taskParams.universeUUID = defaultUniverse.universeUUID;
    taskParams.expectedUniverseVersion = 3;

    TaskInfo taskInfo = submitTask(taskParams, "host-n9");
    assertEquals(Failure, taskInfo.getTaskState());
  }

  @Test
  public void testRemoveNonExistentNode() {
    setUp(false, 4, 3, false);
    NodeTaskParams taskParams = new NodeTaskParams();
    taskParams.universeUUID = defaultUniverse.universeUUID;
    taskParams.expectedUniverseVersion = 3;
    ShellResponse dummyShellResponse = new ShellResponse();
    dummyShellResponse.message = null;
    doReturn(dummyShellResponse).when(mockNodeManager).nodeCommand(any(), any());

    TaskInfo taskInfo = submitTask(taskParams, "host-n1");
    assertEquals(Success, taskInfo.getTaskState());

    List<TaskInfo> subTasks = taskInfo.getSubTasks();
    Map<Integer, List<TaskInfo>> subTasksByPosition =
        subTasks.stream().collect(Collectors.groupingBy(TaskInfo::getPosition));
    assertRemoveNodeSequence(subTasksByPosition, RemoveType.NOT_EXISTS, true);
  }

  @Test
  public void testRemoveNodeWithNoDataMove() {
    setUp(true, 3, 3, false);
    NodeTaskParams taskParams = new NodeTaskParams();
    taskParams.universeUUID = defaultUniverse.universeUUID;
    taskParams.expectedUniverseVersion = 3;

    TaskInfo taskInfo = submitTask(taskParams, "host-n1");
    assertEquals(Success, taskInfo.getTaskState());

    List<TaskInfo> subTasks = taskInfo.getSubTasks();
    Map<Integer, List<TaskInfo>> subTasksByPosition =
        subTasks.stream().collect(Collectors.groupingBy(TaskInfo::getPosition));
    assertRemoveNodeSequence(subTasksByPosition, RemoveType.WITH_MASTER, false);
  }

  @Test
  public void testRemoveNodeWithNoDataMoveRF5() {
    setUp(true, 5, 5, true);
    NodeTaskParams taskParams = new NodeTaskParams();
    taskParams.universeUUID = defaultUniverse.universeUUID;
    taskParams.expectedUniverseVersion = 3;

    TaskInfo taskInfo = submitTask(taskParams, "host-n1");
    assertEquals(Success, taskInfo.getTaskState());

    List<TaskInfo> subTasks = taskInfo.getSubTasks();
    Map<Integer, List<TaskInfo>> subTasksByPosition =
        subTasks.stream().collect(Collectors.groupingBy(TaskInfo::getPosition));
    assertRemoveNodeSequence(subTasksByPosition, RemoveType.WITH_MASTER, false);
  }

  @Test
  public void testRemoveNodeRF5() {
    setUp(true, 6, 5, true);
    NodeTaskParams taskParams = new NodeTaskParams();
    taskParams.universeUUID = defaultUniverse.universeUUID;
    taskParams.expectedUniverseVersion = 3;

    TaskInfo taskInfo = submitTask(taskParams, "host-n1");
    assertEquals(Success, taskInfo.getTaskState());

    List<TaskInfo> subTasks = taskInfo.getSubTasks();
    Map<Integer, List<TaskInfo>> subTasksByPosition =
        subTasks.stream().collect(Collectors.groupingBy(TaskInfo::getPosition));
    assertRemoveNodeSequence(subTasksByPosition, RemoveType.WITH_MASTER, true);
  }

  @Test
  public void testRemoveNodeAllowedState() {
    Set<NodeState> allowedStates = NodeState.allowedStatesForAction(NodeActionType.REMOVE);
    Set<NodeState> expectedStates =
        ImmutableSet.of(
            NodeState.Adding,
            NodeState.Live,
            NodeState.ToBeRemoved,
            NodeState.ToJoinCluster,
            NodeState.Stopped,
            NodeState.Starting,
            NodeState.Stopping,
            NodeState.Removing);
    assertEquals(expectedStates, allowedStates);
  }
}
