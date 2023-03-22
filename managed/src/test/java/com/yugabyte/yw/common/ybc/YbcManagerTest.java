// Copyright (c) YugaByte, Inc.

package com.yugabyte.yw.common.ybc;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.yb.client.YbcClient;
import org.yb.ybc.PingRequest;

import com.yugabyte.yw.common.FakeDBApplication;
import com.yugabyte.yw.common.ModelFactory;
import com.yugabyte.yw.common.config.RuntimeConfGetter;
import com.yugabyte.yw.common.customer.config.CustomerConfigService;
import com.yugabyte.yw.common.utils.Pair;
import com.yugabyte.yw.models.Customer;
import com.yugabyte.yw.models.Universe;

@RunWith(MockitoJUnitRunner.class)
public class YbcManagerTest extends FakeDBApplication {

  private CustomerConfigService mockCustomerConfigService;
  private RuntimeConfGetter mockConfGetter;
  private YbcManager spyYbcManager;
  private YbcManager ybcManager;

  private Universe testUniverse;
  private Customer testCustomer;

  @Before
  public void setup() {
    mockCustomerConfigService = mock(CustomerConfigService.class);
    mockConfGetter = mock(RuntimeConfGetter.class);
    ybcManager =
        new YbcManager(
            mockYbcClientService,
            mockCustomerConfigService,
            mockBackupUtil,
            mockConfGetter,
            mockReleaseManager,
            mockNodeManager);
    spyYbcManager = spy(ybcManager);
    testCustomer = ModelFactory.testCustomer();
    testUniverse = ModelFactory.createUniverse(testCustomer.getCustomerId());
    ModelFactory.addNodesToUniverse(testUniverse.getUniverseUUID(), 3);
  }

  @Test
  public void testGetYbcClientIpPair() {
    YbcClient mockYbcClient_Ip1 = mock(YbcClient.class);
    YbcClient mockYbcClient_Ip2 = mock(YbcClient.class);
    YbcClient mockYbcClient_Ip3 = mock(YbcClient.class);
    lenient()
        .when(mockYbcClientService.getNewClient(eq("127.0.0.1"), anyInt(), eq(null)))
        .thenReturn(mockYbcClient_Ip1);
    lenient()
        .when(mockYbcClientService.getNewClient(eq("127.0.0.2"), anyInt(), eq(null)))
        .thenReturn(mockYbcClient_Ip2);
    lenient()
        .when(mockYbcClientService.getNewClient(eq("127.0.0.3"), anyInt(), eq(null)))
        .thenReturn(mockYbcClient_Ip3);
    when(spyYbcManager.ybcPingCheck(any(YbcClient.class))).thenReturn(true);
    Pair<YbcClient, String> clientIpPair =
        spyYbcManager.getAvailableYbcClientIpPair(testUniverse.getUniverseUUID(), null);
    assert (clientIpPair.getFirst() != null && StringUtils.isNotBlank(clientIpPair.getSecond()));
    assert (new HashSet<>(Arrays.asList(mockYbcClient_Ip1, mockYbcClient_Ip2, mockYbcClient_Ip3))
        .contains(clientIpPair.getFirst()));
  }

  @Test
  public void testGetYbcClientIpPairOneHealthyNode() {
    YbcClient mockYbcClient_Ip1 = mock(YbcClient.class);
    YbcClient mockYbcClient_Ip2 = mock(YbcClient.class);
    YbcClient mockYbcClient_Ip3 = mock(YbcClient.class);
    lenient()
        .when(mockYbcClientService.getNewClient(eq("127.0.0.1"), anyInt(), eq(null)))
        .thenReturn(mockYbcClient_Ip1);
    lenient()
        .when(mockYbcClientService.getNewClient(eq("127.0.0.2"), anyInt(), eq(null)))
        .thenReturn(mockYbcClient_Ip2);
    lenient()
        .when(mockYbcClientService.getNewClient(eq("127.0.0.3"), anyInt(), eq(null)))
        .thenReturn(mockYbcClient_Ip3);

    when(spyYbcManager.ybcPingCheck(any(YbcClient.class))).thenReturn(false);
    when(spyYbcManager.ybcPingCheck(eq(mockYbcClient_Ip2))).thenReturn(true);
    Pair<YbcClient, String> clientIpPair =
        spyYbcManager.getAvailableYbcClientIpPair(testUniverse.getUniverseUUID(), null);
    assert (clientIpPair.getFirst() != null && StringUtils.isNotBlank(clientIpPair.getSecond()));
    assert (clientIpPair.getFirst().equals(mockYbcClient_Ip2));
  }

  @Test
  public void testGetYbcClientIpPairNoHealthyNodes() {
    YbcClient mockYbcClient_Ip1 = mock(YbcClient.class);
    YbcClient mockYbcClient_Ip2 = mock(YbcClient.class);
    YbcClient mockYbcClient_Ip3 = mock(YbcClient.class);
    when(mockYbcClientService.getNewClient(eq("127.0.0.1"), anyInt(), eq(null)))
        .thenReturn(mockYbcClient_Ip1);
    when(mockYbcClientService.getNewClient(eq("127.0.0.2"), anyInt(), eq(null)))
        .thenReturn(mockYbcClient_Ip2);
    when(mockYbcClientService.getNewClient(eq("127.0.0.3"), anyInt(), eq(null)))
        .thenReturn(mockYbcClient_Ip3);

    when(spyYbcManager.ybcPingCheck(any(YbcClient.class))).thenReturn(false);
    assertThrows(
        RuntimeException.class,
        () -> spyYbcManager.getAvailableYbcClientIpPair(testUniverse.getUniverseUUID(), null));
  }
}
