package org.yb.cdc;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService.TabletCheckpointPair;
import org.yb.cdc.common.CDCBaseClass;
import org.yb.cdc.util.CDCSubscriber;
import org.yb.cdc.util.TestUtils;
import org.yb.client.GetTabletListToPollForCDCResponse;
import org.yb.client.YBClient;
import org.yb.util.YBTestRunnerNonTsanOnly;

import static org.yb.AssertionWrappers.*;

@RunWith(value = YBTestRunnerNonTsanOnly.class)
public class TestGetTabletsApiCdc extends CDCBaseClass {
  private final Logger LOGGER = LoggerFactory.getLogger(TestGetTabletsApiCdc.class);

  private CDCSubscriber testSubscriber;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    statement = connection.createStatement();
    statement.execute("drop table if exists test;");
    statement.execute("create table test (a int primary key, b int);");
  }

  @Test
  public void verifyIfNewApiReturnsExpectedValues() throws Exception {
    testSubscriber = new CDCSubscriber(getMasterAddresses());
    testSubscriber.createStream("proto");

    // Insert some records in the table
    for (int i = 0; i < 10; ++i) {
      statement.execute(String.format("INSERT INTO test VALUES (%d,%d);", i, i+1));
    }

    // This is the tablet Id that we need to split
    String tabletId = testSubscriber.getTabletId();

    // Call the new API to see if we are receiving the correct tabletId
    YBClient ybClient = testSubscriber.getSyncClient();
    assertNotNull(ybClient);

    GetTabletListToPollForCDCResponse resp = ybClient.getTabletListToPollForCdc(
      ybClient.openTableByUUID(
        testSubscriber.getTableId()), testSubscriber.getDbStreamId(), testSubscriber.getTableId());

    // Assert that there is only one tablet checkpoint pair
    assertEquals(1, resp.getTabletCheckpointPairListSize());

    // Since there is one tablet only, verify its tablet ID
    TabletCheckpointPair pair = resp.getTabletCheckpointPairList().get(0);
    assertEquals(tabletId, pair.getTabletId().toStringUtf8());

    // TestUtils.splitTablet(tabletId);

    // Wait for tablet split to happen

    // Call the new API to get the tablets
  }
}
