package org.yb.cdc;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;
import org.yb.cdc.CdcService.CDCErrorPB.Code;
import org.yb.cdc.CdcService.TabletCheckpointPair;
import org.yb.cdc.common.CDCBaseClass;
import org.yb.cdc.util.CDCSubscriber;
import org.yb.cdc.util.Checkpoint;
import org.yb.cdc.util.TestUtils;
import org.yb.client.CDCErrorException;
import org.yb.client.GetTabletListToPollForCDCResponse;
import org.yb.client.YBClient;
import org.yb.util.YBTestRunnerNonTsanOnly;

import static org.yb.AssertionWrappers.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.awaitility.Awaitility;

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

    GetTabletListToPollForCDCResponse respBeforeSplit = ybClient.getTabletListToPollForCdc(
      ybClient.openTableByUUID(
        testSubscriber.getTableId()), testSubscriber.getDbStreamId(), testSubscriber.getTableId());

    // Assert that there is only one tablet checkpoint pair
    assertEquals(1, respBeforeSplit.getTabletCheckpointPairListSize());

    // Since there is one tablet only, verify its tablet ID
    TabletCheckpointPair pair = respBeforeSplit.getTabletCheckpointPairList().get(0);
    assertEquals(tabletId, pair.getTabletId().toStringUtf8());

    splitTablet(getMasterAddresses(), tabletId);

    // Keep calling get changes in a loop until we see tablet_split error
    try {
      List<CdcService.CDCSDKProtoRecordPB> outputList = new ArrayList<>();
      // Keep calling GetChanges until it throws an Exception
      while (true) {
        testSubscriber.getResponseFromCDC(outputList, cp);
      }
    } catch (CDCErrorException cdcException) {
      if (cdcException.getCDCError().getCode() == Code.TABLET_SPLIT) {
        System.out.println("Tablet split error reported");
      } else {
        throw cdcException;
      }
    }

    // Wait for tablet split to happen
    waitForTabletSplit(ybClient, testSubscriber.getTableId(), 2);

    // Call the new API to get the tablets
    GetTabletListToPollForCDCResponse respAfterSplit = ybClient.getTabletListToPollForCdc(
      ybClient.openTableByUUID(
        testSubscriber.getTableId()), testSubscriber.getDbStreamId(), testSubscriber.getTableId());

    // Ideally there would be 2 tablets since the original one has split into 2
    assertEquals(2, respAfterSplit.getTabletCheckpointPairListSize());
  }

  private void waitForTabletSplit(YBClient ybClient, String tableId,
                                  int tabletCount) throws Exception {
    Awaitility.await()
      .pollDelay(Duration.ofSeconds(10))
      .atMost(Duration.ofSeconds(120))
      .until(() -> {
        Set<String> tabletIds = ybClient.getTabletUUIDs(ybClient.openTableByUUID(tableId));
        return tabletIds.size() == tabletCount;
      });
  }
}
