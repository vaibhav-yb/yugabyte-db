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
    System.out.println("VKVK before setting the flags");
    setServerFlag(getTserverHostAndPort(), "update_min_cdc_indices_interval_secs", "1");
    setServerFlag(getTserverHostAndPort(), "cdc_state_checkpoint_update_interval_ms", "1");
    System.out.println("VKVK after setting the flags");
    testSubscriber = new CDCSubscriber(getMasterAddresses());
    testSubscriber.createStream("proto");

    // Insert some records in the table
    for (int i = 0; i < 2000; ++i) {
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

    // TestUtils.compactTable(testSubscriber.getTableId());
    System.out.println("VKVK before flush tablet command");
    ybClient.flushTable(testSubscriber.getTableId());

    // Wait for the flush table command to succeed
    System.out.println("VKVK waiting for 60 seconds now");
    TestUtils.waitFor(Duration.ofSeconds(60));

    System.out.println("VKVK before split tablet command");
    ybClient.splitTablet(tabletId);
    System.out.println("VKVK after split tablet command");

    // Insert more records after scheduling the split tablet task
    for (int i = 2000; i < 10000; ++i) {
      statement.execute(String.format("INSERT INTO test VALUES (%d,%d);", i, i+1));
    }

    // Wait for tablet split to happen
    System.out.println("VKVK Before waiting for tablet split");
    waitForTabletSplit(ybClient, testSubscriber.getTableId(), 2);
    System.out.println("VKVK After waiting for tablet split");

    // Keep calling get changes in a loop until we see tablet_split error
    boolean isFirstTime = true;
    try {
      List<CdcService.CDCSDKProtoRecordPB> outputList = new ArrayList<>();
      // Keep calling GetChanges until it throws an Exception
      while (true) {
        if (isFirstTime) {
          testSubscriber.getResponseFromCDC(outputList);
          isFirstTime = false;
        } else {
          testSubscriber.getResponseFromCDC(outputList, testSubscriber.getSubscriberCheckpoint());
        }
      }
    } catch (CDCErrorException cdcException) {
      if (cdcException.getCDCError().getCode() == Code.TABLET_SPLIT) {
        System.out.println("VKVK Tablet split error reported");
      } else {
        throw cdcException;
      }
    }

    // Call the new API to get the tablets
    GetTabletListToPollForCDCResponse respAfterSplit = ybClient.getTabletListToPollForCdc(
      ybClient.openTableByUUID(
        testSubscriber.getTableId()), testSubscriber.getDbStreamId(), testSubscriber.getTableId());

    // Ideally there would be 2 tablets since the original one has split into 2
    assertEquals(2, respAfterSplit.getTabletCheckpointPairListSize());
    System.out.println("BLEH BLEH BLEH BLEH BLEH BLEH");
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
