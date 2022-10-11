// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

package org.yb.cdc;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.common.CDCBaseClass;
import org.yb.cdc.util.CDCSubscriber;
import org.yb.client.GetDBStreamInfoResponse;
import org.yb.client.TestUtils;
import org.yb.client.YBClient;
import org.yb.util.YBTestRunnerNonTsanOnly;

import static org.yb.AssertionWrappers.*;

import java.util.ArrayList;
import java.util.List;

@RunWith(value = YBTestRunnerNonTsanOnly.class)
public class TestDBStreamInfo extends CDCBaseClass {
  private final static Logger LOG = LoggerFactory.getLogger(TestDBStreamInfo.class);

  @Before
  public void setUp() throws Exception {
    statement = connection.createStatement();
    statement.execute("drop table if exists test;");
    statement.execute("drop table if exists test_2;");
    statement.execute("drop table if exists test_3;");
    statement.execute("create table test (a int primary key, b int, c int);");
  }

  @Test
  public void testDBStreamInfoResponse() throws Exception {
    LOG.info("Starting testDBStreamInfoResponse");

    // Inserting a dummy row.
    int rowsAffected = statement.executeUpdate("insert into test values (1, 2, 3);");
    assertEquals(1, rowsAffected);

    CDCSubscriber testSubscriber = new CDCSubscriber(getMasterAddresses());
    testSubscriber.createStream("proto");

    GetDBStreamInfoResponse resp = testSubscriber.getDBStreamInfo();

    assertNotEquals(0, resp.getTableInfoList().size());
    assertNotEquals(0, resp.getNamespaceId().length());
  }

  @Test
  public void streamInfoShouldHaveNewTablesAfterCreation() throws Exception {
    LOG.info("Starting streamInfoShouldHaveNewTablesAfterCreation");

    // Create a stream
    CDCSubscriber testSubscriber = new CDCSubscriber(getMasterAddresses());
    testSubscriber.createStream("proto");

    String dbStreamId = testSubscriber.getDbStreamId();

    YBClient ybClient = testSubscriber.getSyncClient();

    GetDBStreamInfoResponse resp = ybClient.getDBStreamInfo(dbStreamId);

    // The response should have one table at this point in time
    assertEquals(1, resp.getTableInfoList().size());

    // Create new tables
    statement.execute("create table test_2 (a int primary key, b text);");
    statement.execute("create table test_3 (a serial primary key, b varchar(30));");

    // Wait for some time for the background thread to add these tables to cdc_state and related
    // metadata objects
    TestUtils.waitForTTL(20000);
    
    GetDBStreamInfoResponse respAfterTableCreation = ybClient.getDBStreamInfo(dbStreamId);

    // The new response will contain the newly created tables as well so the count will increase
    assertEquals(3, respAfterTableCreation.getTableInfoList().size());
  }

  @Test
  public void newTablesShouldBeAddedToMultipleStreams() throws Exception {
    LOG.info("Starting newTablesShouldBeAddedToMultipleStreams");

    int streamsToBeCreated = 5;

    List<String> streamIds = new ArrayList<>();

    // Create streams
    CDCSubscriber testSubscriber = new CDCSubscriber(getMasterAddresses());

    for (int i = 0; i < streamsToBeCreated; ++i) {
      // The way CDCSubscriber object works is that it creates one stream and stores it within a
      // string variable, so everytime we create a stream, the object will be updated with the
      // newly created stream and before creating a new stream, we can simply add the stream to
      // our list
      testSubscriber.createStream("proto");
      streamIds.add(testSubscriber.getDbStreamId());
    }

    // Get the YBClient object to perform operations
    YBClient ybClient = testSubscriber.getSyncClient();

    // Verify that all the streams have one table each
    for (int i = 0; i < streamsToBeCreated; ++i) {
      GetDBStreamInfoResponse resp = ybClient.getDBStreamInfo(streamIds.get(i));
      assertEquals(1, resp.getTableInfoList().size());
    }

    // Create new tables
    statement.execute("create table test_2 (a int primary key, b text);");
    statement.execute("create table test_3 (a serial primary key, b varchar(30));");

    // Wait for some time for the tables to get added to the cdc_state table
    TestUtils.waitForTTL(20000);

    // Verify that all the streams now contain newly added tables as well
    for (int i = 0; i < streamsToBeCreated; ++i) {
      GetDBStreamInfoResponse resp = ybClient.getDBStreamInfo(streamIds.get(i));
      assertEquals(3, resp.getTableInfoList().size());
    }
  }

  @Test
  public void tableWithoutPrimaryKeyShouldNotBeAdded() throws Exception {
    LOG.info("Starting tableWithoutPrimaryKeyShouldNotBeAdded");

    // Create a stream
    CDCSubscriber testSubscriber = new CDCSubscriber(getMasterAddresses());
    testSubscriber.createStream("proto");

    String dbStreamId = testSubscriber.getDbStreamId();

    // Get the YBClient instance for the test
    YBClient ybClient = testSubscriber.getSyncClient();

    GetDBStreamInfoResponse resp = ybClient.getDBStreamInfo(dbStreamId);

    // The response should have one table at this point in time
    assertEquals(1, resp.getTableInfoList().size());

    // Create new tables
    statement.execute("create table test_2 (a int, b text);");

    // Wait for some time for the background thread to work
    TestUtils.waitForTTL(20000);
    
    GetDBStreamInfoResponse respAfterTableCreation = ybClient.getDBStreamInfo(dbStreamId);

    // The new response should still contain a single table since the newly created table doesn't
    // contain a primary key
    assertEquals(1, respAfterTableCreation.getTableInfoList().size());
  }

  @Test
  public void verifyAllTabletsOfNewlyCreatedTableAreAddedToCdcState() throws Exception {
    // TODO Vaibhav: this test is dependent on the diff which contains the java level changes
    // for the api YBClient#getTabletListToPollForCDC()
    LOG.info("Starting verifyAllTabletsOfNewlyCreatedTableAreAddedToCdcState");

    // Create a stream
    CDCSubscriber testSubscriber = new CDCSubscriber(getMasterAddresses());
    testSubscriber.createStream("proto");

    String dbStreamId = testSubscriber.getDbStreamId();

    // Get the YBClient instance for the test
    YBClient ybClient = testSubscriber.getSyncClient();

    GetDBStreamInfoResponse resp = ybClient.getDBStreamInfo(dbStreamId);

    // The response should have one table at this point in time
    assertEquals(1, resp.getTableInfoList().size());

    // Create new table
    statement.execute("create table test_2 (a int primary key, b text) split into 10 tablets;");

    // Wait for some time for the background thread to work
    TestUtils.waitForTTL(20000);
    
    GetDBStreamInfoResponse respAfterTableCreation = ybClient.getDBStreamInfo(dbStreamId);

    // The new response should still contain the newly created table
    assertEquals(2, respAfterTableCreation.getTableInfoList().size());

    // Verify that all the tablets are there in the cdc_state table
    // ybClient.getTabletListToPollForCDC()
  } 
}
