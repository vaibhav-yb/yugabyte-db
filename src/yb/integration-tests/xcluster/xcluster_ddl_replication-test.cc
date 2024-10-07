// Copyright (c) YugabyteDB, Inc.
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

#include "yb/cdc/xcluster_types.h"
#include "yb/client/table.h"
#include "yb/client/yb_table_name.h"
#include "yb/common/colocated_util.h"
#include "yb/common/common_types.pb.h"
#include "yb/integration-tests/xcluster/xcluster_ddl_replication_test_base.h"
#include "yb/integration-tests/xcluster/xcluster_test_base.h"
#include "yb/master/catalog_manager.h"
#include "yb/master/mini_master.h"
#include "yb/util/tsan_util.h"

DECLARE_int32(cdc_parent_tablet_deletion_task_retry_secs);
DECLARE_uint32(xcluster_consistent_wal_safe_time_frequency_ms);

DECLARE_bool(TEST_xcluster_ddl_queue_handler_fail_at_end);

using namespace std::chrono_literals;

namespace yb {

const MonoDelta kTimeout = 60s * kTimeMultiplier;

class XClusterDDLReplicationTest : public XClusterDDLReplicationTestBase {};

// In automatic mode, sequences_data should have been created on both universe.
TEST_F(XClusterDDLReplicationTest, CheckSequenceDataTable) {
  ASSERT_OK(SetUpClusters());
  ASSERT_OK(CheckpointReplicationGroup());
  ASSERT_OK(CreateReplicationFromCheckpoint());

  ASSERT_OK(RunOnBothClusters([&](Cluster* cluster) -> Status {
    auto table_info = VERIFY_RESULT(cluster->mini_cluster_->GetLeaderMiniMaster())
                          ->catalog_manager_impl()
                          .GetTableInfo(kPgSequencesDataTableId);
    SCHECK_NOTNULL(table_info);
    return Status::OK();
  }));
}

TEST_F(XClusterDDLReplicationTest, CheckExtensionTableTabletCount) {
  ASSERT_OK(SetUpClusters());
  ASSERT_OK(CheckpointReplicationGroup());
  ASSERT_OK(CreateReplicationFromCheckpoint());

  // Ensure that tables are properly created with only one tablet each.
  ASSERT_OK(RunOnBothClusters([&](Cluster* cluster) -> Status {
    for (const auto& table_name :
         {xcluster::kDDLQueueTableName, xcluster::kDDLReplicatedTableName}) {
      auto yb_table_name = VERIFY_RESULT(
          GetYsqlTable(cluster, namespace_name, xcluster::kDDLQueuePgSchemaName, table_name));
      std::shared_ptr<client::YBTable> table;
      RETURN_NOT_OK(cluster->client_->OpenTable(yb_table_name, &table));
      SCHECK_EQ(table->GetPartitionCount(), 1, IllegalState, "Expected 1 tablet");
    }
    return Status::OK();
  }));
}

TEST_F(XClusterDDLReplicationTest, DisableSplitting) {
  // Ensure that splitting of xCluster DDL Replication tables is disabled on both sides.
  ASSERT_OK(SetUpClusters());

  ASSERT_OK(CheckpointReplicationGroup());
  ASSERT_OK(CreateReplicationFromCheckpoint());

  for (auto* cluster : {&producer_cluster_, &consumer_cluster_}) {
    for (const auto& table : {xcluster::kDDLQueueTableName, xcluster::kDDLReplicatedTableName}) {
      auto yb_table_name = ASSERT_RESULT(
          GetYsqlTable(cluster, namespace_name, xcluster::kDDLQueuePgSchemaName, table));

      google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
      ASSERT_OK(cluster->client_->GetTabletsFromTableId(yb_table_name.table_id(), 1, &tablets));

      auto res = CallAdmin(cluster->mini_cluster_.get(), "split_tablet", tablets[0].tablet_id());
      ASSERT_NOK(res);
      ASSERT_TRUE(res.status().message().Contains(
          "Tablet splitting is not supported for xCluster DDL Replication tables"));
    }
  }
}

TEST_F(XClusterDDLReplicationTest, DDLReplicationTablesNotColocated) {
  // Ensure that xCluster DDL Replication system tables are not colocated.

  ASSERT_OK(SetUpClusters(/* is_colocated */ true));
  // Create a colocated table so that we can run xCluster setup.
  ASSERT_OK(RunOnBothClusters([&](Cluster* cluster) -> Status {
    RETURN_NOT_OK(CreateYsqlTable(
        /*idx=*/1, /*num_tablets=*/1, cluster, /*tablegroup_name=*/{}, /*colocated=*/true));
    return Status::OK();
  }));

  ASSERT_OK(CheckpointReplicationGroup());
  ASSERT_OK(CreateReplicationFromCheckpoint());

  for (auto* cluster : {&producer_cluster_, &consumer_cluster_}) {
    for (const auto& table : {xcluster::kDDLQueueTableName, xcluster::kDDLReplicatedTableName}) {
      auto yb_table_name = ASSERT_RESULT(
          GetYsqlTable(cluster, namespace_name, xcluster::kDDLQueuePgSchemaName, table));

      google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
      ASSERT_OK(cluster->client_->GetTabletsFromTableId(yb_table_name.table_id(), 0, &tablets));

      ASSERT_EQ(tablets.size(), 1);
      ASSERT_FALSE(IsColocationParentTableId(tablets[0].table_id()));
    }
  }
}

TEST_F(XClusterDDLReplicationTest, CreateTable) {
  ASSERT_OK(SetUpClusters());
  ASSERT_OK(CheckpointReplicationGroup());
  ASSERT_OK(CreateReplicationFromCheckpoint());

  // Create a simple table.
  auto producer_table_name = ASSERT_RESULT(CreateYsqlTable(
      /*idx=*/1, /*num_tablets=*/3, &producer_cluster_));
  InsertRowsIntoProducerTableAndVerifyConsumer(producer_table_name);

  // Create a table in a new schema.
  const std::string kNewSchemaName = "new_schema";
  ASSERT_OK(RunOnBothClusters([&](Cluster* cluster) -> Status {
    auto conn = VERIFY_RESULT(cluster->Connect());
    // TODO(jhe) can remove this once create schema is replicated.
    RETURN_NOT_OK(conn.Execute("SET yb_xcluster_ddl_replication.enable_manual_ddl_replication=1"));
    RETURN_NOT_OK(conn.ExecuteFormat("CREATE SCHEMA $0", kNewSchemaName));
    return Status::OK();
  }));
  {
    auto conn = ASSERT_RESULT(producer_cluster_.Connect());
    ASSERT_OK(conn.ExecuteFormat(
        "CREATE TABLE $0.$1($2 int)", kNewSchemaName, producer_table_name.table_name(),
        kKeyColumnName));
  }
  auto producer_table_name_new_schema = ASSERT_RESULT(GetYsqlTable(
      &producer_cluster_, producer_table_name.namespace_name(), kNewSchemaName,
      producer_table_name.table_name()));
  InsertRowsIntoProducerTableAndVerifyConsumer(producer_table_name_new_schema);

  // Create a table under a new user.
  const std::string kNewUserName = "new_user";
  const std::string producer_table_name_new_user_str = producer_table_name.table_name() + "newuser";
  ASSERT_OK(RunOnBothClusters([&](Cluster* cluster) -> Status {
    auto conn = VERIFY_RESULT(cluster->Connect());
    RETURN_NOT_OK(conn.ExecuteFormat("CREATE USER $0 WITH PASSWORD '123'", kNewUserName));
    RETURN_NOT_OK(conn.Execute("SET yb_xcluster_ddl_replication.enable_manual_ddl_replication=1"));
    RETURN_NOT_OK(conn.ExecuteFormat("GRANT CREATE ON SCHEMA public TO $0", kNewUserName));
    return Status::OK();
  }));
  {
    auto conn = ASSERT_RESULT(producer_cluster_.Connect());
    ASSERT_OK(conn.ExecuteFormat("SET ROLE $0", kNewUserName));
    ASSERT_OK(conn.ExecuteFormat(
        "CREATE TABLE $0($1 int)", producer_table_name_new_user_str, kKeyColumnName));
    // Also try connecting directly as the user.
    conn = ASSERT_RESULT(producer_cluster_.ConnectToDB(namespace_name, kNewUserName));
    ASSERT_OK(conn.ExecuteFormat(
        "CREATE TABLE $0($1 int)", producer_table_name_new_user_str + "2", kKeyColumnName));
    // Ensure that we are still connected as new_user (ie no elevated permissions).
    ASSERT_EQ(ASSERT_RESULT(conn.FetchRowAsString("SELECT current_user")), kNewUserName);
  }
  auto producer_table_name_new_user = ASSERT_RESULT(GetYsqlTable(
      &producer_cluster_, producer_table_name.namespace_name(), producer_table_name.pgschema_name(),
      producer_table_name_new_user_str));
  InsertRowsIntoProducerTableAndVerifyConsumer(producer_table_name_new_user);
}

TEST_F(XClusterDDLReplicationTest, BlockMultistatementQuery) {
  ASSERT_OK(SetUpClusters());
  ASSERT_OK(CheckpointReplicationGroup());
  ASSERT_OK(CreateReplicationFromCheckpoint());

  // Have to do this through ysqlsh -c since that sends the whole
  // query string as a single command.
  auto call_multistatement_query = [&](const std::string& query) {
    std::vector<std::string> args;
    args.push_back(GetPgToolPath("ysqlsh"));
    args.push_back("--host");
    args.push_back(producer_cluster_.pg_host_port_.host());
    args.push_back("--port");
    args.push_back(AsString(producer_cluster_.pg_host_port_.port()));
    args.push_back("-c");
    args.push_back(query);

    auto s = CallAdminVec(args);
    LOG(INFO) << "Command output: " << s;
    ASSERT_NOK(s);
    ASSERT_TRUE(
        s.status().message().Contains("only a single DDL command is allowed in the query string"));
  };

  call_multistatement_query(
      "CREATE TABLE multistatement(i int PRIMARY KEY);"
      "INSERT INTO multistatement VALUES (1);");
  call_multistatement_query(
      "SELECT 1;"
      "CREATE TABLE multistatement(i int PRIMARY KEY);");
  call_multistatement_query(
      "CREATE TABLE multistatement1(i int PRIMARY KEY);"
      "CREATE TABLE multistatement2(i int PRIMARY KEY);");
  call_multistatement_query(
      "CREATE TABLE multistatement(i int);"
      "CREATE UNIQUE INDEX ON multistatement(i);");
}

TEST_F(XClusterDDLReplicationTest, CreateIndex) {
  ASSERT_OK(SetUpClusters());
  ASSERT_OK(CheckpointReplicationGroup());
  ASSERT_OK(CreateReplicationFromCheckpoint());

  const std::string kBaseTableName = "base_table";
  const std::string kColumn2Name = "a";
  const std::string kColumn3Name = "b";
  auto p_conn = ASSERT_RESULT(producer_cluster_.Connect());
  auto c_conn = ASSERT_RESULT(consumer_cluster_.Connect());

  // Create a base table.
  ASSERT_OK(p_conn.ExecuteFormat(
      "CREATE TABLE $0($1 int PRIMARY KEY, $2 int, $3 text)", kBaseTableName, kKeyColumnName,
      kColumn2Name, kColumn3Name));
  const auto producer_base_table_name = ASSERT_RESULT(
      GetYsqlTable(&producer_cluster_, namespace_name, /*schema_name*/ "", kBaseTableName));

  // Insert some rows.
  ASSERT_OK(p_conn.ExecuteFormat(
      "INSERT INTO $0 SELECT i, i%2, i::text FROM generate_series(1, 100) as i;", kBaseTableName));
  {
    ASSERT_OK(WaitForSafeTimeToAdvanceToNow());
    auto producer_table = ASSERT_RESULT(GetProducerTable(producer_base_table_name));
    auto consumer_table = ASSERT_RESULT(GetConsumerTable(producer_base_table_name));
    ASSERT_OK(VerifyWrittenRecords(producer_table, consumer_table));
  }

  // Create index on column 2.
  ASSERT_OK(p_conn.ExecuteFormat("CREATE INDEX ON $0($1 ASC)", kBaseTableName, kColumn2Name));
  const auto kCol2CountStmt =
      Format("SELECT COUNT(*) FROM $0 WHERE $1 >= 0", kBaseTableName, kColumn2Name);
  ASSERT_TRUE(ASSERT_RESULT(p_conn.HasIndexScan(kCol2CountStmt)));

  // Verify index is replicated on consumer.
  ASSERT_OK(WaitForSafeTimeToAdvanceToNow());
  {
    master::GetUniverseReplicationResponsePB resp;
    ASSERT_OK(VerifyUniverseReplication(&resp));
    EXPECT_EQ(resp.entry().tables_size(), 4);  // ddl_queue + base_table + index + sequences_data
  }
  ASSERT_TRUE(ASSERT_RESULT(c_conn.HasIndexScan(kCol2CountStmt)));

  // Create unique index on column 3.
  ASSERT_OK(p_conn.ExecuteFormat("CREATE UNIQUE INDEX ON $0($1)", kBaseTableName, kColumn3Name));
  // Test inserting duplicate value.
  ASSERT_NOK(p_conn.ExecuteFormat("INSERT INTO $0 VALUES(101, 101, '1');", kBaseTableName));
  ASSERT_OK(p_conn.ExecuteFormat("INSERT INTO $0 VALUES(0, 0, '0');", kBaseTableName));

  // Verify uniqueness constraint on consumer.
  ASSERT_OK(WaitForSafeTimeToAdvanceToNow());
  // Bypass writes being blocked on target clusters.
  ASSERT_OK(c_conn.Execute("SET yb_non_ddl_txn_for_sys_tables_allowed = true"));
  ASSERT_NOK(c_conn.ExecuteFormat("INSERT INTO $0 VALUES(1, 1, '0');", kBaseTableName));
  ASSERT_NOK(c_conn.ExecuteFormat("INSERT INTO $0 VALUES(101, 101, '1');", kBaseTableName));
  ASSERT_OK(c_conn.ExecuteFormat("INSERT INTO $0 VALUES(101, 101, '101');", kBaseTableName));
  ASSERT_OK(c_conn.Execute("SET yb_non_ddl_txn_for_sys_tables_allowed = false"));
}

TEST_F(XClusterDDLReplicationTest, ExactlyOnceReplication) {
  // Test that DDLs are only replicated exactly once.
  const int kNumTablets = 3;

  ASSERT_OK(SetUpClusters());
  ASSERT_OK(CheckpointReplicationGroup());
  ASSERT_OK(CreateReplicationFromCheckpoint());

  // Fail next DDL query and continue to process it.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_xcluster_ddl_queue_handler_fail_at_end) = true;

  // Pause replication so we can accumulate a few DDLs.
  ASSERT_OK(ToggleUniverseReplication(
      consumer_cluster(), consumer_client(), kReplicationGroupId, false /* is_enabled */));

  const int kNumTables = 3;
  std::vector<client::YBTableName> producer_table_names;
  for (int i = 0; i < kNumTables; ++i) {
    producer_table_names.push_back(ASSERT_RESULT(CreateYsqlTable(
        /*idx=*/i, kNumTablets, &producer_cluster_)));
    // Wait for apply safe time to increase.
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_xcluster_consistent_wal_safe_time_frequency_ms));
  }

  // Resume replication.
  ASSERT_OK(ToggleUniverseReplication(
      consumer_cluster(), consumer_client(), kReplicationGroupId, true /* is_enabled */));

  // Safe time should not advance.
  ASSERT_NOK(WaitForSafeTimeToAdvanceToNow());

  // Allow processing to continue.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_xcluster_ddl_queue_handler_fail_at_end) = false;
  ASSERT_OK(WaitForSafeTimeToAdvanceToNow());

  for (int i = 0; i < kNumTables; ++i) {
    InsertRowsIntoProducerTableAndVerifyConsumer(producer_table_names[i]);
  }
}

TEST_F(XClusterDDLReplicationTest, DuplicateTableNames) {
  // TODO(#23078) Can use pause resume in this test to check different cases, but that requires
  // skipping schema checks and allowing replication on hidden tables.

  // Disable the background hidden table deletion task, that way the producer table stays hidden.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_cdc_parent_tablet_deletion_task_retry_secs) = -1;

  const int kNumTablets = 3;
  ASSERT_OK(SetUpClusters());
  ASSERT_OK(CheckpointReplicationGroup());
  ASSERT_OK(CreateReplicationFromCheckpoint());

  // Create a table on the producer.
  auto producer_table_name = ASSERT_RESULT(CreateYsqlTable(
      /*idx=*/1, kNumTablets, &producer_cluster_));
  InsertRowsIntoProducerTableAndVerifyConsumer(producer_table_name);

  // Drop the table with manual replication, it should move to HIDDEN state.
  // TODO: remove manual replication restriction once we support DROPs.

  // Delete first on the producer so that the table is hidden.
  for (Cluster* cluster : {&producer_cluster_, &consumer_cluster_}) {
    auto conn = ASSERT_RESULT(cluster->Connect());
    ASSERT_OK(
        conn.ExecuteFormat("SET yb_xcluster_ddl_replication.enable_manual_ddl_replication=1"));
    ASSERT_OK(conn.ExecuteFormat("DROP TABLE $0", producer_table_name.table_name()));
  }

  // Create a new table with the same name.
  auto producer_table_name2 = ASSERT_RESULT(CreateYsqlTable(
      /*idx=*/1, kNumTablets, &producer_cluster_));
  // Ensure that replication is correctly set up on this new table.
  InsertRowsIntoProducerTableAndVerifyConsumer(producer_table_name2);
}

}  // namespace yb
