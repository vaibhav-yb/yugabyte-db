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

#include "yb/integration-tests/xcluster/xcluster_ddl_replication_test_base.h"

#include "yb/cdc/xcluster_types.h"
#include "yb/client/table.h"
#include "yb/client/yb_table_name.h"
#include "yb/common/common_types.pb.h"
#include "yb/integration-tests/xcluster/xcluster_test_base.h"
#include "yb/integration-tests/xcluster/xcluster_ysql_test_base.h"

DECLARE_bool(enable_xcluster_api_v2);

DECLARE_bool(TEST_xcluster_ddl_queue_handler_log_queries);

using namespace std::chrono_literals;

namespace yb {

void XClusterDDLReplicationTestBase::SetUp() {
  XClusterYsqlTestBase::SetUp();
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_xcluster_api_v2) = true;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_xcluster_ddl_queue_handler_log_queries) = true;
}

Status XClusterDDLReplicationTestBase::SetUpClusters(bool is_colocated) {
  if (is_colocated) {
    namespace_name = "colocated_test_db";
  }
  const SetupParams kDefaultParams{
      // By default start with no consumer or producer tables.
      .num_consumer_tablets = {},
      .num_producer_tablets = {},
      // We only create one pg proxy per cluster, so we need to ensure that the target ddl_queue
      // table leader is on that tserver (so that setting xcluster context works properly).
      .replication_factor = 1,
      .num_masters = 1,
      .ranged_partitioned = false,
      .is_colocated = is_colocated,
  };
  return XClusterYsqlTestBase::SetUpClusters(kDefaultParams);
}

Result<std::shared_ptr<client::YBTable>> XClusterDDLReplicationTestBase::GetProducerTable(
    const client::YBTableName& producer_table_name) {
  std::shared_ptr<client::YBTable> producer_table;
  RETURN_NOT_OK(producer_client()->OpenTable(producer_table_name, &producer_table));
  return producer_table;
}

Result<std::shared_ptr<client::YBTable>> XClusterDDLReplicationTestBase::GetConsumerTable(
    const client::YBTableName& producer_table_name) {
  auto consumer_table_name = VERIFY_RESULT(GetYsqlTable(
      &consumer_cluster_, producer_table_name.namespace_name(), producer_table_name.pgschema_name(),
      producer_table_name.table_name()));
  std::shared_ptr<client::YBTable> consumer_table;
  RETURN_NOT_OK(consumer_client()->OpenTable(consumer_table_name, &consumer_table));
  return consumer_table;
}

void XClusterDDLReplicationTestBase::InsertRowsIntoProducerTableAndVerifyConsumer(
    const client::YBTableName& producer_table_name) {
  std::shared_ptr<client::YBTable> producer_table =
      ASSERT_RESULT(GetProducerTable(producer_table_name));
  ASSERT_OK(InsertRowsInProducer(0, 50, producer_table));

  // Once the safe time advances, the target should have the new table and its rows.
  ASSERT_OK(WaitForSafeTimeToAdvanceToNow());

  std::shared_ptr<client::YBTable> consumer_table =
      ASSERT_RESULT(GetConsumerTable(producer_table_name));

  // Verify that universe was setup on consumer.
  master::GetUniverseReplicationResponsePB resp;
  ASSERT_OK(VerifyUniverseReplication(&resp));
  ASSERT_EQ(resp.entry().replication_group_id(), kReplicationGroupId);
  ASSERT_TRUE(std::any_of(
      resp.entry().tables().begin(), resp.entry().tables().end(),
      [&](const std::string& table) { return table == producer_table_name.table_id(); }));

  ASSERT_OK(VerifyWrittenRecords(producer_table, consumer_table));
}

Status XClusterDDLReplicationTestBase::PrintDDLQueue(Cluster& cluster) {
  const int kMaxJsonStrLen = 500;
  auto conn = VERIFY_RESULT(cluster.ConnectToDB(namespace_name));
  const auto rows = VERIFY_RESULT((conn.FetchRows<int64_t, int64_t, std::string>(Format(
      "SELECT $0, $1, $2 FROM yb_xcluster_ddl_replication.ddl_queue ORDER BY $0 ASC",
      xcluster::kDDLQueueStartTimeColumn, xcluster::kDDLQueueQueryIdColumn,
      xcluster::kDDLQueueYbDataColumn))));

  std::stringstream ss;
  ss << "DDL Queue Table:" << std::endl;
  for (const auto& [start_time, query_id, raw_json_data] : rows) {
    ss << start_time << "\t" << query_id << "\t" << raw_json_data.substr(0, kMaxJsonStrLen)
       << std::endl;
  }
  LOG(INFO) << ss.str();

  return Status::OK();
}
}  // namespace yb
