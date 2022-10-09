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

#include <algorithm>
#include <chrono>
#include <utility>
#include <boost/assign.hpp>
#include <gtest/gtest.h>

#include "yb/cdc/cdc_service.h"
#include "yb/cdc/cdc_service.pb.h"

#include "yb/client/client-test-util.h"
#include "yb/client/client.h"
#include "yb/client/meta_cache.h"
#include "yb/client/schema.h"
#include "yb/client/session.h"
#include "yb/client/table.h"
#include "yb/client/table_alterer.h"
#include "yb/client/table_creator.h"
#include "yb/client/table_handle.h"
#include "yb/client/transaction.h"
#include "yb/master/catalog_manager_if.h"
#include "yb/tablet/transaction_participant.h"
#include "yb/client/yb_op.h"

#include "yb/common/common.pb.h"
#include "yb/common/entity_ids.h"
#include "yb/common/ql_value.h"
#include "yb/common/wire_protocol.h"

#include "yb/gutil/stl_util.h"
#include "yb/gutil/strings/join.h"
#include "yb/gutil/strings/substitute.h"

#include "yb/integration-tests/cdcsdk_test_base.h"
#include "yb/integration-tests/mini_cluster.h"

#include "yb/master/master.h"
#include "yb/master/master_admin.proxy.h"
#include "yb/master/master_client.pb.h"
#include "yb/master/master_cluster.pb.h"
#include "yb/master/master_cluster.proxy.h"
#include "yb/master/master_ddl.pb.h"
#include "yb/master/master_replication.proxy.h"
#include "yb/master/mini_master.h"

#include "yb/rpc/rpc_controller.h"

#include "yb/tablet/tablet.h"
#include "yb/tablet/tablet_peer.h"

#include "yb/tserver/cdc_consumer.h"
#include "yb/tserver/mini_tablet_server.h"
#include "yb/tserver/tablet_server.h"
#include "yb/tserver/ts_tablet_manager.h"
#include "yb/tserver/tserver_admin.proxy.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/enums.h"
#include "yb/util/monotime.h"
#include "yb/util/random_util.h"
#include "yb/util/result.h"
#include "yb/util/stol_utils.h"
#include "yb/util/test_macros.h"
#include "yb/util/thread.h"

#include "yb/yql/cql/ql/util/errcodes.h"
#include "yb/yql/cql/ql/util/statement_result.h"

#include "yb/yql/pgwrapper/libpq_utils.h"
#include "yb/yql/pgwrapper/pg_wrapper.h"

DECLARE_int64(cdc_intent_retention_ms);
DECLARE_bool(enable_update_local_peer_min_index);
DECLARE_int32(update_min_cdc_indices_interval_secs);
DECLARE_bool(stream_truncate_record);
DECLARE_int32(cdc_state_checkpoint_update_interval_ms);
DECLARE_int32(update_metrics_interval_ms);
DECLARE_uint64(log_segment_size_bytes);
DECLARE_uint64(consensus_max_batch_size_bytes);
DECLARE_uint64(aborted_intent_cleanup_ms);
DECLARE_int32(cdc_min_replicated_index_considered_stale_secs);
DECLARE_int32(log_min_seconds_to_retain);
DECLARE_bool(enable_single_record_update);
DECLARE_bool(enable_delete_truncate_cdcsdk_table);
DECLARE_bool(enable_load_balancing);
DECLARE_int32(cdc_parent_tablet_deletion_task_retry_secs);

namespace yb {

using client::YBClient;
using client::YBClientBuilder;
using client::YBColumnSchema;
using client::YBError;
using client::YBSchema;
using client::YBSchemaBuilder;
using client::YBSession;
using client::YBTable;
using client::YBTableAlterer;
using client::YBTableCreator;
using client::YBTableName;
using client::YBTableType;
using master::GetNamespaceInfoResponsePB;
using master::MiniMaster;
using tserver::MiniTabletServer;
using tserver::enterprise::CDCConsumer;

using pgwrapper::GetInt32;
using pgwrapper::PGConn;
using pgwrapper::PGResultPtr;
using pgwrapper::PgSupervisor;
using pgwrapper::ToString;

using rpc::RpcController;

namespace cdc {
namespace enterprise {

YB_DEFINE_ENUM(IntentCountCompareOption, (GreaterThanOrEqualTo)(GreaterThan)(EqualTo));
YB_DEFINE_ENUM(OpIdExpectedValue, (MaxOpId)(InvalidOpId)(ValidNonMaxOpId));

class CDCSDKYsqlTest : public CDCSDKTestBase {
 public:
  struct ExpectedRecord {
    int32_t key;
    int32_t value;
  };

  struct VaryingExpectedRecord {
    uint32_t key;
    vector<std::pair<std::string, uint32_t>> val_vec;
  };

  Result<string> GetUniverseId(Cluster* cluster) {
    yb::master::GetMasterClusterConfigRequestPB req;
    yb::master::GetMasterClusterConfigResponsePB resp;

    master::MasterClusterProxy master_proxy(
        &cluster->client_->proxy_cache(),
        VERIFY_RESULT(cluster->mini_cluster_->GetLeaderMasterBoundRpcAddr()));

    rpc::RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(kRpcTimeout));
    RETURN_NOT_OK(master_proxy.GetMasterClusterConfig(req, &resp, &rpc));
    if (resp.has_error()) {
      return STATUS(IllegalState, "Error getting cluster config");
    }
    return resp.cluster_config().cluster_uuid();
  }

  void VerifyCdcStateMatches(
      client::YBClient* client, const CDCStreamId& stream_id, const TabletId& tablet_id,
      const uint64_t term, const uint64_t index) {
    client::TableHandle table;
    client::YBTableName cdc_state_table(
        YQL_DATABASE_CQL, master::kSystemNamespaceName, master::kCdcStateTableName);
    ASSERT_OK(table.Open(cdc_state_table, client));

    const auto op = table.NewReadOp();
    auto* const req = op->mutable_request();
    QLAddStringHashValue(req, tablet_id);
    auto cond = req->mutable_where_expr()->mutable_condition();
    cond->set_op(QLOperator::QL_OP_AND);
    QLAddStringCondition(
        cond, Schema::first_column_id() + master::kCdcStreamIdIdx, QL_OP_EQUAL, stream_id);
    table.AddColumns({master::kCdcCheckpoint}, req);

    auto session = client->NewSession();
    ASSERT_OK(session->TEST_ApplyAndFlush(op));

    LOG(INFO) << strings::Substitute(
        "Verifying tablet: $0, stream: $1, op_id: $2", tablet_id, stream_id,
        OpId(term, index).ToString());

    auto row_block = ql::RowsResult(op.get()).GetRowBlock();
    ASSERT_EQ(row_block->row_count(), 1);

    string checkpoint = row_block->row(0).column(0).string_value();
    auto result = OpId::FromString(checkpoint);
    ASSERT_OK(result);
    OpId op_id = *result;

    ASSERT_EQ(op_id.term, term);
    ASSERT_EQ(op_id.index, index);
  }

  void VerifyStreamDeletedFromCdcState(
      client::YBClient* client, const CDCStreamId& stream_id, const TabletId& tablet_id,
      int timeout_secs = 120) {
    client::TableHandle table;
    const client::YBTableName cdc_state_table(
        YQL_DATABASE_CQL, master::kSystemNamespaceName, master::kCdcStateTableName);
    ASSERT_OK(table.Open(cdc_state_table, client));

    const auto op = table.NewReadOp();
    auto* const req = op->mutable_request();
    QLAddStringHashValue(req, tablet_id);

    auto cond = req->mutable_where_expr()->mutable_condition();
    cond->set_op(QLOperator::QL_OP_AND);
    QLAddStringCondition(
        cond, Schema::first_column_id() + master::kCdcStreamIdIdx, QL_OP_EQUAL, stream_id);

    table.AddColumns({master::kCdcCheckpoint}, req);
    auto session = client->NewSession();

    // The deletion of cdc_state rows for the specified stream happen in an asynchronous thread,
    // so even if the request has returned, it doesn't mean that the rows have been deleted yet.
    ASSERT_OK(WaitFor(
        [&]() {
          EXPECT_OK(session->TEST_ApplyAndFlush(op));
          auto row_block = ql::RowsResult(op.get()).GetRowBlock();
          if (row_block->row_count() == 0) {
            return true;
          }
          return false;
        },
        MonoDelta::FromSeconds(timeout_secs),
        "Failed to delete stream rows from cdc_state table."));
  }

  void VerifyStreamCheckpointInCdcState(
      client::YBClient* client, const CDCStreamId& stream_id, const TabletId& tablet_id,
      OpIdExpectedValue op_id_expected_value = OpIdExpectedValue::ValidNonMaxOpId,
      int timeout_secs = 120) {
    client::TableHandle table;
    const client::YBTableName cdc_state_table(
        YQL_DATABASE_CQL, master::kSystemNamespaceName, master::kCdcStateTableName);
    ASSERT_OK(table.Open(cdc_state_table, client));

    const auto op = table.NewReadOp();
    auto* const req = op->mutable_request();
    QLAddStringHashValue(req, tablet_id);

    auto cond = req->mutable_where_expr()->mutable_condition();
    cond->set_op(QLOperator::QL_OP_AND);
    QLAddStringCondition(
        cond, Schema::first_column_id() + master::kCdcStreamIdIdx, QL_OP_EQUAL, stream_id);

    table.AddColumns({master::kCdcCheckpoint}, req);
    auto session = client->NewSession();

    ASSERT_OK(WaitFor(
        [&]() {
          EXPECT_OK(session->TEST_ApplyAndFlush(op));
          auto row_block = ql::RowsResult(op.get()).GetRowBlock();
          auto op_id_result = OpId::FromString(row_block->row(0).column(0).string_value());
          if (!op_id_result.ok()) {
            return false;
          }
          auto op_id = *op_id_result;

          switch (op_id_expected_value) {
            case OpIdExpectedValue::MaxOpId:
              if (op_id == OpId::Max()) return true;
              break;
            case (OpIdExpectedValue::InvalidOpId):
              if (op_id == OpId::Invalid()) return true;
              break;
            case (OpIdExpectedValue::ValidNonMaxOpId):
              if (op_id.valid() && op_id != OpId::Max()) return true;
              break;
            default:
              break;
          }

          return false;
        },
        MonoDelta::FromSeconds(timeout_secs),
        "Checkpoint not OpId::Max in cdc_state table."));
  }

  void VerifyTransactionParticipant(const TabletId& tablet_id, const OpId& opid) {
    ASSERT_OK(WaitFor(
        [&]() -> Result<bool> {
          for (size_t i = 0; i < test_cluster()->num_tablet_servers(); ++i) {
            for (const auto& peer : test_cluster()->GetTabletPeers(i)) {
              if (peer->tablet_id() == tablet_id) {
                LOG(INFO) << "Tablet peer cdc_sdk_min_checkpoint_op_id: "
                          << peer->cdc_sdk_min_checkpoint_op_id();
                if (peer->cdc_sdk_min_checkpoint_op_id() == opid) {
                  return true;
                }
              }
            }
          }
          return false;
        },
        MonoDelta::FromSeconds(60),
        "The cdc_sdk_min_checkpoint_op_id doesn't match with expected op_id."));
  }

  Status DropDB(Cluster* cluster) {
    const std::string db_name = "testdatabase";
    RETURN_NOT_OK(CreateDatabase(&test_cluster_, db_name, true));
    auto conn = VERIFY_RESULT(cluster->ConnectToDB(db_name));
    RETURN_NOT_OK(conn.ExecuteFormat("DROP DATABASE $0", kNamespaceName));
    return Status::OK();
  }

  Status TruncateTable(Cluster* cluster, const std::vector<string>& table_ids) {
    RETURN_NOT_OK(cluster->client_->TruncateTables(table_ids));
    return Status::OK();
  }

  // The range is exclusive of end i.e. [start, end)
  Status WriteRows(uint32_t start, uint32_t end, Cluster* cluster) {
    auto conn = VERIFY_RESULT(cluster->ConnectToDB(kNamespaceName));
    LOG(INFO) << "Writing " << end - start << " row(s)";

    for (uint32_t i = start; i < end; ++i) {
      RETURN_NOT_OK(conn.ExecuteFormat(
          "INSERT INTO $0($1, $2) VALUES ($3, $4)", kTableName, kKeyColumnName, kValueColumnName, i,
          i + 1));
    }
    return Status::OK();
  }

  // The range is exclusive of end i.e. [start, end)
  Status WriteRows(uint32_t start, uint32_t end, Cluster* cluster, uint32_t num_cols) {
    auto conn = VERIFY_RESULT(cluster->ConnectToDB(kNamespaceName));
    LOG(INFO) << "Writing " << end - start << " row(s)";

    for (uint32_t i = start; i < end; ++i) {
      uint32_t value = i;
      std::stringstream statement_buff;
      statement_buff << "INSERT INTO $0 VALUES (";
      for (uint32_t iter = 0; iter < num_cols; ++value, ++iter) {
        statement_buff << value << ",";
      }

      std::string statement(statement_buff.str());
      statement.at(statement.size() - 1) = ')';
      RETURN_NOT_OK(conn.ExecuteFormat(statement, kTableName));
    }
    return Status::OK();
  }

  void DropTable(Cluster* cluster, const char* table_name = kTableName) {
    auto conn = EXPECT_RESULT(cluster->ConnectToDB(kNamespaceName));
    ASSERT_OK(conn.ExecuteFormat("DROP TABLE $0", table_name));
  }

  Status WriteRowsHelper(
      uint32_t start, uint32_t end, Cluster* cluster, bool flag, uint32_t num_cols) {
    auto conn = VERIFY_RESULT(cluster->ConnectToDB(kNamespaceName));
    LOG(INFO) << "Writing " << end - start << " row(s) within transaction";

    RETURN_NOT_OK(conn.Execute("BEGIN"));
    for (uint32_t i = start; i < end; ++i) {
      uint32_t value = i;
      std::stringstream statement_buff;
      statement_buff << "INSERT INTO $0 VALUES (";
      for (uint32_t iter = 0; iter < num_cols; ++value, ++iter) {
        statement_buff << value << ",";
      }

      std::string statement(statement_buff.str());
      statement.at(statement.size() - 1) = ')';
      RETURN_NOT_OK(conn.ExecuteFormat(statement, kTableName));
    }
    if (flag) {
      RETURN_NOT_OK(conn.Execute("COMMIT"));
    } else {
      RETURN_NOT_OK(conn.Execute("ABORT"));
    }
    return Status::OK();
  }

  Status WriteRowsHelper(
      uint32_t start, uint32_t end, Cluster* cluster, bool flag,
      const char* const table_name = kTableName) {
    auto conn = VERIFY_RESULT(cluster->ConnectToDB(kNamespaceName));
    LOG(INFO) << "Writing " << end - start << " row(s) within transaction";

    RETURN_NOT_OK(conn.Execute("BEGIN"));
    for (uint32_t i = start; i < end; ++i) {
      RETURN_NOT_OK(conn.ExecuteFormat(
          "INSERT INTO $0($1, $2) VALUES ($3, $4)", table_name, kKeyColumnName, kValueColumnName, i,
          i + 1));
    }
    if (flag) {
      RETURN_NOT_OK(conn.Execute("COMMIT"));
    } else {
      RETURN_NOT_OK(conn.Execute("ABORT"));
    }
    return Status::OK();
  }

  Status CreateColocatedObjects(Cluster* cluster) {
    auto conn = VERIFY_RESULT(cluster->ConnectToDB(kNamespaceName));
    RETURN_NOT_OK(conn.ExecuteFormat("CREATE TABLEGROUP tg1"));
    RETURN_NOT_OK(conn.ExecuteFormat("CREATE TABLE test1(id1 int primary key) TABLEGROUP tg1;"));
    RETURN_NOT_OK(conn.ExecuteFormat("CREATE TABLE test2(id2 text primary key) TABLEGROUP tg1;"));
    return Status::OK();
  }

  Status PopulateColocatedData(Cluster* cluster, int insert_count, bool transaction = false) {
    auto conn = VERIFY_RESULT(cluster->ConnectToDB(kNamespaceName));
    if (transaction) {
      RETURN_NOT_OK(conn.Execute("BEGIN"));
    }
    for (int i = 0; i < insert_count; ++i) {
      LOG(INFO) << "Inserting entry " << i;
      RETURN_NOT_OK(conn.ExecuteFormat("INSERT INTO test1 VALUES ($0)", i));
      RETURN_NOT_OK(conn.ExecuteFormat("INSERT INTO test2 VALUES ('$0')", i));
    }
    if (transaction) {
      RETURN_NOT_OK(conn.Execute("COMMIT"));
    }
    return Status::OK();
  }

  Status WriteEnumsRows(
      uint32_t start, uint32_t end, Cluster* cluster, const string& enum_suffix = "",
      string database_name = kNamespaceName, string table_name = kTableName,
      string schema_name = "public") {
    auto conn = VERIFY_RESULT(cluster->ConnectToDB(database_name));
    LOG(INFO) << "Writing " << end - start << " row(s) within transaction";

    RETURN_NOT_OK(conn.Execute("BEGIN"));
    for (uint32_t i = start; i < end; ++i) {
      RETURN_NOT_OK(conn.ExecuteFormat(
          "INSERT INTO $0.$1($2, $3) VALUES ($4, '$5')", schema_name, table_name + enum_suffix,
          kKeyColumnName, kValueColumnName, i,
          std::string(i % 2 ? "FIXED" : "PERCENTAGE") + enum_suffix));
    }
    RETURN_NOT_OK(conn.Execute("COMMIT"));
    return Status::OK();
  }

  Status UpdateRows(uint32_t key, uint32_t value, Cluster* cluster) {
    auto conn = VERIFY_RESULT(cluster->ConnectToDB(kNamespaceName));
    LOG(INFO) << "Updating row for key " << key << " with value " << value;
    RETURN_NOT_OK(conn.ExecuteFormat(
        "UPDATE $0 SET $1 = $2 WHERE $3 = $4", kTableName, kValueColumnName, value, kKeyColumnName,
        key));
    return Status::OK();
  }

  Status UpdateRows(
      uint32_t key, const std::map<std::string, uint32_t>& col_val_map, Cluster* cluster) {
    auto conn = VERIFY_RESULT(cluster->ConnectToDB(kNamespaceName));
    std::stringstream log_buff;
    log_buff << "Updating row for key " << key << " with";
    for (auto& col_value_pair : col_val_map) {
      log_buff << " (" << col_value_pair.first << ":" << col_value_pair.second << ")";
    }
    LOG(INFO) << log_buff.str();

    std::stringstream statement_buff;
    statement_buff << "UPDATE $0 SET ";
    for (auto col_value_pair : col_val_map) {
      statement_buff << col_value_pair.first << "=" << col_value_pair.second << ",";
    }

    std::string statement(statement_buff.str());
    statement.at(statement.size() - 1) = ' ';
    std::string where_clause("WHERE $1 = $2");
    statement += where_clause;
    RETURN_NOT_OK(conn.ExecuteFormat(statement, kTableName, "col1", key));
    return Status::OK();
  }

  Status UpdateRowsHelper(
      uint32_t start, uint32_t end, Cluster* cluster, bool flag, uint32_t key,
      const std::map<std::string, uint32_t>& col_val_map1,
      const std::map<std::string, uint32_t>& col_val_map2, uint32_t num_cols) {
    auto conn = VERIFY_RESULT(cluster->ConnectToDB(kNamespaceName));
    std::stringstream log_buff1, log_buff2;
    LOG(INFO) << "Writing " << end - start << " row(s) within transaction";

    RETURN_NOT_OK(conn.Execute("BEGIN"));

    for (uint32_t i = start; i < end; ++i) {
      uint32_t value = i;
      std::stringstream statement_buff;
      statement_buff << "INSERT INTO $0 VALUES (";
      for (uint32_t iter = 0; iter < num_cols; ++value, ++iter) {
        statement_buff << value << ",";
      }

      std::string statement(statement_buff.str());
      statement.at(statement.size() - 1) = ')';
      RETURN_NOT_OK(conn.ExecuteFormat(statement, kTableName));
    }

    log_buff1 << "Updating row for key " << key << " with";
    for (auto& col_value_pair : col_val_map1) {
      log_buff1 << " (" << col_value_pair.first << ":" << col_value_pair.second << ")";
    }
    LOG(INFO) << log_buff1.str();

    std::stringstream statement_buff1, statement_buff2;
    statement_buff1 << "UPDATE $0 SET ";
    for (auto& col_value_pair : col_val_map1) {
      statement_buff1 << col_value_pair.first << "=" << col_value_pair.second << ",";
    }

    std::string statement1(statement_buff1.str());
    statement1.at(statement1.size() - 1) = ' ';
    std::string where_clause("WHERE $1 = $2");
    statement1 += where_clause;
    RETURN_NOT_OK(conn.ExecuteFormat(statement1, kTableName, "col1", key));

    log_buff2 << "Updating row for key " << key << " with";
    for (auto& col_value_pair : col_val_map2) {
      log_buff2 << " (" << col_value_pair.first << ":" << col_value_pair.second << ")";
    }
    LOG(INFO) << log_buff2.str();

    statement_buff2 << "UPDATE $0 SET ";
    for (auto& col_value_pair : col_val_map2) {
      statement_buff2 << col_value_pair.first << "=" << col_value_pair.second << ",";
    }

    std::string statement2(statement_buff2.str());
    statement2.at(statement2.size() - 1) = ' ';
    statement2 += where_clause;
    RETURN_NOT_OK(conn.ExecuteFormat(statement2, kTableName, "col1", key));

    if (flag) {
      RETURN_NOT_OK(conn.Execute("COMMIT"));
    } else {
      RETURN_NOT_OK(conn.Execute("ABORT"));
    }
    return Status::OK();
  }

  Status UpdateDeleteRowsHelper(
      uint32_t start, uint32_t end, Cluster* cluster, bool flag, uint32_t key,
      const std::map<std::string, uint32_t>& col_val_map, uint32_t num_cols) {
    auto conn = VERIFY_RESULT(cluster->ConnectToDB(kNamespaceName));
    std::stringstream log_buff1, log_buff2;
    LOG(INFO) << "Writing " << end - start << " row(s) within transaction";

    RETURN_NOT_OK(conn.Execute("BEGIN"));

    for (uint32_t i = start; i < end; ++i) {
      uint32_t value = i;
      std::stringstream statement_buff;
      statement_buff << "INSERT INTO $0 VALUES (";
      for (uint32_t iter = 0; iter < num_cols; ++value, ++iter) {
        statement_buff << value << ",";
      }

      std::string statement(statement_buff.str());
      statement.at(statement.size() - 1) = ')';
      RETURN_NOT_OK(conn.ExecuteFormat(statement, kTableName));
    }

    log_buff1 << "Updating row for key " << key << " with";
    for (auto col_value_pair : col_val_map) {
      log_buff1 << " (" << col_value_pair.first << ":" << col_value_pair.second << ")";
    }
    LOG(INFO) << log_buff1.str();

    std::stringstream statement_buff1, statement_buff2;
    statement_buff1 << "UPDATE $0 SET ";
    for (auto col_value_pair : col_val_map) {
      statement_buff1 << col_value_pair.first << "=" << col_value_pair.second << ",";
    }

    std::string statement1(statement_buff1.str());
    statement1.at(statement1.size() - 1) = ' ';
    std::string where_clause("WHERE $1 = $2");
    statement1 += where_clause;
    RETURN_NOT_OK(conn.ExecuteFormat(statement1, kTableName, "col1", key));

    log_buff2 << "Updating row for key " << key << " with";
    for (auto& col_value_pair : col_val_map) {
      log_buff2 << " (" << col_value_pair.first << ":" << col_value_pair.second << ")";
    }
    LOG(INFO) << log_buff2.str();

    statement_buff2 << "DELETE FROM $0 ";

    std::string statement2(statement_buff2.str());
    statement2.at(statement2.size() - 1) = ' ';
    statement2 += where_clause;
    RETURN_NOT_OK(conn.ExecuteFormat(statement2, kTableName, "col1", key));

    if (flag) {
      RETURN_NOT_OK(conn.Execute("COMMIT"));
    } else {
      RETURN_NOT_OK(conn.Execute("ABORT"));
    }
    return Status::OK();
  }

  Status DeleteRows(uint32_t key, Cluster* cluster) {
    auto conn = VERIFY_RESULT(cluster->ConnectToDB(kNamespaceName));
    LOG(INFO) << "Deleting row for key " << key;
    RETURN_NOT_OK(
        conn.ExecuteFormat("DELETE FROM $0 WHERE $1 = $2", kTableName, kKeyColumnName, key));
    return Status::OK();
  }

  Status SplitTablet(const TabletId& tablet_id, Cluster* cluster) {
    yb::master::SplitTabletRequestPB req;
    req.set_tablet_id(tablet_id);
    yb::master::SplitTabletResponsePB resp;
    rpc::RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(30.0) * kTimeMultiplier);

    RETURN_NOT_OK(cluster->mini_cluster_->mini_master()->catalog_manager().SplitTablet(
        tablet_id, master::ManualSplit::kTrue));

    if (resp.has_error()) {
      RETURN_NOT_OK(StatusFromPB(resp.error().status()));
    }
    return Status::OK();
  }

  Result<google::protobuf::RepeatedPtrField<master::TabletLocationsPB>> SetUpCluster() {
    FLAGS_enable_single_record_update = false;
    RETURN_NOT_OK(SetUpWithParams(3, 1, false));
    auto table = EXPECT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName));
    google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
    RETURN_NOT_OK(test_client()->GetTablets(table, 0, &tablets, nullptr));
    return tablets;
  }

  Result<google::protobuf::RepeatedPtrField<master::TabletLocationsPB>>
  SetUpClusterMultiColumnUsecase(uint32_t num_cols) {
    FLAGS_enable_single_record_update = true;
    RETURN_NOT_OK(SetUpWithParams(3, 1, false));
    auto table = EXPECT_RESULT(CreateTable(
        &test_cluster_, kNamespaceName, kTableName, 1, true, false, 0, false, "", "public",
        num_cols));
    google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
    RETURN_NOT_OK(test_client()->GetTablets(table, 0, &tablets, nullptr));
    return tablets;
  }

  Result<GetChangesResponsePB> UpdateCheckpoint(
      const CDCStreamId& stream_id,
      const google::protobuf::RepeatedPtrField<master::TabletLocationsPB>& tablets,
      GetChangesResponsePB* change_resp) {
    GetChangesRequestPB change_req2;
    GetChangesResponsePB change_resp2;
    PrepareChangeRequest(
        &change_req2, stream_id, tablets, 0, change_resp->cdc_sdk_checkpoint().index(),
        change_resp->cdc_sdk_checkpoint().term(), change_resp->cdc_sdk_checkpoint().key(),
        change_resp->cdc_sdk_checkpoint().write_id(),
        change_resp->cdc_sdk_checkpoint().snapshot_time());
    RpcController get_changes_rpc;
    RETURN_NOT_OK(cdc_proxy_->GetChanges(change_req2, &change_resp2, &get_changes_rpc));
    if (change_resp2.has_error()) {
      return StatusFromPB(change_resp2.error().status());
    }

    return change_resp2;
  }

  std::unique_ptr<tserver::TabletServerAdminServiceProxy> GetTServerAdminProxy(
      const uint32_t tserver_index) {
    auto tserver = test_cluster()->mini_tablet_server(tserver_index);
    return std::make_unique<tserver::TabletServerAdminServiceProxy>(
        &tserver->server()->proxy_cache(), HostPort::FromBoundEndpoint(tserver->bound_rpc_addr()));
  }

  Status GetIntentCounts(const uint32_t tserver_index, int64* num_intents) {
    tserver::CountIntentsRequestPB req;
    tserver::CountIntentsResponsePB resp;
    RpcController rpc;

    auto ts_admin_service_proxy = GetTServerAdminProxy(tserver_index);
    rpc.set_timeout(MonoDelta::FromSeconds(kRpcTimeout));
    RETURN_NOT_OK(ts_admin_service_proxy->CountIntents(req, &resp, &rpc));
    *num_intents = resp.num_intents();
    return Status::OK();
  }

  void PrepareChangeRequest(
      GetChangesRequestPB* change_req, const CDCStreamId& stream_id,
      const google::protobuf::RepeatedPtrField<master::TabletLocationsPB>& tablets,
      const int tablet_idx = 0, int64 index = 0, int64 term = 0, std::string key = "",
      int32_t write_id = 0, int64 snapshot_time = 0) {
    change_req->set_stream_id(stream_id);
    change_req->set_tablet_id(tablets.Get(tablet_idx).tablet_id());
    change_req->mutable_from_cdc_sdk_checkpoint()->set_index(index);
    change_req->mutable_from_cdc_sdk_checkpoint()->set_term(term);
    change_req->mutable_from_cdc_sdk_checkpoint()->set_key(key);
    change_req->mutable_from_cdc_sdk_checkpoint()->set_write_id(write_id);
    change_req->mutable_from_cdc_sdk_checkpoint()->set_snapshot_time(snapshot_time);
  }

  void PrepareChangeRequest(
      GetChangesRequestPB* change_req, const CDCStreamId& stream_id,
      const google::protobuf::RepeatedPtrField<master::TabletLocationsPB>& tablets,
      const CDCSDKCheckpointPB& cp, const int tablet_idx = 0) {
    change_req->set_stream_id(stream_id);
    change_req->set_tablet_id(tablets.Get(tablet_idx).tablet_id());
    change_req->mutable_from_cdc_sdk_checkpoint()->set_term(cp.term());
    change_req->mutable_from_cdc_sdk_checkpoint()->set_index(cp.index());
    change_req->mutable_from_cdc_sdk_checkpoint()->set_key(cp.key());
    change_req->mutable_from_cdc_sdk_checkpoint()->set_write_id(cp.write_id());
  }

  void PrepareSetCheckpointRequest(
      SetCDCCheckpointRequestPB* set_checkpoint_req,
      const CDCStreamId stream_id,
      google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets,
      const int tablet_idx,
      const OpId& op_id,
      bool initial_checkpoint) {
    set_checkpoint_req->set_stream_id(stream_id);
    set_checkpoint_req->set_initial_checkpoint(initial_checkpoint);
    set_checkpoint_req->set_tablet_id(tablets.Get(tablet_idx).tablet_id());
    set_checkpoint_req->mutable_checkpoint()->mutable_op_id()->set_term(op_id.term);
    set_checkpoint_req->mutable_checkpoint()->mutable_op_id()->set_index(op_id.index);
  }

  Result<SetCDCCheckpointResponsePB> SetCDCCheckpoint(
      const CDCStreamId& stream_id,
      const google::protobuf::RepeatedPtrField<master::TabletLocationsPB>& tablets,
      const OpId& op_id = OpId::Min(), bool initial_checkpoint = true, const int tablet_idx = 0) {
    RpcController set_checkpoint_rpc;
    SetCDCCheckpointRequestPB set_checkpoint_req;
    SetCDCCheckpointResponsePB set_checkpoint_resp;
    auto deadline = CoarseMonoClock::now() + test_client()->default_rpc_timeout();
    set_checkpoint_rpc.set_deadline(deadline);
    PrepareSetCheckpointRequest(
        &set_checkpoint_req, stream_id, tablets, tablet_idx, op_id, initial_checkpoint);
    Status st =
        cdc_proxy_->SetCDCCheckpoint(set_checkpoint_req, &set_checkpoint_resp, &set_checkpoint_rpc);

    RETURN_NOT_OK(st);
    return set_checkpoint_resp;
  }

  Result<std::vector<OpId>> GetCDCCheckpoint(
      const CDCStreamId& stream_id,
      const google::protobuf::RepeatedPtrField<master::TabletLocationsPB>& tablets) {
    RpcController get_checkpoint_rpc;
    GetCheckpointRequestPB get_checkpoint_req;
    GetCheckpointResponsePB get_checkpoint_resp;
    auto deadline = CoarseMonoClock::now() + test_client()->default_rpc_timeout();
    get_checkpoint_rpc.set_deadline(deadline);

    std::vector<OpId> op_ids;
    for (auto tablet : tablets) {
      get_checkpoint_req.set_stream_id(stream_id);
      get_checkpoint_req.set_tablet_id(tablets.Get(0).tablet_id());
      RETURN_NOT_OK(
          cdc_proxy_->GetCheckpoint(get_checkpoint_req, &get_checkpoint_resp, &get_checkpoint_rpc));
      op_ids.push_back(OpId::FromPB(get_checkpoint_resp.checkpoint().op_id()));
    }
    return op_ids;
  }

  Result<GetTabletListToPollForCDCResponsePB> GetTabletListToPollForCDC(
      const CDCStreamId& stream_id, const TableId& table_id) {
    RpcController rpc;
    GetTabletListToPollForCDCRequestPB get_tablet_list_req;
    GetTabletListToPollForCDCResponsePB get_tablet_list_resp;
    auto deadline = CoarseMonoClock::now() + test_client()->default_rpc_timeout();
    rpc.set_deadline(deadline);

    TableInfo table_info;
    table_info.set_table_id(table_id);
    table_info.set_stream_id(stream_id);

    get_tablet_list_req.mutable_table_info()->set_table_id(table_id);
    get_tablet_list_req.mutable_table_info()->set_stream_id(stream_id);

    RETURN_NOT_OK(
        cdc_proxy_->GetTabletListToPollForCDC(get_tablet_list_req, &get_tablet_list_resp, &rpc));

    return get_tablet_list_resp;
  }

  void AssertKeyValue(const CDCSDKProtoRecordPB& record, const int32_t& key, const int32_t& value) {
    ASSERT_EQ(key, record.row_message().new_tuple(0).datum_int32());
    ASSERT_EQ(value, record.row_message().new_tuple(1).datum_int32());
  }

  void AssertKeyValues(
      const CDCSDKProtoRecordPB& record, const int32_t& key,
      const vector<std::pair<std::string, uint32_t>>& col_val_vec) {
    uint32_t iter = 1;
    ASSERT_EQ(key, record.row_message().new_tuple(0).datum_int32());
    for (auto vec_iter = col_val_vec.begin(); vec_iter != col_val_vec.end(); ++iter, ++vec_iter) {
      ASSERT_EQ(vec_iter->second, record.row_message().new_tuple(iter).datum_int32());
    }
  }

  void EnableCDCServiceInAllTserver(uint32_t num_tservers) {
    for (uint32_t i = 0; i < num_tservers; ++i) {
      const auto& tserver = test_cluster()->mini_tablet_server(i)->server();
      auto cdc_service = dynamic_cast<CDCServiceImpl*>(
          tserver->rpc_server()->TEST_service_pool("yb.cdc.CDCService")->TEST_get_service().get());
      cdc_service->SetCDCServiceEnabled();
    }
  }

  int FindTserversWithCacheHit(
      const CDCStreamId stream_id, const TabletId tablet_id, uint32_t num_tservers) {
    int count = 0;
    // check the CDC Service Cache of all the tservers.
    for (uint32_t i = 0; i < num_tservers; ++i) {
      const auto& tserver = test_cluster()->mini_tablet_server(i)->server();
      auto cdc_service = dynamic_cast<CDCServiceImpl*>(
          tserver->rpc_server()->TEST_service_pool("yb.cdc.CDCService")->TEST_get_service().get());
      auto status = cdc_service->TEST_GetTabletInfoFromCache({"" /* UUID */, stream_id, tablet_id});
      if (status.ok()) {
        count += 1;
      }
    }
    return count;
  }

  void CheckRecord(
      const CDCSDKProtoRecordPB& record, CDCSDKYsqlTest::ExpectedRecord expected_records,
      uint32_t* count) {
    // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE in that order.
    switch (record.row_message().op()) {
      case RowMessage::DDL: {
        ASSERT_EQ(record.row_message().table(), kTableName);
        count[0]++;
      } break;
      case RowMessage::INSERT: {
        AssertKeyValue(record, expected_records.key, expected_records.value);
        ASSERT_EQ(record.row_message().table(), kTableName);
        count[1]++;
      } break;
      case RowMessage::UPDATE: {
        AssertKeyValue(record, expected_records.key, expected_records.value);
        ASSERT_EQ(record.row_message().table(), kTableName);
        count[2]++;
      } break;
      case RowMessage::DELETE: {
        ASSERT_EQ(record.row_message().old_tuple(0).datum_int32(), expected_records.key);
        ASSERT_EQ(record.row_message().table(), kTableName);
        count[3]++;
      } break;
      case RowMessage::READ: {
        AssertKeyValue(record, expected_records.key, expected_records.value);
        ASSERT_EQ(record.row_message().table(), kTableName);
        count[4]++;
      } break;
      case RowMessage::TRUNCATE: {
        count[5]++;
      } break;
      default:
        ASSERT_FALSE(true);
        break;
    }
  }

  void CheckCount(const uint32_t* expected_count, uint32_t* count) {
    for (int i = 0; i < 6; i++) {
      ASSERT_EQ(expected_count[i], count[i]);
    }
  }

  void CheckRecord(
      const CDCSDKProtoRecordPB& record, CDCSDKYsqlTest::VaryingExpectedRecord expected_records,
      uint32_t* count, uint32_t num_cols) {
    // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE in that order.
    switch (record.row_message().op()) {
      case RowMessage::DDL: {
        ASSERT_EQ(record.row_message().table(), kTableName);
        count[0]++;
      } break;
      case RowMessage::INSERT: {
        AssertKeyValues(record, expected_records.key, expected_records.val_vec);
        ASSERT_EQ(record.row_message().table(), kTableName);
        count[1]++;
      } break;
      case RowMessage::UPDATE: {
        AssertKeyValues(record, expected_records.key, expected_records.val_vec);
        ASSERT_EQ(record.row_message().table(), kTableName);
        count[2]++;
      } break;
      case RowMessage::DELETE: {
        ASSERT_EQ(record.row_message().old_tuple(0).datum_int32(), expected_records.key);
        ASSERT_EQ(record.row_message().table(), kTableName);
        count[3]++;
      } break;
      case RowMessage::READ: {
        AssertKeyValues(record, expected_records.key, expected_records.val_vec);
        ASSERT_EQ(record.row_message().table(), kTableName);
        count[4]++;
      } break;
      case RowMessage::TRUNCATE: {
        count[5]++;
      } break;
      case RowMessage::BEGIN: {
        count[6]++;
      } break;
      case RowMessage::COMMIT: {
        count[7]++;
      } break;
      default:
        ASSERT_FALSE(true);
        break;
    }
  }

  Result<GetChangesResponsePB> GetChangesFromCDC(
      const CDCStreamId& stream_id,
      const google::protobuf::RepeatedPtrField<master::TabletLocationsPB>& tablets,
      const CDCSDKCheckpointPB* cp = nullptr,
      int tablet_idx = 0) {
    GetChangesRequestPB change_req;
    GetChangesResponsePB change_resp;

    if (cp == nullptr) {
      PrepareChangeRequest(&change_req, stream_id, tablets, tablet_idx);
    } else {
      PrepareChangeRequest(&change_req, stream_id, tablets, *cp, tablet_idx);
    }

    // Retry only on LeaderNotReadyToServe errors
    RETURN_NOT_OK(WaitFor(
        [&]() -> Result<bool> {
          RpcController get_changes_rpc;
          auto status = cdc_proxy_->GetChanges(change_req, &change_resp, &get_changes_rpc);

          if (status.ok() && change_resp.has_error()) {
            status = StatusFromPB(change_resp.error().status());
          }

          if (status.IsLeaderNotReadyToServe()) {
            return false;
          }

          RETURN_NOT_OK(status);
          return true;
        },
        MonoDelta::FromSeconds(kRpcTimeout),
        "GetChanges timed out waiting for Leader to get ready"));

    return change_resp;
  }

  bool DeleteCDCStream(const std::string& db_stream_id) {
    RpcController delete_rpc;
    delete_rpc.set_timeout(MonoDelta::FromMilliseconds(FLAGS_cdc_write_rpc_timeout_ms));

    DeleteCDCStreamRequestPB delete_req;
    DeleteCDCStreamResponsePB delete_resp;
    delete_req.add_stream_id(db_stream_id);

    // The following line assumes that cdc_proxy_ has been initialized in the test already
    auto result = cdc_proxy_->DeleteCDCStream(delete_req, &delete_resp, &delete_rpc);
    return result.ok() && !delete_resp.has_error();
  }

  Result<GetChangesResponsePB> GetChangesFromCDCSnapshot(
      const CDCStreamId& stream_id,
      const google::protobuf::RepeatedPtrField<master::TabletLocationsPB>& tablets) {
    GetChangesRequestPB change_req;
    GetChangesResponsePB change_resp;
    PrepareChangeRequest(&change_req, stream_id, tablets, 0, -1, -1, "", -1);
    RpcController get_changes_rpc;
    RETURN_NOT_OK(cdc_proxy_->GetChanges(change_req, &change_resp, &get_changes_rpc));

    if (change_resp.has_error()) {
      return StatusFromPB(change_resp.error().status());
    }
    return change_resp;
  }

  void TestGetChanges(
      const uint32_t replication_factor, bool add_tables_without_primary_key = false) {
    ASSERT_OK(SetUpWithParams(replication_factor, 1, false));

    auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName));

    if (add_tables_without_primary_key) {
      // Adding tables without primary keys, they should not disturb any CDC related processes.
      std::string tables_wo_pk[] = {"table_wo_pk_1", "table_wo_pk_2", "table_wo_pk_3"};
      for (const auto& table_name : tables_wo_pk) {
        auto temp = ASSERT_RESULT(
            CreateTable(&test_cluster_, kNamespaceName, table_name, 1 /* num_tablets */, false));
      }
    }

    google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
    ASSERT_OK(test_client()->GetTablets(
        table, 0, &tablets,
        /* partition_list_version =*/nullptr));
    ASSERT_EQ(tablets.size(), 1);

    std::string table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
    CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());

    auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
    ASSERT_FALSE(resp.has_error());
    ASSERT_OK(WriteRows(0 /* start */, 1 /* end */, &test_cluster_));

    const uint32_t expected_records_size = 1;
    int expected_record[] = {0 /* key */, 1 /* value */};

    SleepFor(MonoDelta::FromSeconds(5));
    GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

    uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
    uint32_t ins_count = 0;
    for (uint32_t i = 0; i < record_size; ++i) {
      if (change_resp.cdc_sdk_proto_records(i).row_message().op() == RowMessage::INSERT) {
        const CDCSDKProtoRecordPB record = change_resp.cdc_sdk_proto_records(i);
        AssertKeyValue(record, expected_record[0], expected_record[1]);
        ++ins_count;
      }
    }
    LOG(INFO) << "Got " << ins_count << " insert records";
    ASSERT_EQ(expected_records_size, ins_count);
  }

  void TestIntentGarbageCollectionFlag(
      const uint32_t num_tservers,
      const bool set_flag_to_a_smaller_value,
      const uint32_t cdc_intent_retention_ms,
      const bool extend_expiration = false) {
    if (set_flag_to_a_smaller_value) {
      FLAGS_cdc_intent_retention_ms = cdc_intent_retention_ms;
    }
    FLAGS_enable_update_local_peer_min_index = false;
    FLAGS_update_min_cdc_indices_interval_secs = 1;

    ASSERT_OK(SetUpWithParams(num_tservers, 1, false));

    auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName));

    google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
    ASSERT_OK(
        test_client()->GetTablets(table, 0, &tablets, /* partition_list_version = */ nullptr));

    TabletId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
    CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(CDCCheckpointType::IMPLICIT));
    auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
    ASSERT_FALSE(resp.has_error());

    EnableCDCServiceInAllTserver(num_tservers);

    // Call GetChanges once to set the initial value in the cdc_state table.
    GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

    // This will write one row with PK = 0.
    ASSERT_OK(WriteRows(0 /* start */, 1 /* end */, &test_cluster_));

    // Count intents here, they should be 0 here.
    for (uint32_t i = 0; i < num_tservers; ++i) {
      int64 intents_count = 0;
      ASSERT_OK(GetIntentCounts(i, &intents_count));
      ASSERT_EQ(0, intents_count);
    }

    ASSERT_OK(WriteRowsHelper(1, 2, &test_cluster_, true));
    // Sleep for 60s for the background thread to update the consumer op_id so that garbage
    // collection can happen.
    vector<int64> intent_counts(num_tservers, -1);
    ASSERT_OK(WaitFor(
        [this, &num_tservers, &set_flag_to_a_smaller_value, &extend_expiration, &intent_counts,
         &stream_id, &tablets]() -> Result<bool> {
          uint32_t i = 0;
          while (i < num_tservers) {
            if (extend_expiration) {
              // Call GetChanges once to set the initial value in the cdc_state table.
              auto result = GetChangesFromCDC(stream_id, tablets);
              if (!result.ok()) {
                return false;
              }
              yb::cdc::GetChangesResponsePB change_resp = *result;
              if (change_resp.has_error()) {
                return false;
              }
            }

            auto status = GetIntentCounts(i, &intent_counts[i]);
            if (!status.ok()) {
              continue;
            }

            if (set_flag_to_a_smaller_value && !extend_expiration) {
              if (intent_counts[i] != 0) {
                continue;
              }
            }
            i++;
          }
          return true;
        },
        MonoDelta::FromSeconds(60), "Waiting for all the tservers intent counts"));

    for (uint32_t i = 0; i < num_tservers; ++i) {
      if (set_flag_to_a_smaller_value && !extend_expiration) {
        ASSERT_EQ(intent_counts[i], 0);
      } else {
        ASSERT_GE(intent_counts[i], 0);
      }
    }

    // After time expired insert few more records
    if (set_flag_to_a_smaller_value && extend_expiration) {
      ASSERT_OK(WriteRowsHelper(10, 20, &test_cluster_, true));
      ASSERT_OK(test_client()->FlushTables(
          {table.table_id()}, /* add_indexes = */ false,
          /* timeout_secs = */ 30, /* is_compaction = */ false));

      SleepFor(MonoDelta::FromMilliseconds(100));

      change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
      uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
      uint32_t insert_count = 0;
      for (uint32_t idx = 0; idx < record_size; idx++) {
        const CDCSDKProtoRecordPB& record = change_resp.cdc_sdk_proto_records(idx);
        if (record.row_message().op() == RowMessage::INSERT) {
          insert_count += 1;
        }
      }
      ASSERT_GE(insert_count, 10);
      LOG(INFO) << "Got insert record after expiration: " << insert_count;
    }
  }

  void TestSetCDCCheckpoint(const uint32_t num_tservers, bool initial_checkpoint) {
    ASSERT_OK(SetUpWithParams(num_tservers, 1, false));
    auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName));
    google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
    ASSERT_OK(
        test_client()->GetTablets(table, 0, &tablets, /* partition_list_version = */ nullptr));

    TabletId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
    CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(CDCCheckpointType::IMPLICIT));
    auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
    ASSERT_FALSE(resp.has_error());
    auto checkpoints = ASSERT_RESULT(GetCDCCheckpoint(stream_id, tablets));
    for (auto op_id : checkpoints) {
      ASSERT_EQ(OpId(0, 0), op_id);
    }

    resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets, OpId(1, 3)));
    ASSERT_FALSE(resp.has_error());

    checkpoints = ASSERT_RESULT(GetCDCCheckpoint(stream_id, tablets));

    for (auto op_id : checkpoints) {
      ASSERT_EQ(OpId(1, 3), op_id);
    }

    resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets, OpId(1, -3)));
    ASSERT_TRUE(resp.has_error());

    resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets, OpId(-2, 1)));
    ASSERT_TRUE(resp.has_error());
  }

  Result<GetChangesResponsePB> VerifyIfDDLRecordPresent(
      const CDCStreamId& stream_id,
      const google::protobuf::RepeatedPtrField<master::TabletLocationsPB>& tablets,
      bool expect_ddl_record, bool is_first_call, const CDCSDKCheckpointPB* cp = nullptr) {
    GetChangesRequestPB req;
    GetChangesResponsePB resp;

    if (cp == nullptr) {
      PrepareChangeRequest(&req, stream_id, tablets, 0);
    } else {
      PrepareChangeRequest(&req, stream_id, tablets, *cp, 0);
    }

    // The default value for need_schema_info is false.
    if (expect_ddl_record) {
      req.set_need_schema_info(true);
    }

    RpcController get_changes_rpc;
    RETURN_NOT_OK(cdc_proxy_->GetChanges(req, &resp, &get_changes_rpc));

    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }

    auto record = resp.cdc_sdk_proto_records(0);

    // If it's the first call to GetChanges, we will get a DDL record irrespective of the
    // value of need_schema_info.
    if (is_first_call || expect_ddl_record) {
      EXPECT_EQ(record.row_message().op(), RowMessage::DDL);
    } else {
      EXPECT_NE(record.row_message().op(), RowMessage::DDL);
    }

    return resp;
  }

  void PollForIntentCount(const int64& min_expected_num_intents, const uint32_t& tserver_index,
                          const IntentCountCompareOption intentCountCompareOption,
                          int64* num_intents) {
    ASSERT_OK(WaitFor(
        [this, &num_intents, &min_expected_num_intents, &tserver_index,
         &intentCountCompareOption]() -> Result<bool> {
          auto status = GetIntentCounts(tserver_index, num_intents);
          if (!status.ok()) {
            return false;
          }

          switch (intentCountCompareOption) {
            case IntentCountCompareOption::GreaterThan:
              return (*num_intents > min_expected_num_intents);
            case IntentCountCompareOption::GreaterThanOrEqualTo:
              return (*num_intents >= min_expected_num_intents);
            case IntentCountCompareOption::EqualTo:
              return (*num_intents == min_expected_num_intents);
          }

          return false;
        },
        MonoDelta::FromSeconds(120),
        "Getting Number of intents"));
  }

  Result<GetCDCDBStreamInfoResponsePB> GetDBStreamInfo(const CDCStreamId db_stream_id) {
    GetCDCDBStreamInfoRequestPB get_req;
    GetCDCDBStreamInfoResponsePB get_resp;
    get_req.set_db_stream_id(db_stream_id);

    RpcController get_rpc;
    get_rpc.set_timeout(MonoDelta::FromMilliseconds(FLAGS_cdc_write_rpc_timeout_ms));
    RETURN_NOT_OK(cdc_proxy_->GetCDCDBStreamInfo(get_req, &get_resp, &get_rpc));
    return get_resp;
  }

  Status ChangeLeaderOfTablet(size_t new_leader_index, const TabletId tablet_id) {
    CHECK(!FLAGS_enable_load_balancing);

    string tool_path = GetToolPath("../bin", "yb-admin");
    vector<string> argv;
    argv.push_back(tool_path);
    argv.push_back("-master_addresses");
    argv.push_back(AsString(test_cluster_.mini_cluster_->mini_master(0)->bound_rpc_addr()));
    argv.push_back("leader_stepdown");
    argv.push_back(tablet_id);
    argv.push_back(
        test_cluster()->mini_tablet_server(new_leader_index)->server()->permanent_uuid());
    RETURN_NOT_OK(Subprocess::Call(argv));

    return Status::OK();
  }

  void GetTabletLeaderAndAnyFollowerIndex(
      const google::protobuf::RepeatedPtrField<master::TabletLocationsPB>& tablets,
      size_t* leader_index, size_t* follower_index) {
    for (auto replica : tablets[0].replicas()) {
      for (size_t i = 0; i < test_cluster()->num_tablet_servers(); i++) {
        if (test_cluster()->mini_tablet_server(i)->server()->permanent_uuid() ==
            replica.ts_info().permanent_uuid()) {
          if (replica.role() == PeerRole::LEADER) {
            *leader_index = i;
            LOG(INFO) << "Found leader index: " << i;
          } else if (replica.role() == PeerRole::FOLLOWER) {
            *follower_index = i;
            LOG(INFO) << "Found follower index: " << i;
          }
        }
      }
    }
  }
  void CompareExpirationTime(
      const TabletId& tablet_id, const CoarseTimePoint& prev_leader_expiry_time,
      size_t current_leader_idx, bool strictly_greater_than = false) {
    ASSERT_OK(WaitFor(
        [&]() {
          CoarseTimePoint current_expiry_time;
          while (true) {
            for (auto const& peer : test_cluster()->GetTabletPeers(current_leader_idx)) {
              if (peer->tablet_id() == tablet_id) {
                current_expiry_time = peer->cdc_sdk_min_checkpoint_op_id_expiration();
                break;
              }
            }
            if (strictly_greater_than) {
              if (current_expiry_time > prev_leader_expiry_time) {
                LOG(INFO) << "The expiration time for the current LEADER is: "
                          << current_expiry_time.time_since_epoch().count()
                          << ", and the previous LEADER expiration time should be: "
                          << prev_leader_expiry_time.time_since_epoch().count();
                return true;
              }
            } else {
              if (current_expiry_time >= prev_leader_expiry_time) {
                LOG(INFO) << "The expiration time for the current LEADER is: "
                          << current_expiry_time.time_since_epoch().count()
                          << ", and the previous LEADER expiration time should be: "
                          << prev_leader_expiry_time.time_since_epoch().count();
                return true;
              }
            }
          }
          return false;
        },
        MonoDelta::FromSeconds(60), "Waiting for active time to be updated"));
  }

  Result<int64_t> GetLastActiveTimeFromCdcStateTable(
      const CDCStreamId& stream_id, const TabletId& tablet_id, client::YBClient* client) {
    auto session = client->NewSession();
    client::TableHandle table;
    const client::YBTableName cdc_state_table(
        YQL_DATABASE_CQL, master::kSystemNamespaceName, master::kCdcStateTableName);
    RETURN_NOT_OK(table.Open(cdc_state_table, client));

    auto read_op = table.NewReadOp();
    auto* read_req = read_op->mutable_request();
    QLAddStringHashValue(read_req, tablet_id);
    auto cond = read_req->mutable_where_expr()->mutable_condition();
    cond->set_op(QLOperator::QL_OP_AND);
    QLAddStringCondition(
        cond, Schema::first_column_id() + master::kCdcStreamIdIdx, QL_OP_EQUAL, stream_id);
    table.AddColumns({master::kCdcData}, read_req);
    // TODO(async_flush): https://github.com/yugabyte/yugabyte-db/issues/12173
    RETURN_NOT_OK(session->TEST_ReadSync(read_op));

    auto row_block = ql::RowsResult(read_op.get()).GetRowBlock();
    if (row_block->row_count() != 1) {
      return STATUS(
          InvalidArgument, "Did not find a row in the cdc_state table for the tablet and stream.");
    }

    const auto& last_active_time_string =
        row_block->row(0).column(0).map_value().values().Get(0).string_value();

    auto last_active_time = VERIFY_RESULT(CheckedStoInt<int64_t>(last_active_time_string));
    return last_active_time;
  }

  void WaitUntilSplitIsSuccesful(
      const TabletId& tablet_id, const yb::client::YBTableName& table,
      const int expected_num_tablets = 2) {
    ASSERT_OK(WaitFor(
        [this, tablet_id, &table, &expected_num_tablets]() -> Result<bool> {
          auto status = SplitTablet(tablet_id, &test_cluster_);
          if (!status.ok()) {
            return false;
          }
          SleepFor(MonoDelta::FromSeconds(10));

          google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets_after_split;
          status = test_client()->GetTablets(table, 0, &tablets_after_split, nullptr);
          if (!status.ok()) {
            return false;
          }

          return (tablets_after_split.size() == expected_num_tablets);
        },
        MonoDelta::FromSeconds(120), "Tabelt Split not succesful"));
  }
};

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestBaseFunctions)) {
  // setting up a cluster with 3 RF
  ASSERT_OK(SetUpWithParams(3, 1, false /* colocated */));

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName));

  ASSERT_FALSE(table.is_cql_namespace());
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestLoadInsertionOnly)) {
  // set up an RF3 cluster
  ASSERT_OK(SetUpWithParams(3, 1, false));

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName));

  ASSERT_OK(WriteRows(0, 10, &test_cluster_));
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(GetChangesWithRF1)) {
  TestGetChanges(1 /* replication factor */);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(GetChangesWithRF3)) {
  TestGetChanges(3 /* replication factor */);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(GetChanges_TablesWithNoPKPresentInDB)) {
  TestGetChanges(3 /* replication_factor */, true /* add_tables_without_primary_key */);
}

// Insert a single row.
// Expected records: (DDL, INSERT).
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(SingleShardInsertWithAutoCommit)) {
  auto tablets = ASSERT_RESULT(SetUpCluster());
  ASSERT_EQ(tablets.size(), 1);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());

  ASSERT_OK(WriteRows(1 /* start */, 2 /* end */, &test_cluster_));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE in that order.
  const uint32_t expected_count[] = {1, 1, 0, 0, 0, 0};
  uint32_t count[] = {0, 0, 0, 0, 0, 0};

  ExpectedRecord expected_records[] = {{0, 0}, {1, 2}};

  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  for (uint32_t i = 0; i < record_size; ++i) {
    const CDCSDKProtoRecordPB record = change_resp.cdc_sdk_proto_records(i);
    CheckRecord(record, expected_records[i], count);
  }
  LOG(INFO) << "Got " << count[1] << " insert record";
  CheckCount(expected_count, count);
}

// Begin transaction, perform some operations and abort transaction.
// Expected records: 1 (DDL).
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(AbortAllWriteOperations)) {
  auto tablets = ASSERT_RESULT(SetUpCluster());
  ASSERT_EQ(tablets.size(), 1);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());
  ASSERT_OK(WriteRowsHelper(1 /* start */, 4 /* end */, &test_cluster_, false));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE in that order.
  const uint32_t expected_count[] = {1, 0, 0, 0, 0, 0};
  uint32_t count[] = {0, 0, 0, 0, 0, 0};

  ExpectedRecord expected_records[] = {{0, 0}};

  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  for (uint32_t i = 0; i < record_size; ++i) {
    const CDCSDKProtoRecordPB record = change_resp.cdc_sdk_proto_records(i);
    CheckRecord(record, expected_records[i], count);
  }
  LOG(INFO) << "Got " << count[1] << " insert record and " << count[0] << " ddl record";
  CheckCount(expected_count, count);
}

// Insert one row, update the inserted row.
// Expected records: (DDL, INSERT, UPDATE).
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(SingleShardUpdateWithAutoCommit)) {
  auto tablets = ASSERT_RESULT(SetUpCluster());
  ASSERT_EQ(tablets.size(), 1);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());

  ASSERT_OK(WriteRows(1 /* start */, 2 /* end */, &test_cluster_));
  ASSERT_OK(UpdateRows(1 /* key */, 1 /* value */, &test_cluster_));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE in that order.
  const uint32_t expected_count[] = {1, 1, 1, 0, 0, 0};
  uint32_t count[] = {0, 0, 0, 0, 0, 0};

  ExpectedRecord expected_records[] = {{0, 0}, {1, 2}, {1, 1}};

  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  for (uint32_t i = 0; i < record_size; ++i) {
    const CDCSDKProtoRecordPB record = change_resp.cdc_sdk_proto_records(i);
    CheckRecord(record, expected_records[i], count);
  }
  LOG(INFO) << "Got " << count[1] << " insert record and " << count[2] << " update record";
  CheckCount(expected_count, count);
}

// Insert one row, update the inserted row.
// Expected records: (DDL, INSERT, UPDATE).
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(SingleShardMultiColUpdateWithAutoCommit)) {
  uint32_t num_cols = 4;
  map<std::string, uint32_t> col_val_map;
  auto tablets = ASSERT_RESULT(SetUpClusterMultiColumnUsecase(num_cols));
  ASSERT_EQ(tablets.size(), 1);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());

  ASSERT_OK(WriteRows(1 /* start */, 2 /* end */, &test_cluster_, num_cols));
  col_val_map.insert({"col2", 1});
  col_val_map.insert({"col3", 1});
  ASSERT_OK(UpdateRows(1 /* key */, col_val_map, &test_cluster_));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE in that order.
  const uint32_t expected_count[] = {1, 1, 1, 0, 0, 0};
  uint32_t count[] = {0, 0, 0, 0, 0, 0};

  VaryingExpectedRecord expected_records[] = {
      {0, {{"col2", 0}, {"col3", 0}, {"col4", 0}}},
      {1, {{"col2", 2}, {"col3", 3}, {"col4", 4}}},
      {1, {{"col2", 1}, {"col3", 1}}}};

  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  for (uint32_t i = 0; i < record_size; ++i) {
    const CDCSDKProtoRecordPB record = change_resp.cdc_sdk_proto_records(i);
    CheckRecord(record, expected_records[i], count, num_cols);
  }
  LOG(INFO) << "Got " << count[1] << " insert record and " << count[2] << " update record";
  CheckCount(expected_count, count);
}

// Insert 3 rows, update 2 of them.
// Expected records: (DDL, 3 INSERT, 2 UPDATE).
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(SingleShardUpdateRows)) {
  auto tablets = ASSERT_RESULT(SetUpCluster());
  ASSERT_EQ(tablets.size(), 1);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());

  ASSERT_OK(WriteRows(1 /* start */, 4 /* end */, &test_cluster_));
  ASSERT_OK(UpdateRows(1 /* key */, 1 /* value */, &test_cluster_));
  ASSERT_OK(UpdateRows(2 /* key */, 2 /* value */, &test_cluster_));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE in that order.
  const uint32_t expected_count[] = {1, 3, 2, 0, 0, 0};
  uint32_t count[] = {0, 0, 0, 0, 0, 0};

  ExpectedRecord expected_records[] = {{0, 0}, {1, 2}, {2, 3}, {3, 4}, {1, 1}, {2, 2}};

  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  for (uint32_t i = 0; i < record_size; ++i) {
    const CDCSDKProtoRecordPB record = change_resp.cdc_sdk_proto_records(i);
    CheckRecord(record, expected_records[i], count);
  }
  LOG(INFO) << "Got " << count[1] << " insert record and " << count[2] << " update record";
  CheckCount(expected_count, count);
}

// Insert 3 rows, update 2 of them.
// Expected records: (DDL, 3 INSERT, 2 UPDATE).
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(SingleShardUpdateMultiColumn)) {
  uint32_t num_cols = 4;
  map<std::string, uint32_t> col_val_map;

  auto tablets = ASSERT_RESULT(SetUpClusterMultiColumnUsecase(num_cols));
  ASSERT_EQ(tablets.size(), 1);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());

  ASSERT_OK(WriteRows(1 /* start */, 4 /* end */, &test_cluster_, num_cols));

  col_val_map.insert(pair<std::string, uint32_t>("col2", 9));
  col_val_map.insert(pair<std::string, uint32_t>("col3", 10));
  ASSERT_OK(UpdateRows(1 /* key */, col_val_map, &test_cluster_));
  ASSERT_OK(UpdateRows(2 /* key */, col_val_map, &test_cluster_));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE in that order.
  const uint32_t expected_count[] = {1, 3, 2, 0, 0, 0};
  uint32_t count[] = {0, 0, 0, 0, 0, 0};

  VaryingExpectedRecord expected_records[] = {
      {0, {std::make_pair("col2", 0), std::make_pair("col3", 0), std::make_pair("col4", 0)}},
      {1, {std::make_pair("col2", 2), std::make_pair("col3", 3), std::make_pair("col4", 4)}},
      {2, {std::make_pair("col2", 3), std::make_pair("col3", 4), std::make_pair("col4", 5)}},
      {3, {std::make_pair("col2", 4), std::make_pair("col3", 5), std::make_pair("col4", 6)}},
      {1, {std::make_pair("col2", 9), std::make_pair("col3", 10)}},
      {2, {std::make_pair("col2", 9), std::make_pair("col3", 10)}}};

  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  for (uint32_t i = 0; i < record_size; ++i) {
    const CDCSDKProtoRecordPB record = change_resp.cdc_sdk_proto_records(i);
    CheckRecord(record, expected_records[i], count, num_cols);
  }
  LOG(INFO) << "Got " << count[1] << " insert record and " << count[2] << " update record";
  CheckCount(expected_count, count);
}

// To test upadtes corresponding to a row packed into one CDC record. This verifies the generated
// CDC record in case of subsequent updates Expected records: (DDL, 1 INSERT, 2 UPDATE).
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(MultiColumnUpdateFollowedByUpdate)) {
  uint32_t num_cols = 3;
  map<std::string, uint32_t> col_val_map1, col_val_map2;

  FLAGS_enable_single_record_update = true;
  ASSERT_OK(SetUpWithParams(3, 1, false));
  auto table = EXPECT_RESULT(CreateTable(
      &test_cluster_, kNamespaceName, kTableName, 1, true, false, 0, false, "", "public",
      num_cols));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, nullptr));
  ASSERT_EQ(tablets.size(), 1);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());

  col_val_map1.insert({"col2", 9});
  col_val_map1.insert({"col3", 10});
  col_val_map2.insert({"col2", 10});
  col_val_map2.insert({"col3", 11});

  ASSERT_OK(UpdateRowsHelper(
      1 /* start */, 2 /* end */, &test_cluster_, true, 1, col_val_map1, col_val_map2, num_cols));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false,
      /* timeout_secs = */ 30, /* is_compaction = */ false));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE, BEGIN, COMMIT in
  // that order.
  const uint32_t expected_count[] = {1, 1, 2, 0, 0, 0, 1, 1};
  uint32_t count[] = {0, 0, 0, 0, 0, 0, 0, 0};

  VaryingExpectedRecord expected_records[] = {
      {0, {{"col2", 0}, {"col3", 0}}},   {0, {{"col2", 0}, {"col3", 0}}},
      {1, {{"col2", 2}, {"col3", 3}}},   {1, {{"col2", 9}, {"col3", 10}}},
      {1, {{"col2", 10}, {"col3", 11}}}, {0, {{"col2", 0}, {"col3", 0}}}};

  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  for (uint32_t i = 0; i < record_size; ++i) {
    const CDCSDKProtoRecordPB record = change_resp.cdc_sdk_proto_records(i);
    CheckRecord(record, expected_records[i], count, num_cols);
  }
  LOG(INFO) << "Got " << count[1] << " insert record and " << count[2] << " update record";
  CheckCount(expected_count, count);
}

// To test upadtes corresponding to a row packed into one CDC record. This verifies the generated
// CDC record in case of subsequent update and delete operations on same row. Expected records:
// (DDL, 1 INSERT, 1 UPDATE, 1 DELETE).
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(MultiColumnUpdateFollowedByDelete)) {
  uint32_t num_cols = 4;
  map<std::string, uint32_t> col_val_map;

  FLAGS_enable_single_record_update = true;
  ASSERT_OK(SetUpWithParams(3, 1, false));
  auto table = EXPECT_RESULT(CreateTable(
      &test_cluster_, kNamespaceName, kTableName, 1, true, false, 0, false, "", "public",
      num_cols));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, nullptr));
  ASSERT_EQ(tablets.size(), 1);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());

  col_val_map.insert({"col2", 9});
  col_val_map.insert({"col3", 10});

  ASSERT_OK(UpdateDeleteRowsHelper(
      1 /* start */, 2 /* end */, &test_cluster_, true, 1, col_val_map, num_cols));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false,
      /* timeout_secs = */ 30, /* is_compaction = */ false));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE, BEGIN, COMMIT in
  // that order.
  const uint32_t expected_count[] = {1, 1, 1, 1, 0, 0, 1, 1};
  uint32_t count[] = {0, 0, 0, 0, 0, 0, 0, 0};

  VaryingExpectedRecord expected_records[] = {
      {0, {{"col2", 0}, {"col3", 0}, {"col4", 0}}}, {0, {{"col2", 0}, {"col3", 0}, {"col4", 0}}},
      {1, {{"col2", 2}, {"col3", 3}, {"col4", 4}}}, {1, {{"col2", 9}, {"col3", 10}}},
      {1, {{"col2", 0}, {"col3", 0}, {"col4", 0}}}, {0, {{"col2", 0}, {"col3", 0}, {"col4", 0}}}};

  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  for (uint32_t i = 0; i < record_size; ++i) {
    const CDCSDKProtoRecordPB record = change_resp.cdc_sdk_proto_records(i);
    CheckRecord(record, expected_records[i], count, num_cols);
  }
  LOG(INFO) << "Got " << count[1] << " insert record, " << count[2] << " update record, and "
            << count[3] << " delete record";
  CheckCount(expected_count, count);
}

// To test upadtes corresponding to a row packed into one CDC record. This verifies the generated
// CDC record in case of subsequent update and update operations on different columns of same row.
// Expected records: (DDL, 1 INSERT, 1 UPDATE, 1 UPDATE).
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(MultiColumnUpdateFollowedByUpdateSameRow)) {
  uint32_t num_cols = 4;
  map<std::string, uint32_t> col_val_map1, col_val_map2;

  FLAGS_enable_single_record_update = true;
  ASSERT_OK(SetUpWithParams(3, 1, false));
  auto table = EXPECT_RESULT(CreateTable(
      &test_cluster_, kNamespaceName, kTableName, 1, true, false, 0, false, "", "public",
      num_cols));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, nullptr));
  ASSERT_EQ(tablets.size(), 1);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());

  col_val_map1.insert({"col2", 9});
  col_val_map2.insert({"col3", 11});

  ASSERT_OK(UpdateRowsHelper(
      1 /* start */, 2 /* end */, &test_cluster_, true, 1, col_val_map1, col_val_map2, num_cols));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false,
      /* timeout_secs = */ 30, /* is_compaction = */ false));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE, BEGIN, COMMIT in
  // that order.
  const uint32_t expected_count[] = {1, 1, 2, 0, 0, 0, 1, 1};
  uint32_t count[] = {0, 0, 0, 0, 0, 0, 0, 0};

  VaryingExpectedRecord expected_records[] = {
      {0, {{"col2", 0}, {"col3", 0}, {"col4", 0}}},
      {0, {{"col2", 0}, {"col3", 0}, {"col4", 0}}},
      {1, {{"col2", 2}, {"col3", 3}, {"col4", 4}}},
      {1, {{"col2", 9}}},
      {1, {{"col3", 11}}},
      {0, {{"col2", 0}, {"col3", 0}, {"col4", 0}}}};

  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  for (uint32_t i = 0; i < record_size; ++i) {
    const CDCSDKProtoRecordPB record = change_resp.cdc_sdk_proto_records(i);
    CheckRecord(record, expected_records[i], count, num_cols);
  }
  LOG(INFO) << "Got " << count[1] << " insert record and " << count[2] << " update record";
  CheckCount(expected_count, count);
}

// Insert one row, delete inserted row.
// Expected records: (DDL, INSERT, DELETE).
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(SingleShardDeleteWithAutoCommit)) {
  auto tablets = ASSERT_RESULT(SetUpCluster());
  ASSERT_EQ(tablets.size(), 1);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());

  ASSERT_OK(WriteRows(1 /* start */, 2 /* end */, &test_cluster_));
  ASSERT_OK(DeleteRows(1 /* key */, &test_cluster_));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE in that order.
  const uint32_t expected_count[] = {1, 1, 0, 1, 0, 0};
  uint32_t count[] = {0, 0, 0, 0, 0, 0};

  ExpectedRecord expected_records[] = {{0, 0}, {1, 2}, {1, 0}};

  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  for (uint32_t i = 0; i < record_size; ++i) {
    const CDCSDKProtoRecordPB record = change_resp.cdc_sdk_proto_records(i);
    CheckRecord(record, expected_records[i], count);
  }
  LOG(INFO) << "Got " << count[1] << " insert record and " << count[3] << " delete record";
  CheckCount(expected_count, count);
}

// Insert 4 rows.
// Expected records: (DDL, INSERT, INSERT, INSERT, INSERT).
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(SingleShardInsert4Rows)) {
  auto tablets = ASSERT_RESULT(SetUpCluster());
  ASSERT_EQ(tablets.size(), 1);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());

  ASSERT_OK(WriteRows(1 /* start */, 5 /* end */, &test_cluster_));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE in that order.
  const uint32_t expected_count[] = {1, 4, 0, 0, 0, 0};
  uint32_t count[] = {0, 0, 0, 0, 0, 0};

  ExpectedRecord expected_records[] = {{0, 0}, {1, 2}, {2, 3}, {3, 4}, {4, 5}};

  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  for (uint32_t i = 0; i < record_size; ++i) {
    const CDCSDKProtoRecordPB record = change_resp.cdc_sdk_proto_records(i);
    CheckRecord(record, expected_records[i], count);
  }
  LOG(INFO) << "Got " << count[1] << " insert records";
  CheckCount(expected_count, count);
}

// Insert a row before snapshot. Insert a row after snapshot.
// Expected records: (DDL, READ) and (DDL, INSERT).
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(InsertBeforeAfterSnapshot)) {
  auto tablets = ASSERT_RESULT(SetUpCluster());
  ASSERT_EQ(tablets.size(), 1);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());

  ASSERT_OK(WriteRows(1 /* start */, 2 /* end */, &test_cluster_));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE in that order.
  const uint32_t expected_count[] = {2, 1, 0, 0, 1, 0};
  uint32_t count[] = {0, 0, 0, 0, 0, 0};

  ExpectedRecord expected_records_before_snapshot[] = {{0, 0}, {1, 2}};
  ExpectedRecord expected_records_after_snapshot[] = {{0, 0}, {2, 3}};

  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDCSnapshot(stream_id, tablets));
  GetChangesResponsePB change_resp_updated =
      ASSERT_RESULT(UpdateCheckpoint(stream_id, tablets, &change_resp));

  uint32_t record_size = change_resp_updated.cdc_sdk_proto_records_size();
  for (uint32_t i = 0; i < record_size; ++i) {
    const CDCSDKProtoRecordPB record = change_resp_updated.cdc_sdk_proto_records(i);
    CheckRecord(record, expected_records_before_snapshot[i], count);
  }

  ASSERT_OK(WriteRows(2 /* start */, 3 /* end */, &test_cluster_));
  GetChangesResponsePB change_resp_after_snapshot =
      ASSERT_RESULT(UpdateCheckpoint(stream_id, tablets, &change_resp_updated));
  uint32_t record_size_after_snapshot = change_resp_after_snapshot.cdc_sdk_proto_records_size();
  for (uint32_t i = 0; i < record_size_after_snapshot; ++i) {
    const CDCSDKProtoRecordPB record = change_resp_after_snapshot.cdc_sdk_proto_records(i);
    CheckRecord(record, expected_records_after_snapshot[i], count);
  }
  CheckCount(expected_count, count);
}

// Begin transaction, insert one row, commit transaction, enable snapshot
// Expected records: (DDL, READ).
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(InsertSingleRowSnapshot)) {
  ASSERT_OK(SetUpWithParams(3, 1, false));
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, nullptr));
  ASSERT_EQ(tablets.size(), 1);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());

  ASSERT_OK(WriteRowsHelper(1 /* start */, 2 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false,
      /* timeout_secs = */ 30, /* is_compaction = */ false));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE in that order.
  const uint32_t expected_count[] = {1, 0, 0, 0, 1, 0};
  uint32_t count[] = {0, 0, 0, 0, 0, 0};

  ExpectedRecord expected_records[] = {{0, 0}, {1, 2}};

  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDCSnapshot(stream_id, tablets));
  GetChangesResponsePB change_resp_updated =
      ASSERT_RESULT(UpdateCheckpoint(stream_id, tablets, &change_resp));

  uint32_t record_size = change_resp_updated.cdc_sdk_proto_records_size();
  for (uint32_t i = 0; i < record_size; ++i) {
    const CDCSDKProtoRecordPB record = change_resp_updated.cdc_sdk_proto_records(i);
    CheckRecord(record, expected_records[i], count);
  }
  LOG(INFO) << "Got " << count[4] << " read record and " << count[0] << " ddl record";
  CheckCount(expected_count, count);
}

// Begin transaction, insert one row, commit transaction, update, enable snapshot
// Expected records: (DDL, READ).
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(UpdateInsertedRowSnapshot)) {
  ASSERT_OK(SetUpWithParams(3, 1, false));
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, nullptr));
  ASSERT_EQ(tablets.size(), 1);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());

  ASSERT_OK(WriteRowsHelper(1 /* start */, 2 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false,
      /* timeout_secs = */ 30, /* is_compaction = */ false));
  ASSERT_OK(UpdateRows(1 /* key */, 1 /* value */, &test_cluster_));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE in that order.
  const uint32_t expected_count[] = {1, 0, 0, 0, 1, 0};
  uint32_t count[] = {0, 0, 0, 0, 0, 0};

  ExpectedRecord expected_records[] = {{0, 0}, {1, 1}};

  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDCSnapshot(stream_id, tablets));
  GetChangesResponsePB change_resp_updated =
      ASSERT_RESULT(UpdateCheckpoint(stream_id, tablets, &change_resp));

  uint32_t record_size = change_resp_updated.cdc_sdk_proto_records_size();
  for (uint32_t i = 0; i < record_size; ++i) {
    const CDCSDKProtoRecordPB record = change_resp_updated.cdc_sdk_proto_records(i);
    CheckRecord(record, expected_records[i], count);
  }
  LOG(INFO) << "Got " << count[4] << " read record and " << count[0] << " ddl record";
  CheckCount(expected_count, count);
}

// Begin transaction, insert one row, commit transaction, delete, enable snapshot
// Expected records: (DDL).
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(DeleteInsertedRowSnapshot)) {
  ASSERT_OK(SetUpWithParams(3, 1, false));
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, nullptr));
  ASSERT_EQ(tablets.size(), 1);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());

  ASSERT_OK(WriteRowsHelper(1 /* start */, 2 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false,
      /* timeout_secs = */ 30, /* is_compaction = */ false));
  ASSERT_OK(DeleteRows(1 /* key */, &test_cluster_));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE in that order.
  const uint32_t expected_count[] = {1, 0, 0, 0, 0, 0};
  uint32_t count[] = {0, 0, 0, 0, 0, 0};

  ExpectedRecord expected_records[] = {{0, 0}};

  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDCSnapshot(stream_id, tablets));
  GetChangesResponsePB change_resp_updated =
      ASSERT_RESULT(UpdateCheckpoint(stream_id, tablets, &change_resp));

  uint32_t record_size = change_resp_updated.cdc_sdk_proto_records_size();
  for (uint32_t i = 0; i < record_size; ++i) {
    const CDCSDKProtoRecordPB record = change_resp_updated.cdc_sdk_proto_records(i);
    CheckRecord(record, expected_records[i], count);
  }
  LOG(INFO) << "Got " << count[4] << " read record and " << count[0] << " ddl record";
  CheckCount(expected_count, count);
}

// Insert 10K rows using a thread and after a while enable snapshot.
// Expected sum of READs and INSERTs is 10K.
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(InsertBeforeDuringSnapshot)) {
  auto tablets = ASSERT_RESULT(SetUpCluster());
  ASSERT_EQ(tablets.size(), 1);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());

  // 10K records inserted using a thread.
  std::vector<std::thread> threads;
  threads.emplace_back(
      [&]() { ASSERT_OK(WriteRows(1 /* start */, 10001 /* end */, &test_cluster_)); });
  SleepFor(MonoDelta::FromMilliseconds(100));

  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDCSnapshot(stream_id, tablets));

  // Count the number of snapshot READs.
  uint32_t reads_snapshot = 0;
  bool end_snapshot = false;
  while (true) {
    GetChangesResponsePB change_resp_updated =
        ASSERT_RESULT(UpdateCheckpoint(stream_id, tablets, &change_resp));
    uint32_t record_size = change_resp_updated.cdc_sdk_proto_records_size();
    uint32_t read_count = 0;
    for (uint32_t i = 0; i < record_size; ++i) {
      const CDCSDKProtoRecordPB record = change_resp_updated.cdc_sdk_proto_records(i);
      if (record.row_message().op() == RowMessage::READ) {
        read_count++;
      } else if (record.row_message().op() == RowMessage::INSERT) {
        end_snapshot = true;
        break;
      }
    }
    if (end_snapshot) {
      break;
    }
    reads_snapshot += read_count;
    change_resp = change_resp_updated;
    if (reads_snapshot == 10000) {
      break;
    }
  }

  for (auto& t : threads) {
    t.join();
  }

  LOG(INFO) << "Insertion of records using threads has completed.";

  // Count the number of INSERTS.
  uint32_t inserts_snapshot = 0;
  while (true) {
    GetChangesResponsePB change_resp_after_snapshot =
        ASSERT_RESULT(UpdateCheckpoint(stream_id, tablets, &change_resp));
    uint32_t record_size_after_snapshot = change_resp_after_snapshot.cdc_sdk_proto_records_size();
    if (record_size_after_snapshot == 0) {
      break;
    }
    uint32_t insert_count = 0;
    for (uint32_t i = 0; i < record_size_after_snapshot; ++i) {
      const CDCSDKProtoRecordPB record = change_resp_after_snapshot.cdc_sdk_proto_records(i);
      if (record.row_message().op() == RowMessage::INSERT) {
        insert_count++;
      }
    }
    inserts_snapshot += insert_count;
    change_resp = change_resp_after_snapshot;
  }
  LOG(INFO) << "Got " << reads_snapshot + inserts_snapshot << " total (read + insert) record";
  ASSERT_EQ(reads_snapshot + inserts_snapshot, 10000);
}

// Insert 10K rows using a thread and after a while enable snapshot.
// After snapshot completes, insert 10K rows using threads.
// Expected sum of READs and INSERTs is 20K.
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(InsertBeforeDuringAfterSnapshot)) {
  auto tablets = ASSERT_RESULT(SetUpCluster());
  ASSERT_EQ(tablets.size(), 1);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());

  // 10K records inserted using a thread.
  std::vector<std::thread> threads;
  threads.emplace_back(
      [&]() { ASSERT_OK(WriteRows(1 /* start */, 10001 /* end */, &test_cluster_)); });
  SleepFor(MonoDelta::FromMilliseconds(100));

  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDCSnapshot(stream_id, tablets));

  // Count the number of snapshot READs.
  uint32_t reads_snapshot = 0;
  bool end_snapshot = false;
  while (true) {
    GetChangesResponsePB change_resp_updated =
        ASSERT_RESULT(UpdateCheckpoint(stream_id, tablets, &change_resp));
    uint32_t record_size = change_resp_updated.cdc_sdk_proto_records_size();
    uint32_t read_count = 0;
    for (uint32_t i = 0; i < record_size; ++i) {
      const CDCSDKProtoRecordPB record = change_resp_updated.cdc_sdk_proto_records(i);
      if (record.row_message().op() == RowMessage::READ) {
        read_count++;
      } else if (record.row_message().op() == RowMessage::INSERT) {
        end_snapshot = true;
        break;
      }
    }
    if (end_snapshot) {
      break;
    }
    reads_snapshot += read_count;
    change_resp = change_resp_updated;
    if (reads_snapshot == 10000) {
      break;
    }
  }

  // Two threads used to insert records after the snapshot is over.
  threads.emplace_back(
      [&]() { ASSERT_OK(WriteRows(10001 /* start */, 15001 /* end */, &test_cluster_)); });
  threads.emplace_back(
      [&]() { ASSERT_OK(WriteRows(15001 /* start */, 20001 /* end */, &test_cluster_)); });

  for (auto& t : threads) {
    t.join();
  }

  LOG(INFO) << "Insertion of records using threads has completed.";

  // Count the number of INSERTS.
  uint32_t inserts_snapshot = 0;
  while (true) {
    GetChangesResponsePB change_resp_after_snapshot =
        ASSERT_RESULT(UpdateCheckpoint(stream_id, tablets, &change_resp));
    uint32_t record_size_after_snapshot = change_resp_after_snapshot.cdc_sdk_proto_records_size();
    if (record_size_after_snapshot == 0) {
      break;
    }
    uint32_t insert_count = 0;
    for (uint32_t i = 0; i < record_size_after_snapshot; ++i) {
      const CDCSDKProtoRecordPB record = change_resp_after_snapshot.cdc_sdk_proto_records(i);
      if (record.row_message().op() == RowMessage::INSERT) {
        insert_count++;
      }
    }
    inserts_snapshot += insert_count;
    change_resp = change_resp_after_snapshot;
  }
  LOG(INFO) << "Got " << reads_snapshot + inserts_snapshot << " total (read + insert) record";
  ASSERT_EQ(reads_snapshot + inserts_snapshot, 20000);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(DropDatabase)) {
  ASSERT_OK(SetUpWithParams(3, 1, false));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  ASSERT_OK(DropDB(&test_cluster_));
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestNeedSchemaInfoFlag)) {
  ASSERT_OK(SetUpWithParams(1, 1, false));

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version = */ nullptr));

  std::string table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());

  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());
  // This will write one row with PK = 0.
  ASSERT_OK(WriteRows(0 /* start */, 1 /* end */, &test_cluster_));

  // This is the first call to GetChanges, we will get a DDL record.
  auto resp = ASSERT_RESULT(VerifyIfDDLRecordPresent(stream_id, tablets, false, true));

  // Write another row to the database with PK = 1.
  ASSERT_OK(WriteRows(1 /* start */, 2 /* end */, &test_cluster_));

  // We will not get any DDL record here since this is not the first call and the flag
  // need_schema_info is also unset.
  resp = ASSERT_RESULT(
      VerifyIfDDLRecordPresent(stream_id, tablets, false, false, &resp.cdc_sdk_checkpoint()));

  // Write another row to the database with PK = 2.
  ASSERT_OK(WriteRows(2 /* start */, 3 /* end */, &test_cluster_));

  // We will get a DDL record since we have enabled the need_schema_info flag.
  resp = ASSERT_RESULT(
      VerifyIfDDLRecordPresent(stream_id, tablets, true, false, &resp.cdc_sdk_checkpoint()));
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestEnableTruncateTable)) {
  ASSERT_OK(SetUpWithParams(1, 1, false));

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version = */ nullptr));

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());
  ASSERT_OK(WriteRows(0 /* start */, 1 /* end */, &test_cluster_));
  ASSERT_NOK(TruncateTable(&test_cluster_, {table_id}));

  FLAGS_enable_delete_truncate_cdcsdk_table = true;
  ASSERT_OK(TruncateTable(&test_cluster_, {table_id}));
}

// Insert a single row, truncate table, insert another row.
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestTruncateTable)) {
  ASSERT_OK(SetUpWithParams(1, 1, false));

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version = */ nullptr));

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(set_resp.has_error());
  ASSERT_OK(WriteRows(0 /* start */, 1 /* end */, &test_cluster_));
  FLAGS_enable_delete_truncate_cdcsdk_table = true;
  ASSERT_OK(TruncateTable(&test_cluster_, {table_id}));
  ASSERT_OK(WriteRows(1 /* start */, 2 /* end */, &test_cluster_));

  // Calling Get Changes without enabling truncate flag.
  // Expected records: (DDL, INSERT, INSERT).
  GetChangesResponsePB resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE in that order.
  const uint32_t expected_count_truncate_disable[] = {1, 2, 0, 0, 0, 0};
  uint32_t count_truncate_disable[] = {0, 0, 0, 0, 0, 0};
  ExpectedRecord expected_records_truncate_disable[] = {{0, 0}, {0, 1}, {1, 2}};
  uint32_t record_size = resp.cdc_sdk_proto_records_size();
  for (uint32_t i = 0; i < record_size; ++i) {
    const CDCSDKProtoRecordPB record = resp.cdc_sdk_proto_records(i);
    CheckRecord(record, expected_records_truncate_disable[i], count_truncate_disable);
  }
  CheckCount(expected_count_truncate_disable, count_truncate_disable);

  // Setting the flag true and calling Get Changes. This will enable streaming of truncate record.
  // Expected records: (DDL, INSERT, TRUNCATE, INSERT).
  FLAGS_stream_truncate_record = true;
  resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE in that order.
  const uint32_t expected_count_truncate_enable[] = {1, 2, 0, 0, 0, 1};
  uint32_t count_truncate_enable[] = {0, 0, 0, 0, 0, 0};
  ExpectedRecord expected_records_truncate_enable[] = {{0, 0}, {0, 1}, {0, 0}, {1, 2}};
  record_size = resp.cdc_sdk_proto_records_size();
  for (uint32_t i = 0; i < record_size; ++i) {
    const CDCSDKProtoRecordPB record = resp.cdc_sdk_proto_records(i);
    CheckRecord(record, expected_records_truncate_enable[i], count_truncate_enable);
  }
  CheckCount(expected_count_truncate_enable, count_truncate_enable);

  LOG(INFO) << "Got " << count_truncate_enable[0] << " ddl records, " << count_truncate_enable[1]
            << " insert records and " << count_truncate_enable[2] << " truncate records";
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestGarbageCollectionFlag)) {
  TestIntentGarbageCollectionFlag(1, true, 10000);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestGarbageCollectionWithSmallInterval)) {
  TestIntentGarbageCollectionFlag(3, true, 5000);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestGarbageCollectionWithLargerInterval)) {
  TestIntentGarbageCollectionFlag(3, true, 10000);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestNoGarbageCollectionBeforeInterval)) {
  TestIntentGarbageCollectionFlag(3, false, 0);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestExtendingIntentRetentionTime)) {
  TestIntentGarbageCollectionFlag(3, true, 10000, true);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestSetCDCCheckpoint)) {
  TestSetCDCCheckpoint(1, false);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestDropTableBeforeCDCStreamDelete)) {
  // Setup cluster.
  ASSERT_OK(SetUpWithParams(1, 1, false));
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version = */ nullptr));

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  DropTable(&test_cluster_, kTableName);

  // Drop table will trigger the background thread to start the stream metadata cleanup, here
  // test case wait for the metadata cleanup to finish by the background thread.
  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        while (true) {
          auto resp = GetDBStreamInfo(stream_id);
          if (resp.ok() && resp->has_error()) {
            return true;
          }
          continue;
        }
        return false;
      },
      MonoDelta::FromSeconds(60), "Waiting for stream metadata cleanup."));
  // Deleting the created DB Stream ID.
  ASSERT_EQ(DeleteCDCStream(stream_id), false);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestDropTableBeforeXClusterStreamDelete)) {
  // Setup cluster.
  ASSERT_OK(SetUpWithParams(1, 1, false));
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version = */ nullptr));

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));

  RpcController rpc;
  CreateCDCStreamRequestPB create_req;
  CreateCDCStreamResponsePB create_resp;

  create_req.set_table_id(table_id);
  create_req.set_source_type(XCLUSTER);
  ASSERT_OK(cdc_proxy_->CreateCDCStream(create_req, &create_resp, &rpc));
  // Drop table on YSQL tables deletes associated xCluster streams.
  DropTable(&test_cluster_, kTableName);

  // Wait for bg thread to cleanup entries from cdc_state.
  client::TableHandle table_handle_cdc;
  client::YBTableName cdc_state_table(
      YQL_DATABASE_CQL, master::kSystemNamespaceName, master::kCdcStateTableName);
  ASSERT_OK(table_handle_cdc.Open(cdc_state_table, test_client()));
  ASSERT_OK(WaitFor([&]() -> Result<bool> {
        for (const auto& row : client::TableRange(table_handle_cdc)) {
          auto stream_id = row.column(master::kCdcStreamIdIdx).string_value();
          if (stream_id == create_resp.stream_id()) {
            return false;
          }
        }
        return true;
      }, MonoDelta::FromSeconds(60), "Waiting for stream metadata cleanup."));

  // This should fail now as the stream is deleted.
  ASSERT_EQ(DeleteCDCStream(create_resp.stream_id()), false);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCheckPointPersistencyNodeRestart)) {
  FLAGS_enable_update_local_peer_min_index = false;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  ASSERT_OK(SetUpWithParams(3, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  std::string table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  // insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  // call get changes.
  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  uint32_t record_size = change_resp_1.cdc_sdk_proto_records_size();
  LOG(INFO) << "Total records read by get change call: " << record_size;

  ASSERT_OK(WriteRowsHelper(100 /* start */, 200 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));
  // Greater than 100 check because  we got records for BEGIN, COMMIT also.
  ASSERT_GT(record_size, 100);

  // call get changes.
  GetChangesResponsePB change_resp_2 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));
  record_size = change_resp_2.cdc_sdk_proto_records_size();
  ASSERT_GT(record_size, 100);
  LOG(INFO) << "Total records read by get change call: " << record_size;

  // Restart one of the node.
  SleepFor(MonoDelta::FromSeconds(1));
  test_cluster()->mini_tablet_server(1)->Shutdown();
  ASSERT_OK(test_cluster()->mini_tablet_server(1)->Start());
  ASSERT_OK(test_cluster()->mini_tablet_server(1)->WaitStarted());

  // Check all the tserver checkpoint info it's should be valid.
  for (size_t i = 0; i < test_cluster()->num_tablet_servers(); ++i) {
    for (const auto& peer : test_cluster()->GetTabletPeers(i)) {
      if (peer->tablet_id() == tablets[0].tablet_id()) {
        // What ever checkpoint persisted in the RAFT logs should be same as what ever in memory
        // transaction participant tablet peer.
        ASSERT_EQ(
            peer->cdc_sdk_min_checkpoint_op_id(),
            peer->tablet()->transaction_participant()->GetRetainOpId());
      }
    }
  }
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCleanupSingleStreamSingleTserver)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  ASSERT_OK(SetUpWithParams(1, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());
  EnableCDCServiceInAllTserver(1);

  // Insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));
  ASSERT_EQ(DeleteCDCStream(stream_id), true);
  VerifyStreamDeletedFromCdcState(test_client(), stream_id, tablets.Get(0).tablet_id());
  VerifyTransactionParticipant(tablets.Get(0).tablet_id(), OpId::Max());
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCleanupSingleStreamMultiTserver)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  ASSERT_OK(SetUpWithParams(3, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());
  EnableCDCServiceInAllTserver(3);

  // insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));
  ASSERT_EQ(DeleteCDCStream(stream_id), true);
  VerifyStreamDeletedFromCdcState(test_client(), stream_id, tablets.Get(0).tablet_id());
  VerifyTransactionParticipant(tablets.Get(0).tablet_id(), OpId::Max());
}

TEST_F(
    CDCSDKYsqlTest,
    YB_DISABLE_TEST_IN_TSAN(TestCleanupMultiStreamDeleteSingleStreamSingleTserver)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  ASSERT_OK(SetUpWithParams(1, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id_1 = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  CDCStreamId stream_id_2 = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp_1 = ASSERT_RESULT(SetCDCCheckpoint(stream_id_1, tablets));
  ASSERT_FALSE(resp_1.has_error());
  auto resp_2 = ASSERT_RESULT(SetCDCCheckpoint(stream_id_2, tablets));
  ASSERT_FALSE(resp_2.has_error());
  EnableCDCServiceInAllTserver(1);

  // insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));
  ASSERT_EQ(DeleteCDCStream(stream_id_1), true);
  VerifyStreamDeletedFromCdcState(test_client(), stream_id_1, tablets.Get(0).tablet_id());
  VerifyCdcStateMatches(test_client(), stream_id_2, tablets.Get(0).tablet_id(), 0, 0);
  VerifyTransactionParticipant(tablets.Get(0).tablet_id(), OpId(0, 0));
}

TEST_F(
    CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCleanupMultiStreamDeleteSingleStreamMultiTserver)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  ASSERT_OK(SetUpWithParams(3, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id_1 = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  CDCStreamId stream_id_2 = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp_1 = ASSERT_RESULT(SetCDCCheckpoint(stream_id_1, tablets));
  ASSERT_FALSE(resp_1.has_error());
  auto resp_2 = ASSERT_RESULT(SetCDCCheckpoint(stream_id_2, tablets));
  ASSERT_FALSE(resp_2.has_error());
  EnableCDCServiceInAllTserver(3);

  // Insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));
  ASSERT_EQ(DeleteCDCStream(stream_id_1), true);
  VerifyStreamDeletedFromCdcState(test_client(), stream_id_1, tablets.Get(0).tablet_id());
  VerifyCdcStateMatches(test_client(), stream_id_2, tablets.Get(0).tablet_id(), 0, 0);
  VerifyTransactionParticipant(tablets.Get(0).tablet_id(), OpId(0, 0));
}

TEST_F(
    CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCleanupMultiStreamDeleteAllStreamsSingleTserver)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  ASSERT_OK(SetUpWithParams(1, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id_1 = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  CDCStreamId stream_id_2 = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp_1 = ASSERT_RESULT(SetCDCCheckpoint(stream_id_1, tablets));
  ASSERT_FALSE(resp_1.has_error());
  auto resp_2 = ASSERT_RESULT(SetCDCCheckpoint(stream_id_2, tablets));
  ASSERT_FALSE(resp_2.has_error());
  EnableCDCServiceInAllTserver(1);

  // insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));
  ASSERT_EQ(DeleteCDCStream(stream_id_1), true);
  VerifyStreamDeletedFromCdcState(test_client(), stream_id_1, tablets.Get(0).tablet_id());
  VerifyTransactionParticipant(tablets.Get(0).tablet_id(), OpId(0, 0));
  ASSERT_EQ(DeleteCDCStream(stream_id_2), true);
  VerifyStreamDeletedFromCdcState(test_client(), stream_id_2, tablets.Get(0).tablet_id());
  VerifyTransactionParticipant(tablets.Get(0).tablet_id(), OpId::Max());
}

TEST_F(
    CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCleanupMultiStreamDeleteAllStreamsMultiTserver)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  ASSERT_OK(SetUpWithParams(3, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id_1 = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  CDCStreamId stream_id_2 = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp_1 = ASSERT_RESULT(SetCDCCheckpoint(stream_id_1, tablets));
  ASSERT_FALSE(resp_1.has_error());
  auto resp_2 = ASSERT_RESULT(SetCDCCheckpoint(stream_id_2, tablets));
  ASSERT_FALSE(resp_2.has_error());
  EnableCDCServiceInAllTserver(3);

  // insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));
  ASSERT_EQ(DeleteCDCStream(stream_id_1), true);
  VerifyStreamDeletedFromCdcState(test_client(), stream_id_1, tablets.Get(0).tablet_id());
  VerifyTransactionParticipant(tablets.Get(0).tablet_id(), OpId(0, 0));
  ASSERT_EQ(DeleteCDCStream(stream_id_2), true);
  VerifyStreamDeletedFromCdcState(test_client(), stream_id_2, tablets.Get(0).tablet_id());
  VerifyTransactionParticipant(tablets.Get(0).tablet_id(), OpId::Max());
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestMultpleStreamOnSameTablet)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_intent_retention_ms = 10000;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  ASSERT_OK(SetUpWithParams(1, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));

  vector<CDCStreamId> stream_id;
  // Create 2 streams
  for (uint32_t idx = 0; idx < 2; idx++) {
    stream_id.push_back(ASSERT_RESULT(CreateDBStream(IMPLICIT)));
    auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id[idx], tablets));
    ASSERT_FALSE(resp.has_error());
  }

  // Insert some records in transaction.
  vector<GetChangesResponsePB> change_resp_01(2);
  vector<GetChangesResponsePB> change_resp_02(2);
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  for (uint32_t stream_idx = 0; stream_idx < 2; stream_idx++) {
    uint32_t record_size = 0;
    change_resp_01[stream_idx] = ASSERT_RESULT(GetChangesFromCDC(stream_id[stream_idx], tablets));
    record_size = change_resp_01[stream_idx].cdc_sdk_proto_records_size();
    LOG(INFO) << "Total records read by get change call on stream_id_" << stream_idx
              << " total records: " << record_size;
  }

  // Keep inserting some records into the table and call GetChange on stream_id_02
  // to see the inserted record count.
  uint32_t idx = 0;
  const uint32_t loop_count = 10;
  GetChangesResponsePB change_resp_2_stream_id_02;
  while (idx < loop_count) {
    change_resp_02[1] = ASSERT_RESULT(UpdateCheckpoint(stream_id[1], tablets, &change_resp_01[1]));
    idx += 1;
    change_resp_01[0] = change_resp_02[1];
    SleepFor(MonoDelta::FromMilliseconds(200));
  }

  // Now call GetChanges for stream_01.
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_cdc_intent_retention_ms));
  auto result = GetChangesFromCDC(stream_id[0], tablets, &change_resp_01[0].cdc_sdk_checkpoint());
  ASSERT_EQ(!result.ok(), true);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestMultpleActiveStreamOnSameTablet)) {
  FLAGS_enable_update_local_peer_min_index = false;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  ASSERT_OK(SetUpWithParams(3, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));

  // Create 2 streams
  vector<CDCStreamId> stream_id(2);
  for (uint32_t idx = 0; idx < 2; idx++) {
    stream_id[idx] = ASSERT_RESULT(CreateDBStream(IMPLICIT));
    auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id[idx], tablets));
    ASSERT_FALSE(resp.has_error());
  }
  // GetChanges for the stream-1 and stream-2
  vector<GetChangesResponsePB> change_resp_01(2);
  vector<GetChangesResponsePB> change_resp_02(2);
  uint32_t start = 0;
  uint32_t end = 100;
  for (uint32_t insert_idx = 0; insert_idx < 3; insert_idx++) {
    ASSERT_OK(WriteRowsHelper(start /* start */, end /* end */, &test_cluster_, true));
    ASSERT_OK(test_client()->FlushTables(
        {table.table_id()}, /* add_indexes = */
        false,              /* timeout_secs = */
        30, /* is_compaction = */ false));
    for (uint32_t stream_idx = 0; stream_idx < 2; stream_idx++) {
      uint32_t record_size = 0;
      if (insert_idx == 0) {
        change_resp_01[stream_idx] =
            ASSERT_RESULT(GetChangesFromCDC(stream_id[stream_idx], tablets));
        record_size = change_resp_01[stream_idx].cdc_sdk_proto_records_size();
      } else {
        change_resp_02[stream_idx] = ASSERT_RESULT(
            UpdateCheckpoint(stream_id[stream_idx], tablets, &change_resp_01[stream_idx]));
        change_resp_01[stream_idx] = change_resp_02[stream_idx];
        record_size = change_resp_02[stream_idx].cdc_sdk_proto_records_size();
      }
      ASSERT_GE(record_size, 100);
    }
    start = end;
    end = start + 100;
  }

  OpId min_checkpoint = OpId::Max();
  client::TableHandle table_handle_cdc;
  client::YBTableName cdc_state_table(
      YQL_DATABASE_CQL, master::kSystemNamespaceName, master::kCdcStateTableName);
  ASSERT_OK(table_handle_cdc.Open(cdc_state_table, test_client()));
  for (const auto& row : client::TableRange(table_handle_cdc)) {
    auto tablet_id = row.column(master::kCdcTabletIdIdx).string_value();
    auto stream_id = row.column(master::kCdcStreamIdIdx).string_value();
    auto checkpoint = row.column(master::kCdcCheckpointIdx).string_value();
    LOG(INFO) << "Read cdc_state table with tablet_id: " << tablet_id << " stream_id: " << stream_id
              << " checkpoint is: " << checkpoint;
    auto result = OpId::FromString(checkpoint);
    ASSERT_OK(result);
    OpId row_checkpoint = *result;
    min_checkpoint = min(min_checkpoint, row_checkpoint);
  }

  ASSERT_OK(WaitFor(
      [&]() {
        // Read the tablet LEADER as well as FOLLOWER's transaction_participation
        // Check all the tserver checkpoint info it's should be valid.
        uint32_t i = 0;
        while (i < test_cluster()->num_tablet_servers()) {
          for (const auto& peer : test_cluster()->GetTabletPeers(i)) {
            if (peer->tablet_id() == tablets[0].tablet_id()) {
              if (peer->tablet()->transaction_participant()->GetRetainOpId() != min_checkpoint) {
                SleepFor(MonoDelta::FromMilliseconds(2));
              } else {
                i += 1;
                LOG(INFO) << "In tserver: " << i
                          << " tablet peer have transaction_participant op_id set as: "
                          << peer->tablet()->transaction_participant()->GetRetainOpId();
              }
              break;
            }
          }
        }
        return true;
      },
      MonoDelta::FromSeconds(60), "Waiting for all the tservers intent counts"));
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestActiveAndInActiveStreamOnSameTablet)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  FLAGS_cdc_intent_retention_ms = 5000;
  ASSERT_OK(SetUpWithParams(3, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));

  vector<CDCStreamId> stream_id;
  // Create 2 streams
  for (uint32_t idx = 0; idx < 2; idx++) {
    stream_id.push_back(ASSERT_RESULT(CreateDBStream(IMPLICIT)));
    auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id[idx], tablets));
    ASSERT_FALSE(resp.has_error());
  }
  // Insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  vector<GetChangesResponsePB> change_resp(2);
  // Call GetChanges for the stream-1 and stream-2
  for (uint32_t idx = 0; idx < 2; idx++) {
    change_resp[idx] = ASSERT_RESULT(GetChangesFromCDC(stream_id[idx], tablets));
    uint32_t record_size = change_resp[idx].cdc_sdk_proto_records_size();
    ASSERT_GE(record_size, 100);
    LOG(INFO) << "Total records read by GetChanges call on stream_id_1: " << record_size;
  }

  // Keep stream-1 active.
  uint32_t idx = 0;
  const uint32_t total_count = 10;
  while (idx < total_count) {
    uint32_t record_size = 0;
    ASSERT_OK(WriteRowsHelper(100 + idx /* start */, 101 + idx /* end */, &test_cluster_, true));
    ASSERT_OK(test_client()->FlushTables(
        {table.table_id()}, /* add_indexes = */
        false,              /* timeout_secs = */
        30, /* is_compaction = */ false));
    GetChangesResponsePB latest_change_resp =
        ASSERT_RESULT(UpdateCheckpoint(stream_id[0], tablets, &change_resp[0]));
    record_size = latest_change_resp.cdc_sdk_proto_records_size();
    change_resp[0] = latest_change_resp;
    ASSERT_GE(record_size, 1);
    idx += 1;
  }

  OpId overall_min_checkpoint = OpId::Max();
  OpId active_stream_checkpoint;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 100000;
  client::TableHandle table_handle_cdc;
  client::YBTableName cdc_state_table(
      YQL_DATABASE_CQL, master::kSystemNamespaceName, master::kCdcStateTableName);
  ASSERT_OK(table_handle_cdc.Open(cdc_state_table, test_client()));
  for (const auto& row : client::TableRange(table_handle_cdc)) {
    auto read_tablet_id = row.column(master::kCdcTabletIdIdx).string_value();
    auto read_stream_id = row.column(master::kCdcStreamIdIdx).string_value();
    auto read_checkpoint = row.column(master::kCdcCheckpointIdx).string_value();
    GetChangesResponsePB latest_change_resp =
        ASSERT_RESULT(UpdateCheckpoint(stream_id[0], tablets, &change_resp[0]));
    auto result = OpId::FromString(read_checkpoint);
    ASSERT_OK(result);
    if (read_tablet_id == tablets[0].tablet_id() && stream_id[0] == read_stream_id) {
      LOG(INFO) << "Read cdc_state table with tablet_id: " << read_tablet_id
                << " stream_id: " << read_stream_id << " checkpoint is: " << read_checkpoint;
      active_stream_checkpoint = *result;
    } else {
      overall_min_checkpoint = min(overall_min_checkpoint, *result);
    }
  }

  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        // Read the tablet LEADER as well as FOLLOWER's transaction_participation
        // Check all the tserver checkpoint info it's should be valid.
        uint32_t i = 0;
        while (i < test_cluster()->num_tablet_servers()) {
          for (const auto& peer : test_cluster()->GetTabletPeers(i)) {
            if (peer->tablet_id() == tablets[0].tablet_id()) {
              if (peer->tablet()->transaction_participant()->GetRetainOpId() !=
                      overall_min_checkpoint &&
                  peer->tablet()->transaction_participant()->GetRetainOpId() !=
                      active_stream_checkpoint) {
                SleepFor(MonoDelta::FromMilliseconds(2));
              } else {
                i += 1;
                LOG(INFO) << "In tserver: " << i
                          << " tablet peer have transaction_participant op_id set as: "
                          << peer->tablet()->transaction_participant()->GetRetainOpId();
              }
              break;
            }
          }
        }
        return true;
      },
      MonoDelta::FromSeconds(60), "Waiting for all the tservers intent counts"));
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCheckPointPersistencyAllNodesRestart)) {
  FLAGS_enable_load_balancing = false;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  FLAGS_update_metrics_interval_ms = 1;
  ASSERT_OK(SetUpWithParams(3, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  // Insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  // Call get changes.
  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  uint32_t record_size = change_resp_1.cdc_sdk_proto_records_size();
  LOG(INFO) << "Total records read by GetChanges call: " << record_size;
  // Greater than 100 check because  we got records for BEGIN, COMMIT also.
  ASSERT_GT(record_size, 100);

  ASSERT_OK(WriteRowsHelper(100 /* start */, 200 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  // Call get changes.
  GetChangesResponsePB change_resp_2 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));
  record_size = change_resp_2.cdc_sdk_proto_records_size();
  ASSERT_GT(record_size, 100);
  LOG(INFO) << "Total records read by second GetChanges call: " << record_size;

  auto checkpoints = ASSERT_RESULT(GetCDCCheckpoint(stream_id, tablets));
  LOG(INFO) << "Checkpoint after final GetChanges: " << checkpoints[0];

  // Restart all the nodes.
  SleepFor(MonoDelta::FromSeconds(1));
  for (size_t i = 0; i < test_cluster()->num_tablet_servers(); ++i) {
    test_cluster()->mini_tablet_server(i)->Shutdown();
    ASSERT_OK(test_cluster()->mini_tablet_server(i)->Start());
    ASSERT_OK(test_cluster()->mini_tablet_server(i)->WaitStarted());
  }
  LOG(INFO) << "All nodes restarted";
  EnableCDCServiceInAllTserver(3);

  // Check the checkpoint info for all tservers - it should be valid.
  for (size_t i = 0; i < test_cluster()->num_tablet_servers(); ++i) {
    for (const auto& peer : test_cluster()->GetTabletPeers(i)) {
      if (peer->tablet_id() == tablets[0].tablet_id()) {
        ASSERT_OK(WaitFor(
            [&]() -> Result<bool> {
              // Checkpoint persisted in the RAFT logs should be same as in memory transaction
              // participant tablet peer.
              if (peer->cdc_sdk_min_checkpoint_op_id() !=
                      peer->tablet()->transaction_participant()->GetRetainOpId() ||
                  checkpoints[0] != peer->cdc_sdk_min_checkpoint_op_id()) {
                return false;
              }
              return true;
            },
            MonoDelta::FromSeconds(60),
            "Checkpoints are not as expected"));
      }
    }
  }
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestIntentCountPersistencyAllNodesRestart)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  // We want to force every GetChanges to update the cdc_state table.
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  ASSERT_OK(SetUpWithParams(1, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  // Insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));
  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  LOG(INFO) << "Number of records after first transaction: " << change_resp_1.records().size();
  change_resp_1 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));

  ASSERT_OK(WriteRowsHelper(100 /* start */, 200 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  ASSERT_OK(WriteRowsHelper(200 /* start */, 300 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));
  SleepFor(MonoDelta::FromSeconds(10));

  int64 initial_num_intents;
  PollForIntentCount(1, 0, IntentCountCompareOption::GreaterThan, &initial_num_intents);

  LOG(INFO) << "All nodes will be restarted";
  for (size_t i = 0; i < test_cluster()->num_tablet_servers(); ++i) {
    test_cluster()->mini_tablet_server(i)->Shutdown();
    ASSERT_OK(test_cluster()->mini_tablet_server(i)->Start());
    ASSERT_OK(test_cluster()->mini_tablet_server(i)->WaitStarted());
  }
  LOG(INFO) << "All nodes restarted";
  SleepFor(MonoDelta::FromSeconds(60));

  int64 num_intents_after_restart;
  PollForIntentCount(
      initial_num_intents, 0, IntentCountCompareOption::EqualTo, &num_intents_after_restart);
  LOG(INFO) << "Number of intents after restart: " << num_intents_after_restart;
  ASSERT_EQ(num_intents_after_restart, initial_num_intents);

  GetChangesResponsePB change_resp_2 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));
  uint32_t record_size = change_resp_2.cdc_sdk_proto_records_size();
  // We have run 2 transactions after the last call to "GetChangesFromCDC", thus we expect
  // atleast 200 records if we call "GetChangesFromCDC" now.
  LOG(INFO) << "Number of records after restart: " << record_size;
  ASSERT_GE(record_size, 200);

  // Now that there are no more transaction, and we have called "GetChangesFromCDC" already, there
  // must be no more records or intents remaining.
  GetChangesResponsePB change_resp_3 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_2.cdc_sdk_checkpoint()));
  uint32_t final_record_size = change_resp_3.cdc_sdk_proto_records_size();
  LOG(INFO) << "Number of recrods after no new transactions: " << final_record_size;
  ASSERT_EQ(final_record_size, 0);

  int64 final_num_intents;
  PollForIntentCount(0, 0, IntentCountCompareOption::EqualTo, &final_num_intents);
  ASSERT_EQ(0, final_num_intents);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestHighIntentCountPersistencyAllNodesRestart)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  FLAGS_log_segment_size_bytes = 100;
  ASSERT_OK(SetUpWithParams(1, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  // Insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 1 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  ASSERT_OK(WriteRowsHelper(1, 75, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  int64 initial_num_intents;
  PollForIntentCount(1, 0, IntentCountCompareOption::GreaterThan, &initial_num_intents);
  LOG(INFO) << "Number of intents before restart: " << initial_num_intents;

  LOG(INFO) << "All nodes will be restarted";
  for (size_t i = 0; i < test_cluster()->num_tablet_servers(); ++i) {
    test_cluster()->mini_tablet_server(i)->Shutdown();
    ASSERT_OK(test_cluster()->mini_tablet_server(i)->Start());
    ASSERT_OK(test_cluster()->mini_tablet_server(i)->WaitStarted());
  }
  LOG(INFO) << "All nodes restarted";
  SleepFor(MonoDelta::FromSeconds(60));

  int64 num_intents_after_restart;
  PollForIntentCount(initial_num_intents, 0, IntentCountCompareOption::EqualTo,
                     &num_intents_after_restart);
  LOG(INFO) << "Number of intents after restart: " << num_intents_after_restart;
  ASSERT_EQ(num_intents_after_restart, initial_num_intents);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestIntentCountPersistencyBootstrap)) {
  // Disable lb as we move tablets around
  FLAGS_enable_load_balancing = false;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_update_metrics_interval_ms = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  ASSERT_OK(SetUpWithParams(3, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  // Insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));
  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  change_resp_1 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));

  size_t first_leader_index = -1;
  size_t first_follower_index = -1;
  GetTabletLeaderAndAnyFollowerIndex(tablets, &first_leader_index, &first_follower_index);
  if (first_leader_index == 0) {
    // We want to avoid the scenario where the first TServer is the leader, since we want to shut
    // the leader TServer down and call GetChanges. GetChanges will be called on the cdc_proxy based
    // on the first TServer's address and we want to avoid the network issues.
    ASSERT_OK(ChangeLeaderOfTablet(first_follower_index, tablets[0].tablet_id()));
    std::swap(first_leader_index, first_follower_index);
  }

  ASSERT_OK(WriteRowsHelper(100 /* start */, 200 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  ASSERT_OK(ChangeLeaderOfTablet(first_follower_index, tablets[0].tablet_id()));
  // Shutdown tserver hosting tablet initial leader, now it is a follower.
  test_cluster()->mini_tablet_server(first_leader_index)->Shutdown();
  LOG(INFO) << "TServer hosting tablet leader shutdown";

  change_resp_1 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));

  // Restart the tserver hosting the initial leader.
  ASSERT_OK(test_cluster()->mini_tablet_server(first_leader_index)->Start());
  ASSERT_OK(test_cluster()->mini_tablet_server(first_leader_index)->WaitStarted());
  SleepFor(MonoDelta::FromSeconds(1));

  OpId last_seen_checkpoint_op_id = OpId::Invalid();
  int64 last_seen_num_intents = -1;
  for (uint32_t i = 0; i < test_cluster()->num_tablet_servers(); ++i) {
    auto tablet_peer_result = test_cluster()->GetTabletManager(i)->GetServingTablet(
        tablets[0].tablet_id());
    if (!tablet_peer_result.ok()) {
      continue;
    }
    auto tablet_peer = std::move(*tablet_peer_result);

    OpId checkpoint = (*tablet_peer).cdc_sdk_min_checkpoint_op_id();
    LOG(INFO) << "Checkpoint OpId : " << checkpoint << " ,  on tserver index: " << i;
    if (last_seen_checkpoint_op_id == OpId::Invalid()) {
      last_seen_checkpoint_op_id = checkpoint;
    } else {
      ASSERT_EQ(last_seen_checkpoint_op_id, checkpoint);
    }

    int64 num_intents;
    PollForIntentCount(0, i, IntentCountCompareOption::GreaterThan, &num_intents);
    LOG(INFO) << "Num of intents: " << num_intents << ", on tserver index" << i;
    if (last_seen_num_intents == -1) {
      last_seen_num_intents = num_intents;
    } else {
      ASSERT_EQ(last_seen_num_intents, num_intents);
    }
  }
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestEnum)) {
  FLAGS_enable_update_local_peer_min_index = false;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  ASSERT_OK(SetUpWithParams(3, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(
      CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets, true, false, 0, true));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  std::string table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  int insert_count = 10;
  // Insert some records in transaction.
  ASSERT_OK(WriteEnumsRows(0, insert_count, &test_cluster_));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  // Call get changes.
  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  ASSERT_GT(record_size, insert_count);

  int expected_key_value = 0;
  for (uint32_t i = 0; i < record_size; ++i) {
    if (change_resp.cdc_sdk_proto_records(i).row_message().op() == RowMessage::INSERT) {
      const CDCSDKProtoRecordPB record = change_resp.cdc_sdk_proto_records(i);
      ASSERT_EQ(expected_key_value, record.row_message().new_tuple(0).datum_int32());
      ASSERT_EQ(
          expected_key_value % 2 ? "FIXED" : "PERCENTAGE",
          record.row_message().new_tuple(1).datum_string());
      expected_key_value++;
    }
  }

  ASSERT_EQ(insert_count, expected_key_value);
}

// Tests that the enum cache is correctly re-populated on a cache miss.
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestEnumOnRestart)) {
  FLAGS_enable_update_local_peer_min_index = false;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  ASSERT_OK(SetUpWithParams(1, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(
      CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets, true, false, 0, true));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  std::string table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  int insert_count = 20;
  // Insert some records in transaction.
  ASSERT_OK(WriteEnumsRows(0, insert_count / 2, &test_cluster_));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  // Restart one of the node.
  SleepFor(MonoDelta::FromSeconds(1));
  test_cluster()->mini_tablet_server(0)->Shutdown();
  ASSERT_OK(test_cluster()->mini_tablet_server(0)->Start());
  ASSERT_OK(test_cluster()->mini_tablet_server(0)->WaitStarted());

  // Insert some more records in transaction.
  ASSERT_OK(WriteEnumsRows(insert_count / 2, insert_count, &test_cluster_));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  // Call get changes.
  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  ASSERT_GT(record_size, insert_count);

  int expected_key_value = 0;
  for (uint32_t i = 0; i < record_size; ++i) {
    if (change_resp.cdc_sdk_proto_records(i).row_message().op() == RowMessage::INSERT) {
      const CDCSDKProtoRecordPB record = change_resp.cdc_sdk_proto_records(i);
      ASSERT_EQ(expected_key_value, record.row_message().new_tuple(0).datum_int32());
      ASSERT_EQ(
          expected_key_value % 2 ? "FIXED" : "PERCENTAGE",
          record.row_message().new_tuple(1).datum_string());
      expected_key_value++;
    }
  }

  ASSERT_EQ(insert_count, expected_key_value);
}

// Tests that the enum cache is correctly re-populated on stream creation.
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestEnumMultipleStreams)) {
  FLAGS_enable_update_local_peer_min_index = false;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  ASSERT_OK(SetUpWithParams(1, 1, false));

  const uint32_t num_tablets = 1;

  auto table = ASSERT_RESULT(
      CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets, true, false, 0, true));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version=*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  std::string table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  int insert_count = 10;

  auto table1 = ASSERT_RESULT(CreateTable(
      &test_cluster_, kNamespaceName, kTableName, num_tablets, true, false, 0, true, "1"));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets1;
  ASSERT_OK(test_client()->GetTablets(table1, 0, &tablets1, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets1.size(), num_tablets);

  CDCStreamId stream_id1 = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp1 = ASSERT_RESULT(SetCDCCheckpoint(stream_id1, tablets1));
  ASSERT_FALSE(resp1.has_error());

  // Insert some records in transaction.
  ASSERT_OK(WriteEnumsRows(0, insert_count, &test_cluster_, "1"));
  ASSERT_OK(test_client()->FlushTables(
      {table1.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  // Call get changes.
  GetChangesResponsePB change_resp1 = ASSERT_RESULT(GetChangesFromCDC(stream_id1, tablets1));
  uint32_t record_size1 = change_resp1.cdc_sdk_proto_records_size();
  ASSERT_GT(record_size1, insert_count);

  int expected_key_value = 0;
  for (uint32_t i = 0; i < record_size1; ++i) {
    if (change_resp1.cdc_sdk_proto_records(i).row_message().op() == RowMessage::INSERT) {
      const CDCSDKProtoRecordPB record = change_resp1.cdc_sdk_proto_records(i);
      ASSERT_EQ(expected_key_value, record.row_message().new_tuple(0).datum_int32());
      ASSERT_EQ(
          expected_key_value % 2 ? "FIXED1" : "PERCENTAGE1",
          record.row_message().new_tuple(1).datum_string());
      expected_key_value++;
    }
  }

  ASSERT_EQ(insert_count, expected_key_value);
}

// Test GetChanges() can return records of a transaction with size was greater than
// 'consensus_max_batch_size_bytes'.
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestTransactionWithLargeBatchSize)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  FLAGS_consensus_max_batch_size_bytes = 1000;
  ASSERT_OK(SetUpWithParams(1, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version=*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  // Insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));
  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  change_resp_1 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));
  LOG(INFO) << "Number of records after first transaction: " << change_resp_1.records().size();

  // Insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(100, 500, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  int64 initial_num_intents;
  PollForIntentCount(400, 0, IntentCountCompareOption::GreaterThan, &initial_num_intents);
  LOG(INFO) << "Number of intents: " << initial_num_intents;

  GetChangesResponsePB change_resp_2 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));
  uint32_t record_size = change_resp_2.cdc_sdk_proto_records_size();
  // We have run 1 transactions after the last call to "GetChangesFromCDC", thus we expect
  // atleast 400 records if we call "GetChangesFromCDC" now.
  LOG(INFO) << "Number of records after second transaction: " << record_size;
  ASSERT_GE(record_size, 400);
  ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_2.cdc_sdk_checkpoint()));

  int64 final_num_intents;
  PollForIntentCount(0, 0, IntentCountCompareOption::EqualTo, &final_num_intents);
  ASSERT_EQ(0, final_num_intents);
  LOG(INFO) << "Final number of intents: " << final_num_intents;
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestIntentCountPersistencyAfterCompaction)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  // We want to force every GetChanges to update the cdc_state table.
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  FLAGS_aborted_intent_cleanup_ms = 1000;  // 1 sec

  ASSERT_OK(SetUpWithParams(1, 1, false));
  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  // Insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));
  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  LOG(INFO) << "Number of records after first transaction: " << change_resp_1.records().size();
  change_resp_1 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));

  ASSERT_OK(WriteRowsHelper(100 /* start */, 200 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  ASSERT_OK(WriteRowsHelper(200 /* start */, 300 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));
  SleepFor(MonoDelta::FromSeconds(10));

  int64 initial_num_intents;
  PollForIntentCount(1, 0, IntentCountCompareOption::GreaterThan, &initial_num_intents);

  SleepFor(MonoDelta::FromSeconds(60));
  LOG(INFO) << "All nodes will be restarted";
  for (size_t i = 0; i < test_cluster()->num_tablet_servers(); ++i) {
    test_cluster()->mini_tablet_server(i)->Shutdown();
    ASSERT_OK(test_cluster()->mini_tablet_server(i)->Start());
    ASSERT_OK(test_cluster()->mini_tablet_server(i)->WaitStarted());
  }
  LOG(INFO) << "All nodes restarted";

  std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_aborted_intent_cleanup_ms));
  ASSERT_OK(test_cluster_.mini_cluster_->CompactTablets());
  std::this_thread::sleep_for(std::chrono::seconds(10));

  int64 num_intents_after_compaction;
  PollForIntentCount(
      initial_num_intents, 0, IntentCountCompareOption::EqualTo, &num_intents_after_compaction);
  LOG(INFO) << "Number of intents after compaction: " << num_intents_after_compaction;
  ASSERT_EQ(num_intents_after_compaction, initial_num_intents);

  GetChangesResponsePB change_resp_2 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));
  uint32_t record_size = change_resp_2.cdc_sdk_proto_records_size();

  // We have run 2 transactions after the last call to "GetChangesFromCDC", thus we expect
  // atleast 200 records if we call "GetChangesFromCDC" now.
  LOG(INFO) << "Number of records after compaction: " << record_size;
  ASSERT_GE(record_size, 200);

  // Now that there are no more transaction, and we have called "GetChangesFromCDC" already, there
  // must be no more records or intents remaining.
  GetChangesResponsePB change_resp_3 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_2.cdc_sdk_checkpoint()));
  uint32_t final_record_size = change_resp_3.cdc_sdk_proto_records_size();
  LOG(INFO) << "Number of recrods after no new transactions: " << final_record_size;
  ASSERT_EQ(final_record_size, 0);

  int64 final_num_intents;
  PollForIntentCount(0, 0, IntentCountCompareOption::EqualTo, &final_num_intents);
  ASSERT_EQ(0, final_num_intents);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestLogGCedWithTabletBootStrap)) {
  FLAGS_update_min_cdc_indices_interval_secs = 100000;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  FLAGS_log_segment_size_bytes = 100;
  FLAGS_log_min_seconds_to_retain = 10;
  ASSERT_OK(SetUpWithParams(1, 1, false));
  const uint32_t num_tablets = 1;

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;

  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version=*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  // Insert some records.
  ASSERT_OK(WriteRows(0 /* start */, 100 /* end */, &test_cluster_));

  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  LOG(INFO) << "Number of records after first transaction: "
            << change_resp_1.cdc_sdk_proto_records_size();
  ASSERT_GE(change_resp_1.cdc_sdk_proto_records_size(), 100);

  ASSERT_OK(WriteRows(100 /* start */, 200 /* end */, &test_cluster_));
  // SleepFor(MonoDelta::FromSeconds(FLAGS_cdc_min_replicated_index_considered_stale_secs * 2));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 100,
      /* is_compaction = */ false));

  // Restart of the tsever will make Tablet Bootstrap.
  test_cluster()->mini_tablet_server(0)->Shutdown();
  ASSERT_OK(test_cluster()->mini_tablet_server(0)->Start());
  ASSERT_OK(test_cluster()->mini_tablet_server(0)->WaitStarted());

  SleepFor(MonoDelta::FromSeconds(FLAGS_log_min_seconds_to_retain));
  // Here testcase behave like a WAL cleaner thread.
  for (size_t i = 0; i < test_cluster()->num_tablet_servers(); ++i) {
    for (const auto& tablet_peer : test_cluster()->GetTabletPeers(i)) {
      if (tablet_peer->tablet_id() == tablets[0].tablet_id()) {
        // Here setting FLAGS_cdc_min_replicated_index_considered_stale_secs to 1, so that CDC
        // replication index will be set to max value, which will create a scenario to clean stale
        // WAL logs, even if CDCSDK no consumed those Logs.
        FLAGS_cdc_min_replicated_index_considered_stale_secs = 1;
        ASSERT_OK(tablet_peer->RunLogGC());
      }
    }
  }

  GetChangesResponsePB change_resp_2 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));
  LOG(INFO) << "Number of records after second transaction: "
            << change_resp_2.cdc_sdk_proto_records_size();
  ASSERT_GE(change_resp_2.cdc_sdk_proto_records_size(), 100);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestXClusterLogGCedWithTabletBootStrap)) {
  FLAGS_update_min_cdc_indices_interval_secs = 100000;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  FLAGS_log_segment_size_bytes = 100;
  FLAGS_log_min_seconds_to_retain = 10;
  ASSERT_OK(SetUpWithParams(1, 1, false));
  const uint32_t num_tablets = 1;

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;

  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version=*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);


  RpcController rpc;
  CreateCDCStreamRequestPB create_req;
  CreateCDCStreamResponsePB create_resp;
  create_req.set_table_id(table_id);
  create_req.set_source_type(XCLUSTER);
  ASSERT_OK(cdc_proxy_->CreateCDCStream(create_req, &create_resp, &rpc));

  // Insert some records.
  ASSERT_OK(WriteRows(0 /* start */, 100 /* end */, &test_cluster_));
  rpc.Reset();

  GetChangesRequestPB change_req;
  GetChangesResponsePB change_resp_1;
  change_req.set_stream_id(create_resp.stream_id());
  change_req.set_tablet_id(tablets[0].tablet_id());
  change_req.mutable_from_checkpoint()->mutable_op_id()->set_index(0);
  change_req.mutable_from_checkpoint()->mutable_op_id()->set_term(0);
  change_req.set_serve_as_proxy(true);
  rpc.set_timeout(MonoDelta::FromSeconds(kRpcTimeout));
  ASSERT_OK(cdc_proxy_->GetChanges(change_req, &change_resp_1, &rpc));
  ASSERT_FALSE(change_resp_1.has_error());

  ASSERT_OK(WriteRows(100 /* start */, 200 /* end */, &test_cluster_));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 100,
      /* is_compaction = */ false));

  // Restart of the tsever will make Tablet Bootstrap.
  test_cluster()->mini_tablet_server(0)->Shutdown();
  ASSERT_OK(test_cluster()->mini_tablet_server(0)->Start());
  ASSERT_OK(test_cluster()->mini_tablet_server(0)->WaitStarted());

  SleepFor(MonoDelta::FromSeconds(FLAGS_log_min_seconds_to_retain));
  // Here testcase behave like a WAL cleaner thread.
  for (size_t i = 0; i < test_cluster()->num_tablet_servers(); ++i) {
    for (const auto& tablet_peer : test_cluster()->GetTabletPeers(i)) {
      if (tablet_peer->tablet_id() == tablets[0].tablet_id()) {
        // Here setting FLAGS_cdc_min_replicated_index_considered_stale_secs to 1, so that CDC
        // replication index will be set to max value, which will create a scenario to clean stale
        // WAL logs, even if CDCSDK no consumed those Logs.
        FLAGS_cdc_min_replicated_index_considered_stale_secs = 1;
        ASSERT_OK(tablet_peer->RunLogGC());
      }
    }
  }

  GetChangesResponsePB change_resp_2;
  rpc.Reset();
  change_req.set_stream_id(create_resp.stream_id());
  change_req.set_tablet_id(tablets[0].tablet_id());
  change_req.mutable_from_checkpoint()->mutable_op_id()->set_index(
      change_resp_1.checkpoint().op_id().index());
  change_req.mutable_from_checkpoint()->mutable_op_id()->set_term(
      change_resp_1.checkpoint().op_id().term());
  change_req.set_serve_as_proxy(true);
  rpc.set_timeout(MonoDelta::FromSeconds(kRpcTimeout));

  ASSERT_OK(cdc_proxy_->GetChanges(change_req, &change_resp_2, &rpc));
  ASSERT_FALSE(change_resp_2.has_error());
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestEnumWithMultipleTablets)) {
  FLAGS_enable_update_local_peer_min_index = false;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;

  const uint32_t num_tablets = 3;
  vector<TabletId> table_id(2);
  vector<CDCStreamId> stream_id(2);
  vector<const char*> listTablesName{"test_table_01", "test_table_02"};
  vector<std::string> tablePrefix{"_01", "_02"};
  const int total_stream_count = 2;

  ASSERT_OK(SetUpWithParams(3, 1, false));

  // Here we are verifying Enum Cache for a tablespace that needs to be re-updated // if there is a
  // cache miss in any of the tsever. This can happen when enum cache entry is created for the
  // all the tservers as part of CreateCDCStream or GetChanges call and later stage client
  // created one more enum type on the same tablespace and a new table, then GetChanges call on
  // the newtable should not fail,(precondition:- create new stream in same namespace).
  for (int idx = 0; idx < total_stream_count; idx++) {
    auto table = ASSERT_RESULT(CreateTable(
        &test_cluster_, kNamespaceName, kTableName, num_tablets, true, false, 0, true,
        tablePrefix[idx]));
    google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
    ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
    ASSERT_EQ(tablets.size(), num_tablets);

    table_id[idx] = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, listTablesName[idx]));
    stream_id[idx] = ASSERT_RESULT(CreateDBStream(IMPLICIT));

    for (uint32_t jdx = 0; jdx < num_tablets; jdx++) {
      auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id[idx], tablets, OpId::Min(), true, jdx));
      ASSERT_FALSE(resp.has_error());
    }

    ASSERT_OK(WriteEnumsRows(0, 100, &test_cluster_, tablePrefix[idx], kNamespaceName, kTableName));
    ASSERT_OK(test_client()->FlushTables(
        {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
        /* is_compaction = */ false));

    int total_count = 0;
    for (uint32_t kdx = 0; kdx < num_tablets; kdx++) {
      GetChangesResponsePB change_resp =
          ASSERT_RESULT(GetChangesFromCDC(stream_id[idx], tablets, nullptr, kdx));
      uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
      for (uint32_t i = 0; i < record_size; ++i) {
        if (change_resp.cdc_sdk_proto_records(i).row_message().op() == RowMessage::INSERT) {
          const CDCSDKProtoRecordPB record = change_resp.cdc_sdk_proto_records(i);
          total_count += 1;
        }
      }
    }
    LOG(INFO) << "Total GetChanges record counts: " << total_count;
    ASSERT_EQ(total_count, 100);
  }
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestSetCDCCheckpointWithHigherTserverThanTablet)) {
  // Create a cluster where the number of tservers are 5 (tserver-1, tserver-2, tserver-3,
  // tserver-4, tserver-5). Create table with tablet split 3(tablet-1, tablet-2, tablet-3).
  // Consider the tablet-1 LEADER is in tserver-3, tablet-2 LEADER in tserver-4 and tablet-3 LEADER
  // is in tserver-5. Consider cdc proxy connection is created with tserver-1. calling
  // setCDCCheckpoint from tserver-1 should PASS.
  // Since number of tablets is lesser than the number of tservers, there must be atleast 2 tservers
  // which do not host any of the tablet. But still, calling setCDCCheckpoint any of the
  // tserver, even the ones not hosting tablet, should PASS.
  ASSERT_OK(SetUpWithParams(5, 1, false));

  const uint32_t num_tablets = 3;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);
  std::string table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());

  for (uint32_t idx = 0; idx < num_tablets; idx++) {
    auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets, OpId::Min(), true, idx));
    ASSERT_FALSE(resp.has_error());
  }
}
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestIntentPersistencyAfterTabletSplit)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(SetUpWithParams(1, 1, false));
  const uint32_t num_tablets = 1;

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  ASSERT_OK(WriteRowsHelper(100, 200, &test_cluster_, true));
  int64 initial_num_intents;
  PollForIntentCount(1, 0, IntentCountCompareOption::GreaterThan, &initial_num_intents);
  LOG(INFO) << "Number of intents before tablet split: " << initial_num_intents;

  ASSERT_OK(SplitTablet(tablets.Get(0).tablet_id(), &test_cluster_));

  LOG(INFO) << "All nodes will be restarted";
  for (size_t i = 0; i < test_cluster()->num_tablet_servers(); ++i) {
    test_cluster()->mini_tablet_server(i)->Shutdown();
    ASSERT_OK(test_cluster()->mini_tablet_server(i)->Start());
    ASSERT_OK(test_cluster()->mini_tablet_server(i)->WaitStarted());
  }
  LOG(INFO) << "All nodes restarted";

  int64 num_intents_after_restart;
  PollForIntentCount(
      initial_num_intents, 0, IntentCountCompareOption::EqualTo, &num_intents_after_restart);
  LOG(INFO) << "Number of intents after tablet split: " << num_intents_after_restart;
  ASSERT_EQ(num_intents_after_restart, initial_num_intents);

  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  ASSERT_GE(change_resp_1.cdc_sdk_proto_records_size(), 100);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCheckpointPersistencyAfterTabletSplit)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  ASSERT_OK(SetUpWithParams(1, 1, false));
  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version=*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  ASSERT_OK(WriteRowsHelper(100, 200, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));
  ASSERT_OK(WriteRowsHelper(200, 300, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  change_resp_1 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));
  SleepFor(MonoDelta::FromSeconds(10));

  OpId cdc_sdk_min_checkpoint = OpId::Invalid();
  for (const auto& peer : test_cluster()->GetTabletPeers(0)) {
    if (peer->tablet_id() == tablets[0].tablet_id()) {
      cdc_sdk_min_checkpoint = peer->cdc_sdk_min_checkpoint_op_id();
      break;
    }
  }
  LOG(INFO) << "Min checkpoint OpId for the tablet peer before tablet split: "
            << cdc_sdk_min_checkpoint;

  ASSERT_OK(SplitTablet(tablets.Get(0).tablet_id(), &test_cluster_));
  SleepFor(MonoDelta::FromSeconds(60));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets_after_split;
  ASSERT_OK(test_client()->GetTablets(
      table, 0, &tablets_after_split, /* partition_list_version =*/nullptr));
  LOG(INFO) << "Number of tablets after the split: " << tablets_after_split.size();
  ASSERT_EQ(tablets_after_split.size(), num_tablets * 2);

  for (const auto& peer : test_cluster()->GetTabletPeers(0)) {
    if (peer->tablet_id() == tablets_after_split[0].tablet_id() ||
        peer->tablet_id() == tablets_after_split[1].tablet_id()) {
      LOG(INFO) << "TabletId before split: " << tablets[0].tablet_id();
      ASSERT_LE(peer->cdc_sdk_min_checkpoint_op_id(), cdc_sdk_min_checkpoint);
      LOG(INFO) << "Post split, Tablet: " << peer->tablet_id()
                << ", has the same or lower cdc_sdk_min_checkpoint: " << cdc_sdk_min_checkpoint
                << ", as before tablet split.";
    }
  }
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestTransactionInsertAfterTabletSplit)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  ASSERT_OK(SetUpWithParams(1, 1, false));
  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version=*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  ASSERT_OK(WriteRowsHelper(100, 200, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));
  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  change_resp_1 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));
  change_resp_1 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));

  ASSERT_OK(SplitTablet(tablets.Get(0).tablet_id(), &test_cluster_));
  SleepFor(MonoDelta::FromSeconds(60));

  // Now that we have streamed all records from the parent tablet, we expect further calls of
  // 'GetChangesFromCDC' to the same tablet to fail.
  ASSERT_NOK(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets_after_split;
  ASSERT_OK(test_client()->GetTablets(
      table, 0, &tablets_after_split, /* partition_list_version =*/nullptr));
  LOG(INFO) << "Number of tablets after the split: " << tablets_after_split.size();
  ASSERT_EQ(tablets_after_split.size(), num_tablets * 2);

  ASSERT_OK(WriteRowsHelper(200, 300, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> first_tablet_after_split;
  first_tablet_after_split.CopyFrom(tablets_after_split);
  ASSERT_EQ(first_tablet_after_split[0].tablet_id(), tablets_after_split[0].tablet_id());

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> second_tablet_after_split;
  second_tablet_after_split.CopyFrom(tablets_after_split);
  second_tablet_after_split.DeleteSubrange(0, 1);
  ASSERT_EQ(second_tablet_after_split.size(), 1);
  ASSERT_EQ(second_tablet_after_split[0].tablet_id(), tablets_after_split[1].tablet_id());

  GetChangesResponsePB change_resp_2 = ASSERT_RESULT(
      GetChangesFromCDC(stream_id, first_tablet_after_split, &change_resp_1.cdc_sdk_checkpoint()));
  LOG(INFO) << "Number of records from GetChanges() call on first tablet after split: "
            << change_resp_2.cdc_sdk_proto_records_size();

  GetChangesResponsePB change_resp_3 = ASSERT_RESULT(
      GetChangesFromCDC(stream_id, second_tablet_after_split, &change_resp_1.cdc_sdk_checkpoint()));
  LOG(INFO) << "Number of records from GetChanges() call on second tablet after split: "
            << change_resp_3.cdc_sdk_proto_records_size();

  ASSERT_GE(
      change_resp_2.cdc_sdk_proto_records_size() + change_resp_3.cdc_sdk_proto_records_size(), 100);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestGetChangesAfterTabletSplitWithMasterShutdown)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  FLAGS_aborted_intent_cleanup_ms = 1000;
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(SetUpWithParams(3, 1, false));
  const uint32_t num_tablets = 1;

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());
  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  ASSERT_OK(WriteRowsHelper(1, 200, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ true));
  std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_aborted_intent_cleanup_ms));
  ASSERT_OK(test_cluster_.mini_cluster_->CompactTablets());
  SleepFor(MonoDelta::FromSeconds(30));

  test_cluster_.mini_cluster_->mini_master()->Shutdown();
  ASSERT_OK(test_cluster_.mini_cluster_->StartMasters());
  LOG(INFO) << "Restart master before tablet split succesfull";
  WaitUntilSplitIsSuccesful(tablets.Get(0).tablet_id(), table);
  SleepFor(MonoDelta::FromSeconds(5));
  test_cluster_.mini_cluster_->mini_master()->Shutdown();
  ASSERT_OK(test_cluster_.mini_cluster_->StartMasters());
  LOG(INFO) << "Restart master after tablet split succesfull";

  // We must still be able to get the remaining records from the parent tablet even after master is
  // restarted.
  change_resp_1 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));
  ASSERT_GE(change_resp_1.cdc_sdk_proto_records_size(), 200);
  LOG(INFO) << "Number of records after restart: " << change_resp_1.cdc_sdk_proto_records_size();

  // Now that there are no more records to stream, further calls of 'GetChangesFromCDC' to the same
  // tablet should fail.
  ASSERT_NOK(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestGetChangesOnParentTabletAfterTabletSplit)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  FLAGS_aborted_intent_cleanup_ms = 1000;
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(SetUpWithParams(3, 1, false));
  const uint32_t num_tablets = 1;

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());
  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  ASSERT_OK(WriteRowsHelper(1, 200, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ true));
  std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_aborted_intent_cleanup_ms));
  ASSERT_OK(test_cluster_.mini_cluster_->CompactTablets());
  SleepFor(MonoDelta::FromSeconds(30));

  WaitUntilSplitIsSuccesful(tablets.Get(0).tablet_id(), table);

  LOG(INFO) << "All nodes will be restarted";
  for (size_t i = 0; i < test_cluster()->num_tablet_servers(); ++i) {
    test_cluster()->mini_tablet_server(i)->Shutdown();
    ASSERT_OK(test_cluster()->mini_tablet_server(i)->Start());
    ASSERT_OK(test_cluster()->mini_tablet_server(i)->WaitStarted());
  }
  LOG(INFO) << "All nodes restarted";
  SleepFor(MonoDelta::FromSeconds(10));

  change_resp_1 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));
  ASSERT_GE(change_resp_1.cdc_sdk_proto_records_size(), 200);
  LOG(INFO) << "Number of records after restart: " << change_resp_1.cdc_sdk_proto_records_size();

  // Now that there are no more records to stream, further calls of 'GetChangesFromCDC' to the same
  // tablet should fail.
  ASSERT_NOK(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestGetChangesMultipleStreamsTabletSplit)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  FLAGS_aborted_intent_cleanup_ms = 1000;
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(SetUpWithParams(1, 1, false));
  const uint32_t num_tablets = 1;

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  CDCStreamId stream_id_1 = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  CDCStreamId stream_id_2 = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id_1, tablets));
  ASSERT_FALSE(resp.has_error());
  resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id_2, tablets));
  ASSERT_FALSE(resp.has_error());
  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id_1, tablets));
  GetChangesResponsePB change_resp_2 = ASSERT_RESULT(GetChangesFromCDC(stream_id_2, tablets));

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));

  ASSERT_OK(WriteRowsHelper(0, 100, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ true));

  // Call GetChanges only on one stream so that the other stream will be lagging behind.
  change_resp_1 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id_1, tablets, &change_resp_1.cdc_sdk_checkpoint()));

  ASSERT_OK(WriteRowsHelper(100, 200, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ true));

  std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_aborted_intent_cleanup_ms));
  ASSERT_OK(test_cluster_.mini_cluster_->CompactTablets());
  SleepFor(MonoDelta::FromSeconds(30));

  WaitUntilSplitIsSuccesful(tablets.Get(0).tablet_id(), table);

  change_resp_1 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id_1, tablets, &change_resp_1.cdc_sdk_checkpoint()));
  ASSERT_GE(change_resp_1.cdc_sdk_proto_records_size(), 100);
  LOG(INFO) << "Number of records on first stream after split: "
            << change_resp_1.cdc_sdk_proto_records_size();

  // Now that there are no more records to stream, further calls of 'GetChangesFromCDC' to the same
  // tablet should fail.
  ASSERT_NOK(GetChangesFromCDC(stream_id_1, tablets, &change_resp_1.cdc_sdk_checkpoint()));

  // Calling GetChanges on stream 2 should still return around 200 records.
  change_resp_2 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id_2, tablets, &change_resp_2.cdc_sdk_checkpoint()));
  ASSERT_GE(change_resp_2.cdc_sdk_proto_records_size(), 200);

  ASSERT_NOK(GetChangesFromCDC(stream_id_2, tablets, &change_resp_2.cdc_sdk_checkpoint()));
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestSetCDCCheckpointAfterTabletSplit)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  FLAGS_aborted_intent_cleanup_ms = 1000;
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets_before_split;
  ASSERT_OK(SetUpWithParams(1, 1, false));
  const uint32_t num_tablets = 1;

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  ASSERT_OK(test_client()->GetTablets(
      table, 0, &tablets_before_split, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets_before_split.size(), num_tablets);

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  ASSERT_OK(WriteRowsHelper(0, 1000, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ true));
  std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_aborted_intent_cleanup_ms));
  ASSERT_OK(test_cluster_.mini_cluster_->CompactTablets());
  SleepFor(MonoDelta::FromSeconds(30));
  WaitUntilSplitIsSuccesful(tablets_before_split.Get(0).tablet_id(), table);

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets_after_split;
  ASSERT_OK(test_client()->GetTablets(
      table, 0, &tablets_after_split, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets_after_split.size(), 2);

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets_after_split, OpId::Min(), true, 0));
  ASSERT_FALSE(resp.has_error());

  resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets_after_split, OpId::Min(), true, 1));
  ASSERT_FALSE(resp.has_error());
}

// TODO Adithya: This test is failing in alma linux with clang builds.
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST(TestTabletSplitBeforeBootstrap)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_aborted_intent_cleanup_ms = 1000;
  FLAGS_update_metrics_interval_ms = 5000;
  FLAGS_cdc_parent_tablet_deletion_task_retry_secs = 1;

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  uint32_t num_tservers = 3;
  ASSERT_OK(SetUpWithParams(num_tservers, 1, false));
  const uint32_t num_tablets = 1;

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  ASSERT_OK(WriteRowsHelper(1, 200, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ true));
  std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_aborted_intent_cleanup_ms));
  ASSERT_OK(test_cluster_.mini_cluster_->CompactTablets());
  SleepFor(MonoDelta::FromSeconds(30));

  WaitUntilSplitIsSuccesful(tablets.Get(0).tablet_id(), table);
  SleepFor(MonoDelta::FromSeconds(10));

  // We are checking the 'cdc_state' table just after tablet split is succesfull, but since we
  // haven't started streaming from the parent tablet, we should only see 2 rows.
  client::TableHandle table_handle;
  client::YBTableName cdc_state_table(
      YQL_DATABASE_CQL, master::kSystemNamespaceName, master::kCdcStateTableName);
  ASSERT_OK(table_handle.Open(cdc_state_table, test_client()));

  uint seen_rows = 0;
  TabletId parent_tablet_id = tablets[0].tablet_id();
  for (const auto& row : client::TableRange(table_handle)) {
    const auto& tablet_id = row.column(master::kCdcTabletIdIdx).string_value();
    const auto& checkpoint = row.column(master::kCdcCheckpointIdx).string_value();
    LOG(INFO) << "Read cdc_state table row for tablet_id: " << tablet_id
              << " and stream_id: " << stream_id << ", with checkpoint: " << checkpoint;

    if (tablet_id != tablets[0].tablet_id()) {
      // Both children should have the min OpId(-1.-1) as the checkpoint.
      ASSERT_EQ(checkpoint, OpId::Invalid().ToString());
    }
    seen_rows += 1;
  }
  ASSERT_EQ(seen_rows, 2);

  // Since we haven't started polling yet, the checkpoint in the tablet peers would be OpId(-1.-1).
  for (uint tserver_index = 0; tserver_index < num_tservers; tserver_index++) {
    for (const auto& peer : test_cluster()->GetTabletPeers(tserver_index)) {
      if (peer->tablet_id() == tablets[0].tablet_id()) {
        ASSERT_EQ(OpId::Invalid(), peer->cdc_sdk_min_checkpoint_op_id());
      }
    }
  }
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCDCStateTableAfterTabletSplit)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  FLAGS_aborted_intent_cleanup_ms = 1000;
  FLAGS_update_metrics_interval_ms = 5000;

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(SetUpWithParams(3, 1, false));
  const uint32_t num_tablets = 1;

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());
  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  ASSERT_OK(WriteRowsHelper(1, 200, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ true));
  std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_aborted_intent_cleanup_ms));
  ASSERT_OK(test_cluster_.mini_cluster_->CompactTablets());
  SleepFor(MonoDelta::FromSeconds(30));

  WaitUntilSplitIsSuccesful(tablets.Get(0).tablet_id(), table);
  SleepFor(MonoDelta::FromSeconds(10));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets_after_split;
  ASSERT_OK(test_client()->GetTablets(
      table, 0, &tablets_after_split, /* partition_list_version =*/nullptr));

  // We are checking the 'cdc_state' table just after tablet split is succesfull, so we must see 3
  // entries, one for the parent tablet and two for the children tablets.
  client::TableHandle table_handle;
  client::YBTableName cdc_state_table(
      YQL_DATABASE_CQL, master::kSystemNamespaceName, master::kCdcStateTableName);
  ASSERT_OK(table_handle.Open(cdc_state_table, test_client()));

  uint seen_rows = 0;
  TabletId parent_tablet_id = tablets[0].tablet_id();
  for (const auto& row : client::TableRange(table_handle)) {
    auto tablet_id = row.column(master::kCdcTabletIdIdx).string_value();
    auto stream_id = row.column(master::kCdcStreamIdIdx).string_value();
    auto checkpoint = row.column(master::kCdcCheckpointIdx).string_value();
    LOG(INFO) << "Read cdc_state table row for tablet_id: " << tablet_id
              << " and stream_id: " << stream_id << ", with checkpoint: " << checkpoint;

    if (tablet_id != tablets[0].tablet_id()) {
      // Both children should have the min OpId(0.0) as the checkpoint.
      ASSERT_EQ(checkpoint, OpId::Min().ToString());
    }

    seen_rows += 1;
  }

  ASSERT_EQ(seen_rows, 3);
}

// TODO Adithya: This test is failing in alma linux with clang builds.
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST(TestCDCStateTableAfterTabletSplitReported)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  FLAGS_aborted_intent_cleanup_ms = 1000;
  FLAGS_cdc_parent_tablet_deletion_task_retry_secs = 1;

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(SetUpWithParams(3, 1, false));
  const uint32_t num_tablets = 1;

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());
  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  ASSERT_OK(WriteRowsHelper(1, 200, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ true));
  std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_aborted_intent_cleanup_ms));
  ASSERT_OK(test_cluster_.mini_cluster_->CompactTablets());
  SleepFor(MonoDelta::FromSeconds(30));

  WaitUntilSplitIsSuccesful(tablets.Get(0).tablet_id(), table);

  change_resp_1 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));
  ASSERT_GE(change_resp_1.cdc_sdk_proto_records_size(), 200);
  LOG(INFO) << "Number of records after restart: " << change_resp_1.cdc_sdk_proto_records_size();

  // Now that there are no more records to stream, further calls of 'GetChangesFromCDC' to the same
  // tablet should fail.
  ASSERT_NOK(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets_after_split;
  ASSERT_OK(test_client()->GetTablets(
      table, 0, &tablets_after_split, /* partition_list_version =*/nullptr));

  // Wait until the 'cdc_parent_tablet_deletion_task_' has run. Then the parent tablet's entry
  // should be removed from 'cdc_state' table.
  SleepFor(MonoDelta::FromSeconds(2));

  client::TableHandle table_handle;
  client::YBTableName cdc_state_table(
      YQL_DATABASE_CQL, master::kSystemNamespaceName, master::kCdcStateTableName);
  ASSERT_OK(table_handle.Open(cdc_state_table, test_client()));

  bool saw_row_child_one = false;
  bool saw_row_child_two = false;
  // We should no longer see the entry corresponding to the parent tablet.
  TabletId parent_tablet_id = tablets[0].tablet_id();
  for (const auto& row : client::TableRange(table_handle)) {
    auto tablet_id = row.column(master::kCdcTabletIdIdx).string_value();
    auto stream_id = row.column(master::kCdcStreamIdIdx).string_value();
    LOG(INFO) << "Read cdc_state table row with tablet_id: " << tablet_id
              << " stream_id: " << stream_id;

    ASSERT_TRUE(parent_tablet_id != tablet_id);

    if (tablet_id == tablets_after_split[0].tablet_id()) {
      saw_row_child_one = true;
    } else if (tablet_id == tablets_after_split[1].tablet_id()) {
      saw_row_child_two = true;
    }
  }

  ASSERT_TRUE(saw_row_child_one && saw_row_child_two);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestGetTabletListToPollForCDC)) {
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(SetUpWithParams(3, 1, false));
  const uint32_t num_tablets = 3;

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);
  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  auto get_tablets_resp = ASSERT_RESULT(GetTabletListToPollForCDC(stream_id, table_id));

  ASSERT_EQ(get_tablets_resp.tablet_checkpoint_pairs().size(), num_tablets);
}

TEST_F(
    CDCSDKYsqlTest,
    YB_DISABLE_TEST_IN_TSAN(TestGetTabletListToPollForCDCAfterTabletSplitReported)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  FLAGS_aborted_intent_cleanup_ms = 1000;
  FLAGS_cdc_parent_tablet_deletion_task_retry_secs = 1;

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(SetUpWithParams(3, 1, false));
  const uint32_t num_tablets = 1;

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());
  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  ASSERT_OK(WriteRowsHelper(1, 200, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ true));
  std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_aborted_intent_cleanup_ms));
  ASSERT_OK(test_cluster_.mini_cluster_->CompactTablets());
  SleepFor(MonoDelta::FromSeconds(30));

  WaitUntilSplitIsSuccesful(tablets.Get(0).tablet_id(), table);

  change_resp_1 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));
  ASSERT_GE(change_resp_1.cdc_sdk_proto_records_size(), 200);
  LOG(INFO) << "Number of records after restart: " << change_resp_1.cdc_sdk_proto_records_size();

  // Now that there are no more records to stream, further calls of 'GetChangesFromCDC' to the same
  // tablet should fail.
  ASSERT_NOK(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));
  LOG(INFO) << "The tablet split error is now communicated to the client.";

  auto get_tablets_resp = ASSERT_RESULT(GetTabletListToPollForCDC(stream_id, table_id));
  ASSERT_EQ(get_tablets_resp.tablet_checkpoint_pairs().size(), 2);

  // Wait until the 'cdc_parent_tablet_deletion_task_' has run.
  SleepFor(MonoDelta::FromSeconds(2));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets_after_split;
  ASSERT_OK(test_client()->GetTablets(
      table, 0, &tablets_after_split, /* partition_list_version =*/nullptr));

  bool saw_row_child_one = false;
  bool saw_row_child_two = false;
  // We should no longer see the entry corresponding to the parent tablet.
  TabletId parent_tablet_id = tablets[0].tablet_id();
  for (const auto& tablet_checkpoint_pair : get_tablets_resp.tablet_checkpoint_pairs()) {
    const auto& tablet_id = tablet_checkpoint_pair.tablet_id();
    ASSERT_TRUE(parent_tablet_id != tablet_id);

    if (tablet_id == tablets_after_split[0].tablet_id()) {
      saw_row_child_one = true;
    } else if (tablet_id == tablets_after_split[1].tablet_id()) {
      saw_row_child_two = true;
    }
  }

  ASSERT_TRUE(saw_row_child_one && saw_row_child_two);
}

TEST_F(
    CDCSDKYsqlTest,
    YB_DISABLE_TEST_IN_TSAN(TestGetTabletListToPollForCDCBeforeTabletSplitReported)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  FLAGS_aborted_intent_cleanup_ms = 1000;
  FLAGS_cdc_parent_tablet_deletion_task_retry_secs = 1;

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(SetUpWithParams(3, 1, false));
  const uint32_t num_tablets = 1;

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  CDCStreamId stream_id_1 = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());
  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  ASSERT_OK(WriteRowsHelper(1, 200, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ true));
  std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_aborted_intent_cleanup_ms));
  ASSERT_OK(test_cluster_.mini_cluster_->CompactTablets());
  SleepFor(MonoDelta::FromSeconds(30));

  WaitUntilSplitIsSuccesful(tablets.Get(0).tablet_id(), table);

  // We are calling: "GetTabletListToPollForCDC" when the client has not yet streamed all the data
  // from the parent tablet.
  auto get_tablets_resp = ASSERT_RESULT(GetTabletListToPollForCDC(stream_id, table_id));

  // Wait until the 'cdc_parent_tablet_deletion_task_' has run.
  SleepFor(MonoDelta::FromSeconds(2));

  // We should only see the entry corresponding to the parent tablet.
  TabletId parent_tablet_id = tablets[0].tablet_id();
  ASSERT_EQ(get_tablets_resp.tablet_checkpoint_pairs().size(), 1);
  for (const auto& tablet_checkpoint_pair : get_tablets_resp.tablet_checkpoint_pairs()) {
    const auto& tablet_id = tablet_checkpoint_pair.tablet_id();
    ASSERT_TRUE(parent_tablet_id == tablet_id);
  }
}

TEST_F(
    CDCSDKYsqlTest,
    YB_DISABLE_TEST_IN_TSAN(TestGetTabletListToPollForCDCBootstrapWithTabletSplit)) {
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(SetUpWithParams(3, 1, false));
  const uint32_t num_tablets = 1;

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));

  ASSERT_OK(WriteRowsHelper(1, 200, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ true));
  ASSERT_OK(test_cluster_.mini_cluster_->CompactTablets());
  SleepFor(MonoDelta::FromSeconds(30));
  WaitUntilSplitIsSuccesful(tablets.Get(0).tablet_id(), table);

  // We are calling: "GetTabletListToPollForCDC" when the client has not yet started streaming.
  auto get_tablets_resp = ASSERT_RESULT(GetTabletListToPollForCDC(stream_id, table_id));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets_after_split;
  ASSERT_OK(test_client()->GetTablets(
      table, 0, &tablets_after_split, /* partition_list_version =*/nullptr));

  bool saw_row_child_one = false;
  bool saw_row_child_two = false;
  // We should only see the entry corresponding to the children tablets.
  TabletId parent_tablet_id = tablets[0].tablet_id();
  for (const auto& tablet_checkpoint_pair : get_tablets_resp.tablet_checkpoint_pairs()) {
    const auto& tablet_id = tablet_checkpoint_pair.tablet_id();
    ASSERT_TRUE(parent_tablet_id != tablet_id);

    if (tablet_id == tablets_after_split[0].tablet_id()) {
      saw_row_child_one = true;
    } else if (tablet_id == tablets_after_split[1].tablet_id()) {
      saw_row_child_two = true;
    }
  }

  ASSERT_TRUE(saw_row_child_one && saw_row_child_two);
}

TEST_F(
    CDCSDKYsqlTest,
    YB_DISABLE_TEST_IN_TSAN(TestGetTabletListToPollForCDCBootstrapWithTwoTabletSplits)) {
  FLAGS_cdc_parent_tablet_deletion_task_retry_secs = 1;
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(SetUpWithParams(3, 1, false));
  const uint32_t num_tablets = 1;

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));

  ASSERT_OK(WriteRowsHelper(1, 200, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ true));
  ASSERT_OK(test_cluster_.mini_cluster_->CompactTablets());
  SleepFor(MonoDelta::FromSeconds(30));
  WaitUntilSplitIsSuccesful(tablets.Get(0).tablet_id(), table);
  LOG(INFO) << "First tablet split succeded on tablet: " << tablets[0].tablet_id();

  ASSERT_OK(WriteRowsHelper(200, 400, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ true));
  ASSERT_OK(test_cluster_.mini_cluster_->CompactTablets());
  SleepFor(MonoDelta::FromSeconds(30));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets_after_first_split;
  ASSERT_OK(test_client()->GetTablets(
      table, 0, &tablets_after_first_split, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets_after_first_split.size(), 2);

  // Now we again split one of the child tablets.
  WaitUntilSplitIsSuccesful(
      tablets_after_first_split.Get(0).tablet_id(), table, 3 /*expected_num_tablets*/);
  LOG(INFO) << "Second tablet split succeded on tablet: "
            << tablets_after_first_split[0].tablet_id();

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets_after_second_split;
  ASSERT_OK(test_client()->GetTablets(
      table, 0, &tablets_after_second_split, /* partition_list_version =*/nullptr));

  // We are calling: "GetTabletListToPollForCDC" when the client has not yet started streaming.
  auto get_tablets_resp = ASSERT_RESULT(GetTabletListToPollForCDC(stream_id, table_id));

  ASSERT_EQ(get_tablets_resp.tablet_checkpoint_pairs_size(), 3);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestGetTabletListToPollForCDCWithTwoTabletSplits)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  FLAGS_aborted_intent_cleanup_ms = 1000;
  FLAGS_cdc_parent_tablet_deletion_task_retry_secs = 1;

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(SetUpWithParams(3, 1, false));
  const uint32_t num_tablets = 1;

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());
  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  ASSERT_OK(WriteRowsHelper(1, 200, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ true));
  ASSERT_OK(test_cluster_.mini_cluster_->CompactTablets());

  WaitUntilSplitIsSuccesful(tablets.Get(0).tablet_id(), table);

  ASSERT_OK(WriteRowsHelper(200, 400, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ true));
  ASSERT_OK(test_cluster_.mini_cluster_->CompactTablets());

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets_after_first_split;
  ASSERT_OK(test_client()->GetTablets(
      table, 0, &tablets_after_first_split, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets_after_first_split.size(), 2);

  WaitUntilSplitIsSuccesful(tablets_after_first_split.Get(0).tablet_id(), table, 3);

  change_resp_1 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));
  ASSERT_GE(change_resp_1.cdc_sdk_proto_records_size(), 200);
  LOG(INFO) << "Number of records after restart: " << change_resp_1.cdc_sdk_proto_records_size();

  // We are calling: "GetTabletListToPollForCDC" when the tablet split on the parent tablet has
  // still not been communicated to the client. Hence we should get only the original parent tablet.
  auto get_tablets_resp = ASSERT_RESULT(GetTabletListToPollForCDC(stream_id, table_id));
  ASSERT_EQ(get_tablets_resp.tablet_checkpoint_pairs().size(), 1);
  for (const auto& tablet_checkpoint_pair : get_tablets_resp.tablet_checkpoint_pairs()) {
    const auto& tablet_id = tablet_checkpoint_pair.tablet_id();
    ASSERT_EQ(tablet_id, tablets[0].tablet_id());
  }

  // Now that there are no more records to stream, further calls of 'GetChangesFromCDC' to the same
  // tablet should fail.
  ASSERT_NOK(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));

  // Wait until the 'cdc_parent_tablet_deletion_task_' has run.
  SleepFor(MonoDelta::FromSeconds(2));

  // We are calling: "GetTabletListToPollForCDC" when the client has streamed all the data from the
  // parent tablet.
  get_tablets_resp = ASSERT_RESULT(GetTabletListToPollForCDC(stream_id, table_id));

  // We should only see the entries for the 2 child tablets, which were created after the first
  // tablet split.
  bool saw_first_child = false;
  bool saw_second_child = false;
  ASSERT_EQ(get_tablets_resp.tablet_checkpoint_pairs().size(), 2);
  const auto& parent_tablet_id = tablets[0].tablet_id();
  for (const auto& tablet_checkpoint_pair : get_tablets_resp.tablet_checkpoint_pairs()) {
    const auto& tablet_id = tablet_checkpoint_pair.tablet_id();
    ASSERT_TRUE(parent_tablet_id != tablet_id);

    if (tablet_id == tablets_after_first_split[0].tablet_id()) {
      saw_first_child = true;
    } else if (tablet_id == tablets_after_first_split[1].tablet_id()) {
      saw_second_child = true;
    }
  }
  ASSERT_TRUE(saw_first_child && saw_second_child);

  change_resp_1 = ASSERT_RESULT(
      GetChangesFromCDC(stream_id, tablets_after_first_split, &change_resp_1.cdc_sdk_checkpoint()));
  ASSERT_NOK(
      GetChangesFromCDC(stream_id, tablets_after_first_split, &change_resp_1.cdc_sdk_checkpoint()));
}

// Here creating a single table inside a namespace and a CDC stream on top of the namespace.
// Deleting the table should clean every thing from master cache as well as the system
// catalog.
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestStreamMetaDataCleanupAndDropTable)) {
  // Setup cluster.
  ASSERT_OK(SetUpWithParams(3, 1, false));

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version = */ nullptr));

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  ASSERT_OK(WriteRows(0 /* start */, 100 /* end */, &test_cluster_));

  DropTable(&test_cluster_, kTableName);
  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        while (true) {
          auto get_resp = GetDBStreamInfo(stream_id);
          // Wait until the background thread cleanup up the stream-id.
          if (get_resp.ok() && get_resp->has_error() && get_resp->table_info_size() == 0) {
            return true;
          }
        }
      },
      MonoDelta::FromSeconds(60), "Waiting for stream metadata cleanup."));
}

// Here we are creating multiple tables and a CDC stream on the same namespace.
// Deleting multiple tables from the namespace should only clean metadata related to
// deleted tables from master cache as well as system catalog.
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestStreamMetaDataCleanupMultiTableDrop)) {
  // Setup cluster.
  ASSERT_OK(SetUpWithParams(3, 1, false));
  const vector<string> table_list_suffix = {"_1", "_2", "_3"};
  const int kNumTables = 3;
  vector<YBTableName> table(kNumTables);
  CDCStreamId stream_id;
  int idx = 0;
  vector<google::protobuf::RepeatedPtrField<master::TabletLocationsPB>> tablets(kNumTables);

  for (auto table_suffix : table_list_suffix) {
    table[idx] = ASSERT_RESULT(CreateTable(
        &test_cluster_, kNamespaceName, kTableName, 1, true, false, 0, true, table_suffix));
    ASSERT_OK(test_client()->GetTablets(
        table[idx], 0, &tablets[idx], /* partition_list_version = */ nullptr));
    TableId table_id =
        ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName + table_suffix));

    ASSERT_OK(WriteEnumsRows(
        0 /* start */, 100 /* end */, &test_cluster_, table_suffix, kNamespaceName, kTableName));
    idx += 1;
  }
  stream_id = ASSERT_RESULT(CreateDBStream());

  // Drop one of the table from the namespace, check stream associated with namespace should not
  // be deleted, but metadata related to the droppped table should be cleaned up from the master.
  for (int idx = 1; idx < kNumTables; idx++) {
    char drop_table[64] = {0};
    (void)snprintf(drop_table, sizeof(drop_table), "%s_%d", kTableName, idx);
    DropTable(&test_cluster_, drop_table);
  }

  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        while (true) {
          auto get_resp = GetDBStreamInfo(stream_id);
          // Wait until the background thread cleanup up the drop table metadata.
          if (get_resp.ok() && !get_resp->has_error() && get_resp->table_info_size() == 1) {
            return true;
          }
        }
      },
      MonoDelta::FromSeconds(60), "Waiting for stream metadata cleanup."));

  for (int idx = 0; idx < 2; idx++) {
    auto change_resp = GetChangesFromCDC(stream_id, tablets[idx], nullptr);
    // test_table_1 and test_table_2 GetChanges should retrun error where as test_table_3 should
    // succeed.
    if (idx == 0 || idx == 1) {
      ASSERT_FALSE(change_resp.ok());

    } else {
      uint32_t record_size = (*change_resp).cdc_sdk_proto_records_size();
      ASSERT_GT(record_size, 100);
    }
  }

  // Deleting the created stream.
  ASSERT_TRUE(DeleteCDCStream(stream_id));

  // GetChanges should retrun error, for all tables.
  for (int idx = 0; idx < 2; idx++) {
    auto change_resp = GetChangesFromCDC(stream_id, tablets[idx], nullptr);
    ASSERT_FALSE(change_resp.ok());
  }
}

// After delete stream, metadata related to stream should be deleted from the master cache as well
// as system catalog.
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestStreamMetaCleanUpAndDeleteStream)) {
  // Setup cluster.
  ASSERT_OK(SetUpWithParams(3, 1, false));

  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version = */ nullptr));

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  ASSERT_OK(WriteRows(0 /* start */, 100 /* end */, &test_cluster_));

  // Deleting the created DB Stream ID.
  ASSERT_TRUE(DeleteCDCStream(stream_id));

  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        while (true) {
          auto get_resp = GetDBStreamInfo(stream_id);
          // Wait until the background thread cleanup up the stream-id.
          if (get_resp.ok() && get_resp->has_error() && get_resp->table_info_size() == 0) {
            return true;
          }
        }
      },
      MonoDelta::FromSeconds(60), "Waiting for stream metadata cleanup."));
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestDeletedStreamRowRemovedEvenAfterGetChanges)) {
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  FLAGS_update_min_cdc_indices_interval_secs = 60;

  ASSERT_OK(SetUpWithParams(1, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  // Insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));
  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  ASSERT_OK(WriteRowsHelper(100 /* start */, 200 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  ASSERT_EQ(DeleteCDCStream(stream_id), true);
  VerifyStreamCheckpointInCdcState(
      test_client(), stream_id, tablets[0].tablet_id(), OpIdExpectedValue::MaxOpId);
  LOG(INFO) << "The stream's checkpoint has been marked as OpId::Max()";

  ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp_1.cdc_sdk_checkpoint()));
  VerifyStreamCheckpointInCdcState(
      test_client(), stream_id, tablets[0].tablet_id(), OpIdExpectedValue::ValidNonMaxOpId);
  LOG(INFO) << "Verified that GetChanges() overwrote checkpoint from OpId::Max().";

  // We shutdown the TServer so that the stream cache is cleared.
  test_cluster()->mini_tablet_server(0)->Shutdown();
  ASSERT_OK(test_cluster()->mini_tablet_server(0)->Start());
  ASSERT_OK(test_cluster()->mini_tablet_server(0)->WaitStarted());

  // We verify that the row is deleted even after GetChanges() overwrote the OpId from Max.
  VerifyStreamDeletedFromCdcState(test_client(), stream_id, tablets[0].tablet_id());
}

// Here we are creating a table test_table_1 and a CDC stream ex:- stream-id-1.
// Now create another table test_table_2 and create another stream ex:- stream-id-2 on the same
// namespace. stream-id-1 and stream-id-2 are now associated with test_table_1. drop test_table_1,
// call GetDBStreamInfo on both stream-id, we should not get any information related to drop table.
TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestMultiStreamOnSameTableAndDropTable)) {
  // Setup cluster.
  ASSERT_OK(SetUpWithParams(3, 1, false));
  const vector<string> table_list_suffix = {"_1", "_2"};
  vector<YBTableName> table(2);
  vector<CDCStreamId> stream_id(2);
  vector<google::protobuf::RepeatedPtrField<master::TabletLocationsPB>> tablets(2);

  for (int idx = 0; idx < 2; idx++) {
    table[idx] = ASSERT_RESULT(CreateTable(
        &test_cluster_, kNamespaceName, kTableName, 1, true, false, 0, true,
        table_list_suffix[idx]));
    ASSERT_OK(test_client()->GetTablets(
        table[idx], 0, &tablets[idx], /* partition_list_version = */ nullptr));
    TableId table_id = ASSERT_RESULT(
        GetTableId(&test_cluster_, kNamespaceName, kTableName + table_list_suffix[idx]));

    stream_id[idx] = ASSERT_RESULT(CreateDBStream());
    ASSERT_OK(WriteEnumsRows(
        0 /* start */, 100 /* end */, &test_cluster_, table_list_suffix[idx], kNamespaceName,
        kTableName));
  }

  // Drop table test_table_1 which is associated with both streams.
  for (int idx = 1; idx < 2; idx++) {
    char drop_table[64] = {0};
    (void)snprintf(drop_table, sizeof(drop_table), "%s_%d", kTableName, idx);
    DropTable(&test_cluster_, drop_table);
  }

  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        int idx = 1;
        while (idx <= 2) {
          auto get_resp = GetDBStreamInfo(stream_id[idx -1]);
          if (!get_resp.ok()) {
            return false;
          }
          // stream-1 is associated with a single table, so as part of table drop, stream-1 should
          // be cleaned and wait until the background thread is done with cleanup.
          if (idx == 1 && false == get_resp->has_error() && get_resp->table_info_size() > 0) {
            continue;
          }
          // stream-2 is associated with both tables, so dropping one table, should not clean the
          // stream from cache as well as from system catalog, except the dropped table metadata.
          if (idx > 1 && get_resp->table_info_size() > 1) {
            continue;
          }
          idx += 1;
        }
        return true;
      },
      MonoDelta::FromSeconds(60), "Waiting for stream metadata cleanup."));
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestMultiStreamOnSameTableAndDeleteStream)) {
  // Setup cluster.
  ASSERT_OK(SetUpWithParams(3, 1, false));
  const vector<string> table_list_suffix = {"_1", "_2"};
  vector<YBTableName> table(2);
  vector<CDCStreamId> stream_id(2);
  vector<google::protobuf::RepeatedPtrField<master::TabletLocationsPB>> tablets(2);

  for (int idx = 0; idx < 2; idx++) {
    table[idx] = ASSERT_RESULT(CreateTable(
        &test_cluster_, kNamespaceName, kTableName, 1, true, false, 0, true,
        table_list_suffix[idx]));
    ASSERT_OK(test_client()->GetTablets(
        table[idx], 0, &tablets[idx], /* partition_list_version = */ nullptr));
    TableId table_id = ASSERT_RESULT(
        GetTableId(&test_cluster_, kNamespaceName, kTableName + table_list_suffix[idx]));

    stream_id[idx] = ASSERT_RESULT(CreateDBStream());
    ASSERT_OK(WriteEnumsRows(
        0 /* start */, 100 /* end */, &test_cluster_, table_list_suffix[idx], kNamespaceName,
        kTableName));
  }

  // Deleting the stream-2 associated with both tables
  ASSERT_TRUE(DeleteCDCStream(stream_id[1]));

  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        int idx = 1;
        while (idx <= 2) {
          auto get_resp = GetDBStreamInfo(stream_id[idx - 1]);
          if (!get_resp.ok()) {
            return false;
          }
          // stream-1 which is not deleted, so there should not be any cleanup
          // for it.
          if (idx == 1 && get_resp->table_info_size() != 1) {
            continue;
          }
          // stream-2 is deleted, so its metadata from the master cache as well as from the system
          // catalog should be cleaned and wait until the background thread is done with the
          // cleanup.
          if (idx > 1 && (false == get_resp->has_error() || get_resp->table_info_size() != 0)) {
            continue;
          }
          idx += 1;
        }
        return true;
      },
      MonoDelta::FromSeconds(60), "Waiting for stream metadata cleanup."));
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCreateStreamAfterSetCheckpointMax)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  // We want to force every GetChanges to update the cdc_state table.
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;

  ASSERT_OK(SetUpWithParams(1, 1, false));
  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp =
      ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  // Insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  GetChangesResponsePB change_resp;
  change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  ASSERT_GE(record_size, 100);
  LOG(INFO) << "Total records read by GetChanges call on stream_id_1: " << record_size;

  // Forcefully update the checkpoint of the stream as MAX.
  OpId commit_op_id = OpId::Max();
  client::TableHandle cdc_state;
  client::YBTableName cdc_state_table(
      YQL_DATABASE_CQL, master::kSystemNamespaceName, master::kCdcStateTableName);
  ASSERT_OK(cdc_state.Open(cdc_state_table, test_client()));
  const auto op = cdc_state.NewUpdateOp();
  auto* const req = op->mutable_request();
  QLAddStringHashValue(req, tablets[0].tablet_id());
  QLAddStringRangeValue(req, stream_id);
  cdc_state.AddStringColumnValue(req, master::kCdcCheckpoint, commit_op_id.ToString());
  auto* condition = req->mutable_if_expr()->mutable_condition();
  condition->set_op(QL_OP_EXISTS);
  auto session = test_client()->NewSession();
  EXPECT_OK(session->TEST_ApplyAndFlush(op));

  // Now Read the cdc_state table check checkpoint is updated to MAX.
  const auto read_op = cdc_state.NewReadOp();
  auto* const req_read = read_op->mutable_request();
  QLAddStringHashValue(req_read, tablets[0].tablet_id());
  auto req_cond = req->mutable_where_expr()->mutable_condition();
  req_cond->set_op(QLOperator::QL_OP_AND);
  QLAddStringCondition(
      req_cond, Schema::first_column_id() + master::kCdcStreamIdIdx, QL_OP_EQUAL, stream_id);
  cdc_state.AddColumns({master::kCdcCheckpoint}, req_read);

  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        EXPECT_OK(session->TEST_ApplyAndFlush(read_op));
        auto row_block = ql::RowsResult(read_op.get()).GetRowBlock();
        if (row_block->row_count() == 1 &&
            row_block->row(0).column(0).string_value() == OpId::Max().ToString()) {
          return true;
        }
        return false;
      },
      MonoDelta::FromSeconds(60),
      "Failed to read from cdc_state table."));
  VerifyCdcStateMatches(
      test_client(), stream_id, tablets[0].tablet_id(), commit_op_id.term, commit_op_id.index);

  CDCStreamId stream_id_2 = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id_2, tablets));
  ASSERT_FALSE(resp.has_error());
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCDCSDKCacheWithLeaderChange)) {
  // Disable lb as we move tablets around
  FLAGS_enable_load_balancing = false;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  FLAGS_cdc_intent_retention_ms = 10000;
  // FLAGS_cdc_intent_retention_ms = 1000;
  const int num_tservers = 3;
  ASSERT_OK(SetUpWithParams(num_tservers, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  EnableCDCServiceInAllTserver(3);
  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  // Insert some records in transaction.
  ASSERT_OK(WriteRows(0 /* start */, 100 /* end */, &test_cluster_));

  GetChangesResponsePB change_resp;
  change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  ASSERT_GE(record_size, 100);
  LOG(INFO) << "Total records read by GetChanges call on stream_id_1: " << record_size;

  int cache_hit_tservers =
      FindTserversWithCacheHit(stream_id, tablets[0].tablet_id(), num_tservers);
  ASSERT_GE(cache_hit_tservers, 1);

  // change LEADER of the tablet to tserver-2
  ASSERT_OK(ChangeLeaderOfTablet(1, tablets[0].tablet_id()));

  // check the condition of cache after LEADER step down.
  // we will see prev as well as current LEADER cache, search stream exist.
  cache_hit_tservers = FindTserversWithCacheHit(stream_id, tablets[0].tablet_id(), num_tservers);
  ASSERT_GE(cache_hit_tservers, 1);

  // Keep refreshing the stream from the new LEADER, till we cross the
  // FLAGS_cdc_intent_retention_ms.
  int idx = 0;
  while (idx < 10) {
    auto result =
        ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp.cdc_sdk_checkpoint()));
    idx += 1;
    SleepFor(MonoDelta::FromMilliseconds(100));
  }

  // change LEADER of the tablet to tserver-1
  ASSERT_OK(ChangeLeaderOfTablet(0, tablets[0].tablet_id()));

  auto result = GetChangesFromCDC(stream_id, tablets, &change_resp.cdc_sdk_checkpoint());
  ASSERT_OK(result);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCDCSDKCacheWithLeaderReElect)) {
  // Disable lb as we move tablets around
  FLAGS_enable_load_balancing = false;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_update_metrics_interval_ms = 1000;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  const int num_tservers = 3;
  ASSERT_OK(SetUpWithParams(num_tservers, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  // Insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  GetChangesResponsePB change_resp;
  change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  ASSERT_GE(record_size, 100);
  LOG(INFO) << "Total records read by GetChanges call on stream_id_1: " << record_size;
  SleepFor(MonoDelta::FromSeconds(1));
  size_t first_leader_index = 0;
  size_t first_follower_index = 0;
  GetTabletLeaderAndAnyFollowerIndex(tablets, &first_leader_index, &first_follower_index);

  GetChangesResponsePB change_resp_1 = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  LOG(INFO) << "Number of records after first transaction: " << change_resp_1.records().size();

  ASSERT_OK(ChangeLeaderOfTablet(first_follower_index, tablets[0].tablet_id()));

  size_t second_leader_index = -1;
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets2;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets2, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  for (auto replica : tablets2[0].replicas()) {
    if (replica.role() == PeerRole::LEADER) {
      for (size_t i = 0; i < test_cluster()->num_tablet_servers(); i++) {
        if (test_cluster()->mini_tablet_server(i)->server()->permanent_uuid() ==
            replica.ts_info().permanent_uuid()) {
          second_leader_index = i;
          LOG(INFO) << "Found second leader index: " << i;
          break;
        }
      }
    }
  }

  // Insert some records in transaction after first leader stepdown.
  ASSERT_OK(WriteRowsHelper(100 /* start */, 200 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  // Call GetChanges so that the last active time is updated on the new leader.
  auto result = GetChangesFromCDC(stream_id, tablets2, &change_resp.cdc_sdk_checkpoint());

  SleepFor(MonoDelta::FromSeconds(2));
  CoarseTimePoint correct_expiry_time;
  for (auto const& peer : test_cluster()->GetTabletPeers(second_leader_index)) {
    if (peer->tablet_id() == tablets2[0].tablet_id()) {
      correct_expiry_time = peer->cdc_sdk_min_checkpoint_op_id_expiration();
      break;
    }
  }
  LOG(INFO) << "The correct expiry time after the final GetChanges call: "
            << correct_expiry_time.time_since_epoch().count();

  // we need to ensure the initial leader get's back leadership
  ASSERT_OK(ChangeLeaderOfTablet(first_follower_index, tablets[0].tablet_id()));
  LOG(INFO) << "Changed leadership back to the first leader TServer";

  // Call the test RPC to get last active time of the current leader (original), and it should
  // be lower than the previously recorded last_active_time.
  CompareExpirationTime(tablets2[0].tablet_id(), correct_expiry_time, first_leader_index);
  LOG(INFO) << "Succesfully compared expiry times";
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCDCSDKCacheWithLeaderRestart)) {
  // Disable lb as we move tablets around
  FLAGS_enable_load_balancing = false;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  const int num_tservers = 3;
  ASSERT_OK(SetUpWithParams(num_tservers, 1, false));

  // RF: 3, num of tservers: 4.
  for (int i = 0; i < 1; ++i) {
    ASSERT_OK(test_cluster()->AddTabletServer());
    ASSERT_OK(test_cluster()->WaitForAllTabletServers());
    LOG(INFO) << "Added new TServer to test cluster";
  }

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  size_t first_leader_index = 0;
  size_t first_follower_index = 0;
  GetTabletLeaderAndAnyFollowerIndex(tablets, &first_leader_index, &first_follower_index);

  // Insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  GetChangesResponsePB change_resp;
  change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  ASSERT_GE(record_size, 100);
  LOG(INFO) << "Total records read by GetChanges call on stream_id_1: " << record_size;
  SleepFor(MonoDelta::FromSeconds(10));

  ASSERT_OK(ChangeLeaderOfTablet(first_follower_index, tablets[0].tablet_id()));

  // Shutdown tserver hosting tablet leader.
  test_cluster()->mini_tablet_server(first_leader_index)->Shutdown();
  LOG(INFO) << "TServer hosting tablet leader shutdown";
  SleepFor(MonoDelta::FromSeconds(10));

  size_t second_leader_index = -1;
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets2;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets2, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  for (auto replica : tablets2[0].replicas()) {
    if (replica.role() == PeerRole::LEADER) {
      for (size_t i = 0; i < test_cluster()->num_tablet_servers(); i++) {
        if (i == first_leader_index) continue;
        if (test_cluster()->mini_tablet_server(i)->server()->permanent_uuid() ==
            replica.ts_info().permanent_uuid()) {
          second_leader_index = i;
          LOG(INFO) << "Found second leader index: " << i;
          break;
        }
      }
    }
    if (replica.role() == PeerRole::FOLLOWER) {
      for (size_t i = 0; i < test_cluster()->num_tablet_servers(); i++) {
        if (i == first_leader_index) continue;
        if (test_cluster()->mini_tablet_server(i)->server()->permanent_uuid() ==
            replica.ts_info().permanent_uuid()) {
          LOG(INFO) << "Found second follower index: " << i;
          break;
        }
      }
    }
  }

  // restart the initial leader tserver
  ASSERT_OK(test_cluster()->mini_tablet_server(first_leader_index)->Start());
  ASSERT_OK(test_cluster()->mini_tablet_server(first_leader_index)->WaitStarted());

  // Insert some records in transaction after leader shutdown.
  ASSERT_OK(WriteRowsHelper(100 /* start */, 200 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  // Call GetChanges so that the last active time is updated on the new leader.
  GetChangesResponsePB prev_change_resp = change_resp;
  change_resp =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets2, &prev_change_resp.cdc_sdk_checkpoint()));
  record_size = change_resp.cdc_sdk_proto_records_size();
  ASSERT_GE(record_size, 100);

  SleepFor(MonoDelta::FromSeconds(2));
  CoarseTimePoint correct_expiry_time;
  for (auto const& peer : test_cluster()->GetTabletPeers(second_leader_index)) {
    if (peer->tablet_id() == tablets2[0].tablet_id()) {
      correct_expiry_time = peer->cdc_sdk_min_checkpoint_op_id_expiration();
    }
  }
  LOG(INFO) << "CDKSDK checkpoint expiration time with LEADER tserver:"
            << second_leader_index << " : "
            << correct_expiry_time.time_since_epoch().count();

  // We need to ensure the initial leader get's back leadership.
  ASSERT_OK(ChangeLeaderOfTablet(first_leader_index, tablets[0].tablet_id()));

  ASSERT_OK(WriteRowsHelper(200 /* start */, 300 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  // Call GetChanges so that the last active time is updated on the new leader.
  prev_change_resp = change_resp;
  change_resp =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets2, &prev_change_resp.cdc_sdk_checkpoint()));
  record_size = change_resp.cdc_sdk_proto_records_size();
  ASSERT_GE(record_size, 100);

  // Call the test RPC to get last active time of the current leader (original), and it will
  // be lower than the previously recorded last_active_time.
  CompareExpirationTime(tablets2[0].tablet_id(), correct_expiry_time, first_leader_index);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCDCSDKActiveTimeCacheInSyncWithCDCStateTable)) {
  // Disable lb as we move tablets around
  FLAGS_enable_load_balancing = false;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_update_metrics_interval_ms = 1000;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  const int num_tservers = 3;
  ASSERT_OK(SetUpWithParams(num_tservers, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  // Insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  GetChangesResponsePB change_resp;
  change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  ASSERT_GE(record_size, 100);
  LOG(INFO) << "Total records read by GetChanges call on stream_id_1: " << record_size;

  size_t first_leader_index = -1;
  size_t first_follower_index = -1;
  GetTabletLeaderAndAnyFollowerIndex(tablets, &first_leader_index, &first_follower_index);

  GetChangesResponsePB change_resp_1 =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp.cdc_sdk_checkpoint()));
  LOG(INFO) << "Number of records after first transaction: " << change_resp_1.records().size();

  const auto& first_leader_tserver =
      test_cluster()->mini_tablet_server(first_leader_index)->server();
  auto cdc_service = dynamic_cast<CDCServiceImpl*>(first_leader_tserver->rpc_server()
                                                       ->TEST_service_pool("yb.cdc.CDCService")
                                                       ->TEST_get_service()
                                                       .get());
  auto tablet_info = ASSERT_RESULT(
      cdc_service->TEST_GetTabletInfoFromCache({"", stream_id, tablets[0].tablet_id()}));
  auto first_last_active_time = tablet_info.last_active_time;
  auto last_active_time_from_table = ASSERT_RESULT(
      GetLastActiveTimeFromCdcStateTable(stream_id, tablets[0].tablet_id(), test_client()));
  // Now check the active time in CDCSTate table, it should be greater than or equal to the
  // last_active_time from the cache.
  ASSERT_GE(last_active_time_from_table, first_last_active_time);
  LOG(INFO) << "The active time is equal in both the cache and cdc_state table";

  const size_t& second_leader_index = first_follower_index;
  ASSERT_OK(ChangeLeaderOfTablet(second_leader_index, tablets[0].tablet_id()));

  // Insert some records in transaction after first leader stepdown.
  ASSERT_OK(WriteRowsHelper(100 /* start */, 200 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  // Call GetChanges so that the last active time is updated on the new leader.
  auto result = GetChangesFromCDC(stream_id, tablets, &change_resp.cdc_sdk_checkpoint());

  const auto& second_leader_tserver =
      test_cluster()->mini_tablet_server(second_leader_index)->server();
  cdc_service = dynamic_cast<CDCServiceImpl*>(second_leader_tserver->rpc_server()
                                                  ->TEST_service_pool("yb.cdc.CDCService")
                                                  ->TEST_get_service()
                                                  .get());
  tablet_info = ASSERT_RESULT(
      cdc_service->TEST_GetTabletInfoFromCache({"", stream_id, tablets[0].tablet_id()}));
  auto second_last_active_time = tablet_info.last_active_time;

  last_active_time_from_table = ASSERT_RESULT(
      GetLastActiveTimeFromCdcStateTable(stream_id, tablets[0].tablet_id(), test_client()));
  ASSERT_GE(last_active_time_from_table, second_last_active_time);
  LOG(INFO) << "The active time is equal in both the cache and cdc_state table";
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCDCSDKCacheWhenAFollowerIsUnavailable)) {
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_update_metrics_interval_ms = 500;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  const int num_tservers = 5;
  ASSERT_OK(SetUpWithParams(num_tservers, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  size_t first_leader_index = 0;
  size_t first_follower_index = 0;
  GetTabletLeaderAndAnyFollowerIndex(tablets, &first_leader_index, &first_follower_index);

  SleepFor(MonoDelta::FromSeconds(2));

  // Insert some records in transaction.
  ASSERT_OK(WriteRowsHelper(0 /* start */, 100 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  GetChangesResponsePB change_resp;
  change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  ASSERT_GE(record_size, 100);
  LOG(INFO) << "Total records read by GetChanges call on stream_id_1: " << record_size;
  SleepFor(MonoDelta::FromSeconds(10));

  // Insert some records in transaction after leader shutdown.
  ASSERT_OK(WriteRowsHelper(100 /* start */, 200 /* end */, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  auto result = GetChangesFromCDC(stream_id, tablets, &change_resp.cdc_sdk_checkpoint());

  CoarseTimePoint first_expiry_time;
  for (auto const& peer : test_cluster()->GetTabletPeers(first_leader_index)) {
    if (peer->tablet_id() == tablets[0].tablet_id()) {
      first_expiry_time = peer->cdc_sdk_min_checkpoint_op_id_expiration();
    }
  }
  LOG(INFO) << "The expiry time after the first GetChanges call: "
            << first_expiry_time.time_since_epoch().count();

  // Shutdown tserver having tablet FOLLOWER.
  test_cluster()->mini_tablet_server(first_follower_index)->Shutdown();
  LOG(INFO) << "TServer hosting tablet follower shutdown";
  // Call GetChanges so that the last active time is updated on the new leader.
  result = GetChangesFromCDC(stream_id, tablets, &change_resp.cdc_sdk_checkpoint());

  // Call the test RPC to get last active time of the current leader (original), and it must
  // be greater than or equal to the previously recorded last_active_time.
  CompareExpirationTime(tablets[0].tablet_id(), first_expiry_time, first_leader_index, true);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestColocation)) {
  FLAGS_enable_update_local_peer_min_index = false;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  ASSERT_OK(SetUpWithParams(3, 1, false));

  ASSERT_OK(CreateColocatedObjects(&test_cluster_));
  auto table = ASSERT_RESULT(GetTable(&test_cluster_, kNamespaceName, "test1"));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), 1);

  std::string table_id = table.table_id();
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  int insert_count = 30;
  ASSERT_OK(PopulateColocatedData(&test_cluster_, insert_count));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  // Call get changes.
  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  ASSERT_GT(record_size, insert_count);

  int expected_key1 = 0;
  int expected_key2 = 0;
  int ddl_count = 0;
  std::unordered_set<string> ddl_tables;
  for (uint32_t i = 0; i < record_size; ++i) {
    const auto record = change_resp.cdc_sdk_proto_records(i);
    if (record.row_message().op() == RowMessage::INSERT) {
      if (record.row_message().table() == "test1") {
        ASSERT_EQ(expected_key1, record.row_message().new_tuple(0).datum_int32());
        expected_key1++;
      } else if (record.row_message().table() == "test2") {
        ASSERT_EQ(std::to_string(expected_key2), record.row_message().new_tuple(0).datum_string());
        expected_key2++;
      }
    } else if (record.row_message().op() == RowMessage::DDL) {
      ddl_tables.insert(record.row_message().table());
      ddl_count++;
    }
  }

  ASSERT_TRUE(ddl_tables.contains("test1"));
  ASSERT_TRUE(ddl_tables.contains("test2"));

  ASSERT_EQ(insert_count, expected_key1);
  ASSERT_EQ(insert_count, expected_key2);
  ASSERT_EQ(ddl_count, 3);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestIntentsInColocation)) {
  FLAGS_enable_update_local_peer_min_index = false;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 1;
  ASSERT_OK(SetUpWithParams(3, 1, false));

  ASSERT_OK(CreateColocatedObjects(&test_cluster_));
  auto table = ASSERT_RESULT(GetTable(&test_cluster_, kNamespaceName, "test1"));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), 1);

  std::string table_id = table.table_id();
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  int insert_count = 30;
  ASSERT_OK(PopulateColocatedData(&test_cluster_, insert_count, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  // Call get changes.
  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  ASSERT_GT(record_size, insert_count);

  int expected_key1 = 0;
  int expected_key2 = 0;
  for (uint32_t i = 0; i < record_size; ++i) {
    const auto record = change_resp.cdc_sdk_proto_records(i);
    LOG(INFO) << "Record found: " << record.ShortDebugString();
    if (record.row_message().op() == RowMessage::INSERT) {
      if (record.row_message().table() == "test1") {
        ASSERT_EQ(expected_key1, record.row_message().new_tuple(0).datum_int32());
        expected_key1++;
      } else if (record.row_message().table() == "test2") {
        ASSERT_EQ(std::to_string(expected_key2), record.row_message().new_tuple(0).datum_string());
        expected_key2++;
      }
    }
  }

  ASSERT_EQ(insert_count, expected_key1);
  ASSERT_EQ(insert_count, expected_key2);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCDCSDKLagMetrics)) {
  FLAGS_update_metrics_interval_ms = 1;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  ASSERT_OK(SetUpWithParams(1, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  vector<CDCStreamId> stream_id(2);
  for (int idx = 0; idx < 2; idx++) {
    stream_id[idx] = ASSERT_RESULT(CreateDBStream(IMPLICIT));
  }

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id[0], tablets));
  ASSERT_FALSE(resp.has_error());

  const auto& tserver = test_cluster()->mini_tablet_server(0)->server();
  auto cdc_service = dynamic_cast<CDCServiceImpl*>(
      tserver->rpc_server()->TEST_service_pool("yb.cdc.CDCService")->TEST_get_service().get());

  ASSERT_OK(WaitFor(
      [&]() { return cdc_service->CDCEnabled(); }, MonoDelta::FromSeconds(30), "IsCDCEnabled"));
  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        auto metrics =
            std::static_pointer_cast<cdc::CDCSDKTabletMetrics>(cdc_service->GetCDCTabletMetrics(
                {"" /* UUID */, stream_id[0], tablets[0].tablet_id()}, nullptr, CDCSDK));
        return metrics->cdcsdk_sent_lag_micros->value() == 0;
      },
      MonoDelta::FromSeconds(10) * kTimeMultiplier, "Wait for Lag == 0"));
  // Insert test rows, one at a time so they have different hybrid times.
  ASSERT_OK(WriteRowsHelper(0, 1, &test_cluster_, true));
  ASSERT_OK(WriteRowsHelper(1, 2, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  // Call get changes.
  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id[0], tablets));
  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  ASSERT_GT(record_size, 2);
  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        auto metrics =
            std::static_pointer_cast<cdc::CDCSDKTabletMetrics>(cdc_service->GetCDCTabletMetrics(
                {"" /* UUID */, stream_id[0], tablets[0].tablet_id()}, nullptr, CDCSDK));
        return metrics->cdcsdk_sent_lag_micros->value() > 0;
      },
      MonoDelta::FromSeconds(10) * kTimeMultiplier, "Wait for Lag > 0"));

  // Now, delete the CDC stream and check the metrics information for the tablet_id and stream_id
  // combination should be deleted from the cdc metrics map.
  ASSERT_EQ(DeleteCDCStream(stream_id[0]), true);
  VerifyStreamDeletedFromCdcState(test_client(), stream_id[0], tablets.Get(0).tablet_id());
  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        auto metrics =
            std::static_pointer_cast<cdc::CDCSDKTabletMetrics>(cdc_service->GetCDCTabletMetrics(
                {"" /* UUID */, stream_id[0], tablets[0].tablet_id()},
                /* tablet_peer */ nullptr, CDCSDK, CreateCDCMetricsEntity::kFalse));
        return metrics == nullptr;
      },
      MonoDelta::FromSeconds(10) * kTimeMultiplier, "Wait for tablet metrics entry remove."));
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCDCSDKExpiryMetric)) {
  FLAGS_update_metrics_interval_ms = 1;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  ASSERT_OK(SetUpWithParams(1, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id;
  stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  const auto& tserver = test_cluster()->mini_tablet_server(0)->server();
  auto cdc_service = dynamic_cast<CDCServiceImpl*>(
      tserver->rpc_server()->TEST_service_pool("yb.cdc.CDCService")->TEST_get_service().get());
  ASSERT_OK(WriteRowsHelper(1, 100, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  // Call get changes.
  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  ASSERT_GT(record_size, 100);
  auto metrics =
      std::static_pointer_cast<cdc::CDCSDKTabletMetrics>(cdc_service->GetCDCTabletMetrics(
          {"" /* UUID */, stream_id, tablets[0].tablet_id()},
          /* tablet_peer */ nullptr, CDCSDK, CreateCDCMetricsEntity::kFalse));
  uint64_t current_stream_expiry_time = metrics->cdcsdk_expiry_time_ms->value();
  LOG(INFO) << "stream expiry time in milli seconds after GetChanges call: "
            << current_stream_expiry_time;
  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        auto metrics =
            std::static_pointer_cast<cdc::CDCSDKTabletMetrics>(cdc_service->GetCDCTabletMetrics(
                {"" /* UUID */, stream_id, tablets[0].tablet_id()}, nullptr, CDCSDK));
        return current_stream_expiry_time > metrics->cdcsdk_expiry_time_ms->value();
      },
      MonoDelta::FromSeconds(10) * kTimeMultiplier, "Wait for stream expiry time update."));
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCDCSDKTrafficSentMetric)) {
  FLAGS_update_metrics_interval_ms = 1;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  ASSERT_OK(SetUpWithParams(1, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id;
  stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  const auto& tserver = test_cluster()->mini_tablet_server(0)->server();
  auto cdc_service = dynamic_cast<CDCServiceImpl*>(
      tserver->rpc_server()->TEST_service_pool("yb.cdc.CDCService")->TEST_get_service().get());
  ASSERT_OK(WriteRowsHelper(1, 100, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  // Call get changes.
  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  ASSERT_GT(record_size, 100);
  auto metrics =
      std::static_pointer_cast<cdc::CDCSDKTabletMetrics>(cdc_service->GetCDCTabletMetrics(
          {"" /* UUID */, stream_id, tablets[0].tablet_id()},
          /* tablet_peer */ nullptr, CDCSDK, CreateCDCMetricsEntity::kFalse));
  int64_t current_traffic_sent_bytes = metrics->cdcsdk_traffic_sent->value();

  // Isnert few more records
  ASSERT_OK(WriteRowsHelper(101, 200, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  // Call get changes.
  GetChangesResponsePB new_change_resp =
      ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets, &change_resp.cdc_sdk_checkpoint()));
  record_size = new_change_resp.cdc_sdk_proto_records_size();
  ASSERT_GT(record_size, 100);

  LOG(INFO) << "Traffic sent in bytes after GetChanges call: " << current_traffic_sent_bytes;
  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        auto metrics =
            std::static_pointer_cast<cdc::CDCSDKTabletMetrics>(cdc_service->GetCDCTabletMetrics(
                {"" /* UUID */, stream_id, tablets[0].tablet_id()}, nullptr, CDCSDK));
        return current_traffic_sent_bytes < metrics->cdcsdk_traffic_sent->value();
      },
      MonoDelta::FromSeconds(10) * kTimeMultiplier,
      "Wait for CDCSDK traffic sent attribute update."));
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCDCSDKChangeEventCountMetric)) {
  FLAGS_update_metrics_interval_ms = 1;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  ASSERT_OK(SetUpWithParams(1, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id;
  stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  const auto& tserver = test_cluster()->mini_tablet_server(0)->server();
  auto cdc_service = dynamic_cast<CDCServiceImpl*>(
      tserver->rpc_server()->TEST_service_pool("yb.cdc.CDCService")->TEST_get_service().get());
  ASSERT_OK(WriteRowsHelper(1, 100, &test_cluster_, true));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  // Call get changes.
  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));
  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  ASSERT_GT(record_size, 100);
  auto metrics =
      std::static_pointer_cast<cdc::CDCSDKTabletMetrics>(cdc_service->GetCDCTabletMetrics(
          {"" /* UUID */, stream_id, tablets[0].tablet_id()},
          /* tablet_peer */ nullptr, CDCSDK, CreateCDCMetricsEntity::kFalse));
  LOG(INFO) << "Total event counts after GetChanges call: "
            << metrics->cdcsdk_change_event_count->value();
  ASSERT_GT(metrics->cdcsdk_change_event_count->value(), 100);
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCDCSDKMetricsTwoTablesSingleStream)) {
  FLAGS_update_metrics_interval_ms = 1;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  ASSERT_OK(SetUpWithParams(1, 1, false));

  const uint32_t num_tablets = 1;
  const uint32_t num_tables = 2;
  vector<string> table_suffix = {"_1", "_2"};

  vector<YBTableName> table(num_tables);
  vector<google::protobuf::RepeatedPtrField<master::TabletLocationsPB>> tablets(num_tables);
  vector<TableId> table_id(num_tables);

  for (uint32_t idx = 0; idx < num_tables; idx++) {
    table[idx] = ASSERT_RESULT(
        CreateTable(&test_cluster_, kNamespaceName, kTableName + table_suffix[idx], num_tablets));

    ASSERT_OK(test_client()->GetTablets(
        table[idx], 0, &tablets[idx],
        /* partition_list_version =*/nullptr));
    ASSERT_EQ(tablets[idx].size(), num_tablets);

    table_id[idx] =
        ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName + table_suffix[idx]));
  }

  CDCStreamId stream_id;
  stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  for (auto tablet : tablets) {
    auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablet));
    ASSERT_FALSE(resp.has_error());
  }

  const auto& tserver = test_cluster()->mini_tablet_server(0)->server();
  auto cdc_service = dynamic_cast<CDCServiceImpl*>(
      tserver->rpc_server()->TEST_service_pool("yb.cdc.CDCService")->TEST_get_service().get());

  int64_t current_traffic_sent_bytes = 0;

  for (uint32_t idx = 0; idx < num_tables; idx++) {
    ASSERT_OK(
        WriteRowsHelper(1, 50, &test_cluster_, true, (kTableName + table_suffix[idx]).c_str()));
    ASSERT_OK(test_client()->FlushTables(
        {table[idx].table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
        /* is_compaction = */ false));
  }

  vector<GetChangesResponsePB> change_resp(num_tables);
  vector<std::shared_ptr<cdc::CDCSDKTabletMetrics>> metrics(num_tables);
  uint32_t total_record_size = 0;
  int64_t total_traffic_sent = 0;
  uint64_t total_change_event_count = 0;

  for (uint32_t idx = 0; idx < num_tables; idx++) {
    change_resp[idx] = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets[idx]));
    total_record_size += change_resp[idx].cdc_sdk_proto_records_size();

    metrics[idx] =
        std::static_pointer_cast<cdc::CDCSDKTabletMetrics>(cdc_service->GetCDCTabletMetrics(
            {"" /* UUID */, stream_id, tablets[idx][0].tablet_id()},
            /* tablet_peer */ nullptr, CDCSDK, CreateCDCMetricsEntity::kFalse));
    total_traffic_sent += metrics[idx]->cdcsdk_traffic_sent->value();
    total_change_event_count += metrics[idx]->cdcsdk_change_event_count->value();

    auto current_expiry_time = metrics[idx]->cdcsdk_expiry_time_ms->value();
    ASSERT_OK(WaitFor(
        [&]() -> Result<bool> {
          auto metrics =
              std::static_pointer_cast<cdc::CDCSDKTabletMetrics>(cdc_service->GetCDCTabletMetrics(
                  {"" /* UUID */, stream_id, tablets[idx][0].tablet_id()}, nullptr, CDCSDK));
          return current_expiry_time > metrics->cdcsdk_expiry_time_ms->value();
        },
        MonoDelta::FromSeconds(10) * kTimeMultiplier, "Wait for stream expiry time update."));
  }

  ASSERT_GT(total_record_size, 100);
  ASSERT_GT(total_change_event_count, 100);
  ASSERT_TRUE(current_traffic_sent_bytes < total_traffic_sent);
}

TEST_F(
    CDCSDKYsqlTest,
    YB_DISABLE_TEST_IN_TSAN(TestCDCSDKMetricsTwoTablesTwoStreamsOnIndividualTables)) {
  FLAGS_update_metrics_interval_ms = 1;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  ASSERT_OK(SetUpWithParams(1, 1, false));

  const uint32_t num_tablets = 1;
  const uint32_t num_tables = 2;
  const uint32_t num_streams = 2;
  string underscore = "_";

  vector<YBTableName> table(num_tables);
  vector<google::protobuf::RepeatedPtrField<master::TabletLocationsPB>> tablets(num_tables);
  vector<TableId> table_id(num_tables);
  vector<CDCStreamId> stream_id(num_streams);

  for (uint32_t idx = 0; idx < num_tables; idx++) {
    table[idx] = ASSERT_RESULT(CreateTable(
        &test_cluster_, kNamespaceName, kTableName + underscore + std::to_string(idx),
        num_tablets));
    ASSERT_OK(test_client()->GetTablets(
        table[idx], 0, &tablets[idx], /* partition_list_version =*/nullptr));
    ASSERT_EQ(tablets[idx].size(), num_tablets);

    table_id[idx] = ASSERT_RESULT(
        GetTableId(&test_cluster_, kNamespaceName, kTableName + underscore + std::to_string(idx)));
    stream_id[idx] = ASSERT_RESULT(CreateDBStream(IMPLICIT));
    auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id[idx], tablets[idx]));
    ASSERT_FALSE(resp.has_error());
  }
  const auto& tserver = test_cluster()->mini_tablet_server(0)->server();
  auto cdc_service = dynamic_cast<CDCServiceImpl*>(
      tserver->rpc_server()->TEST_service_pool("yb.cdc.CDCService")->TEST_get_service().get());

  for (uint32_t idx = 0; idx < num_tables; idx++) {
    int64_t current_traffic_sent_bytes = 0;
    ASSERT_OK(WriteRowsHelper(
        1, 100, &test_cluster_, true, (kTableName + underscore + std::to_string(idx)).c_str()));
    ASSERT_OK(test_client()->FlushTables(
        {table[idx].table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
        /* is_compaction = */ false));
    GetChangesResponsePB change_resp =
        ASSERT_RESULT(GetChangesFromCDC(stream_id[idx], tablets[idx]));

    uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
    ASSERT_GT(record_size, 100);

    auto metrics =
        std::static_pointer_cast<cdc::CDCSDKTabletMetrics>(cdc_service->GetCDCTabletMetrics(
            {"" /* UUID */, stream_id[idx], tablets[idx][0].tablet_id()},
            /* tablet_peer */ nullptr, CDCSDK, CreateCDCMetricsEntity::kFalse));

    auto current_expiry_time = metrics->cdcsdk_expiry_time_ms->value();
    ASSERT_OK(WaitFor(
        [&]() -> Result<bool> {
          auto metrics =
              std::static_pointer_cast<cdc::CDCSDKTabletMetrics>(cdc_service->GetCDCTabletMetrics(
                  {"" /* UUID */, stream_id[idx], tablets[idx][0].tablet_id()}, nullptr, CDCSDK));
          return current_expiry_time > metrics->cdcsdk_expiry_time_ms->value();
        },
        MonoDelta::FromSeconds(10) * kTimeMultiplier, "Wait for stream expiry time update."));

    ASSERT_GT(metrics->cdcsdk_change_event_count->value(), 100);
    ASSERT_TRUE(current_traffic_sent_bytes < metrics->cdcsdk_traffic_sent->value());
  }
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCDCSDKMetricsTwoTablesTwoStreamsOnBothTables)) {
  FLAGS_update_metrics_interval_ms = 1;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  ASSERT_OK(SetUpWithParams(1, 1, false));

  const uint32_t num_tablets = 1;
  const uint32_t num_tables = 2;
  const uint32_t num_streams = 2;
  string underscore = "_";

  vector<YBTableName> table(num_tables);
  vector<google::protobuf::RepeatedPtrField<master::TabletLocationsPB>> tablets(num_tables);
  vector<TableId> table_id(num_tables);
  vector<CDCStreamId> stream_id(num_streams);

  for (uint32_t idx = 0; idx < num_tables; idx++) {
    table[idx] = ASSERT_RESULT(CreateTable(
        &test_cluster_, kNamespaceName, kTableName + underscore + std::to_string(idx),
        num_tablets));
    ASSERT_OK(test_client()->GetTablets(
        table[idx], 0, &tablets[idx], /* partition_list_version =*/nullptr));
    ASSERT_EQ(tablets[idx].size(), num_tablets);

    table_id[idx] = ASSERT_RESULT(
        GetTableId(&test_cluster_, kNamespaceName, kTableName + underscore + std::to_string(idx)));
  }

  for (uint32_t idx = 0; idx < num_streams; idx++) {
    stream_id[idx] = ASSERT_RESULT(CreateDBStream(IMPLICIT));
    for (auto tablet : tablets) {
      auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id[idx], tablet));
      ASSERT_FALSE(resp.has_error());
    }
  }
  const auto& tserver = test_cluster()->mini_tablet_server(0)->server();
  auto cdc_service = dynamic_cast<CDCServiceImpl*>(
      tserver->rpc_server()->TEST_service_pool("yb.cdc.CDCService")->TEST_get_service().get());

  for (uint32_t idx = 0; idx < num_tables; idx++) {
    int64_t current_traffic_sent_bytes = 0;
    ASSERT_OK(WriteRowsHelper(
        1, 100, &test_cluster_, true, (kTableName + underscore + std::to_string(idx)).c_str()));
    ASSERT_OK(test_client()->FlushTables(
        {table[idx].table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
        /* is_compaction = */ false));

    for (uint32_t stream_idx = 0; stream_idx < num_streams; stream_idx++) {
      GetChangesResponsePB change_resp =
          ASSERT_RESULT(GetChangesFromCDC(stream_id[stream_idx], tablets[idx]));
      uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
      ASSERT_GT(record_size, 100);

      auto metrics =
          std::static_pointer_cast<cdc::CDCSDKTabletMetrics>(cdc_service->GetCDCTabletMetrics(
              {"" /* UUID */, stream_id[stream_idx], tablets[idx][0].tablet_id()},
              /* tablet_peer */ nullptr, CDCSDK, CreateCDCMetricsEntity::kFalse));
      auto current_expiry_time = metrics->cdcsdk_expiry_time_ms->value();
      ASSERT_OK(WaitFor(
          [&]() -> Result<bool> {
            auto metrics =
                std::static_pointer_cast<cdc::CDCSDKTabletMetrics>(cdc_service->GetCDCTabletMetrics(
                    {"" /* UUID */, stream_id[idx], tablets[idx][0].tablet_id()}, nullptr, CDCSDK));
            return current_expiry_time > metrics->cdcsdk_expiry_time_ms->value();
          },
          MonoDelta::FromSeconds(10) * kTimeMultiplier, "Wait for stream expiry time update."));
      ASSERT_GT(metrics->cdcsdk_change_event_count->value(), 100);
      ASSERT_TRUE(current_traffic_sent_bytes < metrics->cdcsdk_traffic_sent->value());
    }
  }
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCDCSDKMetricsWithAddStream)) {
  FLAGS_update_metrics_interval_ms = 1;
  FLAGS_update_min_cdc_indices_interval_secs = 1;
  FLAGS_cdc_state_checkpoint_update_interval_ms = 0;
  ASSERT_OK(SetUpWithParams(1, 1, false));

  const uint32_t num_tablets = 1;
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName, num_tablets));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, /* partition_list_version =*/nullptr));
  ASSERT_EQ(tablets.size(), num_tablets);

  TableId table_id = ASSERT_RESULT(GetTableId(&test_cluster_, kNamespaceName, kTableName));
  CDCStreamId stream_id;
  stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets));
  ASSERT_FALSE(resp.has_error());

  const auto& tserver = test_cluster()->mini_tablet_server(0)->server();
  auto cdc_service = dynamic_cast<CDCServiceImpl*>(
      tserver->rpc_server()->TEST_service_pool("yb.cdc.CDCService")->TEST_get_service().get());

  int64_t current_traffic_sent_bytes = 0;

  ASSERT_OK(WriteRowsHelper(1, 100, &test_cluster_, true, kTableName));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  GetChangesResponsePB change_resp = ASSERT_RESULT(GetChangesFromCDC(stream_id, tablets));

  uint32_t record_size = change_resp.cdc_sdk_proto_records_size();
  ASSERT_GT(record_size, 100);

  auto metrics =
      std::static_pointer_cast<cdc::CDCSDKTabletMetrics>(cdc_service->GetCDCTabletMetrics(
          {"" /* UUID */, stream_id, tablets[0].tablet_id()},
          /* tablet_peer */ nullptr, CDCSDK, CreateCDCMetricsEntity::kFalse));

  auto current_expiry_time = metrics->cdcsdk_expiry_time_ms->value();
  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        auto metrics =
            std::static_pointer_cast<cdc::CDCSDKTabletMetrics>(cdc_service->GetCDCTabletMetrics(
                {"" /* UUID */, stream_id, tablets[0].tablet_id()}, nullptr, CDCSDK));
        return current_expiry_time > metrics->cdcsdk_expiry_time_ms->value();
      },
      MonoDelta::FromSeconds(10) * kTimeMultiplier, "Wait for stream expiry time update."));

  ASSERT_GT(metrics->cdcsdk_change_event_count->value(), 100);
  ASSERT_TRUE(current_traffic_sent_bytes < metrics->cdcsdk_traffic_sent->value());

  // Create a new stream
  CDCStreamId new_stream_id;
  new_stream_id = ASSERT_RESULT(CreateDBStream(IMPLICIT));

  auto new_resp = ASSERT_RESULT(SetCDCCheckpoint(new_stream_id, tablets));
  ASSERT_FALSE(new_resp.has_error());

  current_traffic_sent_bytes = metrics->cdcsdk_traffic_sent->value();

  ASSERT_OK(WriteRowsHelper(101, 200, &test_cluster_, true, kTableName));
  ASSERT_OK(test_client()->FlushTables(
      {table.table_id()}, /* add_indexes = */ false, /* timeout_secs = */ 30,
      /* is_compaction = */ false));

  GetChangesResponsePB new_change_resp = ASSERT_RESULT(GetChangesFromCDC(new_stream_id, tablets));

  record_size = new_change_resp.cdc_sdk_proto_records_size();
  ASSERT_GT(record_size, 100);

  auto new_metrics =
      std::static_pointer_cast<cdc::CDCSDKTabletMetrics>(cdc_service->GetCDCTabletMetrics(
          {"" /* UUID */, new_stream_id, tablets[0].tablet_id()},
          /* tablet_peer */ nullptr, CDCSDK, CreateCDCMetricsEntity::kFalse));

  current_expiry_time = new_metrics->cdcsdk_expiry_time_ms->value();
  ASSERT_OK(WaitFor(
      [&]() -> Result<bool> {
        auto metrics =
            std::static_pointer_cast<cdc::CDCSDKTabletMetrics>(cdc_service->GetCDCTabletMetrics(
                {"" /* UUID */, new_stream_id, tablets[0].tablet_id()}, nullptr, CDCSDK));
        return current_expiry_time > metrics->cdcsdk_expiry_time_ms->value();
      },
      MonoDelta::FromSeconds(10) * kTimeMultiplier, "Wait for stream expiry time update."));
  ASSERT_GT(new_metrics->cdcsdk_change_event_count->value(), 100);
  ASSERT_TRUE(current_traffic_sent_bytes < new_metrics->cdcsdk_traffic_sent->value());
}

}  // namespace enterprise
}  // namespace cdc
}  // namespace yb
