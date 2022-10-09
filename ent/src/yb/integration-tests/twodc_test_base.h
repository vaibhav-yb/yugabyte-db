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

#ifndef ENT_SRC_YB_INTEGRATION_TESTS_TWODC_TEST_BASE_H
#define ENT_SRC_YB_INTEGRATION_TESTS_TWODC_TEST_BASE_H

#include <string>

#include "yb/client/transaction_manager.h"

#include "yb/integration-tests/cdc_test_util.h"
#include "yb/integration-tests/mini_cluster.h"

#include "yb/master/master_replication.fwd.h"

#include "yb/util/string_util.h"
#include "yb/util/test_util.h"
#include "yb/util/tsan_util.h"

#include "yb/yql/pgwrapper/libpq_utils.h"
#include "yb/yql/pgwrapper/pg_wrapper.h"

DECLARE_int32(cdc_read_rpc_timeout_ms);
DECLARE_int32(cdc_write_rpc_timeout_ms);
DECLARE_bool(TEST_check_broadcast_address);
DECLARE_bool(flush_rocksdb_on_shutdown);

namespace yb {

using client::YBClient;

namespace enterprise {

constexpr int kRpcTimeout = NonTsanVsTsan(60, 120);
static const std::string kUniverseId = "test_universe";
static const std::string kNamespaceName = "test_namespace";
static const std::string kKeyColumnName = "key";

class TwoDCTestBase : public YBTest {
 public:
  class Cluster {
   public:
    std::unique_ptr<MiniCluster> mini_cluster_;
    std::unique_ptr<YBClient> client_;
    std::unique_ptr<yb::pgwrapper::PgSupervisor> pg_supervisor_;
    HostPort pg_host_port_;
    boost::optional<client::TransactionManager> txn_mgr_;

    Result<pgwrapper::PGConn> Connect() {
      return ConnectToDB(std::string() /* dbname */);
    }

    Result<pgwrapper::PGConn> ConnectToDB(
        const std::string& dbname, bool simple_query_protocol = false) {
      return pgwrapper::PGConnBuilder({
        .host = pg_host_port_.host(),
        .port = pg_host_port_.port(),
        .dbname = dbname
      }).Connect(simple_query_protocol);
    }
  };

  void SetUp() override {
    YBTest::SetUp();
    // Allow for one-off network instability by ensuring a single CDC RPC timeout << test timeout.
    FLAGS_cdc_read_rpc_timeout_ms = (kRpcTimeout / 2) * 1000;
    FLAGS_cdc_write_rpc_timeout_ms = (kRpcTimeout / 2) * 1000;
    // Not a useful test for us. It's testing Public+Private IP NW errors and we're only public
    FLAGS_TEST_check_broadcast_address = false;
    FLAGS_flush_rocksdb_on_shutdown = false;
  }

  Status InitClusters(const MiniClusterOptions& opts, bool init_postgres = false);

  void TearDown() override;

  Status RunOnBothClusters(std::function<Status(MiniCluster*)> run_on_cluster);
  Status RunOnBothClusters(std::function<Status(Cluster*)> run_on_cluster);

  Status WaitForLoadBalancersToStabilize();

  Status CreateDatabase(
      Cluster* cluster, const std::string& namespace_name = kNamespaceName, bool colocated = false);

  static Result<client::YBTableName> CreateTable(
      YBClient* client, const std::string& namespace_name, const std::string& table_name,
      uint32_t num_tablets, const client::YBSchema* schema);

  Result<client::YBTableName> CreateYsqlTable(
      Cluster* cluster,
      const std::string& namespace_name,
      const std::string& schema_name,
      const std::string& table_name,
      const boost::optional<std::string>& tablegroup_name,
      uint32_t num_tablets,
      bool colocated = false,
      const ColocationId colocation_id = 0);

  Status CreateYsqlTable(
      uint32_t idx, uint32_t num_tablets, Cluster* cluster,
      std::vector<client::YBTableName>* table_names,
      const boost::optional<std::string>& tablegroup_name = {}, bool colocated = false);

  Result<client::YBTableName> GetYsqlTable(
      Cluster* cluster,
      const std::string& namespace_name,
      const std::string& schema_name,
      const std::string& table_name,
      bool verify_table_name = true,
      bool verify_schema_name = false,
      bool exclude_system_tables = true);

  Status SetupUniverseReplication(
      const std::vector<std::shared_ptr<client::YBTable>>& tables, bool leader_only = true);

  Status SetupUniverseReplication(
      const std::string& universe_id, const std::vector<std::shared_ptr<client::YBTable>>& tables,
      bool leader_only = true);

  Status SetupReverseUniverseReplication(
      const std::vector<std::shared_ptr<client::YBTable>>& tables);

  Status SetupUniverseReplication(
      MiniCluster* producer_cluster, MiniCluster* consumer_cluster, YBClient* consumer_client,
      const std::string& universe_id, const std::vector<std::shared_ptr<client::YBTable>>& tables,
      bool leader_only = true);

  Status SetupNSUniverseReplication(
      MiniCluster* producer_cluster, MiniCluster* consumer_cluster, YBClient* consumer_client,
      const std::string& universe_id, const std::string& producer_ns_name,
      const YQLDatabase& producer_ns_type,
      bool leader_only = true);

  Status VerifyUniverseReplication(master::GetUniverseReplicationResponsePB* resp);

  Status VerifyUniverseReplication(
      const std::string& universe_id, master::GetUniverseReplicationResponsePB* resp);

  Status VerifyUniverseReplication(
      MiniCluster* consumer_cluster, YBClient* consumer_client,
      const std::string& universe_id, master::GetUniverseReplicationResponsePB* resp);

  Status VerifyNSUniverseReplication(
      MiniCluster* consumer_cluster, YBClient* consumer_client,
      const std::string& universe_id, int num_expected_table);

  Status ToggleUniverseReplication(
      MiniCluster* consumer_cluster, YBClient* consumer_client,
      const std::string& universe_id, bool is_enabled);

  Status VerifyUniverseReplicationDeleted(MiniCluster* consumer_cluster,
      YBClient* consumer_client, const std::string& universe_id, int timeout);

  Status VerifyUniverseReplicationFailed(MiniCluster* consumer_cluster,
      YBClient* consumer_client, const std::string& producer_uuid,
      master::IsSetupUniverseReplicationDoneResponsePB* resp);

  Status IsSetupUniverseReplicationDone(
      MiniCluster* consumer_cluster, YBClient* consumer_client,
      const std::string& universe_id, master::IsSetupUniverseReplicationDoneResponsePB* resp);

  Status GetCDCStreamForTable(
      const std::string& table_id, master::ListCDCStreamsResponsePB* resp);

  uint32_t GetSuccessfulWriteOps(MiniCluster* cluster);

  Status DeleteUniverseReplication(const std::string& universe_id = kUniverseId);

  Status DeleteUniverseReplication(
      const std::string& universe_id, YBClient* client, MiniCluster* cluster);

  Status CorrectlyPollingAllTablets(MiniCluster* cluster, uint32_t num_producer_tablets);

  Status WaitForSetupUniverseReplicationCleanUp(string producer_uuid);

  YBClient* producer_client() {
    return producer_cluster_.client_.get();
  }

  YBClient* consumer_client() {
    return consumer_cluster_.client_.get();
  }

  MiniCluster* producer_cluster() {
    return producer_cluster_.mini_cluster_.get();
  }

  MiniCluster* consumer_cluster() {
    return consumer_cluster_.mini_cluster_.get();
  }

  client::TransactionManager* producer_txn_mgr() {
    return producer_cluster_.txn_mgr_.get_ptr();
  }

  client::TransactionManager* consumer_txn_mgr() {
    return consumer_cluster_.txn_mgr_.get_ptr();
  }

  std::string GetAdminToolPath() {
    const std::string kAdminToolName = "yb-admin";
    return GetToolPath(kAdminToolName);
  }

  template <class... Args>
  Result<std::string> CallAdmin(MiniCluster* cluster, Args&&... args) {
    return CallAdminVec(ToStringVector(
        GetAdminToolPath(), "-master_addresses", cluster->GetMasterAddresses(),
        std::forward<Args>(args)...));
  }

  Result<std::string> CallAdminVec(const std::vector<std::string>& args) {
    std::string result;
    LOG(INFO) << "Execute: " << AsString(args);
    auto status = Subprocess::Call(args, &result, StdFdTypes{StdFdType::kOut, StdFdType::kErr});
    if (!status.ok()) {
      return status.CloneAndAppend(result);
    }
    return result;
  }

 protected:
  Cluster producer_cluster_;
  Cluster consumer_cluster_;

 private:
  // Not thread safe. FLAGS_pgsql_proxy_webserver_port is modified each time this is called so this
  // is not safe to run in parallel.
  Status InitPostgres(Cluster* cluster, const size_t pg_ts_idx, uint16_t pg_port);
};

} // namespace enterprise
} // namespace yb

#endif // ENT_SRC_YB_INTEGRATION_TESTS_TWODC_TEST_BASE_H
