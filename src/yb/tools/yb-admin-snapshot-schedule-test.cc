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

#include "yb/client/ql-dml-test-base.h"
#include "yb/client/yb_table_name.h"

#include "yb/common/json_util.h"

#include "yb/gutil/logging-inl.h"

#include "yb/gutil/strings/split.h"
#include "yb/integration-tests/cql_test_util.h"
#include "yb/integration-tests/external_mini_cluster.h"
#include "yb/integration-tests/load_balancer_test_util.h"

#include "yb/master/master_ddl.proxy.h"

#include "yb/rpc/rpc_controller.h"

#include "yb/tools/admin-test-base.h"

#include "yb/tserver/tserver_admin.proxy.h"
#include "yb/tserver/tserver_service.proxy.h"

#include "yb/util/date_time.h"
#include "yb/util/format.h"
#include "yb/util/random_util.h"
#include "yb/util/range.h"
#include "yb/util/scope_exit.h"
#include "yb/util/status_format.h"
#include "yb/util/test_thread_holder.h"
#include "yb/util/tsan_util.h"

#include "yb/yql/pgwrapper/libpq_utils.h"

DECLARE_uint64(max_clock_skew_usec);

DECLARE_int32(num_tablet_servers);
DECLARE_int32(num_replicas);

namespace yb {
namespace tools {

namespace {

const std::string kClusterName = "yugacluster";

constexpr auto kInterval = 6s;
constexpr auto kRetention = 10min;
constexpr auto kHistoryRetentionIntervalSec = 5;
constexpr auto kCleanupSplitTabletsInterval = 1s;
const std::string old_sys_catalog_snapshot_path = "/opt/yb-build/ysql-sys-catalog-snapshots/";
const std::string old_sys_catalog_snapshot_name = "initial_sys_catalog_snapshot_2.0.9.0";

} // namespace

class YbAdminSnapshotScheduleTest : public AdminTestBase {
 public:
  Result<rapidjson::Document> GetSnapshotSchedule(const std::string& id = std::string()) {
    auto out = VERIFY_RESULT(id.empty() ? CallJsonAdmin("list_snapshot_schedules")
                                        : CallJsonAdmin("list_snapshot_schedules", id));
    auto schedules = VERIFY_RESULT(Get(&out, "schedules")).get().GetArray();
    if (schedules.Empty()) {
      return STATUS(NotFound, "Snapshot schedule not found");
    }
    SCHECK_EQ(schedules.Size(), 1U, NotFound, "Wrong schedules number");
    rapidjson::Document result;
    result.CopyFrom(schedules[0], result.GetAllocator());
    return result;
  }

  Result<rapidjson::Document> ListSnapshots() {
    auto out = VERIFY_RESULT(CallJsonAdmin("list_snapshots", "JSON"));
    rapidjson::Document result;
    result.CopyFrom(VERIFY_RESULT(Get(&out, "snapshots")).get(), result.GetAllocator());
    return result;
  }

  Result<rapidjson::Document> ListTablets(
      const client::YBTableName& table_name = client::kTableName) {
    auto out = VERIFY_RESULT(CallJsonAdmin(
        "list_tablets", "ycql." + table_name.namespace_name(), table_name.table_name(), "JSON"));
    rapidjson::Document result;
    result.CopyFrom(VERIFY_RESULT(Get(&out, "tablets")).get(), result.GetAllocator());
    return result;
  }

  Result<rapidjson::Document> WaitScheduleSnapshot(
      MonoDelta duration, const std::string& id = std::string(), uint32_t num_snapshots = 1) {
    rapidjson::Document result;
    RETURN_NOT_OK(WaitFor([this, id, num_snapshots, &result]() -> Result<bool> {
      auto schedule = VERIFY_RESULT(GetSnapshotSchedule(id));
      auto snapshots = VERIFY_RESULT(Get(&schedule, "snapshots")).get().GetArray();
      if (snapshots.Size() < num_snapshots) {
        return false;
      }
      result.CopyFrom(snapshots[snapshots.Size() - 1], result.GetAllocator());
      return true;
    }, duration, "Wait schedule snapshot"));

    // Wait for the present time to become at-least the time chosen by the first snapshot.
    auto snapshot_time_string = VERIFY_RESULT(Get(&result, "snapshot_time")).get().GetString();
    HybridTime snapshot_ht = VERIFY_RESULT(HybridTime::ParseHybridTime(snapshot_time_string));

    RETURN_NOT_OK(WaitFor([&snapshot_ht]() -> Result<bool> {
      Timestamp current_time(VERIFY_RESULT(WallClock()->Now()).time_point);
      HybridTime current_ht = HybridTime::FromMicros(current_time.ToInt64());
      return snapshot_ht <= current_ht;
    }, duration, "Wait Snapshot Time Elapses"));
    return result;
  }

  Result<std::string> StartRestoreSnapshotSchedule(
      const std::string& schedule_id, Timestamp restore_at) {
    auto out = VERIFY_RESULT(CallJsonAdmin(
        "restore_snapshot_schedule", schedule_id, restore_at.ToFormattedString()));
    std::string restoration_id = VERIFY_RESULT(Get(out, "restoration_id")).get().GetString();
    LOG(INFO) << "Restoration id: " << restoration_id;
    return restoration_id;
  }

  Status RestoreSnapshotSchedule(const std::string& schedule_id, Timestamp restore_at) {
    return WaitRestorationDone(
        VERIFY_RESULT(
            StartRestoreSnapshotSchedule(schedule_id, restore_at)), 40s * kTimeMultiplier);
  }

  Status WaitRestorationDone(const std::string& restoration_id, MonoDelta timeout) {
    return WaitFor([this, restoration_id]() -> Result<bool> {
      auto out = VERIFY_RESULT(CallJsonAdmin("list_snapshot_restorations", restoration_id));
      LOG(INFO) << "Restorations: " << common::PrettyWriteRapidJsonToString(out);
      const auto& restorations = VERIFY_RESULT(Get(out, "restorations")).get().GetArray();
      SCHECK_EQ(restorations.Size(), 1U, IllegalState, "Wrong restorations number");
      auto id = VERIFY_RESULT(Get(restorations[0], "id")).get().GetString();
      SCHECK_EQ(id, restoration_id, IllegalState, "Wrong restoration id");
      std::string state_str = VERIFY_RESULT(Get(restorations[0], "state")).get().GetString();
      master::SysSnapshotEntryPB::State state;
      if (!master::SysSnapshotEntryPB_State_Parse(state_str, &state)) {
        return STATUS_FORMAT(IllegalState, "Failed to parse restoration state: $0", state_str);
      }
      if (state == master::SysSnapshotEntryPB::RESTORING) {
        return false;
      }
      if (state == master::SysSnapshotEntryPB::RESTORED) {
        return true;
      }
      return STATUS_FORMAT(IllegalState, "Unexpected restoration state: $0",
                           master::SysSnapshotEntryPB_State_Name(state));
    }, timeout, "Wait restoration complete");
  }

  Result<std::vector<std::string>> GetAllRestorationIds() {
    std::vector<std::string> res;
    auto out = VERIFY_RESULT(CallJsonAdmin("list_snapshot_restorations"));
    LOG(INFO) << "Restorations: " << common::PrettyWriteRapidJsonToString(out);
    const auto& restorations = VERIFY_RESULT(Get(out, "restorations")).get().GetArray();
    auto id = VERIFY_RESULT(Get(restorations[0], "id")).get().GetString();
    res.push_back(id);

    return res;
  }

  Status PrepareCommon() {
    LOG(INFO) << "Create cluster";
    CreateCluster(kClusterName, ExtraTSFlags(), ExtraMasterFlags());

    LOG(INFO) << "Create client";
    client_ = VERIFY_RESULT(CreateClient());

    return Status::OK();
  }

  virtual std::vector<std::string> ExtraTSFlags() {
    return { Format("--timestamp_history_retention_interval_sec=$0", kHistoryRetentionIntervalSec),
             "--history_cutoff_propagation_interval_ms=1000",
             "--enable_automatic_tablet_splitting=true",
             Format("--cleanup_split_tablets_interval_sec=$0",
                      MonoDelta(kCleanupSplitTabletsInterval).ToSeconds()) };
  }

  virtual std::vector<std::string> ExtraMasterFlags() {
    // To speed up tests.
    return { "--snapshot_coordinator_cleanup_delay_ms=1000",
             "--snapshot_coordinator_poll_interval_ms=500",
             "--enable_automatic_tablet_splitting=true",
             "--enable_transactional_ddl_gc=false",
             "--allow_consecutive_restore=true" };
  }

  Result<std::string> PrepareQl(MonoDelta interval = kInterval, MonoDelta retention = kRetention) {
    RETURN_NOT_OK(PrepareCommon());

    LOG(INFO) << "Create namespace";
    RETURN_NOT_OK(client_->CreateNamespaceIfNotExists(
        client::kTableName.namespace_name(), client::kTableName.namespace_type()));

    return CreateSnapshotScheduleAndWaitSnapshot(
        client::kTableName.namespace_name(), interval, retention);
  }

  Result<std::string> CreateSnapshotScheduleAndWaitSnapshot(
      const std::string& filter, MonoDelta interval, MonoDelta retention) {
    LOG(INFO) << "Create snapshot schedule";
    std::string schedule_id = VERIFY_RESULT(CreateSnapshotSchedule(
        interval, retention, filter));

    LOG(INFO) << "Wait snapshot schedule";
    RETURN_NOT_OK(WaitScheduleSnapshot(30s, schedule_id));

    return schedule_id;
  }

  Result<std::string> PreparePg(bool colocated = false) {
    RETURN_NOT_OK(PrepareCommon());

    auto conn = VERIFY_RESULT(PgConnect());
    if (colocated) {
      RETURN_NOT_OK(conn.ExecuteFormat(
          "CREATE DATABASE $0 with colocated=true", client::kTableName.namespace_name()));
    } else {
      RETURN_NOT_OK(conn.ExecuteFormat("CREATE DATABASE $0", client::kTableName.namespace_name()));
    }

    return CreateSnapshotScheduleAndWaitSnapshot(
        "ysql." + client::kTableName.namespace_name(), kInterval, kRetention);
  }

  Result<pgwrapper::PGConn> PgConnect(const std::string& db_name = std::string()) {
    auto* ts = cluster_->tablet_server(
        RandomUniformInt<size_t>(0, cluster_->num_tablet_servers() - 1));
    return pgwrapper::PGConn::Connect(HostPort(ts->bind_host(), ts->pgsql_rpc_port()), db_name);
  }

  Result<std::string> PrepareCql(MonoDelta interval = kInterval, MonoDelta retention = kRetention) {
    RETURN_NOT_OK(PrepareCommon());

    auto conn = VERIFY_RESULT(CqlConnect());
    RETURN_NOT_OK(conn.ExecuteQuery(Format(
        "CREATE KEYSPACE IF NOT EXISTS $0", client::kTableName.namespace_name())));

    return CreateSnapshotScheduleAndWaitSnapshot(
        "ycql." + client::kTableName.namespace_name(), interval, retention);
  }

  template <class... Args>
  Result<std::string> CreateSnapshotSchedule(
      MonoDelta interval, MonoDelta retention, Args&&... args) {
    auto out = VERIFY_RESULT(CallJsonAdmin(
        "create_snapshot_schedule", interval.ToMinutes(), retention.ToMinutes(),
        std::forward<Args>(args)...));

    std::string schedule_id = VERIFY_RESULT(Get(out, "schedule_id")).get().GetString();
    LOG(INFO) << "Schedule id: " << schedule_id;
    return schedule_id;
  }

  Status DeleteSnapshotSchedule(const std::string& schedule_id) {
    auto out = VERIFY_RESULT(CallJsonAdmin("delete_snapshot_schedule", schedule_id));

    SCHECK_EQ(VERIFY_RESULT(Get(out, "schedule_id")).get().GetString(), schedule_id, IllegalState,
              "Deleted wrong schedule");
    return Status::OK();
  }

  Status WaitTabletsCleaned(CoarseTimePoint deadline) {
    return Wait([this, deadline]() -> Result<bool> {
      size_t alive_tablets = 0;
      for (size_t i = 0; i != cluster_->num_tablet_servers(); ++i) {
        auto proxy = cluster_->GetTServerProxy<tserver::TabletServerServiceProxy>(i);
        tserver::ListTabletsRequestPB req;
        tserver::ListTabletsResponsePB resp;
        rpc::RpcController controller;
        controller.set_deadline(deadline);
        RETURN_NOT_OK(proxy.ListTablets(req, &resp, &controller));
        for (const auto& tablet : resp.status_and_schema()) {
          if (tablet.tablet_status().table_type() != TableType::TRANSACTION_STATUS_TABLE_TYPE) {
            LOG(INFO) << "Not yet deleted tablet: " << tablet.ShortDebugString();
            ++alive_tablets;
          }
        }
      }
      LOG(INFO) << "Alive tablets: " << alive_tablets;
      return alive_tablets == 0;
    }, deadline, "Deleted tablet cleanup");
  }

  void TestUndeleteTable(bool restart_masters);

  void UpdateMiniClusterOptions(ExternalMiniClusterOptions* options) override {
    options->bind_to_unique_loopback_addresses = true;
    options->use_same_ts_ports = true;
    options->num_masters = 3;
  }

  std::unique_ptr<CppCassandraDriver> cql_driver_;
};

class YbAdminSnapshotScheduleTestWithYsql : public YbAdminSnapshotScheduleTest {
 public:
  void UpdateMiniClusterOptions(ExternalMiniClusterOptions* opts) override {
    opts->enable_ysql = true;
    opts->extra_tserver_flags.emplace_back("--ysql_num_shards_per_tserver=1");
    opts->num_masters = 3;
  }
};

TEST_F(YbAdminSnapshotScheduleTest, BadArguments) {
  BuildAndStart();

  ASSERT_NOK(CreateSnapshotSchedule(
      6s, 10min, kTableName.namespace_name(), kTableName.table_name()));
}

TEST_F(YbAdminSnapshotScheduleTest, Basic) {
  BuildAndStart();

  std::string schedule_id = ASSERT_RESULT(CreateSnapshotSchedule(
      6s, 10min, kTableName.namespace_name()));
  std::this_thread::sleep_for(20s);

  Timestamp last_snapshot_time;
  ASSERT_OK(WaitFor([this, schedule_id, &last_snapshot_time]() -> Result<bool> {
    auto schedule = VERIFY_RESULT(GetSnapshotSchedule());
    auto received_schedule_id = VERIFY_RESULT(Get(schedule, "id")).get().GetString();
    SCHECK_EQ(schedule_id, received_schedule_id, IllegalState, "Wrong schedule id");
    // Check schedule options.
    auto& options = VERIFY_RESULT(Get(schedule, "options")).get();
    std::string filter = VERIFY_RESULT(
        Get(options, "filter")).get().GetString();
    SCHECK_EQ(filter, Format("ycql.$0", kTableName.namespace_name()),
              IllegalState, "Wrong filter");
    std::string interval = VERIFY_RESULT(
        Get(options, "interval")).get().GetString();
    SCHECK_EQ(interval, "0 min", IllegalState, "Wrong interval");
    std::string retention = VERIFY_RESULT(
        Get(options, "retention")).get().GetString();
    SCHECK_EQ(retention, "10 min", IllegalState, "Wrong retention");
    // Check actual snapshots.
    const auto& snapshots = VERIFY_RESULT(Get(schedule, "snapshots")).get().GetArray();
    if (snapshots.Size() < 2) {
      return false;
    }
    std::string last_snapshot_time_str;
    for (const auto& snapshot : snapshots) {
      std::string snapshot_time = VERIFY_RESULT(
          Get(snapshot, "snapshot_time")).get().GetString();
      if (!last_snapshot_time_str.empty()) {
        std::string previous_snapshot_time = VERIFY_RESULT(
            Get(snapshot, "previous_snapshot_time")).get().GetString();
        SCHECK_EQ(previous_snapshot_time, last_snapshot_time_str, IllegalState,
                  "Wrong previous_snapshot_hybrid_time");
      }
      last_snapshot_time_str = snapshot_time;
    }
    LOG(INFO) << "Last snapshot time: " << last_snapshot_time_str;
    last_snapshot_time = VERIFY_RESULT(DateTime::TimestampFromString(last_snapshot_time_str));
    return true;
  }, 20s, "At least 2 snapshots"));

  last_snapshot_time.set_value(last_snapshot_time.value() + 1);
  LOG(INFO) << "Restore at: " << last_snapshot_time.ToFormattedString();

  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, last_snapshot_time));
}

TEST_F(YbAdminSnapshotScheduleTest, TestTruncateDisallowedWithPitr) {
  auto schedule_id = ASSERT_RESULT(PrepareCql());
  auto conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));
  ASSERT_OK(
      conn.ExecuteQuery("CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT) "
                        "WITH transactions = { 'enabled' : true }"));
  auto s = conn.ExecuteQuery("TRUNCATE TABLE test_table");
  ASSERT_NOK(s);
  ASSERT_STR_CONTAINS(s.ToString(), "Cannot truncate table test_table which has schedule");

  LOG(INFO) << "Enable flag to allow truncate and validate that truncate succeeds";
  ASSERT_OK(cluster_->SetFlagOnMasters("enable_truncate_on_pitr_table", "true"));
  ASSERT_OK(conn.ExecuteQuery("TRUNCATE TABLE test_table"));
}

TEST_F(YbAdminSnapshotScheduleTest, Delete) {
  auto schedule_id = ASSERT_RESULT(PrepareQl(kRetention, kRetention));

  auto session = client_->NewSession();
  LOG(INFO) << "Create table";
  ASSERT_NO_FATALS(client::kv_table_test::CreateTable(
      client::Transactional::kTrue, 3, client_.get(), &table_));

  LOG(INFO) << "Write values";
  const auto kKeys = Range(100);
  for (auto i : kKeys) {
    ASSERT_OK(client::kv_table_test::WriteRow(&table_, session, i, -i));
  }

  for (auto i : kKeys) {
    ASSERT_OK(client::kv_table_test::DeleteRow(&table_, session, i));
  }

  ASSERT_OK(DeleteSnapshotSchedule(schedule_id));

  ASSERT_OK(WaitFor([this, schedule_id]() -> Result<bool> {
    auto schedule = GetSnapshotSchedule();
    if (!schedule.ok()) {
      if (schedule.status().IsNotFound()) {
        return true;
      }
      return schedule.status();
    }

    auto& options = VERIFY_RESULT(Get(*schedule, "options")).get();
    auto delete_time = VERIFY_RESULT(Get(options, "delete_time")).get().GetString();
    LOG(INFO) << "Delete time: " << delete_time;
    return false;
  }, 10s * kTimeMultiplier, "Snapshot schedule cleaned up"));

  ASSERT_OK(WaitFor(
      [this]() -> Result<bool> {
        auto snapshots_json = VERIFY_RESULT(ListSnapshots());
        auto snapshots = snapshots_json.GetArray();
        LOG(INFO) << "Snapshots left: " << snapshots.Size();
        return snapshots.Empty();
      },
      10s * kTimeMultiplier, "Snapshots cleaned up"));

  ASSERT_OK(WaitFor([this]() -> Result<bool> {
    for (auto* tserver : cluster_->tserver_daemons()) {
      auto proxy = cluster_->GetProxy<tserver::TabletServerAdminServiceProxy>(tserver);
      tserver::FlushTabletsRequestPB req;
      req.set_dest_uuid(tserver->uuid());
      req.set_all_tablets(true);
      tserver::FlushTabletsResponsePB resp;
      rpc::RpcController controller;
      controller.set_timeout(30s);
      RETURN_NOT_OK(proxy.FlushTablets(req, &resp, &controller));

      req.set_operation(tserver::FlushTabletsRequestPB::COMPACT);
      controller.Reset();
      RETURN_NOT_OK(proxy.FlushTablets(req, &resp, &controller));
    }

    for (auto* tserver : cluster_->tserver_daemons()) {
      auto proxy = cluster_->GetProxy<tserver::TabletServerServiceProxy>(tserver);
      tserver::ListTabletsRequestPB req;
      tserver::ListTabletsResponsePB resp;

      rpc::RpcController controller;
      controller.set_timeout(30s);

      RETURN_NOT_OK(proxy.ListTablets(req, &resp, &controller));

      for (const auto& tablet : resp.status_and_schema()) {
        if (tablet.tablet_status().table_type() == TableType::TRANSACTION_STATUS_TABLE_TYPE) {
          continue;
        }
        if (tablet.tablet_status().sst_files_disk_size() != 0) {
          LOG(INFO) << "Tablet status: " << tablet.tablet_status().ShortDebugString();
          return false;
        }
      }
    }
    return true;
  }, 1s * kHistoryRetentionIntervalSec * kTimeMultiplier, "Compact SST files"));
}

void YbAdminSnapshotScheduleTest::TestUndeleteTable(bool restart_masters) {
  auto schedule_id = ASSERT_RESULT(PrepareQl());

  auto session = client_->NewSession();
  LOG(INFO) << "Create table";
  ASSERT_NO_FATALS(client::kv_table_test::CreateTable(
      client::Transactional::kTrue, 3, client_.get(), &table_));

  LOG(INFO) << "Write values";
  constexpr int kMinKey = 1;
  constexpr int kMaxKey = 100;
  for (int i = kMinKey; i <= kMaxKey; ++i) {
    ASSERT_OK(client::kv_table_test::WriteRow(&table_, session, i, -i));
  }

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

  LOG(INFO) << "Delete table";
  ASSERT_OK(client_->DeleteTable(client::kTableName));

  ASSERT_NOK(client::kv_table_test::WriteRow(&table_, session, kMinKey, 0));

  ASSERT_NO_FATALS(client::kv_table_test::CreateTable(
      client::Transactional::kTrue, 3, client_.get(), &table_));

  ASSERT_OK(client::kv_table_test::WriteRow(&table_, session, kMinKey, 0));

  if (restart_masters) {
    ASSERT_OK(RestartAllMasters(cluster_.get()));
  }

  LOG(INFO) << "Restore schedule";
  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time));

  ASSERT_OK(table_.Open(client::kTableName, client_.get()));

  LOG(INFO) << "Reading rows";
  auto rows = ASSERT_RESULT(client::kv_table_test::SelectAllRows(&table_, session));
  LOG(INFO) << "Rows: " << AsString(rows);
  ASSERT_EQ(rows.size(), kMaxKey - kMinKey + 1);
  for (int i = kMinKey; i <= kMaxKey; ++i) {
    ASSERT_EQ(rows[i], -i);
  }

  constexpr int kExtraKey = kMaxKey + 1;
  ASSERT_OK(client::kv_table_test::WriteRow(&table_, session, kExtraKey, -kExtraKey));
  auto extra_value = ASSERT_RESULT(client::kv_table_test::SelectRow(&table_, session, kExtraKey));
  ASSERT_EQ(extra_value, -kExtraKey);
}

TEST_F(YbAdminSnapshotScheduleTest, UndeleteTable) {
  TestUndeleteTable(false);
}

TEST_F(YbAdminSnapshotScheduleTest, UndeleteTableWithRestart) {
  TestUndeleteTable(true);
}

TEST_F(YbAdminSnapshotScheduleTest, CleanupDeletedTablets) {
  auto schedule_id = ASSERT_RESULT(PrepareQl(kInterval, kInterval));

  auto session = client_->NewSession();
  LOG(INFO) << "Create table";
  ASSERT_NO_FATALS(client::kv_table_test::CreateTable(
      client::Transactional::kTrue, 3, client_.get(), &table_));

  LOG(INFO) << "Write values";
  constexpr int kMinKey = 1;
  constexpr int kMaxKey = 100;
  for (int i = kMinKey; i <= kMaxKey; ++i) {
    ASSERT_OK(client::kv_table_test::WriteRow(&table_, session, i, -i));
  }

  LOG(INFO) << "Delete table";
  ASSERT_OK(client_->DeleteTable(client::kTableName));

  auto deadline = CoarseMonoClock::now() + kInterval + 10s;

  // Wait tablets deleted from tservers.
  ASSERT_OK(WaitTabletsCleaned(deadline));

  // Wait table marked as deleted.
  ASSERT_OK(Wait([this, deadline]() -> Result<bool> {
    auto proxy = cluster_->GetLeaderMasterProxy<master::MasterDdlProxy>();
    master::ListTablesRequestPB req;
    master::ListTablesResponsePB resp;
    rpc::RpcController controller;
    controller.set_deadline(deadline);
    req.set_include_not_running(true);
    RETURN_NOT_OK(proxy.ListTables(req, &resp, &controller));
    for (const auto& table : resp.tables()) {
      if (table.table_type() != TableType::TRANSACTION_STATUS_TABLE_TYPE
          && table.relation_type() != master::RelationType::SYSTEM_TABLE_RELATION
          && table.state() != master::SysTablesEntryPB::DELETED) {
        LOG(INFO) << "Not yet deleted table: " << table.ShortDebugString();
        return false;
      }
    }
    return true;
  }, deadline, "Deleted table cleanup"));
}

class YbAdminSnapshotScheduleTestWithYsqlColocatedParam
    : public YbAdminSnapshotScheduleTestWithYsql,
      public ::testing::WithParamInterface<bool> {
 public:
  Result<std::string> PreparePgWithColocatedParam() { return PreparePg(GetParam()); }
};

INSTANTIATE_TEST_CASE_P(PITRFlags, YbAdminSnapshotScheduleTestWithYsqlColocatedParam,
                        ::testing::Values(false, true));

TEST_P(YbAdminSnapshotScheduleTestWithYsqlColocatedParam, Pgsql) {
  YB_SKIP_TEST_IN_TSAN();
  auto schedule_id = ASSERT_RESULT(PreparePgWithColocatedParam());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  ASSERT_OK(conn.Execute("CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT)"));

  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'before')"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

  ASSERT_OK(conn.Execute("UPDATE test_table SET value = 'after'"));

  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time));

  auto res = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table"));

  ASSERT_EQ(res, "before");
}

TEST_P(YbAdminSnapshotScheduleTestWithYsqlColocatedParam, PgsqlDropDatabaseAndSchedule) {
  auto schedule_id = ASSERT_RESULT(PreparePgWithColocatedParam());

  auto conn = ASSERT_RESULT(PgConnect());

  auto res = conn.Execute(Format("DROP DATABASE $0", client::kTableName.namespace_name()));
  ASSERT_NOK(res);
  ASSERT_STR_CONTAINS(res.message().ToBuffer(), "Cannot delete database which has schedule");

  // Once the schedule is deleted, we should be able to drop the database.
  ASSERT_OK(DeleteSnapshotSchedule(schedule_id));
  ASSERT_OK(conn.Execute(Format("DROP DATABASE $0", client::kTableName.namespace_name())));
}

TEST_P(YbAdminSnapshotScheduleTestWithYsqlColocatedParam, PgsqlCreateTable) {
  YB_SKIP_TEST_IN_TSAN();
  auto schedule_id = ASSERT_RESULT(PreparePgWithColocatedParam());
  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

  ASSERT_OK(conn.Execute("CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'before')"));

  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);

  // Wait for Restore to complete.
  ASSERT_OK(WaitFor([this]() -> Result<bool> {
    bool all_tablets_hidden = true;
    for (size_t i = 0; i < cluster_->num_tablet_servers(); i++) {
      auto proxy = cluster_->GetTServerProxy<tserver::TabletServerServiceProxy>(i);
      tserver::ListTabletsRequestPB req;
      tserver::ListTabletsResponsePB resp;
      rpc::RpcController controller;
      controller.set_timeout(30s);
      RETURN_NOT_OK(proxy.ListTablets(req, &resp, &controller));
      for (const auto& tablet : resp.status_and_schema()) {
        if (tablet.tablet_status().namespace_name() == client::kTableName.namespace_name()) {
          LOG(INFO) << "Tablet " << tablet.tablet_status().tablet_id() << " of table "
                    << tablet.tablet_status().table_name() << ", hidden status "
                    << tablet.tablet_status().is_hidden();
          all_tablets_hidden = all_tablets_hidden && tablet.tablet_status().is_hidden();
        }
      }
    }
    return all_tablets_hidden;
  }, 30s, "Restore failed."));

  ASSERT_NOK(conn.Execute("INSERT INTO test_table VALUES (2, 'now')"));
  ASSERT_OK(conn.Execute("CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'after')"));
  auto res = ASSERT_RESULT(conn.FetchValue<std::string>(
      "SELECT value FROM test_table WHERE key = 1"));
  ASSERT_EQ(res, "after");
}

TEST_P(YbAdminSnapshotScheduleTestWithYsqlColocatedParam, FailAfterMigration) {
  YB_SKIP_TEST_IN_TSAN();
  auto schedule_id = ASSERT_RESULT(PreparePgWithColocatedParam());
  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));
  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Save time to restore to: " << time;
  LOG(INFO) << "Insert new row into pb_yg_migration table.";
  ASSERT_OK(conn.Execute(
      "INSERT INTO pg_yb_migration (major, minor, name) VALUES (2147483640, 0, 'version n')"));
  LOG(INFO) << "Assert restore for time " << time
            << " fails because of new row in pg_yb_migration.";
  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_NOK(restore_status);
  ASSERT_STR_CONTAINS(
      restore_status.message().ToBuffer(), "Unable to restore as YSQL upgrade was performed");
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlCreateIndex),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());
  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  ASSERT_OK(conn.Execute("CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'before')"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

  ASSERT_OK(conn.Execute("CREATE INDEX test_table_idx ON test_table (value)"));

  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);

  auto res = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table"));
  ASSERT_EQ(res, "before");
  ASSERT_OK(conn.Execute("CREATE INDEX test_table_idx ON test_table (value)"));
  ASSERT_OK(conn.Execute("UPDATE test_table SET value = 'after'"));
  res = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table"));
  ASSERT_EQ(res, "after");
}

TEST_P(YbAdminSnapshotScheduleTestWithYsqlColocatedParam, PgsqlDropTable) {
  YB_SKIP_TEST_IN_TSAN();
  auto schedule_id = ASSERT_RESULT(PreparePgWithColocatedParam());
  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  ASSERT_OK(conn.Execute("CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'before')"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  ASSERT_OK(conn.Execute("DROP TABLE test_table"));

  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);

  auto res = ASSERT_RESULT(conn.FetchValue<std::string>(
      "SELECT value FROM test_table WHERE key = 1"));
  ASSERT_EQ(res, "before");
  ASSERT_OK(conn.Execute("UPDATE test_table SET value = 'after'"));
  res = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table WHERE key = 1"));
  ASSERT_EQ(res, "after");
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlDropIndex),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  ASSERT_OK(conn.Execute("CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT)"));
  ASSERT_OK(conn.Execute("CREATE INDEX test_table_idx ON test_table (value)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'before')"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

  ASSERT_OK(conn.Execute("DROP INDEX test_table_idx"));

  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);

  auto res = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table"));
  ASSERT_EQ(res, "before");
  ASSERT_NOK(conn.Execute("CREATE INDEX test_table_idx ON test_table (value)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (2, 'after')"));
  res = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table WHERE key = 2"));
  ASSERT_EQ(res, "after");
}

TEST_P(YbAdminSnapshotScheduleTestWithYsqlColocatedParam, PgsqlAddColumn) {
  YB_SKIP_TEST_IN_TSAN();
  auto schedule_id = ASSERT_RESULT(PreparePgWithColocatedParam());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  LOG(INFO) << "Create table 'test_table' and insert a row";
  ASSERT_OK(conn.Execute("CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'before')"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time to restore back: " << time;
  LOG(INFO) << "Alter table test_table -> Add 'value2' column";
  ASSERT_OK(conn.Execute("ALTER TABLE test_table ADD COLUMN value2 TEXT"));
  ASSERT_OK(conn.Execute("UPDATE test_table SET value = 'now'"));
  ASSERT_OK(conn.Execute("UPDATE test_table SET value2 = 'now2'"));
  auto res = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value2 FROM test_table"));
  ASSERT_EQ(res, "now2");

  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);
  LOG(INFO) << "Select data from table after restore";
  res = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table"));
  ASSERT_EQ(res, "before");
  LOG(INFO) << "Insert data to the table after restore";
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (2, 'one more')"));
  ASSERT_NOK(conn.Execute("INSERT INTO test_table VALUES (3, 'again one more', 'new_value')"));
  auto result_status = conn.FetchValue<std::string>("SELECT value2 FROM test_table");
  ASSERT_EQ(result_status.ok(), false);
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlDeleteColumn),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));
  LOG(INFO) << "Create table 'test_table' and insert a row";
  ASSERT_OK(conn.Execute("CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'before')"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time to restore back: " << time;
  LOG(INFO) << "Alter table 'test_table' -> Drop 'value' column";
  ASSERT_OK(conn.Execute("ALTER TABLE test_table DROP COLUMN value"));
  auto drop_result_status = conn.FetchValue<std::string>("SELECT value FROM test_table");
  ASSERT_EQ(drop_result_status.ok(), false);
  LOG(INFO) << "Reading Rows";
  auto select_res = ASSERT_RESULT(conn.FetchValue<int>("SELECT * FROM test_table"));
  LOG(INFO) << "Read result: " << select_res;
  ASSERT_EQ(select_res, 1);
  ASSERT_NOK(conn.Execute("INSERT INTO test_table VALUES (2, 'new_value')"));

  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);
  LOG(INFO) << "Select data from table after restore";
  auto res = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table"));
  ASSERT_EQ(res, "before");
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (2, 'next value')"));
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlRenameTable),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));
  LOG(INFO) << "Create table 'test_table' and insert a row";
  ASSERT_OK(conn.Execute("CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'before')"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time to restore back: " << time;
  LOG(INFO) << "Alter table 'test_table' -> Rename table to 'new_table'";
  ASSERT_OK(conn.Execute("ALTER TABLE test_table RENAME TO new_table"));
  auto renamed_result = conn.FetchValue<std::string>("SELECT value FROM new_table");
  ASSERT_EQ(renamed_result.ok(), true);
  LOG(INFO) << "Reading Rows";
  auto select_result = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM new_table"));
  LOG(INFO) << "Read result: " << select_result;
  ASSERT_EQ(select_result, "before");
  auto result_with_old_name = conn.FetchValue<std::string>("SELECT value FROM test_table");
  ASSERT_EQ(result_with_old_name.ok(), false);

  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);
  LOG(INFO) << "Select data from table after restore";
  auto renamed_result_after_restore = conn.FetchValue<std::string>("SELECT value FROM new_table");
  ASSERT_EQ(renamed_result_after_restore.ok(), false);
  auto res = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table"));
  ASSERT_EQ(res, "before");
  LOG(INFO) << "Insert data to table after restore";
  ASSERT_NOK(conn.Execute("INSERT INTO new_table VALUES (2, 'new value')"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (2, 'new value')"));
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlRenameColumn),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));
  LOG(INFO) << "Create table 'test_table' and insert a row";
  ASSERT_OK(conn.Execute("CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'before')"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time to restore back: " << time;
  LOG(INFO) << "Alter table 'test_table' -> Rename 'value' column to 'value2'";
  ASSERT_OK(conn.Execute("ALTER TABLE test_table RENAME COLUMN value TO value2"));
  auto result_with_old_name = conn.FetchValue<std::string>("SELECT value FROM test_table");
  ASSERT_EQ(result_with_old_name.ok(), false);
  auto renamed_result = conn.FetchValue<std::string>("SELECT value2 FROM test_table");
  ASSERT_EQ(renamed_result.ok(), true);
  LOG(INFO) << "Reading Rows";
  auto select_res = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value2 FROM test_table"));
  LOG(INFO) << "Read result: " << select_res;
  ASSERT_EQ(select_res, "before");

  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);
  LOG(INFO) << "Select data from table after restore";
  auto res = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table"));
  ASSERT_EQ(res, "before");
  LOG(INFO) << "Insert data to table after restore";
  ASSERT_NOK(conn.Execute("INSERT INTO test_table(key, value2) VALUES (2, 'new_value')"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table(key, value) VALUES (2, 'new_value')"));
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlSetDefault),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));
  LOG(INFO) << "Create table and insert a row";
  ASSERT_OK(conn.Execute("CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'before')"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time noted to to restore the database: " << time;

  LOG(INFO) << "Alter table and set a default value to the value column";
  ASSERT_OK(conn.Execute("ALTER TABLE test_table ALTER COLUMN value SET DEFAULT 'default_value'"));

  LOG(INFO) << "Insert a row without providing a value for the default column";
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (2)"));

  LOG(INFO) << "Fetch the row inserted above and verify default value is inserted correctly";
  auto res = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table "
      "WHERE key=2"));
  ASSERT_EQ(res, "default_value");

  LOG(INFO) << "Perform a Restore to the time noted above";
  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);

  LOG(INFO) << "Insert a new row and verify that the default clause is no longer present";
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (3)"));
  res = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table WHERE key=3"));
  ASSERT_EQ(res, "");

  LOG(INFO) << "Verify that the row with key=2 is no longer present after restore";
  auto result_status = conn.FetchValue<std::string>("SELECT * FROM test_table where key=2");
  ASSERT_EQ(result_status.ok(), false);
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlDropDefault),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  LOG(INFO) << "Create table with default on the value column and insert a row";
  ASSERT_OK(conn.Execute(
      "CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT default('default_value'))"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1)"));

  LOG(INFO) << "Verify default value is set correctly";
  auto res =
      ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table WHERE key=1"));
  ASSERT_EQ(res, "default_value");

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time noted to to restore the database: " << time;

  LOG(INFO) << "Alter table and drop the default value on the column value";
  ASSERT_OK(conn.Execute("ALTER TABLE test_table ALTER COLUMN value DROP DEFAULT"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (2)"));

  LOG(INFO) << "Verify default is dropped correctly";
  auto res2 =
      ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table where key=2"));
  ASSERT_EQ(res2, "");

  LOG(INFO) << "Perform a Restore to the time noted above";
  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);

  LOG(INFO) << "Insert a row and verify that the default clause is still present";
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (3)"));
  auto res3 =
      ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table where key=3"));
  ASSERT_EQ(res3, "default_value");

  LOG(INFO) << "Verify that the row with key=2 is no longer present after restore";
  auto result_status = conn.FetchValue<std::string>("SELECT * FROM test_table where key=2");
  ASSERT_EQ(result_status.ok(), false);
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlSetNotNull),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  LOG(INFO) << "Create a table and insert data";
  ASSERT_OK(conn.Execute(
      "CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'Before')"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time noted to to restore the database: " << time;

  LOG(INFO) << "Alter table and set not null";
  ASSERT_OK(conn.Execute("ALTER TABLE test_table ALTER COLUMN value SET NOT NULL"));

  LOG(INFO) << "Insert null value in the not null column and assert failure";
  ASSERT_NOK(conn.Execute("INSERT INTO test_table VALUES (2)"));

  LOG(INFO) << "Perform a Restore to the time noted above";
  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);

  LOG(INFO) << "Insert rows with null values and verify it goes through successfully";
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (2)"));
  auto res3 =
      ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table where key=2"));
  ASSERT_EQ(res3, "");
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlDropNotNull),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  LOG(INFO) << "Create a table with not null clause and insert data";
  ASSERT_OK(conn.Execute(
      "CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT NOT NULL)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'Before')"));

  LOG(INFO) << "Verify failure on null insertion";
  ASSERT_NOK(conn.Execute("INSERT INTO test_table VALUES (2)"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time noted to to restore the database: " << time;

  LOG(INFO) << "Alter table and drop not null clause";
  ASSERT_OK(conn.Execute("ALTER TABLE test_table ALTER COLUMN value DROP NOT NULL"));

  LOG(INFO) << "Insert null values and verify success";
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (2)"));

  LOG(INFO) << "Perform a Restore to the time noted above";
  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);

  LOG(INFO) << "Verify failure on null insertion since the drop is restored via PITR";
  ASSERT_NOK(conn.Execute("INSERT INTO test_table VALUES (3)"));
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlAlterTableAddPK),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  LOG(INFO) << "Create a table and insert data";
  ASSERT_OK(conn.Execute(
      "CREATE TABLE test_table (key INT, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'BeforePK')"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time noted to to restore the database: " << time;

  LOG(INFO) << "Alter table and add primary key constraint";
  ASSERT_OK(conn.Execute("ALTER TABLE test_table ADD PRIMARY KEY (key)"));

  LOG(INFO) << "Verify Primary key constraint added";
  ASSERT_NOK(conn.Execute("INSERT INTO test_table(value) VALUES (1, 'AfterPK')"));
  ASSERT_NOK(conn.Execute("INSERT INTO test_table(value) VALUES ('DuringPK')"));

  LOG(INFO) << "Perform a Restore to the time noted above";
  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);

  LOG(INFO) << "Verify Primary key constraint no longer exists";
  ASSERT_OK(conn.Execute("INSERT INTO test_table(value) VALUES ('AfterPITR')"));

  LOG(INFO) << "Insert a row with key=1 and verify that it succeeds.";
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'AfterPKRemoval')"));
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlAlterTableAddFK),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  LOG(INFO) << "Create tables and insert data";
  ASSERT_OK(conn.Execute(
      "CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'BeforeFK')"));
  ASSERT_OK(conn.Execute(
      "CREATE TABLE test_table_2 (key_id1 INT PRIMARY KEY, key_id2 INT)"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time noted to to restore the database: " << time;

  LOG(INFO) << "Alter table 2 and add Foreign key constraint";
  ASSERT_OK(conn.Execute("ALTER TABLE test_table_2 ADD CONSTRAINT fk2 "
      "FOREIGN KEY (key_id2) REFERENCES test_table(key)"));
  ASSERT_NOK(conn.Execute("INSERT INTO test_table_2 VALUES (1, 2)"));

  LOG(INFO) << "Perform a Restore to the time noted above";
  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);

  LOG(INFO) << "Verify Foreign key no longer exists post PITR";
  ASSERT_OK(conn.Execute("INSERT INTO test_table_2 VALUES (1, 2)"));
  ASSERT_OK(conn.Execute("DROP TABLE test_table"));
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlAlterTableSetOwner),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));
  LOG(INFO) << "Create user user1";
  ASSERT_OK(conn.Execute("CREATE USER user1"));

  LOG(INFO) << "Create user user2";
  ASSERT_OK(conn.Execute("CREATE USER user2"));

  LOG(INFO) << "Set Session authorization to user1";
  ASSERT_OK(conn.Execute("SET SESSION AUTHORIZATION user1"));

  LOG(INFO) << "Create table with user1 as the owner";
  ASSERT_OK(conn.Execute(
      "CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT)"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time noted to to restore the database: " << time;

  LOG(INFO) << "Set session authorization to super user";
  ASSERT_OK(conn.Execute("SET SESSION AUTHORIZATION yugabyte"));

  LOG(INFO) << "Alter table and set owner of the table to user2";
  ASSERT_OK(conn.Execute("ALTER TABLE test_table OWNER TO user2"));

  LOG(INFO) << "Set session authorization to user2";
  ASSERT_OK(conn.Execute("SET SESSION AUTHORIZATION user2"));

  LOG(INFO) << "Verify user session is set correctly";
  ASSERT_OK(conn.Execute("ALTER TABLE test_table RENAME key TO key_new"));

  LOG(INFO) << "Perform a Restore to the time noted above";
  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);

  LOG(INFO) << "Set session authorization to user2";
  ASSERT_OK(conn.Execute("SET SESSION AUTHORIZATION user2"));

  LOG(INFO) << "Verify user2 is no longer the owner of the table post PITR and "
               "is unable to perform writes on the table";
  ASSERT_NOK(conn.Execute("ALTER TABLE test_table RENAME key TO key_new2"));

  LOG(INFO) << "Set session authorization to user1";
  ASSERT_OK(conn.Execute("SET SESSION AUTHORIZATION user1"));

  LOG(INFO) << "Verify user1 is able to perform write operations on the table ";
  ASSERT_OK(conn.Execute("ALTER TABLE test_table RENAME key TO key_new3"));
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlAddUniqueConstraint),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  LOG(INFO) << "Create table and insert data";
  ASSERT_OK(conn.Execute(
      "CREATE TABLE test_table (key INT, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'ABC')"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time noted to to restore the database: " << time;

  LOG(INFO) << "Alter table add unique constraint";
  ASSERT_OK(conn.Execute("ALTER TABLE test_table ADD CONSTRAINT uniquecst UNIQUE (value)"));

  LOG(INFO) << "Verify Unique constraint added";
  ASSERT_NOK(conn.Execute("INSERT INTO test_table VALUES (2, 'ABC')"));

  LOG(INFO) << "Perform a Restore to the time noted above";
  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);

  LOG(INFO) << "Verify unique constraint is no longer present";
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (2, 'ABC')"));
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlDropUniqueConstraint),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  LOG(INFO) << "Create table and insert data";
  ASSERT_OK(conn.Execute(
      "CREATE TABLE test_table (key INT, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'ABC')"));

  LOG(INFO) << "Add unique constraint to the table";
  ASSERT_OK(conn.Execute("ALTER TABLE test_table ADD CONSTRAINT uniquecst UNIQUE (value)"));

  LOG(INFO) << "Verify unique constraint added";
  ASSERT_NOK(conn.Execute("INSERT INTO test_table VALUES (2, 'ABC')"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time noted to to restore the database " << time;

  LOG(INFO) << "Drop Unique constraint";
  ASSERT_OK(conn.Execute("ALTER TABLE test_table DROP CONSTRAINT uniquecst"));

  LOG(INFO) << "Verify unique constraint is dropped";
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (2, 'ABC')"));

  LOG(INFO) << "Perform a Restore to the time noted above";
  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);

  LOG(INFO) << "Verify that the unique constraint is present and drop is restored";
  ASSERT_NOK(conn.Execute("INSERT INTO test_table VALUES (3, 'ABC')"));

  LOG(INFO) << "Verify that insertion of a row satisfying the unique constraint works";
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (3, 'DEF')"));

  LOG(INFO) << "Verify that the row with key=2 is no longer present after restore";
  auto result_status = conn.FetchValue<std::string>("SELECT * FROM test_table where key=2");
  ASSERT_EQ(result_status.ok(), false);
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlAddCheckConstraint),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  LOG(INFO) << "Create table and insert data";
  ASSERT_OK(conn.Execute(
      "CREATE TABLE test_table (key INT, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (150, 'ABC')"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time noted to to restore the database: " << time;

  LOG(INFO) << "Alter table and add check constraint";
  ASSERT_OK(conn.Execute("ALTER TABLE test_table ADD CONSTRAINT check_1 CHECK (key > 100)"));

  LOG(INFO) << "Verify Check constraint added";
  ASSERT_NOK(conn.Execute("INSERT INTO test_table VALUES (2, 'XYZ')"));

  LOG(INFO) << "Perform a Restore to the time noted above";
  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);

  LOG(INFO) << "Verify check constraint is removed post PITR";
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (2, 'PQR')"));
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlDropCheckConstraint),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  LOG(INFO) << "Create table and insert data";
  ASSERT_OK(conn.Execute(
      "CREATE TABLE test_table (key INT, value TEXT, CONSTRAINT con1 CHECK (key > 100))"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (101, 'WithCon1')"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time noted to to restore the database: " << time;

  LOG(INFO) << "Alter table and drop the check constraint";
  ASSERT_OK(conn.Execute("ALTER TABLE test_table DROP CONSTRAINT con1"));

  LOG(INFO) << "Verify check constraint is dropped";
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (2, 'Constraint_Dropped')"));

  LOG(INFO) << "Perform a Restore to the time noted above";
  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);

  LOG(INFO) << "Verify drop constraint is undone post PITR";
  ASSERT_NOK(conn.Execute("INSERT INTO test_table VALUES (3, 'With_Constraint')"));

  LOG(INFO) << "Verify insertion of a row satisfying the constraint post restore";
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (102, 'After_PITR')"));

  LOG(INFO) << "Verify that the row with key=2 is no longer present after restore";
  auto result_status = conn.FetchValue<std::string>("SELECT * FROM test_table where key=2");
  ASSERT_EQ(result_status.ok(), false);
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlSequenceUndoDeletedData),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));
  LOG(INFO) << "Create table 'test_table'";
  ASSERT_OK(conn.Execute("CREATE TABLE test_table (key INT PRIMARY KEY, value INT)"));
  LOG(INFO) << "Create Sequence 'value_data'";
  ASSERT_OK(conn.Execute("CREATE SEQUENCE value_data INCREMENT 5 OWNED BY test_table.value"));
  LOG(INFO) << "Insert some rows to 'test_table'";
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, nextval('value_data'))"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (2, nextval('value_data'))"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (3, nextval('value_data'))"));
  LOG(INFO) << "Reading Rows";
  auto res = ASSERT_RESULT(conn.FetchValue<int32_t>("SELECT value FROM test_table where key=3"));
  LOG(INFO) << "Select result " << res;
  ASSERT_EQ(res, 11);

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

  LOG(INFO) << "Time to restore back " << time;
  LOG(INFO) << "Deleting last row";
  ASSERT_OK(conn.Execute("DELETE FROM test_table where key=3"));

  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);

  LOG(INFO) << "Select data from 'test_table' after restore";
  res = ASSERT_RESULT(conn.FetchValue<int32_t>("SELECT value FROM test_table where key=3"));
  LOG(INFO) << "Select result " << res;
  ASSERT_EQ(res, 11);
  LOG(INFO) << "Insert a row into 'test_table' and validate";
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (4, nextval('value_data'))"));
  res = ASSERT_RESULT(conn.FetchValue<int32_t>("SELECT value FROM test_table where key=4"));
  LOG(INFO) << "Select result " << res;
  ASSERT_EQ(res, 16);
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlSequenceUndoInsertedData),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));
  LOG(INFO) << "Create table 'test_table'";
  ASSERT_OK(conn.Execute("CREATE TABLE test_table (key INT PRIMARY KEY, value INT)"));
  LOG(INFO) << "Create Sequence 'value_data'";
  ASSERT_OK(conn.Execute("CREATE SEQUENCE value_data INCREMENT 5 OWNED BY test_table.value"));
  LOG(INFO) << "Insert some rows to 'test_table'";
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, nextval('value_data'))"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (2, nextval('value_data'))"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (3, nextval('value_data'))"));
  LOG(INFO) << "Reading Rows";
  auto res = ASSERT_RESULT(conn.FetchValue<int32_t>("SELECT value FROM test_table where key=3"));
  LOG(INFO) << "Select result " << res;
  ASSERT_EQ(res, 11);

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

  LOG(INFO) << "Time to restore back " << time;
  LOG(INFO) << "Inserting new row in 'test_table'";
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (4, nextval('value_data'))"));
  LOG(INFO) << "Reading Rows from 'test_table'";
  res = ASSERT_RESULT(conn.FetchValue<int32_t>("SELECT value FROM test_table where key=4"));
  LOG(INFO) << "Select result " << res;
  ASSERT_EQ(res, 16);

  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);

  LOG(INFO) << "Select row from 'test_table' after restore";
  auto result_status = conn.FetchValue<int32_t>("SELECT value FROM test_table where key=4");
  ASSERT_EQ(result_status.ok(), false);

  res = ASSERT_RESULT(conn.FetchValue<int32_t>("SELECT value FROM test_table where key=3"));
  LOG(INFO) << "Select result " << res;
  ASSERT_EQ(res, 11);
  LOG(INFO) << "Insert a row into 'test_table' and validate";
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (4, nextval('value_data'))"));
  // Here value should be 21 instead of 16 as previous insert has value 16
  res = ASSERT_RESULT(conn.FetchValue<int32_t>("SELECT value FROM test_table where key=4"));
  LOG(INFO) << "Select result " << res;
  ASSERT_EQ(res, 21);
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlSequenceUndoCreateSequence),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));
  LOG(INFO) << "Create table 'test_table'";
  ASSERT_OK(conn.Execute("CREATE TABLE test_table (key INT PRIMARY KEY, value INT)"));

  Timestamp time_before_create(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time to restore back before sequence creation : " << time_before_create;

  LOG(INFO) << "Create Sequence 'value_data'";
  ASSERT_OK(conn.Execute("CREATE SEQUENCE value_data INCREMENT 5 OWNED BY test_table.value"));

  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, nextval('value_data'))"));

  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time_before_create));
  ASSERT_NOK(conn.Execute("INSERT INTO test_table VALUES (1, nextval('value_data'))"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 45)"));

  // Ensure that you are able to create sequences post restore.
  LOG(INFO) << "Create Sequence 'value_data'";
  ASSERT_OK(conn.Execute("CREATE SEQUENCE value_data INCREMENT 5 OWNED BY test_table.value"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (2, nextval('value_data'))"));
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlSequenceUndoDropSequence),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));
  LOG(INFO) << "Create table 'test_table'";
  ASSERT_OK(conn.Execute("CREATE TABLE test_table (key SERIAL, value TEXT)"));

  ASSERT_OK(conn.Execute("INSERT INTO test_table (value) values ('before')"));

  Timestamp time_before_drop(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time to restore back before table drop : " << time_before_drop;

  LOG(INFO) << "Drop table 'test_table'";
  ASSERT_OK(conn.Execute("DROP TABLE test_table"));

  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time_before_drop));
  auto res = ASSERT_RESULT(
      conn.FetchValue<std::string>("SELECT value FROM test_table where key=1"));
  ASSERT_EQ(res, "before");
  ASSERT_OK(conn.Execute("INSERT INTO test_table (value) values ('after')"));

  // Verify that we are able to create more sequences post restore.
  LOG(INFO) << "Create table 'test_table_new'";
  ASSERT_OK(conn.Execute("CREATE TABLE test_table_new (key SERIAL, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table_new (value) values ('before')"));
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlSequenceVerifyPartialRestore),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));
  // Connection to yugabyte database.
  auto conn_yugabyte = ASSERT_RESULT(PgConnect());

  LOG(INFO) << "Create table 'demo.test_table'";
  ASSERT_OK(conn.Execute("CREATE TABLE test_table (key SERIAL, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table (value) values ('before')"));

  LOG(INFO) << "Create table 'yugabyte.test_table'";
  ASSERT_OK(conn_yugabyte.Execute("CREATE TABLE test_table (key SERIAL, value TEXT)"));
  ASSERT_OK(conn_yugabyte.Execute("INSERT INTO test_table (value) values ('before')"));

  LOG(INFO) << "Create table 'demo.test_table2'";
  ASSERT_OK(conn.Execute("CREATE TABLE test_table2 (key SERIAL, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table2 (value) values ('before')"));

  LOG(INFO) << "Create table 'yugabyte.test_table2'";
  ASSERT_OK(conn_yugabyte.Execute("CREATE TABLE test_table2 (key SERIAL, value TEXT)"));
  ASSERT_OK(conn_yugabyte.Execute("INSERT INTO test_table2 (value) values ('before')"));

  Timestamp time_before_drop(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time to restore back before table drop : " << time_before_drop;

  LOG(INFO) << "Create table 'demo.test_table3'";
  ASSERT_OK(conn.Execute("CREATE TABLE test_table3 (key SERIAL, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table3 (value) values ('before')"));

  LOG(INFO) << "Create table 'yugabyte.test_table3'";
  ASSERT_OK(conn_yugabyte.Execute("CREATE TABLE test_table3 (key SERIAL, value TEXT)"));
  ASSERT_OK(conn_yugabyte.Execute("INSERT INTO test_table3 (value) values ('before')"));

  LOG(INFO) << "Drop table 'demo.test_table'";
  ASSERT_OK(conn.Execute("DROP TABLE test_table"));
  LOG(INFO) << "Drop table 'yugabyte.test_table'";
  ASSERT_OK(conn_yugabyte.Execute("DROP TABLE test_table"));

  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time_before_drop));
  // demo.test_table should be recreated.
  LOG(INFO) << "Select from demo.test_table";
  auto res = ASSERT_RESULT(
      conn.FetchValue<std::string>("SELECT value FROM test_table where key=1"));
  ASSERT_EQ(res, "before");
  ASSERT_OK(conn.Execute("INSERT INTO test_table (value) values ('after')"));

  // demo.test_table2 should remain as it was.
  LOG(INFO) << "Select from demo.test_table2s";
  res = ASSERT_RESULT(
      conn.FetchValue<std::string>("SELECT value FROM test_table2 where key=1"));
  ASSERT_EQ(res, "before");
  ASSERT_OK(conn.Execute("INSERT INTO test_table2 (value) values ('after')"));

  // demo.test_table3 should be dropped.
  LOG(INFO) << "Select from demo.test_table3";
  auto r = conn.FetchValue<std::string>("SELECT value FROM test_table3 where key=1");
  ASSERT_EQ(r.ok(), false);

  // yugabyte.test_table shouldn't be recreated.
  LOG(INFO) << "Select from yugabyte.test_table";
  r = conn_yugabyte.FetchValue<std::string>("SELECT value FROM test_table where key=1");
  ASSERT_EQ(r.ok(), false);

  // yugabyte.test_table2 should remain as it was.
  LOG(INFO) << "Select from yugabyte.test_table2";
  res = ASSERT_RESULT(
      conn_yugabyte.FetchValue<std::string>("SELECT value FROM test_table2 where key=1"));
  ASSERT_EQ(res, "before");
  ASSERT_OK(conn_yugabyte.Execute("INSERT INTO test_table2 (value) values ('after')"));

  // yugabyte.test_table3 should remain as it was.
  LOG(INFO) << "Select from yugabyte.test_table3";
  res = ASSERT_RESULT(
      conn_yugabyte.FetchValue<std::string>("SELECT value FROM test_table3 where key=1"));
  ASSERT_EQ(res, "before");
  ASSERT_OK(conn_yugabyte.Execute("INSERT INTO test_table3 (value) values ('after')"));

  // Verify that we are able to create more sequences post restore.
  LOG(INFO) << "Create table 'test_table_new'";
  ASSERT_OK(conn.Execute("CREATE TABLE test_table_new (key SERIAL, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table_new (value) values ('before')"));
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlTestTruncateDisallowedWithPitr),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));
  LOG(INFO) << "Create table 'test_table'";
  ASSERT_OK(conn.Execute("CREATE TABLE test_table (key INT PRIMARY KEY, value INT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 1)"));

  auto s = conn.Execute("TRUNCATE TABLE test_table");
  ASSERT_NOK(s);
  ASSERT_STR_CONTAINS(s.ToString(), "Cannot truncate table test_table which has schedule");

  LOG(INFO) << "Enable flag to allow truncate and validate that truncate succeeds";
  ASSERT_OK(cluster_->SetFlagOnMasters("enable_truncate_on_pitr_table", "true"));
  ASSERT_OK(conn.Execute("TRUNCATE TABLE test_table"));
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlDisableTablegroup),
          YbAdminSnapshotScheduleTestWithYsql) {
  auto schedule_id = ASSERT_RESULT(PreparePg());
  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  // Try creating tablegroup inside the database on which snapshot schedule is setup.
  auto res = conn.Execute("CREATE TABLEGROUP tg1");
  ASSERT_FALSE(res.ok());
  ASSERT_STR_CONTAINS(
      res.ToString(), "Cannot create tablegroup when there are one or more snapshot schedules");

  // Try to create tablegroup inside another database.
  auto conn1 = ASSERT_RESULT(PgConnect("yugabyte"));
  res = conn1.Execute("CREATE TABLEGROUP tg1");
  ASSERT_FALSE(res.ok());
  ASSERT_STR_CONTAINS(
      res.ToString(), "Cannot create tablegroup when there are one or more snapshot schedules");

  // Delete the snapshot schedule and try creating tablegroups.
  ASSERT_OK(DeleteSnapshotSchedule(schedule_id));

  ASSERT_OK(conn.Execute("CREATE TABLEGROUP tg1"));
  ASSERT_OK(conn1.Execute("CREATE TABLEGROUP tg2"));
  ASSERT_OK(conn.Execute("CREATE TABLE t1 (id int primary key) TABLEGROUP tg1"));
}

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlDisableScheduleOnTablegroups),
          YbAdminSnapshotScheduleTestWithYsql) {
  ASSERT_OK(PrepareCommon());
  auto conn1 = ASSERT_RESULT(PgConnect("yugabyte"));
  ASSERT_OK(conn1.ExecuteFormat("CREATE DATABASE $0", client::kTableName.namespace_name()));
  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  // Create a tablegroup.
  ASSERT_OK(conn.Execute("CREATE TABLEGROUP tg1"));

  // Try creating a snapshot schedule, it should fail.
  auto res = CreateSnapshotScheduleAndWaitSnapshot(
      "ysql." + client::kTableName.namespace_name(), kInterval, kRetention);
  ASSERT_FALSE(res.ok());
  ASSERT_STR_CONTAINS(
      res.ToString(), "Not allowed to create snapshot schedule "
                      "when one or more tablegroups exist");

  // Try creating snapshot schedule on another database, it should fail too.
  res = CreateSnapshotScheduleAndWaitSnapshot("ysql.yugabyte", kInterval, kRetention);
  ASSERT_FALSE(res.ok());
  ASSERT_STR_CONTAINS(
      res.ToString(), "Not allowed to create snapshot schedule "
                      "when one or more tablegroups exist");

  // Drop this tablegroup.
  ASSERT_OK(conn.Execute("DROP TABLEGROUP tg1"));

  // Now we should be able to create a snapshot schedule.
  auto schedule_id = ASSERT_RESULT(CreateSnapshotScheduleAndWaitSnapshot(
      "ysql." + client::kTableName.namespace_name(), kInterval, kRetention));
}

class YbAdminSnapshotScheduleUpgradeTestWithYsql : public YbAdminSnapshotScheduleTestWithYsql {
  std::vector<std::string> ExtraMasterFlags() override {
    // To speed up tests.
    std::string build_type;
    if (DEBUG_MODE) {
      build_type = "debug";
    } else {
      build_type = "release";
    }
    std::string old_sys_catalog_snapshot_full_path =
        old_sys_catalog_snapshot_path + old_sys_catalog_snapshot_name + "_" + build_type;
    return { "--snapshot_coordinator_cleanup_delay_ms=1000",
             "--snapshot_coordinator_poll_interval_ms=500",
             "--enable_automatic_tablet_splitting=true",
             "--enable_transactional_ddl_gc=false",
             "--allow_consecutive_restore=true",
             "--initial_sys_catalog_snapshot_path="+old_sys_catalog_snapshot_full_path };
  }
};

TEST_F(YbAdminSnapshotScheduleUpgradeTestWithYsql,
       YB_DISABLE_TEST_IN_TSAN(PgsqlTestOldSysCatalogSnapshot)) {
  auto schedule_id = ASSERT_RESULT(PreparePg());
  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  ASSERT_OK(conn.Execute("CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT)"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'before')"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  ASSERT_OK(conn.Execute("DROP TABLE test_table"));

  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  ASSERT_OK(restore_status);

  auto res = ASSERT_RESULT(conn.FetchValue<std::string>(
      "SELECT value FROM test_table WHERE key = 1"));
  ASSERT_EQ(res, "before");
  ASSERT_OK(conn.Execute("UPDATE test_table SET value = 'after'"));
  res = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table WHERE key = 1"));
  ASSERT_EQ(res, "after");
}

TEST_F(
    YbAdminSnapshotScheduleUpgradeTestWithYsql,
    YB_DISABLE_TEST_IN_TSAN(PgsqlTestMigrationFromEarliestSysCatalogSnapshot)) {
  auto schedule_id = ASSERT_RESULT(PreparePg());
  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));
  LOG(INFO) << "Assert pg_yb_migration table does not exist.";
  std::string query = "SELECT count(*) FROM pg_yb_migration LIMIT 1";
  auto query_status = conn.Execute(query);
  ASSERT_NOK(query_status);
  ASSERT_STR_CONTAINS(query_status.message().ToBuffer(), "does not exist");
  LOG(INFO) << "Save time to restore to.";
  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Run upgrade_ysql to create and populate pg_yb_migration table.";
  auto result = ASSERT_RESULT(CallAdmin("-timeout_ms", 10 * 60 * 1000, "upgrade_ysql"));
  LOG(INFO) << "Assert pg_yb_migration table exists.";
  ASSERT_RESULT(conn.FetchValue<int64_t>(query));
  auto restore_status = RestoreSnapshotSchedule(schedule_id, time);
  LOG(INFO) << "Assert restore fails because of system catalog changes.";
  ASSERT_NOK(restore_status);
  ASSERT_STR_CONTAINS(
      restore_status.message().ToBuffer(),
      "Snapshot state and current state have different system catalogs");
}

TEST_F(YbAdminSnapshotScheduleTest, UndeleteIndex) {
  auto schedule_id = ASSERT_RESULT(PrepareCql());

  auto conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));

  ASSERT_OK(conn.ExecuteQuery(
      "CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT) "
      "WITH transactions = { 'enabled' : true }"));
  ASSERT_OK(conn.ExecuteQuery("CREATE UNIQUE INDEX test_table_idx ON test_table (value)"));

  ASSERT_OK(conn.ExecuteQuery("INSERT INTO test_table (key, value) VALUES (1, 'value')"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

  ASSERT_OK(conn.ExecuteQuery("DROP INDEX test_table_idx"));

  ASSERT_OK(conn.ExecuteQuery("INSERT INTO test_table (key, value) VALUES (3, 'value')"));

  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time));

  ASSERT_NOK(conn.ExecuteQuery("INSERT INTO test_table (key, value) VALUES (5, 'value')"));

  auto res = ASSERT_RESULT(conn.FetchValue<int32_t>(
      "SELECT key FROM test_table WHERE value = 'value'"));

  ASSERT_EQ(res, 1);
}

// This test is for schema version patching after restore.
// Consider the following scenario, w/o patching:
//
// 1) Create table.
// 2) Add text column to table. Schema version - 1.
// 3) Insert values into table. Each CQL proxy suppose schema version 1 for this table.
// 4) Restore to time between (1) and (2). Schema version - 0.
// 5) Add int column to table. Schema version - 1.
// 6) Try insert values with wrong type into table.
//
// So table has schema version 1, but new column is INT.
// CQL proxy suppose schema version is also 1, but the last column is TEXT.
TEST_F(YbAdminSnapshotScheduleTest, AlterTable) {
  const auto kKeys = Range(10);

  auto schedule_id = ASSERT_RESULT(PrepareCql());

  auto conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));

  ASSERT_OK(conn.ExecuteQuery(
      "CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT)"));

  for (auto key : kKeys) {
    ASSERT_OK(conn.ExecuteQuery(Format(
        "INSERT INTO test_table (key, value) VALUES ($0, 'A')", key)));
  }

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

  ASSERT_OK(conn.ExecuteQuery(
      "ALTER TABLE test_table ADD value2 TEXT"));

  for (auto key : kKeys) {
    ASSERT_OK(conn.ExecuteQuery(Format(
        "INSERT INTO test_table (key, value, value2) VALUES ($0, 'B', 'X')", key)));
  }

  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time));

  ASSERT_OK(conn.ExecuteQuery(
      "ALTER TABLE test_table ADD value2 INT"));

  for (auto key : kKeys) {
    // It would succeed on some TServers if we would not refresh metadata after restore.
    // But it should not succeed because of last column type.
    ASSERT_NOK(conn.ExecuteQuery(Format(
        "INSERT INTO test_table (key, value, value2) VALUES ($0, 'D', 'Y')", key)));
  }
}

TEST_F(YbAdminSnapshotScheduleTest, TestVerifyRestorationLogic) {
  const auto kKeys = Range(10);

  auto schedule_id = ASSERT_RESULT(PrepareCql());

  auto conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));

  ASSERT_OK(conn.ExecuteQuery(
      "CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT)"));

  for (auto key : kKeys) {
    ASSERT_OK(conn.ExecuteQuery(Format(
        "INSERT INTO test_table (key, value) VALUES ($0, 'A')", key)));
  }

  ASSERT_OK(conn.ExecuteQuery("DROP TABLE test_table"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

  ASSERT_OK(conn.ExecuteQuery(
      "CREATE TABLE test_table1 (key INT PRIMARY KEY)"));

  for (auto key : kKeys) {
    ASSERT_OK(conn.ExecuteQuery(Format(
        "INSERT INTO test_table1 (key) VALUES ($0)", key)));
  }

  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time));

  // Reconnect because of caching issues with YCQL.
  conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));

  ASSERT_NOK(conn.ExecuteQuery("SELECT * from test_table"));

  ASSERT_OK(WaitFor([&conn]() -> Result<bool> {
    auto res = conn.ExecuteQuery("SELECT * from test_table1");
    if (res.ok()) {
      return false;
    }
    return true;
  }, 30s * kTimeMultiplier, "Wait for table to be deleted"));
}

TEST_F(YbAdminSnapshotScheduleTest, TestGCHiddenTables) {
  const auto interval = 15s;
  const auto retention = 30s * kTimeMultiplier;
  auto schedule_id = ASSERT_RESULT(PrepareQl(interval, retention));

  auto session = client_->NewSession();
  LOG(INFO) << "Create table";
  ASSERT_NO_FATALS(client::kv_table_test::CreateTable(
      client::Transactional::kTrue, 3, client_.get(), &table_));

  LOG(INFO) << "Write values";
  constexpr int kMinKey = 1;
  constexpr int kMaxKey = 100;
  for (int i = kMinKey; i <= kMaxKey; ++i) {
    ASSERT_OK(client::kv_table_test::WriteRow(&table_, session, i, -i));
  }

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

  LOG(INFO) << "Delete table";
  ASSERT_OK(client_->DeleteTable(client::kTableName));

  ASSERT_NOK(client::kv_table_test::WriteRow(&table_, session, kMinKey, 0));

  LOG(INFO) << "Restore schedule";
  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time));

  ASSERT_OK(table_.Open(client::kTableName, client_.get()));

  LOG(INFO) << "Reading rows";
  auto rows = ASSERT_RESULT(client::kv_table_test::SelectAllRows(&table_, session));
  LOG(INFO) << "Rows: " << AsString(rows);
  ASSERT_EQ(rows.size(), kMaxKey - kMinKey + 1);
  for (int i = kMinKey; i <= kMaxKey; ++i) {
    ASSERT_EQ(rows[i], -i);
  }

  // Wait for snapshot schedule retention time and verify that GC
  // for the table isn't initiated.
  SleepFor(2*retention);

  Timestamp time1(ASSERT_RESULT(WallClock()->Now()).time_point);

  LOG(INFO) << "Delete table again.";
  ASSERT_OK(client_->DeleteTable(client::kTableName));

  ASSERT_NOK(client::kv_table_test::WriteRow(&table_, session, kMinKey, 0));

  LOG(INFO) << "Restore schedule again.";
  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time1));

  ASSERT_OK(table_.Open(client::kTableName, client_.get()));

  LOG(INFO) << "Reading rows again.";
  rows = ASSERT_RESULT(client::kv_table_test::SelectAllRows(&table_, session));
  LOG(INFO) << "Rows: " << AsString(rows);
  ASSERT_EQ(rows.size(), kMaxKey - kMinKey + 1);
  for (int i = kMinKey; i <= kMaxKey; ++i) {
    ASSERT_EQ(rows[i], -i);
  }

  // Write and verify the extra row.
  constexpr int kExtraKey = kMaxKey + 1;
  ASSERT_OK(client::kv_table_test::WriteRow(&table_, session, kExtraKey, -kExtraKey));
  auto extra_value = ASSERT_RESULT(client::kv_table_test::SelectRow(&table_, session, kExtraKey));
  ASSERT_EQ(extra_value, -kExtraKey);
}

class YbAdminSnapshotConsistentRestoreTest : public YbAdminSnapshotScheduleTest {
 public:
  virtual std::vector<std::string> ExtraTSFlags() {
    return { "--consistent_restore=true", "--TEST_tablet_delay_restore_ms=0" };
  }
};

Status WaitWrites(int num, std::atomic<int>* current) {
  auto stop = current->load() + num;
  return WaitFor([current, stop] { return current->load() >= stop; },
                 20s, Format("Wait $0 ($1) writes", stop, num));
}

YB_DEFINE_ENUM(KeyState, (kNone)(kMissing)(kBeforeMissing)(kAfterMissing));

// Check that restoration is consistent across tablets.
// Concurrently write keys and store event stream, i.e when we started or finished to write key.
// After restore we fetch all keys and see what keys were removed during restore.
// So for each such key we could find what keys were written strictly before it,
// i.e. before restore. Or strictly after, i.e. after restore.
// Then we check that key is not marked as before and after restore simultaneously.
TEST_F_EX(YbAdminSnapshotScheduleTest, ConsistentRestore, YbAdminSnapshotConsistentRestoreTest) {
  auto schedule_id = ASSERT_RESULT(PrepareCql());

  auto conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));

  ASSERT_OK(conn.ExecuteQuery("CREATE TABLE test_table (k1 INT PRIMARY KEY)"));

  struct EventData {
    int key;
    bool finished;
  };

  std::atomic<int> written{0};
  TestThreadHolder thread_holder;

  std::vector<EventData> events;

  thread_holder.AddThreadFunctor([&conn, &written, &events, &stop = thread_holder.stop_flag()] {
    auto prepared = ASSERT_RESULT(conn.Prepare("INSERT INTO test_table (k1) VALUES (?)"));
    std::vector<std::pair<int, CassandraFuture>> futures;
    int key = 0;
    constexpr int kBlock = 10;
    while (!stop.load() || !futures.empty()) {
      auto filter = [&events, &written](auto& key_and_future) {
        if (!key_and_future.second.Ready()) {
          return false;
        }
        auto write_status = key_and_future.second.Wait();
        if (write_status.ok()) {
          events.push_back(EventData{.key = key_and_future.first, .finished = true});
          ++written;
        } else {
          LOG(WARNING) << "Write failed: " << write_status;
        }
        return true;
      };
      ASSERT_NO_FATALS(EraseIf(filter, &futures));
      if (futures.size() < kBlock && !stop.load()) {
        auto write_key = ++key;
        auto stmt = prepared.Bind();
        stmt.Bind(0, write_key);
        futures.emplace_back(write_key, conn.ExecuteGetFuture(stmt));
        events.push_back(EventData{.key = write_key, .finished = false});
      }
      std::this_thread::sleep_for(10ms);
    }
  });

  {
    auto se = ScopeExit([&thread_holder] {
      thread_holder.Stop();
    });
    ASSERT_OK(WaitWrites(50, &written));

    Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

    ASSERT_OK(WaitWrites(10, &written));

    ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time));

    ASSERT_OK(WaitWrites(10, &written));
  }

  struct KeyData {
    KeyState state;
    ssize_t start = -1;
    ssize_t finish = -1;
    ssize_t set_by = -1;
  };

  std::vector<KeyData> keys;

  for (;;) {
    keys.clear();
    auto result = conn.ExecuteWithResult("SELECT * FROM test_table");
    if (!result.ok()) {
      LOG(WARNING) << "Select failed: " << result.status();
      continue;
    }

    auto iter = result->CreateIterator();
    while (iter.Next()) {
      auto row = iter.Row();
      int key = row.Value(0).As<int32_t>();
      keys.resize(std::max<size_t>(keys.size(), key + 1), {.state = KeyState::kMissing});
      keys[key].state = KeyState::kNone;
    }
    break;
  }

  for (size_t i = 0; i != events.size(); ++i) {
    auto& event = events[i];
    auto& key_data = keys[event.key];
    if (key_data.state == KeyState::kMissing) {
      (event.finished ? key_data.finish : key_data.start) = i;
    }
  }

  for (size_t key = 1; key != keys.size(); ++key) {
    if (keys[key].state != KeyState::kMissing || keys[key].finish == -1) {
      continue;
    }
    for (auto set_state : {KeyState::kBeforeMissing, KeyState::kAfterMissing}) {
      auto begin = set_state == KeyState::kBeforeMissing ? 0 : keys[key].finish + 1;
      auto end = set_state == KeyState::kBeforeMissing ? keys[key].start : events.size();
      for (size_t i = begin; i != end; ++i) {
        auto& event = events[i];
        if (keys[event.key].state == KeyState::kMissing ||
            (event.finished != (set_state == KeyState::kBeforeMissing))) {
          continue;
        }
        if (keys[event.key].state == KeyState::kNone) {
          keys[event.key].state = set_state;
          keys[event.key].set_by = key;
        } else if (keys[event.key].state != set_state) {
          FAIL() << "Key " << event.key << " already marked as " << keys[event.key].state
                 << ", while trying to set: " << set_state << " with " << key << ", prev set: "
                 << keys[event.key].set_by;
        }
      }
    }
  }
}

// Write multiple transactions and restore.
// Then check that we don't have partially applied transaction.
TEST_F_EX(YbAdminSnapshotScheduleTest, ConsistentTxnRestore, YbAdminSnapshotConsistentRestoreTest) {
  constexpr int kBatchSize = 10;
  auto schedule_id = ASSERT_RESULT(PrepareCql());

  auto conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));

  ASSERT_OK(conn.ExecuteQuery("CREATE TABLE test_table (k1 INT PRIMARY KEY)"
                              "WITH transactions = { 'enabled' : true }"));
  TestThreadHolder thread_holder;
  thread_holder.AddThreadFunctor([&stop = thread_holder.stop_flag(), &conn] {
    const int kConcurrency = 10;
    std::string expr = "BEGIN TRANSACTION ";
    for (ATTRIBUTE_UNUSED int i : Range(kBatchSize)) {
      expr += "INSERT INTO test_table (k1) VALUES (?); ";
    }
    expr += "END TRANSACTION;";
    auto prepared = ASSERT_RESULT(conn.Prepare(expr));
    int base = 0;
    std::vector<CassandraFuture> futures;
    while (!stop.load(std::memory_order_acquire)) {
      auto filter = [](CassandraFuture& future) {
        if (!future.Ready()) {
          return false;
        }
        auto status = future.Wait();
        if (!status.ok() && !status.IsTimedOut()) {
          EXPECT_OK(status);
        }
        return true;
      };
      ASSERT_NO_FATALS(EraseIf(filter, &futures));
      if (futures.size() < kConcurrency) {
        auto stmt = prepared.Bind();
        for (int i : Range(kBatchSize)) {
          stmt.Bind(i, base + i);
        }
        base += kBatchSize;
        futures.push_back(conn.ExecuteGetFuture(stmt));
      }
    }
  });

  std::this_thread::sleep_for(250ms);

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

  std::this_thread::sleep_for(250ms);

  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time));

  thread_holder.WaitAndStop(250ms);

  std::vector<int> keys;
  for (;;) {
    keys.clear();
    auto result = conn.ExecuteWithResult("SELECT * FROM test_table");
    if (!result.ok()) {
      LOG(WARNING) << "Select failed: " << result.status();
      continue;
    }

    auto iter = result->CreateIterator();
    while (iter.Next()) {
      auto row = iter.Row();
      int key = row.Value(0).As<int32_t>();
      keys.push_back(key);
    }
    break;
  }

  std::sort(keys.begin(), keys.end());
  // Check that we have whole batches only.
  // Actually this check is little bit relaxed, but it is enough to catch the bug.
  for (size_t i : Range(keys.size())) {
    ASSERT_EQ(keys[i] % kBatchSize, i % kBatchSize)
        << "i: " << i << ", key: " << keys[i] << ", batch: "
        << AsString(RangeOfSize<size_t>((i / kBatchSize - 1) * kBatchSize, kBatchSize * 3)[keys]);
  }
}

// Tests that DDLs are blocked during restore.
TEST_F_EX(YbAdminSnapshotScheduleTest, DDLsDuringRestore, YbAdminSnapshotConsistentRestoreTest) {
  auto schedule_id = ASSERT_RESULT(PrepareCql());

  auto conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));

  ASSERT_OK(conn.ExecuteQuery("CREATE TABLE test_table (k1 INT PRIMARY KEY)"));
  ASSERT_OK(conn.ExecuteQuery("INSERT INTO test_table (k1) VALUES (1)"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Created table test_table";

  // Drop the table.
  ASSERT_OK(conn.ExecuteQuery("DROP TABLE test_table"));
  LOG(INFO) << "Dropped table test_table";

  // Introduce a delay between catalog patching and loading into memory.
  ASSERT_OK(cluster_->SetFlagOnMasters("TEST_delay_sys_catalog_reload_secs", "4"));

  // Now start restore.
  auto restoration_id = ASSERT_RESULT(StartRestoreSnapshotSchedule(schedule_id, time));
  LOG(INFO) << "Restored sys catalog metadata";

  // Issue DDLs in-between.
  ASSERT_OK(conn.ExecuteQuery("CREATE TABLE test_table2 (k1 INT PRIMARY KEY)"));
  ASSERT_OK(conn.ExecuteQuery("INSERT INTO test_table2 (k1) VALUES (1)"));
  LOG(INFO) << "Created table test_table2";

  ASSERT_OK(WaitRestorationDone(restoration_id, 40s));

  // Validate data.
  auto out = ASSERT_RESULT(conn.ExecuteAndRenderToString("SELECT * from test_table"));
  LOG(INFO) << "test_table entry: " << out;
  ASSERT_EQ(out, "1");

  out = ASSERT_RESULT(conn.ExecuteAndRenderToString("SELECT * from test_table2"));
  LOG(INFO) << "test_table2 entry: " << out;
  ASSERT_EQ(out, "1");
}

class YbAdminSnapshotConsistentRestoreFailoverTest : public YbAdminSnapshotScheduleTest {
 public:
  std::vector<std::string> ExtraTSFlags() override {
    return { "--consistent_restore=true" };
  }

  std::vector<std::string> ExtraMasterFlags() override {
    return { "--TEST_skip_sending_restore_finished=true" };
  }
};

TEST_F_EX(YbAdminSnapshotScheduleTest, ConsistentRestoreFailover,
          YbAdminSnapshotConsistentRestoreFailoverTest) {
  auto schedule_id = ASSERT_RESULT(PrepareCql());
  auto conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));

  ASSERT_OK(conn.ExecuteQuery("CREATE TABLE test_table (k1 INT PRIMARY KEY)"));
  auto expr = "INSERT INTO test_table (k1) VALUES ($0)";
  ASSERT_OK(conn.ExecuteQueryFormat(expr, 1));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

  ASSERT_OK(conn.ExecuteQueryFormat(expr, 2));

  auto restoration_id = ASSERT_RESULT(StartRestoreSnapshotSchedule(schedule_id, time));

  ASSERT_OK(WaitRestorationDone(restoration_id, 40s));

  for (auto* master : cluster_->master_daemons()) {
    master->Shutdown();
    ASSERT_OK(master->Restart());
  }

  ASSERT_OK(conn.ExecuteQueryFormat(expr, 3));

  auto rows = ASSERT_RESULT(conn.ExecuteAndRenderToString("SELECT * FROM test_table"));
  ASSERT_EQ(rows, "1;3");
}

TEST_F(YbAdminSnapshotScheduleTest, DropKeyspaceAndSchedule) {
  auto schedule_id = ASSERT_RESULT(PrepareCql(kInterval, kInterval));
  auto conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));
  ASSERT_OK(conn.ExecuteQuery(
      "CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT) "
      "WITH transactions = { 'enabled' : true }"));

  ASSERT_OK(conn.ExecuteQuery("INSERT INTO test_table (key, value) VALUES (1, 'before')"));
  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  ASSERT_OK(conn.ExecuteQuery("INSERT INTO test_table (key, value) VALUES (1, 'after')"));
  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time));
  auto res = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table"));
  ASSERT_EQ(res, "before");
  ASSERT_OK(conn.ExecuteQuery("DROP TABLE test_table"));
  // Wait until table completely removed, because of schedule retention.
  std::this_thread::sleep_for(kInterval * 3);
  ASSERT_NOK(conn.ExecuteQuery(Format("DROP KEYSPACE $0", client::kTableName.namespace_name())));
  ASSERT_OK(DeleteSnapshotSchedule(schedule_id));
  ASSERT_OK(conn.ExecuteQuery(Format("DROP KEYSPACE $0", client::kTableName.namespace_name())));
}

TEST_F(YbAdminSnapshotScheduleTest, DeleteIndexOnRestore) {
  auto schedule_id = ASSERT_RESULT(PrepareCql(kInterval, kInterval * 4));

  auto conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));

  ASSERT_OK(conn.ExecuteQuery(
      "CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT) "
      "WITH transactions = { 'enabled' : true }"));

  for (int i = 0; i != 3; ++i) {
    LOG(INFO) << "Iteration: " << i;
    ASSERT_OK(conn.ExecuteQuery("INSERT INTO test_table (key, value) VALUES (1, 'value')"));
    Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
    ASSERT_OK(conn.ExecuteQuery("CREATE UNIQUE INDEX test_table_idx ON test_table (value)"));
    std::this_thread::sleep_for(kInterval * 2);
    ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time));
  }

  auto snapshots = ASSERT_RESULT(ListSnapshots());
  LOG(INFO) << "Snapshots:\n" << common::PrettyWriteRapidJsonToString(snapshots);
  std::string id = ASSERT_RESULT(Get(snapshots[0], "id")).get().GetString();
  ASSERT_OK(WaitFor([this, &id]() -> Result<bool> {
    auto snapshots = VERIFY_RESULT(ListSnapshots());
    LOG(INFO) << "Snapshots:\n" << common::PrettyWriteRapidJsonToString(snapshots);
    auto current_id = VERIFY_RESULT(Get(snapshots[0], "id")).get().GetString();
    return current_id != id;
  }, kInterval * 3, "Wait first snapshot to be deleted"));
}

class YbAdminRestoreAfterSplitTest : public YbAdminSnapshotScheduleTest {
  std::vector<std::string> ExtraMasterFlags() override {
    return { "--snapshot_coordinator_cleanup_delay_ms=1000",
            "--snapshot_coordinator_poll_interval_ms=500",
            "--enable_automatic_tablet_splitting=false",
            "--enable_transactional_ddl_gc=false",
            "--allow_consecutive_restore=true"
    };
  }
};

TEST_F_EX(YbAdminSnapshotScheduleTest, RestoreAfterSplit, YbAdminRestoreAfterSplitTest) {
  const int kNumRows = 10000;
  // Create exactly one tserver so that we only have to invalidate one cache.
  FLAGS_num_tablet_servers = 1;
  FLAGS_num_replicas = 1;
  auto schedule_id = ASSERT_RESULT(PrepareCql());

  auto conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));

  ASSERT_OK(conn.ExecuteQueryFormat(
      "CREATE TABLE $0 (key INT PRIMARY KEY, value TEXT) "
      "WITH tablets = 1 AND transactions = { 'enabled' : true }",
      client::kTableName.table_name()));

  // Insert enough data conducive to splitting.
  int i;
  for (i = 0; i < kNumRows; i++) {
    ASSERT_OK(conn.ExecuteQueryFormat(
        "INSERT INTO $0 (key, value) VALUES ($1, 'before$2')",
        client::kTableName.table_name(), i, i));
  }
  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

  // This row should be absent after restoration.
  ASSERT_OK(conn.ExecuteQueryFormat(
      "INSERT INTO $0 (key, value) VALUES ($1, 'after')",
      client::kTableName.table_name(), i));

  {
    auto tablets_obj = ASSERT_RESULT(ListTablets());
    auto tablets = tablets_obj.GetArray();
    ASSERT_EQ(tablets.Size(), 1);
    auto tablet_id = ASSERT_RESULT(Get(tablets[0], "id")).get().GetString();
    LOG(INFO) << "Tablet id: " << tablet_id;

    // Flush the table to ensure that there's at least one sst file.
    ASSERT_OK(CallAdmin(
        "flush_table", Format("ycql.$0", client::kTableName.namespace_name()),
        client::kTableName.table_name()));

    // Split the tablet.
    LOG(INFO) << "Triggering a manual split.";
    ASSERT_OK(CallAdmin("split_tablet", tablet_id));
  }

  std::this_thread::sleep_for(kCleanupSplitTabletsInterval * 5);

  // Read data so that the partitions in the cache get updated to the
  // post-split values.
  LOG(INFO) << "Reading rows after split before restoration";
  auto select_query = Format("SELECT count(*) FROM $0", client::kTableName.table_name());
  auto rows = ASSERT_RESULT(conn.ExecuteAndRenderToString(select_query));
  LOG(INFO) << "Found #rows " << rows;
  ASSERT_EQ(stoi(rows), kNumRows + 1);

  // There should be 2 tablets since we split 1 to 2.
  auto tablets_obj = ASSERT_RESULT(ListTablets());
  auto tablets = tablets_obj.GetArray();
  LOG(INFO) << "Tablet size: " << tablets.Size();
  ASSERT_EQ(tablets.Size(), 2);

  // Perform a restoration.
  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time));

  LOG(INFO) << "Reading rows after restoration";
  select_query = Format(
      "SELECT count(*) FROM $0", client::kTableName.table_name());
  rows = ASSERT_RESULT(conn.ExecuteAndRenderToString(select_query));
  LOG(INFO) << "Found #rows " << rows;
  ASSERT_EQ(stoi(rows), kNumRows);

  auto tablets_size = ASSERT_RESULT(ListTablets()).GetArray().Size();
  ASSERT_EQ(tablets_size, 1);
}

TEST_F(YbAdminSnapshotScheduleTest, ConsecutiveRestore) {
  const auto retention = kInterval * 5 * kTimeMultiplier;
  auto schedule_id = ASSERT_RESULT(PrepareCql(kInterval, retention));

  auto conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));

  std::this_thread::sleep_for(FLAGS_max_clock_skew_usec * 1us);

  Timestamp time1(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time1: " << time1;

  ASSERT_OK(conn.ExecuteQueryFormat(
      "CREATE TABLE $0 (key INT PRIMARY KEY, value TEXT) WITH tablets = 1",
      client::kTableName.table_name()));

  auto insert_pattern = Format(
      "INSERT INTO $0 (key, value) VALUES (1, '$$0')", client::kTableName.table_name());
  ASSERT_OK(conn.ExecuteQueryFormat(insert_pattern, "before"));
  Timestamp time2(ASSERT_RESULT(WallClock()->Now()).time_point);
  ASSERT_OK(conn.ExecuteQueryFormat(insert_pattern, "after"));

  auto select_expr = Format("SELECT * FROM $0", client::kTableName.table_name());
  auto rows = ASSERT_RESULT(conn.ExecuteAndRenderToString(select_expr));
  ASSERT_EQ(rows, "1,after");

  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time1));

  std::this_thread::sleep_for(3s * kTimeMultiplier);

  ASSERT_NOK(conn.ExecuteAndRenderToString(select_expr));

  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time2));

  rows = ASSERT_RESULT(conn.ExecuteAndRenderToString(select_expr));
  ASSERT_EQ(rows, "1,before");

  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time1));

  ASSERT_OK(WaitTabletsCleaned(CoarseMonoClock::now() + retention + kInterval));
}

TEST_F(YbAdminSnapshotScheduleTest, CatalogLoadRace) {
  auto schedule_id = ASSERT_RESULT(PrepareCql());

  auto conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));

  // Change the snapshot throttling flags.
  ASSERT_OK(cluster_->SetFlagOnMasters("max_concurrent_snapshot_rpcs", "-1"));
  ASSERT_OK(cluster_->SetFlagOnMasters("max_concurrent_snapshot_rpcs_per_tserver", "1"));
  ASSERT_OK(cluster_->SetFlagOnMasters("schedule_snapshot_rpcs_out_of_band", "true"));
  // Delay loading of cluster config by 2 secs (i.e. 4 cycles of snapshot coordinator).
  // This ensures that the snapshot coordinator accesses an empty cluster config at least once
  // and thus triggers the codepath where the case is handled
  // and a default value is used for throttling.
  ASSERT_OK(cluster_->SetFlagOnMasters("TEST_slow_cluster_config_load_secs", "2"));

  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);
  // Restore to trigger loading cluster config.
  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time));
}

class YbAdminSnapshotScheduleFlushTest : public YbAdminSnapshotScheduleTest {
 public:
  std::vector<std::string> ExtraMasterFlags() override {
    // To speed up tests.
    return { "--snapshot_coordinator_cleanup_delay_ms=1000",
             "--snapshot_coordinator_poll_interval_ms=500",
             "--enable_automatic_tablet_splitting=true",
             "--enable_transactional_ddl_gc=false",
             "--flush_rocksdb_on_shutdown=false",
             "--vmodule=tablet_bootstrap=3" };
  }
};

TEST_F_EX(YbAdminSnapshotScheduleTest, TestSnapshotBootstrap, YbAdminSnapshotScheduleFlushTest) {
  LOG(INFO) << "Create cluster";
  CreateCluster(kClusterName, ExtraTSFlags(), ExtraMasterFlags());

  // Disable modifying flushed frontier when snapshot is created.
  ASSERT_OK(cluster_->SetFlagOnMasters("TEST_modify_flushed_frontier_snapshot_op", "false"));

  // Create a database and a table.
  auto conn = ASSERT_RESULT(CqlConnect());
  ASSERT_OK(conn.ExecuteQuery(Format(
      "CREATE KEYSPACE IF NOT EXISTS $0", client::kTableName.namespace_name())));

  conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));

  ASSERT_OK(conn.ExecuteQueryFormat(
      "CREATE TABLE $0 (key INT PRIMARY KEY, value TEXT) WITH tablets = 1",
      client::kTableName.table_name()));
  LOG(INFO) << "Created Keyspace and table";

  // Create a CREATE_ON_MASTER op in WALs without flushing frontier.
  ASSERT_OK(CallAdmin("create_keyspace_snapshot",
                      Format("ycql.$0", client::kTableName.namespace_name())));
  SleepFor(MonoDelta::FromSeconds(5 * kTimeMultiplier));
  LOG(INFO) << "Created snapshot on keyspace";

  // Enable modifying flushed frontier when snapshot is replayed.
  LOG(INFO) << "Resetting test flag to modify flushed frontier";

  // Restart the masters so that this op gets replayed.
  ASSERT_OK(cluster_->SetFlagOnMasters("TEST_modify_flushed_frontier_snapshot_op", "true"));
  LOG(INFO) << "Restart#1";
  cluster_->Shutdown();
  ASSERT_OK(cluster_->Restart());

  // Restart the masters again. Now this op shouldn't be replayed.
  LOG(INFO) << "Restart#2";
  cluster_->Shutdown();
  ASSERT_OK(cluster_->Restart());
}

class YbAdminSnapshotScheduleFailoverTests : public YbAdminSnapshotScheduleTest {
 public:
  std::vector<std::string> ExtraMasterFlags() override {
    // Slow down restoration rpcs.
    return { "--snapshot_coordinator_cleanup_delay_ms=1000",
             "--snapshot_coordinator_poll_interval_ms=5000",
             "--enable_automatic_tablet_splitting=true",
             "--max_concurrent_restoration_rpcs=1",
             "--schedule_restoration_rpcs_out_of_band=false",
             "--vmodule=tablet_bootstrap=4" };
  }
};

TEST_F(YbAdminSnapshotScheduleFailoverTests, LeaderFailoverDuringRestorationRpcs) {
  auto schedule_id = ASSERT_RESULT(PrepareCql());
  LOG(INFO) << "Snapshot schedule id " << schedule_id;

  auto conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));

  // Create a table with large number of tablets.
  ASSERT_OK(conn.ExecuteQueryFormat(
      "CREATE TABLE $0 (key INT PRIMARY KEY, value TEXT) WITH TABLETS = 24",
      client::kTableName.table_name()));

  // Insert some data.
  ASSERT_OK(conn.ExecuteQueryFormat(
      "INSERT INTO $0 (key, value) values (1, 'before')",
      client::kTableName.table_name()));

  LOG(INFO) << "Created Keyspace and table";

  // Record time for restoring.
  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

  // Drop the table.
  ASSERT_OK(conn.ExecuteQueryFormat(
      "DROP TABLE $0", client::kTableName.table_name()));
  LOG(INFO) << "Dropped the table";

  // Now start restore to the noted time. Since the RPCs are slow, we can failover the master
  // leader when they are in progress to see if restoration still completes.
  auto restoration_id = ASSERT_RESULT(StartRestoreSnapshotSchedule(schedule_id, time));

  ASSERT_OK(cluster_->StepDownMasterLeaderAndWaitForNewLeader());
  LOG(INFO) << "Master leader changed";

  // Now speed up rpcs.
  ASSERT_OK(cluster_->SetFlagOnMasters("schedule_restoration_rpcs_out_of_band", "true"));
  ASSERT_OK(cluster_->SetFlagOnMasters("max_concurrent_restoration_rpcs", "9"));
  ASSERT_OK(cluster_->SetFlagOnMasters("snapshot_coordinator_poll_interval_ms", "500"));

  ASSERT_OK(WaitRestorationDone(restoration_id, 120s * kTimeMultiplier));

  // Validate data.
  auto select_expr = Format("SELECT * FROM $0", client::kTableName.table_name());
  auto rows = ASSERT_RESULT(conn.ExecuteAndRenderToString(select_expr));
  LOG(INFO) << "Data after restoration: " << rows;
  ASSERT_EQ(rows, "1,before");
}

TEST_F(YbAdminSnapshotScheduleFailoverTests, ClusterRestartDuringRestore) {
  auto schedule_id = ASSERT_RESULT(PrepareCql());
  LOG(INFO) << "Snapshot schedule id " << schedule_id;

  auto conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));

  // Create a table with large number of tablets.
  ASSERT_OK(conn.ExecuteQueryFormat(
      "CREATE TABLE $0 (key INT PRIMARY KEY, value TEXT) WITH TABLETS = 24",
      client::kTableName.table_name()));

  // Insert some data.
  ASSERT_OK(conn.ExecuteQueryFormat(
      "INSERT INTO $0 (key, value) values (1, 'before')",
      client::kTableName.table_name()));

  LOG(INFO) << "Created Keyspace and table";

  // Record time for restoring.
  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

  // Drop the table.
  ASSERT_OK(conn.ExecuteQueryFormat(
      "DROP TABLE $0", client::kTableName.table_name()));
  LOG(INFO) << "Dropped the table";

  // Now start restore to the noted time. Since the RPCs are slow, we can restart
  // the cluster in the meantime.
  auto restoration_id = ASSERT_RESULT(StartRestoreSnapshotSchedule(schedule_id, time));

  LOG(INFO) << "Now restarting cluster";
  cluster_->Shutdown();
  ASSERT_OK(cluster_->Restart());
  LOG(INFO) << "Cluster restarted";

  // Now speed up rpcs.
  ASSERT_OK(cluster_->SetFlagOnMasters("schedule_restoration_rpcs_out_of_band", "true"));
  ASSERT_OK(cluster_->SetFlagOnMasters("max_concurrent_restoration_rpcs", "9"));
  ASSERT_OK(cluster_->SetFlagOnMasters("snapshot_coordinator_poll_interval_ms", "500"));

  ASSERT_OK(WaitRestorationDone(restoration_id, 120s * kTimeMultiplier));

  // Validate data.
  auto select_expr = Format("SELECT * FROM $0", client::kTableName.table_name());
  auto rows = ASSERT_RESULT(conn.ExecuteAndRenderToString(select_expr));
  LOG(INFO) << "Data after restoration: " << rows;
  ASSERT_EQ(rows, "1,before");
}

TEST_F(YbAdminSnapshotScheduleFailoverTests, LeaderFailoverDuringSysCatalogRestorationPhase) {
  auto schedule_id = ASSERT_RESULT(PrepareCql());
  LOG(INFO) << "Snapshot schedule id " << schedule_id;

  auto conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));

  // Create table with large number of tablets.
  ASSERT_OK(conn.ExecuteQueryFormat(
      "CREATE TABLE $0 (key INT PRIMARY KEY, value TEXT) WITH TABLETS = 24",
      client::kTableName.table_name()));

  // Insert some data.
  ASSERT_OK(conn.ExecuteQueryFormat(
      "INSERT INTO $0 (key, value) values (1, 'before')",
      client::kTableName.table_name()));

  LOG(INFO) << "Created Keyspace and table";

  // Set the crash flag only on the leader master.
  auto* leader = cluster_->GetLeaderMaster();
  ASSERT_OK(cluster_->SetFlag(leader, "TEST_crash_during_sys_catalog_restoration", "1.0"));
  LOG(INFO) << "Crash flag set on the leader master";

  // Record time for restoring.
  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

  // Drop the table.
  ASSERT_OK(conn.ExecuteQueryFormat(
      "DROP TABLE $0", client::kTableName.table_name()));
  LOG(INFO) << "Table dropped";

  // Now start restore to the noted time. Because of the test flag, the master leader
  // will crash during the sys catalog restoration phase. We can then validate if
  // restoration still succeeds.
  auto restoration_id_result = StartRestoreSnapshotSchedule(schedule_id, time);
  ASSERT_NOK(restoration_id_result);

  // Since we don't have the restoration id, query the new master leader for it.
  auto restoration_ids = ASSERT_RESULT(GetAllRestorationIds());
  ASSERT_EQ(restoration_ids.size(), 1);
  LOG(INFO) << "Restoration id " << restoration_ids[0];

  // Now speed up rpcs.
  auto* new_leader = cluster_->GetLeaderMaster();
  ASSERT_OK(cluster_->SetFlag(new_leader, "schedule_restoration_rpcs_out_of_band", "true"));
  ASSERT_OK(cluster_->SetFlag(new_leader, "max_concurrent_restoration_rpcs", "9"));
  ASSERT_OK(cluster_->SetFlag(new_leader, "snapshot_coordinator_poll_interval_ms", "500"));

  ASSERT_OK(WaitRestorationDone(restoration_ids[0], 120s * kTimeMultiplier));

  // Validate data.
  auto select_expr = Format("SELECT * FROM $0", client::kTableName.table_name());
  auto rows = ASSERT_RESULT(conn.ExecuteAndRenderToString(select_expr));
  LOG(INFO) << "Rows: " << rows;
  ASSERT_EQ(rows, "1,before");

  // Restart all masters just so that the test doesn't fail during teardown().
  ASSERT_OK(RestartAllMasters(cluster_.get()));
}

TEST_F(YbAdminSnapshotScheduleFailoverTests, LeaderFailoverRestoreSnapshot) {
  LOG(INFO) << "Create cluster";
  CreateCluster(kClusterName, ExtraTSFlags(), ExtraMasterFlags());

  // Create a database and a table.
  auto conn = ASSERT_RESULT(CqlConnect());
  ASSERT_OK(conn.ExecuteQuery(Format(
      "CREATE KEYSPACE IF NOT EXISTS $0", client::kTableName.namespace_name())));

  conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));

  // Create table with large number of tablets.
  ASSERT_OK(conn.ExecuteQueryFormat(
      "CREATE TABLE $0 (key INT PRIMARY KEY, value TEXT) WITH tablets = 24",
      client::kTableName.table_name()));

  // Insert some data.
  ASSERT_OK(conn.ExecuteQueryFormat(
      "INSERT INTO $0 (key, value) values (1, 'before')",
      client::kTableName.table_name()));
  LOG(INFO) << "Created Keyspace and table";

  // Create a snapshot.
  auto out = ASSERT_RESULT(
      CallAdmin("create_keyspace_snapshot",
                Format("ycql.$0", client::kTableName.namespace_name())));

  vector<string> admin_result = strings::Split(out, ": ");
  std::string snapshot_id = admin_result[1].substr(0, admin_result[1].size() - 1);
  LOG(INFO) << "Snapshot id " << snapshot_id;

  // Wait for snapshot to be created.
  ASSERT_OK(WaitFor([this]() -> Result<bool> {
    std::string out = VERIFY_RESULT(CallAdmin("list_snapshots"));
    LOG(INFO) << out;
    return out.find("COMPLETE") != std::string::npos;
  }, 120s, "Wait for snapshot to be created"));

  // Update the entry.
  ASSERT_OK(conn.ExecuteQueryFormat(
      "UPDATE $0 SET value='after' where key=1",
      client::kTableName.table_name()));
  auto select_expr = Format("SELECT * FROM $0", client::kTableName.table_name());
  auto rows = ASSERT_RESULT(conn.ExecuteAndRenderToString(select_expr));
  LOG(INFO) << "Rows after update: " << rows;
  ASSERT_EQ(rows, "1,after");

  // Restore this snapshot now.
  out = ASSERT_RESULT(CallAdmin("restore_snapshot", snapshot_id));

  // Failover the leader.
  ASSERT_OK(cluster_->StepDownMasterLeaderAndWaitForNewLeader());
  LOG(INFO) << "Failed over the master leader";

  // Now speed up rpcs.
  ASSERT_OK(cluster_->SetFlagOnMasters("schedule_restoration_rpcs_out_of_band", "true"));
  ASSERT_OK(cluster_->SetFlagOnMasters("max_concurrent_restoration_rpcs", "9"));
  ASSERT_OK(cluster_->SetFlagOnMasters("snapshot_coordinator_poll_interval_ms", "500"));

  // Wait for restoration to finish.
  ASSERT_OK(WaitFor([this]() -> Result<bool> {
    std::string out = VERIFY_RESULT(CallAdmin("list_snapshots"));
    LOG(INFO) << out;
    return out.find("RESTORED") != std::string::npos;
  }, 120s, "Wait for restoration to complete"));

  // Validate data.
  select_expr = Format("SELECT * FROM $0", client::kTableName.table_name());
  rows = ASSERT_RESULT(conn.ExecuteAndRenderToString(select_expr));
  LOG(INFO) << "Rows after restoration: " << rows;
  ASSERT_EQ(rows, "1,before");
}

class YbAdminSnapshotScheduleTestWithoutConsecutiveRestore : public YbAdminSnapshotScheduleTest {
  std::vector<std::string> ExtraMasterFlags() override {
    // To speed up tests.
    return { "--snapshot_coordinator_cleanup_delay_ms=1000",
             "--snapshot_coordinator_poll_interval_ms=500",
             "--enable_automatic_tablet_splitting=true",
             "--enable_transactional_ddl_gc=false",
             "--allow_consecutive_restore=false" };
  }
};

TEST_F(YbAdminSnapshotScheduleTestWithoutConsecutiveRestore, DisallowConsecutiveRestore) {
  const auto retention = kInterval * 5 * kTimeMultiplier;
  auto schedule_id = ASSERT_RESULT(PrepareCql(kInterval, retention));

  auto conn = ASSERT_RESULT(CqlConnect(client::kTableName.namespace_name()));

  std::this_thread::sleep_for(FLAGS_max_clock_skew_usec * 1us);

  Timestamp time1(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time1: " << time1;

  ASSERT_OK(conn.ExecuteQueryFormat(
      "CREATE TABLE $0 (key INT PRIMARY KEY, value TEXT) WITH tablets = 1",
      client::kTableName.table_name()));

  auto insert_pattern = Format(
      "INSERT INTO $0 (key, value) VALUES (1, '$$0')", client::kTableName.table_name());
  ASSERT_OK(conn.ExecuteQueryFormat(insert_pattern, "before"));
  Timestamp time2(ASSERT_RESULT(WallClock()->Now()).time_point);
  ASSERT_OK(conn.ExecuteQueryFormat(insert_pattern, "after"));

  auto select_expr = Format("SELECT * FROM $0", client::kTableName.table_name());
  auto rows = ASSERT_RESULT(conn.ExecuteAndRenderToString(select_expr));
  ASSERT_EQ(rows, "1,after");

  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time1));

  std::this_thread::sleep_for(3s * kTimeMultiplier);

  auto s = conn.ExecuteAndRenderToString(select_expr);
  ASSERT_NOK(s);
  ASSERT_STR_CONTAINS(s.status().message().ToBuffer(), "Object Not Found");

  Status s2 = RestoreSnapshotSchedule(schedule_id, time2);
  ASSERT_NOK(s2);
  ASSERT_STR_CONTAINS(
      s2.message().ToBuffer(), "Cannot restore before the previous restoration time");

  Timestamp time3(ASSERT_RESULT(WallClock()->Now()).time_point);
  LOG(INFO) << "Time3: " << time1;

  ASSERT_OK(conn.ExecuteQueryFormat(
      "CREATE TABLE $0 (key INT PRIMARY KEY, value TEXT) WITH tablets = 1",
      client::kTableName.table_name()));

  ASSERT_OK(conn.ExecuteQueryFormat(insert_pattern, "after"));

  rows = ASSERT_RESULT(conn.ExecuteAndRenderToString(select_expr));
  ASSERT_EQ(rows, "1,after");

  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time3));

  std::this_thread::sleep_for(3s * kTimeMultiplier);

  s = conn.ExecuteAndRenderToString(select_expr);
  ASSERT_NOK(s);
  ASSERT_STR_CONTAINS(s.status().message().ToBuffer(), "Object Not Found");
}

class YbAdminSnapshotScheduleTestWithLB : public YbAdminSnapshotScheduleTest {
  std::vector<std::string> ExtraMasterFlags() override {
    std::vector<std::string> flags;
    flags = YbAdminSnapshotScheduleTest::ExtraMasterFlags();
    flags.push_back("--enable_load_balancing=true");

    return flags;
  }

 public:
  void WaitForLoadBalanceCompletion(yb::MonoDelta timeout) {
    ASSERT_OK(WaitFor([&]() -> Result<bool> {
      bool is_idle = VERIFY_RESULT(client_->IsLoadBalancerIdle());
      return !is_idle;
    }, timeout, "IsLoadBalancerActive"));

    ASSERT_OK(WaitFor([&]() -> Result<bool> {
      return client_->IsLoadBalancerIdle();
    }, timeout, "IsLoadBalancerIdle"));
  }

  Result<std::vector<uint32_t>> GetTServerLoads(yb::MonoDelta timeout) {
    std::vector<uint32_t> tserver_loads;
    for (size_t i = 0; i != cluster_->num_tablet_servers(); ++i) {
      auto proxy = cluster_->GetTServerProxy<tserver::TabletServerServiceProxy>(i);
      tserver::ListTabletsRequestPB req;
      tserver::ListTabletsResponsePB resp;
      rpc::RpcController controller;
      controller.set_timeout(timeout);
      RETURN_NOT_OK(proxy.ListTablets(req, &resp, &controller));
      int tablet_count = 0;
      for (const auto& tablet : resp.status_and_schema()) {
        if (tablet.tablet_status().table_type() == TableType::TRANSACTION_STATUS_TABLE_TYPE) {
          continue;
        }
        if (tablet.tablet_status().namespace_name() == client::kTableName.namespace_name()) {
          if (tablet.tablet_status().tablet_data_state() != tablet::TABLET_DATA_TOMBSTONED) {
            ++tablet_count;
          }
        }
      }
      LOG(INFO) << "For TS " << cluster_->tablet_server(i)->id() << ", load: " << tablet_count;
      tserver_loads.push_back(tablet_count);
    }
    return tserver_loads;
  }

  void WaitForLoadToBeBalanced(yb::MonoDelta timeout) {
    ASSERT_OK(WaitFor([&]() -> Result<bool> {
      auto tserver_loads = VERIFY_RESULT(GetTServerLoads(timeout));
      return integration_tests::AreLoadsBalanced(tserver_loads);
    }, timeout, "Are loads balanced"));
  }
};

TEST_F(YbAdminSnapshotScheduleTestWithLB, TestLBHiddenTables) {
  // Create a schedule.
  auto schedule_id = ASSERT_RESULT(PrepareCql());

  // Create a table with 8 tablets.
  LOG(INFO) << "Create table " << client::kTableName.table_name() << " with 8 tablets";
  ASSERT_NO_FATALS(client::kv_table_test::CreateTable(
      client::Transactional::kTrue, 8, client_.get(), &table_));

  // Drop the table so that it becomes Hidden.
  LOG(INFO) << "Hiding table " << client::kTableName.table_name();
  ASSERT_OK(client_->DeleteTable(client::kTableName));

  // Add a tserver and wait for LB to balance the load.
  LOG(INFO) << "Adding a fourth tablet server";
  std::vector<std::string> ts_flags = ExtraTSFlags();
  ASSERT_OK(cluster_->AddTabletServer(true, ts_flags));
  ASSERT_OK(cluster_->WaitForTabletServerCount(4, 30s));

  // Wait for LB to be idle.
  WaitForLoadBalanceCompletion(30s * kTimeMultiplier * 10);

  // Validate loads are balanced.
  WaitForLoadToBeBalanced(30s * kTimeMultiplier * 10);
}

class YbAdminSnapshotScheduleTestWithLBYsql : public YbAdminSnapshotScheduleTestWithLB {
 public:
  void UpdateMiniClusterOptions(ExternalMiniClusterOptions* opts) override {
    opts->enable_ysql = true;
    opts->extra_tserver_flags.emplace_back("--ysql_num_shards_per_tserver=1");
    opts->num_masters = 3;
    opts->extra_master_flags.emplace_back("--enable_ysql_tablespaces_for_placement=true");
  }

  // Adds tserver in c1.r1 and specified zone.
  Status AddTServerInZone(const std::string& zone, int count) {
    std::vector<std::string> ts_flags = ExtraTSFlags();
    ts_flags.push_back("--placement_cloud=c1");
    ts_flags.push_back("--placement_region=r1");
    ts_flags.push_back(Format("--placement_zone=$0", zone));
    RETURN_NOT_OK(cluster_->AddTabletServer(true, ts_flags));
    RETURN_NOT_OK(cluster_->WaitForTabletServerCount(count, 30s));
    return Status::OK();
  }

  std::string GetCreateTablespaceCommand() {
    return "create tablespace demo_ts with (replica_placement='{\"num_replicas\": 3, "
           "\"placement_blocks\": [{\"cloud\":\"c1\", \"region\":\"r1\", "
           "\"zone\":\"z1\", \"min_num_replicas\":1}, "
           "{\"cloud\":\"c1\", \"region\":\"r1\", \"zone\":\"z2\", "
           "\"min_num_replicas\":1}, {\"cloud\":\"c1\", \"region\":\"r1\", "
           "\"zone\":\"z3\", \"min_num_replicas\":1}]}')";
  }
};

TEST_F_EX(YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlPreventTablespaceDrop),
          YbAdminSnapshotScheduleTestWithLBYsql) {
  // Start a cluster with 3 nodes. Create a snapshot schedule on a db.
  auto schedule_id = ASSERT_RESULT(PreparePg());
  LOG(INFO) << "Cluster started with 3 nodes";

  // Add 3 more tablet servers in custom placments.
  ASSERT_OK(AddTServerInZone("z1", 4));
  ASSERT_OK(AddTServerInZone("z2", 5));
  ASSERT_OK(AddTServerInZone("z3", 6));
  LOG(INFO) << "Added 3 more tservers in z1, z2 and z3";

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  // Create a tablespace and an associated table.
  std::string tblspace_command = GetCreateTablespaceCommand();

  LOG(INFO) << "Tablespace command: " << tblspace_command;
  ASSERT_OK(conn.Execute(tblspace_command));

  ASSERT_OK(conn.Execute(
      "CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT) TABLESPACE demo_ts"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'before')"));
  LOG(INFO) << "Created tablespace and table";

  // Now drop the table.
  ASSERT_OK(conn.Execute("DROP TABLE test_table"));
  LOG(INFO) << "Dropped the table test_table";

  // Try dropping the tablespace, it should fail.
  auto res = conn.Execute("DROP TABLESPACE demo_ts");
  LOG(INFO) << res.ToString();
  ASSERT_FALSE(res.ok());
  ASSERT_STR_CONTAINS(
      res.ToString(), "Dropping tablespaces is not allowed on clusters "
                      "with Point in Time Restore activated");

  // Delete the schedule.
  ASSERT_OK(DeleteSnapshotSchedule(schedule_id));
  LOG(INFO) << "Deleted snapshot schedule successfully";

  // Now drop the tablespace, it should succeed.
  ASSERT_OK(conn.Execute("DROP TABLESPACE demo_ts"));
  LOG(INFO) << "Successfully dropped the tablespace";
}

TEST_F_EX(
    YbAdminSnapshotScheduleTest, YB_DISABLE_TEST_IN_TSAN(PgsqlRestoreDroppedTableWithTablespace),
    YbAdminSnapshotScheduleTestWithLBYsql) {
  // Start a cluster with 3 nodes. Create a snapshot schedule on a db.
  auto schedule_id = ASSERT_RESULT(PreparePg());
  LOG(INFO) << "Cluster started with 3 nodes";

  // Add 3 more tablet servers in custom placments.
  ASSERT_OK(AddTServerInZone("z1", 4));
  ASSERT_OK(AddTServerInZone("z2", 5));
  ASSERT_OK(AddTServerInZone("z3", 6));
  LOG(INFO) << "Added 3 more tservers in z1, z2 and z3";

  auto conn = ASSERT_RESULT(PgConnect(client::kTableName.namespace_name()));

  // Create a tablespace and an associated table.
  std::string tblspace_command = GetCreateTablespaceCommand();

  LOG(INFO) << "Tablespace command: " << tblspace_command;
  ASSERT_OK(conn.Execute(tblspace_command));

  ASSERT_OK(conn.Execute(
      "CREATE TABLE test_table (key INT PRIMARY KEY, value TEXT) "
      "TABLESPACE demo_ts SPLIT INTO 24 TABLETS"));
  ASSERT_OK(conn.Execute("INSERT INTO test_table VALUES (1, 'before')"));
  LOG(INFO) << "Created tablespace and table with 24 tablets";

  // Wait for some time before noting down the time.
  SleepFor(MonoDelta::FromSeconds(5 * kTimeMultiplier));

  // Note down the time.
  Timestamp time(ASSERT_RESULT(WallClock()->Now()).time_point);

  // Now drop the table.
  ASSERT_OK(conn.Execute("DROP TABLE test_table"));
  LOG(INFO) << "Dropped the table test_table";

  // Restore to the time when the table existed.
  ASSERT_OK(RestoreSnapshotSchedule(schedule_id, time));

  // Verify data.
  auto res = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table"));
  LOG(INFO) << "Got value " << res;
  ASSERT_EQ(res, "before");

  // Add another tserver in z1, the load should get evenly balanced.
  ASSERT_OK(AddTServerInZone("z1", 7));
  LOG(INFO) << "Added tserver 7";

  // Validate loads are balanced.
  ASSERT_OK(WaitFor([&]() -> Result<bool> {
    auto tserver_loads = VERIFY_RESULT(GetTServerLoads(30s * kTimeMultiplier * 10));
    for (size_t i = 0; i < cluster_->num_tablet_servers(); i++) {
      if (i < 3 && tserver_loads[i] != 0) {
        return false;
      }
      if ((i == 3 || i == 6) && tserver_loads[i] != 12) {
        return false;
      }
      if ((i == 4 || i == 5) && tserver_loads[i] != 24) {
        return false;
      }
    }
    return true;
  }, 30s * kTimeMultiplier * 10, "Are loads balanced"));
  LOG(INFO) << "Loads are now balanced";

  // Verify table is still functional.
  ASSERT_OK(conn.Execute("INSERT INTO test_table (key, value) VALUES (2, 'after')"));
  res = ASSERT_RESULT(conn.FetchValue<std::string>("SELECT value FROM test_table WHERE key=2"));
  LOG(INFO) << "Got value " << res;
  ASSERT_EQ(res, "after");
}

}  // namespace tools
}  // namespace yb
