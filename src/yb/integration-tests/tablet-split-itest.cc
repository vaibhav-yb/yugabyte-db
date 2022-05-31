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

#include <chrono>
#include <limits>
#include <thread>

#include <gtest/gtest.h>

#include "yb/client/table.h"
#include "yb/client/table_alterer.h"

#include "yb/common/entity_ids_types.h"
#include "yb/common/ql_expr.h"
#include "yb/common/ql_value.h"
#include "yb/common/schema.h"
#include "yb/common/wire_protocol.h"

#include "yb/consensus/consensus.h"
#include "yb/consensus/consensus.pb.h"
#include "yb/consensus/consensus.proxy.h"
#include "yb/consensus/consensus_util.h"
#include "yb/consensus/raft_consensus.h"

#include "yb/docdb/doc_key.h"

#include "yb/fs/fs_manager.h"

#include "yb/gutil/dynamic_annotations.h"
#include "yb/gutil/strings/join.h"
#include "yb/gutil/strings/util.h"

#include "yb/integration-tests/cluster_itest_util.h"
#include "yb/integration-tests/redis_table_test_base.h"
#include "yb/integration-tests/tablet-split-itest-base.h"
#include "yb/integration-tests/test_workload.h"

#include "yb/master/catalog_entity_info.h"
#include "yb/master/catalog_manager_if.h"
#include "yb/master/master_admin.proxy.h"
#include "yb/master/master_admin.pb.h"
#include "yb/master/master_client.pb.h"
#include "yb/master/master_cluster.proxy.h"
#include "yb/master/master_defaults.h"
#include "yb/master/master_error.h"
#include "yb/master/master_heartbeat.pb.h"
#include "yb/master/tablet_split_manager.h"
#include "yb/master/ts_descriptor.h"

#include "yb/rocksdb/db.h"

#include "yb/rpc/messenger.h"
#include "yb/rpc/proxy.h"
#include "yb/rpc/rpc_controller.h"

#include "yb/tablet/tablet.h"
#include "yb/tablet/tablet_metadata.h"
#include "yb/tablet/tablet_peer.h"
#include "yb/tablet/transaction_participant.h"

#include "yb/tserver/mini_tablet_server.h"
#include "yb/tserver/tablet_server.h"
#include "yb/tserver/ts_tablet_manager.h"
#include "yb/tserver/tserver_admin.pb.h"
#include "yb/tserver/tserver_admin.proxy.h"
#include "yb/tserver/tserver_service.pb.h"

#include "yb/util/atomic.h"
#include "yb/util/format.h"
#include "yb/util/monotime.h"
#include "yb/util/protobuf_util.h"
#include "yb/util/random_util.h"
#include "yb/util/result.h"
#include "yb/util/size_literals.h"
#include "yb/util/status.h"
#include "yb/util/status_format.h"
#include "yb/util/status_log.h"
#include "yb/util/test_util.h"
#include "yb/util/tsan_util.h"

using namespace std::literals;  // NOLINT
using namespace yb::client::kv_table_test; // NOLINT

DECLARE_int64(db_block_size_bytes);
DECLARE_int64(db_write_buffer_size);
DECLARE_bool(enable_load_balancing);
DECLARE_bool(enable_maintenance_manager);
DECLARE_int32(load_balancer_max_concurrent_adds);
DECLARE_int32(load_balancer_max_concurrent_removals);
DECLARE_int32(load_balancer_max_concurrent_moves);
DECLARE_int32(maintenance_manager_polling_interval_ms);
DECLARE_int64(rocksdb_compact_flush_rate_limit_bytes_per_sec);
DECLARE_int32(rocksdb_level0_file_num_compaction_trigger);
DECLARE_bool(rocksdb_disable_compactions);
DECLARE_bool(TEST_do_not_start_election_test_only);
DECLARE_int32(TEST_apply_tablet_split_inject_delay_ms);
DECLARE_int32(heartbeat_interval_ms);
DECLARE_int32(leader_lease_duration_ms);
DECLARE_int32(raft_heartbeat_interval_ms);
DECLARE_double(leader_failure_max_missed_heartbeat_periods);
DECLARE_bool(TEST_skip_deleting_split_tablets);
DECLARE_uint64(tablet_split_limit_per_table);
DECLARE_bool(TEST_pause_before_post_split_compaction);
DECLARE_bool(TEST_pause_apply_tablet_split);
DECLARE_int32(TEST_slowdown_backfill_alter_table_rpcs_ms);
DECLARE_int32(retryable_request_timeout_secs);
DECLARE_int32(rocksdb_base_background_compactions);
DECLARE_int32(rocksdb_max_background_compactions);
DECLARE_int32(rocksdb_level0_file_num_compaction_trigger);
DECLARE_bool(enable_automatic_tablet_splitting);
DECLARE_int64(tablet_split_low_phase_shard_count_per_node);
DECLARE_int64(tablet_split_high_phase_shard_count_per_node);
DECLARE_int64(tablet_split_low_phase_size_threshold_bytes);
DECLARE_int64(tablet_split_high_phase_size_threshold_bytes);
DECLARE_int64(tablet_force_split_threshold_bytes);
DECLARE_int32(tserver_heartbeat_metrics_interval_ms);
DECLARE_bool(TEST_validate_all_tablet_candidates);
DECLARE_uint64(outstanding_tablet_split_limit);
DECLARE_uint64(outstanding_tablet_split_limit_per_tserver);
DECLARE_double(TEST_fail_tablet_split_probability);
DECLARE_bool(TEST_skip_post_split_compaction);
DECLARE_int32(TEST_nodes_per_cloud);
DECLARE_int32(replication_factor);
DECLARE_int32(txn_max_apply_batch_records);
DECLARE_int32(TEST_pause_and_skip_apply_intents_task_loop_ms);
DECLARE_bool(TEST_pause_tserver_get_split_key);
DECLARE_bool(TEST_reject_delete_not_serving_tablet_rpc);
DECLARE_int32(timestamp_history_retention_interval_sec);
DECLARE_int64(db_block_cache_size_bytes);
DECLARE_uint64(rocksdb_max_file_size_for_compaction);
DECLARE_uint64(prevent_split_for_ttl_tables_for_seconds);
DECLARE_bool(sort_automatic_tablet_splitting_candidates);
DECLARE_int32(intents_flush_max_delay_ms);

namespace yb {
class TabletSplitITestWithIsolationLevel : public TabletSplitITest,
                                           public testing::WithParamInterface<IsolationLevel> {
 public:
  void SetUp() override {
    SetIsolationLevel(GetParam());
    TabletSplitITest::SetUp();
  }
};

// Tests splitting of the single tablet in following steps:
// - Create single-tablet table and populates it with specified number of rows.
// - Do full scan using `select count(*)`.
// - Send SplitTablet RPC to the tablet leader.
// - After tablet split is completed - check that new tablets have exactly the same rows.
// - Check that source tablet is rejecting reads and writes.
// - Do full scan using `select count(*)`.
// - Restart cluster.
// - ClusterVerifier will check cluster integrity at the end of the test.

TEST_P(TabletSplitITestWithIsolationLevel, SplitSingleTablet) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_skip_deleting_split_tablets) = true;

  // TODO(tsplit): add delay of applying part of intents after tablet is split.
  // TODO(tsplit): test split during long-running transactions.

  constexpr auto kNumRows = kDefaultNumRows;

  const auto source_tablet_id = ASSERT_RESULT(CreateSingleTabletAndSplit(kNumRows));

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_skip_deleting_split_tablets) = false;

  ASSERT_OK(WaitForTabletSplitCompletion(/* expected_non_split_tablets =*/ 2));

  ASSERT_OK(CheckRowsCount(kNumRows));

  ASSERT_OK(WriteRows(kNumRows, kNumRows + 1));

  ASSERT_OK(cluster_->RestartSync());

  ASSERT_OK(CheckPostSplitTabletReplicasData(kNumRows * 2));
}

TEST_F(TabletSplitITest, SplitTabletIsAsync) {
  constexpr auto kNumRows = kDefaultNumRows;

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_before_post_split_compaction) = true;

  ASSERT_OK(CreateSingleTabletAndSplit(kNumRows));

  for (auto peer : ASSERT_RESULT(ListPostSplitChildrenTabletPeers())) {
    EXPECT_FALSE(peer->tablet()->metadata()->has_been_fully_compacted());
  }
  std::this_thread::sleep_for(1s * kTimeMultiplier);
  for (auto peer : ASSERT_RESULT(ListPostSplitChildrenTabletPeers())) {
    EXPECT_FALSE(peer->tablet()->metadata()->has_been_fully_compacted());
  }
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_before_post_split_compaction) = false;
  ASSERT_OK(WaitForTestTablePostSplitTabletsFullyCompacted(15s * kTimeMultiplier));
}

TEST_F(TabletSplitITest, ParentTabletCleanup) {
  constexpr auto kNumRows = kDefaultNumRows;

  ASSERT_OK(CreateSingleTabletAndSplit(kNumRows));

  // This will make client first try to access deleted tablet and that should be handled correctly.
  ASSERT_OK(CheckRowsCount(kNumRows));
}

class TabletSplitNoBlockCacheITest : public TabletSplitITest {
 public:
  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_db_block_cache_size_bytes) = -2;
    TabletSplitITest::SetUp();
  }
};

TEST_F_EX(TabletSplitITest, TestInitiatesCompactionAfterSplit, TabletSplitNoBlockCacheITest) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_skip_deleting_split_tablets) = true;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_outstanding_tablet_split_limit) = 5;
  constexpr auto kNumRows = kDefaultNumRows;
  constexpr auto kNumPostSplitTablets = 2;

  CreateSingleTablet();
  const auto split_hash_code = ASSERT_RESULT(WriteRowsAndGetMiddleHashCode(kNumRows));

  ASSERT_OK(SplitTabletAndValidate(split_hash_code, kNumRows));
  ASSERT_OK(LoggedWaitFor(
      [this]() -> Result<bool> {
        const auto count = VERIFY_RESULT(NumPostSplitTabletPeersFullyCompacted());
        return count >= kNumPostSplitTablets * ANNOTATE_UNPROTECTED_READ(FLAGS_replication_factor);
      },
      15s * kTimeMultiplier, "Waiting for post-split tablets to be fully compacted..."));

  // Get the sum of compaction bytes read by each child tablet replica grouped by peer uuid
  auto replicas = ListTableActiveTabletPeers(cluster_.get(), ASSERT_RESULT(GetTestTableId()));
  ASSERT_EQ(replicas.size(),
            kNumPostSplitTablets * ANNOTATE_UNPROTECTED_READ(FLAGS_replication_factor));
  std::unordered_map<std::string, uint64_t> child_replicas_bytes_read;
  for (const auto& replica : replicas) {
    auto replica_bytes = replica->tablet()->regulardb_statistics()->getTickerCount(
         rocksdb::Tickers::COMPACT_READ_BYTES);
    ASSERT_GT(replica_bytes, 0) << "Expected replica's read bytes to be greater than zero.";
    child_replicas_bytes_read[replica->permanent_uuid()] += replica_bytes;
  }

  // Get parent tablet's bytes written and also check value is the same for all replicas
  replicas = ASSERT_RESULT(ListSplitCompleteTabletPeers());
  ASSERT_EQ(replicas.size(), ANNOTATE_UNPROTECTED_READ(FLAGS_replication_factor));
  uint64_t pre_split_sst_files_size = 0;
  for (const auto& replica : replicas) {
    auto replica_bytes = replica->tablet()->GetCurrentVersionSstFilesSize();
    ASSERT_GT(replica_bytes, 0) << "Expected replica's SST file size to be greater than zero.";
    if (pre_split_sst_files_size == 0) {
      pre_split_sst_files_size = replica_bytes;
    } else {
      ASSERT_EQ(replica_bytes, pre_split_sst_files_size)
          << "Expected the number of SST files size at each replica to be the same.";
    }
  }

  // Make sure that during child tablets compaction we don't read the same row twice, in other words
  // we don't process parent tablet rows that are not served by child tablet. A specific scaling
  // factor is used to measure relation between child replicas bytes read and parent SST files
  // due to unpredictable space overhead for reading files and SST files sturcture after the split.
  constexpr double kScalingFactor = 1.05;
  const double child_replicas_bytes_read_upper_bound = pre_split_sst_files_size * kScalingFactor;
  uint64_t post_split_bytes_read = 0;
  for (const auto& replica_stat : child_replicas_bytes_read) {
    if (post_split_bytes_read == 0) {
      post_split_bytes_read = replica_stat.second;
    } else {
      ASSERT_EQ(replica_stat.second, post_split_bytes_read);
    }
    // There are two ways to resolve a failure at the point if happens. The first one is to increase
    // the value of kScalingFactor but this approach is not very accurate. The second way is to
    // use rocksdb::EventListener to retrieve CompactionJobInfo.info.stats.num_input_records and
    // to measure it with the number of records in regular DB via TableProperties::num_entries,
    // see VerifyTableProperties() for an example.
    ASSERT_LE(static_cast<double>(post_split_bytes_read), child_replicas_bytes_read_upper_bound);
  }
}

// Test for https://github.com/yugabyte/yugabyte-db/issues/8295.
// Checks that slow post-split tablet compaction doesn't block that tablet's cleanup.
TEST_F(TabletSplitITest, PostSplitCompactionDoesntBlockTabletCleanup) {
  constexpr auto kNumRows = kDefaultNumRows;
  const MonoDelta kCleanupTimeout = 15s * kTimeMultiplier;

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_load_balancing) = false;
  // Keep tablets without compaction after split.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_before_post_split_compaction) = true;

  ASSERT_OK(CreateSingleTabletAndSplit(kNumRows));

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_do_not_start_election_test_only) = true;
  auto tablet_peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
  ASSERT_EQ(tablet_peers.size(), 2);
  const auto first_child_tablet = tablet_peers[0]->shared_tablet();
  ASSERT_OK(first_child_tablet->Flush(tablet::FlushMode::kSync));
  // Force compact on leader, so we can split first_child_tablet.
  ASSERT_OK(first_child_tablet->ForceFullRocksDBCompact());
  // Turn off split tablets cleanup in order to later turn it on during compaction of the
  // first_child_tablet to make sure manual compaction won't block tablet shutdown.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_skip_deleting_split_tablets) = true;
  ASSERT_OK(SplitTablet(ASSERT_RESULT(catalog_manager()), *first_child_tablet));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_do_not_start_election_test_only) = false;
  ASSERT_OK(WaitForTabletSplitCompletion(
      /* expected_non_split_tablets = */ 3, /* expected_split_tablets = */ 1));

  // Simulate slow compaction, so it takes at least kCleanupTimeout * 1.5 for first child tablet
  // followers.
  const auto original_compact_flush_rate_bytes_per_sec =
      FLAGS_rocksdb_compact_flush_rate_limit_bytes_per_sec;
  SetCompactFlushRateLimitBytesPerSec(
      cluster_.get(),
      first_child_tablet->GetCurrentVersionSstFilesSize() / (kCleanupTimeout.ToSeconds() * 1.5));
  // Resume post-split compaction.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_before_post_split_compaction) = false;
  // Turn on split tablets cleanup.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_skip_deleting_split_tablets) = false;

  // Cleanup of first_child_tablet will shutdown that tablet after deletion and we check that
  // shutdown is not stuck due to its slow post-split compaction.
  const auto wait_message =
      Format("Waiting for tablet $0 cleanup", first_child_tablet->tablet_id());
  LOG(INFO) << wait_message << "...";
  std::vector<Result<tablet::TabletPeerPtr>> first_child_tablet_peer_results;
  const auto s = WaitFor(
      [this, &first_child_tablet, &first_child_tablet_peer_results] {
        first_child_tablet_peer_results.clear();
        for (auto mini_ts : cluster_->mini_tablet_servers()) {
          auto tablet_peer_result =
              mini_ts->server()->tablet_manager()->LookupTablet(first_child_tablet->tablet_id());
          if (tablet_peer_result.ok() || !tablet_peer_result.status().IsNotFound()) {
            first_child_tablet_peer_results.push_back(tablet_peer_result);
          }
        }
        return first_child_tablet_peer_results.empty();
      },
      kCleanupTimeout, wait_message);
  for (const auto& peer_result : first_child_tablet_peer_results) {
    LOG(INFO) << "Tablet peer not cleaned: "
              << (peer_result.ok() ? (*peer_result)->LogPrefix() : AsString(peer_result.status()));
  }
  ASSERT_OK(s);
  LOG(INFO) << wait_message << " - DONE";

  SetCompactFlushRateLimitBytesPerSec(cluster_.get(), original_compact_flush_rate_bytes_per_sec);
}

TEST_F(TabletSplitITest, TestLoadBalancerAndSplit) {
  constexpr auto kNumRows = kDefaultNumRows;

  // To speed up load balancing (it also processes transaction status tablets).
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_max_concurrent_adds) = 5;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_max_concurrent_removals) = 5;

  // Keep tablets without compaction after split.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_before_post_split_compaction) = true;

  CreateSingleTablet();

  const auto split_hash_code = ASSERT_RESULT(WriteRowsAndGetMiddleHashCode(kNumRows));

  ASSERT_OK(SplitTabletAndValidate(split_hash_code, kNumRows));

  auto test_table_id = ASSERT_RESULT(GetTestTableId());
  auto test_tablet_ids = ListTabletIdsForTable(cluster_.get(), test_table_id);

  // Verify that heartbeat contains flag should_disable_lb_move for all tablets of the test
  // table on each tserver to have after split.
  for (size_t i = 0; i != cluster_->num_tablet_servers(); ++i) {
    auto* ts_manager = cluster_->mini_tablet_server(i)->server()->tablet_manager();

    master::TabletReportPB report;
    ts_manager->GenerateTabletReport(&report);
    for (const auto& reported_tablet : report.updated_tablets()) {
      if (test_tablet_ids.count(reported_tablet.tablet_id()) == 0) {
        continue;
      }
      ASSERT_TRUE(reported_tablet.should_disable_lb_move());
    }
  }

  // Add new tserver in to force load balancer moves.
  auto new_ts = cluster_->num_tablet_servers();
  ASSERT_OK(cluster_->AddTabletServer());
  ASSERT_OK(cluster_->WaitForTabletServerCount(new_ts + 1));
  const auto new_ts_uuid = cluster_->mini_tablet_server(new_ts)->server()->permanent_uuid();

  LOG(INFO) << "Added new tserver: " << new_ts_uuid;

  // Wait for the LB run.
  const auto lb_wait_period = MonoDelta::FromMilliseconds(
      FLAGS_catalog_manager_bg_task_wait_ms * 2 + FLAGS_raft_heartbeat_interval_ms * 2);
  SleepFor(lb_wait_period);

  // Verify none test tablet replica on the new tserver.
  for (const auto& tablet : ASSERT_RESULT(GetTabletInfosForTable(test_table_id))) {
    const auto replica_map = tablet->GetReplicaLocations();
    ASSERT_TRUE(replica_map->find(new_ts_uuid) == replica_map->end())
        << "Not expected tablet " << tablet->id()
        << " to be on newly added tserver: " << new_ts_uuid;
  }

  // Verify that custom placement info is honored when tablets are split.
  const auto& blacklisted_ts = *ASSERT_NOTNULL(cluster_->mini_tablet_server(1));
  const auto blacklisted_ts_uuid = blacklisted_ts.server()->permanent_uuid();
  ASSERT_OK(cluster_->AddTServerToBlacklist(blacklisted_ts));
  LOG(INFO) << "Blacklisted tserver: " << blacklisted_ts_uuid;
  std::vector<TabletId> on_blacklisted_ts;
  std::vector<TabletId> no_replicas_on_new_ts;
  auto s = LoggedWaitFor(
      [&] {
        auto tablet_infos = GetTabletInfosForTable(test_table_id);
        if (!tablet_infos.ok()) {
          return false;
        }
        on_blacklisted_ts.clear();
        no_replicas_on_new_ts.clear();
        for (const auto& tablet : *tablet_infos) {
          auto replica_map = tablet->GetReplicaLocations();
          if (replica_map->count(new_ts_uuid) == 0) {
            no_replicas_on_new_ts.push_back(tablet->id());
          }
          if (replica_map->count(blacklisted_ts_uuid) > 0) {
            on_blacklisted_ts.push_back(tablet->id());
          }
        }
        return on_blacklisted_ts.empty() && no_replicas_on_new_ts.empty();
      },
      60s * kTimeMultiplier,
      Format(
          "Wait for all test tablet replicas to be moved from tserver $0 to $1 on master",
          blacklisted_ts_uuid, new_ts_uuid));
  ASSERT_TRUE(s.ok()) << Format(
      "Replicas are still on blacklisted tserver $0: $1\nNo replicas for tablets on new tserver "
      "$2: $3",
      blacklisted_ts_uuid, on_blacklisted_ts, new_ts_uuid, no_replicas_on_new_ts);

  ASSERT_OK(cluster_->ClearBlacklist());
  // Wait for the LB run.
  SleepFor(lb_wait_period);

  // Test tablets should not move until compaction.
  for (const auto& tablet : ASSERT_RESULT(GetTabletInfosForTable(test_table_id))) {
    const auto replica_map = tablet->GetReplicaLocations();
    ASSERT_TRUE(replica_map->find(blacklisted_ts_uuid) == replica_map->end())
        << "Not expected tablet " << tablet->id() << " to be on tserver " << blacklisted_ts_uuid
        << " that moved out of blacklist before post-split compaction completed";
  }

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_before_post_split_compaction) = false;

  ASSERT_OK(WaitForTestTablePostSplitTabletsFullyCompacted(15s * kTimeMultiplier));

  ASSERT_OK(LoggedWaitFor(
      [&] {
        auto tablet_infos = GetTabletInfosForTable(test_table_id);
        if (!tablet_infos.ok()) {
          return false;
        }
        for (const auto& tablet : *tablet_infos) {
          auto replica_map = tablet->GetReplicaLocations();
          if (replica_map->find(blacklisted_ts_uuid) == replica_map->end()) {
            return true;
          }
        }
        return false;
      },
      60s * kTimeMultiplier,
      Format(
          "Wait for at least one test tablet replica on tserver that moved out of blacklist: $0",
          blacklisted_ts_uuid)));
}

// Test for https://github.com/yugabyte/yugabyte-db/issues/4312 reproducing a deadlock
// between TSTabletManager::ApplyTabletSplit and Heartbeater::Thread::TryHeartbeat.
TEST_F(TabletSplitITest, SlowSplitSingleTablet) {
  const auto leader_failure_timeout = FLAGS_leader_failure_max_missed_heartbeat_periods *
        FLAGS_raft_heartbeat_interval_ms;

  FLAGS_TEST_apply_tablet_split_inject_delay_ms = 200 * kTimeMultiplier;
  // We want heartbeater to be called during tablet split apply to reproduce deadlock bug.
  FLAGS_heartbeat_interval_ms = FLAGS_TEST_apply_tablet_split_inject_delay_ms / 3;
  // We reduce FLAGS_leader_lease_duration_ms for ReplicaState::GetLeaderState to avoid always
  // reusing results from cache on heartbeat, otherwise it won't lock ReplicaState mutex.
  FLAGS_leader_lease_duration_ms = FLAGS_TEST_apply_tablet_split_inject_delay_ms / 2;
  // Reduce raft_heartbeat_interval_ms for leader lease to be reliably replicated.
  FLAGS_raft_heartbeat_interval_ms = FLAGS_leader_lease_duration_ms / 2;
  // Keep leader failure timeout the same to avoid flaky losses of leader with short heartbeats.
  FLAGS_leader_failure_max_missed_heartbeat_periods =
      leader_failure_timeout / FLAGS_raft_heartbeat_interval_ms;

  constexpr auto kNumRows = 50;

  ASSERT_OK(CreateSingleTabletAndSplit(kNumRows));
}

TEST_F(TabletSplitITest, SplitSystemTable) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_validate_all_tablet_candidates) = false;

  auto* catalog_mgr = ASSERT_RESULT(catalog_manager());

  // Attempt splits on "sys.catalog" and "tables" system tables and verify that they fail.
  std::vector<master::TableInfoPtr> systables = {
      catalog_mgr->GetTableInfo("sys.catalog.uuid"),
      catalog_mgr->GetTableInfoFromNamespaceNameAndTableName(
          YQL_DATABASE_CQL, "system_schema", "tables")};

  for (const auto& systable : systables) {
    for (const auto& tablet : systable->GetTablets()) {
      LOG(INFO) << "Splitting : " << systable->name() << " Tablet :" << tablet->id();
      auto s = catalog_mgr->TEST_SplitTablet(tablet, true /* is_manual_split */);
      LOG(INFO) << s.ToString();
      EXPECT_TRUE(s.IsNotSupported());
      LOG(INFO) << "Split of system table failed as expected";
    }
  }
}

TEST_F(TabletSplitITest, SplitTabletDuringReadWriteLoad) {
  constexpr auto kNumTablets = 3;

  FLAGS_db_write_buffer_size = 100_KB;

  TestWorkload workload(cluster_.get());
  workload.set_table_name(client::kTableName);
  workload.set_write_timeout_millis(MonoDelta(kRpcTimeout).ToMilliseconds());
  workload.set_num_tablets(kNumTablets);
  workload.set_num_read_threads(4);
  workload.set_num_write_threads(2);
  workload.set_write_batch_size(50);
  workload.set_payload_bytes(16);
  workload.set_sequential_write(true);
  workload.set_retry_on_restart_required_error(true);
  workload.set_read_only_written_keys(true);
  workload.Setup();

  const auto test_table_id = ASSERT_RESULT(GetTestTableId());

  auto peers = ASSERT_RESULT(WaitForTableActiveTabletLeadersPeers(
      cluster_.get(), test_table_id, kNumTablets));

  LOG(INFO) << "Starting workload ...";
  workload.Start();

  for (const auto& peer : peers) {
    ASSERT_OK(LoggedWaitFor(
        [&peer] {
          const auto data_size =
              peer->tablet()->TEST_db()->GetCurrentVersionSstFilesUncompressedSize();
          YB_LOG_EVERY_N_SECS(INFO, 5) << "Data written: " << data_size;
          size_t expected_size = (FLAGS_rocksdb_level0_file_num_compaction_trigger + 1) *
                                 FLAGS_db_write_buffer_size;
          return data_size > expected_size;
        },
        60s * kTimeMultiplier, Format("Writing data to split (tablet $0) ...", peer->tablet_id())));
  }

  DumpWorkloadStats(workload);

  auto* catalog_mgr = ASSERT_RESULT(catalog_manager());

  for (const auto& peer : peers) {
    const auto& source_tablet = *ASSERT_NOTNULL(peer->tablet());
    ASSERT_OK(SplitTablet(catalog_mgr, source_tablet));
  }

  ASSERT_OK(WaitForTabletSplitCompletion(/* expected_non_split_tablets =*/ kNumTablets * 2));

  DumpTableLocations(catalog_mgr, client::kTableName);

  // Generate some more read/write traffic after tablets are split and after that we check data
  // for consistency and that failures rates are acceptable.
  std::this_thread::sleep_for(5s);

  LOG(INFO) << "Stopping workload ...";
  workload.StopAndJoin();

  DumpWorkloadStats(workload);

  ASSERT_NO_FATALS(CheckTableKeysInRange(workload.rows_inserted()));

  const auto insert_failure_rate = 1.0 * workload.rows_insert_failed() / workload.rows_inserted();
  const auto read_failure_rate = 1.0 * workload.rows_read_error() / workload.rows_read_ok();
  const auto read_try_again_rate = 1.0 * workload.rows_read_try_again() / workload.rows_read_ok();

  ASSERT_LT(insert_failure_rate, 0.01);
  ASSERT_LT(read_failure_rate, 0.01);
  // TODO(tsplit): lower this threshold as internal (without reaching client app) read retries
  //  implemented for split tablets.
  ASSERT_LT(read_try_again_rate, 0.1);
  ASSERT_EQ(workload.rows_read_empty(), 0);

  // TODO(tsplit): Check with different isolation levels.

  ASSERT_OK(cluster_->RestartSync());
}

namespace {

void SetSmallDbBlockSize() {
  // Set data block size low enough, so we have enough data blocks for middle key
  // detection to work correctly.
  FLAGS_db_block_size_bytes = 1_KB;
}

}

void TabletSplitITest::SplitClientRequestsIds(int split_depth) {
  SetSmallDbBlockSize();
  const auto kNumRows = 50 * (1 << split_depth);

  SetNumTablets(1);
  CreateTable();

  ASSERT_OK(WriteRows(kNumRows, 1));

  ASSERT_OK(CheckRowsCount(kNumRows));

  auto* catalog_mgr = ASSERT_RESULT(catalog_manager());

  for (int i = 0; i < split_depth; ++i) {
    auto peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
    ASSERT_EQ(peers.size(), 1 << i);
    for (const auto& peer : peers) {
      const auto tablet = peer->shared_tablet();
      ASSERT_OK(tablet->Flush(tablet::FlushMode::kSync));
      tablet->TEST_ForceRocksDBCompact();
      ASSERT_OK(SplitTablet(catalog_mgr, *tablet));
    }

    ASSERT_OK(WaitForTabletSplitCompletion(
        /* expected_non_split_tablets =*/ 1 << (i + 1)));
  }

  Status s;
  ASSERT_OK(WaitFor([&] {
    s = ResultToStatus(WriteRows(1, 1));
    return !s.IsTryAgain();
  }, 60s * kTimeMultiplier, "Waiting for successful write"));
  ASSERT_OK(s);
}

// Test for https://github.com/yugabyte/yugabyte-db/issues/5415.
// Client knows about split parent for final tablets.
TEST_F(TabletSplitITest, SplitClientRequestsIdsDepth1) {
  SplitClientRequestsIds(1);
}

// Test for https://github.com/yugabyte/yugabyte-db/issues/5415.
// Client doesn't know about split parent for final tablets.
TEST_F(TabletSplitITest, SplitClientRequestsIdsDepth2) {
  SplitClientRequestsIds(2);
}

class TabletSplitITestSlowMainenanceManager : public TabletSplitITest {
 public:
  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_maintenance_manager_polling_interval_ms) = 60 * 1000;
    TabletSplitITest::SetUp();
  }
};

TEST_F_EX(TabletSplitITest, SplitClientRequestsClean, TabletSplitITestSlowMainenanceManager) {
  constexpr auto kSplitDepth = 3;
  constexpr auto kNumRows = 50 * (1 << kSplitDepth);
  constexpr auto kRetryableRequestTimeoutSecs = 1;
  SetSmallDbBlockSize();

  // Prevent periodic retryable requests cleanup by maintenance manager.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_maintenance_manager) = false;

  SetNumTablets(1);
  CreateTable();

  ASSERT_OK(WriteRows(kNumRows, 1));
  ASSERT_OK(CheckRowsCount(kNumRows));

  auto* catalog_mgr = ASSERT_RESULT(catalog_manager());

  auto client = ASSERT_RESULT(cluster_->CreateClient());
  LOG(INFO) << "Creating new client, id: " << client->id();

  for (int i = 0; i < kSplitDepth; ++i) {
    auto peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
    ASSERT_EQ(peers.size(), 1 << i);
    for (const auto& peer : peers) {
      const auto tablet = peer->shared_tablet();
      ASSERT_OK(tablet->Flush(tablet::FlushMode::kSync));
      tablet->TEST_ForceRocksDBCompact();
      ASSERT_OK(SplitTablet(catalog_mgr, *tablet));
    }

    ASSERT_OK(WaitForTabletSplitCompletion(/* expected_non_split_tablets =*/ 1 << (i + 1)));

    if (i == 0) {
      // This will set client's request_id_seq for tablets with split depth 1 to > 1^24 (see
      // YBClient::MaybeUpdateMinRunningRequestId) and update min_running_request_id for this
      // client at tserver side.
      auto session = client->NewSession();
      ASSERT_OK(WriteRows(&this->table_, kNumRows, 1, session));
    }
  }

  auto leader_peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
  // Force cleaning retryable requests on leaders of active (split depth = kSplitDepth) tablets.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_retryable_request_timeout_secs) = kRetryableRequestTimeoutSecs;
  SleepFor(kRetryableRequestTimeoutSecs * 1s);
  for (int i = 0; i < 2; ++i) {
    for (auto& leader_peer : leader_peers) {
      LOG(INFO) << leader_peer->LogPrefix() << "MinRetryableRequestOpId(): "
                << AsString(leader_peer->raft_consensus()->MinRetryableRequestOpId());
      // Delay to make RetryableRequests::CleanExpiredReplicatedAndGetMinOpId (called by
      // MinRetryableRequestOpId) do delayed cleanup.
      SleepFor(kRetryableRequestTimeoutSecs * 1s);
    }
  }

  auto session = client->NewSession();
  // Since client doesn't know about tablets with split depth > 1, it will set request_id_seq for
  // active tablets based on min_running_request_id on leader, but on leader it has been cleaned up.
  // So, request_id_seq will be set to 0 + 1^24 that is less than min_running_request_id on the
  // follower (at which retryable requests is not yet cleaned up).
  // This will test how follower handles getting request_id less than min_running_request_id.
  LOG(INFO) << "Starting write to active tablets after retryable requests cleanup on leaders...";
  ASSERT_OK(WriteRows(&this->table_, kNumRows, 1, session));
  LOG(INFO) << "Write to active tablets completed.";
}


TEST_F(TabletSplitITest, SplitSingleTabletWithLimit) {
  SetSmallDbBlockSize();
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_validate_all_tablet_candidates) = false;
  const auto kSplitDepth = 3;
  const auto kNumRows = 50 * (1 << kSplitDepth);
  FLAGS_tablet_split_limit_per_table = (1 << kSplitDepth) - 1;

  CreateSingleTablet();
  ASSERT_OK(WriteRows(kNumRows, 1));
  ASSERT_OK(CheckRowsCount(kNumRows));

  auto* catalog_mgr = ASSERT_RESULT(catalog_manager());

  master::TableIdentifierPB table_id_pb;
  table_id_pb.set_table_id(table_->id());
  bool reached_split_limit = false;

  for (int i = 0; i < kSplitDepth; ++i) {
    auto peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
    bool expect_split = false;
    for (const auto& peer : peers) {
      const auto tablet = peer->shared_tablet();
      ASSERT_OK(tablet->Flush(tablet::FlushMode::kSync));
      tablet->TEST_ForceRocksDBCompact();
      auto table_info = ASSERT_RESULT(catalog_mgr->FindTable(table_id_pb));

      expect_split = table_info->NumPartitions() < FLAGS_tablet_split_limit_per_table;

      if (expect_split) {
        ASSERT_OK(DoSplitTablet(catalog_mgr, *tablet));
      } else {
        const auto split_status = DoSplitTablet(catalog_mgr, *tablet);
        ASSERT_EQ(master::MasterError(split_status),
                  master::MasterErrorPB::REACHED_SPLIT_LIMIT);
        reached_split_limit = true;
      }
    }
    if (expect_split) {
      ASSERT_OK(WaitForTabletSplitCompletion(
          /* expected_non_split_tablets =*/1 << (i + 1)));
    }
  }

  ASSERT_TRUE(reached_split_limit);

  Status s;
  ASSERT_OK(WaitFor([&] {
    s = ResultToStatus(WriteRows(1, 1));
    return !s.IsTryAgain();
  }, 60s * kTimeMultiplier, "Waiting for successful write"));

  auto table_info = ASSERT_RESULT(catalog_mgr->FindTable(table_id_pb));
  ASSERT_EQ(table_info->NumPartitions(), FLAGS_tablet_split_limit_per_table);
}

TEST_F(TabletSplitITest, SplitDuringReplicaOffline) {
  constexpr auto kNumRows = kDefaultNumRows;

  SetNumTablets(1);
  CreateTable();

  const auto split_hash_code = ASSERT_RESULT(WriteRowsAndGetMiddleHashCode(kNumRows));

  auto rows_count = ASSERT_RESULT(SelectRowsCount(NewSession(), table_));
  ASSERT_EQ(rows_count, kNumRows);

  auto* catalog_mgr = ASSERT_RESULT(catalog_manager());

  auto source_tablet_info = ASSERT_RESULT(GetSingleTestTabletInfo(catalog_mgr));
  const auto source_tablet_id = source_tablet_info->id();

  cluster_->mini_tablet_server(0)->Shutdown();

  LOG(INFO) << "Stopped TS-1";

  ASSERT_OK(catalog_mgr->TEST_SplitTablet(source_tablet_info, split_hash_code));

  ASSERT_OK(WaitForTabletSplitCompletion(
      /* expected_non_split_tablets =*/ 2, /* expected_split_tablets =*/ 1,
      /* num_replicas_online =*/ 2));

  ASSERT_OK(CheckPostSplitTabletReplicasData(kNumRows, 2));

  ASSERT_OK(CheckSourceTabletAfterSplit(source_tablet_id));

  DumpTableLocations(catalog_mgr, client::kTableName);

  rows_count = ASSERT_RESULT(SelectRowsCount(NewSession(), table_));
  ASSERT_EQ(rows_count, kNumRows);

  ASSERT_OK(WriteRows(kNumRows, kNumRows + 1));

  LOG(INFO) << "Starting TS-1";

  ASSERT_OK(cluster_->mini_tablet_server(0)->Start());

  // This time we expect all replicas to be online.
  ASSERT_OK(WaitForTabletSplitCompletion(
      /* expected_non_split_tablets =*/ 2, /* expected_split_tablets =*/ 0));

  Status s;
  ASSERT_OK_PREPEND(LoggedWaitFor([&] {
      s = CheckPostSplitTabletReplicasData(kNumRows * 2);
      return s.IsOk();
    }, 30s * kTimeMultiplier, "Waiting for TS-1 to catch up ..."), AsString(s));
}

// Test for https://github.com/yugabyte/yugabyte-db/issues/6890.
// Writes data to the tablet, splits it and then tries to do full scan with `select count(*)`
// using two different instances of YBTable one after another.
TEST_F(TabletSplitITest, DifferentYBTableInstances) {
  constexpr auto kNumRows = kDefaultNumRows;

  CreateSingleTablet();

  client::TableHandle table1, table2;
  for (auto* table : {&table1, &table2}) {
    ASSERT_OK(table->Open(client::kTableName, client_.get()));
  }

  const auto split_hash_code = ASSERT_RESULT(WriteRowsAndGetMiddleHashCode(kNumRows));
  const auto source_tablet_id = ASSERT_RESULT(SplitTabletAndValidate(split_hash_code, kNumRows));

  ASSERT_OK(WaitForTabletSplitCompletion(/* expected_non_split_tablets =*/ 2));

  auto rows_count = ASSERT_RESULT(SelectRowsCount(NewSession(), table1));
  ASSERT_EQ(rows_count, kNumRows);

  rows_count = ASSERT_RESULT(SelectRowsCount(NewSession(), table2));
  ASSERT_EQ(rows_count, kNumRows);
}

TEST_F(TabletSplitITest, SplitSingleTabletLongTransactions) {
  constexpr auto kNumRows = 1000;
  constexpr auto kNumApplyLargeTxnBatches = 10;
  FLAGS_txn_max_apply_batch_records = kNumRows / kNumApplyLargeTxnBatches;
  FLAGS_TEST_pause_and_skip_apply_intents_task_loop_ms = 1;

  // Write enough rows to trigger the large transaction apply path with kNumApplyLargeTxnBatches
  // batches. Wait for post split compaction and validate data before returning.
  ASSERT_OK(CreateSingleTabletAndSplit(kNumRows));

  // At this point, post split compaction has happened, and no apply intent task iterations have
  // run. If post-split compaction has improperly handled ApplyTransactionState present in
  // regulardb, e.g. by deleting it, then upon restart, one or both of the new child subtablets will
  // lose all unapplied data.
  ASSERT_OK(cluster_->RestartSync());

  // If we did not lose any large transaction apply data during post-split compaction, then we
  // should have all rows present in the database.
  EXPECT_OK(CheckRowsCount(kNumRows));
}

class TabletSplitYedisTableTest : public integration_tests::RedisTableTestBase {
 protected:
  int num_tablets() override { return 1; }
};

TEST_F(TabletSplitYedisTableTest, BlockSplittingYedisTablet) {
  constexpr int kNumRows = 10000;

  for (int i = 0; i < kNumRows; ++i) {
    PutKeyValue(Format("$0", i), Format("$0", i));
  }

  for (const auto& peer : ListTableActiveTabletPeers(mini_cluster(), table_->id())) {
    ASSERT_OK(peer->shared_tablet()->Flush(tablet::FlushMode::kSync));
  }

  for (const auto& peer : ListTableActiveTabletLeadersPeers(mini_cluster(), table_->id())) {
    auto catalog_manager = &CHECK_NOTNULL(
        ASSERT_RESULT(this->mini_cluster()->GetLeaderMiniMaster()))->catalog_manager();

    auto s = DoSplitTablet(catalog_manager, *peer->shared_tablet());
    EXPECT_NOT_OK(s);
    EXPECT_TRUE(s.IsNotSupported()) << s.ToString();
  }
}

class AutomaticTabletSplitITest : public TabletSplitITest {
 public:
  void SetUp() override {
    // This value must be set before calling TabletSplitITest::SetUp(), since it is copied into a
    // variable in TServerMetricsHeartbeatDataProvider.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_heartbeat_metrics_interval_ms) = 100;
    TabletSplitITest::SetUp();

    ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_automatic_tablet_splitting) = true;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_validate_all_tablet_candidates) = false;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_outstanding_tablet_split_limit) = 5;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_outstanding_tablet_split_limit_per_tserver) = 5;
  }

 protected:
  Status FlushAllTabletReplicas(const TabletId& tablet_id, const TableId& table_id) {
    for (const auto& active_peer : ListTableActiveTabletPeers(cluster_.get(), table_id)) {
      if (active_peer->tablet_id() == tablet_id) {
        RETURN_NOT_OK(active_peer->shared_tablet()->Flush(tablet::FlushMode::kSync));
      }
    }
    return Status::OK();
  }

  Status AutomaticallySplitSingleTablet(
      const string& tablet_id, int num_rows_per_batch,
      uint64_t threshold, int* key) {
    uint64_t current_size = 0;
    auto cur_num_tablets = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id()).size();
    while (current_size <= threshold) {
      RETURN_NOT_OK(WriteRows(num_rows_per_batch, *key));
      *key += num_rows_per_batch;
      auto peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
      LOG(INFO) << "Active peers: " << peers.size();
      if (peers.size() == cur_num_tablets + 1) {
        break;
      }
      if (peers.size() != cur_num_tablets) {
        return STATUS_FORMAT(IllegalState,
          "Expected number of peers: $0, actual: $1", cur_num_tablets, peers.size());
      }
      auto leader_peer = peers.at(0);
      for (auto peer : peers) {
        if (peer->tablet_id() == tablet_id) {
          leader_peer = peer;
          break;
        }
      }
      // Flush all replicas of this shard to ensure that even if the leader changed we will be in a
      // state where yb-master should initiate a split.
      RETURN_NOT_OK(FlushAllTabletReplicas(leader_peer->tablet_id(), table_->id()));
      current_size = leader_peer->shared_tablet()->GetCurrentVersionSstFilesSize();
    }
    RETURN_NOT_OK(WaitForTabletSplitCompletion(
      /* expected_non_split_tablets =*/ cur_num_tablets + 1));
    return Status::OK();
  }

  Status CompactTablet(const string& tablet_id) {
    auto peers = ListTabletPeers(cluster_.get(), [&tablet_id](auto peer) {
      return peer->tablet_id() == tablet_id;
    });
    for (const auto& peer : peers) {
      const auto tablet = peer->shared_tablet();
      RETURN_NOT_OK(tablet->Flush(tablet::FlushMode::kSync));
      tablet->TEST_ForceRocksDBCompact();
    }
    return Status::OK();
  }

  void SleepForBgTaskIters(int num_iters) {
    std::this_thread::sleep_for(1ms *
        (FLAGS_catalog_manager_bg_task_wait_ms * num_iters +
         FLAGS_tserver_heartbeat_metrics_interval_ms));
    std::this_thread::sleep_for((FLAGS_catalog_manager_bg_task_wait_ms * num_iters +
                                FLAGS_tserver_heartbeat_metrics_interval_ms) * 1ms);
  }
};

class AutomaticTabletSplitExternalMiniClusterITest : public TabletSplitExternalMiniClusterITest {
 public:
  void SetFlags() override {
    TabletSplitITestBase<ExternalMiniCluster>::SetFlags();
    for (const auto& master_flag : {
              "--enable_automatic_tablet_splitting=true",
              "--outstanding_tablet_split_limit=5",
          }) {
      mini_cluster_opt_.extra_master_flags.push_back(master_flag);
    }
  }
};

TEST_F(AutomaticTabletSplitITest, AutomaticTabletSplitting) {
  constexpr int kNumRowsPerBatch = 1000;

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_shard_count_per_node) = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_size_threshold_bytes) = 100_KB;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rocksdb_disable_compactions) = true;

  int key = 1;
  CreateSingleTablet();
  auto peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
  ASSERT_EQ(peers.size(), 1);
  ASSERT_OK(AutomaticallySplitSingleTablet(peers.at(0)->tablet_id(), kNumRowsPerBatch,
    FLAGS_tablet_split_low_phase_size_threshold_bytes, &key));

  // Since compaction is off, the tablets should not be further split since they won't have had
  // their post split compaction. Assert this is true by tripling the number of keys written and
  // seeing the number of tablets not grow.
  auto triple_keys = key * 2;
  while (key < triple_keys) {
    ASSERT_OK(WriteRows(kNumRowsPerBatch, key));
    key += kNumRowsPerBatch;
    auto peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
    EXPECT_EQ(peers.size(), 2);
  }
}

TEST_F(AutomaticTabletSplitITest, IsTabletSplittingComplete) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_shard_count_per_node) = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_size_threshold_bytes) = 0;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rocksdb_disable_compactions) = true;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_automatic_tablet_splitting) = false;

  CreateSingleTablet();
  ASSERT_OK(WriteRows(1000, 1));
  const auto peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
  // Flush other replicas of this shard to ensure that even if the leader changed we will be in
  // a state where yb-master should initiate a split.
  ASSERT_OK(FlushAllTabletReplicas(peers[0]->tablet_id(), table_->id()));

  auto master_admin_proxy = std::make_unique<master::MasterAdminProxy>(
      proxy_cache_.get(), client_->GetMasterLeaderAddress());

  // No splits at the beginning.
  ASSERT_TRUE(ASSERT_RESULT(IsSplittingComplete(master_admin_proxy.get())));

  // Create a split task by pausing when trying to get split key. IsTabletSplittingComplete should
  // include this ongoing task.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_tserver_get_split_key) = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_automatic_tablet_splitting) = true;
  std::this_thread::sleep_for(FLAGS_catalog_manager_bg_task_wait_ms * 2ms);
  ASSERT_FALSE(ASSERT_RESULT(IsSplittingComplete(master_admin_proxy.get())));

  // Now let the split occur on master but not tserver.
  // IsTabletSplittingComplete should include splits that are only complete on master.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_fail_tablet_split_probability) = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_tserver_get_split_key) = 0;
  ASSERT_FALSE(ASSERT_RESULT(IsSplittingComplete(master_admin_proxy.get())));

  // Verify that the split finishes, and that IsTabletSplittingComplete returns true even though
  // compactions are not done.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_fail_tablet_split_probability) = 0;
  ASSERT_OK(WaitForTabletSplitCompletion(2));
  ASSERT_TRUE(ASSERT_RESULT(IsSplittingComplete(master_admin_proxy.get())));
}

// This test tests both FLAGS_enable_automatic_tablet_splitting and the DisableTabletSplitting API
// (which temporarily disables splitting).
TEST_F(AutomaticTabletSplitITest, DisableTabletSplitting) {
  // Must disable splitting for at least as long as we wait in WaitForTabletSplitCompletion.
  const auto kExtraSleepDuration = 5s * kTimeMultiplier;
  const auto kDisableDuration = split_completion_timeout_sec_ + kExtraSleepDuration;

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_shard_count_per_node) = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_size_threshold_bytes) = 0;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rocksdb_disable_compactions) = true;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_automatic_tablet_splitting) = false;

  CreateSingleTablet();
  ASSERT_OK(WriteRows(1000, 1));
  const auto peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
  // Flush other replicas of this shard to ensure that even if the leader changed we will be in
  // a state where yb-master should initiate a split.
  ASSERT_OK(FlushAllTabletReplicas(peers[0]->tablet_id(), table_->id()));

  // Splitting should fail while FLAGS_enable_automatic_tablet_splitting is false.
  ASSERT_NOK(WaitForTabletSplitCompletion(
      2,                  // expected_non_split_tablets
      0,                  // expected_split_tablets (default)
      0,                  // num_replicas_online (default)
      client::kTableName, // table (default)
      false));            // core_dump_on_failure

  auto master_admin_proxy = std::make_unique<master::MasterAdminProxy>(
      proxy_cache_.get(), client_->GetMasterLeaderAddress());
  rpc::RpcController controller;
  controller.set_timeout(kRpcTimeout);

  master::DisableTabletSplittingRequestPB disable_req;
  disable_req.set_disable_duration_ms(kDisableDuration.ToMilliseconds());
  master::DisableTabletSplittingResponsePB disable_resp;
  ASSERT_OK(master_admin_proxy->DisableTabletSplitting(disable_req, &disable_resp, &controller));

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_automatic_tablet_splitting) = true;
  // Splitting should not occur while it is temporarily disabled.
  ASSERT_NOK(WaitForTabletSplitCompletion(
      2,                  // expected_non_split_tablets
      0,                  // expected_split_tablets (default)
      0,                  // num_replicas_online (default)
      client::kTableName, // table (default)
      false));            // core_dump_on_failure

  // Sleep until the splitting is no longer disabled (we already waited for
  // split_completion_timeout_sec_ seconds in the previous step).
  std::this_thread::sleep_for(kExtraSleepDuration);
  // Splitting should succeed once the delay has expired and FLAGS_enable_automatic_tablet_splitting
  // is true.
  ASSERT_OK(WaitForTabletSplitCompletion(2));
}

TEST_F(AutomaticTabletSplitITest, TabletSplitHasClusterReplicationInfo) {
  constexpr int kNumRowsPerBatch = 1000;
  // This test relies on the fact that the high_phase_size_threshold > force_split_threshold
  // to ensure that without the placement code the test will fail
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_shard_count_per_node) = 0;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_high_phase_shard_count_per_node) = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_size_threshold_bytes) = 50_KB;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_high_phase_size_threshold_bytes) = 100_KB;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_force_split_threshold_bytes) = 50_KB;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_skip_post_split_compaction) = true;
  // Disable automatic compactions
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rocksdb_level0_file_num_compaction_trigger) =
      std::numeric_limits<int32>::max();
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_nodes_per_cloud) = 1;

  std::vector<string> clouds = {"cloud1_split", "cloud2_split", "cloud3_split", "cloud4_split"};
  std::vector<string> regions = {"rack1_split", "rack2_split", "rack3_split", "rack4_split"};
  std::vector<string> zones = {"zone1_split", "zone2_split", "zone3_split", "zone4_split"};

  // Create 4 tservers with the placement info from above
  for (size_t i = 0; i < clouds.size(); i++) {
    tserver::TabletServerOptions extra_opts =
      ASSERT_RESULT(tserver::TabletServerOptions::CreateTabletServerOptions());
    extra_opts.SetPlacement(clouds.at(i), regions.at(i), zones.at(i));
    auto new_ts = cluster_->num_tablet_servers();
    ASSERT_OK(cluster_->AddTabletServer(extra_opts));
    ASSERT_OK(cluster_->WaitForTabletServerCount(new_ts + 1));
  }

  // Set cluster level placement information using only the first 3 clouds/regions/zones
  master::ReplicationInfoPB replication_info;
  replication_info.mutable_live_replicas()->set_num_replicas(
      narrow_cast<int32_t>(clouds.size() - 1));
  for (size_t i = 0; i < clouds.size() - 1; i++) {
    auto* placement_block = replication_info.mutable_live_replicas()->add_placement_blocks();
    auto* cloud_info = placement_block->mutable_cloud_info();
    cloud_info->set_placement_cloud(clouds.at(i));
    cloud_info->set_placement_region(regions.at(i));
    cloud_info->set_placement_zone(zones.at(i));
    placement_block->set_min_num_replicas(1);
  }
  ASSERT_OK(client_->SetReplicationInfo(replication_info));

  // Create and split single tablet into 2 partitions
  // The split should happen at the high threshold
  int key = 1;
  CreateSingleTablet();
  auto peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
  ASSERT_EQ(peers.size(), 1);
  ASSERT_OK(AutomaticallySplitSingleTablet(peers.at(0)->tablet_id(), kNumRowsPerBatch,
    FLAGS_tablet_split_high_phase_size_threshold_bytes, &key));

  peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
  ASSERT_EQ(peers.size(), 2);
  auto tablet_id_to_split = peers.at(0)->tablet_id();

  // Split one of the 2 tablets to get 3 partitions
  // The split should happen at the high threshhold
  ASSERT_OK(CompactTablet(tablet_id_to_split));
  ASSERT_OK(AutomaticallySplitSingleTablet(tablet_id_to_split, kNumRowsPerBatch,
    FLAGS_tablet_split_high_phase_size_threshold_bytes, &key));

  // Split one of the 3 remaining tablets to get 4 partitions
  // The split should happen at the force split threshold
  // We set the high phase > force split to ensure that we split at the force split level
  // given the custom placement information
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_high_phase_size_threshold_bytes) = 300_KB;
  tablet_id_to_split = peers.at(1)->tablet_id();
  ASSERT_OK(CompactTablet(tablet_id_to_split));
  ASSERT_OK(AutomaticallySplitSingleTablet(tablet_id_to_split, kNumRowsPerBatch,
    FLAGS_tablet_force_split_threshold_bytes, &key));
}

TEST_F(AutomaticTabletSplitITest, AutomaticTabletSplittingWaitsForAllPeersCompacted) {
  constexpr auto kNumRowsPerBatch = 1000;

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_outstanding_tablet_split_limit) = 5;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_shard_count_per_node) = 2;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_size_threshold_bytes) = 100_KB;
  // Disable post split compaction
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_skip_post_split_compaction) = true;
  // Disable automatic compactions
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rocksdb_level0_file_num_compaction_trigger) =
      std::numeric_limits<int32>::max();

  int key = 1;
  CreateSingleTablet();
  auto peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
  ASSERT_EQ(peers.size(), 1);
  ASSERT_OK(AutomaticallySplitSingleTablet(peers.at(0)->tablet_id(), kNumRowsPerBatch,
    FLAGS_tablet_split_low_phase_size_threshold_bytes, &key));

  std::unordered_set<string> tablet_ids = ListActiveTabletIdsForTable(cluster_.get(), table_->id());
  ASSERT_EQ(tablet_ids.size(), 2);
  auto expected_num_tablets = 2;

  // Compact peers one by one and ensure a tablet is not split until all peers are compacted
  for (const auto& tablet_id : tablet_ids) {
    auto peers = ListTabletPeers(cluster_.get(), [&tablet_id](auto peer) {
      return peer->tablet_id() == tablet_id;
    });
    ASSERT_EQ(peers.size(), FLAGS_replication_factor);
    for (const auto& peer : peers) {
      // We shouldn't have split this tablet yet since not all peers are compacted yet
      EXPECT_EQ(
        ListTableActiveTabletPeers(cluster_.get(), table_->id()).size(),
        expected_num_tablets * FLAGS_replication_factor);

      // Force a manual rocksdb compaction on the peer tablet and wait for it to complete
      const auto tablet = peer->shared_tablet();
      ASSERT_OK(tablet->Flush(tablet::FlushMode::kSync));
      tablet->TEST_ForceRocksDBCompact();
      ASSERT_OK(LoggedWaitFor(
        [peer]() -> Result<bool> {
          return peer->tablet_metadata()->has_been_fully_compacted();
        },
        15s * kTimeMultiplier,
        "Wait for post tablet split compaction to be completed for peer: " + peer->tablet_id()));

      // Write enough data to get the tablet into a state where it's large enough for a split
      int64_t current_size = 0;
      while (current_size <= FLAGS_tablet_split_low_phase_size_threshold_bytes) {
        ASSERT_OK(WriteRows(kNumRowsPerBatch, key));
        key += kNumRowsPerBatch;
        ASSERT_OK(FlushAllTabletReplicas(tablet_id, table_->id()));
        auto current_size_res = GetMinSstFileSizeAmongAllReplicas(tablet_id);
        if (!current_size_res.ok()) {
          break;
        }
        current_size = current_size_res.get();
      }

      // Wait for a potential split to get triggered
      std::this_thread::sleep_for(
        2 * (FLAGS_catalog_manager_bg_task_wait_ms * 2ms + FLAGS_raft_heartbeat_interval_ms * 2ms));
    }

    // Now that all peers have been compacted, we expect this tablet to get split.
    ASSERT_OK(
      WaitForTabletSplitCompletion(++expected_num_tablets));
  }
}


TEST_F(AutomaticTabletSplitITest, AutomaticTabletSplittingMovesToNextPhase) {
  constexpr int kNumRowsPerBatch = 1000;

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_outstanding_tablet_split_limit) = 5;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_size_threshold_bytes) = 50_KB;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_high_phase_size_threshold_bytes) = 100_KB;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_shard_count_per_node) = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_high_phase_shard_count_per_node) = 2;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rocksdb_disable_compactions) = true;

  auto num_tservers = cluster_->num_tablet_servers();
  const auto this_phase_tablet_lower_limit =
      FLAGS_tablet_split_low_phase_shard_count_per_node * num_tservers;
  const auto this_phase_tablet_upper_limit =
      FLAGS_tablet_split_high_phase_shard_count_per_node * num_tservers;

  // Create table with a number of tablets that puts it into the high phase for tablet splitting.
  SetNumTablets(narrow_cast<uint32_t>(this_phase_tablet_lower_limit));
  CreateTable();

  auto get_num_tablets = [this]() {
    return ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id()).size();
  };

  auto key = 1;
  while (get_num_tablets() < this_phase_tablet_upper_limit) {
    ASSERT_OK(WriteRows(kNumRowsPerBatch, key));
    key += kNumRowsPerBatch;
    auto peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
    for (const auto& peer : peers) {
      // Flush other replicas of this shard to ensure that even if the leader changed we will be in
      // a state where yb-master should initiate a split.
      ASSERT_OK(FlushAllTabletReplicas(peer->tablet_id(), table_->id()));
      auto peer_tablet = peer->shared_tablet();
      if (!peer_tablet) {
        // If this tablet was split after we computed peers above, then the shared_tablet() call may
        // return null.
        continue;
      }
      ssize_t size = peer->shared_tablet()->GetCurrentVersionSstFilesSize();
      if (size > FLAGS_tablet_split_high_phase_size_threshold_bytes) {
        // Wait for the tablet count to go up by at least one, indicating some tablet was split, or
        // for the total number of tablets to put this table outside of the high phase.
        ASSERT_OK(WaitFor([&]() {
          auto num_tablets = get_num_tablets();
          return num_tablets > peers.size() || num_tablets >= this_phase_tablet_upper_limit;
        }, 10s * kTimeMultiplier, "Waiting for split of oversized tablet."));
      }
    }
  }
  EXPECT_EQ(get_num_tablets(), this_phase_tablet_upper_limit);
}

TEST_F(AutomaticTabletSplitITest, PrioritizeLargeTablets) {
  constexpr int kNumRowsBase = 3000;
  constexpr int kNumExtraRowsPerTable = 100;
  constexpr int kNumTables = 5;

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_size_threshold_bytes) = 0;
  // Disable automatic compactions.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rocksdb_level0_file_num_compaction_trigger) =
      std::numeric_limits<int32>::max();
  // Disable post split compactions.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_skip_post_split_compaction) = true;

  // Disable splitting until all data has been written.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_automatic_tablet_splitting) = false;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_outstanding_tablet_split_limit) = 1;

  // Create tables with 1 tablet each.
  client::TableHandle tables[kNumTables];
  auto catalog_mgr = ASSERT_RESULT(catalog_manager());
  for (int i = 0; i < kNumTables; ++i) {
    auto table_name = client::YBTableName(YQL_DATABASE_CQL,
                                          "my_keyspace",
                                          "table_" + std::to_string(i));
    client::kv_table_test::CreateTable(
        client::Transactional(true), 1 /* num_tablets */, client_.get(), &tables[i], table_name);
    ASSERT_OK(WriteRows(&tables[i], kNumRowsBase + i * kNumExtraRowsPerTable, 1));
    const auto peers = ListTableActiveTabletPeers(cluster_.get(), tables[i]->id());
    ASSERT_EQ(peers.size(), 3);

    // Wait for the write transaction to move from intents db to regular db on each peer before
    // trying to flush.
    for (const auto& peer : peers) {
      ASSERT_OK(WaitFor([&]() {
        return peer->shared_tablet()->transaction_participant()->TEST_CountIntents().first == 0;
      }, 30s, "Did not apply write transactions from intents db in time."));
      LOG(INFO) << "Peer size: " << peer->shared_tablet()->GetCurrentVersionSstFilesSize()
                                 << " bytes. Tablet id: " << peer->tablet_id();
    }
    ASSERT_OK(FlushAllTabletReplicas(peers[0]->tablet_id(), tables[i]->id()));

    // Wait for SST file sizes to be updated on the master (via a metrics heartbeat) before enabling
    // splitting (otherwise we might split a tablet which is not the largest tablet).
    auto tablet = ASSERT_RESULT(catalog_mgr->GetTabletInfo(peers[0]->tablet_id()));
    ASSERT_OK(WaitFor([&]() {
      auto drive_info = tablet->GetLeaderReplicaDriveInfo();
      if (!drive_info.ok()) {
        return false;
      }
      return drive_info.get().sst_files_size > 0;
    }, 10s, "Wait for tablet heartbeat."));
  }

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_automatic_tablet_splitting) = true;
  for (int i = kNumTables - 1; i >= 0; --i) {
    ASSERT_OK(WaitForTabletSplitCompletion(
        2,                   // expected_non_split_tablets
        0,                   // expected_split_tablets (default)
        0,                   // num_replicas_online (default)
        tables[i]->name())); // table (default)
    for (const auto& peer : ListTableActiveTabletPeers(cluster_.get(), tables[i]->id())) {
      ASSERT_OK(peer->shared_tablet()->ForceFullRocksDBCompact());
    }
  }
}

TEST_F(AutomaticTabletSplitITest, AutomaticTabletSplittingMultiPhase) {
  constexpr int kNumRowsPerBatch = RegularBuildVsSanitizers(5000, 1000);

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_size_threshold_bytes) = 10_KB;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_high_phase_size_threshold_bytes) = 20_KB;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_shard_count_per_node) = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_high_phase_shard_count_per_node) = 2;
  // Disable automatic compactions, but continue to allow manual compactions.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rocksdb_level0_file_num_compaction_trigger) =
      std::numeric_limits<int32>::max();
  // Disable post split compactions.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_skip_post_split_compaction) = true;

  SetNumTablets(1);
  CreateTable();

  int key = 1;
  const auto num_tservers = cluster_->num_tablet_servers();
  std::unordered_map<TabletId, std::unordered_set<TabletServerId>> tablet_id_to_known_peers;
  for (const auto& peer : ListTableActiveTabletPeers(cluster_.get(), table_->id())) {
    tablet_id_to_known_peers[peer->tablet_id()].insert(peer->permanent_uuid());
  }

  size_t num_peers = num_tservers;

  auto test_phase = [&key, &num_peers, &tablet_id_to_known_peers, this](
      size_t tablet_count_limit, uint64_t split_threshold_bytes) {
    while (num_peers < tablet_count_limit) {
      ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_automatic_tablet_splitting) = false;
      ASSERT_OK(WriteRows(kNumRowsPerBatch, key));
      key += kNumRowsPerBatch;

      // Iterate through all peers (not just leaders) since we require post-split compactions to
      // complete on all of a tablet's peers before splitting it.
      const auto peers = ListTableActiveTabletPeers(cluster_.get(), table_->id());
      if (peers.size() > num_peers) {
        // If a new tablet was formed, it means one of the tablets from the last iteration was
        // split. In that case, verify that some peer has greater than the current split threshold
        // bytes on disk. Note that it would not have compacted away the post split orphaned bytes
        // since automatic compactions are off, so we expect this verification to pass with
        // certainty.
        int new_peers = 0;
        for (const auto& peer : peers) {
          if (tablet_id_to_known_peers[peer->tablet_id()].count(peer->permanent_uuid()) == 0) {
            ++new_peers;
            // Since we've disabled compactions, each post-split subtablet should be larger than the
            // split size threshold.
            ASSERT_GE(peer->shared_tablet()->GetCurrentVersionSstFilesSize(),
                      split_threshold_bytes);
            tablet_id_to_known_peers[peer->tablet_id()].insert(peer->permanent_uuid());
          }
        }
        // Should have two new peers per split tablet (on a tserver).
        const uint64_t num_new_splits = peers.size() - num_peers;
        ASSERT_EQ(new_peers, 2 * num_new_splits);

        num_peers = peers.size();
      }

      for (const auto& peer : peers) {
        ASSERT_OK(peer->shared_tablet()->Flush(tablet::FlushMode::kSync));
        // Compact each tablet to remove the orphaned post-split data so that it can be split again.
        ASSERT_OK(peer->shared_tablet()->ForceFullRocksDBCompact());
      }
      ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_automatic_tablet_splitting) = true;
      SleepForBgTaskIters(2);
    }
  };
  test_phase(
    FLAGS_tablet_split_low_phase_shard_count_per_node * num_tservers,
    FLAGS_tablet_split_low_phase_size_threshold_bytes);
  test_phase(
    FLAGS_tablet_split_high_phase_shard_count_per_node * num_tservers,
    FLAGS_tablet_split_high_phase_size_threshold_bytes);
}

TEST_F(AutomaticTabletSplitITest, LimitNumberOfOutstandingTabletSplits) {
  constexpr int kNumRowsPerBatch = 1000;
  constexpr int kTabletSplitLimit = 3;

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_shard_count_per_node) = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_size_threshold_bytes) = 0;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_high_phase_size_threshold_bytes) = 0;

  // Limit the number of tablet splits.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_outstanding_tablet_split_limit) = kTabletSplitLimit;
  // Start with candidate processing off.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_automatic_tablet_splitting) = false;

  // Randomly fail a percentage of tablet splits to ensure that failed splits get removed.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_fail_tablet_split_probability) = IsTsan() ? 0.1 : 0.2;

  // Create a table with kTabletSplitLimit tablets.
  int num_tablets = kTabletSplitLimit;
  SetNumTablets(num_tablets);
  CreateTable();
  // Add some data.
  ASSERT_OK(WriteRows(kNumRowsPerBatch, 1));

  // Main test loop:
  // Each loop we will split kTabletSplitLimit tablets with post split compactions disabled.
  // We will then wait until we have that many tablets split, at which point we will reenable post
  // split compactions.
  // We will then wait until the post split compactions are done, then repeat.
  auto peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
  for (int split_round = 0; split_round < 3; ++split_round) {
    for (const auto& peer : peers) {
      // Flush other replicas of this shard to ensure that even if the leader changed we will be in
      // a state where yb-master should initiate a split.
      ASSERT_OK(FlushAllTabletReplicas(peer->tablet_id(), table_->id()));
    }

    // Keep tablets without compaction after split.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_before_post_split_compaction) = true;
    // Enable splitting.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_automatic_tablet_splitting) = true;

    ASSERT_OK(WaitForTabletSplitCompletion(num_tablets + kTabletSplitLimit));
    // Ensure that we don't split any more tablets.
    SleepForBgTaskIters(2);
    ASSERT_NOK(WaitForTabletSplitCompletion(
        num_tablets + kTabletSplitLimit + 1,  // expected_non_split_tablets
        0,                                    // expected_split_tablets (default)
        0,                                    // num_replicas_online (default)
        client::kTableName,                   // table (default)
        false));                              // core_dump_on_failure

    // Pause any more tablet splits.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_automatic_tablet_splitting) = false;
    // Reenable post split compaction, wait for this to complete so next tablets can be split.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_before_post_split_compaction) = false;

    ASSERT_OK(WaitForTestTablePostSplitTabletsFullyCompacted(15s * kTimeMultiplier));

    peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
    num_tablets = narrow_cast<int32_t>(peers.size());

    // There should be kTabletSplitLimit initial tablets + kTabletSplitLimit new tablets per loop.
    EXPECT_EQ(num_tablets, (split_round + 2) * kTabletSplitLimit);
  }

  // TODO (jhe) For now we need to manually delete the cluster otherwise the cluster verifier can
  // get stuck waiting for tablets that got registered but whose tablet split got cancelled by
  // FLAGS_TEST_fail_tablet_split_probability.
  // We should either have a way to wait for these tablets to get split, or have a way to delete
  // these tablets in case a tablet split fails.
  cluster_->Shutdown();
}

TEST_F(AutomaticTabletSplitITest, LimitNumberOfOutstandingTabletSplitsPerTserver) {
  constexpr int kNumRowsPerBatch = 2000;

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_outstanding_tablet_split_limit) = 5;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_outstanding_tablet_split_limit_per_tserver) = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_shard_count_per_node) = 10000;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_size_threshold_bytes) = 0;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rocksdb_disable_compactions) = true;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_tserver_get_split_key) = true;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_fail_tablet_split_probability) = 1.0;
  // Need to disable load balancing until the first tablet is split, otherwise it might end up
  // being overreplicated on 4 tservers when we split, resulting in the split children being on 4
  // tservers.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_load_balancing) = false;

  ASSERT_OK(cluster_->AddTabletServer());
  ASSERT_OK(cluster_->AddTabletServer());
  ASSERT_OK(cluster_->WaitForTabletServerCount(5));

  SetNumTablets(2);
  CreateTable();

  auto catalog_mgr = ASSERT_RESULT(catalog_manager());
  auto table_info = catalog_mgr->GetTableInfo(table_->id());

  auto peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
  ASSERT_EQ(peers.size(), 2);
  ASSERT_OK(WriteRows(kNumRowsPerBatch, 1));
  for (const auto& peer : peers) {
    ASSERT_OK(WaitFor([&]() {
      return peer->shared_tablet()->transaction_participant()->TEST_CountIntents().first == 0;
    }, 30s, "Did not apply transaction from intents db in time."));
  }
  // Flush to ensure an SST file is generated so splitting can occur.
  // One of the tablets (call it A) should be automatically split after the flush. Since RF=3 and we
  // have 5 tservers, the other tablet (B) must share at least one tserver with A. Since we limit
  // the number of outstanding splits on a tserver to 1, B should not be split (since that would
  // result in two outstanding splits on the tserver that hosted a replica of A and B).
  ASSERT_OK(FlushAllTabletReplicas(peers[0]->tablet_id(), table_->id()));
  ASSERT_OK(FlushAllTabletReplicas(peers[1]->tablet_id(), table_->id()));

  // Check that no more than 1 split task is created (the split task should be counted as an
  // ongoing split).
  SleepForBgTaskIters(4);
  int num_split_tasks = 0;
  for (const auto& task : table_info->GetTasks()) {
    // These tasks will retry automatically until they succeed or fail.
    if (task->type() == yb::server::MonitoredTask::ASYNC_GET_TABLET_SPLIT_KEY ||
        task->type() == yb::server::MonitoredTask::ASYNC_SPLIT_TABLET) {
      ++num_split_tasks;
    }
  }
  ASSERT_EQ(num_split_tasks, 1);
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_tserver_get_split_key) = false;

  // Check that non-running child tablets count against the per-tserver split limit, and so only
  // one split is triggered.
  SleepForBgTaskIters(4);
  ASSERT_EQ(table_info->GetTablets().size(), 3);
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_fail_tablet_split_probability) = 0.0;

  ASSERT_OK(WaitForTabletSplitCompletion(3));
  ASSERT_NOK(WaitForTabletSplitCompletion(
      4,                                    // expected_non_split_tablets
      0,                                    // expected_split_tablets (default)
      0,                                    // num_replicas_online (default)
      client::kTableName,                   // table (default)
      false));                              // core_dump_on_failure

  // Add a 6th tserver. Tablet B should be load balanced onto the three tservers that do not have
  // replicas of tablet A, and should subsequently split. Note that the children of A should remain
  // on the same 3 tservers as A, since we don't move compacting tablets (this is important to
  // ensure that there are no ongoing splits on the 3 tservers that B is on).
  ASSERT_OK(cluster_->AddTabletServer());
  ASSERT_OK(cluster_->WaitForTabletServerCount(6));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_load_balancing) = true;
  ASSERT_OK(WaitFor([&]() {
    std::unordered_set<std::string> seen_tservers;
    for (const auto& peer : ListTableActiveTabletPeers(cluster_.get(), table_->id())) {
      seen_tservers.insert(peer->permanent_uuid());
    }
    LOG(INFO) << "seen_tservers.size(): " <<  seen_tservers.size();
    return seen_tservers.size() == 6;
  }, 30s * kTimeMultiplier, "Did not load balance in time."));

  ASSERT_OK(WaitForTabletSplitCompletion(4));
}

TEST_F(AutomaticTabletSplitITest, DroppedTablesExcludedFromOutstandingSplitLimit) {
  constexpr int kNumRowsPerBatch = 1000;
  constexpr int kTabletSplitLimit = 1;
  constexpr int kNumInitialTablets = 1;

  // Limit the number of tablet splits.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_outstanding_tablet_split_limit) = kTabletSplitLimit;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_shard_count_per_node) = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_size_threshold_bytes) = 0;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rocksdb_disable_compactions) = true;

  SetNumTablets(kNumInitialTablets);
  CreateTable();
  auto table1_peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
  ASSERT_EQ(table1_peers.size(), 1);
  ASSERT_OK(WriteRows(kNumRowsPerBatch, 1));
  // Flush to ensure an SST file is generated so splitting can occur.
  ASSERT_OK(FlushAllTabletReplicas(table1_peers[0]->tablet_id(), table_->id()));
  ASSERT_OK(WaitForTabletSplitCompletion(kNumInitialTablets + 1));

  client::TableHandle table2;
  auto table2_name = client::YBTableName(YQL_DATABASE_CQL, "my_keyspace", "ql_client_test_table_2");
  client::kv_table_test::CreateTable(
      client::Transactional(true), kNumInitialTablets, client_.get(), &table2, table2_name);
  auto table2_peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table2->id());
  ASSERT_EQ(table2_peers.size(), 1);
  ASSERT_OK(WriteRows(&table2, kNumRowsPerBatch, 1));
  // Flush to ensure an SST file is generated so splitting can occur.
  ASSERT_OK(FlushAllTabletReplicas(table2_peers[0]->tablet_id(), table2->id()));

  // The tablet should not split while the split for the first table is outstanding.
  SleepForBgTaskIters(2);
  ASSERT_NOK(WaitForTabletSplitCompletion(
      kNumInitialTablets + 1, // expected_non_split_tablets
      0,                      // expected_split_tablets (default)
      0,                      // num_replicas_online (default)
      table2_name,            // table
      false));                // core_dump_on_failure

  // After deleting the first table, its split should no longer be counted for an ongoing split, so
  // the second table's tablet should split.
  ASSERT_OK(client_->DeleteTable(client::kTableName));
  ASSERT_OK(WaitForTabletSplitCompletion(
      kNumInitialTablets + 1, // expected_non_split_tablets
      0,                      // expected_split_tablets (default)
      0,                      // num_replicas_online (default)
      table2_name));          // table
}

TEST_F(AutomaticTabletSplitITest, IncludeTasksInOutstandingSplits) {
  constexpr int kNumRowsPerBatch = 1000;
  constexpr int kInitialNumTablets = 2;

  // Only allow one tablet split to start.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_outstanding_tablet_split_limit) = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_shard_count_per_node) = 100;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_size_threshold_bytes) = 0;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_tserver_get_split_key) = true;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_reject_delete_not_serving_tablet_rpc) = true;

  // Start with two tablets. Only one should be chosen for splitting, and it should stall (but be
  // counted as outstanding) until the pause is removed.
  SetNumTablets(kInitialNumTablets);
  CreateTable();
  auto peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
  ASSERT_EQ(peers.size(), kInitialNumTablets);
  ASSERT_OK(WriteRows(kNumRowsPerBatch, 1));
  // Flush to ensure an SST file is generated so splitting can occur.
  for (const auto& peer : peers) {
    ASSERT_OK(FlushAllTabletReplicas(peer->tablet_id(), table_->id()));
  }
  // Assert that the other tablet does not get split.
  SleepForBgTaskIters(2);
  ASSERT_NOK(WaitForTabletSplitCompletion(
      kInitialNumTablets + 1,               // expected_non_split_tablets
      1,                                    // expected_split_tablets
      0,                                    // num_replicas_online (default)
      client::kTableName,                   // table (default)
      false));                              // core_dump_on_failure

  // Allow no new splits. The stalled split task should resume after the pause is removed.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_automatic_tablet_splitting) = false;
  auto catalog_mgr = ASSERT_RESULT(catalog_manager());
  ASSERT_OK(WaitFor([&]() {
    return !catalog_mgr->tablet_split_manager()->IsRunning();
  }, 10s, "Wait for tablet split manager to stop running."));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_tserver_get_split_key) = false;
  ASSERT_OK(WaitForTabletSplitCompletion(kInitialNumTablets + 1, /* expected_non_split_tablets */
                                         1 /* expected_split_tablets */));
  SleepForBgTaskIters(2);
  ASSERT_NOK(WaitForTabletSplitCompletion(
      kInitialNumTablets + 2,               // expected_non_split_tablets
      1,                                    // expected_split_tablets
      0,                                    // num_replicas_online (default)
      client::kTableName,                   // table (default)
      false));                              // core_dump_on_failure
}

TEST_F(AutomaticTabletSplitITest, FailedSplitIsRestarted) {
  constexpr int kNumRowsPerBatch = 1000;

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_shard_count_per_node) = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tablet_split_low_phase_size_threshold_bytes) = 0;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rocksdb_disable_compactions) = true;
  // Fail the split on the tserver.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_fail_tablet_split_probability) = 1;

  CreateSingleTablet();
  auto peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
  ASSERT_EQ(peers.size(), 1);
  ASSERT_OK(WriteRows(kNumRowsPerBatch, 1));
  // Flush to ensure an SST file is generated so splitting can occur.
  ASSERT_OK(FlushAllTabletReplicas(peers[0]->tablet_id(), table_->id()));

  // The split should fail because of the test flag.
  SleepForBgTaskIters(2);
  ASSERT_NOK(WaitForTabletSplitCompletion(
      2,                                    // expected_non_split_tablets
      0,                                    // expected_split_tablets (default)
      0,                                    // num_replicas_online (default)
      client::kTableName,                   // table (default)
      false));                              // core_dump_on_failure

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_fail_tablet_split_probability) = 0;
  ASSERT_OK(WaitForTabletSplitCompletion(2));
}

// Similar to the FailedSplitIsRestarted test, but crash instead.
TEST_F(AutomaticTabletSplitExternalMiniClusterITest, CrashedSplitIsRestarted) {
  constexpr int kNumRows = 1000;

  ASSERT_OK(cluster_->SetFlagOnMasters("tablet_split_low_phase_shard_count_per_node", "1"));
  ASSERT_OK(cluster_->SetFlagOnMasters("tablet_split_low_phase_size_threshold_bytes", "0"));
  ASSERT_OK(cluster_->SetFlagOnMasters("TEST_fault_crash_after_registering_split_children", "1.0"));
  ASSERT_OK(cluster_->SetFlagOnTServers("rocksdb_disable_compactions", "true"));

  CreateSingleTablet();
  const TabletId tablet_id = ASSERT_RESULT(GetOnlyTestTabletId());
  ExternalMaster* master_leader = cluster_->GetLeaderMaster();
  ASSERT_OK(WriteRows(kNumRows, 1));

  // Sleep to wait for the transaction to be applied from intents.
  std::this_thread::sleep_for(2s);
  // Flush to ensure SST files are generated so splitting can occur.
  for (size_t i = 0; i < cluster_->num_tablet_servers(); ++i) {
    ASSERT_OK(cluster_->FlushTabletsOnSingleTServer(cluster_->tablet_server(i),
                                                    {tablet_id},
                                                    false /* is_compaction */));
  }

  ASSERT_OK(WaitFor([&]() {
    return !master_leader->IsProcessAlive();
  }, (FLAGS_catalog_manager_bg_task_wait_ms + FLAGS_tserver_heartbeat_metrics_interval_ms) * 2ms *
     kTimeMultiplier,
     "Waiting for master leader to crash after trying to split."));

  ASSERT_OK(master_leader->Restart());
  ASSERT_OK(WaitFor([&]() {
    return master_leader->IsProcessAlive();
  }, 30s * kTimeMultiplier, "Waiting for master leader to restart."));

  // These flags get cleared when we restart, so we need to set them again.
  ASSERT_OK(cluster_->SetFlag(master_leader, "tablet_split_low_phase_shard_count_per_node", "1"));
  ASSERT_OK(cluster_->SetFlag(master_leader, "tablet_split_low_phase_size_threshold_bytes", "0"));

  ASSERT_OK(WaitFor([&]() -> Result<bool> {
    auto tablets = CHECK_RESULT(cluster_->ListTablets(cluster_->tablet_server(0)));
    int running_tablets = 0;
    for (const auto& tablet : tablets.status_and_schema()) {
      const auto& status = tablet.tablet_status();
      if (status.state() == tablet::RaftGroupStatePB::RUNNING &&
          status.table_id() == table_->id() &&
          status.tablet_data_state() == tablet::TABLET_DATA_READY) {
        ++running_tablets;
      }
    }
    return running_tablets == 2;
  }, split_completion_timeout_sec_, "Waiting for split children to be running."));
}

class TabletSplitSingleServerITest : public TabletSplitITest {
 protected:
  int64_t GetRF() override { return 1; }

  Result<tablet::TabletPeerPtr> GetSingleTabletLeaderPeer() {
    auto peers = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id());
    SCHECK_EQ(peers.size(), 1U, IllegalState, "Expected only a single tablet leader.");
    return peers.at(0);
  }

  Status AlterTableSetDefaultTTL(int ttl_sec) {
    const auto table_name = table_.name();
    auto alterer = client_->NewTableAlterer(table_name);
    alterer->wait(true);
    TableProperties table_properties;
    table_properties.SetDefaultTimeToLive(ttl_sec * MonoTime::kMillisecondsPerSecond);
    alterer->SetTableProperties(table_properties);
    return alterer->Alter();
  }
};

// Start tablet split, create Index to start backfill while split operation in progress
// and check backfill state.
TEST_F(TabletSplitSingleServerITest, TestBackfillDuringSplit) {
  // TODO(#11695) -- Switch this back to a TabletServerITest
  constexpr auto kNumRows = 10000UL;
  FLAGS_TEST_apply_tablet_split_inject_delay_ms = 200 * kTimeMultiplier;

  CreateSingleTablet();
  const auto split_hash_code = ASSERT_RESULT(WriteRowsAndGetMiddleHashCode(kNumRows));
  auto* catalog_mgr = ASSERT_RESULT(catalog_manager());
  auto table = catalog_mgr->GetTableInfo(table_->id());
  auto source_tablet_info = ASSERT_RESULT(GetSingleTestTabletInfo(catalog_mgr));
  const auto source_tablet_id = source_tablet_info->id();

  // Send SplitTablet RPC to the tablet leader.
  ASSERT_OK(catalog_mgr->TEST_SplitTablet(source_tablet_info, split_hash_code));

  int indexed_column_index = 1;
  const client::YBTableName index_name(
        YQL_DATABASE_CQL, table_.name().namespace_name(),
        table_.name().table_name() + '_' +
          table_.schema().Column(indexed_column_index).name() + "_idx");
  // Create index while split operation in progress
  PrepareIndex(client::Transactional(GetIsolationLevel() != IsolationLevel::NON_TRANSACTIONAL),
               index_name, indexed_column_index);

  // Check that source table is not backfilling and wait for tablet split completion
  ASSERT_FALSE(table->IsBackfilling());
  ASSERT_OK(WaitForTabletSplitCompletion(2));
  ASSERT_OK(CheckPostSplitTabletReplicasData(kNumRows));

  ASSERT_OK(index_.Open(index_name, client_.get()));
  ASSERT_OK(WaitFor([&] {
    auto rows_count = SelectRowsCount(NewSession(), index_);
    if (!rows_count.ok()) {
      return false;
    }
    return *rows_count == kNumRows;
  }, 30s * kTimeMultiplier, "Waiting for backfill index"));
}

// Create Index to start backfill, check split is not working while backfill in progress
// and check backfill state.
TEST_F(TabletSplitSingleServerITest, TestSplitDuringBackfill) {
  // TODO(#11695) -- Switch this back to a TabletServerITest
  constexpr auto kNumRows = 10000;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_validate_all_tablet_candidates) = false;
  FLAGS_TEST_slowdown_backfill_alter_table_rpcs_ms = 200 * kTimeMultiplier;

  CreateSingleTablet();
  const auto split_hash_code = ASSERT_RESULT(WriteRowsAndGetMiddleHashCode(kNumRows));

  int indexed_column_index = 1;
  const client::YBTableName index_name(
        YQL_DATABASE_CQL, table_.name().namespace_name(),
        table_.name().table_name() + '_' +
          table_.schema().Column(indexed_column_index).name() + "_idx");
  // Create index and start backfill
  PrepareIndex(client::Transactional(GetIsolationLevel() != IsolationLevel::NON_TRANSACTIONAL),
               index_name, indexed_column_index);

  auto* catalog_mgr = ASSERT_RESULT(catalog_manager());
  auto table = catalog_mgr->GetTableInfo(table_->id());
  auto source_tablet_info = ASSERT_RESULT(GetSingleTestTabletInfo(catalog_mgr));
  const auto source_tablet_id = source_tablet_info->id();

  // Check that source table is backfilling
  ASSERT_OK(WaitFor([&] {
    return table->IsBackfilling();
  }, 30s * kTimeMultiplier, "Waiting for start backfill index"));

  // Send SplitTablet RPC to the tablet leader while backfill in progress
  ASSERT_NOK(catalog_mgr->TEST_SplitTablet(source_tablet_info, split_hash_code));

  ASSERT_OK(WaitFor([&] {
    return !table->IsBackfilling();
  }, 30s * kTimeMultiplier, "Waiting for backfill index"));
  ASSERT_OK(catalog_mgr->TEST_SplitTablet(source_tablet_info, split_hash_code));
  ASSERT_OK(WaitForTabletSplitCompletion(2));
  ASSERT_OK(CheckPostSplitTabletReplicasData(kNumRows));
}

TEST_F(TabletSplitSingleServerITest, TabletServerGetSplitKey) {
  constexpr auto kNumRows = kDefaultNumRows;
  // Setup table with rows.
  CreateSingleTablet();
  ASSERT_OK(WriteRowsAndGetMiddleHashCode(kNumRows));
  const auto source_tablet_id =
      ASSERT_RESULT(GetSingleTestTabletInfo(ASSERT_RESULT(catalog_manager())))->id();

  // Flush tablet and directly compute expected middle key.
  auto tablet_peer = ASSERT_RESULT(GetSingleTabletLeaderPeer());
  ASSERT_OK(tablet_peer->shared_tablet()->Flush(tablet::FlushMode::kSync));
  auto middle_key = ASSERT_RESULT(tablet_peer->shared_tablet()->GetEncodedMiddleSplitKey());
  auto expected_middle_key_hash = CHECK_RESULT(docdb::DocKey::DecodeHash(middle_key));

  // Send RPC.
  auto resp = ASSERT_RESULT(GetSplitKey(source_tablet_id));

  // Validate response.
  CHECK(!resp.has_error()) << resp.error().DebugString();
  auto decoded_split_key_hash = CHECK_RESULT(docdb::DocKey::DecodeHash(resp.split_encoded_key()));
  CHECK_EQ(decoded_split_key_hash, expected_middle_key_hash);
  auto decoded_partition_key_hash = PartitionSchema::DecodeMultiColumnHashValue(
      resp.split_partition_key());
  CHECK_EQ(decoded_partition_key_hash, expected_middle_key_hash);
}

TEST_F(TabletSplitSingleServerITest, SplitKeyNotSupportedForTTLTablets) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rocksdb_max_file_size_for_compaction) = 100_KB;

  constexpr auto kNumRows = kDefaultNumRows;

  CreateSingleTablet();
  ASSERT_RESULT(WriteRowsAndFlush(kNumRows));

  const auto source_tablet_id =
      ASSERT_RESULT(GetSingleTestTabletInfo(ASSERT_RESULT(catalog_manager())))->id();

  // Flush tablet and directly compute expected middle key.
  auto tablet_peer = ASSERT_RESULT(GetSingleTabletLeaderPeer());
  auto tablet = tablet_peer->shared_tablet();
  ASSERT_OK(tablet_peer->shared_tablet()->Flush(tablet::FlushMode::kSync));
  tablet->TEST_ForceRocksDBCompact();

  auto resp = ASSERT_RESULT(GetSplitKey(source_tablet_id));
  EXPECT_FALSE(resp.has_error());

  // Alter the table with a table TTL, and call GetSplitKey RPC, expecting a
  // "not supported" response.
  // Amount of time for the TTL is irrelevant, so long as it's larger than 0.
  ASSERT_OK(AlterTableSetDefaultTTL(1));

  resp = ASSERT_RESULT(GetSplitKey(source_tablet_id));

  // Validate response
  EXPECT_TRUE(resp.has_error());
  EXPECT_EQ(resp.error().code(),
      tserver::TabletServerErrorPB_Code::TabletServerErrorPB_Code_TABLET_SPLIT_DISABLED_TTL_EXPIRY);
  EXPECT_TRUE(resp.error().has_status());
  EXPECT_TRUE(resp.error().status().has_message());
  EXPECT_EQ(resp.error().status().code(),
            yb::AppStatusPB::ErrorCode::AppStatusPB_ErrorCode_NOT_SUPPORTED);
}

TEST_F(TabletSplitSingleServerITest, MaxFileSizeTTLTabletOnlyValidForManualSplit) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rocksdb_max_file_size_for_compaction) = 100_KB;

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rocksdb_disable_compactions) = true;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_validate_all_tablet_candidates) = false;

  constexpr auto kNumRows = kDefaultNumRows;

  CreateSingleTablet();
  ASSERT_RESULT(WriteRowsAndFlush(kNumRows));

  auto* catalog_mgr = ASSERT_RESULT(catalog_manager());
  auto* split_manager = catalog_mgr->tablet_split_manager();
  auto source_tablet_info = ASSERT_RESULT(GetSingleTestTabletInfo(catalog_mgr));

  // Requires a metrics heartbeat to get max_file_size_for_compaction flag to master.
  auto ts_desc = ASSERT_RESULT(source_tablet_info->GetLeader());
  EXPECT_OK(WaitFor([&]() -> Result<bool> {
      return ts_desc->uptime_seconds() > 0;
    }, 10s * kTimeMultiplier, "Wait for TServer to report metrics."));
  EXPECT_TRUE(ts_desc->get_disable_tablet_split_if_default_ttl());

  // Candidate tablet should still be valid since default TTL not enabled.
  ASSERT_OK(split_manager->ValidateSplitCandidateTablet(*source_tablet_info));

  // Alter the table with a table TTL, at which point tablet should no longer be valid
  // for tablet splitting.
  // Amount of time for the TTL is irrelevant, so long as it's larger than 0.
  ASSERT_OK(AlterTableSetDefaultTTL(1));
  ASSERT_NOK(split_manager->ValidateSplitCandidateTablet(*source_tablet_info));

  // Tablet should still be a valid candidate if ignore_ttl_validation is set to true
  // (e.g. for manual tablet splitting).
  ASSERT_OK(split_manager->ValidateSplitCandidateTablet(*source_tablet_info,
      master::IgnoreTtlValidation::kTrue, master::IgnoreDisabledList::kTrue));
}

TEST_F(TabletSplitSingleServerITest, AutoSplitNotValidOnceCheckedForTtl) {
  const auto kSecondsBetweenChecks = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rocksdb_disable_compactions) = true;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_validate_all_tablet_candidates) = false;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_prevent_split_for_ttl_tables_for_seconds)
      = kSecondsBetweenChecks;

  constexpr auto kNumRows = kDefaultNumRows;

  CreateSingleTablet();
  ASSERT_RESULT(WriteRowsAndFlush(kNumRows));

  auto* catalog_mgr = ASSERT_RESULT(catalog_manager());
  auto* split_manager = catalog_mgr->tablet_split_manager();
  auto& table_info = *ASSERT_NOTNULL(catalog_mgr->GetTableInfo(table_->id()));

  // Candidate table should start as a valid split candidate.
  ASSERT_OK(split_manager->ValidateSplitCandidateTable(table_info));

  // State that table should not be split for the next 1 second.
  // Candidate table should no longer be valid.
  split_manager->MarkTtlTableForSplitIgnore(table_->id());
  ASSERT_NOK(split_manager->ValidateSplitCandidateTable(table_info));

  // After 2 seconds, table is a valid split candidate again.
  SleepFor(kSecondsBetweenChecks * 2s);
  ASSERT_OK(split_manager->ValidateSplitCandidateTable(table_info));

  // State again that table should not be split for the next 1 second.
  // Candidate table should still be a valid candidate if ignore_disabled_list
  // is true (e.g. in the case of manual tablet splitting).
  split_manager->MarkTtlTableForSplitIgnore(table_->id());
  ASSERT_OK(split_manager->ValidateSplitCandidateTable(table_info,
      master::IgnoreDisabledList::kTrue));
}

TEST_F(TabletSplitSingleServerITest, TabletServerOrphanedPostSplitData) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_rocksdb_disable_compactions) = true;
  constexpr auto kNumRows = 2000;

  auto source_tablet_id = CreateSingleTabletAndSplit(kNumRows);

  // Try to call GetSplitKey RPC on each child tablet that resulted from the split above
  const auto& peers = ListTableActiveTabletPeers(cluster_.get(), table_->id());
  ASSERT_EQ(peers.size(), 2);

  for (const auto& peer : peers) {
      // Send RPC to child tablet.
      auto resp = ASSERT_RESULT(GetSplitKey(peer->tablet_id()));

      // Validate response
      EXPECT_TRUE(resp.has_error());
      EXPECT_TRUE(resp.error().has_status());
      EXPECT_TRUE(resp.error().status().has_message());
      EXPECT_EQ(resp.error().status().code(),
                yb::AppStatusPB::ErrorCode::AppStatusPB_ErrorCode_ILLEGAL_STATE);
      EXPECT_EQ(resp.error().status().message(), "Tablet has orphaned post-split data");
  }
}

TEST_F(TabletSplitSingleServerITest, TabletServerSplitAlreadySplitTablet) {
  constexpr auto kNumRows = 2000;

  CreateSingleTablet();
  auto split_hash_code = ASSERT_RESULT(WriteRowsAndGetMiddleHashCode(kNumRows));
  auto tablet_peer = ASSERT_RESULT(GetSingleTabletLeaderPeer());
  const auto tserver_uuid = tablet_peer->permanent_uuid();

  SetAtomicFlag(true, &FLAGS_TEST_skip_deleting_split_tablets);
  const auto source_tablet_id = ASSERT_RESULT(SplitSingleTablet(split_hash_code));
  ASSERT_OK(WaitForTabletSplitCompletion(
      /* expected_non_split_tablets =*/ 2, /* expected_split_tablets = */ 1));

  auto send_split_request = [this, &tserver_uuid, &source_tablet_id]()
      -> Result<tserver::SplitTabletResponsePB> {
    auto tserver = cluster_->mini_tablet_server(0);
    auto ts_admin_service_proxy = std::make_unique<tserver::TabletServerAdminServiceProxy>(
      proxy_cache_.get(), HostPort::FromBoundEndpoint(tserver->bound_rpc_addr()));
    tablet::SplitTabletRequestPB req;
    req.set_dest_uuid(tserver_uuid);
    req.set_tablet_id(source_tablet_id);
    req.set_new_tablet1_id(Format("$0$1", source_tablet_id, "1"));
    req.set_new_tablet2_id(Format("$0$1", source_tablet_id, "2"));
    req.set_split_partition_key("abc");
    req.set_split_encoded_key("def");
    rpc::RpcController controller;
    controller.set_timeout(kRpcTimeout);
    tserver::SplitTabletResponsePB resp;
    RETURN_NOT_OK(ts_admin_service_proxy->SplitTablet(req, &resp, &controller));
    return resp;
  };

  // If the parent tablet is still around, this should trigger an AlreadyPresent error
  auto resp = ASSERT_RESULT(send_split_request());
  EXPECT_TRUE(resp.has_error());
  EXPECT_TRUE(StatusFromPB(resp.error().status()).IsAlreadyPresent()) << resp.error().DebugString();

  SetAtomicFlag(false, &FLAGS_TEST_skip_deleting_split_tablets);
  ASSERT_OK(WaitForTabletSplitCompletion(/* expected_non_split_tablets =*/ 2));

  // If the parent tablet has been cleaned up, this should trigger a Not Found error.
  resp = ASSERT_RESULT(send_split_request());
  EXPECT_TRUE(resp.has_error());
  EXPECT_TRUE(
      StatusFromPB(resp.error().status()).IsNotFound() ||
      resp.error().code() == tserver::TabletServerErrorPB::TABLET_NOT_FOUND)
      << resp.error().DebugString();
}

TEST_F(TabletSplitExternalMiniClusterITest, Simple) {
  CreateSingleTablet();
  CHECK_OK(WriteRowsAndFlush());
  auto tablet_id = CHECK_RESULT(GetOnlyTestTabletId());
  CHECK_OK(SplitTablet(tablet_id));
  ASSERT_OK(WaitForTablets(3));
}

TEST_F(TabletSplitExternalMiniClusterITest, CrashMasterDuringSplit) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_heartbeat_metrics_interval_ms) =
    FLAGS_heartbeat_interval_ms + 1000;

  ASSERT_OK(SplitTabletCrashMaster(false, nullptr));
}

TEST_F(TabletSplitExternalMiniClusterITest, CrashMasterCheckConsistentPartitionKeys) {
  // Tests that when master crashes during a split and a new split key is used
  // we will revert to an older boundary used by the inital split
  // Used to validate the fix for: https://github.com/yugabyte/yugabyte-db/issues/8148
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_tserver_heartbeat_metrics_interval_ms) =
    FLAGS_heartbeat_interval_ms + 1000;

  string split_partition_key;
  ASSERT_OK(SplitTabletCrashMaster(true, &split_partition_key));

  auto tablets = CHECK_RESULT(ListTablets());
  ASSERT_EQ(tablets.size(), 2);
  auto part = tablets.at(0).tablet_status().partition();
  auto part2 = tablets.at(1).tablet_status().partition();

  // check that both partitions have the same boundary
  if (part.partition_key_end() == part2.partition_key_start() && part.partition_key_end() != "") {
    ASSERT_EQ(part.partition_key_end(), split_partition_key);
  } else {
    ASSERT_EQ(part.partition_key_start(), split_partition_key);
    ASSERT_EQ(part2.partition_key_end(), split_partition_key);
  }
}

TEST_F(TabletSplitExternalMiniClusterITest, FaultedSplitNodeRejectsRemoteBootstrap) {
  constexpr int kTabletSplitInjectDelayMs = 20000 * kTimeMultiplier;
  CreateSingleTablet();
  ASSERT_OK(WriteRowsAndFlush());
  const auto tablet_id = CHECK_RESULT(GetOnlyTestTabletId());

  const auto leader_idx = CHECK_RESULT(cluster_->GetTabletLeaderIndex(tablet_id));
  const auto healthy_follower_idx = (leader_idx + 1) % 3;
  const auto faulted_follower_idx = (leader_idx + 2) % 3;

  auto faulted_follower = cluster_->tablet_server(faulted_follower_idx);
  ASSERT_OK(cluster_->SetFlag(
      faulted_follower, "TEST_crash_before_apply_tablet_split_op", "true"));

  ASSERT_OK(SplitTablet(tablet_id));
  ASSERT_OK(cluster_->WaitForTSToCrash(faulted_follower));

  ASSERT_OK(faulted_follower->Restart(ExternalMiniClusterOptions::kDefaultStartCqlProxy, {
    std::make_pair(
        "TEST_apply_tablet_split_inject_delay_ms", Format("$0", kTabletSplitInjectDelayMs))
  }));

  consensus::StartRemoteBootstrapRequestPB req;
  req.set_split_parent_tablet_id(tablet_id);
  req.set_dest_uuid(faulted_follower->uuid());
  // We put some bogus values for these next two required fields.
  req.set_tablet_id("::std::string &&value");
  req.set_bootstrap_peer_uuid("abcdefg");
  consensus::StartRemoteBootstrapResponsePB resp;
  rpc::RpcController rpc;
  rpc.set_timeout(kRpcTimeout);
  auto s = cluster_->GetConsensusProxy(faulted_follower).StartRemoteBootstrap(req, &resp, &rpc);
  EXPECT_OK(s);
  EXPECT_TRUE(resp.has_error());
  EXPECT_EQ(resp.error().code(), tserver::TabletServerErrorPB::TABLET_SPLIT_PARENT_STILL_LIVE);

  SleepFor(1ms * kTabletSplitInjectDelayMs);
  EXPECT_OK(WaitForTablets(2));
  EXPECT_OK(WaitForTablets(2, faulted_follower_idx));

  // By shutting down the healthy follower and writing rows to the table, we ensure the faulted
  // follower is eventually able to rejoin the raft group.
  auto healthy_follower = cluster_->tablet_server(healthy_follower_idx);
  healthy_follower->Shutdown();
  EXPECT_OK(WaitFor([&]() -> Result<bool> {
    return WriteRows(10).ok();
  }, 20s * kTimeMultiplier, "Write rows after requiring faulted follower."));

  ASSERT_OK(healthy_follower->Restart());
}

TEST_F(TabletSplitExternalMiniClusterITest, CrashesAfterChildLogCopy) {
  ASSERT_OK(cluster_->SetFlagOnMasters("unresponsive_ts_rpc_retry_limit", "0"));

  CreateSingleTablet();
  CHECK_OK(WriteRowsAndFlush());
  const auto tablet_id = CHECK_RESULT(GetOnlyTestTabletId());

  // We will fault one of the non-leader servers after it performs a WAL Log copy from parent to
  // the first child, but before it can mark the child as TABLET_DATA_READY.
  const auto leader_idx = CHECK_RESULT(cluster_->GetTabletLeaderIndex(tablet_id));
  const auto faulted_follower_idx = (leader_idx + 2) % 3;
  const auto non_faulted_follower_idx = (leader_idx + 1) % 3;

  auto faulted_follower = cluster_->tablet_server(faulted_follower_idx);
  CHECK_OK(cluster_->SetFlag(
      faulted_follower, "TEST_fault_crash_in_split_after_log_copied", "1.0"));

  CHECK_OK(SplitTablet(tablet_id));
  CHECK_OK(cluster_->WaitForTSToCrash(faulted_follower));

  CHECK_OK(faulted_follower->Restart());

  ASSERT_OK(cluster_->WaitForTabletsRunning(faulted_follower, 20s * kTimeMultiplier));
  ASSERT_OK(WaitForTablets(3));

  ASSERT_OK(WaitFor([&]() -> Result<bool> {
    return WriteRows().ok();
  }, 20s * kTimeMultiplier, "Write rows after faulted follower resurrection."));

  auto non_faulted_follower = cluster_->tablet_server(non_faulted_follower_idx);
  non_faulted_follower->Shutdown();
  CHECK_OK(cluster_->WaitForTSToCrash(non_faulted_follower));

  ASSERT_OK(WaitFor([&]() -> Result<bool> {
    return WriteRows().ok();
  }, 20s * kTimeMultiplier, "Write rows after requiring bootstraped node consensus."));

  CHECK_OK(non_faulted_follower->Restart());
}

class TabletSplitRemoteBootstrapEnabledTest : public TabletSplitExternalMiniClusterITest {
 protected:
  void SetFlags() override {
    TabletSplitExternalMiniClusterITest::SetFlags();
    mini_cluster_opt_.extra_tserver_flags.push_back(
        "--TEST_disable_post_split_tablet_rbs_check=true");
  }
};

TEST_F(TabletSplitRemoteBootstrapEnabledTest, TestSplitAfterFailedRbsCreatesDirectories) {
  const auto kApplyTabletSplitDelay = 15s * kTimeMultiplier;

  const auto get_tablet_meta_dirs =
      [this](ExternalTabletServer* node) -> Result<std::vector<string>> {
    auto tablet_meta_dirs = VERIFY_RESULT(env_->GetChildren(
        JoinPathSegments(node->GetRootDir(), "yb-data", "tserver", "tablet-meta")));
    std::sort(tablet_meta_dirs.begin(), tablet_meta_dirs.end());
    return tablet_meta_dirs;
  };

  const auto wait_for_same_tablet_metas =
      [&get_tablet_meta_dirs]
      (ExternalTabletServer* node_1, ExternalTabletServer* node_2) -> Status {
    return WaitFor([node_1, node_2, &get_tablet_meta_dirs]() -> Result<bool> {
      auto node_1_metas = VERIFY_RESULT(get_tablet_meta_dirs(node_1));
      auto node_2_metas = VERIFY_RESULT(get_tablet_meta_dirs(node_2));
      if (node_1_metas.size() != node_2_metas.size()) {
        return false;
      }
      for (size_t i = 0; i < node_2_metas.size(); ++i) {
        if (node_1_metas.at(i) != node_2_metas.at(i)) {
          return false;
        }
      }
      return true;
    }, 5s * kTimeMultiplier, "Waiting for nodes to have same set of tablet metas.");
  };

  CreateSingleTablet();
  ASSERT_OK(WriteRowsAndFlush());
  const auto tablet_id = CHECK_RESULT(GetOnlyTestTabletId());

  const auto leader_idx = CHECK_RESULT(cluster_->GetTabletLeaderIndex(tablet_id));
  const auto leader = cluster_->tablet_server(leader_idx);
  const auto healthy_follower_idx = (leader_idx + 1) % 3;
  const auto healthy_follower = cluster_->tablet_server(healthy_follower_idx);
  const auto faulted_follower_idx = (leader_idx + 2) % 3;
  const auto faulted_follower = cluster_->tablet_server(faulted_follower_idx);

  // Make one node fail on tablet split, and ensure the leader does not remote bootstrap to it at
  // first.
  ASSERT_OK(cluster_->SetFlag(
      faulted_follower, "TEST_crash_before_apply_tablet_split_op", "true"));
  ASSERT_OK(cluster_->SetFlag(leader, "TEST_enable_remote_bootstrap", "false"));
  ASSERT_OK(SplitTablet(tablet_id));
  ASSERT_OK(cluster_->WaitForTSToCrash(faulted_follower));

  // Once split is applied on two nodes, re-enable remote bootstrap before restarting the faulted
  // node. Ensure that remote bootstrap requests can be retried until the faulted node is up.
  ASSERT_OK(WaitForTablets(3, leader_idx));
  ASSERT_OK(WaitForTablets(3, healthy_follower_idx));
  ASSERT_OK(wait_for_same_tablet_metas(leader, healthy_follower));
  ASSERT_OK(cluster_->SetFlag(leader, "unresponsive_ts_rpc_retry_limit", "100"));
  ASSERT_OK(cluster_->SetFlag(leader, "TEST_enable_remote_bootstrap", "true"));

  // Restart the faulted node. Ensure it waits a long time in ApplyTabletSplit to allow a remote
  // bootstrap request to come in and create a directory for the subtablets before returning error.
  ASSERT_OK(faulted_follower->Restart(ExternalMiniClusterOptions::kDefaultStartCqlProxy, {
    std::make_pair(
        "TEST_apply_tablet_split_inject_delay_ms",
        Format("$0", MonoDelta(kApplyTabletSplitDelay).ToMilliseconds())),
    std::make_pair("TEST_simulate_already_present_in_remote_bootstrap", "true"),
    std::make_pair("TEST_crash_before_apply_tablet_split_op", "false"),
  }));

  // Once the faulted node has the same tablet metas written to disk as the leader, disable remote
  // bootstrap to avoid registering transition status for the subtablets.
  ASSERT_OK(wait_for_same_tablet_metas(leader, faulted_follower));
  ASSERT_OK(cluster_->SetFlagOnTServers("TEST_enable_remote_bootstrap", "false"));

  // Sleep some time to allow the ApplyTabletSplit pause to run out, and then ensure we have healthy
  // subtablets at the formerly faulted follower node.
  std::this_thread::sleep_for(kApplyTabletSplitDelay);
  EXPECT_OK(WaitFor([&]() -> Result<bool> {
    return WriteRows().ok();
  }, 10s * kTimeMultiplier, "Write rows after faulted follower resurrection."));
  cluster_->Shutdown();
}

class TabletSplitRbsTest : public TabletSplitExternalMiniClusterITest {
 protected:
  void SetFlags() override {
    TabletSplitExternalMiniClusterITest::SetFlags();

    // Disable leader moves.
    mini_cluster_opt_.extra_master_flags.push_back("--load_balancer_max_concurrent_moves=0");
  }
};

// TODO(tsplit): should be RemoteBootstrapsFromNodeWithNotAppliedSplitOp, but not renaming now to
// have common test results history.
TEST_F_EX(
    TabletSplitExternalMiniClusterITest, RemoteBootstrapsFromNodeWithUncommittedSplitOp,
    TabletSplitRbsTest) {
  // If a new tablet is created and split with one node completely uninvolved, then when that node
  // rejoins it will have to do a remote bootstrap.

  const auto kWaitForTabletsRunningTimeout = 20s * kTimeMultiplier;
  const auto server_to_bootstrap_idx = 0;

  auto ts_map = ASSERT_RESULT(itest::CreateTabletServerMap(
      cluster_->GetLeaderMasterProxy<master::MasterClusterProxy>(), &cluster_->proxy_cache()));

  CreateSingleTablet();
  const auto source_tablet_id = CHECK_RESULT(GetOnlyTestTabletId());
  LOG(INFO) << "Source tablet ID: " << source_tablet_id;

  // Delete tablet on server_to_bootstrap and shutdown server, so it will be RBSed after SPLIT_OP
  // is Raft-committed on the leader.
  auto* const server_to_bootstrap = cluster_->tablet_server(server_to_bootstrap_idx);
  auto* ts_details_to_bootstrap = ts_map[server_to_bootstrap->uuid()].get();
  ASSERT_OK(
      itest::WaitUntilTabletRunning(ts_details_to_bootstrap, source_tablet_id, kRpcTimeout));
  // We might need to retry attempt to delete tablet if tablet state transition is not yet
  // completed even after it is running.
  ASSERT_OK(WaitFor(
      [&source_tablet_id, ts_details_to_bootstrap]() -> Result<bool> {
        const auto s = itest::DeleteTablet(
            ts_details_to_bootstrap, source_tablet_id, tablet::TABLET_DATA_TOMBSTONED, boost::none,
            kRpcTimeout);
        if (s.ok()) {
          return true;
        }
        if (s.IsAlreadyPresent()) {
          return false;
        }
        return s;
      },
      10s * kTimeMultiplier, Format("Delete parent tablet on $0", server_to_bootstrap->uuid())));
  server_to_bootstrap->Shutdown();
  ASSERT_OK(cluster_->WaitForTSToCrash(server_to_bootstrap));

  ASSERT_OK(WriteRows());

  for (size_t i = 0; i < cluster_->num_tablet_servers(); i++) {
    auto* ts = cluster_->tablet_server(i);
    if (i != server_to_bootstrap_idx) {
      ASSERT_OK(WaitForAllIntentsApplied(ts_map[ts->uuid()].get(), 15s * kTimeMultiplier));
      ASSERT_OK(cluster_->FlushTabletsOnSingleTServer(ts, {source_tablet_id}, false));
      // Prevent leader changes.
      ASSERT_OK(cluster_->SetFlag(ts, "enable_leader_failure_detection", "false"));
    }
  }

  const auto leader_idx = CHECK_RESULT(cluster_->GetTabletLeaderIndex(source_tablet_id));

  const auto other_follower_idx = 3 - leader_idx - server_to_bootstrap_idx;
  auto* const other_follower = cluster_->tablet_server(other_follower_idx);

  auto* const leader = cluster_->tablet_server(leader_idx);

  // We need to pause leader on UpdateMajorityReplicated for SPLIT_OP, not for previous OPs, so
  // wait for tablet to be quiet.
  OpId leader_last_op_id;
  auto* const leader_ts_details = ts_map[leader->uuid()].get();
  ASSERT_OK(WaitFor(
      [&source_tablet_id, &leader_last_op_id, leader_ts_details]() -> Result<bool> {
        for (auto op_id_type : {consensus::RECEIVED_OPID, consensus::COMMITTED_OPID}) {
          const auto op_id = VERIFY_RESULT(
              GetLastOpIdForReplica(source_tablet_id, leader_ts_details, op_id_type, kRpcTimeout));
          if (op_id > leader_last_op_id) {
            leader_last_op_id = op_id;
            return false;
          }
        }
        return true;
      },
      10s * kTimeMultiplier, "Wait for the parent tablet to be quiet"));

  // We want the leader to not apply the split operation for now, but commit it, so RBSed node
  // replays it.
  ASSERT_OK(cluster_->SetFlag(
      leader, "TEST_pause_update_majority_replicated", "true"));

  ASSERT_OK(SplitTablet(source_tablet_id));

  LOG(INFO) << "Restarting server to bootstrap: " << server_to_bootstrap->uuid();
  // Delaying RBS WAL downloading to start after leader marked SPLIT_OP as Raft-committed.
  // By the time RBS starts to download WAL, RocksDB is already downloaded, so flushed op ID will
  // be less than SPLIT_OP ID, so it will be replayed by server_to_bootstrap.
  LogWaiter log_waiter(
      server_to_bootstrap,
      source_tablet_id + ": Pausing due to flag TEST_pause_rbs_before_download_wal");
  ASSERT_OK(server_to_bootstrap->Restart(
      ExternalMiniClusterOptions::kDefaultStartCqlProxy,
      {std::make_pair("TEST_pause_rbs_before_download_wal", "true")}));
  ASSERT_OK(log_waiter.WaitFor(30s * kTimeMultiplier));

  // Resume leader to mark SPLIT_OP as Raft-committed.
  ASSERT_OK(cluster_->SetFlag(
      leader, "TEST_pause_update_majority_replicated", "false"));

  // Wait for SPLIT_OP to apply at leader.
  ASSERT_OK(WaitForTabletsExcept(2, leader_idx, source_tablet_id));

  // Resume RBS.
  ASSERT_OK(cluster_->SetFlag(
      server_to_bootstrap, "TEST_pause_rbs_before_download_wal", "false"));

  // Wait until RBS replays SPLIT_OP.
  ASSERT_OK(WaitForTabletsExcept(2, server_to_bootstrap_idx, source_tablet_id));
  ASSERT_OK(cluster_->WaitForTabletsRunning(server_to_bootstrap, kWaitForTabletsRunningTimeout));

  ASSERT_OK(cluster_->WaitForTabletsRunning(leader, kWaitForTabletsRunningTimeout));

  ASSERT_OK(WaitForTabletsExcept(2, other_follower_idx, source_tablet_id));
  ASSERT_OK(cluster_->WaitForTabletsRunning(other_follower, kWaitForTabletsRunningTimeout));

  ASSERT_OK(cluster_->SetFlagOnTServers("enable_leader_failure_detection", "true"));

  ASSERT_OK(WaitFor([&]() -> Result<bool> {
    return WriteRows().ok();
  }, 20s * kTimeMultiplier, "Write rows after split."));

  other_follower->Shutdown();
  ASSERT_OK(cluster_->WaitForTSToCrash(other_follower));

  ASSERT_OK(WaitFor([&]() -> Result<bool> {
    return WriteRows().ok();
  }, 20s * kTimeMultiplier, "Write rows after requiring bootstraped node consensus."));

  ASSERT_OK(other_follower->Restart());
}

class TabletSplitReplaceNodeITest : public TabletSplitExternalMiniClusterITest {
 protected:
  void SetFlags() override {
    TabletSplitExternalMiniClusterITest::SetFlags();

    for (const auto& tserver_flag : std::initializer_list<std::string>{
        // We want to test behavior of the source tablet, so setting up to skip deleting it.
        "--TEST_skip_deleting_split_tablets=true",
        // Reduce follower_unavailable_considered_failed_sec, so offline tserver is evicted
        // from Raft group faster.
        Format("--follower_unavailable_considered_failed_sec=$0", 5 * kTimeMultiplier)
      }) {
      mini_cluster_opt_.extra_tserver_flags.push_back(tserver_flag);
    }

    for (const auto& master_flag : {
        // Should be less than follower_unavailable_considered_failed_sec, so load balancer
        // doesn't go into infinite loop trying to add failed follower back.
        "--tserver_unresponsive_timeout_ms=3000",
        // To speed up load balancing:
        // - Allow more concurrent adds/removes, so we deal with transaction status tablets
        // faster.
        "--load_balancer_max_concurrent_adds=10", "--load_balancer_max_concurrent_removals=10",
        // - Allow more over replicated tablets, so temporary child tablets over replication
        // doesn't block parent tablet move.
        "--load_balancer_max_over_replicated_tablets=5",
        // To speed up test in case of intermittent failures due to leader re-elections.
        "--retrying_ts_rpc_max_delay_ms=1000",
      }) {
      mini_cluster_opt_.extra_master_flags.push_back(master_flag);
    }
  }
};

TEST_F_EX(
    TabletSplitExternalMiniClusterITest, ReplaceNodeForParentTablet, TabletSplitReplaceNodeITest) {
  constexpr auto kReplicationFactor = 3;
  constexpr auto kNumRows = kDefaultNumRows;

  CreateSingleTablet();
  ASSERT_OK(WriteRows(kNumRows));
  const auto source_tablet_id = ASSERT_RESULT(GetOnlyTestTabletId());
  LOG(INFO) << "Source tablet ID: " << source_tablet_id;

  constexpr auto offline_ts_idx = 0;
  auto* offline_ts = cluster_->tablet_server(offline_ts_idx);
  offline_ts->Shutdown();
  LOG(INFO) << "Shutdown completed for tserver: " << offline_ts->uuid();
  const auto offline_ts_id = offline_ts->uuid();

  for (size_t ts_idx = 0; ts_idx < cluster_->num_tablet_servers(); ++ts_idx) {
    auto* ts = cluster_->tablet_server(ts_idx);
    if (ts->IsProcessAlive()) {
      ASSERT_OK(cluster_->FlushTabletsOnSingleTServer(
          ts, {source_tablet_id}, /* is_compaction = */ false));
    }
  }
  ASSERT_OK(SplitTablet(source_tablet_id));
  ASSERT_OK(WaitForTablets(3));

  ASSERT_OK(cluster_->AddTabletServer());
  const auto new_ts_id = cluster_->tablet_server(3)->uuid();
  LOG(INFO) << "Started new tserver: " << new_ts_id;

  ASSERT_OK(cluster_->WaitForTabletServerCount(4, 20s));
  LOG(INFO) << "New tserver has been added: " << new_ts_id;

  const auto deadline = CoarseMonoClock::Now() + 30s * kTimeMultiplier;
  std::set<TabletServerId> source_tablet_replicas;
  auto s = LoggedWait(
      [this, &deadline, &source_tablet_id, &offline_ts_id, &new_ts_id, &source_tablet_replicas] {
        const MonoDelta remaining_timeout = deadline - CoarseMonoClock::Now();
        if (remaining_timeout.IsNegative()) {
          return false;
        }
        master::TabletLocationsPB resp;
        const auto s = itest::GetTabletLocations(
            cluster_.get(), source_tablet_id, remaining_timeout, &resp);
        if (!s.ok()) {
          return false;
        }
        source_tablet_replicas.clear();
        for (auto& replica : resp.replicas()) {
          source_tablet_replicas.insert(replica.ts_info().permanent_uuid());
        }
        if (source_tablet_replicas.size() != kReplicationFactor) {
          return false;
        }
        if (source_tablet_replicas.count(offline_ts_id) > 0) {
          // We don't expect source tablet to have replica on offline tserver.
          return false;
        }
        return source_tablet_replicas.count(new_ts_id) > 0;
      },
      deadline,
      Format("Waiting for source tablet $0 to be moved to ts-4 ($1)", source_tablet_id, new_ts_id));

  ASSERT_TRUE(s.ok()) << s << ". Source tablet replicas: " << AsString(source_tablet_replicas);

  LOG(INFO) << "Waiting for parent + child tablets on all online tservers...";

  // Wait for the split to be completed on all online tservers.
  for (size_t ts_idx = 0; ts_idx < cluster_->num_tablet_servers(); ++ts_idx) {
    if (cluster_->tablet_server(ts_idx)->IsProcessAlive()) {
      ASSERT_OK(WaitForTablets(3, ts_idx));
    }
  }

  // Restarting offline_ts, because ClusterVerifier requires all tservers to be online.
  ASSERT_OK(offline_ts->Start());
  // Wait for tablet moves to be completed and have 1 parent + 2 child tablets with
  // kReplicationFactor replicas each.
  const auto expected_tablet_replicas = (1 + 2) * kReplicationFactor;
  std::vector<std::set<TabletId>> test_tablets_by_tserver;
  size_t num_test_tablet_replicas = 0;
  auto status = LoggedWaitFor(
      [&]() -> Result<bool> {
        test_tablets_by_tserver.clear();
        num_test_tablet_replicas = 0;
        for (size_t ts_idx = 0; ts_idx < cluster_->num_tablet_servers(); ++ts_idx) {
          auto tablets = VERIFY_RESULT(GetTestTableTabletIds(ts_idx));
          num_test_tablet_replicas += tablets.size();
          test_tablets_by_tserver.emplace_back(std::move(tablets));
        }
        return num_test_tablet_replicas == expected_tablet_replicas;
      },
      30s * kTimeMultiplier,
      Format(
          "Waiting for all tablet servers to have $0 total tablet replicas",
          expected_tablet_replicas));
  LOG(INFO) << "Test tablets replicas (" << num_test_tablet_replicas
            << "): " << AsString(test_tablets_by_tserver);
  ASSERT_OK(status);
}

// Make sure RBS of split parent and replay of SPLIT_OP don't delete child tablets data.
TEST_F(TabletSplitITest, ParentRemoteBootstrapAfterWritesToChildren) {
  constexpr auto kNumRows = kDefaultNumRows;

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_skip_deleting_split_tablets) = true;
  // Disable leader moves.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_load_balancer_max_concurrent_moves) = 0;

  auto source_tablet_id = ASSERT_RESULT(CreateSingleTabletAndSplit(kNumRows));

  LOG(INFO) << "Source tablet ID: " << source_tablet_id;

  // Write more rows into child tablets.
  ASSERT_OK(WriteRows(kNumRows, /* start_key = */ kNumRows + 1));
  ASSERT_OK(CheckRowsCount(kNumRows * 2));

  // Trigger and wait for RBS to complete on the followers of split parent tablet.
  for (auto& ts : cluster_->mini_tablet_servers()) {
    const auto* tablet_manager = ts->server()->tablet_manager();
    const auto peer = ASSERT_RESULT(tablet_manager->LookupTablet(source_tablet_id));
    if (peer->consensus()->GetLeaderStatus() != consensus::LeaderStatus::NOT_LEADER) {
      continue;
    }
    LOG(INFO) << Format(
        "Triggering RBS for tablet $0 peer $1", peer->tablet_id(), peer->permanent_uuid());
    peer->SetFailed(STATUS(InternalError, "Set to failed to initiate RBS for test"));

    ASSERT_OK(LoggedWaitFor(
        [&] { return peer->state() == tablet::RaftGroupStatePB::SHUTDOWN; }, 30s * kTimeMultiplier,
        Format(
            "Waiting for tablet $0 peer $1 shutdown", peer->tablet_id(), peer->permanent_uuid())));

    ASSERT_OK(LoggedWaitFor(
        [&] {
          const auto result = tablet_manager->LookupTablet(source_tablet_id);
          if (!result.ok()) {
            return false;
          }
          if ((*result)->state() != tablet::RaftGroupStatePB::RUNNING) {
            return false;
          }
          const auto shared_tablet = (*result)->shared_tablet();
          if (!shared_tablet) {
            return false;
          }
          return shared_tablet->metadata()->tablet_data_state() ==
                 tablet::TabletDataState::TABLET_DATA_SPLIT_COMPLETED;
        },
        30s * kTimeMultiplier,
        Format(
            "Waiting for tablet $0 peer $1 remote bootstrap completed", peer->tablet_id(),
            peer->permanent_uuid())));
  }

  // Make sure all child tablet replicas are not affected by split parent RBS that replayed
  // SPLIT_OP (should be idempotent).
  ASSERT_OK(CheckPostSplitTabletReplicasData(kNumRows * 2));

  // Write more data and make sure it is there.
  ASSERT_OK(WriteRows(kNumRows, /* start_key = */ kNumRows * 2 + 1));
  ASSERT_OK(CheckRowsCount(kNumRows * 3));
}

class TabletSplitSystemRecordsITest :
    public TabletSplitSingleServerITest,
    public testing::WithParamInterface<Partitioning> {
 protected:
  void SetUp() override {
    TabletSplitSingleServerITest::SetUp();
    SetNumTablets(1);

    // Disable automatic tablet splitting.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_automatic_tablet_splitting) = false;
    // Disable automatic compactions.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_rocksdb_level0_file_num_compaction_trigger) =
        std::numeric_limits<int32>::max();
  }

  Status VerifySplitKeyError(yb::tablet::TabletPtr tablet) {
    SCHECK_NOTNULL(tablet.get());

    // Get middle key directly from in-memory tablet and check correct message has been returned.
    auto middle_key = tablet->GetEncodedMiddleSplitKey();
    SCHECK_EQ(middle_key.ok(), false, IllegalState, "Valid split key is not expected.");
    const auto key_message = middle_key.status().message();
    const auto is_expected_key_message =
        strnstr(key_message.cdata(), "got internal record", key_message.size()) != nullptr;
    SCHECK_EQ(is_expected_key_message, true, IllegalState,
              Format("Unexepected error message: $0", middle_key.status().ToString()));
    LOG(INFO) << "System record middle key result: " << middle_key.status().ToString();

    // Test that tablet GetSplitKey RPC returns the same message.
    auto response = VERIFY_RESULT(GetSplitKey(tablet->tablet_id()));
    SCHECK_EQ(response.has_error(), true, IllegalState,
              "GetSplitKey RPC unexpectedly succeeded.");
    const Status op_status = StatusFromPB(response.error().status());
    SCHECK_EQ(op_status.ToString(), middle_key.status().ToString(), IllegalState,
              Format("Unexpected error message: $0", op_status.ToString()));
    LOG(INFO) << "System record get split key result: " << op_status.ToString();

    return Status::OK();
  }
};

TEST_P(TabletSplitSystemRecordsITest, GetSplitKey) {
  // The idea of the test is to generate data with kNumTxns ApplyTransactionState records following
  // by 2 * kNumRows user records (very small number). This can be achieved by the following steps:
  //   1) pause ApplyIntentsTasks to keep ApplyTransactionState records
  //   2) run kNumTxns transaction with the same keys
  //   3) run manual compaction to collapse all user records to the latest transaciton content
  //   4) at this step there are kNumTxns internal records followed by 2 * kNumRows user records

  // Selecting a small period for history cutoff to force compacting records with the same keys.
  constexpr auto kHistoryRetentionSec = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_timestamp_history_retention_interval_sec)
      = kHistoryRetentionSec;

  // This flag shoudn't be less than 2, setting it to 1 may cause RemoveIntentsTask to become stuck.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_txn_max_apply_batch_records) = 2;

  // Force intents to not apply in ApplyIntentsTask.
  // Note: transactions are still partly applied via tablet::UpdateTxnOperation::DoReplicated(),
  // but ApplyTransactionState will not be removed from SST during compaction.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_and_skip_apply_intents_task_loop_ms) = 1;

  // This combination of txns number and user rows is enough to generate suitable number of
  // internal records to make middle key point to one of the ApplyTransactionState records.
  // Also this will prevent spawning logs with long operation timeout warnings.
  constexpr auto kNumTxns = 50;
  constexpr auto kNumRows = 3;

  Schema schema;
  BuildSchema(GetParam(), &schema);
  ASSERT_OK(CreateTable(schema));
  auto peer = ASSERT_RESULT(GetSingleTabletLeaderPeer());
  auto tablet = peer->shared_tablet();
  auto partition_schema = tablet->metadata()->partition_schema();
  LOG(INFO) << "System records partitioning: "
            << "hash = "  << partition_schema->IsHashPartitioning() << ", "
            << "range = " << partition_schema->IsRangePartitioning();

  for (auto i = 0; i < kNumTxns; ++i) {
    ASSERT_OK(WriteRows(&table_, kNumRows, 1, kNumRows * i));
  }

  // Sleep for kHistoryRetentionSec + delta to make sure all the records with the same keys
  // will be compacted. Taking into account FLAGS_TEST_pause_and_skip_apply_intents_task_loop_ms
  // is set, this leads to a warning for too long ScopedRWOperation (see ProcessApply routine).
  std::this_thread::sleep_for(std::chrono::seconds(kHistoryRetentionSec + 1));

  // Force manual compaction
  ASSERT_OK(FlushTestTable());
  ASSERT_OK(tablet->ForceFullRocksDBCompact());
  ASSERT_OK(LoggedWaitFor(
      [peer]() -> Result<bool> {
        return peer->tablet_metadata()->has_been_fully_compacted();
      },
      15s * kTimeMultiplier,
      "Wait for tablet manual compaction to be completed for peer: " + peer->tablet_id()));

  ASSERT_OK(VerifySplitKeyError(tablet));
}

TEST_F_EX(TabletSplitITest, SplitOpApplyAfterLeaderChange, TabletSplitExternalMiniClusterITest) {
  constexpr auto kNumRows = kDefaultNumRows;

  ASSERT_OK(cluster_->SetFlagOnMasters("enable_load_balancing", "false"));

  auto ts_map = ASSERT_RESULT(itest::CreateTabletServerMap(
      cluster_->GetLeaderMasterProxy<master::MasterClusterProxy>(), &cluster_->proxy_cache()));

  CreateSingleTablet();
  ASSERT_OK(WriteRowsAndFlush(kNumRows));
  const auto source_tablet_id = ASSERT_RESULT(GetOnlyTestTabletId());
  LOG(INFO) << "Source tablet ID: " << source_tablet_id;

  // Select tserver to pause making sure it is not leader for source tablet (just for master to not
  // wait for re-election and GetSplitKey timeout before proceeding with the split).
  const auto paused_ts_idx =
      (ASSERT_RESULT(cluster_->GetTabletLeaderIndex(source_tablet_id)) + 1) %
      cluster_->num_tablet_servers();
  auto* paused_ts = cluster_->tablet_server(paused_ts_idx);

  // We want to avoid leader changes in child tablets for this test.
  // Disabling leader failure detection for paused_ts now before pausing it.
  // And will disable it for other tservers after child tablets are created and their leaders are
  // elected.
  ASSERT_OK(cluster_->SetFlag(paused_ts, "enable_leader_failure_detection", "false"));

  const auto paused_ts_id = paused_ts->uuid();
  LOG(INFO) << Format("Pausing ts-$0: $1", paused_ts_idx + 1, paused_ts_id);
  ASSERT_OK(paused_ts->Pause());

  ASSERT_OK(SplitTablet(source_tablet_id));
  // Wait for split to compete on all non-paused replicas.
  for (size_t ts_idx = 0; ts_idx < cluster_->num_tablet_servers(); ++ts_idx) {
    if (ts_idx != paused_ts_idx) {
      ASSERT_OK(WaitForTablets(3, ts_idx));
    }
  }

  // Make sure all test tablets have leaders.
  const auto tablet_ids = ASSERT_RESULT(GetTestTableTabletIds());
  for (auto& tablet_id : tablet_ids) {
    itest::TServerDetails* leader;
    ASSERT_OK(FindTabletLeader(ts_map, tablet_id, kRpcTimeout, &leader));
  }

  // We want to avoid leader changes in child tablets for this test.
  for (size_t ts_idx = 0; ts_idx < cluster_->num_tablet_servers(); ++ts_idx) {
    if (ts_idx != paused_ts_idx) {
      ASSERT_OK(cluster_->SetFlag(
          cluster_->tablet_server(ts_idx), "enable_leader_failure_detection", "false"));
    }
  }

  struct TabletLeaderInfo {
    PeerId peer_id;
    int64_t term;
  };

  auto get_leader_info = [&](const TabletId& tablet_id) -> Result<TabletLeaderInfo> {
    const auto leader_idx = VERIFY_RESULT(cluster_->GetTabletLeaderIndex(tablet_id));
    const auto leader_peer_id = cluster_->tablet_server(leader_idx)->uuid();
    LOG(INFO) << "Tablet " << tablet_id << " leader: " << leader_peer_id << " idx: " << leader_idx;
    consensus::ConsensusStatePB cstate;
    RETURN_NOT_OK(itest::GetConsensusState(
        ts_map[leader_peer_id].get(), tablet_id, consensus::CONSENSUS_CONFIG_ACTIVE, kRpcTimeout,
        &cstate));
    LOG(INFO) << "Tablet " << tablet_id << " cstate: " << cstate.DebugString();
    return TabletLeaderInfo {
        .peer_id = leader_peer_id,
        .term = cstate.current_term(),
    };
  };

  std::unordered_map<TabletId, TabletLeaderInfo> leader_info;
  int64_t max_leader_term = 0;
  for (auto& tablet_id : tablet_ids) {
    auto info = ASSERT_RESULT(get_leader_info(tablet_id));
    max_leader_term = std::max(max_leader_term, info.term);
    leader_info[tablet_id] = info;
  }
  LOG(INFO) << "Max leader term: " << max_leader_term;

  // Make source tablet to advance term to larger than max_leader_term, so resumed replica will
  // apply split in later term than child leader replicas have.
  while (leader_info[source_tablet_id].term <= max_leader_term) {
    ASSERT_OK(itest::LeaderStepDown(
        ts_map[leader_info[source_tablet_id].peer_id].get(), source_tablet_id, nullptr,
        kRpcTimeout));

    const auto source_leader_term = leader_info[source_tablet_id].term;
    ASSERT_OK(LoggedWaitFor(
        [&]() -> Result<bool> {
          auto result = get_leader_info(source_tablet_id);
          if (!result.ok()) {
            return false;
          }
          leader_info[source_tablet_id] = *result;
          return result->term > source_leader_term;
        },
        30s * kTimeMultiplier,
        Format("Waiting for term >$0 on source tablet ...", source_leader_term)));
  }

  ASSERT_OK(paused_ts->Resume());
  // To avoid term changes.
  ASSERT_OK(cluster_->SetFlag(paused_ts, "enable_leader_failure_detection", "false"));

  // Wait for all replicas to have only 2 child tablets (parent tablet will be deleted).
  for (size_t ts_idx = 0; ts_idx < cluster_->num_tablet_servers(); ++ts_idx) {
    ASSERT_OK(WaitForTablets(2, ts_idx));
  }

  ASSERT_OK(cluster_->WaitForTabletsRunning(paused_ts, 20s * kTimeMultiplier));

  // Make sure resumed replicas are not ahead of leader.
  const auto child_tablet_ids = ASSERT_RESULT(GetTestTableTabletIds());
  for (auto& child_tablet_id : child_tablet_ids) {
    consensus::ConsensusStatePB cstate;
    ASSERT_OK(itest::GetConsensusState(
        ts_map[paused_ts_id].get(), child_tablet_id, consensus::CONSENSUS_CONFIG_ACTIVE,
        kRpcTimeout, &cstate));
    LOG(INFO) << "Child tablet " << child_tablet_id
              << " resumed replica cstate: " << cstate.DebugString();

    ASSERT_LE(cstate.current_term(), leader_info[child_tablet_id].term);
  }
}

namespace {

PB_ENUM_FORMATTERS(IsolationLevel);

template <typename T>
std::string TestParamToString(const testing::TestParamInfo<T>& param_info) {
  return ToString(param_info.param);
}

} // namespace

INSTANTIATE_TEST_CASE_P(
    TabletSplitITest,
    TabletSplitITestWithIsolationLevel,
    ::testing::ValuesIn(GetAllPbEnumValues<IsolationLevel>()),
    TestParamToString<IsolationLevel>);

INSTANTIATE_TEST_CASE_P(
    TabletSplitSingleServerITest,
    TabletSplitSystemRecordsITest,
    ::testing::ValuesIn(kPartitioningArray),
    TestParamToString<Partitioning>);

}  // namespace yb
