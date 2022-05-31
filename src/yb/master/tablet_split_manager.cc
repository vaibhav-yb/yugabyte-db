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

#include "yb/common/constants.h"

#include "yb/gutil/casts.h"
#include "yb/gutil/map-util.h"

#include "yb/common/partition.h"
#include "yb/common/schema.h"

#include "yb/master/async_rpc_tasks.h"
#include "yb/master/catalog_entity_info.h"
#include "yb/master/master_error.h"
#include "yb/master/master_fwd.h"
#include "yb/master/tablet_split_candidate_filter.h"
#include "yb/master/tablet_split_driver.h"
#include "yb/master/tablet_split_manager.h"
#include "yb/master/ts_descriptor.h"
#include "yb/master/xcluster_split_driver.h"

#include "yb/server/monitored_task.h"

#include "yb/util/flag_tags.h"
#include "yb/util/monotime.h"
#include "yb/util/result.h"
#include "yb/util/unique_lock.h"

DEFINE_int32(process_split_tablet_candidates_interval_msec, 0,
             "The minimum time between automatic splitting attempts. The actual splitting time "
             "between runs is also affected by catalog_manager_bg_task_wait_ms, which controls how "
             "long the bg tasks thread sleeps at the end of each loop. The top-level automatic "
             "tablet splitting method, which checks for the time since last run, is run once per "
             "loop.");
DEFINE_int32(max_queued_split_candidates, 0,
             "DEPRECATED. The max number of pending tablet split candidates we will hold onto. We "
             "potentially iterate through every candidate in the queue for each tablet we process "
             "in a tablet report so this size should be kept relatively small to avoid any "
             "issues.");

DECLARE_bool(enable_automatic_tablet_splitting);

DEFINE_uint64(outstanding_tablet_split_limit, 1,
              "Limit of the number of outstanding tablet splits. Limitation is disabled if this "
              "value is set to 0.");

DEFINE_uint64(outstanding_tablet_split_limit_per_tserver, 1,
              "Limit of the number of outstanding tablet splits per node. Limitation is disabled "
              "if this value is set to 0.");

DECLARE_bool(TEST_validate_all_tablet_candidates);

DEFINE_bool(enable_tablet_split_of_pitr_tables, true,
            "When set, it enables automatic tablet splitting of tables covered by "
            "Point In Time Restore schedules.");
TAG_FLAG(enable_tablet_split_of_pitr_tables, runtime);

DEFINE_bool(enable_tablet_split_of_xcluster_replicated_tables, false,
            "When set, it enables automatic tablet splitting for tables that are part of an "
            "xCluster replication setup");
TAG_FLAG(enable_tablet_split_of_xcluster_replicated_tables, runtime);
TAG_FLAG(enable_tablet_split_of_xcluster_replicated_tables, hidden);

DEFINE_uint64(tablet_split_limit_per_table, 256,
              "Limit of the number of tablets per table for tablet splitting. Limitation is "
              "disabled if this value is set to 0.");

DEFINE_uint64(prevent_split_for_ttl_tables_for_seconds, 86400,
              "Seconds between checks for whether to split a table with TTL. Checks are disabled "
              "if this value is set to 0.");

DEFINE_uint64(prevent_split_for_small_key_range_tablets_for_seconds, 300,
              "Seconds between checks for whether to split a tablet whose key range is too small "
              "to be split. Checks are disabled if this value is set to 0.");

DEFINE_bool(sort_automatic_tablet_splitting_candidates, true,
            "Whether we should sort candidates for new automatic tablet splits, so the largest "
            "candidates are picked first.");

namespace yb {
namespace master {

using strings::Substitute;
using namespace std::literals;

namespace {

template <typename IdType>
Status ValidateAgainstDisabledList(const IdType& id,
                                   std::unordered_map<IdType, CoarseTimePoint>* map) {
  const auto entry = map->find(id);
  if (entry == map->end()) {
    return Status::OK();
  }

  const auto ignored_until = entry->second;
  if (ignored_until <= CoarseMonoClock::Now()) {
    map->erase(entry);
    return Status::OK();
  }

  VLOG(1) << Format("Table/tablet is ignored for splitting until $0. id: $1",
                    ToString(ignored_until), id);
  return STATUS_FORMAT(
      IllegalState,
      "Table/tablet is ignored for splitting until $0. id: $1",
      ToString(ignored_until), id);
}

} // namespace


TabletSplitManager::TabletSplitManager(
    TabletSplitCandidateFilterIf* filter,
    TabletSplitDriverIf* driver,
    XClusterSplitDriverIf* xcluster_split_driver):
    filter_(filter),
    driver_(driver),
    xcluster_split_driver_(xcluster_split_driver),
    is_running_(false),
    splitting_disabled_until_(CoarseDuration::zero()),
    last_run_time_(CoarseDuration::zero()) {}

Status TabletSplitManager::ValidateTableAgainstDisabledList(const TableId& table_id) {
  UniqueLock<decltype(mutex_)> lock(mutex_);
  return ValidateAgainstDisabledList(table_id, &ignore_table_for_splitting_until_);
}

Status TabletSplitManager::ValidateTabletAgainstDisabledList(const TabletId& tablet_id) {
  UniqueLock<decltype(mutex_)> lock(mutex_);
  return ValidateAgainstDisabledList(tablet_id, &ignore_tablet_for_splitting_until_);
}

Status TabletSplitManager::ValidateIndexTablePartitioning(const TableInfo& table) {
  if (!table.is_index()) {
    return Status::OK();
  }

  auto table_locked = table.LockForRead();
  if (!table_locked->schema().has_table_properties() || !table_locked->pb.has_partition_schema()) {
    return Status::OK();
  }

  // Nothing to validate for hash partitioned tables
  if (PartitionSchema::IsHashPartitioning(table_locked->pb.partition_schema())) {
    return Status::OK();
  }

  // Check partition key version is valid for tablet splitting
  const auto& table_properties = table_locked->schema().table_properties();
  if (table_properties.has_partition_key_version() &&
      table_properties.partition_key_version() > 0) {
    return Status::OK();
  }

  return STATUS_FORMAT(NotSupported,
                       "Tablet splitting is not supported for the index table"
                       " \"$0\" with table_id \"$1\". Please, rebuild the index!",
                       table.name(), table.id());
}

Status TabletSplitManager::ValidateSplitCandidateTable(
    const TableInfo& table,
    const IgnoreDisabledList ignore_disabled_list) {
  if (PREDICT_FALSE(FLAGS_TEST_validate_all_tablet_candidates)) {
    return Status::OK();
  }
  if (table.is_deleted()) {
    VLOG(1) << Format("Table is deleted; ignoring for splitting. table_id: $0", table.id());
    return STATUS_FORMAT(
        NotSupported, "Table is deleted; ignoring for splitting. table_id: $0", table.id());
  }

  if (!ignore_disabled_list) {
    RETURN_NOT_OK(ValidateTableAgainstDisabledList(table.id()));
  }

  // Check if this table is covered by a PITR schedule.
  if (!FLAGS_enable_tablet_split_of_pitr_tables &&
      VERIFY_RESULT(filter_->IsTablePartOfSomeSnapshotSchedule(table))) {
    VLOG(1) << Format("Tablet splitting is not supported for tables that are a part of"
                      " some active PITR schedule, table_id: $0", table.id());
    return STATUS_FORMAT(
        NotSupported,
        "Tablet splitting is not supported for tables that are a part of"
        " some active PITR schedule, table_id: $0", table.id());
  }
  // Check if this table is part of a cdc stream.
  if (PREDICT_TRUE(!FLAGS_enable_tablet_split_of_xcluster_replicated_tables) &&
      filter_->IsCdcEnabled(table)) {
    VLOG(1) << Format("Tablet splitting is not supported for tables that are a part of"
                      " a CDC stream, table_id: $0", table.id());
    return STATUS_FORMAT(
        NotSupported,
        "Tablet splitting is not supported for tables that are a part of"
        " a CDC stream, tablet_id: $0", table.id());
  }
  if (table.GetTableType() == TableType::TRANSACTION_STATUS_TABLE_TYPE) {
    VLOG(1) << Format("Tablet splitting is not supported for transaction status tables, "
                      "table_id: $0", table.id());
    return STATUS_FORMAT(
        NotSupported,
        "Tablet splitting is not supported for transaction status tables, table_id: $0",
        table.id());
  }
  if (table.is_system()) {
    VLOG(1) << Format("Tablet splitting is not supported for system table: $0 with "
                      "table_id: $1", table.name(), table.id());
    return STATUS_FORMAT(
        NotSupported,
        "Tablet splitting is not supported for system table: $0 with table_id: $1",
        table.name(), table.id());
  }
  if (table.GetTableType() == REDIS_TABLE_TYPE) {
    VLOG(1) << Format("Tablet splitting is not supported for YEDIS tables, table_id: $0",
                      table.id());
    return STATUS_FORMAT(
        NotSupported,
        "Tablet splitting is not supported for YEDIS tables, table_id: $0", table.id());
  }
  if (FLAGS_tablet_split_limit_per_table != 0 &&
      table.NumPartitions() >= FLAGS_tablet_split_limit_per_table) {
    // TODO(tsplit): Avoid tablet server of scanning tablets for the tables that already
    //  reached the split limit of tablet #6220
    VLOG(1) << Format("Too many tablets for the table, table_id: $0, limit: $1",
                      table.id(), FLAGS_tablet_split_limit_per_table);
    return STATUS_EC_FORMAT(IllegalState, MasterError(MasterErrorPB::REACHED_SPLIT_LIMIT),
                            "Too many tablets for the table, table_id: $0, limit: $1",
                            table.id(), FLAGS_tablet_split_limit_per_table);
  }
  if (table.IsBackfilling()) {
    VLOG(1) << Format("Backfill operation in progress, table_id: $0", table.id());
    return STATUS_EC_FORMAT(IllegalState, MasterError(MasterErrorPB::SPLIT_OR_BACKFILL_IN_PROGRESS),
                            "Backfill operation in progress, table_id: $0", table.id());
  }

  return ValidateIndexTablePartitioning(table);
}

Status TabletSplitManager::ValidateSplitCandidateTablet(
    const TabletInfo& tablet,
    const IgnoreTtlValidation ignore_ttl_validation,
    const IgnoreDisabledList ignore_disabled_list) {
  if (PREDICT_FALSE(FLAGS_TEST_validate_all_tablet_candidates)) {
    return Status::OK();
  }

  Schema schema;
  RETURN_NOT_OK(tablet.table()->GetSchema(&schema));
  auto ts_desc = VERIFY_RESULT(tablet.GetLeader());
  if (!ignore_ttl_validation
      && schema.table_properties().HasDefaultTimeToLive()
      && ts_desc->get_disable_tablet_split_if_default_ttl()) {
    MarkTtlTableForSplitIgnore(tablet.table()->id());
    return STATUS_FORMAT(
        NotSupported, "Tablet splitting is not supported for tables with default time to live, "
        "tablet_id: $0", tablet.tablet_id());
  }

  if (tablet.colocated()) {
    return STATUS_FORMAT(
        NotSupported, "Tablet splitting is not supported for colocated tables, tablet_id: $0",
        tablet.tablet_id());
  }

  if (!ignore_disabled_list) {
    RETURN_NOT_OK(ValidateTabletAgainstDisabledList(tablet.id()));
  }

  {
    auto tablet_state = tablet.LockForRead()->pb.state();
    if (tablet_state != SysTabletsEntryPB::RUNNING) {
      return STATUS_EC_FORMAT(IllegalState, MasterError(MasterErrorPB::TABLET_NOT_RUNNING),
                              "Tablet is not in running state: $0",
                              tablet_state);
    }
  }
  return Status::OK();
}

void TabletSplitManager::MarkTtlTableForSplitIgnore(const TableId& table_id) {
  if (FLAGS_prevent_split_for_ttl_tables_for_seconds != 0) {
    const auto recheck_at = CoarseMonoClock::Now()
        + MonoDelta::FromSeconds(FLAGS_prevent_split_for_ttl_tables_for_seconds);
    UniqueLock<decltype(mutex_)> lock(mutex_);
    ignore_table_for_splitting_until_[table_id] = recheck_at;
  }
}

void TabletSplitManager::MarkSmallKeyRangeTabletForSplitIgnore(const TabletId& tablet_id) {
  if (FLAGS_prevent_split_for_small_key_range_tablets_for_seconds != 0) {
    const auto recheck_at = CoarseMonoClock::Now()
        + MonoDelta::FromSeconds(FLAGS_prevent_split_for_small_key_range_tablets_for_seconds);
    UniqueLock<decltype(mutex_)> lock(mutex_);
    ignore_tablet_for_splitting_until_[tablet_id] = recheck_at;
  }
}

bool AllReplicasHaveFinishedCompaction(const TabletReplicaMap& replicas) {
  for (const auto& replica : replicas) {
    if (replica.second.drive_info.may_have_orphaned_post_split_data) {
      return false;
    }
  }
  return true;
}

void TabletSplitManager::ScheduleSplits(const std::unordered_set<TabletId>& splits_to_schedule) {
  for (const auto& tablet_id : splits_to_schedule) {
    auto s = driver_->SplitTablet(tablet_id, ManualSplit::kFalse);
    if (!s.ok()) {
      WARN_NOT_OK(s, Format("Failed to start/restart split for tablet_id: $0.", tablet_id));
    } else {
      LOG(INFO) << Substitute("Scheduled split for tablet_id: $0.", tablet_id);
    }
  }
}

// A cache of the shared_ptrs to each tablet's replicas, to avoid having to repeatedly lock the
// tablet, and to ensure that we use a consistent set of replicas for each tablet within each
// iteration of the tablet split manager.
class TabletReplicaMapCache {
 public:
  const std::shared_ptr<const TabletReplicaMap> GetOrAdd(const TabletInfo& tablet) {
    auto it = replica_cache_.find(tablet.id());
    if (it != replica_cache_.end()) {
      return it->second;
    } else {
      const std::shared_ptr<const TabletReplicaMap> replicas = tablet.GetReplicaLocations();
      if (replicas->empty()) {
        LOG(WARNING) << "No replicas found for tablet. Id: " << tablet.id();
      }
      return replica_cache_[tablet.id()] = replicas;
    }
  }

 private:
  std::unordered_map<TabletId, std::shared_ptr<const TabletReplicaMap>> replica_cache_;
};

class OutstandingSplitState {
 public:
  OutstandingSplitState(
      const TabletInfoMap& tablet_info_map, TabletReplicaMapCache* replica_cache):
      tablet_info_map_{tablet_info_map}, replica_cache_{replica_cache} {}

  // Helper method to determine if more splits can be scheduled, or if we should exit early.
  bool CanSplitMoreGlobal() const {
    uint64_t outstanding_splits = splits_with_task_.size() +
                                  compacting_splits_.size() +
                                  splits_to_schedule_.size();
    return FLAGS_outstanding_tablet_split_limit == 0 ||
           outstanding_splits < FLAGS_outstanding_tablet_split_limit;
  }

  bool CanSplitMoreOnReplicas(const TabletReplicaMap& replicas) const {
    for (const auto& location : replicas) {
      auto it = ts_to_ongoing_splits_.find(location.first);
      if (it != ts_to_ongoing_splits_.end() &&
          it->second.size() >= FLAGS_outstanding_tablet_split_limit_per_tserver) {
        return false;
      }
    }
    return true;
  }

  bool HasSplitWithTask(const TabletId& split_tablet_id) const {
    return splits_with_task_.contains(split_tablet_id);
  }

  void AddSplitWithTask(const TabletId& split_tablet_id) {
    splits_with_task_.insert(split_tablet_id);
    auto it = tablet_info_map_.find(split_tablet_id);
    if (it == tablet_info_map_.end()) {
      LOG(WARNING) << "Split tablet with task not found in tablet info map. ID: "
                   << split_tablet_id;
      return;
    }
    TrackTserverSplits(split_tablet_id, *replica_cache_->GetOrAdd(*it->second));
    auto l = it->second->LockForRead();
    for (auto child_id : l->pb.split_tablet_ids()) {
      // Track split_tablet_id as an ongoing split on its children's tservers.
      TrackTserverSplits(split_tablet_id, child_id);
    }
  }

  void AddSplitToRestart(const TabletId& split_tablet_id, const TabletInfo& split_child) {
    if (!compacting_splits_.contains(split_tablet_id)) {
      bool inserted_split_to_schedule = splits_to_schedule_.insert(split_tablet_id).second;
      if (inserted_split_to_schedule) {
        // Track split_tablet_id as an ongoing split on its tservers. This is required since it is
        // possible that one of the split children is not running yet, but we still want to count
        // the split against the limits of the tservers on which the children will eventually
        // appear.
        TrackTserverSplits(split_tablet_id, split_tablet_id);
      }
    }
    TrackTserverSplits(split_tablet_id, *replica_cache_->GetOrAdd(split_child));
  }

  void AddCompactingSplit(
      const TabletId& split_tablet_id, const TabletInfo& split_child) {
    // It's possible that one child subtablet leads us to insert the parent tablet id into
    // splits_to_schedule, and another leads us to insert into compacting_splits. In this
    // case, it means one of the children is live, thus both children have been created and
    // the split RPC does not need to be scheduled.
    bool was_scheduled_for_split = splits_to_schedule_.erase(split_tablet_id);
    if (was_scheduled_for_split) {
      LOG(INFO) << Substitute("Found compacting split child ($0), so removing split parent "
                              "($1) from splits to schedule.", split_child.id(), split_tablet_id);
    }
    bool inserted_compacting_split = compacting_splits_.insert(split_tablet_id).second;
    if (inserted_compacting_split && !was_scheduled_for_split) {
      // Track split_tablet_id as an ongoing split on its tservers. This is required since it is
      // possible that one of the split children is not running yet, but we still want to count
      // the split against the limits of the tservers on which the children will eventually
      // appear.
      TrackTserverSplits(split_tablet_id, split_tablet_id);
    }
    TrackTserverSplits(split_tablet_id, *replica_cache_->GetOrAdd(split_child));
  }

  const std::unordered_set<TabletId>& GetSplitsToSchedule() const {
    return splits_to_schedule_;
  }

  void AddCandidate(TabletInfoPtr tablet, uint64_t leader_sst_size) {
    new_split_candidates_.emplace_back(SplitCandidate{tablet, leader_sst_size});
  }

  void ProcessCandidates() {
    // Add any new splits to the set of splits to schedule (while respecting the max number of
    // outstanding splits).
    if (CanSplitMoreGlobal()) {
      if (FLAGS_sort_automatic_tablet_splitting_candidates) {
        sort(new_split_candidates_.begin(), new_split_candidates_.end(), LargestTabletFirst);
      }
      for (const auto& candidate : new_split_candidates_) {
        if (!CanSplitMoreGlobal()) {
          break;
        }
        auto replicas = replica_cache_->GetOrAdd(*candidate.tablet);
        if (!CanSplitMoreOnReplicas(*replicas)) {
          continue;
        }
        splits_to_schedule_.insert(candidate.tablet->id());
        TrackTserverSplits(candidate.tablet->id(), *replicas);
      }
    }
  }

 private:
  const TabletInfoMap& tablet_info_map_;
  TabletReplicaMapCache* replica_cache_;
  // Splits which are tracked by an AsyncGetTabletSplitKey or AsyncSplitTablet task.
  std::unordered_set<TabletId> splits_with_task_;
  // Splits for which at least one child tablet is still undergoing compaction.
  std::unordered_set<TabletId> compacting_splits_;
  // Splits that need to be started / restarted.
  std::unordered_set<TabletId> splits_to_schedule_;

  struct SplitCandidate {
    TabletInfoPtr tablet;
    uint64_t leader_sst_size;
  };
  // New split candidates. The chosen candidates are eventually added to splits_to_schedule.
  vector<SplitCandidate> new_split_candidates_;

  std::unordered_map<TabletServerId, std::unordered_set<TabletId>> ts_to_ongoing_splits_;

  // Tracks split_tablet_id as an ongoing split on the replicas of replica_tablet_id, which is
  // either split_tablet_id itself or one of split_tablet_id's children.
  void TrackTserverSplits(const TabletId& split_tablet_id, const TabletId& replica_tablet_id) {
    auto it = tablet_info_map_.find(replica_tablet_id);
    if (it == tablet_info_map_.end()) {
      LOG(INFO) << "Tablet not found in tablet info map. ID: " << replica_tablet_id;
    }
    TrackTserverSplits(split_tablet_id, *replica_cache_->GetOrAdd(*it->second));
  }

  void TrackTserverSplits(const TabletId& tablet_id, const TabletReplicaMap& split_replicas) {
    for (const auto& location : split_replicas) {
      VLOG(1) << "Inserting location " << location.first << " for tablet " << tablet_id;
      ts_to_ongoing_splits_[location.first].insert(tablet_id);
    }
  }

  static inline bool LargestTabletFirst(const SplitCandidate& c1, const SplitCandidate& c2) {
    return c1.leader_sst_size > c2.leader_sst_size;
  }
};

void TabletSplitManager::DoSplitting(
    const TableInfoMap& table_info_map, const TabletInfoMap& tablet_info_map) {
  // TODO(asrivastava): We might want to loop over all running tables when determining outstanding
  // splits, to avoid missing outstanding splits for tables that have recently become invalid for
  // splitting. This is most critical for tables that frequently switch between being valid and
  // invalid for splitting (e.g. for tables with frequent PITR schedules).
  // https://github.com/yugabyte/yugabyte-db/issues/11459
  vector<TableInfoPtr> valid_tables;
  for (const auto& table : table_info_map) {
    if (ValidateSplitCandidateTable(*table.second).ok()) {
      valid_tables.push_back(table.second);
    }
  }

  TabletReplicaMapCache replica_cache;
  OutstandingSplitState state(tablet_info_map, &replica_cache);
  for (const auto& table : valid_tables) {
    for (const auto& task : table->GetTasks()) {
      // These tasks will retry automatically until they succeed or fail.
      if (task->type() == yb::server::MonitoredTask::ASYNC_GET_TABLET_SPLIT_KEY ||
          task->type() == yb::server::MonitoredTask::ASYNC_SPLIT_TABLET) {
        const TabletId tablet_id = static_cast<AsyncTabletLeaderTask*>(task.get())->tablet_id();
        auto tablet_info_it = tablet_info_map.find(tablet_id);
        if (tablet_info_it != tablet_info_map.end()) {
          const auto& tablet = tablet_info_it->second;
          state.AddSplitWithTask(tablet->id());
        } else {
          LOG(WARNING) << "Could not find tablet info for tablet with task. Tablet id: "
                      << tablet_id;
        }
        LOG(INFO) << Substitute("Found split with ongoing task. Task type: $0. "
                                "Split parent id: $1.", task->type_name(), tablet_id);
        if (!state.CanSplitMoreGlobal()) {
          return;
        }
      }
    }
  }

  for (const auto& table : valid_tables) {
    for (const auto& tablet : table->GetTablets()) {
      if (!state.CanSplitMoreGlobal()) {
        break;
      }
      if (state.HasSplitWithTask(tablet->id())) {
        continue;
      }

      auto tablet_lock = tablet->LockForRead();
      if (tablet_lock->pb.has_split_parent_tablet_id()) {
        const TabletId& parent_id = tablet_lock->pb.split_parent_tablet_id();
        if (state.HasSplitWithTask(parent_id)) {
          continue;
        }

        // If a split child is not running, schedule a restart for the split.
        if (!tablet_lock->is_running()) {
          LOG(INFO) << Substitute("Found split child ($0) that is not running. Adding parent ($1) "
                                  "to list of splits to reschedule.", tablet->id(), parent_id);
          state.AddSplitToRestart(parent_id, *tablet);
          continue;
        }

        // If this (running) tablet is the child of a split and is still compacting, track it as a
        // compacting split but do not schedule a restart (we assume that this split will eventually
        // complete for both tablets).
        if (!AllReplicasHaveFinishedCompaction(*replica_cache.GetOrAdd(*tablet))) {
          LOG(INFO) << Substitute("Found split child ($0) that is compacting. Adding parent ($1) "
                                  "to list of compacting splits.", tablet->id(), parent_id);
          state.AddCompactingSplit(parent_id, *tablet);
          continue;
        }
      }

      // Check if this tablet is a valid candidate for splitting, and if so, add it to the list of
      // split candidates.
      auto drive_info_opt = tablet->GetLeaderReplicaDriveInfo();
      if (!drive_info_opt.ok()) {
        continue;
      }
      if (ValidateSplitCandidateTablet(*tablet).ok() &&
          filter_->ShouldSplitValidCandidate(*tablet, drive_info_opt.get())) {
        const auto replicas = replica_cache.GetOrAdd(*tablet);
        if (AllReplicasHaveFinishedCompaction(*replicas) &&
            state.CanSplitMoreOnReplicas(*replicas)) {
          state.AddCandidate(tablet, drive_info_opt.get().sst_files_size);
        }
      }
    }
    if (!state.CanSplitMoreGlobal()) {
      break;
    }
  }

  // Sort candidates if required and add as many desired candidates to the list of splits to
  // schedule as possible (while respecting the limits on ongoing splits).
  state.ProcessCandidates();
  // Schedule any new splits and any splits that need to be restarted.
  ScheduleSplits(state.GetSplitsToSchedule());
}

bool TabletSplitManager::HasOutstandingTabletSplits(const TableInfoMap& table_info_map) {
  vector<TableInfoPtr> valid_tables;
  for (const auto& table : table_info_map) {
    // Check all potential tables for outstanding splits (including temporarily disabled tables).
    if (ValidateSplitCandidateTable(*table.second, IgnoreDisabledList::kTrue).ok()) {
      valid_tables.push_back(table.second);
    }
  }

  for (const auto& table : valid_tables) {
    for (const auto& task : table->GetTasks()) {
      if (task->type() == yb::server::MonitoredTask::ASYNC_GET_TABLET_SPLIT_KEY ||
          task->type() == yb::server::MonitoredTask::ASYNC_SPLIT_TABLET) {
        return true;
      }
    }
  }

  for (const auto& table : valid_tables) {
    for (const auto& tablet : table->GetTablets()) {
      auto tablet_lock = tablet->LockForRead();
      if (tablet_lock->pb.has_split_parent_tablet_id() && !tablet_lock->is_running()) {
        return true;
      }
    }
  }
  return false;
}

bool TabletSplitManager::IsRunning() {
  return is_running_;
}

bool TabletSplitManager::IsTabletSplittingComplete(const TableInfoMap& table_info_map) {
  return !HasOutstandingTabletSplits(table_info_map) && !is_running_;
}

void TabletSplitManager::DisableSplittingFor(const MonoDelta& disable_duration) {
  LOG(INFO) << Substitute("Disabling tablet splitting for $0 milliseconds.",
                          disable_duration.ToMilliseconds());
  splitting_disabled_until_ = CoarseMonoClock::Now() + disable_duration;
}

void TabletSplitManager::MaybeDoSplitting(
    const TableInfoMap& table_info_map, const TabletInfoMap& tablet_info_map) {
  if (!FLAGS_enable_automatic_tablet_splitting) {
    return;
  }

  auto time_since_last_run = CoarseMonoClock::Now() - last_run_time_;
  if (time_since_last_run < (FLAGS_process_split_tablet_candidates_interval_msec * 1ms)) {
    return;
  }

  // Setting and unsetting is_running_ could also be accomplished using a scoped object, but this is
  // simpler for now.
  is_running_ = true;
  if (CoarseMonoClock::Now() < splitting_disabled_until_) {
    is_running_ = false;
    return;
  }
  DoSplitting(table_info_map, tablet_info_map);
  is_running_ = false;
  last_run_time_ = CoarseMonoClock::Now();
}

Status TabletSplitManager::ProcessSplitTabletResult(
    const TableId& split_table_id,
    const SplitTabletIds& split_tablet_ids) {
  // Since this can get called multiple times from DoSplitTablet (if a tablet split is retried),
  // everything here needs to be idempotent.
  LOG(INFO) << "Processing split tablet result for table " << split_table_id
            << ", split tablet ids: " << split_tablet_ids.ToString();

  // Update the xCluster tablet mapping.
  Status s = xcluster_split_driver_->UpdateXClusterConsumerOnTabletSplit(
      split_table_id, split_tablet_ids);
  RETURN_NOT_OK_PREPEND(s, Format(
      "Encountered an error while updating the xCluster consumer tablet mapping. "
      "Table id: $0, Split Tablets: $1",
      split_table_id, split_tablet_ids.ToString()));
  // Also process tablet splits for producer side splits.
  s = xcluster_split_driver_->UpdateXClusterProducerOnTabletSplit(
      split_table_id, split_tablet_ids);
  RETURN_NOT_OK_PREPEND(s, Format(
      "Encountered an error while updating the xCluster producer tablet mapping. "
      "Table id: $0, Split Tablets: $1",
      split_table_id, split_tablet_ids.ToString()));

  return Status::OK();
}

}  // namespace master
}  // namespace yb
