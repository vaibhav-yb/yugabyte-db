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

#ifndef YB_MASTER_TABLET_SPLIT_MANAGER_H
#define YB_MASTER_TABLET_SPLIT_MANAGER_H

#include <unordered_set>

#include "yb/master/master_fwd.h"

namespace yb {
namespace master {

YB_STRONGLY_TYPED_BOOL(IgnoreTtlValidation);
YB_STRONGLY_TYPED_BOOL(IgnoreDisabledList);

class TabletSplitManager {
 public:
  TabletSplitManager(TabletSplitCandidateFilterIf* filter,
                     TabletSplitDriverIf* driver,
                     XClusterSplitDriverIf* xcluster_split_driver);

  // Temporarily disable splitting for the specified amount of time.
  void DisableSplittingFor(const MonoDelta& disable_duration);

  bool IsRunning();

  // Returns true if there are no outstanding tablet splits, and the automatic splitting thread is
  // not running (to ensure that no splits are started immediately after returning).
  bool IsTabletSplittingComplete(const TableInfoMap& table_info_map);

  // Perform one round of tablet splitting. This method is not thread-safe.
  void MaybeDoSplitting(const TableInfoMap& table_info_map, const TabletInfoMap& tablet_info_map);

  Status ProcessSplitTabletResult(
      const TableId& split_table_id, const SplitTabletIds& split_tablet_ids);

  // Validate whether a candidate table is eligible for a split.
  // Any temporarily disabled tablets are assumed ineligible by default.
  Status ValidateSplitCandidateTable(
      const TableInfo& table,
      IgnoreDisabledList ignore_disabled_list = IgnoreDisabledList::kFalse);

  // Validate whether a candidate tablet is eligible for a split.
  // Any tablets with default TTL and a max file size for compaction limit are assumed
  // ineligible by default.
  Status ValidateSplitCandidateTablet(
      const TabletInfo& tablet,
      IgnoreTtlValidation ignore_ttl_validation = IgnoreTtlValidation::kFalse,
      IgnoreDisabledList ignore_disabled_list = IgnoreDisabledList::kFalse);

  void MarkTtlTableForSplitIgnore(const TableId& table_id);

  void MarkSmallKeyRangeTabletForSplitIgnore(const TabletId& tablet_id);

 private:
  void ScheduleSplits(const std::unordered_set<TabletId>& splits_to_schedule);

  bool HasOutstandingTabletSplits(const TableInfoMap& table_info_map);

  void DoSplitting(const TableInfoMap& table_info_map, const TabletInfoMap& tablet_info_map);

  Status ValidateIndexTablePartitioning(const TableInfo& table);
  Status ValidateTableAgainstDisabledList(const TableId& table_id);
  Status ValidateTabletAgainstDisabledList(const TabletId& tablet_id);

  TabletSplitCandidateFilterIf* filter_;
  TabletSplitDriverIf* driver_;
  XClusterSplitDriverIf* xcluster_split_driver_;

  // Used to signal (e.g. to IsTabletSplittingComplete) that the tablet split manager is not
  // running, and hence it is safe to assume that no more splitting will occur if splitting was
  // disabled before calling IsTabletSplittingComplete. This should only be written to by the
  // automatic tablet splitting code.
  bool is_running_;
  CoarseTimePoint splitting_disabled_until_;
  CoarseTimePoint last_run_time_;

  template <typename IdType>
  using DisabledSet = std::unordered_map<IdType, CoarseTimePoint>;

  std::mutex mutex_;
  DisabledSet<TableId> ignore_table_for_splitting_until_ GUARDED_BY(mutex_);
  DisabledSet<TabletId> ignore_tablet_for_splitting_until_ GUARDED_BY(mutex_);
};

}  // namespace master
}  // namespace yb
#endif // YB_MASTER_TABLET_SPLIT_MANAGER_H
