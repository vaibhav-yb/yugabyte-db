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

#ifndef YB_MASTER_MASTER_SNAPSHOT_COORDINATOR_H
#define YB_MASTER_MASTER_SNAPSHOT_COORDINATOR_H

#include "yb/common/entity_ids.h"
#include "yb/common/hybrid_time.h"
#include "yb/common/snapshot.h"

#include "yb/docdb/docdb.pb.h"
#include "yb/gutil/ref_counted.h"

#include "yb/master/master_fwd.h"
#include "yb/master/master_heartbeat.fwd.h"
#include "yb/master/master_types.pb.h"

#include "yb/tablet/snapshot_coordinator.h"

#include "yb/util/status_fwd.h"
#include "yb/util/opid.h"

namespace yb {
namespace master {

struct SnapshotScheduleRestoration {
  TxnSnapshotId snapshot_id;
  HybridTime restore_at;
  TxnSnapshotRestorationId restoration_id;
  OpId op_id;
  HybridTime write_time;
  int64_t term;
  std::vector<std::pair<SnapshotScheduleId, SnapshotScheduleFilterPB>> schedules;
  std::vector<std::pair<TabletId, SysTabletsEntryPB>> non_system_obsolete_tablets;
  std::vector<std::pair<TableId, SysTablesEntryPB>> non_system_obsolete_tables;
  std::unordered_map<std::string, SysRowEntryType> non_system_objects_to_restore;
  // YSQL pg_catalog_tables in the current state (as of restore request time).
  std::unordered_map<TableId, TableName> existing_system_tables;
  // YSQL pg_catalog_tables present in the snapshot to restore to.
  std::unordered_set<TableId> restoring_system_tables;
};

// Class that coordinates transaction aware snapshots at master.
class MasterSnapshotCoordinator : public tablet::SnapshotCoordinator {
 public:
  explicit MasterSnapshotCoordinator(
      SnapshotCoordinatorContext* context, enterprise::CatalogManager* cm);
  ~MasterSnapshotCoordinator();

  Result<TxnSnapshotId> Create(
      const SysRowEntries& entries, bool imported, int64_t leader_term, CoarseTimePoint deadline);

  Result<TxnSnapshotId> CreateForSchedule(
      const SnapshotScheduleId& schedule_id, int64_t leader_term, CoarseTimePoint deadline);

  Status Delete(
      const TxnSnapshotId& snapshot_id, int64_t leader_term, CoarseTimePoint deadline);

  // As usual negative leader_term means that this operation was replicated at the follower.
  Status CreateReplicated(
      int64_t leader_term, const tablet::SnapshotOperation& operation) override;

  Status DeleteReplicated(
      int64_t leader_term, const tablet::SnapshotOperation& operation) override;

  Status RestoreSysCatalogReplicated(
      int64_t leader_term, const tablet::SnapshotOperation& operation,
      Status* complete_status) override;

  Status ListSnapshots(
      const TxnSnapshotId& snapshot_id, bool list_deleted, ListSnapshotsResponsePB* resp);

  Result<TxnSnapshotRestorationId> Restore(
      const TxnSnapshotId& snapshot_id, HybridTime restore_at, int64_t leader_term);

  Status ListRestorations(
      const TxnSnapshotRestorationId& restoration_id, const TxnSnapshotId& snapshot_id,
      ListSnapshotRestorationsResponsePB* resp);

  Result<SnapshotScheduleId> CreateSchedule(
      const CreateSnapshotScheduleRequestPB& request, int64_t leader_term,
      CoarseTimePoint deadline);

  Status ListSnapshotSchedules(
      const SnapshotScheduleId& snapshot_schedule_id, ListSnapshotSchedulesResponsePB* resp);

  Status DeleteSnapshotSchedule(
      const SnapshotScheduleId& snapshot_schedule_id, int64_t leader_term,
      CoarseTimePoint deadline);

  // Load snapshots data from system catalog.
  Status Load(tablet::Tablet* tablet) override;

  // Check whether we have write request for snapshot while replaying write request during
  // bootstrap. And upsert snapshot from it in this case.
  // key and value are entry from the write batch.
  Status ApplyWritePair(const Slice& key, const Slice& value) override;

  Status FillHeartbeatResponse(TSHeartbeatResponsePB* resp);

  void SysCatalogLoaded(int64_t term);

  Result<docdb::KeyValuePairPB> UpdateRestorationAndGetWritePair(
      SnapshotScheduleRestoration* restoration);

  // For each returns map from schedule id to sorted vectors of tablets id in this schedule.
  Result<SnapshotSchedulesToObjectIdsMap> MakeSnapshotSchedulesToObjectIdsMap(
      SysRowEntryType type);

  Result<bool> IsTableCoveredBySomeSnapshotSchedule(const TableInfo& table_info);

  // Returns true if there are one or more non-deleted
  // snapshot schedules present.
  bool IsPitrActive();

  void Start();

  void Shutdown();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace master
} // namespace yb

#endif // YB_MASTER_MASTER_SNAPSHOT_COORDINATOR_H
