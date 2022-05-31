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

#ifndef ENT_SRC_YB_MASTER_CATALOG_MANAGER_H
#define ENT_SRC_YB_MASTER_CATALOG_MANAGER_H

#include "../../../../src/yb/master/catalog_manager.h"
#include "yb/master/master_snapshot_coordinator.h"
#include "yb/master/snapshot_coordinator_context.h"
#include "yb/cdc/cdc_service.proxy.h"

namespace yb {

class UniverseKeyRegistryPB;

namespace master {
namespace enterprise {

struct KeyRange;

YB_DEFINE_ENUM(CreateObjects, (kOnlyTables)(kOnlyIndexes));

class CatalogManager : public yb::master::CatalogManager, SnapshotCoordinatorContext {
  typedef yb::master::CatalogManager super;
 public:
  explicit CatalogManager(yb::master::Master* master)
      : super(master), snapshot_coordinator_(this, this) {}

  virtual ~CatalogManager();
  void CompleteShutdown();

  Status RunLoaders(int64_t term) override REQUIRES(mutex_);

  // API to start a snapshot creation.
  Status CreateSnapshot(const CreateSnapshotRequestPB* req,
                                CreateSnapshotResponsePB* resp,
                                rpc::RpcContext* rpc);

  // API to list all available snapshots.
  Status ListSnapshots(const ListSnapshotsRequestPB* req,
                               ListSnapshotsResponsePB* resp);

  Status ListSnapshotRestorations(const ListSnapshotRestorationsRequestPB* req,
                                          ListSnapshotRestorationsResponsePB* resp);

  // API to restore a snapshot.
  Status RestoreSnapshot(const RestoreSnapshotRequestPB* req,
                                 RestoreSnapshotResponsePB* resp);

  // API to delete a snapshot.
  Status DeleteSnapshot(const DeleteSnapshotRequestPB* req,
                                DeleteSnapshotResponsePB* resp,
                                rpc::RpcContext* rpc);

  Status ImportSnapshotMeta(const ImportSnapshotMetaRequestPB* req,
                                    ImportSnapshotMetaResponsePB* resp,
                                    rpc::RpcContext* rpc);

  Status CreateSnapshotSchedule(const CreateSnapshotScheduleRequestPB* req,
                                        CreateSnapshotScheduleResponsePB* resp,
                                        rpc::RpcContext* rpc);

  Status ListSnapshotSchedules(const ListSnapshotSchedulesRequestPB* req,
                                       ListSnapshotSchedulesResponsePB* resp,
                                       rpc::RpcContext* rpc);

  Status DeleteSnapshotSchedule(const DeleteSnapshotScheduleRequestPB* req,
                                        DeleteSnapshotScheduleResponsePB* resp,
                                        rpc::RpcContext* rpc);

  Status ChangeEncryptionInfo(const ChangeEncryptionInfoRequestPB* req,
                                      ChangeEncryptionInfoResponsePB* resp) override;

  Status UpdateXClusterConsumerOnTabletSplit(
      const TableId& consumer_table_id, const SplitTabletIds& split_tablet_ids) override;

  Status UpdateXClusterProducerOnTabletSplit(
      const TableId& producer_table_id, const SplitTabletIds& split_tablet_ids) override;

  Status InitCDCConsumer(const std::vector<CDCConsumerStreamInfo>& consumer_info,
                                 const std::string& master_addrs,
                                 const std::string& producer_universe_uuid,
                                 std::shared_ptr<CDCRpcTasks> cdc_rpc_tasks);

  void HandleCreateTabletSnapshotResponse(TabletInfo *tablet, bool error) override;

  void HandleRestoreTabletSnapshotResponse(TabletInfo *tablet, bool error) override;

  void HandleDeleteTabletSnapshotResponse(
      const SnapshotId& snapshot_id, TabletInfo *tablet, bool error) override;

  void DumpState(std::ostream* out, bool on_disk_dump = false) const override;

  // Fills the heartbeat response with the decrypted universe key registry.
  Status FillHeartbeatResponse(const TSHeartbeatRequestPB* req,
                                       TSHeartbeatResponsePB* resp) override;

  // Is encryption at rest enabled for this cluster.
  Status IsEncryptionEnabled(const IsEncryptionEnabledRequestPB* req,
                                     IsEncryptionEnabledResponsePB* resp);

  // Create a new CDC stream with the specified attributes.
  Status CreateCDCStream(const CreateCDCStreamRequestPB* req,
                                 CreateCDCStreamResponsePB* resp,
                                 rpc::RpcContext* rpc);

  // Delete the specified CDCStream.
      Status DeleteCDCStream(const DeleteCDCStreamRequestPB* req,
                                 DeleteCDCStreamResponsePB* resp,
                                 rpc::RpcContext* rpc);

  // List CDC streams (optionally, for a given table).
  Status ListCDCStreams(const ListCDCStreamsRequestPB* req,
                                ListCDCStreamsResponsePB* resp) override;

  // Fetch CDC stream info corresponding to a db stream id
  Status GetCDCDBStreamInfo(const GetCDCDBStreamInfoRequestPB* req,
                                    GetCDCDBStreamInfoResponsePB* resp) override;

  // Get CDC stream.
  Status GetCDCStream(const GetCDCStreamRequestPB* req,
                              GetCDCStreamResponsePB* resp,
                              rpc::RpcContext* rpc);

  // Update a CDC stream.
  Status UpdateCDCStream(const UpdateCDCStreamRequestPB* req,
                                 UpdateCDCStreamResponsePB* resp,
                                 rpc::RpcContext* rpc);

  // Query if Bootstrapping is required for a CDC stream (e.g. Are we missing logs).
  Status IsBootstrapRequired(const IsBootstrapRequiredRequestPB* req,
                                     IsBootstrapRequiredResponsePB* resp,
                                     rpc::RpcContext* rpc);

  // Delete CDC streams for a table.
  Status DeleteCDCStreamsForTable(const TableId& table_id) override;
  Status DeleteCDCStreamsForTables(const vector<TableId>& table_ids) override;

  // Setup Universe Replication to consume data from another YB universe.
  Status SetupUniverseReplication(const SetupUniverseReplicationRequestPB* req,
                                          SetupUniverseReplicationResponsePB* resp,
                                          rpc::RpcContext* rpc);

  // Delete Universe Replication.
  Status DeleteUniverseReplication(const DeleteUniverseReplicationRequestPB* req,
                                           DeleteUniverseReplicationResponsePB* resp,
                                           rpc::RpcContext* rpc);

  // Alter Universe Replication.
  Status AlterUniverseReplication(const AlterUniverseReplicationRequestPB* req,
                                          AlterUniverseReplicationResponsePB* resp,
                                          rpc::RpcContext* rpc);

  // Rename an existing Universe Replication.
  Status RenameUniverseReplication(scoped_refptr<UniverseReplicationInfo> universe,
                                           const AlterUniverseReplicationRequestPB* req,
                                           AlterUniverseReplicationResponsePB* resp,
                                           rpc::RpcContext* rpc);

  // Enable/Disable an Existing Universe Replication.
  Status SetUniverseReplicationEnabled(const SetUniverseReplicationEnabledRequestPB* req,
                                               SetUniverseReplicationEnabledResponsePB* resp,
                                               rpc::RpcContext* rpc);

  // Get Universe Replication.
  Status GetUniverseReplication(const GetUniverseReplicationRequestPB* req,
                                        GetUniverseReplicationResponsePB* resp,
                                        rpc::RpcContext* rpc);

  // Checks if the universe is in an active state or has failed during setup.
  Status IsSetupUniverseReplicationDone(const IsSetupUniverseReplicationDoneRequestPB* req,
                                                IsSetupUniverseReplicationDoneResponsePB* resp,
                                                rpc::RpcContext* rpc);

  // On a producer side split, creates new pollers on the consumer for the new tablet children.
  Status UpdateConsumerOnProducerSplit(const UpdateConsumerOnProducerSplitRequestPB* req,
                                               UpdateConsumerOnProducerSplitResponsePB* resp,
                                               rpc::RpcContext* rpc);

  // Find all the CDC streams that have been marked as DELETED.
  Status FindCDCStreamsMarkedAsDeleting(std::vector<scoped_refptr<CDCStreamInfo>>* streams);

  // Delete specified CDC streams.
  Status CleanUpDeletedCDCStreams(const std::vector<scoped_refptr<CDCStreamInfo>>& streams);

  bool IsCdcEnabled(const TableInfo& table_info) const override;

  tablet::SnapshotCoordinator& snapshot_coordinator() override {
    return snapshot_coordinator_;
  }

  Result<size_t> GetNumLiveTServersForActiveCluster() override;

  Status ClearFailedUniverse();

  void SetCDCServiceEnabled();

  void PrepareRestore() override;

 private:
  friend class SnapshotLoader;
  friend class yb::master::ClusterLoadBalancer;
  friend class CDCStreamLoader;
  friend class UniverseReplicationLoader;

  Status RestoreEntry(const SysRowEntry& entry, const SnapshotId& snapshot_id)
      REQUIRES(mutex_);

  // Per table structure for external cluster snapshot importing to this cluster.
  // Old IDs mean IDs on external cluster, new IDs - IDs on this cluster.
  struct ExternalTableSnapshotData {
    bool is_index() const {
      return !table_entry_pb.indexed_table_id().empty();
    }

    NamespaceId old_namespace_id;
    TableId old_table_id;
    TableId new_table_id;
    SysTablesEntryPB table_entry_pb;
    std::string pg_schema_name;
    size_t num_tablets = 0;
    typedef std::pair<std::string, std::string> PartitionKeys;
    typedef std::map<PartitionKeys, TabletId> PartitionToIdMap;
    typedef std::vector<PartitionPB> Partitions;
    Partitions partitions;
    PartitionToIdMap new_tablets_map;
    // Mapping: Old tablet ID -> New tablet ID.
    google::protobuf::RepeatedPtrField<IdPairPB>* tablet_id_map = nullptr;

    ImportSnapshotMetaResponsePB_TableMetaPB* table_meta = nullptr;
  };

  // Map: old_namespace_id (key) -> new_namespace_id (value) + db_type.
  typedef std::pair<NamespaceId, YQLDatabase> NamespaceData;
  typedef std::map<NamespaceId, NamespaceData> NamespaceMap;
  typedef std::map<TableId, ExternalTableSnapshotData> ExternalTableSnapshotDataMap;

  Status ImportSnapshotPreprocess(const SnapshotInfoPB& snapshot_pb,
                                          ImportSnapshotMetaResponsePB* resp,
                                          NamespaceMap* namespace_map,
                                          ExternalTableSnapshotDataMap* tables_data);
  Status ImportSnapshotCreateObject(const SnapshotInfoPB& snapshot_pb,
                                            ImportSnapshotMetaResponsePB* resp,
                                            NamespaceMap* namespace_map,
                                            ExternalTableSnapshotDataMap* tables_data,
                                            CreateObjects create_objects);
  Status ImportSnapshotWaitForTables(const SnapshotInfoPB& snapshot_pb,
                                             ImportSnapshotMetaResponsePB* resp,
                                             ExternalTableSnapshotDataMap* tables_data,
                                             CoarseTimePoint deadline);
  Status ImportSnapshotProcessTablets(const SnapshotInfoPB& snapshot_pb,
                                              ImportSnapshotMetaResponsePB* resp,
                                              ExternalTableSnapshotDataMap* tables_data);
  void DeleteNewSnapshotObjects(const NamespaceMap& namespace_map,
                                const ExternalTableSnapshotDataMap& tables_data);

  // Helper function for ImportTableEntry.
  Result<bool> CheckTableForImport(
      scoped_refptr<TableInfo> table,
      ExternalTableSnapshotData* snapshot_data) REQUIRES_SHARED(mutex_);

  Status ImportNamespaceEntry(const SysRowEntry& entry,
                                      NamespaceMap* namespace_map);
  Status RecreateTable(const NamespaceId& new_namespace_id,
                               const ExternalTableSnapshotDataMap& table_map,
                               ExternalTableSnapshotData* table_data);
  Status RepartitionTable(scoped_refptr<TableInfo> table,
                                  const ExternalTableSnapshotData* table_data);
  Status ImportTableEntry(const NamespaceMap& namespace_map,
                                  const ExternalTableSnapshotDataMap& table_map,
                                  ExternalTableSnapshotData* s_data);
  Status PreprocessTabletEntry(const SysRowEntry& entry,
                                       ExternalTableSnapshotDataMap* table_map);
  Status ImportTabletEntry(const SysRowEntry& entry,
                                   ExternalTableSnapshotDataMap* table_map);

  TabletInfos GetTabletInfos(const std::vector<TabletId>& ids) override;

  Result<std::map<std::string, KeyRange>> GetTableKeyRanges(const TableId& table_id);

  Result<SysRowEntries> CollectEntries(
      const google::protobuf::RepeatedPtrField<TableIdentifierPB>& tables,
      CollectFlags flags);

  Result<SysRowEntries> CollectEntriesForSnapshot(
      const google::protobuf::RepeatedPtrField<TableIdentifierPB>& tables) override;

  server::Clock* Clock() override;

  const Schema& schema() override;

  const docdb::DocReadContext& doc_read_context();

  void Submit(std::unique_ptr<tablet::Operation> operation, int64_t leader_term) override;

  AsyncTabletSnapshotOpPtr CreateAsyncTabletSnapshotOp(
      const TabletInfoPtr& tablet, const std::string& snapshot_id,
      tserver::TabletSnapshotOpRequestPB::Operation operation,
      TabletSnapshotOperationCallback callback) override;

  void ScheduleTabletSnapshotOp(const AsyncTabletSnapshotOpPtr& operation) override;

  Status RestoreSysCatalog(
      SnapshotScheduleRestoration* restoration, tablet::Tablet* tablet,
      Status* complete_status) override;

  Status VerifyRestoredObjects(
      const std::unordered_map<std::string, SysRowEntryType>& objects,
      const google::protobuf::RepeatedPtrField<TableIdentifierPB>& tables) override;

  void CleanupHiddenObjects(const ScheduleMinRestoreTime& schedule_min_restore_time) override;
  void CleanupHiddenTablets(
      const std::vector<TabletInfoPtr>& hidden_tablets,
      const ScheduleMinRestoreTime& schedule_min_restore_time);
  // Will filter tables content, so pass it by value here.
  void CleanupHiddenTables(
      std::vector<TableInfoPtr> tables,
      const ScheduleMinRestoreTime& schedule_min_restore_time);

  rpc::Scheduler& Scheduler() override;

  int64_t LeaderTerm() override;

  Result<bool> IsTablePartOfSomeSnapshotSchedule(const TableInfo& table_info) override;

  Result<SnapshotSchedulesToObjectIdsMap> MakeSnapshotSchedulesToObjectIdsMap(
      SysRowEntryType type) override;

  bool IsPitrActive() override;

  static void SetTabletSnapshotsState(SysSnapshotEntryPB::State state,
                                      SysSnapshotEntryPB* snapshot_pb);

  // Create the cdc_state table if needed (i.e. if it does not exist already).
  //
  // This is called at the end of CreateCDCStream.
  Status CreateCdcStateTableIfNeeded(rpc::RpcContext *rpc);

  // Check if cdc_state table creation is done.
  Status IsCdcStateTableCreated(IsCreateTableDoneResponsePB* resp);

  // Return all CDC streams.
  void GetAllCDCStreams(std::vector<scoped_refptr<CDCStreamInfo>>* streams);

  // Mark specified CDC streams as DELETING so they can be removed later.
  Status MarkCDCStreamsAsDeleting(const std::vector<scoped_refptr<CDCStreamInfo>>& streams);

  // Find CDC streams for a table.
  std::vector<scoped_refptr<CDCStreamInfo>> FindCDCStreamsForTableUnlocked(const TableId& table_id)
      const REQUIRES_SHARED(mutex_);

  bool CDCStreamExistsUnlocked(const CDCStreamId& stream_id) override REQUIRES_SHARED(mutex_);

  Status FillHeartbeatResponseEncryption(const SysClusterConfigEntryPB& cluster_config,
                                                 const TSHeartbeatRequestPB* req,
                                                 TSHeartbeatResponsePB* resp);

  Status FillHeartbeatResponseCDC(const SysClusterConfigEntryPB& cluster_config,
                                          const TSHeartbeatRequestPB* req,
                                          TSHeartbeatResponsePB* resp);

  // Helper functions for GetTableSchemaCallback, GetTablegroupSchemaCallback
  // and GetColocatedTabletSchemaCallback.

  // Validates a single table's schema with the corresponding table on the consumer side, and
  // updates consumer_table_id with the new table id. Return the consumer table schema if the
  // validation is successful.
  Status ValidateTableSchema(
      const std::shared_ptr<client::YBTableInfo>& info,
      const std::unordered_map<TableId, std::string>& table_bootstrap_ids,
      GetTableSchemaResponsePB* resp);
  // Adds a validated table to the sys catalog table map for the given universe, and if all tables
  // have been validated, creates a CDC stream for each table.
  Status AddValidatedTableAndCreateCdcStreams(
      scoped_refptr<UniverseReplicationInfo> universe,
      const std::unordered_map<TableId, std::string>& table_bootstrap_ids,
      const TableId& producer_table,
      const TableId& consumer_table);

  void GetTableSchemaCallback(
      const std::string& universe_id, const std::shared_ptr<client::YBTableInfo>& info,
      const std::unordered_map<TableId, std::string>& producer_bootstrap_ids, const Status& s);
  void GetTablegroupSchemaCallback(
      const std::string& universe_id, const std::shared_ptr<std::vector<client::YBTableInfo>>& info,
      const TablegroupId& producer_tablegroup_id,
      const std::unordered_map<TableId, std::string>& producer_bootstrap_ids, const Status& s);
  void GetColocatedTabletSchemaCallback(
      const std::string& universe_id, const std::shared_ptr<std::vector<client::YBTableInfo>>& info,
      const std::unordered_map<TableId, std::string>& producer_bootstrap_ids, const Status& s);
  void GetCDCStreamCallback(const CDCStreamId& bootstrap_id,
                            std::shared_ptr<TableId> table_id,
                            std::shared_ptr<std::unordered_map<std::string, std::string>> options,
                            const std::string& universe_id,
                            const TableId& table,
                            std::shared_ptr<CDCRpcTasks> cdc_rpc,
                            const Status& s);
  void AddCDCStreamToUniverseAndInitConsumer(const std::string& universe_id, const TableId& table,
                                             const Result<CDCStreamId>& stream_id,
                                             std::function<void()> on_success_cb = nullptr);

  void MergeUniverseReplication(scoped_refptr<UniverseReplicationInfo> info,
                                std::string original_id);
  Status DeleteUniverseReplicationUnlocked(scoped_refptr<UniverseReplicationInfo> info);
  void MarkUniverseReplicationFailed(scoped_refptr<UniverseReplicationInfo> universe,
                                     const Status& failure_status);

  // Checks if table has at least one cdc stream (includes producers for xCluster replication).
  bool IsTableCdcProducer(const TableInfo& table_info) const override REQUIRES_SHARED(mutex_);

  // Checks if the table is a consumer in an xCluster replication universe.
  bool IsTableCdcConsumer(const TableInfo& table_info) const REQUIRES_SHARED(mutex_);

  // Maps producer universe id to the corresponding cdc stream for that table.
  typedef std::unordered_map<std::string, CDCStreamId> XClusterConsumerTableStreamInfoMap;

  std::shared_ptr<cdc::CDCServiceProxy> GetCDCServiceProxy(
      client::internal::RemoteTabletServer* ts);

  Result<client::internal::RemoteTabletServer*> GetLeaderTServer(
      client::internal::RemoteTabletPtr tablet);

  // Consumer API: Find out if bootstrap is required for the Producer tables.
  Status IsBootstrapRequiredOnProducer(scoped_refptr<UniverseReplicationInfo> universe,
                                               const TableId& producer_table,
                                               const std::unordered_map<TableId, std::string>&
                                                 table_bootstrap_ids);

  // Gets the set of CDC stream info for an xCluster consumer table.
  XClusterConsumerTableStreamInfoMap GetXClusterStreamInfoForConsumerTable(const TableId& table_id)
      const;

  XClusterConsumerTableStreamInfoMap GetXClusterStreamInfoForConsumerTableUnlocked(
      const TableId& table_id) const REQUIRES_SHARED(mutex_);

  Status CreateTransactionAwareSnapshot(
      const CreateSnapshotRequestPB& req, CreateSnapshotResponsePB* resp, rpc::RpcContext* rpc);

  Status CreateNonTransactionAwareSnapshot(
      const CreateSnapshotRequestPB* req, CreateSnapshotResponsePB* resp, rpc::RpcContext* rpc);

  Status RestoreNonTransactionAwareSnapshot(const SnapshotId& snapshot_id);

  Status DeleteNonTransactionAwareSnapshot(const SnapshotId& snapshot_id);

  void Started() override;

  void SysCatalogLoaded(int64_t term) override;

  Result<SysRowEntries> CollectEntriesForSequencesDataTable();

  // Snapshot map: snapshot-id -> SnapshotInfo.
  typedef std::unordered_map<SnapshotId, scoped_refptr<SnapshotInfo>> SnapshotInfoMap;
  SnapshotInfoMap non_txn_snapshot_ids_map_;
  SnapshotId current_snapshot_id_;

  // mutex on should_send_universe_key_registry_mutex_.
  mutable simple_spinlock should_send_universe_key_registry_mutex_;
  // Should catalog manager resend latest universe key registry to tserver.
  std::unordered_map<TabletServerId, bool> should_send_universe_key_registry_
  GUARDED_BY(should_send_universe_key_registry_mutex_);

  // CDC Stream map: CDCStreamId -> CDCStreamInfo.
  typedef std::unordered_map<CDCStreamId, scoped_refptr<CDCStreamInfo>> CDCStreamInfoMap;
  CDCStreamInfoMap cdc_stream_map_ GUARDED_BY(mutex_);

  // Map of tables -> number of cdc streams they are producers for.
  std::unordered_map<TableId, int> cdc_stream_tables_count_map_ GUARDED_BY(mutex_);

  // Map of all consumer tables that are part of xcluster replication, to a map of the stream infos.
  std::unordered_map<TableId, XClusterConsumerTableStreamInfoMap>
      xcluster_consumer_tables_to_stream_map_ GUARDED_BY(mutex_);

  typedef std::unordered_map<std::string, scoped_refptr<UniverseReplicationInfo>>
      UniverseReplicationInfoMap;
  UniverseReplicationInfoMap universe_replication_map_ GUARDED_BY(mutex_);

  // List of universe ids to universes that must be deleted
  std::deque<std::string> universes_to_clear_ GUARDED_BY(mutex_);

  // mutex on should_send_consumer_registry_mutex_.
  mutable simple_spinlock should_send_consumer_registry_mutex_;
  // Should catalog manager resend latest consumer registry to tserver.
  std::unordered_map<TabletServerId, bool> should_send_consumer_registry_
  GUARDED_BY(should_send_consumer_registry_mutex_);

  MasterSnapshotCoordinator snapshot_coordinator_;

  // True when the cluster is a producer of a valid replication stream.
  std::atomic<bool> cdc_enabled_{false};

  DISALLOW_COPY_AND_ASSIGN(CatalogManager);
};

} // namespace enterprise
} // namespace master
} // namespace yb

#endif // ENT_SRC_YB_MASTER_CATALOG_MANAGER_H
