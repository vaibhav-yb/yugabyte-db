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

#ifndef ENT_SRC_YB_CDC_CDC_SERVICE_H
#define ENT_SRC_YB_CDC_CDC_SERVICE_H

#include <memory>

#include "yb/cdc/cdc_error.h"
#include "yb/cdc/cdc_metrics.h"
#include "yb/cdc/cdc_producer.h"
#include "yb/cdc/cdc_service.proxy.h"
#include "yb/cdc/cdc_service.service.h"
#include "yb/cdc/cdc_util.h"
#include "yb/client/async_initializer.h"

#include "yb/common/schema.h"

#include "yb/master/master_client.fwd.h"

#include "yb/rpc/rpc.h"
#include "yb/rpc/rpc_context.h"
#include "yb/rpc/rpc_controller.h"

#include "yb/util/net/net_util.h"
#include "yb/util/service_util.h"
#include "yb/util/semaphore.h"

#include <boost/optional.hpp>

namespace yb {

namespace client {

class TableHandle;

}

namespace tserver {

class TSTabletManager;

}

namespace cdc {

typedef std::unordered_map<HostPort, std::shared_ptr<CDCServiceProxy>, HostPortHash>
  CDCServiceProxyMap;

YB_STRONGLY_TYPED_BOOL(CreateCDCMetricsEntity);

static const char* const kRecordType = "record_type";
static const char* const kRecordFormat = "record_format";
static const char* const kRetentionSec = "retention_sec";
static const char* const kSourceType = "source_type";
static const char* const kCheckpointType = "checkpoint_type";
static const char* const kIdType = "id_type";
static const char* const kNamespaceId = "NAMESPACEID";
static const char* const kTableId = "TABLEID";

struct TabletCheckpoint {
  OpId op_id;
  // Timestamp at which the op ID was last updated.
  CoarseTimePoint last_update_time;

  bool ExpiredAt(std::chrono::milliseconds duration, std::chrono::time_point<CoarseMonoClock> now) {
    return (now - last_update_time) > duration;
  }
};

// Maintain each tablet minimum checkpoint info for
// log cache eviction as well as for intent cleanup.
struct TabletCDCCheckpointInfo {
  OpId cdc_op_id = OpId::Max();
  OpId cdc_sdk_op_id = OpId::Invalid();
};

using TabletOpIdMap = std::unordered_map<TabletId, TabletCDCCheckpointInfo>;

class CDCServiceImpl : public CDCServiceIf {
 public:
  CDCServiceImpl(tserver::TSTabletManager* tablet_manager,
                 const scoped_refptr<MetricEntity>& metric_entity_server,
                 MetricRegistry* metric_registry);

  CDCServiceImpl(const CDCServiceImpl&) = delete;
  void operator=(const CDCServiceImpl&) = delete;

  ~CDCServiceImpl();

  void CreateCDCStream(const CreateCDCStreamRequestPB* req,
                       CreateCDCStreamResponsePB* resp,
                       rpc::RpcContext rpc) override;
  void DeleteCDCStream(const DeleteCDCStreamRequestPB *req,
                       DeleteCDCStreamResponsePB* resp,
                       rpc::RpcContext rpc) override;
  void ListTablets(const ListTabletsRequestPB *req,
                   ListTabletsResponsePB* resp,
                   rpc::RpcContext rpc) override;
  void GetChanges(const GetChangesRequestPB* req,
                  GetChangesResponsePB* resp,
                  rpc::RpcContext rpc) override;
  void GetCheckpoint(const GetCheckpointRequestPB* req,
                     GetCheckpointResponsePB* resp,
                     rpc::RpcContext rpc) override;

  // Update peers in other tablet servers about the latest minimum applied cdc index for a specific
  // tablet.
  void UpdateCdcReplicatedIndex(const UpdateCdcReplicatedIndexRequestPB* req,
                                UpdateCdcReplicatedIndexResponsePB* resp,
                                rpc::RpcContext rpc) override;

  void GetLatestEntryOpId(const GetLatestEntryOpIdRequestPB* req,
                          GetLatestEntryOpIdResponsePB* resp,
                          rpc::RpcContext context) override;

  void BootstrapProducer(const BootstrapProducerRequestPB* req,
                         BootstrapProducerResponsePB* resp,
                         rpc::RpcContext rpc) override;

  void GetCDCDBStreamInfo(const GetCDCDBStreamInfoRequestPB* req,
                          GetCDCDBStreamInfoResponsePB* resp,
                          rpc::RpcContext context) override;

  CHECKED_STATUS UpdateCdcReplicatedIndexEntry(
      const string& tablet_id, int64 replicated_index, boost::optional<int64> replicated_term,
      const OpId& cdc_sdk_replicated_op);

  Result<SetCDCCheckpointResponsePB> SetCDCCheckpoint(
      const SetCDCCheckpointRequestPB& req, CoarseTimePoint deadline) override;

  void IsBootstrapRequired(const IsBootstrapRequiredRequestPB* req,
                           IsBootstrapRequiredResponsePB* resp,
                           rpc::RpcContext rpc) override;

  void Shutdown() override;

  // Gets the associated metrics entity object stored in the additional metadata of the tablet.
  // If the metrics object is not present, then create it if create == true (eg if we have just
  // moved leaders) and not else (used to not recreate deleted metrics).
  std::shared_ptr<CDCTabletMetrics> GetCDCTabletMetrics(
      const ProducerTabletInfo& producer,
      std::shared_ptr<tablet::TabletPeer> tablet_peer = nullptr,
      CreateCDCMetricsEntity create = CreateCDCMetricsEntity::kTrue);

  void RemoveCDCTabletMetrics(
      const ProducerTabletInfo& producer,
      std::shared_ptr<tablet::TabletPeer> tablet_peer);

  void UpdateCDCTabletMetrics(const GetChangesResponsePB* resp,
                              const ProducerTabletInfo& producer_tablet,
                              const std::shared_ptr<tablet::TabletPeer>& tablet_peer,
                              const OpId& op_id,
                              int64_t last_readable_index);

  std::shared_ptr<CDCServerMetrics> GetCDCServerMetrics() {
    return server_metrics_;
  }

  // Returns true if this server has received a GetChanges call.
  bool CDCEnabled();

  void RetainIntents(
      const std::shared_ptr<tablet::TabletPeer>& tablet_peer, const OpId& cdc_sdk_op_id);

 private:
  FRIEND_TEST(CDCServiceTest, TestMetricsOnDeletedReplication);
  FRIEND_TEST(CDCServiceTestMultipleServersOneTablet, TestMetricsAfterServerFailure);

  class Impl;

  template <class ReqType, class RespType>
  bool CheckOnline(const ReqType* req, RespType* resp, rpc::RpcContext* rpc);

  Result<OpId> GetLastCheckpoint(const ProducerTabletInfo& producer_tablet,
                                 const client::YBSessionPtr& session);

  Result<std::vector<pair<std::string, std::string>>> GetDBStreamInfo(
          const std::string& db_stream_id,
          const client::YBSessionPtr& session);

  Result<std::string> GetCdcStreamId(const ProducerTabletInfo& producer_tablet,
                                     const std::shared_ptr<client::YBSession>& session);

  CHECKED_STATUS UpdateCheckpoint(const ProducerTabletInfo& producer_tablet,
                                  const OpId& sent_op_id,
                                  const OpId& commit_op_id,
                                  const client::YBSessionPtr& session,
                                  uint64_t last_record_hybrid_time,
                                  bool force_update = false);

  Result<google::protobuf::RepeatedPtrField<master::TabletLocationsPB>> GetTablets(
      const CDCStreamId& stream_id);

  Status CreateCDCStreamForTable(
      const TableId& table_id,
      const std::unordered_map<std::string, std::string>& options,
      const CDCStreamId& stream_id);

  void RollbackPartialCreate(const CDCCreationState& creation_state);

  Result<NamespaceId> GetNamespaceId(const std::string& ns_name);

  Result<std::shared_ptr<StreamMetadata>> GetStream(const std::string& stream_id);

  std::shared_ptr<StreamMetadata> GetStreamMetadataFromCache(const std::string& stream_id);
  void AddStreamMetadataToCache(const std::string& stream_id,
                                const std::shared_ptr<StreamMetadata>& stream_metadata);

  CHECKED_STATUS CheckTabletValidForStream(const ProducerTabletInfo& producer_info);

  void TabletLeaderGetChanges(const GetChangesRequestPB* req,
                              GetChangesResponsePB* resp,
                              std::shared_ptr<rpc::RpcContext> context,
                              std::shared_ptr<tablet::TabletPeer> peer);

  void TabletLeaderGetCheckpoint(const GetCheckpointRequestPB* req,
                                 GetCheckpointResponsePB* resp,
                                 rpc::RpcContext* context,
                                 const std::shared_ptr<tablet::TabletPeer>& peer);

  void UpdateTabletPeersWithMinReplicatedIndex(const TabletOpIdMap& tablet_min_checkpoint_map);

  Result<OpId> TabletLeaderLatestEntryOpId(const TabletId& tablet_id);

  void TabletLeaderIsBootstrapRequired(const IsBootstrapRequiredRequestPB* req,
                                       IsBootstrapRequiredResponsePB* resp,
                                       rpc::RpcContext* context,
                                       const std::shared_ptr<tablet::TabletPeer>& peer);

  Result<client::internal::RemoteTabletPtr> GetRemoteTablet(const TabletId& tablet_id);
  Result<client::internal::RemoteTabletServer *> GetLeaderTServer(const TabletId& tablet_id);
  CHECKED_STATUS GetTServers(const TabletId& tablet_id,
                             std::vector<client::internal::RemoteTabletServer*>* servers);

  std::shared_ptr<CDCServiceProxy> GetCDCServiceProxy(client::internal::RemoteTabletServer* ts);

  OpId GetMinSentCheckpointForTablet(const std::string& tablet_id);

  std::shared_ptr<MemTracker> GetMemTracker(
      const std::shared_ptr<tablet::TabletPeer>& tablet_peer,
      const ProducerTabletInfo& producer_info);

  OpId GetMinAppliedCheckpointForTablet(const std::string& tablet_id,
                                        const client::YBSessionPtr& session);

  CHECKED_STATUS UpdatePeersCdcMinReplicatedIndex(
      const TabletId& tablet_id,
      const TabletCDCCheckpointInfo& cdc_checkpoint_min);

  // Used as a callback function for parallelizing async cdc rpc calls.
  // Given a finished tasks counter, and the number of total rpc calls
  // in flight, the callback will increment the counter when called, and
  // set the promise to be fulfilled when all tasks have completed.
  void XClusterAsyncPromiseCallback(std::promise<void>* const promise,
                                    std::atomic<int>* const finished_tasks,
                                    int total_tasks);

  CHECKED_STATUS BootstrapProducerHelperParallelized(
    const BootstrapProducerRequestPB* req,
    BootstrapProducerResponsePB* resp,
    std::vector<client::YBOperationPtr>* ops,
    CDCCreationState* creation_state);

  CHECKED_STATUS BootstrapProducerHelper(
    const BootstrapProducerRequestPB* req,
    BootstrapProducerResponsePB* resp,
    std::vector<client::YBOperationPtr>* ops,
    CDCCreationState* creation_state);

  void ComputeLagMetric(int64_t last_replicated_micros, int64_t metric_last_timestamp_micros,
                        int64_t cdc_state_last_replication_time_micros,
                        scoped_refptr<AtomicGauge<int64_t>> metric);

  // Update metrics async_replication_sent_lag_micros and async_replication_committed_lag_micros.
  // Called periodically default 1s.
  void UpdateLagMetrics();

  // This method is used to read the cdc_state table to find the minimum replicated index for each
  // tablet and then update the peers' log objects. Also used to update lag metrics.
  void UpdatePeersAndMetrics();

  MicrosTime GetLastReplicatedTime(const std::shared_ptr<tablet::TabletPeer>& tablet_peer);

  bool ShouldUpdateLagMetrics(MonoTime time_since_update_metrics);

  Result<std::shared_ptr<client::TableHandle>> GetCdcStateTable() EXCLUDES(mutex_);

  void RefreshCdcStateTable() EXCLUDES(mutex_);

  Status RefreshCacheOnFail(const Status& s) EXCLUDES(mutex_);

  client::YBClient* client();

  void CreateEntryInCdcStateTable(
      const std::shared_ptr<client::TableHandle>& cdc_state_table,
      std::vector<ProducerTabletInfo>* producer_entries_modified,
      std::vector<client::YBOperationPtr>* ops,
      const CDCStreamId& stream_id,
      const TableId& table_id,
      const TabletId& tablet_id);

  Status CreateCDCStreamForNamespace(
      const CreateCDCStreamRequestPB* req,
      CreateCDCStreamResponsePB* resp,
      CoarseTimePoint deadline);

  Result<TabletOpIdMap> PopulateTabletCheckPointInfo(const TabletId& input_tablet_id = "");

  Status SetInitialCheckPoint(
      const OpId& checkpoint, const string& tablet_id,
      const std::shared_ptr<tablet::TabletPeer>& tablet_peer);

  rpc::Rpcs rpcs_;

  tserver::TSTabletManager* tablet_manager_;

  MetricRegistry* metric_registry_;

  std::shared_ptr<CDCServerMetrics> server_metrics_;

  // Prevents GetChanges "storms" by rejecting when all permits have been acquired.
  Semaphore get_changes_rpc_sem_;

  // Used to protect tablet_checkpoints_ and stream_metadata_ maps.
  mutable rw_spinlock mutex_;

  std::unique_ptr<Impl> impl_;

  std::shared_ptr<client::TableHandle> cdc_state_table_ GUARDED_BY(mutex_);

  std::unordered_map<std::string, std::shared_ptr<StreamMetadata>> stream_metadata_
      GUARDED_BY(mutex_);

  // Map of HostPort -> CDCServiceProxy. This is used to redirect requests to tablet leader's
  // CDC service proxy.
  CDCServiceProxyMap cdc_service_map_ GUARDED_BY(mutex_);

  // Thread with a few functions:
  //
  // Read the cdc_state table and get the minimum checkpoint for each tablet
  // and then, for each tablet this tserver is a leader, update the log minimum cdc replicated
  // index so we can use this information to correctly keep log files that are needed so we
  // can continue replicating cdc records. This runs periodically to handle
  // leadership changes (FLAGS_update_min_cdc_indices_interval_secs).
  // TODO(hector): It would be better to do this update only when a local peer becomes a leader.
  //
  // Periodically update lag metrics (FLAGS_update_metrics_interval_ms).
  std::unique_ptr<std::thread> update_peers_and_metrics_thread_;

  // True when this service is stopped. Used to inform
  // get_minimum_checkpoints_and_update_peers_thread_ that it should exit.
  bool cdc_service_stopped_ GUARDED_BY(mutex_){false};

  // True when this service has received a GetChanges request on a valid replication stream.
  std::atomic<bool> cdc_enabled_{false};
};

}  // namespace cdc
}  // namespace yb

#endif  // ENT_SRC_YB_CDC_CDC_SERVICE_H
