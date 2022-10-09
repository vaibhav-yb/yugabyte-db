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

#ifndef ENT_SRC_YB_TSERVER_CDC_CONSUMER_H
#define ENT_SRC_YB_TSERVER_CDC_CONSUMER_H

#include <condition_variable>
#include <unordered_map>
#include <unordered_set>

#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index_container.hpp>

#include "yb/cdc/cdc_util.h"
#include "yb/client/client_fwd.h"
#include "yb/common/common_types.pb.h"
#include "yb/tablet/tablet_types.pb.h"
#include "yb/util/locks.h"
#include "yb/util/monotime.h"

namespace yb {

class Thread;
class ThreadPool;

namespace rpc {

class Messenger;
class ProxyCache;
class Rpcs;
class SecureContext;

} // namespace rpc

namespace cdc {

class ConsumerRegistryPB;

} // namespace cdc

namespace tserver {
namespace enterprise {

class CDCPoller;
class TabletServer;

struct CDCClient {
  std::unique_ptr<rpc::Messenger> messenger;
  std::unique_ptr<rpc::SecureContext> secure_context;
  std::shared_ptr<client::YBClient> client;

  ~CDCClient();
  void Shutdown();
};

class CDCConsumer {
 public:
  static Result<std::unique_ptr<CDCConsumer>> Create(
      std::function<bool(const std::string&)> is_leader_for_tablet,
      rpc::ProxyCache* proxy_cache,
     TabletServer* tserver);

  CDCConsumer(std::function<bool(const std::string&)> is_leader_for_tablet,
      rpc::ProxyCache* proxy_cache,
      const std::string& ts_uuid,
      std::unique_ptr<CDCClient> local_client,
      client::TransactionManager* transaction_manager);

  ~CDCConsumer();
  void Shutdown() EXCLUDES(should_run_mutex_);

  // Refreshes the in memory state when we receive a new registry from master.
  void RefreshWithNewRegistryFromMaster(const cdc::ConsumerRegistryPB* consumer_registry,
                                        int32_t cluster_config_version);

  std::vector<std::string> TEST_producer_tablets_running();

  std::vector<std::shared_ptr<CDCPoller>> TEST_ListPollers();

  std::string LogPrefix();

  // Return the value stored in cluster_config_version_. Since we are reading an atomic variable,
  // we don't need to hold the mutex.
  int32_t cluster_config_version() const NO_THREAD_SAFETY_ANALYSIS;

  void IncrementNumSuccessfulWriteRpcs() {
    TEST_num_successful_write_rpcs++;
  }

  uint32_t GetNumSuccessfulWriteRpcs() {
    return TEST_num_successful_write_rpcs.load(std::memory_order_acquire);
  }

  Status ReloadCertificates();

  Status PublishXClusterSafeTime();

  client::TransactionManager* TransactionManager();

  Result<cdc::ConsumerTabletInfo> GetConsumerTableInfo(const TabletId& producer_tablet_id);

  // Stores a replication error and detail. This overwrites a previously stored 'error'.
  void StoreReplicationError(
    const TabletId& tablet_id, const CDCStreamId& stream_id, ReplicationErrorPb error,
    const std::string& detail);

  // Returns the replication error map.
  cdc::TabletReplicationErrorMap GetReplicationErrors() const;

 private:
  // Runs a thread that periodically polls for any new threads.
  void RunThread() EXCLUDES(should_run_mutex_);

  // Loops through all the entries in the registry and creates a producer -> consumer tablet
  // mapping.
  void UpdateInMemoryState(const cdc::ConsumerRegistryPB* consumer_producer_map,
                           int32_t cluster_config_version);

  // Loops through all entries in registry from master to check if all producer tablets are being
  // polled for.
  void TriggerPollForNewTablets();

  bool ShouldContinuePolling(
      const cdc::ProducerTabletInfo producer_tablet_info,
      const cdc::ConsumerTabletInfo consumer_tablet_info) EXCLUDES(should_run_mutex_);

  void RemoveFromPollersMap(const cdc::ProducerTabletInfo producer_tablet_info);

  // Mutex and cond for should_run_ state.
  std::mutex should_run_mutex_;
  std::condition_variable cond_;
  bool should_run_ = true;

  // Mutex for producer_consumer_tablet_map_from_master_.
  rw_spinlock master_data_mutex_;

  // Mutex for producer_pollers_map_.
  rw_spinlock producer_pollers_map_mutex_ ACQUIRED_AFTER(master_data_mutex_);

  std::function<bool(const std::string&)> is_leader_for_tablet_;

  class TabletTag;
  using ProducerConsumerTabletMap = boost::multi_index_container <
    cdc::XClusterTabletInfo,
    boost::multi_index::indexed_by <
      boost::multi_index::hashed_unique <
          boost::multi_index::member <
              cdc::XClusterTabletInfo, cdc::ProducerTabletInfo,
              &cdc::XClusterTabletInfo::producer_tablet_info>
      >,
      boost::multi_index::hashed_non_unique <
          boost::multi_index::tag <TabletTag>,
          boost::multi_index::const_mem_fun <
              cdc::XClusterTabletInfo, const TabletId&,
              &cdc::XClusterTabletInfo::producer_tablet_id
          >
      >
    >
  >;

  ProducerConsumerTabletMap producer_consumer_tablet_map_from_master_
      GUARDED_BY(master_data_mutex_);

  std::unordered_set<std::string> streams_with_local_tserver_optimization_
      GUARDED_BY(master_data_mutex_);
  std::unordered_map<std::string, uint32_t> stream_to_schema_version_
      GUARDED_BY(master_data_mutex_);

  scoped_refptr<Thread> run_trigger_poll_thread_;

  std::unordered_map<cdc::ProducerTabletInfo, std::shared_ptr<CDCPoller>,
                     cdc::ProducerTabletInfo::Hash> producer_pollers_map_
                     GUARDED_BY(producer_pollers_map_mutex_);

  std::unique_ptr<ThreadPool> thread_pool_;
  std::unique_ptr<rpc::Rpcs> rpcs_;

  std::string log_prefix_;
  std::shared_ptr<CDCClient> local_client_;

  // map: {universe_uuid : ...}.
  std::unordered_map<std::string, std::shared_ptr<CDCClient>> remote_clients_
    GUARDED_BY(producer_pollers_map_mutex_);
  std::unordered_map<std::string, std::string> uuid_master_addrs_
    GUARDED_BY(master_data_mutex_);
  std::unordered_set<std::string> changed_master_addrs_ GUARDED_BY(master_data_mutex_);

  std::atomic<int32_t> cluster_config_version_ GUARDED_BY(master_data_mutex_) = {-1};

  std::atomic<uint32_t> TEST_num_successful_write_rpcs {0};

  std::mutex safe_time_update_mutex_;
  MonoTime last_safe_time_published_at_ GUARDED_BY(safe_time_update_mutex_);

  bool xcluster_safe_time_table_ready_ GUARDED_BY(safe_time_update_mutex_);
  std::unique_ptr<client::TableHandle> safe_time_table_ GUARDED_BY(safe_time_update_mutex_);

  client::TransactionManager* transaction_manager_;

  client::YBTablePtr global_transaction_status_table_;

  bool enable_replicate_transaction_status_table_;

  mutable simple_spinlock tablet_replication_error_map_lock_;
  cdc::TabletReplicationErrorMap tablet_replication_error_map_
    GUARDED_BY(tablet_replication_error_map_lock_);
};

} // namespace enterprise
} // namespace tserver
} // namespace yb

#endif // ENT_SRC_YB_TSERVER_CDC_CONSUMER_H
