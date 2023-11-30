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

#pragma once

#include <stdint.h>

#include <array>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>
#include <optional>

#include <boost/preprocessor/seq/for_each.hpp>
#include <boost/range/iterator_range.hpp>

#include "yb/client/client_fwd.h"

#include "yb/common/entity_ids.h"
#include "yb/common/read_hybrid_time.h"
#include "yb/common/transaction.h"
#include "yb/gutil/casts.h"

#include "yb/rpc/rpc_fwd.h"

#include "yb/tserver/tserver_fwd.h"
#include "yb/tserver/pg_client.pb.h"
#include "yb/tserver/tserver_shared_mem.h"
#include "yb/tserver/xcluster_context.h"

#include "yb/util/coding_consts.h"
#include "yb/util/enums.h"
#include "yb/util/locks.h"
#include "yb/util/strongly_typed_bool.h"
#include "yb/util/thread.h"

DECLARE_bool(ysql_enable_db_catalog_version_mode);

namespace yb {
class ConsistentReadPoint;

namespace tserver {

class PgMutationCounter;

#define PG_CLIENT_SESSION_METHODS \
    (AlterDatabase) \
    (AlterTable) \
    (BackfillIndex) \
    (CreateDatabase) \
    (CreateReplicationSlot) \
    (CreateTable) \
    (CreateTablegroup) \
    (DeleteDBSequences) \
    (DeleteSequenceTuple) \
    (DropDatabase) \
    (DropReplicationSlot) \
    (DropTable) \
    (DropTablegroup) \
    (FetchData) \
    (FetchSequenceTuple) \
    (FinishTransaction) \
    (InsertSequenceTuple) \
    (ReadSequenceTuple) \
    (RollbackToSubTransaction) \
    (SetActiveSubTransaction) \
    (TruncateTable) \
    (UpdateSequenceTuple) \
    (WaitForBackendsCatalogVersion) \
    /**/

// These methods may respond with Status::OK() and continue async processing (including network
// operations). In this case it is their responsibility to fill response and call
// context.RespondSuccess asynchronously.
// If such method responds with error Status, it will be handled by the upper layer that will fill
// response with error status and call context.RespondSuccess.
#define PG_CLIENT_SESSION_ASYNC_METHODS \
    (GetTableKeyRanges) \
    /**/

using PgClientSessionOperations = std::vector<std::shared_ptr<client::YBPgsqlOp>>;

YB_DEFINE_ENUM(PgClientSessionKind, (kPlain)(kDdl)(kCatalog)(kSequence));

YB_STRONGLY_TYPED_BOOL(IsDDL);

class PgClientSession {
  using TransactionBuilder = std::function<
      client::YBTransactionPtr(IsDDL, client::ForceGlobalTransaction, CoarseTimePoint)>;
  using SharedThisSource = std::shared_ptr<void>;

 public:
  struct UsedReadTime {
    simple_spinlock lock;
    ReadHybridTime value;
    TabletId tablet_id;
  };

  struct SessionData {
    client::YBSessionPtr session;
    client::YBTransactionPtr transaction;
  };

  using UsedReadTimePtr = std::weak_ptr<UsedReadTime>;

  PgClientSession(
      TransactionBuilder&& transaction_builder, SharedThisSource shared_this_source, uint64_t id,
      client::YBClient* client,
      const scoped_refptr<ClockBase>& clock, PgTableCache* table_cache,
      const std::optional<XClusterContext>& xcluster_context,
      PgMutationCounter* pg_node_level_mutation_counter, PgResponseCache* response_cache,
      PgSequenceCache* sequence_cache);

  uint64_t id() const { return id_; }

  Status Perform(PgPerformRequestPB* req, PgPerformResponsePB* resp, rpc::RpcContext* context);

  std::shared_ptr<CountDownLatch> ProcessSharedRequest(size_t size, SharedExchange* exchange);

  #define PG_CLIENT_SESSION_METHOD_DECLARE(r, data, method) \
  Status method( \
      const BOOST_PP_CAT(BOOST_PP_CAT(Pg, method), RequestPB)& req, \
      BOOST_PP_CAT(BOOST_PP_CAT(Pg, method), ResponsePB)* resp, \
      rpc::RpcContext* context);

  #define PG_CLIENT_SESSION_ASYNC_METHOD_DECLARE(r, data, method) \
  void method( \
      const BOOST_PP_CAT(BOOST_PP_CAT(Pg, method), RequestPB)& req, \
      BOOST_PP_CAT(BOOST_PP_CAT(Pg, method), ResponsePB)* resp, \
      rpc::RpcContext context);

  BOOST_PP_SEQ_FOR_EACH(PG_CLIENT_SESSION_METHOD_DECLARE, ~, PG_CLIENT_SESSION_METHODS);
  BOOST_PP_SEQ_FOR_EACH(PG_CLIENT_SESSION_ASYNC_METHOD_DECLARE, ~, PG_CLIENT_SESSION_ASYNC_METHODS);

  size_t SaveData(const RefCntBuffer& buffer);

 private:
  std::string LogPrefix();

  Result<const TransactionMetadata*> GetDdlTransactionMetadata(
      bool use_transaction, CoarseTimePoint deadline);
  Status BeginTransactionIfNecessary(
      const PgPerformOptionsPB& options, CoarseTimePoint deadline);
  Status DoBeginTransactionIfNecessary(
      const PgPerformOptionsPB& options, CoarseTimePoint deadline);

  Result<client::YBTransactionPtr> RestartTransaction(
      client::YBSession* session, client::YBTransaction* transaction);

  Result<std::pair<SessionData, PgClientSession::UsedReadTimePtr>> SetupSession(
      const PgPerformRequestPB& req, CoarseTimePoint deadline, HybridTime in_txn_limit);
  Status ProcessResponse(
      const PgClientSessionOperations& operations, const PgPerformRequestPB& req,
      PgPerformResponsePB* resp, rpc::RpcContext* context);
  Result<PgClientSession::UsedReadTimePtr> ProcessReadTimeManipulation(
      ReadTimeManipulation manipulation, uint64_t txn_serial_no);

  client::YBClient& client();
  client::YBSessionPtr& EnsureSession(PgClientSessionKind kind, CoarseTimePoint deadline);

  template <class T>
  static auto& DoSessionData(T* that, PgClientSessionKind kind) {
    return that->sessions_[to_underlying(kind)];
  }

  SessionData& GetSessionData(PgClientSessionKind kind) {
    return DoSessionData(this, kind);
  }

  const SessionData& GetSessionData(PgClientSessionKind kind) const {
    return DoSessionData(this, kind);
  }

  client::YBSessionPtr& Session(PgClientSessionKind kind) {
    return GetSessionData(kind).session;
  }

  client::YBTransactionPtr& Transaction(PgClientSessionKind kind) {
    return GetSessionData(kind).transaction;
  }

  const client::YBTransactionPtr& Transaction(PgClientSessionKind kind) const {
    return GetSessionData(kind).transaction;
  }

  Status CheckPlainSessionReadTime();

  // Set the read point to the databases xCluster safe time if consistent reads are enabled
  Status UpdateReadPointForXClusterConsistentReads(
      const PgPerformOptionsPB& options, const CoarseTimePoint& deadline,
      ConsistentReadPoint* read_point);

  template <class InRequestPB, class OutRequestPB>
  Status SetCatalogVersion(const InRequestPB& in_req, OutRequestPB* out_req) const {
    // Note that in initdb/bootstrap mode, even if FLAGS_enable_db_catalog_version_mode is
    // on it will be ignored and we'll use ysql_catalog_version not ysql_db_catalog_version.
    // That's why we must use in_req as the source of truth. Unlike the older version google
    // protobuf, this protobuf of in_req (google proto3) does not have has_ysql_catalog_version()
    // and has_ysql_db_catalog_version() member functions so we use invalid version 0 as an
    // alternative.
    // For now we either use global catalog version or db catalog version but not both.
    // So it is an error if both are set.
    // It is possible that neither is set during initdb.
    SCHECK(in_req.ysql_catalog_version() == 0 || in_req.ysql_db_catalog_version() == 0,
           InvalidArgument, "Wrong catalog versions: $0 and $1",
           in_req.ysql_catalog_version(), in_req.ysql_db_catalog_version());
    if (in_req.ysql_db_catalog_version()) {
      CHECK(FLAGS_ysql_enable_db_catalog_version_mode);
      out_req->set_ysql_db_catalog_version(in_req.ysql_db_catalog_version());
      out_req->set_ysql_db_oid(narrow_cast<uint32_t>(in_req.db_oid()));
    } else if (in_req.ysql_catalog_version()) {
      out_req->set_ysql_catalog_version(in_req.ysql_catalog_version());
    }
    return Status::OK();
  }

  template <class DataPtr>
  Status DoPerform(const DataPtr& data, CoarseTimePoint deadline, rpc::RpcContext* context);

  // Resets the session's current read point.
  //
  // For kPlain sessions, also reset the plain session used read time since the tserver will pick a
  // read time and send back as "used read time" in the response for use by future rpcs of the
  // session.
  PgClientSession::UsedReadTimePtr ResetReadPoint(PgClientSessionKind kind);

  // NOTE: takes ownership of paging_state.
  void GetTableKeyRanges(
      client::YBSessionPtr session, const std::shared_ptr<client::YBTable>& table,
      Slice lower_bound_key, Slice upper_bound_key, uint64_t max_num_ranges,
      uint64_t range_size_bytes, bool is_forward, uint32_t max_key_length, rpc::Sidecars* sidecars,
      PgsqlPagingStatePB* paging_state, std::function<void(Status)> callback);

  const std::weak_ptr<PgClientSession> shared_this_;
  const uint64_t id_;
  client::YBClient& client_;
  scoped_refptr<ClockBase> clock_;
  const TransactionBuilder transaction_builder_;
  PgTableCache& table_cache_;
  const std::optional<XClusterContext> xcluster_context_;
  PgMutationCounter* pg_node_level_mutation_counter_;
  PgResponseCache& response_cache_;
  PgSequenceCache& sequence_cache_;

  std::array<SessionData, kPgClientSessionKindMapSize> sessions_;
  uint64_t txn_serial_no_ = 0;
  uint64_t read_time_serial_no_ = 0;
  std::optional<uint64_t> saved_priority_;
  TransactionMetadata ddl_txn_metadata_;
  UsedReadTime plain_session_used_read_time_;

  simple_spinlock pending_data_mutex_;
  std::vector<RefCntBuffer> pending_data_ GUARDED_BY(pending_data_mutex_);
};

}  // namespace tserver
}  // namespace yb
