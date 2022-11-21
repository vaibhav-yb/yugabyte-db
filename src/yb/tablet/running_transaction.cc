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

#include "yb/tablet/running_transaction.h"

#include "yb/client/transaction_rpc.h"

#include "yb/common/hybrid_time.h"
#include "yb/common/pgsql_error.h"

#include "yb/tablet/transaction_participant_context.h"

#include "yb/tserver/tserver_service.pb.h"

#include "yb/util/flags.h"
#include "yb/util/logging.h"
#include "yb/util/trace.h"
#include "yb/util/tsan_util.h"
#include "yb/util/yb_pg_errcodes.h"

using namespace std::placeholders;
using namespace std::literals;

DEFINE_test_flag(uint64, transaction_delay_status_reply_usec_in_tests, 0,
                 "For tests only. Delay handling status reply by specified amount of usec.");

DEFINE_UNKNOWN_int64(transaction_abort_check_interval_ms, 5000 * yb::kTimeMultiplier,
             "Interval to check whether running transaction was aborted.");

DEFINE_UNKNOWN_int64(transaction_abort_check_timeout_ms, 30000 * yb::kTimeMultiplier,
             "Timeout used when checking for aborted transactions.");

namespace yb {
namespace tablet {

RunningTransaction::RunningTransaction(TransactionMetadata metadata,
                                       const TransactionalBatchData& last_batch_data,
                                       OneWayBitmap&& replicated_batches,
                                       HybridTime base_time_for_abort_check_ht_calculation,
                                       RunningTransactionContext* context)
    : metadata_(std::move(metadata)),
      last_batch_data_(last_batch_data),
      replicated_batches_(std::move(replicated_batches)),
      context_(*context),
      remove_intents_task_(&context->applier_, &context->participant_context_, context,
                           metadata_.transaction_id),
      get_status_handle_(context->rpcs_.InvalidHandle()),
      abort_handle_(context->rpcs_.InvalidHandle()),
      apply_intents_task_(&context->applier_, context, &apply_data_),
      abort_check_ht_(base_time_for_abort_check_ht_calculation.AddDelta(
                          1ms * FLAGS_transaction_abort_check_interval_ms)) {
}

RunningTransaction::~RunningTransaction() {
  context_.rpcs_.Abort({&get_status_handle_, &abort_handle_});
}

void RunningTransaction::AddReplicatedBatch(
  size_t batch_idx, boost::container::small_vector_base<uint8_t>* encoded_replicated_batches) {
  VLOG_WITH_PREFIX(4) << __func__ << "(" << batch_idx << ")";
  replicated_batches_.Set(batch_idx);
  encoded_replicated_batches->push_back(docdb::KeyEntryTypeAsChar::kBitSet);
  replicated_batches_.EncodeTo(encoded_replicated_batches);
}

void RunningTransaction::BatchReplicated(const TransactionalBatchData& value) {
  VLOG_WITH_PREFIX(4) << __func__ << "(" << value.ToString() << ")";
  last_batch_data_ = value;
}

void RunningTransaction::SetLocalCommitData(
    HybridTime time, const AbortedSubTransactionSet& aborted_subtxn_set) {
  last_known_aborted_subtxn_set_ = aborted_subtxn_set;
  local_commit_time_ = time;
  last_known_status_hybrid_time_ = local_commit_time_;
  last_known_status_ = TransactionStatus::COMMITTED;
}

void RunningTransaction::Aborted() {
  VLOG_WITH_PREFIX(4) << __func__ << "()";

  last_known_status_ = TransactionStatus::ABORTED;
  last_known_status_hybrid_time_ = HybridTime::kMax;
}

void RunningTransaction::RequestStatusAt(const StatusRequest& request,
                                         std::unique_lock<std::mutex>* lock) {
  DCHECK_LE(request.global_limit_ht, HybridTime::kMax);
  DCHECK_LE(request.read_ht, request.global_limit_ht);

  if (last_known_status_hybrid_time_ > HybridTime::kMin) {
    auto transaction_status =
        GetStatusAt(request.global_limit_ht, last_known_status_hybrid_time_, last_known_status_,
                    external_transaction());
    // If we don't have status at global_limit_ht, then we should request updated status.
    if (transaction_status) {
      HybridTime last_known_status_hybrid_time = last_known_status_hybrid_time_;
      AbortedSubTransactionSet local_commit_aborted_subtxn_set;
      if (transaction_status == TransactionStatus::COMMITTED ||
          transaction_status == TransactionStatus::PENDING) {
        local_commit_aborted_subtxn_set = last_known_aborted_subtxn_set_;
      }
      lock->unlock();
      request.callback(TransactionStatusResult{
          *transaction_status, last_known_status_hybrid_time, local_commit_aborted_subtxn_set});
      return;
    }
  }
  bool was_empty = status_waiters_.empty();
  status_waiters_.push_back(request);
  if (!was_empty) {
    return;
  }
  auto request_id = context_.NextRequestIdUnlocked();
  auto shared_self = shared_from_this();

  VLOG_WITH_PREFIX(4) << Format(
      "Existing status knowledge ($0, $1) does not satisfy requested: $2, sending: $3",
      TransactionStatus_Name(last_known_status_), last_known_status_hybrid_time_, request,
      request_id);

  lock->unlock();
  SendStatusRequest(request_id, shared_self);
}

bool RunningTransaction::WasAborted() const {
  return last_known_status_ == TransactionStatus::ABORTED;
}

Status RunningTransaction::CheckAborted() const {
  if (WasAborted()) {
    return MakeAbortedStatus(id());
  }
  return Status::OK();
}

void RunningTransaction::Abort(client::YBClient* client,
                               TransactionStatusCallback callback,
                               std::unique_lock<std::mutex>* lock) {
  if (last_known_status_ == TransactionStatus::ABORTED ||
      last_known_status_ == TransactionStatus::COMMITTED) {
    // Transaction is already in final state, so no reason to send abort request.
    VLOG_WITH_PREFIX(3) << "Abort shortcut: " << last_known_status_;
    TransactionStatusResult status{last_known_status_, last_known_status_hybrid_time_};
    lock->unlock();
    callback(status);
    return;
  }
  bool was_empty = abort_waiters_.empty();
  abort_waiters_.push_back(std::move(callback));
  lock->unlock();
  VLOG_WITH_PREFIX(3) << "Abort request: " << was_empty;
  if (!was_empty) {
    return;
  }
  tserver::AbortTransactionRequestPB req;
  req.set_tablet_id(metadata_.status_tablet);
  req.set_transaction_id(metadata_.transaction_id.data(), metadata_.transaction_id.size());
  req.set_propagated_hybrid_time(context_.participant_context_.Now().ToUint64());
  context_.rpcs_.RegisterAndStart(
      client::AbortTransaction(
          TransactionRpcDeadline(),
          nullptr /* tablet */,
          client,
          &req,
          std::bind(&RunningTransaction::AbortReceived, this, _1, _2, shared_from_this())),
      &abort_handle_);
}

std::string RunningTransaction::ToString() const {
  return Format("{ metadata: $0 last_batch_data: $1 replicated_batches: $2 local_commit_time: $3 "
                    "last_known_status: $4 last_known_status_hybrid_time: $5 }",
                metadata_, last_batch_data_, replicated_batches_, local_commit_time_,
                TransactionStatus_Name(last_known_status_), last_known_status_hybrid_time_);
}

void RunningTransaction::ScheduleRemoveIntents(
    const RunningTransactionPtr& shared_self, RemoveReason reason) {
  if (remove_intents_task_.Prepare(shared_self, reason)) {
    context_.participant_context_.StrandEnqueue(&remove_intents_task_);
    VLOG_WITH_PREFIX(1) << "Intents should be removed asynchronously";
  }
}

boost::optional<TransactionStatus> RunningTransaction::GetStatusAt(
    HybridTime time,
    HybridTime last_known_status_hybrid_time,
    TransactionStatus last_known_status,
    bool external_transaction) {
  switch (last_known_status) {
    case TransactionStatus::ABORTED: {
      if (!external_transaction || last_known_status_hybrid_time >= time) {
        return TransactionStatus::ABORTED;
      }
      return boost::none;
    }
    case TransactionStatus::COMMITTED:
      return last_known_status_hybrid_time > time
          ? TransactionStatus::PENDING
          : TransactionStatus::COMMITTED;
    case TransactionStatus::PENDING:
      if (last_known_status_hybrid_time >= time) {
        return TransactionStatus::PENDING;
      }
      return boost::none;
    default:
      FATAL_INVALID_ENUM_VALUE(TransactionStatus, last_known_status);
  }
}

void RunningTransaction::SendStatusRequest(
    int64_t serial_no, const RunningTransactionPtr& shared_self) {
  TRACE_FUNC();
  VTRACE(1, yb::ToString(metadata_.transaction_id));
  tserver::GetTransactionStatusRequestPB req;
  req.set_tablet_id(metadata_.status_tablet);
  req.add_transaction_id()->assign(
      pointer_cast<const char*>(metadata_.transaction_id.data()), metadata_.transaction_id.size());
  req.set_propagated_hybrid_time(context_.participant_context_.Now().ToUint64());
  context_.rpcs_.RegisterAndStart(
      client::GetTransactionStatus(
          TransactionRpcDeadline(),
          nullptr /* tablet */,
          context_.participant_context_.client_future().get(),
          &req,
          std::bind(&RunningTransaction::StatusReceived, this, _1, _2, serial_no, shared_self)),
      &get_status_handle_);
}

void RunningTransaction::StatusReceived(
    const Status& status,
    const tserver::GetTransactionStatusResponsePB& response,
    int64_t serial_no,
    const RunningTransactionPtr& shared_self) {
  auto delay_usec = FLAGS_TEST_transaction_delay_status_reply_usec_in_tests;
  if (delay_usec > 0) {
    context_.delayer().Delay(
        MonoTime::Now() + MonoDelta::FromMicroseconds(delay_usec),
        std::bind(&RunningTransaction::DoStatusReceived, this, status, response,
                  serial_no, shared_self));
  } else {
    DoStatusReceived(status, response, serial_no, shared_self);
  }
}

bool RunningTransaction::UpdateStatus(
    TransactionStatus transaction_status, HybridTime time_of_status,
    HybridTime coordinator_safe_time, AbortedSubTransactionSet aborted_subtxn_set) {
  if (!local_commit_time_ && transaction_status != TransactionStatus::ABORTED) {
    // If we've already committed locally, then last_known_aborted_subtxn_set_ is already set
    // properly. Otherwise, we should update it here.
    last_known_aborted_subtxn_set_ = aborted_subtxn_set;
  }

  // Check for local_commit_time_ is not required for correctness, but useful for optimization.
  // So we could avoid unnecessary actions.
  if (local_commit_time_) {
    return false;
  }

  if (transaction_status == TransactionStatus::ABORTED && coordinator_safe_time) {
    time_of_status = coordinator_safe_time;
  }
  last_known_status_hybrid_time_ = time_of_status;

  if (transaction_status == last_known_status_) {
    return false;
  }

  last_known_status_ = transaction_status;

  return transaction_status == TransactionStatus::ABORTED;
}

void RunningTransaction::DoStatusReceived(const Status& status,
                                          const tserver::GetTransactionStatusResponsePB& response,
                                          int64_t serial_no,
                                          const RunningTransactionPtr& shared_self) {
  TRACE("$0: $1", __func__, response.ShortDebugString());
  VLOG_WITH_PREFIX(4) << __func__ << "(" << status << ", " << response.ShortDebugString() << ", "
                      << serial_no << ")";

  if (response.has_propagated_hybrid_time()) {
    context_.participant_context_.UpdateClock(HybridTime(response.propagated_hybrid_time()));
  }

  context_.rpcs_.Unregister(&get_status_handle_);
  decltype(status_waiters_) status_waiters;
  HybridTime time_of_status = HybridTime::kMin;
  TransactionStatus transaction_status = TransactionStatus::PENDING;
  AbortedSubTransactionSet aborted_subtxn_set;
  const bool ok = status.ok();
  int64_t new_request_id = -1;
  {
    MinRunningNotifier min_running_notifier(&context_.applier_);
    std::unique_lock<std::mutex> lock(context_.mutex_);
    if (!ok) {
      status_waiters_.swap(status_waiters);
      lock.unlock();
      for (const auto& waiter : status_waiters) {
        waiter.callback(status);
      }
      return;
    }

    if (response.status_hybrid_time().size() != 1 ||
        response.status().size() != 1 ||
        (response.aborted_subtxn_set().size() != 0 && response.aborted_subtxn_set().size() != 1)) {
      LOG_WITH_PREFIX(DFATAL)
          << "Wrong number of status, status hybrid time, or aborted subtxn set entries, "
          << "exactly one entry expected: "
          << response.ShortDebugString();
    } else if (PREDICT_FALSE(response.aborted_subtxn_set().empty())) {
      YB_LOG_EVERY_N(WARNING, 1)
          << "Empty aborted_subtxn_set in transaction status response. "
          << "This should only happen when nodes are on different versions, e.g. during upgrade.";
    } else {
      auto aborted_subtxn_set_or_status = AbortedSubTransactionSet::FromPB(
          response.aborted_subtxn_set(0).set());
      if (aborted_subtxn_set_or_status.ok()) {
        time_of_status = HybridTime(response.status_hybrid_time()[0]);
        transaction_status = response.status(0);
        aborted_subtxn_set = aborted_subtxn_set_or_status.get();
      } else {
        LOG_WITH_PREFIX(DFATAL)
            << "Could not deserialize AbortedSubTransactionSet: "
            << "error - " << aborted_subtxn_set_or_status.status().ToString()
            << " response - " << response.ShortDebugString();
      }
    }

    LOG_IF_WITH_PREFIX(DFATAL, response.coordinator_safe_time().size() > 1)
        << "Wrong number of coordinator safe time entries, at most one expected: "
        << response.ShortDebugString();
    auto coordinator_safe_time = response.coordinator_safe_time().size() == 1
        ? HybridTime::FromPB(response.coordinator_safe_time(0)) : HybridTime();
    auto did_abort_txn = UpdateStatus(
        transaction_status, time_of_status, coordinator_safe_time, aborted_subtxn_set);
    if (did_abort_txn) {
      context_.EnqueueRemoveUnlocked(id(), RemoveReason::kStatusReceived, &min_running_notifier);
    }

    time_of_status = last_known_status_hybrid_time_;
    transaction_status = last_known_status_;
    aborted_subtxn_set = last_known_aborted_subtxn_set_;

    status_waiters = ExtractFinishedStatusWaitersUnlocked(
        serial_no, time_of_status, transaction_status);
    if (!status_waiters_.empty()) {
      new_request_id = context_.NextRequestIdUnlocked();
      VLOG_WITH_PREFIX(4) << "Waiters still present, send new status request: " << new_request_id;
    }
  }
  if (new_request_id >= 0) {
    SendStatusRequest(new_request_id, shared_self);
  }
  NotifyWaiters(serial_no, time_of_status, transaction_status, aborted_subtxn_set, status_waiters);
}

std::vector<StatusRequest> RunningTransaction::ExtractFinishedStatusWaitersUnlocked(
    int64_t serial_no, HybridTime time_of_status, TransactionStatus transaction_status) {
  if (transaction_status == TransactionStatus::ABORTED) {
    return std::move(status_waiters_);
  }
  std::vector<StatusRequest> result;
  result.reserve(status_waiters_.size());
  auto w = status_waiters_.begin();
  for (auto it = status_waiters_.begin(); it != status_waiters_.end(); ++it) {
    if (it->serial_no <= serial_no ||
        GetStatusAt(
            it->global_limit_ht, time_of_status, transaction_status, external_transaction()) ||
        time_of_status < it->read_ht) {
      result.push_back(std::move(*it));
    } else {
      if (w != it) {
        *w = std::move(*it);
      }
      ++w;
    }
  }
  status_waiters_.erase(w, status_waiters_.end());
  return result;
}

void RunningTransaction::NotifyWaiters(int64_t serial_no, HybridTime time_of_status,
                                       TransactionStatus transaction_status,
                                       const AbortedSubTransactionSet& aborted_subtxn_set,
                                       const std::vector<StatusRequest>& status_waiters) {
  for (const auto& waiter : status_waiters) {
    auto status_for_waiter = GetStatusAt(
        waiter.global_limit_ht, time_of_status, transaction_status, external_transaction());
    if (status_for_waiter) {
      if (external_transaction() && *status_for_waiter == TransactionStatus::PENDING) {
        last_known_status_hybrid_time_ = waiter.read_ht;
        last_known_status_ = TransactionStatus::PENDING;
        waiter.callback(TransactionStatusResult{
            TransactionStatus::PENDING, waiter.read_ht, aborted_subtxn_set});
      } else {
        // We know status at global_limit_ht, so could notify waiter.
        auto result = TransactionStatusResult{*status_for_waiter, time_of_status};
        if (result.status == TransactionStatus::COMMITTED ||
            result.status == TransactionStatus::PENDING) {
          result.aborted_subtxn_set = aborted_subtxn_set;
        }
        waiter.callback(std::move(result));
      }
    } else if (time_of_status >= waiter.read_ht) {
      // It means that between read_ht and global_limit_ht transaction was pending.
      // It implies that transaction was not committed before request was sent.
      // We could safely respond PENDING to caller.
      LOG_IF_WITH_PREFIX(DFATAL, waiter.serial_no > serial_no)
          << "Notify waiter with request id greater than id of status request: "
          << waiter.serial_no << " vs " << serial_no;
      waiter.callback(TransactionStatusResult{
          TransactionStatus::PENDING, time_of_status, aborted_subtxn_set});
    } else {
      waiter.callback(STATUS(TryAgain,
          Format("Cannot determine transaction status with read_ht $0, and global_limit_ht $1, "
                 "last known: $2 at $3", waiter.read_ht, waiter.global_limit_ht,
                 TransactionStatus_Name(transaction_status), time_of_status), Slice(),
          PgsqlError(YBPgErrorCode::YB_PG_T_R_SERIALIZATION_FAILURE) ));
    }
  }
}

Result<TransactionStatusResult> RunningTransaction::MakeAbortResult(
    const Status& status,
    const tserver::AbortTransactionResponsePB& response) {
  if (!status.ok()) {
    return status;
  }

  HybridTime status_time = response.has_status_hybrid_time()
       ? HybridTime(response.status_hybrid_time())
       : HybridTime::kInvalid;
  return TransactionStatusResult{response.status(), status_time, AbortedSubTransactionSet()};
}

void RunningTransaction::AbortReceived(const Status& status,
                                       const tserver::AbortTransactionResponsePB& response,
                                       const RunningTransactionPtr& shared_self) {
  if (response.has_propagated_hybrid_time()) {
    context_.participant_context_.UpdateClock(HybridTime(response.propagated_hybrid_time()));
  }

  decltype(abort_waiters_) abort_waiters;
  auto result = MakeAbortResult(status, response);

  VLOG_WITH_PREFIX(3) << "AbortReceived: " << yb::ToString(result);

  {
    MinRunningNotifier min_running_notifier(&context_.applier_);
    std::lock_guard<std::mutex> lock(context_.mutex_);
    context_.rpcs_.Unregister(&abort_handle_);
    abort_waiters_.swap(abort_waiters);
    // kMax status_time means that this status is not yet replicated and could be rejected.
    // So we could use it as reply to Abort, but cannot store it as transaction status.
    if (result.ok() && result->status_time != HybridTime::kMax) {
      auto coordinator_safe_time = HybridTime::FromPB(response.coordinator_safe_time());
      if (UpdateStatus(
          result->status, result->status_time, coordinator_safe_time, result->aborted_subtxn_set)) {
        context_.EnqueueRemoveUnlocked(id(), RemoveReason::kAbortReceived, &min_running_notifier);
      }
    }
  }
  for (const auto& waiter : abort_waiters) {
    waiter(result);
  }
}

std::string RunningTransaction::LogPrefix() const {
  return Format(
      "$0 ID $1: ", context_.LogPrefix().substr(0, context_.LogPrefix().length() - 2), id());
}

Status MakeAbortedStatus(const TransactionId& id) {
  return STATUS(
      TryAgain, Format("Transaction aborted: $0", id), Slice(),
      PgsqlError(YBPgErrorCode::YB_PG_T_R_SERIALIZATION_FAILURE));
}

void RunningTransaction::SetApplyData(const docdb::ApplyTransactionState& apply_state,
                                      const TransactionApplyData* data,
                                      ScopedRWOperation* operation) {
  // TODO(savepoints): Add test to ensure that apply_state.aborted is properly set here.
  apply_state_ = apply_state;
  bool active = apply_state_.active();
  if (active) {
    // We are trying to assign set processing apply before starting actual process, and unset
    // after we complete processing.
    processing_apply_.store(true, std::memory_order_release);
  }

  if (data) {
    if (!active) {
      LOG_WITH_PREFIX(DFATAL)
          << "Starting processing apply, but provided data in inactive state: " << data->ToString();
      return;
    }

    apply_data_ = *data;
    apply_data_.apply_state = &apply_state_;

    LOG_IF_WITH_PREFIX(DFATAL, local_commit_time_ != data->commit_ht)
        << "Commit time does not match: " << local_commit_time_ << " vs " << data->commit_ht;

    if (apply_intents_task_.Prepare(shared_from_this(), operation)) {
      context_.participant_context_.StrandEnqueue(&apply_intents_task_);
    } else {
      LOG_WITH_PREFIX(DFATAL) << "Unable to prepare apply intents task";
    }
  }

  if (!active) {
    processing_apply_.store(false, std::memory_order_release);

    VLOG_WITH_PREFIX(3) << "Finished applying intents";

    MinRunningNotifier min_running_notifier(&context_.applier_);
    std::lock_guard<std::mutex> lock(context_.mutex_);
    context_.RemoveUnlocked(id(), RemoveReason::kLargeApplied, &min_running_notifier);
  }
}

void RunningTransaction::SetApplyOpId(const OpId& op_id) {
  apply_record_op_id_ = op_id;
}

bool RunningTransaction::ProcessingApply() const {
  return processing_apply_.load(std::memory_order_acquire);
}

const TabletId& RunningTransaction::status_tablet() const {
  return metadata_.status_tablet;
}

void RunningTransaction::UpdateTransactionStatusLocation(const TabletId& new_status_tablet) {
  metadata_.old_status_tablet = std::move(metadata_.status_tablet);
  metadata_.status_tablet = new_status_tablet;
}

void RunningTransaction::UpdateAbortCheckHT(HybridTime now, UpdateAbortCheckHTMode mode) {
  if (last_known_status_ == TransactionStatus::ABORTED ||
      last_known_status_ == TransactionStatus::COMMITTED) {
    abort_check_ht_ = HybridTime::kMax;
    return;
  }
  // When we send a status request, we schedule the transaction status to be re-checked around the
  // same time the request is supposed to time out. When we get a status response (normal case, no
  // timeout), we go back to the normal interval of re-checking the status of this transaction.
  auto delta_ms = mode == UpdateAbortCheckHTMode::kStatusRequestSent
      ? FLAGS_transaction_abort_check_timeout_ms
      : FLAGS_transaction_abort_check_interval_ms;
  abort_check_ht_ = now.AddDelta(1ms * delta_ms);
}

} // namespace tablet
} // namespace yb
