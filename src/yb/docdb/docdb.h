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

#include <cstdint>
#include <ostream>
#include <string>
#include <vector>

#include <boost/function.hpp>

#include "yb/common/doc_hybrid_time.h"
#include "yb/common/hybrid_time.h"
#include "yb/common/read_hybrid_time.h"
#include "yb/common/transaction.h"

#include "yb/docdb/docdb_fwd.h"
#include "yb/docdb/shared_lock_manager_fwd.h"
#include "yb/docdb/doc_path.h"
#include "yb/docdb/doc_write_batch.h"
#include "yb/docdb/docdb.pb.h"
#include "yb/docdb/docdb_types.h"
#include "yb/docdb/lock_batch.h"
#include "yb/docdb/subdocument.h"
#include "yb/docdb/value.h"

#include "yb/rocksdb/rocksdb_fwd.h"

#include "yb/util/memory/arena_list.h"
#include "yb/util/result.h"
#include "yb/util/strongly_typed_bool.h"

// DocDB mapping on top of the key-value map in RocksDB:
// <document_key> <hybrid_time> -> <doc_type>
// <document_key> <hybrid_time> <key_a> <gen_ts_a> -> <subdoc_a_type_or_value>
//
// Assuming the type of subdocument corresponding to key_a in the above example is "object", the
// contents of that subdocument are stored in a similar way:
// <document_key> <hybrid_time> <key_a> <gen_ts_a> <key_aa> <gen_ts_aa> -> <subdoc_aa_type_or_value>
// <document_key> <hybrid_time> <key_a> <gen_ts_a> <key_ab> <gen_ts_ab> -> <subdoc_ab_type_or_value>
// ...
//
// See doc_key.h for the encoding of the <document_key> part.
//
// <key_a>, <key_aa> are subkeys indicating a path inside a document.
// Their encoding is as follows:
//   <value_type> -- one byte, see the ValueType enum.
//   <value_specific_encoding> -- e.g. a big-endian 8-byte integer, or a string in a "zero encoded"
//                                format. This is empty for null or true/false values.
//
// <hybrid_time>, <gen_ts_a>, <gen_ts_ab> are "generation hybrid_times" corresponding to hybrid
// clock hybrid_times of the last time a particular top-level document / subdocument was fully
// overwritten or deleted.
//
// <subdoc_a_type_or_value>, <subdoc_aa_type_or_value>, <subdoc_ab_type_or_value> are values of the
// following form:
//   - One-byte value type (see the ValueType enum).
//   - For primitive values, the encoded value. Note: the value encoding may be different from the
//     key encoding for the same data type. E.g. we only flip the sign bit for signed 64-bit
//     integers when encoded as part of a RocksDB key, not value.
//
// Also see this document for a high-level overview of how we lay out JSON documents on top of
// RocksDB:
// https://docs.google.com/document/d/1uEOHUqGBVkijw_CGD568FMt8UOJdHtiE3JROUOppYBU/edit

namespace yb {

class Histogram;

namespace docdb {

class DocOperation;

// This function prepares the transaction by taking locks. The set of keys locked are returned to
// the caller via the keys_locked argument (because they need to be saved and unlocked when the
// transaction commits). A flag is also returned to indicate if any of the write operations
// requires a clean read snapshot to be taken before being applied (see DocOperation for details).
//
// Example: doc_write_ops might consist of the following operations:
// a.b = {}, a.b.c = 1, a.b.d = 2, e.d = 3
// We will generate all the lock_prefixes for the keys with lock types
// a - shared, a.b - exclusive, a - shared, a.b - shared, a.b.c - exclusive ...
// Then we will deduplicate the keys and promote shared locks to exclusive, and sort them.
// Finally, the locks taken will be in order:
// a - shared, a.b - exclusive, a.b.c - exclusive, a.b.d - exclusive, e - shared, e.d - exclusive.
// Then the sorted lock key list will be returned. (Type is not returned because it is not needed
// for unlocking)
// TODO(akashnil): If a.b is exclusive, we don't need to lock any sub-paths under it.
//
// Input: doc_write_ops
// Context: lock_manager

struct PrepareDocWriteOperationResult {
  LockBatch lock_batch;
  bool need_read_snapshot = false;
};

Result<PrepareDocWriteOperationResult> PrepareDocWriteOperation(
    const std::vector<std::unique_ptr<DocOperation>>& doc_write_ops,
    const ArenaList<LWKeyValuePairPB>& read_pairs,
    const scoped_refptr<Histogram>& write_lock_latency,
    const IsolationLevel isolation_level,
    const OperationKind operation_kind,
    const RowMarkType row_mark_type,
    bool transactional_table,
    bool write_transaction_metadata,
    CoarseTimePoint deadline,
    PartialRangeKeyIntents partial_range_key_intents,
    SharedLockManager *lock_manager);

// This constructs a DocWriteBatch using the given list of DocOperations, reading the previous
// state of data from RocksDB when necessary.
//
// Input: doc_write_ops, read snapshot hybrid_time if requested in PrepareDocWriteOperation().
// Context: rocksdb
// Outputs: keys_locked, write_batch
Status AssembleDocWriteBatch(
    const std::vector<std::unique_ptr<DocOperation>>& doc_write_ops,
    CoarseTimePoint deadline,
    const ReadHybridTime& read_time,
    const DocDB& doc_db,
    LWKeyValueWriteBatchPB* write_batch,
    InitMarkerBehavior init_marker_behavior,
    std::atomic<int64_t>* monotonic_counter,
    HybridTime* restart_read_ht,
    const std::string& table_name);

struct ExternalTxnApplyStateData {
  HybridTime commit_ht;
  IntraTxnWriteId write_id = 0;

  std::string ToString() const {
    return YB_STRUCT_TO_STRING(commit_ht, write_id);
  }
};

using ExternalTxnApplyState = std::map<TransactionId, ExternalTxnApplyStateData>;

class ExternalTxnIntentsState {
 public:
  IntraTxnWriteId GetWriteIdAndIncrement(const TransactionId& txn_id);
  void EraseEntry(const TransactionId& txn_id);
 private:
  std::mutex mutex_;
  std::unordered_map<TransactionId, IntraTxnWriteId, TransactionIdHash> map_;
};
// Adds external pair to write batch.
// Returns true if add was skipped because pair is a regular (non external) record.
bool AddExternalPairToWriteBatch(
    const LWKeyValuePairPB& kv_pair,
    HybridTime hybrid_time,
    ExternalTxnApplyState* apply_external_transactions,
    rocksdb::WriteBatch* regular_write_batch,
    rocksdb::WriteBatch* intents_write_batch,
    ExternalTxnIntentsState* external_txn_intents_state);

// Prepares external part of non transaction write batch.
// Batch could contain intents for external transactions, in this case those intents
// will be added to intents_write_batch.
//
// Returns true if batch contains regular entries.
bool PrepareExternalWriteBatch(
    const LWKeyValueWriteBatchPB& put_batch,
    HybridTime hybrid_time,
    rocksdb::DB* intents_db,
    rocksdb::WriteBatch* regular_write_batch,
    rocksdb::WriteBatch* intents_write_batch,
    ExternalTxnIntentsState* external_txn_intents_state);

YB_STRONGLY_TYPED_BOOL(LastKey);

// Enumerates intents corresponding to provided key value pairs.
// For each key it generates a strong intent and for each parent of each it generates a weak one.
// functor should accept 3 arguments:
// intent_kind - kind of intent weak or strong
// value_slice - value of intent
// key - pointer to key in format of SubDocKey (no ht)
// last_key - whether it is last strong key in enumeration

// Indicates that the intent contains a full document key, i.e. it does not omit any final range
// components of the document key. This flag is also true for intents that include subdocument keys.
YB_STRONGLY_TYPED_BOOL(FullDocKey);

// TODO(dtxn) don't expose this method outside of DocDB if TransactionConflictResolver is moved
// inside DocDB.
// Note: From https://stackoverflow.com/a/17278470/461529:
// "As of GCC 4.8.1, the std::function in libstdc++ optimizes only for pointers to functions and
// methods. So regardless the size of your functor (lambdas included), initializing a std::function
// from it triggers heap allocation."
// So, we use boost::function which doesn't have such issue:
// http://www.boost.org/doc/libs/1_65_1/doc/html/function/misc.html
typedef boost::function<
    Status(IntentStrength, FullDocKey, Slice, KeyBytes*, LastKey)> EnumerateIntentsCallback;

Status EnumerateIntents(
    const ArenaList<docdb::LWKeyValuePairPB>& kv_pairs,
    const EnumerateIntentsCallback& functor, PartialRangeKeyIntents partial_range_key_intents);

Status EnumerateIntents(
    Slice key, const Slice& intent_value, const EnumerateIntentsCallback& functor,
    KeyBytes* encoded_key_buffer, PartialRangeKeyIntents partial_range_key_intents,
    LastKey last_key = LastKey::kFalse);

// replicated_batches_state format does not matter at this point, because it is just
// appended to appropriate value.
void PrepareTransactionWriteBatch(
    const docdb::KeyValueWriteBatchPB& put_batch,
    HybridTime hybrid_time,
    rocksdb::WriteBatch* rocksdb_write_batch,
    const TransactionId& transaction_id,
    IsolationLevel isolation_level,
    PartialRangeKeyIntents partial_range_key_intents,
    const Slice& replicated_batches_state,
    IntraTxnWriteId* write_id);


struct IntentKeyValueForCDC {
  Slice key;
  Slice value;
  Slice ht;
  std::string key_buf, value_buf, ht_buf;
  std::string reverse_index_key;
  DocHybridTime intent_ht;
  IntraTxnWriteId write_id = 0;

  std::string ToString() const;

  template <class PB>
  void ToPB(PB* pb) const {
    pb->set_key(key);
    pb->set_value(value);
    pb->set_reverse_index_key(reverse_index_key);
    pb->set_write_id(write_id);
  }

  template <class PB>
  static IntentKeyValueForCDC FromPB(const PB& pb) {
    return IntentKeyValueForCDC {
        .key = pb.key(),
        .value = pb.value(),
        .reverse_index_key = pb.reverse_index_key(),
        .write_id = pb.write_id(),
    };
  }
};

// See ApplyTransactionStatePB for details.
struct ApplyTransactionState {
  std::string key;
  IntraTxnWriteId write_id = 0;
  AbortedSubTransactionSet aborted;

  bool active() const {
    return !key.empty();
  }

  std::string ToString() const;

  template <class PB>
  void ToPB(PB* pb) const {
    pb->set_key(key);
    pb->set_write_id(write_id);
    aborted.ToPB(pb->mutable_aborted()->mutable_set());
  }

  template <class PB>
  static Result<ApplyTransactionState> FromPB(const PB& pb) {
    return ApplyTransactionState {
      .key = pb.key(),
      .write_id = pb.write_id(),
      .aborted = VERIFY_RESULT(AbortedSubTransactionSet::FromPB(pb.aborted().set())),
    };
  }
};

Result<ApplyTransactionState> GetIntentsBatch(
    const TransactionId& transaction_id,
    const KeyBounds* key_bounds,
    const ApplyTransactionState* stream_state,
    rocksdb::DB* intents_db,
    std::vector<IntentKeyValueForCDC>* keyValueIntents);

void AppendTransactionKeyPrefix(const TransactionId& transaction_id, docdb::KeyBytes* out);

// Class that is used while combining external intents into single key value pair.
class ExternalIntentsProvider {
 public:
  // Set output key.
  virtual void SetKey(const Slice& slice) = 0;

  // Set output value.
  virtual void SetValue(const Slice& slice) = 0;

  // Get next external intent, returns false when there are no more intents.
  virtual boost::optional<std::pair<Slice, Slice>> Next() = 0;

  virtual const Uuid& InvolvedTablet() = 0;

  virtual ~ExternalIntentsProvider() = default;
};

// Combine external intents into single key value pair.
void CombineExternalIntents(
    const TransactionId& txn_id,
    ExternalIntentsProvider* provider);

}  // namespace docdb
}  // namespace yb
