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

#include "yb/docdb/intent_aware_iterator.h"

#include <future>

#include "yb/common/doc_hybrid_time.h"
#include "yb/common/hybrid_time.h"
#include "yb/common/transaction.h"

#include "yb/docdb/docdb_fwd.h"
#include "yb/docdb/shared_lock_manager_fwd.h"
#include "yb/docdb/conflict_resolution.h"
#include "yb/docdb/doc_key.h"
#include "yb/docdb/doc_kv_util.h"
#include "yb/docdb/docdb-internal.h"
#include "yb/docdb/docdb_rocksdb_util.h"
#include "yb/docdb/intent.h"
#include "yb/docdb/intent_iterator.h"
#include "yb/docdb/key_bounds.h"
#include "yb/docdb/transaction_dump.h"
#include "yb/docdb/value.h"
#include "yb/docdb/value_type.h"

#include "yb/util/bytes_formatter.h"
#include "yb/util/debug-util.h"
#include "yb/util/logging.h"
#include "yb/util/result.h"
#include "yb/util/status_format.h"
#include "yb/util/trace.h"

using namespace std::literals;

namespace yb {
namespace docdb {

namespace {

void GetIntentPrefixForKeyWithoutHt(const Slice& key, KeyBytes* out) {
  out->Clear();
  // Since caller guarantees that key_bytes doesn't have hybrid time, we can simply use it
  // to get prefix for all related intents.
  out->AppendRawBytes(key);
}

KeyBytes GetIntentPrefixForKeyWithoutHt(const Slice& key) {
  KeyBytes result;
  GetIntentPrefixForKeyWithoutHt(key, &result);
  return result;
}

void AppendEncodedDocHt(const Slice& encoded_doc_ht, KeyBytes* key_bytes) {
  key_bytes->AppendKeyEntryType(KeyEntryType::kHybridTime);
  key_bytes->AppendRawBytes(encoded_doc_ht);
}

} // namespace

namespace {

// Given that key is well-formed DocDB encoded key, checks if it is an intent key for the same key
// as intent_prefix. If key is not well-formed DocDB encoded key, result could be true or false.
bool IsIntentForTheSameKey(const Slice& key, const Slice& intent_prefix) {
  return key.starts_with(intent_prefix) &&
         key.size() > intent_prefix.size() &&
         IntentValueType(key[intent_prefix.size()]);
}

std::string DebugDumpKeyToStr(const Slice &key) {
  return key.ToDebugString() + " (" + SubDocKey::DebugSliceToString(key) + ")";
}

std::string DebugDumpKeyToStr(const KeyBytes &key) {
  return DebugDumpKeyToStr(key.AsSlice());
}

bool DebugHasHybridTime(const Slice& subdoc_key_encoded) {
  SubDocKey subdoc_key;
  CHECK(subdoc_key.FullyDecodeFromKeyWithOptionalHybridTime(subdoc_key_encoded).ok());
  return subdoc_key.has_hybrid_time();
}

std::string EncodeHybridTime(HybridTime value) {
  return DocHybridTime(value, kMaxWriteId).EncodedInDocDbFormat();
}

} // namespace

IntentAwareIterator::IntentAwareIterator(
    const DocDB& doc_db,
    const rocksdb::ReadOptions& read_opts,
    CoarseTimePoint deadline,
    const ReadHybridTime& read_time,
    const TransactionOperationContext& txn_op_context)
    : read_time_(read_time),
      encoded_read_time_read_(EncodeHybridTime(read_time_.read)),
      encoded_read_time_local_limit_(EncodeHybridTime(read_time_.local_limit)),
      encoded_read_time_global_limit_(EncodeHybridTime(read_time_.global_limit)),
      encoded_read_time_regular_limit_(
          read_time_.local_limit > read_time_.read ? Slice(encoded_read_time_local_limit_)
                                                   : Slice(encoded_read_time_read_)),
      txn_op_context_(txn_op_context),
      transaction_status_cache_(txn_op_context_, read_time, deadline) {
  VTRACE(1, __func__);
  VLOG(4) << "IntentAwareIterator, read_time: " << read_time
          << ", txn_op_context: " << txn_op_context_;

  if (txn_op_context) {
    if (txn_op_context.txn_status_manager->MinRunningHybridTime() != HybridTime::kMax) {
      intent_iter_ = docdb::CreateRocksDBIterator(doc_db.intents,
                                                  doc_db.key_bounds,
                                                  docdb::BloomFilterMode::DONT_USE_BLOOM_FILTER,
                                                  boost::none,
                                                  rocksdb::kDefaultQueryId,
                                                  nullptr /* file_filter */,
                                                  &intent_upperbound_);
    } else {
      VLOG(4) << "No transactions running";
    }
  }
  // WARNING: Is is important for regular DB iterator to be created after intents DB iterator,
  // otherwise consistency could break, for example in following scenario:
  // 1) Transaction is T1 committed with value v1 for k1, but not yet applied to regular DB.
  // 2) Client reads v1 for k1.
  // 3) Regular DB iterator is created on a regular DB snapshot containing no values for k1.
  // 4) Transaction T1 is applied, k1->v1 is written into regular DB, intent k1->v1 is deleted.
  // 5) Intents DB iterator is created on an intents DB snapshot containing no intents for k1.
  // 6) Client reads no values for k1.
  iter_ = BoundedRocksDbIterator(doc_db.regular, read_opts, doc_db.key_bounds);
  VTRACE(2, "Created iterator");
}

void IntentAwareIterator::Seek(const DocKey &doc_key) {
  Seek(doc_key.Encode());
}

void IntentAwareIterator::Seek(const Slice& key) {
  VLOG(4) << "Seek(" << SubDocKey::DebugSliceToString(key) << ")";
  DOCDB_DEBUG_SCOPE_LOG(
      key.ToDebugString(),
      std::bind(&IntentAwareIterator::DebugDump, this));
  if (!status_.ok()) {
    return;
  }

  ROCKSDB_SEEK(&iter_, key);
  skip_future_records_needed_ = true;

  if (intent_iter_.Initialized()) {
    seek_intent_iter_needed_ = SeekIntentIterNeeded::kSeek;
    GetIntentPrefixForKeyWithoutHt(key, &seek_key_buffer_);
    AppendStrongWrite(&seek_key_buffer_);
  }
}

void IntentAwareIterator::SeekForward(const Slice& key) {
  KeyBytes key_bytes;
  // Reserve space for key plus kMaxBytesPerEncodedHybridTime + 1 bytes for SeekForward() below to
  // avoid extra realloc while appending the read time.
  key_bytes.Reserve(key.size() + kMaxBytesPerEncodedHybridTime + 1);
  key_bytes.AppendRawBytes(key);
  SeekForward(&key_bytes);
}

void IntentAwareIterator::SeekForward(KeyBytes* key_bytes) {
  VLOG(4) << "SeekForward(" << SubDocKey::DebugSliceToString(*key_bytes) << ")";
  DOCDB_DEBUG_SCOPE_LOG(
      SubDocKey::DebugSliceToString(*key_bytes),
      std::bind(&IntentAwareIterator::DebugDump, this));
  if (!status_.ok()) {
    return;
  }

  const size_t key_size = key_bytes->size();
  AppendEncodedDocHt(encoded_read_time_global_limit_, key_bytes);
  SeekForwardRegular(*key_bytes);
  key_bytes->Truncate(key_size);
  if (intent_iter_.Initialized() && status_.ok()) {
    UpdatePlannedIntentSeekForward(
        *key_bytes, StrongWriteSuffix(*key_bytes), /* use_suffix_for_prefix= */ false);
  }
}

void IntentAwareIterator::UpdatePlannedIntentSeekForward(const Slice& key,
                                                         const Slice& suffix,
                                                         bool use_suffix_for_prefix) {
  VLOG_WITH_FUNC(4)
      << "key: " << SubDocKey::DebugSliceToString(key) << ", suffix: " << suffix.ToDebugHexString()
      << ", use_suffix_for_prefix: " << use_suffix_for_prefix << ", seek_intent_iter_needed: "
      << seek_intent_iter_needed_ << ", seek_key_buffer: "
      << SubDocKey::DebugSliceToString(seek_key_buffer_.AsSlice());
  if (seek_intent_iter_needed_ != SeekIntentIterNeeded::kNoNeed &&
      seek_key_buffer_.AsSlice().GreaterOrEqual(key, suffix)) {
    return;
  }
  seek_key_buffer_.Clear();
  seek_key_buffer_.AppendRawBytes(key);
  seek_key_buffer_.AppendRawBytes(suffix);
  if (seek_intent_iter_needed_ == SeekIntentIterNeeded::kNoNeed) {
    seek_intent_iter_needed_ = SeekIntentIterNeeded::kSeekForward;
  }
  seek_key_prefix_ = seek_key_buffer_.AsSlice();
  if (!use_suffix_for_prefix) {
    seek_key_prefix_.remove_suffix(suffix.size());
  }
}

// TODO: If TTL rows are ever supported on subkeys, this may need to change appropriately.
// Otherwise, this function might seek past the TTL merge record, but not the original
// record for the actual subkey.
void IntentAwareIterator::SeekPastSubKey(const Slice& key) {
  VLOG(4) << "SeekPastSubKey(" << SubDocKey::DebugSliceToString(key) << ")";
  if (!status_.ok()) {
    return;
  }

  if (intent_iter_.Initialized()) {
    // Skip all intents for subdoc_key.
    char kSuffix = KeyEntryTypeAsChar::kGreaterThanIntentType;
    UpdatePlannedIntentSeekForward(key, Slice(&kSuffix, 1));
  }

  docdb::SeekPastSubKey(key, &iter_);
  skip_future_records_needed_ = true;
}

void IntentAwareIterator::SeekOutOfSubDoc(KeyBytes* key_bytes) {
  VLOG(4) << "SeekOutOfSubDoc(" << SubDocKey::DebugSliceToString(*key_bytes) << ")";
  if (!status_.ok()) {
    return;
  }

  docdb::SeekOutOfSubKey(key_bytes, &iter_);
  skip_future_records_needed_ = true;
  if (intent_iter_.Initialized() && status_.ok()) {
    // See comment for SubDocKey::AdvanceOutOfSubDoc.
    const char kSuffix = KeyEntryTypeAsChar::kMaxByte;
    UpdatePlannedIntentSeekForward(*key_bytes, Slice(&kSuffix, 1));
  }
}

void IntentAwareIterator::SeekOutOfSubDoc(const Slice& key) {
  KeyBytes key_bytes;
  // Reserve space for key + 1 byte for docdb::SeekOutOfSubKey() above to avoid extra realloc while
  // appending kMaxByte.
  key_bytes.Reserve(key.size() + 1);
  key_bytes.AppendRawBytes(key);
  SeekOutOfSubDoc(&key_bytes);
}

bool IntentAwareIterator::HasCurrentEntry() {
  return iter_valid_ || resolved_intent_state_ == ResolvedIntentState::kValid;
}

void IntentAwareIterator::SeekToLastDocKey() {
  iter_.SeekToLast();
  SkipFutureRecords(Direction::kBackward);
  if (intent_iter_.Initialized()) {
    ResetIntentUpperbound();
    intent_iter_.SeekToLast();
    SeekToSuitableIntent<Direction::kBackward>();
    seek_intent_iter_needed_ = SeekIntentIterNeeded::kNoNeed;
    skip_future_intents_needed_ = false;
  }
  if (HasCurrentEntry()) {
    SeekToLatestDocKeyInternal();
  }
}

template <class T>
void Assign(const T& value, T* out) {
  if (out) {
    *out = value;
  }
}

// If we reach a different key, stop seeking.
Status IntentAwareIterator::NextFullValue(
    DocHybridTime* latest_record_ht,
    Slice* result_value,
    Slice* final_key) {
  if (!latest_record_ht || !result_value)
    return STATUS(Corruption, "The arguments latest_record_ht and "
                              "result_value cannot be null pointers.");
  RETURN_NOT_OK(status_);
  Slice v;
  if (!valid() || !IsMergeRecord(v = value())) {
    auto key_data = VERIFY_RESULT(FetchKey());
    Assign(key_data.key, final_key);
    Assign(key_data.write_time, latest_record_ht);
    *result_value = v;
    return status_;
  }

  *latest_record_ht = DocHybridTime::kMin;
  const auto key_data = VERIFY_RESULT(FetchKey());
  auto key = key_data.key;
  const size_t key_size = key.size();
  bool found_record = false;

  while ((found_record = iter_.Valid()) &&  // as long as we're pointing to a record
         (key = iter_.key()).starts_with(key_data.key) &&  // with the same key we started with
         key[key_size] == KeyEntryTypeAsChar::kHybridTime && // whose key ends with a HT
         IsMergeRecord(v = iter_.value())) { // and whose value is a merge record
    iter_.Next(); // advance the iterator
  }

  if (found_record) {
    *result_value = v;
    *latest_record_ht = VERIFY_RESULT(DocHybridTime::DecodeFromEnd(&key));
    Assign(key, final_key);
  }

  found_record = false;
  if (intent_iter_.Initialized()) {
    while ((found_record = IsIntentForTheSameKey(intent_iter_.key(), key_data.key)) &&
           IsMergeRecord(v = intent_iter_.value())) {
      intent_iter_.Next();
    }
    DocHybridTime doc_ht;
    if (found_record && !(key = intent_iter_.key()).empty() &&
        (doc_ht = VERIFY_RESULT(DocHybridTime::DecodeFromEnd(&key))) >= *latest_record_ht) {
      *latest_record_ht = doc_ht;
      *result_value = v;
      Assign(key, final_key);
    }
  }

  if (*latest_record_ht == DocHybridTime::kMin) {
    iter_valid_ = false;
  }
  return status_;
}

bool IntentAwareIterator::PreparePrev(const Slice& key) {
  VLOG(4) << __func__ << "(" << SubDocKey::DebugSliceToString(key) << ")";

  ROCKSDB_SEEK(&iter_, key);

  if (iter_.Valid()) {
    iter_.Prev();
  } else {
    iter_.SeekToLast();
  }
  SkipFutureRecords(Direction::kBackward);

  if (intent_iter_.Initialized()) {
    ResetIntentUpperbound();
    ROCKSDB_SEEK(&intent_iter_, GetIntentPrefixForKeyWithoutHt(key));
    if (intent_iter_.Valid()) {
      intent_iter_.Prev();
    } else {
      intent_iter_.SeekToLast();
    }
    SeekToSuitableIntent<Direction::kBackward>();
    seek_intent_iter_needed_ = SeekIntentIterNeeded::kNoNeed;
    skip_future_intents_needed_ = false;
  }

  return HasCurrentEntry();
}

void IntentAwareIterator::PrevSubDocKey(const KeyBytes& key_bytes) {
  if (PreparePrev(key_bytes)) {
    SeekToLatestSubDocKeyInternal();
  }
}

void IntentAwareIterator::PrevDocKey(const DocKey& doc_key) {
  PrevDocKey(doc_key.Encode().AsSlice());
}

void IntentAwareIterator::PrevDocKey(const Slice& encoded_doc_key) {
  if (PreparePrev(encoded_doc_key)) {
    SeekToLatestDocKeyInternal();
  }
}

Slice IntentAwareIterator::LatestSubDocKey() {
  DCHECK(HasCurrentEntry())
      << "Expected iter_valid(" << iter_valid_ << ") || resolved_intent_state_("
      << resolved_intent_state_ << ") == ResolvedIntentState::kValid";
  return IsEntryRegular(/* descending */ true) ? iter_.key()
                                               : resolved_intent_key_prefix_.AsSlice();
}

void IntentAwareIterator::SeekToLatestSubDocKeyInternal() {
  auto subdockey_slice = LatestSubDocKey();

  // Strip the hybrid time and seek the slice.
  auto doc_ht = DocHybridTime::DecodeFromEnd(&subdockey_slice);
  if (!doc_ht.ok()) {
    status_ = doc_ht.status();
    return;
  }
  subdockey_slice.remove_suffix(1);
  Seek(subdockey_slice);
}

void IntentAwareIterator::SeekToLatestDocKeyInternal() {
  auto subdockey_slice = LatestSubDocKey();

  // Seek to the first key for row containing found subdockey.
  auto dockey_size = DocKey::EncodedSize(subdockey_slice, DocKeyPart::kWholeDocKey);
  if (!dockey_size.ok()) {
    status_ = dockey_size.status();
    return;
  }
  Seek(Slice(subdockey_slice.data(), *dockey_size));
}

void IntentAwareIterator::SeekIntentIterIfNeeded() {
  if (seek_intent_iter_needed_ == SeekIntentIterNeeded::kNoNeed || !status_.ok()) {
    return;
  }
  status_ = SetIntentUpperbound();
  if (!status_.ok()) {
    return;
  }
  switch (seek_intent_iter_needed_) {
    case SeekIntentIterNeeded::kNoNeed:
      break;
    case SeekIntentIterNeeded::kSeek:
      VLOG(4) << __func__ << ", seek: " << SubDocKey::DebugSliceToString(seek_key_buffer_);
      ROCKSDB_SEEK(&intent_iter_, seek_key_buffer_);
      SeekToSuitableIntent<Direction::kForward>();
      seek_intent_iter_needed_ = SeekIntentIterNeeded::kNoNeed;
      return;
    case SeekIntentIterNeeded::kSeekForward:
      SeekForwardToSuitableIntent();
      seek_intent_iter_needed_ = SeekIntentIterNeeded::kNoNeed;
      return;
  }
  FATAL_INVALID_ENUM_VALUE(SeekIntentIterNeeded, seek_intent_iter_needed_);
}

bool IntentAwareIterator::valid() {
  if (skip_future_records_needed_) {
    SkipFutureRecords(Direction::kForward);
  }
  SeekIntentIterIfNeeded();
  if (skip_future_intents_needed_) {
    SkipFutureIntents();
  }
  return !status_.ok() || HasCurrentEntry();
}

bool IntentAwareIterator::IsEntryRegular(bool descending) {
  if (PREDICT_FALSE(!iter_valid_)) {
    return false;
  }
  if (resolved_intent_state_ == ResolvedIntentState::kValid) {
    return (iter_.key().compare(resolved_intent_sub_doc_key_encoded_) < 0) != descending;
  }
  return true;
}

Result<FetchKeyResult> IntentAwareIterator::FetchKey() {
  RETURN_NOT_OK(status_);
  FetchKeyResult result;
  if (IsEntryRegular()) {
    result.key = iter_.key();
    result.write_time = VERIFY_RESULT(DocHybridTime::DecodeFromEnd(&result.key));
    DCHECK(result.key.ends_with(KeyEntryTypeAsChar::kHybridTime)) << result.key.ToDebugString();
    result.key.remove_suffix(1);
    result.same_transaction = false;
    max_seen_ht_.MakeAtLeast(result.write_time.hybrid_time());
  } else {
    DCHECK_EQ(ResolvedIntentState::kValid, resolved_intent_state_);
    result.key = resolved_intent_key_prefix_.AsSlice();
    result.write_time = GetIntentDocHybridTime();
    result.same_transaction = ResolvedIntentFromSameTransaction();
    max_seen_ht_.MakeAtLeast(resolved_intent_txn_dht_.hybrid_time());
  }
  VLOG(4) << "Fetched key " << SubDocKey::DebugSliceToString(result.key)
          << ", regular: " << IsEntryRegular()
          << ", with time: " << result.write_time
          << ", while read bounds are: " << read_time_;

  YB_TRANSACTION_DUMP(
      Read, txn_op_context_ ? txn_op_context_.txn_status_manager->tablet_id() : TabletId(),
      txn_op_context_ ? txn_op_context_.transaction_id : TransactionId::Nil(),
      read_time_, result.write_time, result.same_transaction,
      result.key.size(), result.key, value().size(), value());

  return result;
}

Slice IntentAwareIterator::value() {
  if (IsEntryRegular()) {
    VLOG(4) << "IntentAwareIterator::value() returning iter_.value(): "
            << iter_.value().ToDebugHexString() << " or " << FormatSliceAsStr(iter_.value());
    return iter_.value();
  } else {
    DCHECK_EQ(ResolvedIntentState::kValid, resolved_intent_state_);
    VLOG(4) << "IntentAwareIterator::value() returning resolved_intent_value_: "
            << resolved_intent_value_.AsSlice().ToDebugHexString();
    return resolved_intent_value_;
  }
}

void IntentAwareIterator::SeekForwardRegular(const Slice& slice) {
  VLOG(4) << "SeekForwardRegular(" << SubDocKey::DebugSliceToString(slice) << ")";
  docdb::SeekForward(slice, &iter_);
  skip_future_records_needed_ = true;
}

bool IntentAwareIterator::SatisfyBounds(const Slice& slice) {
  return upperbound_.empty() || slice.compare(upperbound_) <= 0;
}

void IntentAwareIterator::ProcessIntent() {
  auto decode_result = DecodeStrongWriteIntent(
      txn_op_context_, &intent_iter_, &transaction_status_cache_);
  if (!decode_result.ok()) {
    status_ = decode_result.status();
    return;
  }
  VLOG(4) << "Intent decode: " << DebugIntentKeyToString(intent_iter_.key())
      << " => " << intent_iter_.value().ToDebugHexString() << ", result: " << *decode_result;
  DOCDB_DEBUG_LOG(
      "resolved_intent_txn_dht_: $0 value_time: $1 read_time: $2",
      resolved_intent_txn_dht_.ToString(),
      decode_result->value_time.ToString(),
      read_time_.ToString());
  auto resolved_intent_time = decode_result->same_transaction ? intent_dht_from_same_txn_
                                                              : resolved_intent_txn_dht_;
  VLOG(4) << "Intent decode: " << DebugIntentKeyToString(intent_iter_.key())
          << " => " << intent_iter_.value().ToDebugHexString() << ", result: " << *decode_result
          << ", resolved_intent_time: " << resolved_intent_time.ToString();
  // If we already resolved intent that is newer that this one, we should ignore current
  // intent because we are interested in the most recent intent only.
  if (decode_result->value_time <= resolved_intent_time) {
    return;
  }

  // Ignore intent past read limit.
  if (decode_result->value_time.hybrid_time() > decode_result->MaxAllowedValueTime(read_time_)) {
    return;
  }

  if (resolved_intent_state_ == ResolvedIntentState::kNoIntent) {
    resolved_intent_key_prefix_.Reset(decode_result->intent_prefix);
    auto prefix = CurrentPrefix();
    if (!decode_result->intent_prefix.starts_with(prefix)) {
      resolved_intent_state_ = ResolvedIntentState::kInvalidPrefix;
    } else if (!SatisfyBounds(decode_result->intent_prefix)) {
      resolved_intent_state_ = ResolvedIntentState::kNoIntent;
    } else {
      resolved_intent_state_ = ResolvedIntentState::kValid;
    }
  }
  if (decode_result->same_transaction) {
    intent_dht_from_same_txn_ = decode_result->value_time;
    // We set resolved_intent_txn_dht_ to maximum possible time (time higher than read_time_.read
    // will cause read restart or will be ignored if higher than read_time_.global_limit) in
    // order to ignore intents/values from other transactions. But we save origin intent time into
    // intent_dht_from_same_txn_, so we can compare time of intents for the same key from the same
    // transaction and select the latest one.
    resolved_intent_txn_dht_ = DocHybridTime(read_time_.read, kMaxWriteId);
  } else {
    resolved_intent_txn_dht_ = decode_result->value_time;
  }
  resolved_intent_value_.Reset(decode_result->intent_value);
}

void IntentAwareIterator::UpdateResolvedIntentSubDocKeyEncoded() {
  resolved_intent_sub_doc_key_encoded_.Reset(resolved_intent_key_prefix_.AsSlice());
  resolved_intent_sub_doc_key_encoded_.AppendKeyEntryType(KeyEntryType::kHybridTime);
  resolved_intent_sub_doc_key_encoded_.AppendHybridTime(resolved_intent_txn_dht_);
  VLOG(4) << "Resolved intent SubDocKey: "
          << DebugDumpKeyToStr(resolved_intent_sub_doc_key_encoded_);
}

void IntentAwareIterator::SeekForwardToSuitableIntent() {
  VLOG(4) << __func__ << "(" << DebugDumpKeyToStr(seek_key_buffer_) << ")";

  DOCDB_DEBUG_SCOPE_LOG(seek_key_buffer_.ToString(),
                        std::bind(&IntentAwareIterator::DebugDump, this));
  if (resolved_intent_state_ != ResolvedIntentState::kNoIntent &&
      resolved_intent_key_prefix_.CompareTo(seek_key_prefix_) >= 0) {
    VLOG(4) << __func__ << ", has suitable " << AsString(resolved_intent_state_) << " intent: "
            << DebugDumpKeyToStr(resolved_intent_key_prefix_);
    return;
  }

  if (VLOG_IS_ON(4)) {
    if (resolved_intent_state_ != ResolvedIntentState::kNoIntent) {
      VLOG(4) << __func__ << ", has NOT suitable " << AsString(resolved_intent_state_)
              << " intent: " << DebugDumpKeyToStr(resolved_intent_key_prefix_);
    }

    if (intent_iter_.Valid()) {
      VLOG(4) << __func__ << ", current position: " << DebugDumpKeyToStr(intent_iter_.key());
    } else {
      VLOG(4) << __func__ << ", iterator invalid";
    }
  }

  docdb::SeekForward(seek_key_buffer_.AsSlice(), &intent_iter_);
  SeekToSuitableIntent<Direction::kForward>();
}

template<Direction direction>
void IntentAwareIterator::SeekToSuitableIntent() {
  DOCDB_DEBUG_SCOPE_LOG(/* msg */ "", std::bind(&IntentAwareIterator::DebugDump, this));
  resolved_intent_state_ = ResolvedIntentState::kNoIntent;
  resolved_intent_txn_dht_ = DocHybridTime::kMin;
  intent_dht_from_same_txn_ = DocHybridTime::kMin;
  auto prefix = CurrentPrefix();

  // Find latest suitable intent for the first SubDocKey having suitable intents.
  while (intent_iter_.Valid()) {
    auto intent_key = intent_iter_.key();
    if (intent_key[0] == KeyEntryTypeAsChar::kTransactionId) {
      // If the intent iterator ever enters the transaction metadata and reverse index region, skip
      // past it.
      switch (direction) {
        case Direction::kForward: {
          static const std::array<char, 1> kAfterTransactionId{
              KeyEntryTypeAsChar::kTransactionId + 1};
          static const Slice kAfterTxnRegion(kAfterTransactionId);
          intent_iter_.Seek(kAfterTxnRegion);
          break;
        }
        case Direction::kBackward:
          intent_upperbound_keybytes_.Clear();
          intent_upperbound_keybytes_.AppendKeyEntryType(KeyEntryType::kTransactionId);
          intent_upperbound_ = intent_upperbound_keybytes_.AsSlice();
          intent_iter_.SeekToLast();
          break;
      }
      continue;
    }
    VLOG(4) << "Intent found: " << DebugIntentKeyToString(intent_key)
            << ", resolved state: " << yb::ToString(resolved_intent_state_);
    if (resolved_intent_state_ != ResolvedIntentState::kNoIntent &&
        // Only scan intents for the first SubDocKey having suitable intents.
        !IsIntentForTheSameKey(intent_key, resolved_intent_key_prefix_)) {
      break;
    }
    if (!intent_key.starts_with(prefix) || !SatisfyBounds(intent_key)) {
      break;
    }
    ProcessIntent();
    if (!status_.ok()) {
      return;
    }
    switch (direction) {
      case Direction::kForward:
        intent_iter_.Next();
        break;
      case Direction::kBackward:
        intent_iter_.Prev();
        break;
    }
  }
  if (resolved_intent_state_ != ResolvedIntentState::kNoIntent) {
    UpdateResolvedIntentSubDocKeyEncoded();
  }
}

void IntentAwareIterator::DebugDump() {
  bool is_valid = valid();
  LOG(INFO) << ">> IntentAwareIterator dump";
  LOG(INFO) << "iter_.Valid(): " << iter_.Valid();
  if (iter_.Valid()) {
    LOG(INFO) << "iter_.key(): " << DebugDumpKeyToStr(iter_.key());
  }
  if (intent_iter_.Initialized()) {
    LOG(INFO) << "intent_iter_.Valid(): " << intent_iter_.Valid();
    if (intent_iter_.Valid()) {
      LOG(INFO) << "intent_iter_.key(): " << intent_iter_.key().ToDebugHexString();
    }
  }
  LOG(INFO) << "resolved_intent_state_: " << yb::ToString(resolved_intent_state_);
  if (resolved_intent_state_ != ResolvedIntentState::kNoIntent) {
    LOG(INFO) << "resolved_intent_sub_doc_key_encoded_: "
              << DebugDumpKeyToStr(resolved_intent_sub_doc_key_encoded_);
  }
  LOG(INFO) << "valid(): " << is_valid;
  if (valid()) {
    auto key_data = FetchKey();
    if (key_data.ok()) {
      LOG(INFO) << "key(): " << DebugDumpKeyToStr(key_data->key)
                << ", doc_ht: " << key_data->write_time;
    } else {
      LOG(INFO) << "key(): fetch failed: " << key_data.status();
    }
  }
  LOG(INFO) << "<< IntentAwareIterator dump";
}

Result<DocHybridTime>
IntentAwareIterator::FindMatchingIntentRecordDocHybridTime(const Slice& key_without_ht) {
  VLOG(4) << __func__ << "(" << SubDocKey::DebugSliceToString(key_without_ht) << ")";
  GetIntentPrefixForKeyWithoutHt(key_without_ht, &seek_key_buffer_);
  seek_key_prefix_ = seek_key_buffer_.AsSlice();

  SeekForwardToSuitableIntent();
  RETURN_NOT_OK(status_);

  if (resolved_intent_state_ != ResolvedIntentState::kValid) {
    return DocHybridTime::kInvalid;
  }

  if (resolved_intent_key_prefix_.CompareTo(seek_key_buffer_) == 0) {
    max_seen_ht_.MakeAtLeast(resolved_intent_txn_dht_.hybrid_time());
    return GetIntentDocHybridTime();
  }
  return DocHybridTime::kInvalid;
}

Result<DocHybridTime> IntentAwareIterator::GetMatchingRegularRecordDocHybridTime(
    const Slice& key_without_ht) {
  size_t other_encoded_ht_size = VERIFY_RESULT(CheckHybridTimeSizeAndValueType(iter_.key()));
  Slice iter_key_without_ht = iter_.key();
  iter_key_without_ht.remove_suffix(1 + other_encoded_ht_size);
  if (key_without_ht == iter_key_without_ht) {
    DocHybridTime doc_ht = VERIFY_RESULT(DocHybridTime::DecodeFromEnd(iter_.key()));
    max_seen_ht_.MakeAtLeast(doc_ht.hybrid_time());
    return doc_ht;
  }
  return DocHybridTime::kInvalid;
}

Result<HybridTime> IntentAwareIterator::FindOldestRecord(
    const Slice& key_without_ht, HybridTime min_hybrid_time) {
  VLOG_WITH_FUNC(4)
      << SubDocKey::DebugSliceToString(key_without_ht) << " = "
      << key_without_ht.ToDebugHexString() << ", " << min_hybrid_time;
#define DOCDB_DEBUG
  DOCDB_DEBUG_SCOPE_LOG(SubDocKey::DebugSliceToString(key_without_ht) + ", " +
                            yb::ToString(min_hybrid_time),
                        std::bind(&IntentAwareIterator::DebugDump, this));
#undef DOCDB_DEBUG
  DCHECK(!DebugHasHybridTime(key_without_ht));

  RETURN_NOT_OK(status_);
  if (!valid()) {
    VLOG_WITH_FUNC(4) << "Returning kInvalid";
    return HybridTime::kInvalid;
  }

  HybridTime result;
  if (intent_iter_.Initialized()) {
    auto intent_dht = VERIFY_RESULT(FindMatchingIntentRecordDocHybridTime(key_without_ht));
    VLOG_WITH_FUNC(4) << "Looking for Intent Record found ?  =  "
            << (intent_dht != DocHybridTime::kInvalid);
    if (intent_dht != DocHybridTime::kInvalid &&
        intent_dht.hybrid_time() > min_hybrid_time) {
      result = intent_dht.hybrid_time();
      VLOG_WITH_FUNC(4) << " oldest_record_ht is now " << result;
    }
  } else {
    VLOG_WITH_FUNC(4) << "intent_iter_ not Initialized";
  }

  seek_key_buffer_.Reserve(key_without_ht.size() +
                           kMaxBytesPerEncodedHybridTime);
  seek_key_buffer_.Reset(key_without_ht);
  seek_key_buffer_.AppendKeyEntryType(KeyEntryType::kHybridTime);
  seek_key_buffer_.AppendHybridTime(
      DocHybridTime(min_hybrid_time, kMaxWriteId));
  SeekForwardRegular(seek_key_buffer_);
  RETURN_NOT_OK(status_);
  if (iter_.Valid()) {
    iter_.Prev();
  } else {
    iter_.SeekToLast();
  }
  SkipFutureRecords(Direction::kForward);

  if (iter_valid_) {
    DocHybridTime regular_dht =
        VERIFY_RESULT(GetMatchingRegularRecordDocHybridTime(key_without_ht));
    VLOG(4) << "Looking for Matching Regular Record found   =  " << regular_dht;
    if (regular_dht != DocHybridTime::kInvalid &&
        regular_dht.hybrid_time() > min_hybrid_time) {
      result.MakeAtMost(regular_dht.hybrid_time());
    }
  } else {
    VLOG(4) << "iter_valid_ is false";
  }
  VLOG(4) << "Returning " << result;
  return result;
}

Status IntentAwareIterator::FindLatestRecord(
    const Slice& key_without_ht,
    DocHybridTime* latest_record_ht,
    Slice* result_value) {
  if (!latest_record_ht) {
    return STATUS(Corruption, "latest_record_ht should not be a null pointer");
  }
  VLOG_WITH_FUNC(4)
      << SubDocKey::DebugSliceToString(key_without_ht) << ", " << *latest_record_ht;
  DOCDB_DEBUG_SCOPE_LOG(
      SubDocKey::DebugSliceToString(key_without_ht) + ", " + yb::ToString(latest_record_ht) + ", "
      + yb::ToString(result_value),
      std::bind(&IntentAwareIterator::DebugDump, this));
  DCHECK(!DebugHasHybridTime(key_without_ht)) << SubDocKey::DebugSliceToString(key_without_ht);

  RETURN_NOT_OK(status_);
  if (!valid()) {
    return Status::OK();
  }

  bool found_later_intent_result = false;
  if (intent_iter_.Initialized()) {
    DocHybridTime dht = VERIFY_RESULT(FindMatchingIntentRecordDocHybridTime(key_without_ht));
    if (dht != DocHybridTime::kInvalid && dht > *latest_record_ht) {
      *latest_record_ht = dht;
      found_later_intent_result = true;
    }
  }

  seek_key_buffer_.Reserve(key_without_ht.size() + encoded_read_time_global_limit_.size() + 1);
  seek_key_buffer_.Reset(key_without_ht);
  AppendEncodedDocHt(encoded_read_time_global_limit_, &seek_key_buffer_);

  SeekForwardRegular(seek_key_buffer_);
  RETURN_NOT_OK(status_);
  // After SeekForwardRegular(), we need to call valid() to skip future records and see if the
  // current key still matches the pushed prefix if any. If it does not, we are done.
  if (!valid()) {
    return Status::OK();
  }

  bool found_later_regular_result = false;
  if (iter_valid_) {
    DocHybridTime dht = VERIFY_RESULT(GetMatchingRegularRecordDocHybridTime(key_without_ht));
    if (dht != DocHybridTime::kInvalid && dht > *latest_record_ht) {
      *latest_record_ht = dht;
      found_later_regular_result = true;
    }
  }

  if (result_value) {
    if (found_later_regular_result) {
      *result_value = iter_.value();
    } else if (found_later_intent_result) {
      *result_value = resolved_intent_value_;
    }
  }
  return Status::OK();
}

void IntentAwareIterator::PushPrefix(const Slice& prefix) {
  VLOG(4) << "PushPrefix: " << SubDocKey::DebugSliceToString(prefix);
  prefix_stack_.push_back(prefix);
  skip_future_records_needed_ = true;
  skip_future_intents_needed_ = true;
}

void IntentAwareIterator::PopPrefix() {
  prefix_stack_.pop_back();
  skip_future_records_needed_ = true;
  skip_future_intents_needed_ = true;
  VLOG(4) << "PopPrefix: "
          << (prefix_stack_.empty() ? std::string()
              : SubDocKey::DebugSliceToString(prefix_stack_.back()));
}

Slice IntentAwareIterator::CurrentPrefix() const {
  return prefix_stack_.empty() ? Slice() : prefix_stack_.back();
}

void IntentAwareIterator::SkipFutureRecords(const Direction direction) {
  skip_future_records_needed_ = false;
  if (!status_.ok()) {
    return;
  }
  auto prefix = CurrentPrefix();
  while (iter_.Valid()) {
    Slice encoded_doc_ht = iter_.key();
    if (!encoded_doc_ht.starts_with(prefix)) {
      VLOG(4) << "Unmatched prefix: " << SubDocKey::DebugSliceToString(iter_.key())
              << ", prefix: " << SubDocKey::DebugSliceToString(prefix);
      iter_valid_ = false;
      return;
    }
    if (!SatisfyBounds(encoded_doc_ht)) {
      VLOG(4) << "Out of bounds: " << SubDocKey::DebugSliceToString(iter_.key())
              << ", upperbound: " << SubDocKey::DebugSliceToString(upperbound_);
      iter_valid_ = false;
      return;
    }
    if (encoded_doc_ht.TryConsumeByte(KeyEntryTypeAsChar::kTransactionApplyState)) {
      if (!NextRegular(direction)) {
        return;
      }
      continue;
    }
    auto doc_ht_size = DocHybridTime::GetEncodedSize(encoded_doc_ht);
    if (!doc_ht_size.ok()) {
      LOG(ERROR) << "Decode doc ht from key failed: " << doc_ht_size.status()
                 << ", key: " << iter_.key().ToDebugHexString();
      status_ = doc_ht_size.status();
      return;
    }
    encoded_doc_ht.remove_prefix(encoded_doc_ht.size() - *doc_ht_size);
    auto value = iter_.value();
    auto value_type = DecodeKeyEntryType(value);
    VLOG(4) << "Checking for skip, type " << value_type << ", encoded_doc_ht: "
            << DocHybridTime::DebugSliceToString(encoded_doc_ht)
            << " value: " << value.ToDebugHexString() << ", current key: "
            << SubDocKey::DebugSliceToString(iter_.key());
    if (value_type == KeyEntryType::kHybridTime) {
      // Value came from a transaction, we could try to filter it by original intent time.
      Slice encoded_intent_doc_ht = value;
      encoded_intent_doc_ht.consume_byte();
      // The logic here replicates part of the logic in
      // DecodeStrongWriteIntentResult:: MaxAllowedValueTime for intents that have been committed
      // and applied to regular RocksDB only. Note that here we are comparing encoded hybrid times,
      // so comparisons are reversed vs. the un-encoded case. If a value is found "invalid", it
      // can't cause a read restart. If it is found "valid", it will cause a read restart if it is
      // greater than read_time.read. That last comparison is done outside this function.
      Slice max_allowed = encoded_intent_doc_ht.compare(encoded_read_time_local_limit_) > 0
          ? Slice(encoded_read_time_global_limit_)
          : Slice(encoded_read_time_read_);
      if (encoded_doc_ht.compare(max_allowed) > 0) {
        iter_valid_ = true;
        return;
      }
    } else if (encoded_doc_ht.compare(encoded_read_time_regular_limit_) > 0) {
      // If a value does not contain the hybrid time of the intent that wrote the original
      // transaction, then it either (a) originated from a single-shard transaction or (b) the
      // intent hybrid time has already been garbage-collected during a compaction because the
      // corresponding transaction's commit time (stored in the key) became lower than the history
      // cutoff. See the following commit for the details of this intent hybrid time GC.
      //
      // https://github.com/yugabyte/yugabyte-db/commit/26260e0143e521e219d93f4aba6310fcc030a628
      //
      // encoded_read_time_regular_limit_ is simply the encoded value of max(read_ht, local_limit).
      // The above condition
      //
      //   encoded_doc_ht.compare(encoded_read_time_regular_limit_) >= 0
      //
      // corresponds to the following in terms of decoded hybrid times (order is reversed):
      //
      //   commit_ht <= max(read_ht, local_limit)
      //
      // and the inverse of that can be written as
      //
      //   commit_ht > read_ht && commit_ht > local_limit
      //
      // The reason this is correct here is that in case (a) the event of writing a single-shard
      // record to the tablet would certainly be after our read transaction's start time in case
      // commit_ht > local_limit, so it can never cause a read restart. In case (b) we know that
      // commit_ht < history_cutoff and read_ht >= history_cutoff (by definition of history cutoff)
      // so commit_ht < read_ht, and in this case read restart is impossible regardless of the
      // value of local_limit.
      iter_valid_ = true;
      return;
    }
    VLOG(4) << "Skipping because of time: " << SubDocKey::DebugSliceToString(iter_.key())
            << ", read time: " << read_time_;
    if (!NextRegular(direction)) {
      return;
    }
  }
  iter_valid_ = false;
}

bool IntentAwareIterator::NextRegular(Direction direction) {
  switch (direction) {
    case Direction::kForward:
      iter_.Next(); // TODO(dtxn) use seek with the same key, but read limit as doc hybrid time.
      return true;
    case Direction::kBackward:
      iter_.Prev();
      return true;
  }

  status_ = STATUS_FORMAT(Corruption, "Unexpected direction: $0", direction);
  LOG(ERROR) << status_;
  iter_valid_ = false;
  return false;
}

void IntentAwareIterator::SkipFutureIntents() {
  skip_future_intents_needed_ = false;
  if (!intent_iter_.Initialized() || !status_.ok()) {
    return;
  }
  auto prefix = CurrentPrefix();
  if (resolved_intent_state_ != ResolvedIntentState::kNoIntent) {
    auto compare_result = resolved_intent_key_prefix_.AsSlice().compare_prefix(prefix);
    VLOG(4) << "Checking resolved intent subdockey: "
            << DebugDumpKeyToStr(resolved_intent_key_prefix_)
            << ", against new prefix: " << DebugDumpKeyToStr(prefix) << ": "
            << compare_result;
    if (compare_result == 0) {
      if (!SatisfyBounds(resolved_intent_key_prefix_.AsSlice())) {
        resolved_intent_state_ = ResolvedIntentState::kNoIntent;
      } else {
        resolved_intent_state_ = ResolvedIntentState::kValid;
      }
      return;
    } else if (compare_result > 0) {
      resolved_intent_state_ = ResolvedIntentState::kInvalidPrefix;
      return;
    }
  }
  SeekToSuitableIntent<Direction::kForward>();
}

Status IntentAwareIterator::SetIntentUpperbound() {
  if (iter_.Valid()) {
    intent_upperbound_keybytes_.Clear();
    // Strip ValueType::kHybridTime + DocHybridTime at the end of SubDocKey in iter_ and append
    // to upperbound with 0xff.
    Slice subdoc_key = iter_.key();
    size_t doc_ht_size = VERIFY_RESULT(DocHybridTime::GetEncodedSize(subdoc_key));
    subdoc_key.remove_suffix(1 + doc_ht_size);
    intent_upperbound_keybytes_.AppendRawBytes(subdoc_key);
    VLOG(4) << "SetIntentUpperbound = "
            << SubDocKey::DebugSliceToString(intent_upperbound_keybytes_.AsSlice()) << "/"
            << intent_upperbound_keybytes_.AsSlice().ToDebugHexString();
    intent_upperbound_keybytes_.AppendKeyEntryType(KeyEntryType::kMaxByte);
    intent_upperbound_ = intent_upperbound_keybytes_.AsSlice();
    intent_iter_.RevalidateAfterUpperBoundChange();
  } else {
    // In case the current position of the regular iterator is invalid, set the exclusive intent
    // upperbound high to be able to find all intents higher than the last regular record.
    ResetIntentUpperbound();
  }
  return Status::OK();
}

void IntentAwareIterator::ResetIntentUpperbound() {
  intent_upperbound_keybytes_.Clear();
  intent_upperbound_keybytes_.AppendKeyEntryType(KeyEntryType::kHighest);
  intent_upperbound_ = intent_upperbound_keybytes_.AsSlice();
  intent_iter_.RevalidateAfterUpperBoundChange();
  VLOG(4) << "ResetIntentUpperbound = " << intent_upperbound_.ToDebugString();
}

std::string IntentAwareIterator::DebugPosToString() {
  if (!valid()) {
    return "<INVALID>";
  }
  auto key = FetchKey();
  if (!key.ok()) {
    return key.status().ToString();
  }
  return SubDocKey::DebugSliceToString(key->key);
}

}  // namespace docdb
}  // namespace yb
