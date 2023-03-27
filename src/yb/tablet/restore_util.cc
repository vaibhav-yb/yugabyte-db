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
#include "yb/tablet/restore_util.h"

#include "yb/docdb/docdb.messages.h"
#include "yb/docdb/doc_read_context.h"

#include "yb/rpc/lightweight_message.h"

#include "yb/tablet/tablet_metadata.h"

#include "yb/util/logging.h"
namespace yb {

Status FetchState::SetPrefix(const Slice& prefix) {
  if (prefix_.empty()) {
    iterator_->Seek(prefix);
  } else {
    iterator_->SeekForward(prefix);
  }
  prefix_ = prefix;
  finished_ = false;
  key_write_stack_.clear();
  num_rows_ = 0;
  return Next(MoveForward::kFalse);
}

Result<bool> FetchState::Update() {
  if (!iterator_->valid()) {
    finished_ = true;
    return true;
  }
  key_ = VERIFY_RESULT(iterator_->FetchKey());
  auto rest_of_key = key_.key;
  if (!rest_of_key.starts_with(prefix_)) {
    finished_ = true;
    return true;
  }

  rest_of_key.remove_prefix(prefix_.size());
  for (auto i = key_write_stack_.begin(); i != key_write_stack_.end(); ++i) {
    if (!rest_of_key.starts_with(i->key.AsSlice())) {
      key_write_stack_.erase(i, key_write_stack_.end());
      break;
    }
    if (i->time > key_.write_time) {
      // This key-value entry is outdated and we should pick the next one.
      return false;
    }
    rest_of_key.remove_prefix(i->key.size());
  }

  auto alive_row = !VERIFY_RESULT(docdb::Value::IsTombstoned(value()));
  if (key_write_stack_.empty()) {
    // Empty stack means new row, i.e. doc key. So rest_of_key is not updated and matches
    // full key, that contains doc key.
    if (alive_row) {
      ++num_rows_;
    }
    auto doc_key_size = VERIFY_RESULT(
        docdb::DocKey::EncodedHashPartAndDocKeySizes(rest_of_key)).doc_key_size;
    key_write_stack_.push_back(KeyWriteEntry {
      .key = KeyBuffer(rest_of_key.Prefix(doc_key_size)),
      // If doc key does not have its own write time, then we use min time to avoid ignoring
      // updates for other columns.
      .time = doc_key_size == rest_of_key.size() ? key_.write_time : DocHybridTime::kMin,
    });
    rest_of_key.remove_prefix(doc_key_size);
  }

  // See comment for key_write_stack_ field.
  if (!rest_of_key.empty()) {
    // If we have multiple subkeys in rest_of_key, it is NOT necessary to split them.
    // Since complete subkey cannot be prefix of another subkey.
    key_write_stack_.push_back(KeyWriteEntry {
      .key = KeyBuffer(rest_of_key),
      .time = key_.write_time,
    });
  }

  return alive_row;
}

Status FetchState::Next(MoveForward move_forward) {
  while (!finished()) {
    if (move_forward) {
      iterator_->SeekPastSubKey(key_.key);
    } else {
      move_forward = MoveForward::kTrue;
    }
    if (VERIFY_RESULT(Update())) {
      break;
    }
  }
  return Status::OK();
}

Status RestorePatch::ProcessCommonEntry(
    const Slice& key, const Slice& existing_value, const Slice& restoring_value) {
  VLOG_WITH_FUNC(3) << "Key: " << key.ToDebugHexString() << ", existing value: "
                    << existing_value.ToDebugHexString() << ", restoring value: "
                    << restoring_value.ToDebugHexString();
  if (restoring_value.compare(existing_value)) {
    IncrementTicker(RestoreTicker::kUpdates);
    AddKeyValue(key, restoring_value, doc_batch_);
  }
  // If this is a packed row, update the state.
  return TryUpdateLastPackedRow(key, restoring_value);
}

Status RestorePatch::ProcessRestoringOnlyEntry(
    const Slice& restoring_key, const Slice& restoring_value) {
  VLOG_WITH_FUNC(3) << "Restoring key: " << restoring_key.ToDebugHexString() << ", "
                    << "restoring value: " << restoring_value.ToDebugHexString();
  IncrementTicker(RestoreTicker::kInserts);
  AddKeyValue(restoring_key, restoring_value, doc_batch_);
  // If this is a packed row, update the state.
  return TryUpdateLastPackedRow(restoring_key, restoring_value);
}

Status RestorePatch::ProcessExistingOnlyEntry(
    const Slice& existing_key, const Slice& existing_value) {
  VLOG_WITH_FUNC(3) << "Existing key: " << existing_key.ToDebugHexString() << ", "
                    << "existing value: " << existing_value.ToDebugHexString() << ", "
                    << "last packed row key: "
                    << last_packed_row_restoring_state_.key.AsSlice().ToDebugHexString()
                    << ", last packed row value: "
                    << last_packed_row_restoring_state_.value.AsSlice().ToDebugHexString();
  if (!last_packed_row_restoring_state_.key.empty() &&
      existing_key.starts_with(last_packed_row_restoring_state_.key.AsSlice())) {
    // Find out this column's value from the packed row.
    Slice subkey = existing_key.WithoutPrefix(last_packed_row_restoring_state_.key.size());
    if (!subkey.empty()) {
      char type = subkey.consume_byte();
      if (docdb::IsColumnId(static_cast<docdb::KeyEntryType>(type))) {
        Slice packed_value = last_packed_row_restoring_state_.value.AsSlice();
        const docdb::SchemaPacking& packing = VERIFY_RESULT(
            table_info_->doc_read_context->schema_packing_storage.GetPacking(&packed_value));
        int64_t column_id_as_int64 = VERIFY_RESULT(util::FastDecodeSignedVarIntUnsafe(&subkey));
        // Expect only one subkey.
        SCHECK_EQ(subkey.empty(), true, Corruption, "Only one subkey expected");
        ColumnId column_id;
        RETURN_NOT_OK(ColumnId::FromInt64(column_id_as_int64, &column_id));
        auto value = packing.GetValue(column_id, packed_value);
        // Insert this column's packed row value.
        if (value) {
          VLOG_WITH_FUNC(1) << "Inserting key: " << existing_key.ToDebugHexString()
                            << ", value: " << (*value).ToDebugHexString();
          AddKeyValue(existing_key, *value, doc_batch_);
          IncrementTicker(RestoreTicker::kInserts);
          return Status::OK();
        }
      }
    }
  }
  // Otherwise delete this kv.
  char tombstone_char = docdb::ValueEntryTypeAsChar::kTombstone;
  Slice tombstone(&tombstone_char, 1);
  IncrementTicker(RestoreTicker::kDeletes);
  AddKeyValue(existing_key, tombstone, doc_batch_);
  return Status::OK();
}

Status RestorePatch::PatchCurrentStateFromRestoringState() {
  while (restoring_state_ && existing_state_ && !restoring_state_->finished() &&
         !existing_state_->finished()) {
    if (VERIFY_RESULT(ShouldSkipEntry(restoring_state_->key(), restoring_state_->value()))) {
      RETURN_NOT_OK(restoring_state_->Next());
      continue;
    }
    if (VERIFY_RESULT(ShouldSkipEntry(existing_state_->key(), existing_state_->value()))) {
      RETURN_NOT_OK(existing_state_->Next());
      continue;
    }
    auto compare_result = restoring_state_->key().compare(existing_state_->key());
    if (compare_result == 0) {
      RETURN_NOT_OK(ProcessCommonEntry(
          existing_state_->key(), existing_state_->value(), restoring_state_->value()));
      RETURN_NOT_OK(restoring_state_->Next());
      RETURN_NOT_OK(existing_state_->Next());
    } else if (compare_result < 0) {
      RETURN_NOT_OK(ProcessRestoringOnlyEntry(
          restoring_state_->key(), restoring_state_->value()));
      RETURN_NOT_OK(restoring_state_->Next());
    } else {
      RETURN_NOT_OK(ProcessExistingOnlyEntry(
          existing_state_->key(), existing_state_->value()));
      RETURN_NOT_OK(existing_state_->Next());
    }
  }

  while (restoring_state_ && !restoring_state_->finished()) {
    if (VERIFY_RESULT(ShouldSkipEntry(restoring_state_->key(), restoring_state_->value()))) {
      RETURN_NOT_OK(restoring_state_->Next());
      continue;
    }
    RETURN_NOT_OK(ProcessRestoringOnlyEntry(
        restoring_state_->key(), restoring_state_->value()));
    RETURN_NOT_OK(restoring_state_->Next());
  }

  while (existing_state_ && !existing_state_->finished()) {
    if (VERIFY_RESULT(ShouldSkipEntry(existing_state_->key(), existing_state_->value()))) {
      RETURN_NOT_OK(existing_state_->Next());
      continue;
    }
    RETURN_NOT_OK(ProcessExistingOnlyEntry(
        existing_state_->key(), existing_state_->value()));
    RETURN_NOT_OK(existing_state_->Next());
  }

  return Status::OK();
}

Status RestorePatch::TryUpdateLastPackedRow(const Slice& key, const Slice& value) {
  VLOG_WITH_FUNC(3) << "Key: " << key.ToDebugHexString()
                    << ", value: " << value.ToDebugHexString();
  auto value_slice = value;
  RETURN_NOT_OK(docdb::ValueControlFields::Decode(&value_slice));
  if (value_slice.TryConsumeByte(docdb::ValueEntryTypeAsChar::kPackedRow)) {
    VLOG_WITH_FUNC(2) << "Packed row encountered in the restoring state. Key: "
                      << key.ToDebugHexString() << ", value: " << value.ToDebugHexString();
    last_packed_row_restoring_state_.key = key;
    last_packed_row_restoring_state_.value = value_slice;
  }
  return Status::OK();
}

void AddKeyValue(const Slice& key, const Slice& value, docdb::DocWriteBatch* write_batch) {
  auto& pair = write_batch->AddRaw();
  pair.key.assign(key.cdata(), key.size());
  pair.value.assign(value.cdata(), value.size());
}

void WriteToRocksDB(
    docdb::DocWriteBatch* write_batch, const HybridTime& write_time, const OpId& op_id,
    tablet::Tablet* tablet, const std::optional<docdb::KeyValuePairPB>& restore_kv) {
  auto kv_write_batch = rpc::MakeSharedMessage<docdb::LWKeyValueWriteBatchPB>();
  write_batch->MoveToWriteBatchPB(kv_write_batch.get());

  // Append restore entry to the write batch.
  if (restore_kv) {
    kv_write_batch->add_write_pairs()->CopyFrom(*restore_kv);
  }

  docdb::NonTransactionalWriter writer(*kv_write_batch, write_time);
  rocksdb::WriteBatch rocksdb_write_batch;
  rocksdb_write_batch.SetDirectWriter(&writer);
  docdb::ConsensusFrontiers frontiers;
  set_op_id(op_id, &frontiers);
  set_hybrid_time(write_time, &frontiers);

  tablet->WriteToRocksDB(
      &frontiers, &rocksdb_write_batch, docdb::StorageDbType::kRegular);
}
} // namespace yb
