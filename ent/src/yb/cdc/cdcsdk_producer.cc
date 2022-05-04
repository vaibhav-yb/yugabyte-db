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

#include "yb/cdc/cdc_producer.h"

#include "yb/cdc/cdc_common_util.h"

#include "yb/common/wire_protocol.h"
#include "yb/common/ql_expr.h"

#include "yb/docdb/docdb_util.h"
#include "yb/docdb/doc_key.h"

#include "yb/util/flag_tags.h"

DEFINE_int32(cdc_snapshot_batch_size, 250, "Batch size for the snapshot operation in CDC");
TAG_FLAG(cdc_snapshot_batch_size, runtime);

DEFINE_bool(stream_truncate_record, false, "Enable streaming of TRUNCATE record");
TAG_FLAG(stream_truncate_record, runtime);

namespace yb {
namespace cdc {

using consensus::ReplicateMsgPtr;
using consensus::ReplicateMsgs;
using docdb::PrimitiveValue;
using tablet::TransactionParticipant;
using yb::QLTableRow;

YB_DEFINE_ENUM(OpType, (INSERT)(UPDATE)(DELETE));

void SetOperation(RowMessage* row_message, OpType type, const Schema& schema) {
  switch (type) {
    case OpType::INSERT:
      row_message->set_op(RowMessage_Op_INSERT);
      break;
    case OpType::UPDATE:
      row_message->set_op(RowMessage_Op_UPDATE);
      break;
    case OpType::DELETE:
      row_message->set_op(RowMessage_Op_DELETE);
      break;
  }

  row_message->set_pgschema_name(schema.SchemaName());
}

template <class Value>
void AddColumnToMap(
    const std::shared_ptr<tablet::TabletPeer>& tablet_peer,
    const ColumnSchema& col_schema,
    const Value& col,
    DatumMessagePB* cdc_datum_message) {
  cdc_datum_message->set_column_name(col_schema.name());
  QLValuePB ql_value;
  if (tablet_peer->tablet()->table_type() == PGSQL_TABLE_TYPE) {
    col.ToQLValuePB(col_schema.type(), &ql_value);
    if (!IsNull(ql_value) && col_schema.pg_type_oid() != 0 /*kInvalidOid*/) {
      docdb::SetValueFromQLBinaryWrapper(ql_value, col_schema.pg_type_oid(), cdc_datum_message);
    } else {
      cdc_datum_message->set_column_type(col_schema.pg_type_oid());
    }
  }
}

DatumMessagePB* AddTuple(RowMessage* row_message) {
  if (!row_message) {
    return nullptr;
  }
  DatumMessagePB* tuple = nullptr;

  if (row_message->op() == RowMessage_Op_DELETE) {
    tuple = row_message->add_old_tuple();
    row_message->add_new_tuple();
  } else {
    tuple = row_message->add_new_tuple();
    row_message->add_old_tuple();
  }
  return tuple;
}

void AddPrimaryKey(
    const std::shared_ptr<tablet::TabletPeer>& tablet_peer, const docdb::SubDocKey& decoded_key,
    const Schema& tablet_schema, RowMessage* row_message) {
  size_t i = 0;
  for (const auto& col : decoded_key.doc_key().hashed_group()) {
    DatumMessagePB* tuple = AddTuple(row_message);

    AddColumnToMap(
        tablet_peer, tablet_schema.column(i), col, tuple);
    i++;
  }

  for (const auto& col : decoded_key.doc_key().range_group()) {
    DatumMessagePB* tuple = AddTuple(row_message);

    AddColumnToMap(
        tablet_peer, tablet_schema.column(i), col, tuple);
    i++;
  }
}

void SetCDCSDKOpId(
    int64_t term, int64_t index, uint32_t write_id, const std::string& key,
    CDCSDKOpIdPB* cdc_sdk_op_id_pb) {
  cdc_sdk_op_id_pb->set_term(term);
  cdc_sdk_op_id_pb->set_index(index);
  cdc_sdk_op_id_pb->set_write_id(write_id);
  cdc_sdk_op_id_pb->set_write_id_key(key);
}

void SetCheckpoint(
    int64_t term, int64_t index, int32 write_id, const std::string& key, uint64 time,
    CDCSDKCheckpointPB* cdc_sdk_checkpoint_pb, OpId* last_streamed_op_id) {
  cdc_sdk_checkpoint_pb->set_term(term);
  cdc_sdk_checkpoint_pb->set_index(index);
  cdc_sdk_checkpoint_pb->set_write_id(write_id);
  cdc_sdk_checkpoint_pb->set_key(key);
  cdc_sdk_checkpoint_pb->set_snapshot_time(time);
  if (last_streamed_op_id) {
    last_streamed_op_id->term = term;
    last_streamed_op_id->index = index;
  }
}

bool ShouldCreateNewProtoRecord(
    const RowMessage& row_message, const Schema& schema, size_t col_count) {
  return (row_message.op() == RowMessage_Op_INSERT && col_count == schema.num_columns()) ||
         (row_message.op() == RowMessage_Op_UPDATE || row_message.op() == RowMessage_Op_DELETE);
}

bool IsInsertOperation(const RowMessage& row_message) {
  return row_message.op() == RowMessage_Op_INSERT;
}

bool IsInsertOrUpdate(const RowMessage& row_message) {
  return row_message.IsInitialized()  &&
      (row_message.op() == RowMessage_Op_INSERT
       || row_message.op() == RowMessage_Op_UPDATE);
}

void MakeNewProtoRecord(
    const docdb::IntentKeyValueForCDC& intent, const OpId& op_id, const RowMessage& row_message,
    const Schema& schema, size_t col_count, CDCSDKProtoRecordPB* proto_record,
    GetChangesResponsePB* resp, IntraTxnWriteId* write_id, std::string* reverse_index_key) {
  if (ShouldCreateNewProtoRecord(row_message, schema, col_count)) {
    CDCSDKOpIdPB* cdc_sdk_op_id_pb = proto_record->mutable_cdc_sdk_op_id();
    SetCDCSDKOpId(
        op_id.term, op_id.index, intent.write_id, intent.reverse_index_key, cdc_sdk_op_id_pb);

    CDCSDKProtoRecordPB* record_to_be_added = resp->add_cdc_sdk_proto_records();
    record_to_be_added->CopyFrom(*proto_record);
    record_to_be_added->mutable_row_message()->CopyFrom(row_message);

    *write_id = intent.write_id;
    *reverse_index_key = intent.reverse_index_key;
  }
}
// Populate CDC record corresponding to WAL batch in ReplicateMsg.
CHECKED_STATUS PopulateCDCSDKIntentRecord(
    const OpId& op_id,
    const TransactionId& transaction_id,
    const std::vector<docdb::IntentKeyValueForCDC>& intents,
    const StreamMetadata& metadata,
    const std::shared_ptr<tablet::TabletPeer>& tablet_peer,
    GetChangesResponsePB* resp,
    ScopedTrackedConsumption* consumption,
    IntraTxnWriteId* write_id,
    std::string* reverse_index_key,
    Schema* old_schema) {
  Schema& schema = old_schema ? *old_schema : *tablet_peer->tablet()->schema();
  Slice prev_key;
  CDCSDKProtoRecordPB proto_record;
  RowMessage* row_message = proto_record.mutable_row_message();
  size_t col_count = 0;
  for (const auto& intent : intents) {
    Slice key(intent.key_buf);
    Slice value(intent.value_buf);
    const auto key_size =
        VERIFY_RESULT(docdb::DocKey::EncodedSize(key, docdb::DocKeyPart::kWholeDocKey));

    docdb::KeyEntryValue column_id;
    boost::optional<docdb::KeyEntryValue> column_id_opt;
    Slice key_column = key.WithoutPrefix(key_size);
    if (!key_column.empty()) {
      RETURN_NOT_OK(docdb::KeyEntryValue::DecodeKey(&key_column, &column_id));
      column_id_opt = column_id;
    }

    Slice sub_doc_key = key;
    docdb::SubDocKey decoded_key;
    RETURN_NOT_OK(decoded_key.DecodeFrom(&sub_doc_key, docdb::HybridTimeRequired::kFalse));

    docdb::Value decoded_value;
    RETURN_NOT_OK(decoded_value.Decode(value));

    if (column_id_opt && column_id_opt->type() == docdb::KeyEntryType::kColumnId &&
        schema.is_key_column(column_id_opt->GetColumnId())) {
      *write_id = intent.write_id;
      *reverse_index_key = intent.reverse_index_key;
      continue;
    }

    if (*consumption) {
      consumption->Add(key.size());
    }

    // Compare key hash with previously seen key hash to determine whether the write pair
    // is part of the same row or not.
    Slice primary_key(key.data(), key_size);
    if (prev_key != primary_key || col_count >= schema.num_columns()) {
      proto_record.Clear();
      row_message->Clear();

      // Check whether operation is WRITE or DELETE.
      if (decoded_value.value_type() == docdb::ValueEntryType::kTombstone &&
          decoded_key.num_subkeys() == 0) {
        SetOperation(row_message, OpType::DELETE, schema);
        *write_id = intent.write_id;
      } else {
        if (column_id_opt &&
            column_id_opt->type() == docdb::KeyEntryType::kSystemColumnId &&
            decoded_value.value_type() == docdb::ValueEntryType::kNullLow) {
          SetOperation(row_message, OpType::INSERT, schema);
          col_count = schema.num_key_columns() - 1;
        } else {
          SetOperation(row_message, OpType::UPDATE, schema);
          col_count = schema.num_columns();
          *write_id = intent.write_id;
        }
      }

      // Write pair contains record for different row. Create a new CDCRecord in this case.
      row_message->set_transaction_id(transaction_id.ToString());
      AddPrimaryKey(tablet_peer, decoded_key, schema, row_message);
    }

    if (IsInsertOperation(*row_message)) {
      ++col_count;
    }

    prev_key = primary_key;
    if (IsInsertOrUpdate(*row_message)) {
      if (column_id_opt && column_id_opt->type() == docdb::KeyEntryType::kColumnId) {
        const ColumnSchema& col = VERIFY_RESULT(schema.column_by_id(column_id_opt->GetColumnId()));

        AddColumnToMap(
            tablet_peer, col, decoded_value.primitive_value(), row_message->add_new_tuple());
        row_message->add_old_tuple();

      } else if (
          column_id_opt && column_id_opt->type() != docdb::KeyEntryType::kSystemColumnId) {
        LOG(DFATAL) << "Unexpected value type in key: " << column_id_opt->type()
                    << " key: " << decoded_key.ToString()
                    << " value: " << decoded_value.primitive_value();
      }
    }
    row_message->set_table(tablet_peer->tablet()->metadata()->table_name());
    MakeNewProtoRecord(
        intent, op_id, *row_message, schema, col_count, &proto_record, resp, write_id,
        reverse_index_key);
  }

  return Status::OK();
}

// Populate CDC record corresponding to WAL batch in ReplicateMsg.
CHECKED_STATUS PopulateCDCSDKWriteRecord(
    const ReplicateMsgPtr& msg,
    const StreamMetadata& metadata,
    const std::shared_ptr<tablet::TabletPeer>& tablet_peer,
    GetChangesResponsePB* resp,
    const Schema& schema) {
  const auto& batch = msg->write().write_batch();
  CDCSDKProtoRecordPB* proto_record = nullptr;
  RowMessage* row_message = nullptr;

  // Write batch may contain records from different rows.
  // For CDC, we need to split the batch into 1 CDC record per row of the table.
  // We'll use DocDB key hash to identify the records that belong to the same row.
  Slice prev_key;

  for (const auto& write_pair : batch.write_pairs()) {
    Slice key = write_pair.key();
    const auto key_size =
        VERIFY_RESULT(docdb::DocKey::EncodedSize(key, docdb::DocKeyPart::kWholeDocKey));

    Slice value = write_pair.value();
    docdb::Value decoded_value;
    RETURN_NOT_OK(decoded_value.Decode(value));

    // Compare key hash with previously seen key hash to determine whether the write pair
    // is part of the same row or not.
    Slice primary_key(key.data(), key_size);
    if (prev_key != primary_key) {
      // Write pair contains record for different row. Create a new CDCRecord in this case.
      proto_record = resp->add_cdc_sdk_proto_records();
      row_message = proto_record->mutable_row_message();
      row_message->set_pgschema_name(schema.SchemaName());
      row_message->set_table(tablet_peer->tablet()->metadata()->table_name());

      CDCSDKOpIdPB* cdc_sdk_op_id_pb = proto_record->mutable_cdc_sdk_op_id();
      SetCDCSDKOpId(msg->id().term(), msg->id().index(), 0, "", cdc_sdk_op_id_pb);

      Slice sub_doc_key = key;
      docdb::SubDocKey decoded_key;
      RETURN_NOT_OK(decoded_key.DecodeFrom(&sub_doc_key, docdb::HybridTimeRequired::kFalse));

      // Check whether operation is WRITE or DELETE.
      if (decoded_value.value_type() == docdb::ValueEntryType::kTombstone &&
          decoded_key.num_subkeys() == 0) {
        SetOperation(row_message, OpType::DELETE, schema);
      } else {
        docdb::KeyEntryValue column_id;
        Slice key_column(key.WithoutPrefix(key_size));
        RETURN_NOT_OK(docdb::KeyEntryValue::DecodeKey(&key_column, &column_id));

        if (column_id.type() == docdb::KeyEntryType::kSystemColumnId &&
            decoded_value.value_type() == docdb::ValueEntryType::kNullLow) {
          SetOperation(row_message, OpType::INSERT, schema);
        } else {
          SetOperation(row_message, OpType::UPDATE, schema);
        }
      }

      AddPrimaryKey(tablet_peer, decoded_key, schema, row_message);

      // Process intent records.
      row_message->set_commit_time(msg->hybrid_time());
    }
    prev_key = primary_key;
    DCHECK(proto_record);

    if (IsInsertOrUpdate(*row_message)) {
      docdb::KeyEntryValue column_id;
      Slice key_column = key.WithoutPrefix(key_size);
      RETURN_NOT_OK(docdb::KeyEntryValue::DecodeKey(&key_column, &column_id));
      if (column_id.type() == docdb::KeyEntryType::kColumnId) {
        const ColumnSchema& col = VERIFY_RESULT(schema.column_by_id(column_id.GetColumnId()));

        AddColumnToMap(
            tablet_peer, col, decoded_value.primitive_value(), row_message->add_new_tuple());
        row_message->add_old_tuple();

      } else if (column_id.type() != docdb::KeyEntryType::kSystemColumnId) {
        LOG(DFATAL) << "Unexpected value type in key: " << column_id.type();
      }
    }
  }

  return Status::OK();
}

void SetTableProperties(
    const TablePropertiesPB* table_properties,
    CDCSDKTablePropertiesPB* cdc_sdk_table_properties_pb) {
  cdc_sdk_table_properties_pb->set_default_time_to_live(table_properties->default_time_to_live());
  cdc_sdk_table_properties_pb->set_num_tablets(table_properties->num_tablets());
  cdc_sdk_table_properties_pb->set_is_ysql_catalog_table(table_properties->is_ysql_catalog_table());
}

void SetColumnInfo(const ColumnSchemaPB& column, CDCSDKColumnInfoPB* column_info) {
  column_info->set_name(column.name());
  column_info->mutable_type()->CopyFrom(column.type());
  column_info->set_is_key(column.is_key());
  column_info->set_is_hash_key(column.is_hash_key());
  column_info->set_is_nullable(column.is_nullable());
  column_info->set_oid(column.pg_type_oid());
}

CHECKED_STATUS PopulateCDCSDKDDLRecord(
    const ReplicateMsgPtr& msg, CDCSDKProtoRecordPB* proto_record, const string& table_name,
    const Schema& schema) {
  SCHECK(
      msg->has_change_metadata_request(), InvalidArgument,
      Format(
          "Change metadata (DDL) message requires metadata information: $0",
          msg->ShortDebugString()));

  RowMessage* row_message = nullptr;

  row_message = proto_record->mutable_row_message();
  row_message->set_op(RowMessage_Op_DDL);
  row_message->set_table(table_name);

  CDCSDKOpIdPB* cdc_sdk_op_id_pb = proto_record->mutable_cdc_sdk_op_id();
  SetCDCSDKOpId(msg->id().term(), msg->id().index(), 0, "", cdc_sdk_op_id_pb);

  for (const auto& column : msg->change_metadata_request().schema().columns()) {
    CDCSDKColumnInfoPB* column_info = nullptr;
    column_info = row_message->mutable_schema()->add_column_info();
    SetColumnInfo(column, column_info);
  }

  CDCSDKTablePropertiesPB* cdc_sdk_table_properties_pb;
  const TablePropertiesPB* table_properties =
      &(msg->change_metadata_request().schema().table_properties());

  cdc_sdk_table_properties_pb = row_message->mutable_schema()->mutable_tab_info();
  row_message->set_schema_version(msg->change_metadata_request().schema_version());
  row_message->set_new_table_name(msg->change_metadata_request().new_table_name());
  row_message->set_pgschema_name(schema.SchemaName());
  SetTableProperties(table_properties, cdc_sdk_table_properties_pb);

  return Status::OK();
}

CHECKED_STATUS PopulateCDCSDKTruncateRecord(
    const ReplicateMsgPtr& msg, CDCSDKProtoRecordPB* proto_record, const Schema& schema) {
  SCHECK(
      msg->has_truncate(), InvalidArgument,
      Format(
          "Truncate message requires truncate request information: $0", msg->ShortDebugString()));

  RowMessage* row_message = nullptr;

  row_message = proto_record->mutable_row_message();
  row_message->set_op(RowMessage_Op_TRUNCATE);
  row_message->set_pgschema_name(schema.SchemaName());

  CDCSDKOpIdPB* cdc_sdk_op_id_pb;

  cdc_sdk_op_id_pb = proto_record->mutable_cdc_sdk_op_id();
  SetCDCSDKOpId(msg->id().term(), msg->id().index(), 0, "", cdc_sdk_op_id_pb);

  return Status::OK();
}

void SetTermIndex(int64_t term, int64_t index, CDCSDKCheckpointPB* checkpoint) {
  checkpoint->set_term(term);
  checkpoint->set_index(index);
}

void SetKeyWriteId(string key, int32_t write_id, CDCSDKCheckpointPB* checkpoint) {
  checkpoint->set_key(key);
  checkpoint->set_write_id(write_id);
}

CHECKED_STATUS ProcessIntents(
    const OpId& op_id,
    const TransactionId& transaction_id,
    const StreamMetadata& metadata,
    GetChangesResponsePB* resp,
    ScopedTrackedConsumption* consumption,
    CDCSDKCheckpointPB* checkpoint,
    const std::shared_ptr<tablet::TabletPeer>& tablet_peer,
    std::vector<docdb::IntentKeyValueForCDC>* keyValueIntents,
    docdb::ApplyTransactionState* stream_state,
    Schema* schema) {
  if (stream_state->key.empty() && stream_state->write_id == 0) {
    CDCSDKProtoRecordPB* proto_record = resp->add_cdc_sdk_proto_records();
    RowMessage* row_message = proto_record->mutable_row_message();
    row_message->set_op(RowMessage_Op_BEGIN);
    row_message->set_transaction_id(transaction_id.ToString());
    row_message->set_table(tablet_peer->tablet()->metadata()->table_name());
  }

  auto tablet = tablet_peer->shared_tablet();
  RETURN_NOT_OK(tablet->GetIntents(transaction_id, keyValueIntents, stream_state));

  for (auto& keyValue : *keyValueIntents) {
    docdb::SubDocKey sub_doc_key;
    CHECK_OK(
        sub_doc_key.FullyDecodeFrom(Slice(keyValue.key_buf), docdb::HybridTimeRequired::kFalse));
    docdb::Value decoded_value;
    RETURN_NOT_OK(decoded_value.Decode(Slice(keyValue.value_buf)));
  }

  std::string reverse_index_key;
  IntraTxnWriteId write_id = 0;

  // Need to populate the CDCSDKRecords
  RETURN_NOT_OK(PopulateCDCSDKIntentRecord(
      op_id, transaction_id, *keyValueIntents, metadata, tablet_peer, resp, consumption, &write_id,
      &reverse_index_key, schema));

  SetTermIndex(op_id.term, op_id.index, checkpoint);

  if (stream_state->key.empty() && stream_state->write_id == 0) {
    CDCSDKProtoRecordPB* proto_record = resp->add_cdc_sdk_proto_records();
    RowMessage* row_message = proto_record->mutable_row_message();

    row_message->set_op(RowMessage_Op_COMMIT);
    row_message->set_transaction_id(transaction_id.ToString());
    row_message->set_table(tablet_peer->tablet()->metadata()->table_name());

    CDCSDKOpIdPB* cdc_sdk_op_id_pb = proto_record->mutable_cdc_sdk_op_id();
    SetCDCSDKOpId(op_id.term, op_id.index, 0, "", cdc_sdk_op_id_pb);
    SetKeyWriteId("", 0, checkpoint);
  } else {
    SetKeyWriteId(reverse_index_key, write_id, checkpoint);
  }

  return Status::OK();
}

CHECKED_STATUS PopulateCDCSDKSnapshotRecord(
    GetChangesResponsePB* resp,
    const QLTableRow* row,
    const Schema& schema,
    const std::shared_ptr<tablet::TabletPeer>& tablet_peer,
    ReadHybridTime time) {
  CDCSDKProtoRecordPB* proto_record = nullptr;
  RowMessage* row_message = nullptr;
  string table_name = tablet_peer->tablet()->metadata()->table_name();

  proto_record = resp->add_cdc_sdk_proto_records();
  row_message = proto_record->mutable_row_message();
  row_message->set_table(table_name);
  row_message->set_op(RowMessage_Op_READ);
  row_message->set_pgschema_name(schema.SchemaName());
  row_message->set_commit_time(time.read.ToUint64());

  DatumMessagePB* cdc_datum_message = nullptr;

  for (size_t col_idx = 0; col_idx < schema.num_columns(); col_idx++) {
    ColumnId col_id = schema.column_id(col_idx);
    const auto* value = row->GetColumn(col_id);
    const ColumnSchema& col_schema = VERIFY_RESULT(schema.column_by_id(col_id));

    cdc_datum_message = row_message->add_new_tuple();
    cdc_datum_message->set_column_name(col_schema.name());

    if (value && value->value_case() != QLValuePB::VALUE_NOT_SET
        && col_schema.pg_type_oid() != 0 /*kInvalidOid*/) {
      docdb::SetValueFromQLBinaryWrapper(*value, col_schema.pg_type_oid(), cdc_datum_message);
    } else {
      cdc_datum_message->set_column_type(col_schema.pg_type_oid());
    }

    row_message->add_old_tuple();
  }

  return Status::OK();
}

void FillDDLInfo(RowMessage* row_message, const SchemaPB& schema, const uint32_t schema_version) {
  for (const auto& column : schema.columns()) {
    CDCSDKColumnInfoPB* column_info;
    column_info = row_message->mutable_schema()->add_column_info();
    SetColumnInfo(column, column_info);
  }

  row_message->set_schema_version(schema_version);
  row_message->set_pgschema_name(schema.pgschema_name());
  CDCSDKTablePropertiesPB* cdc_sdk_table_properties_pb =
      row_message->mutable_schema()->mutable_tab_info();

  const TablePropertiesPB* table_properties = &(schema.table_properties());
  SetTableProperties(table_properties, cdc_sdk_table_properties_pb);
}

// CDC get changes is different from 2DC as it doesn't need
// to read intents from WAL.

Status GetChangesForCDCSDK(
    const std::string& stream_id,
    const std::string& tablet_id,
    const CDCSDKCheckpointPB& from_op_id,
    const StreamMetadata& stream_metadata,
    const std::shared_ptr<tablet::TabletPeer>& tablet_peer,
    const MemTrackerPtr& mem_tracker,
    consensus::ReplicateMsgsHolder* msgs_holder,
    GetChangesResponsePB* resp,
    std::string* commit_timestamp,
    std::shared_ptr<Schema>* cached_schema,
    OpId* last_streamed_op_id,
    int64_t* last_readable_opid_index,
    const CoarseTimePoint deadline) {
  OpId op_id{from_op_id.term(), from_op_id.index()};
  ScopedTrackedConsumption consumption;
  CDCSDKProtoRecordPB* proto_record = nullptr;
  RowMessage* row_message = nullptr;
  CDCSDKCheckpointPB checkpoint;
  bool checkpoint_updated = false;

  // It is snapshot call.
  if (from_op_id.write_id() == -1) {
    auto txn_participant = tablet_peer->tablet()->transaction_participant();
    ReadHybridTime time;
    std::string nextKey;
    SchemaPB schema_pb;
    // It is first call in snapshot then take snapshot.
    if ((from_op_id.key().empty()) && (from_op_id.snapshot_time() == 0)) {
      if (txn_participant == nullptr || txn_participant->context() == nullptr)
        return STATUS_SUBSTITUTE(
            Corruption, "Cannot read data as the transaction participant context is null");
      tablet::RemoveIntentsData data;
      RETURN_NOT_OK(txn_participant->context()->GetLastReplicatedData(&data));
      // Set the checkpoint and communicate to the follower.
      VLOG(1) << "The first snapshot term " << data.op_id.term << "index  " << data.op_id.index
              << "time " << data.log_ht.ToUint64();
      // Update the CDCConsumerOpId.
      std::shared_ptr<consensus::Consensus> shared_consensus = tablet_peer->shared_consensus();
      shared_consensus->UpdateCDCConsumerOpId(data.op_id);

      if (txn_participant == nullptr || txn_participant->context() == nullptr) {
        return STATUS_SUBSTITUTE(
            Corruption, "Cannot read data as the transaction participant context is null");
      }
      txn_participant->SetRetainOpId(data.op_id);
      RETURN_NOT_OK(txn_participant->context()->GetLastReplicatedData(&data));
      time = ReadHybridTime::SingleTime(data.log_ht);

      // This should go to cdc_state table.
      // Below condition update the checkpoint in cdc_state table.
      SetCheckpoint(
          data.op_id.term, data.op_id.index, -1, "", time.read.ToUint64(), &checkpoint, nullptr);
      checkpoint_updated = true;
    } else {
      // Snapshot is already taken.
      HybridTime ht;
      time = ReadHybridTime::FromUint64(from_op_id.snapshot_time());
      nextKey = from_op_id.key();
      VLOG(1) << "The after snapshot term " << from_op_id.term() << "index  " << from_op_id.index()
              << "key " << from_op_id.key() << "snapshot time " << from_op_id.snapshot_time();

      Schema schema = *tablet_peer->tablet()->schema().get();
      int limit = FLAGS_cdc_snapshot_batch_size;
      int fetched = 0;
      std::vector<QLTableRow> rows;
      auto iter = VERIFY_RESULT(tablet_peer->tablet()->CreateCDCSnapshotIterator(
          schema.CopyWithoutColumnIds(), time, nextKey));

      QLTableRow row;
      SchemaToPB(*tablet_peer->tablet()->schema().get(), &schema_pb);

      proto_record = resp->add_cdc_sdk_proto_records();
      row_message = proto_record->mutable_row_message();
      row_message->set_op(RowMessage_Op_DDL);
      row_message->set_table(tablet_peer->tablet()->metadata()->table_name());

      FillDDLInfo(row_message, schema_pb, tablet_peer->tablet()->metadata()->schema_version());

      while (VERIFY_RESULT(iter->HasNext()) && fetched < limit) {
        RETURN_NOT_OK(iter->NextRow(&row));
        RETURN_NOT_OK(PopulateCDCSDKSnapshotRecord(resp, &row, schema, tablet_peer, time));
        fetched++;
      }
      docdb::SubDocKey sub_doc_key;
      RETURN_NOT_OK(iter->GetNextReadSubDocKey(&sub_doc_key));

      // Snapshot ends when next key is empty.
      if (sub_doc_key.doc_key().empty()) {
        VLOG(1) << "Setting next sub doc key empty ";
        // Get the checkpoint or read the checkpoint from the table/cache.
        SetCheckpoint(from_op_id.term(), from_op_id.index(), 0, "", 0, &checkpoint, nullptr);
        checkpoint_updated = true;
      } else {
        VLOG(1) << "Setting next sub doc key is " << sub_doc_key.Encode().ToStringBuffer();

        checkpoint.set_write_id(-1);
        SetCheckpoint(
            from_op_id.term(), from_op_id.index(), -1, sub_doc_key.Encode().ToStringBuffer(),
            time.read.ToUint64(), &checkpoint, nullptr);
        checkpoint_updated = true;
      }
    }
  } else if (!from_op_id.key().empty() && from_op_id.write_id() != 0) {
    std::string reverse_index_key = from_op_id.key();
    Slice reverse_index_key_slice(reverse_index_key);
    std::vector<docdb::IntentKeyValueForCDC> keyValueIntents;
    docdb::ApplyTransactionState stream_state;
    stream_state.key = from_op_id.key();
    stream_state.write_id = from_op_id.write_id();

    RETURN_NOT_OK(reverse_index_key_slice.consume_byte(docdb::KeyEntryTypeAsChar::kTransactionId));
    auto transaction_id = VERIFY_RESULT(DecodeTransactionId(&reverse_index_key_slice));

    RETURN_NOT_OK(ProcessIntents(
        op_id, transaction_id, stream_metadata, resp, &consumption, &checkpoint, tablet_peer,
        &keyValueIntents, &stream_state, nullptr));

    if (checkpoint.write_id() == 0 && checkpoint.key().empty()) {
      last_streamed_op_id->term = checkpoint.term();
      last_streamed_op_id->index = checkpoint.index();
    }
    checkpoint_updated = true;
  } else {
    RequestScope request_scope;

    auto read_ops = VERIFY_RESULT(tablet_peer->consensus()->ReadReplicatedMessagesForCDC(
        op_id, last_readable_opid_index, deadline));

    if (read_ops.read_from_disk_size && mem_tracker) {
      consumption = ScopedTrackedConsumption(mem_tracker, read_ops.read_from_disk_size);
    }

    auto txn_participant = tablet_peer->tablet()->transaction_participant();
    if (txn_participant) {
      request_scope = RequestScope(txn_participant);
    }

    Schema current_schema;
    bool pending_intents = false;
    bool schema_streamed = false;

    for (const auto& msg : read_ops.messages) {
      if (!schema_streamed && !(**cached_schema).initialized()) {
        current_schema.CopyFrom(*tablet_peer->tablet()->schema().get());
        string table_name = tablet_peer->tablet()->metadata()->table_name();
        schema_streamed = true;

        proto_record = resp->add_cdc_sdk_proto_records();
        row_message = proto_record->mutable_row_message();
        row_message->set_op(RowMessage_Op_DDL);
        row_message->set_table(table_name);

        *cached_schema = std::make_shared<Schema>(std::move(current_schema));
        SchemaPB current_schema_pb;
        SchemaToPB(**cached_schema, &current_schema_pb);
        FillDDLInfo(row_message,
                    current_schema_pb,
                    tablet_peer->tablet()->metadata()->schema_version());
      } else {
        current_schema = **cached_schema;
      }

      switch (msg->op_type()) {
        case consensus::OperationType::UPDATE_TRANSACTION_OP:
          // Ignore intents.
          // Read from IntentDB after they have been applied.
          if (msg->transaction_state().status() == TransactionStatus::APPLYING) {
            auto txn_id =
                VERIFY_RESULT(FullyDecodeTransactionId(msg->transaction_state().transaction_id()));
            auto result = GetTransactionStatus(txn_id, tablet_peer->Now(), txn_participant);
            std::vector<docdb::IntentKeyValueForCDC> intents;
            docdb::ApplyTransactionState new_stream_state;

            *commit_timestamp = msg->transaction_state().commit_hybrid_time();
            op_id.term = msg->id().term();
            op_id.index = msg->id().index();
            RETURN_NOT_OK(ProcessIntents(
                op_id, txn_id, stream_metadata, resp, &consumption, &checkpoint, tablet_peer,
                &intents, &new_stream_state, &current_schema));

            if (new_stream_state.write_id != 0 && !new_stream_state.key.empty()) {
              pending_intents = true;
            } else {
              last_streamed_op_id->term = msg->id().term();
              last_streamed_op_id->index = msg->id().index();
            }
          }
          checkpoint_updated = true;
          break;

        case consensus::OperationType::WRITE_OP: {
          const auto& batch = msg->write().write_batch();

          if (!batch.has_transaction()) {
                RETURN_NOT_OK(
                PopulateCDCSDKWriteRecord(msg, stream_metadata, tablet_peer, resp, current_schema));

            SetCheckpoint(
                msg->id().term(), msg->id().index(), 0, "", 0, &checkpoint, last_streamed_op_id);
            checkpoint_updated = true;
          }
        }
        break;

        case consensus::OperationType::CHANGE_METADATA_OP: {
          RETURN_NOT_OK(SchemaFromPB(msg->change_metadata_request().schema(), &current_schema));
          string table_name = tablet_peer->tablet()->metadata()->table_name();
          *cached_schema = std::make_shared<Schema>(std::move(current_schema));
          if ((resp->cdc_sdk_proto_records_size() > 0 &&
               resp->cdc_sdk_proto_records(resp->cdc_sdk_proto_records_size() - 1)
                       .row_message()
                       .op() == RowMessage_Op_DDL)) {
            if ((resp->cdc_sdk_proto_records(resp->cdc_sdk_proto_records_size() - 1)
                     .row_message()
                     .schema_version() != msg->change_metadata_request().schema_version())) {
              RETURN_NOT_OK(PopulateCDCSDKDDLRecord(
                  msg, resp->add_cdc_sdk_proto_records(), table_name, current_schema));
            }
          } else {
            RETURN_NOT_OK(PopulateCDCSDKDDLRecord(
                msg, resp->add_cdc_sdk_proto_records(), table_name, current_schema));
          }
          SetCheckpoint(
              msg->id().term(), msg->id().index(), 0, "", 0, &checkpoint, last_streamed_op_id);
          checkpoint_updated = true;
        }
        break;

        case consensus::OperationType::TRUNCATE_OP: {
          if (FLAGS_stream_truncate_record) {
            RETURN_NOT_OK(PopulateCDCSDKTruncateRecord(
                msg, resp->add_cdc_sdk_proto_records(), current_schema));
            SetCheckpoint(
                msg->id().term(), msg->id().index(), 0, "", 0, &checkpoint, last_streamed_op_id);
            checkpoint_updated = true;
          }
        }
        break;

        default:
          // Nothing to do for other operation types.
          break;
      }

      if (pending_intents) break;
    }
    if (read_ops.messages.size() > 0)
      *msgs_holder = consensus::ReplicateMsgsHolder(
          nullptr, std::move(read_ops.messages), std::move(consumption));
  }

  if (consumption) {
    consumption.Add(resp->SpaceUsedLong());
  }

  checkpoint_updated ? resp->mutable_cdc_sdk_checkpoint()->CopyFrom(checkpoint)
                       : resp->mutable_cdc_sdk_checkpoint()->CopyFrom(from_op_id);

  if (last_streamed_op_id->index > 0) {
    last_streamed_op_id->ToPB(resp->mutable_checkpoint()->mutable_op_id());
  }
  if (checkpoint_updated) {
    VLOG(1) << "The cdcsdk checkpoint is updated " << resp->cdc_sdk_checkpoint().ShortDebugString();
    VLOG(1) << "The checkpoint is updated " << resp->checkpoint().ShortDebugString();
  } else {
    VLOG(1) << "The cdcsdk checkpoint is not  updated "
            << resp->cdc_sdk_checkpoint().ShortDebugString();
    VLOG(1) << "The checkpoint is not updated " << resp->checkpoint().ShortDebugString();
  }
  return Status::OK();
}

}  // namespace cdc
}  // namespace yb
