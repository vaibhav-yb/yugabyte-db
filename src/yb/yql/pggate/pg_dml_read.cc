//--------------------------------------------------------------------------------------------------
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
//--------------------------------------------------------------------------------------------------

#include "yb/yql/pggate/pg_dml_read.h"

#include <algorithm>
#include <functional>
#include <memory>
#include <utility>


#include "yb/common/partition.h"
#include "yb/common/pg_system_attr.h"
#include "yb/common/ql_datatype.h"
#include "yb/common/row_mark.h"
#include "yb/common/schema.h"

#include "yb/docdb/doc_key.h"
#include "yb/docdb/primitive_value.h"
#include "yb/docdb/value_type.h"

#include "yb/gutil/strings/substitute.h"

#include "yb/util/slice.h"

#include "yb/yql/pggate/pg_column.h"
#include "yb/yql/pggate/pg_expr.h"
#include "yb/yql/pggate/pg_select_index.h"
#include "yb/yql/pggate/pg_table.h"
#include "yb/yql/pggate/pg_tabledesc.h"
#include "yb/yql/pggate/ybc_pg_typedefs.h"

#include "yb/util/status_format.h"

using std::make_shared;
using std::vector;

namespace yb {
namespace pggate {

namespace {

class DocKeyBuilder {
 public:
  Status Prepare(
      const std::vector<docdb::KeyEntryValue>& hashed_components,
      const LWQLValuePB*const* hashed_values,
      const PartitionSchema& partition_schema) {
    if (hashed_components.empty()) {
      return Status::OK();
    }

    hash_ = VERIFY_RESULT(partition_schema.PgsqlHashColumnCompoundValue(
        boost::make_iterator_range(hashed_values, hashed_values + hashed_components.size())));
    hashed_components_ = &hashed_components;
    return Status::OK();
  }

  docdb::DocKey operator()(const vector<docdb::KeyEntryValue>& range_components) const {
    if (!hashed_components_) {
      return docdb::DocKey(range_components);
    }
    return docdb::DocKey(hash_, *hashed_components_, range_components);
  }

 private:
  uint16_t hash_;
  const vector<docdb::KeyEntryValue>* hashed_components_ = nullptr;
};

inline void ApplyBound(
    ::yb::LWPgsqlReadRequestPB* req, const std::optional<Bound>& bound, bool is_lower) {
  if (bound) {
    auto* mutable_bound = is_lower ? req->mutable_lower_bound() : req->mutable_upper_bound();
    mutable_bound->dup_key(PartitionSchema::EncodeMultiColumnHashValue(bound->value));
    mutable_bound->set_is_inclusive(bound->is_inclusive);
  }
}

} // namespace

//--------------------------------------------------------------------------------------------------
// PgDmlRead
//--------------------------------------------------------------------------------------------------

PgDmlRead::PgDmlRead(PgSession::ScopedRefPtr pg_session, const PgObjectId& table_id,
                     const PgObjectId& index_id, const PgPrepareParameters *prepare_params,
                     bool is_region_local)
    : PgDml(std::move(pg_session), table_id, index_id, prepare_params, is_region_local) {
}

PgDmlRead::~PgDmlRead() {
}

void PgDmlRead::PrepareBinds() {
  if (!bind_) {
    // This statement doesn't have bindings.
    return;
  }

  for (auto& col : bind_.columns()) {
    col.AllocPrimaryBindPB(read_req_.get());
  }
}

void PgDmlRead::SetForwardScan(const bool is_forward_scan) {
  if (secondary_index_query_) {
    return secondary_index_query_->SetForwardScan(is_forward_scan);
  }
  read_req_->set_is_forward_scan(is_forward_scan);
}

//--------------------------------------------------------------------------------------------------
// DML support.
// TODO(neil) WHERE clause is not yet supported. Revisit this function when it is.

LWPgsqlExpressionPB *PgDmlRead::AllocColumnBindPB(PgColumn *col) {
  return col->AllocBindPB(read_req_.get());
}

LWPgsqlExpressionPB *PgDmlRead::AllocColumnBindConditionExprPB(PgColumn *col) {
  return col->AllocBindConditionExprPB(read_req_.get());
}

LWPgsqlExpressionPB *PgDmlRead::AllocColumnAssignPB(PgColumn *col) {
  // SELECT statement should not have an assign expression (SET clause).
  LOG(FATAL) << "Pure virtual function is being called";
  return nullptr;
}

LWPgsqlExpressionPB *PgDmlRead::AllocTargetPB() {
  return read_req_->add_targets();
}

LWPgsqlExpressionPB *PgDmlRead::AllocQualPB() {
  return read_req_->add_where_clauses();
}

LWPgsqlColRefPB *PgDmlRead::AllocColRefPB() {
  return read_req_->add_col_refs();
}

void PgDmlRead::ClearColRefPBs() {
  read_req_->mutable_col_refs()->clear();
}

//--------------------------------------------------------------------------------------------------
// RESULT SET SUPPORT.
// For now, selected expressions are just a list of column names (ref).
//   SELECT column_l, column_m, column_n FROM ...

void PgDmlRead::SetColumnRefs() {
  if (secondary_index_query_) {
    DCHECK(!has_aggregate_targets()) << "Aggregate pushdown should not happen with index";
  }
  read_req_->set_is_aggregate(has_aggregate_targets());
  // Populate column references in the read request
  ColRefsToPB();
  // Compatibility: set column ids in a form that is expected by legacy nodes
  ColumnRefsToPB(read_req_->mutable_column_refs());
}

// Method removes empty primary binds and moves tailing non empty range primary binds
// which are following after empty binds into the 'condition_expr' field.
Status PgDmlRead::ProcessEmptyPrimaryBinds() {
  if (!bind_) {
    // This query does not have any binds.
    read_req_->mutable_partition_column_values()->clear();
    read_req_->mutable_range_column_values()->clear();
    return Status::OK();
  }

  // NOTE: ybctid is a system column and not processed as bind.
  bool miss_partition_columns = false;
  bool has_partition_columns = false;

  // Collecting column indexes that are involved in a tuple
  std::vector<size_t> tuple_col_ids;

  bool preceding_key_column_missed = false;

  for (size_t index = 0; index != bind_->num_hash_key_columns(); ++index) {
    auto expr = bind_.ColumnForIndex(index).bind_pb();
    auto colid = bind_.ColumnForIndex(index).id();
    // For IN clause expr->has_condition() returns 'true'.
    if (!expr || (!expr->has_condition() &&
                  (expr_binds_.find(expr) == expr_binds_.end()) &&
                  (std::find(tuple_col_ids.begin(), tuple_col_ids.end(), colid) ==
                   tuple_col_ids.end()))) {
      miss_partition_columns = true;
      continue;
    } else {
      has_partition_columns = true;
    }

    if (expr && expr->has_condition()) {
      // Move any range column binds into the 'condition_expr' field if
      // we are batching hash columns.
      preceding_key_column_missed = pg_session_->IsHashBatchingEnabled();
      const auto& lhs = *expr->condition().operands().begin();
      if (lhs.has_tuple()) {
        const auto& tuple = lhs.tuple();
        for (const auto& elem : tuple.elems()) {
          tuple_col_ids.push_back(elem.column_id());
        }
      }
    }
  }

  SCHECK(!has_partition_columns || !miss_partition_columns, InvalidArgument,
      "Partition key must be fully specified");

  if (miss_partition_columns) {
    VLOG(1) << "Full scan is needed";
    read_req_->mutable_partition_column_values()->clear();
    // Reset binding of columns whose values has been deleted.
    for (size_t i = 0, end = bind_->num_hash_key_columns(); i != end; ++i) {
      bind_.ColumnForIndex(i).ResetBindPB();
    }

    // Move all range column binds (if any) into the 'condition_expr' field.
    preceding_key_column_missed = true;
  }

  size_t num_bound_range_columns = 0;

  for (auto index = bind_->num_hash_key_columns(); index < bind_->num_key_columns(); ++index) {
    auto& col = bind_.ColumnForIndex(index);
    auto expr = col.bind_pb();
    const auto expr_bind = expr ? expr_binds_.find(expr) : expr_binds_.end();
    // For IN clause expr->has_condition() returns 'true'.
    if (expr && expr->has_condition()) {
      preceding_key_column_missed = true;
      RETURN_NOT_OK(MoveBoundKeyInOperator(&col, expr->condition()));
    } else if (expr_bind == expr_binds_.end()) {
      preceding_key_column_missed = true;
    } else {
      if (preceding_key_column_missed) {
        // Move current bind into the 'condition_expr' field.
        auto* condition_expr_pb = AllocColumnBindConditionExprPB(&col);
        condition_expr_pb->mutable_condition()->set_op(QL_OP_EQUAL);

        auto op1_pb = condition_expr_pb->mutable_condition()->add_operands();
        auto op2_pb = condition_expr_pb->mutable_condition()->add_operands();

        op1_pb->set_column_id(col.id());

        auto attr_value = expr_bind->second;
        RETURN_NOT_OK(attr_value->EvalTo(op2_pb));
        expr_binds_.erase(expr_bind);
      } else {
        ++num_bound_range_columns;
      }
    }
  }

  auto& range_column_values = *read_req_->mutable_range_column_values();
  while (range_column_values.size() > num_bound_range_columns) {
    range_column_values.pop_back();
  }
  // Reset binding of columns whose values has been deleted.
  for (size_t i = num_bound_range_columns, end = bind_->num_columns(); i != end; ++i) {
    bind_.ColumnForIndex(i).ResetBindPB();
  }
  return Status::OK();
}

//--------------------------------------------------------------------------------------------------

bool PgDmlRead::IsConcreteRowRead() const {
  // Operation reads a concrete row at least one of the following conditions is met:
  // - ybctid is explicitly bound
  // - ybctid is used implicitly by using secondary index
  // - all hash and range key components are bound (Note: each key component can be bound only once)
  return has_doc_op() && bind_ &&
         (ybctid_bind_ ||
          (secondary_index_query_ && secondary_index_query_->has_doc_op()) ||
          (bind_->num_key_columns() ==
              static_cast<size_t>(read_req_->partition_column_values().size() +
                                  read_req_->range_column_values().size())));
}

Status PgDmlRead::Exec(const PgExecParameters *exec_params) {
  // Save IN/OUT parameters from Postgres.
  pg_exec_params_ = exec_params;

  // Set column references in protobuf and whether query is aggregate.
  SetColumnRefs();

  const auto row_mark_type = GetRowMarkType(exec_params);
  if (has_doc_op() &&
      !secondary_index_query_ &&
      IsValidRowMarkType(row_mark_type) &&
      CanBuildYbctidsFromPrimaryBinds()) {
    RETURN_NOT_OK(SubstitutePrimaryBindsWithYbctids(exec_params));
  } else {
    RETURN_NOT_OK(ProcessEmptyPrimaryBinds());
    if (has_doc_op()) {
      if (row_mark_type == RowMarkType::ROW_MARK_KEYSHARE && !IsConcreteRowRead()) {
        // ROW_MARK_KEYSHARE creates a weak read intent on DocDB side. As a result it is only
        // applicable when the read operation reads a concrete row (by using ybctid or by specifying
        // all primary key columns). In case some columns of the primary key are not specified,
        // a strong read intent is required to prevent rows from being deleted by another
        // transaction. For this purpose ROW_MARK_KEYSHARE must be replaced with ROW_MARK_SHARE.
        auto actual_exec_params = *exec_params;
        actual_exec_params.rowmark = RowMarkType::ROW_MARK_SHARE;
        RETURN_NOT_OK(doc_op_->ExecuteInit(&actual_exec_params));
      } else {
        RETURN_NOT_OK(doc_op_->ExecuteInit(exec_params));
      }
    }
  }

  // First, process the secondary index request.
  bool has_ybctid = VERIFY_RESULT(ProcessSecondaryIndexRequest(exec_params));

  if (!has_ybctid && secondary_index_query_ && secondary_index_query_->has_doc_op()) {
    // No ybctid is found from the IndexScan. Instruct "doc_op_" to abandon the execution and not
    // querying any data from tablet server.
    //
    // Note: For system catalog (colocated table), the secondary_index_query_ won't send a separate
    // scan read request to DocDB.  For this case, the index request is embedded inside the SELECT
    // request (PgsqlReadRequestPB::index_request).
    doc_op_->AbandonExecution();
  } else {
    // Update bind values for constants and placeholders.
    RETURN_NOT_OK(UpdateBindPBs());

    // Execute select statement and prefetching data from DocDB.
    // Note: For SysTable, doc_op_ === null, IndexScan doesn't send separate request.
    if (doc_op_) {
      SCHECK_EQ(VERIFY_RESULT(doc_op_->Execute()), RequestSent::kTrue, IllegalState,
                "YSQL read operation was not sent");
    }
  }

  return Status::OK();
}

Status PgDmlRead::BindColumnCondBetween(int attr_num, PgExpr *attr_value,
                                        bool start_inclusive,
                                        PgExpr *attr_value_end,
                                        bool end_inclusive) {
  if (secondary_index_query_) {
    // Bind by secondary key.
    return secondary_index_query_->BindColumnCondBetween(attr_num, attr_value,
                                                         start_inclusive,
                                                         attr_value_end,
                                                         end_inclusive);
  }

  DCHECK(attr_num != static_cast<int>(PgSystemAttrNum::kYBTupleId))
    << "Operator BETWEEN cannot be applied to ROWID";

  // Find column.
  PgColumn& col = VERIFY_RESULT(bind_.ColumnForAttr(attr_num));

  // Check datatype.
  if (attr_value) {
    SCHECK_EQ(col.internal_type(), attr_value->internal_type(), Corruption,
              "Attribute value type does not match column type");
  }

  if (attr_value_end) {
    SCHECK_EQ(col.internal_type(), attr_value_end->internal_type(), Corruption,
              "Attribute value type does not match column type");
  }

  CHECK(!col.is_partition()) << "This method cannot be used for binding partition column!";

  // Alloc the protobuf.
  auto* condition_expr_pb = AllocColumnBindConditionExprPB(&col);

  if (attr_value != nullptr) {
    if (attr_value_end != nullptr) {
      condition_expr_pb->mutable_condition()->set_op(QL_OP_BETWEEN);

      auto op1_pb = condition_expr_pb->mutable_condition()->add_operands();
      auto op2_pb = condition_expr_pb->mutable_condition()->add_operands();
      auto op3_pb = condition_expr_pb->mutable_condition()->add_operands();

      op1_pb->set_column_id(col.id());

      RETURN_NOT_OK(attr_value->EvalTo(op2_pb));
      RETURN_NOT_OK(attr_value_end->EvalTo(op3_pb));

      if (yb_pushdown_strict_inequality) {
        auto op4_pb = condition_expr_pb->mutable_condition()->add_operands();
        auto op5_pb = condition_expr_pb->mutable_condition()->add_operands();
        op4_pb->mutable_value()->set_bool_value(start_inclusive);
        op5_pb->mutable_value()->set_bool_value(end_inclusive);
      }
    } else {
      auto op = QL_OP_GREATER_THAN_EQUAL;
      if (!start_inclusive) {
        op = QL_OP_GREATER_THAN;
      }
      condition_expr_pb->mutable_condition()->set_op(op);

      auto op1_pb = condition_expr_pb->mutable_condition()->add_operands();
      auto op2_pb = condition_expr_pb->mutable_condition()->add_operands();

      op1_pb->set_column_id(col.id());

      RETURN_NOT_OK(attr_value->EvalTo(op2_pb));
    }
  } else {
    if (attr_value_end != nullptr) {
      auto op = QL_OP_LESS_THAN_EQUAL;
      if (!end_inclusive) {
        op = QL_OP_LESS_THAN;
      }
      condition_expr_pb->mutable_condition()->set_op(op);

      auto op1_pb = condition_expr_pb->mutable_condition()->add_operands();
      auto op2_pb = condition_expr_pb->mutable_condition()->add_operands();

      op1_pb->set_column_id(col.id());

      RETURN_NOT_OK(attr_value_end->EvalTo(op2_pb));
    } else {
      // Unreachable.
    }
  }

  return Status::OK();
}

Status PgDmlRead::BindColumnCondIn(PgExpr *lhs, int n_attr_values, PgExpr **attr_values) {
  if (secondary_index_query_) {
    // Bind by secondary key.
    return secondary_index_query_->BindColumnCondIn(lhs, n_attr_values, attr_values);
  }

  auto cols = VERIFY_RESULT(lhs->GetColumns(&bind_));
  for (PgColumn &col : cols) {
    SCHECK(col.attr_num() != static_cast<int>(PgSystemAttrNum::kYBTupleId),
           InvalidArgument,
           "Operator IN cannot be applied to ROWID");
  }

  // Check datatype.
  // TODO(neil) Current code combine TEXT and BINARY datatypes into ONE representation.  Once that
  // is fixed, we can remove the special if() check for BINARY type.
  for (int i = 0; i < n_attr_values; i++) {
    if (attr_values[i]) {
      auto vals = attr_values[i]->Unpack();
      auto curr_val_it = vals.begin();
      for (const PgColumn &curr_col : cols) {
        const PgExpr &curr_val = *curr_val_it;
        auto col_type = curr_val.internal_type();
        if (curr_col.internal_type() == InternalType::kBinaryValue)
            continue;
        SCHECK_EQ(curr_col.internal_type(), col_type, Corruption,
          "Attribute value type does not match column type");
        curr_val_it++;
      }
    }
  }

  for (const PgColumn &curr_col : cols) {
    // Check primary column bindings
    if (curr_col.is_primary() && curr_col.bind_pb() != nullptr) {
      if (expr_binds_.find(curr_col.bind_pb()) != expr_binds_.end()) {
        LOG(WARNING) << strings::Substitute("Column $0 is already bound to another value.",
                                            curr_col.attr_num());
      }
    }
  }

  // Find column.
  // Note that in the case that we are dealing with a tuple IN,
  // we only bind this condition to the first column in the IN. The nature of that
  // column (hash or range) will decide how this tuple IN condition will be processed.
  PgColumn& col = cols.front();
  bool col_is_primary = col.is_primary();

  if (col_is_primary) {
    // Alloc the protobuf.
    auto *bind_pb = col.bind_pb();
    if (bind_pb == nullptr) {
      bind_pb = AllocColumnBindPB(&col);
    }

    bind_pb->mutable_condition()->set_op(QL_OP_IN);
    auto lhs_bind = bind_pb->mutable_condition()->add_operands();

    RETURN_NOT_OK(lhs->PrepareForRead(this, lhs_bind));

    // There's no "list of expressions" field so we simulate it with an artificial nested OR
    // with repeated operands, one per bind expression.
    // This is only used for operation unrolling in pg_doc_op and is not understood by DocDB.
    auto op2_pb = bind_pb->mutable_condition()->add_operands();
    op2_pb->mutable_condition()->set_op(QL_OP_OR);

    for (int i = 0; i < n_attr_values; i++) {
      auto *attr_pb = op2_pb->mutable_condition()->add_operands();
      // Link the expression and protobuf. During execution, expr will write result to the pb.
      RETURN_NOT_OK(attr_values[i]->PrepareForRead(this, attr_pb));

      expr_binds_[attr_pb] = attr_values[i];
    }
  } else {
    // Alloc the protobuf.
    auto* condition_expr_pb = AllocColumnBindConditionExprPB(&col);

    condition_expr_pb->mutable_condition()->set_op(QL_OP_IN);

    auto op1_pb = condition_expr_pb->mutable_condition()->add_operands();
    auto op2_pb = condition_expr_pb->mutable_condition()->add_operands();

    op1_pb->set_column_id(col.id());

    for (int i = 0; i < n_attr_values; i++) {
      // Link the given expression "attr_value" with the allocated protobuf.
      // Note that except for constants and place_holders, all other expressions can be setup
      // just one time during prepare.
      // Examples:
      // - Bind values for primary columns in where clause.
      //     WHERE hash = ?
      // - Bind values for a column in INSERT statement.
      //     INSERT INTO a_table(hash, key, col) VALUES(?, ?, ?)

      if (attr_values[i]) {
        RETURN_NOT_OK(attr_values[i]->EvalTo(
            op2_pb->mutable_value()->mutable_list_value()->add_elems()));
      }
    }
  }
  return Status::OK();
}

Result<docdb::DocKey> PgDmlRead::EncodeRowKeyForBound(
    YBCPgStatement handle, size_t n_col_values, PgExpr **col_values, bool for_lower_bound) {
  std::vector<docdb::KeyEntryValue> hashed_components;
  hashed_components.reserve(bind_->num_hash_key_columns());
  size_t i = 0;
  auto hashed_values = arena().AllocateArray<LWQLValuePB*>(bind_->num_hash_key_columns());
  for (; i < bind_->num_hash_key_columns(); ++i) {
    auto &col = bind_.columns()[i];

    hashed_values[i] = VERIFY_RESULT(col_values[i]->Eval());
    auto docdbval = docdb::KeyEntryValue::FromQLValuePB(
        *hashed_values[i], col.desc().sorting_type());
    hashed_components.push_back(std::move(docdbval));
  }

  DocKeyBuilder dockey_builder;
  RETURN_NOT_OK(dockey_builder.Prepare(
      hashed_components, hashed_values, bind_->partition_schema()));

  std::vector<docdb::KeyEntryValue> range_components;
  n_col_values = std::max(std::min(n_col_values, bind_->num_key_columns()),
                          bind_->num_hash_key_columns());
  range_components.reserve(n_col_values - bind_->num_hash_key_columns());
  for (; i < n_col_values; ++i) {
    auto& col = bind_.columns()[i];

    if (col_values[i] == nullptr) {
      range_components.emplace_back(
          for_lower_bound ? docdb::KeyEntryType::kLowest : docdb::KeyEntryType::kHighest);
    } else {
      auto value = VERIFY_RESULT(col_values[i]->Eval());
      range_components.push_back(
          docdb::KeyEntryValue::FromQLValuePB(*value, col.desc().sorting_type()));
    }
  }

  return dockey_builder(range_components);
}

Status PgDmlRead::AddRowUpperBound(YBCPgStatement handle,
                                    int n_col_values,
                                    PgExpr **col_values,
                                    bool is_inclusive) {
  if (secondary_index_query_) {
      return secondary_index_query_->AddRowUpperBound(handle,
                                                        n_col_values,
                                                        col_values,
                                                        is_inclusive);
  }

  auto dockey = VERIFY_RESULT(EncodeRowKeyForBound(handle, n_col_values, col_values, false));

  if (read_req_->has_upper_bound()) {
      docdb::DocKey current_upper_bound_key;
      RETURN_NOT_OK(current_upper_bound_key.DecodeFrom(
                    read_req_->upper_bound().key(),
                    docdb::DocKeyPart::kWholeDocKey,
                    docdb::AllowSpecial::kTrue));

      if (current_upper_bound_key < dockey) {
        return Status::OK();
      }

      if (current_upper_bound_key == dockey) {
          is_inclusive = is_inclusive & read_req_->upper_bound().is_inclusive();
          read_req_->mutable_upper_bound()->set_is_inclusive(is_inclusive);
          return Status::OK();
      }

      // current_upper_bound_key > dockey
  }
  read_req_->mutable_upper_bound()->dup_key(dockey.Encode().AsSlice());
  read_req_->mutable_upper_bound()->set_is_inclusive(is_inclusive);

  return Status::OK();
}

Status PgDmlRead::AddRowLowerBound(YBCPgStatement handle,
                                   int n_col_values,
                                   PgExpr **col_values,
                                   bool is_inclusive) {

  if (secondary_index_query_) {
      return secondary_index_query_->AddRowLowerBound(handle,
                                                        n_col_values,
                                                        col_values,
                                                        is_inclusive);
  }

  auto dockey = VERIFY_RESULT(EncodeRowKeyForBound(handle, n_col_values, col_values, true));
  if (read_req_->has_lower_bound()) {
      docdb::DocKey current_lower_bound_key;
      RETURN_NOT_OK(current_lower_bound_key.DecodeFrom(
                    read_req_->lower_bound().key(),
                    docdb::DocKeyPart::kWholeDocKey,
                    docdb::AllowSpecial::kTrue));

      if (current_lower_bound_key > dockey) {
        return Status::OK();
      }

      if (current_lower_bound_key == dockey) {
          is_inclusive = is_inclusive & read_req_->lower_bound().is_inclusive();
          read_req_->mutable_lower_bound()->set_is_inclusive(is_inclusive);
          return Status::OK();
      }

      // current_lower_bound_key > dockey
  }
  read_req_->mutable_lower_bound()->dup_key(dockey.Encode().AsSlice());
  read_req_->mutable_lower_bound()->set_is_inclusive(is_inclusive);

  return Status::OK();
}

Status PgDmlRead::SubstitutePrimaryBindsWithYbctids(const PgExecParameters* exec_params) {
  const auto ybctids = VERIFY_RESULT(BuildYbctidsFromPrimaryBinds());
  expr_binds_.clear();
  read_req_->mutable_partition_column_values()->clear();
  read_req_->mutable_range_column_values()->clear();
  RETURN_NOT_OK(doc_op_->ExecuteInit(exec_params));
  auto i = ybctids.begin();
  return doc_op_->PopulateDmlByYbctidOps({make_lw_function([&i, end = ybctids.end()] {
    return i != end ? Slice(*i++) : Slice();
  }), ybctids.size()});
}

// Function builds vector of ybctids from primary key binds.
// Required precondition that one and only one range key component has IN clause and all
// other key components are set must be checked by caller code.
Result<std::vector<std::string>> PgDmlRead::BuildYbctidsFromPrimaryBinds() {
  auto hashed_values = arena().AllocateArray<LWQLValuePB*>(bind_->num_hash_key_columns());
  vector<docdb::KeyEntryValue> hashed_components, range_components;
  hashed_components.reserve(bind_->num_hash_key_columns());
  range_components.reserve(bind_->num_key_columns() - bind_->num_hash_key_columns());
  for (size_t i = 0; i < bind_->num_hash_key_columns(); ++i) {
    auto& col = bind_.ColumnForIndex(i);
    hashed_components.push_back(VERIFY_RESULT(
        BuildKeyColumnValue(col, *col.bind_pb(), hashed_values + i)));
  }

  DocKeyBuilder dockey_builder;
  RETURN_NOT_OK(dockey_builder.Prepare(
      hashed_components, hashed_values, bind_->partition_schema()));

  for (size_t i = bind_->num_hash_key_columns(); i < bind_->num_key_columns(); ++i) {
    auto& col = bind_.ColumnForIndex(i);
    auto& expr = *col.bind_pb();
    // For IN clause expr->has_condition() returns 'true'.
    if (expr.has_condition()) {
      const auto prefix_len = range_components.size();
      // Form ybctid for each value in IN clause.
      std::vector<std::string> ybctids;
      auto it = expr.condition().operands().begin();
      ++it;
      for (const auto& in_exp : it->condition().operands()) {
        range_components.push_back(VERIFY_RESULT(BuildKeyColumnValue(col, in_exp)));
        // Range key component has one and only one IN clause,
        // all remains components has explicit values. Add them as is.
        for (size_t j = i + 1; j < bind_->num_key_columns(); ++j) {
          auto& suffix_col = bind_.ColumnForIndex(j);
          range_components.push_back(VERIFY_RESULT(
              BuildKeyColumnValue(suffix_col, *suffix_col.bind_pb())));
        }
        const auto doc_key = dockey_builder(range_components);
        ybctids.push_back(doc_key.Encode().ToStringBuffer());
        range_components.resize(prefix_len);
      }
      return ybctids;
    } else {
      range_components.push_back(VERIFY_RESULT(BuildKeyColumnValue(col, expr)));
    }
  }
  return STATUS(IllegalState, "Can't build ybctids, bad preconditions");
}

bool PgDmlRead::IsAllPrimaryKeysBound(size_t num_range_components_in_expected) {
  if (!bind_) {
    return false;
  }

  int64_t range_components_in_clause_remain = num_range_components_in_expected;

  for (size_t i = 0; i < bind_->num_key_columns(); ++i) {
    auto& col = bind_.ColumnForIndex(i);
    auto* expr = col.bind_pb();
    // For IN clause expr->has_condition() returns 'true'.
    if (expr->has_condition()) {
      if ((i < bind_->num_hash_key_columns()) || (--range_components_in_clause_remain < 0)) {
        // unsupported IN clause
        return false;
      }
    } else if (expr_binds_.find(expr) == expr_binds_.end()) {
      // missing key component found
      return false;
    }
  }
  return range_components_in_clause_remain == 0;
}

// Function checks that one and only one range key component has IN clause
// and all other key components are set.
bool PgDmlRead::CanBuildYbctidsFromPrimaryBinds() {
  return IsAllPrimaryKeysBound(1 /* num_range_components_in_expected */);
}

// Moves IN operator bound for range key component into 'condition_expr' field
Status PgDmlRead::MoveBoundKeyInOperator(PgColumn* col, const LWPgsqlConditionPB& in_operator) {
  auto* condition_expr_pb = AllocColumnBindConditionExprPB(col);
  condition_expr_pb->mutable_condition()->set_op(QL_OP_IN);

  auto op1_pb = condition_expr_pb->mutable_condition()->add_operands();
  *op1_pb = *in_operator.operands().begin();

  auto op2_pb = condition_expr_pb->mutable_condition()->add_operands();
  auto it = in_operator.operands().begin();
  ++it;
  for (const auto& expr : it->condition().operands()) {
    auto* value = VERIFY_RESULT(GetBoundValue(*col, expr));
    auto* out = op2_pb->mutable_value()->mutable_list_value()->add_elems();
    if (value) {
      *out = *value;
    }
    expr_binds_.erase(expr_binds_.find(&expr));
  }
  return Status::OK();
}

Result<LWQLValuePB*> PgDmlRead::GetBoundValue(
    const PgColumn& col, const LWPgsqlExpressionPB& src) const {
  // 'src' expression has no value yet,
  // it is used as the key to find actual source in 'expr_binds_'.
  const auto it = expr_binds_.find(&src);
  if (it == expr_binds_.end()) {
    return STATUS_FORMAT(IllegalState, "Bind value not found for $0", col.id());
  }
  return it->second->Eval();
}

Result<docdb::KeyEntryValue> PgDmlRead::BuildKeyColumnValue(
    const PgColumn& col, const LWPgsqlExpressionPB& src, LWQLValuePB** dest) {
  *dest = VERIFY_RESULT(GetBoundValue(col, src));
  if (*dest) {
    return docdb::KeyEntryValue::FromQLValuePB(**dest, col.desc().sorting_type());
  }
  return docdb::KeyEntryValue();
}

Result<docdb::KeyEntryValue> PgDmlRead::BuildKeyColumnValue(
    const PgColumn& col, const LWPgsqlExpressionPB& src) {
  LWQLValuePB* temp_value;
  return BuildKeyColumnValue(col, src, &temp_value);
}

Status PgDmlRead::BindHashCode(const std::optional<Bound>& start, const std::optional<Bound>& end) {
  if (secondary_index_query_) {
    return secondary_index_query_->BindHashCode(start, end);
  }
  ApplyBound(read_req_.get(), start, true /* is_lower */);
  ApplyBound(read_req_.get(), end, false /* is_lower */);
  return Status::OK();
}

void PgDmlRead::UpgradeDocOp(PgDocOp::SharedPtr doc_op) {
  CHECK(!original_doc_op_) << "DocOp can be upgraded only once";
  CHECK(doc_op_) << "No DocOp object for upgrade";
  original_doc_op_.swap(doc_op_);
  doc_op_.swap(doc_op);
}

bool PgDmlRead::IsReadFromYsqlCatalog() const {
  return target_->schema().table_properties().is_ysql_catalog_table();
}

bool PgDmlRead::IsIndexOrderedScan() const {
  return secondary_index_query_ &&
      !secondary_index_query_->IsAllPrimaryKeysBound(0 /* num_range_components_in_expected */);
}

}  // namespace pggate
}  // namespace yb
