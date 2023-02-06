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

#include "yb/master/system_tablet.h"

#include "yb/common/common.pb.h"
#include "yb/common/schema.h"
#include "yb/common/transaction.h"

#include "yb/docdb/doc_read_context.h"

#include "yb/master/sys_catalog_constants.h"
#include "yb/master/yql_virtual_table.h"

namespace yb {
namespace master {

SystemTablet::SystemTablet(const Schema& schema, std::unique_ptr<YQLVirtualTable> yql_virtual_table,
                           const TabletId& tablet_id)
    : log_prefix_(Format("T $0: ", tablet_id)), // Don't have UUID here to log in T XX P YY format.
      doc_read_context_(std::make_shared<docdb::DocReadContext>(
          log_prefix_, schema, kSysCatalogSchemaVersion)),
      yql_virtual_table_(std::move(yql_virtual_table)),
      tablet_id_(tablet_id) {
}

docdb::DocReadContextPtr SystemTablet::GetDocReadContext(const std::string& table_id) const {
  // table_id is ignored. It should match the system table's id.
  return doc_read_context_;
}

const docdb::YQLStorageIf& SystemTablet::QLStorage() const {
  return *yql_virtual_table_;
}

TableType SystemTablet::table_type() const {
  return TableType::YQL_TABLE_TYPE;
}

const TabletId& SystemTablet::tablet_id() const {
  return tablet_id_;
}

Result<HybridTime> SystemTablet::DoGetSafeTime(
    tablet::RequireLease require_lease, HybridTime min_allowed, CoarseTimePoint deadline) const {
  // HybridTime doesn't matter for SystemTablets.
  return HybridTime::kMax;
}

Status SystemTablet::HandleQLReadRequest(CoarseTimePoint deadline,
                                         const ReadHybridTime& read_time,
                                         const QLReadRequestPB& ql_read_request,
                                         const TransactionMetadataPB& transaction_metadata,
                                         tablet::QLReadRequestResult* result,
                                         WriteBuffer* rows_data) {
  DCHECK(!transaction_metadata.has_transaction_id());
  return tablet::AbstractTablet::HandleQLReadRequest(
      deadline, read_time, ql_read_request, TransactionOperationContext(), result, rows_data);
}

Status SystemTablet::CreatePagingStateForRead(const QLReadRequestPB& ql_read_request,
                                              const size_t row_count,
                                              QLResponsePB* response) const {
  // We don't support pagination for system tablets. Although we need to return an OK() status
  // here since we don't want to raise this as an error to the client, but just want to avoid
  // populating any paging state here for the client.
  return Status::OK();
}

const TableName& SystemTablet::GetTableName() const {
  return yql_virtual_table_->table_name();
}

}  // namespace master
}  // namespace yb
