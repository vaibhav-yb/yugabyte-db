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

#include <memory>

#include "boost/function/function_fwd.hpp"
#include "boost/optional.hpp"

#include "yb/common/common_fwd.h"

#include "yb/docdb/docdb_fwd.h"

#include "yb/util/status_fwd.h"

namespace yb {

class Slice;

namespace docdb {

YB_STRONGLY_TYPED_BOOL(ContinueScan);
using YQLScanCallback = boost::function<Result<ContinueScan>(const QLTableRow& row)>;

class YQLRowwiseIteratorIf {
 public:
  typedef std::unique_ptr<YQLRowwiseIteratorIf> UniPtr;
  virtual ~YQLRowwiseIteratorIf() {}

  //------------------------------------------------------------------------------------------------
  // Pure virtual API methods.
  //------------------------------------------------------------------------------------------------
  // Checks whether next row exists.
  virtual Result<bool> HasNext() = 0;

  // Skip the current row.
  virtual void SkipRow() = 0;

  // If restart is required returns restart hybrid time, based on iterated records.
  // Otherwise returns invalid hybrid time.
  virtual HybridTime RestartReadHt() = 0;

  virtual std::string ToString() const = 0;

  // Could be subset of actual table schema.
  virtual const Schema& schema() const = 0;

  //------------------------------------------------------------------------------------------------
  // Virtual API methods.
  // These methods are not applied to virtual/system table.
  //------------------------------------------------------------------------------------------------

  // Apache Cassandra Only: CQL supports static columns while all other intefaces do not.
  // Is the next row column to read a static column?
  virtual bool IsNextStaticColumn() const {
    return false;
  }

  // Retrieves the next key to read after the iterator finishes for the given page.
  virtual Status GetNextReadSubDocKey(SubDocKey* sub_doc_key);

  // Returns the tuple id of the current tuple. See DocRowwiseIterator for details.
  virtual Result<Slice> GetTupleId() const;

  // Seeks to the given tuple by its id. See DocRowwiseIterator for details.
  virtual Result<bool> SeekTuple(const Slice& tuple_id);

  //------------------------------------------------------------------------------------------------
  // Common API methods.
  //------------------------------------------------------------------------------------------------
  // Read next row using the specified projection.
  // REQUIRES: projection should be a subset of schema().
  Status NextRow(const Schema& projection, QLTableRow* table_row);

  // Read next row using whole schema() as a projection.
  Status NextRow(QLTableRow* table_row);

  // Iterates over the rows until --
  //  - callback fails or returns false.
  //  - Iterator reaches end of iteration.
  virtual Status Iterate(const YQLScanCallback& callback);

 private:
  virtual Status DoNextRow(boost::optional<const Schema&> projection, QLTableRow* table_row) = 0;
};

}  // namespace docdb
}  // namespace yb
