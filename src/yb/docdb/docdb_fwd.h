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

#include "yb/common/common_fwd.h"

#include "yb/docdb/docdb.fwd.h"
#include "yb/docdb/docdb_encoding_fwd.h"

#include "yb/util/enums.h"
#include "yb/util/math_util.h"
#include "yb/util/strongly_typed_bool.h"

namespace yb {
namespace docdb {

class ConsensusFrontier;
class DeadlineInfo;
class DocDBCompactionFilterFactory;
class DocOperation;
class DocPgsqlScanSpec;
class DocQLScanSpec;
class DocRowwiseIterator;
class DocWriteBatch;
class ExternalTxnIntentsState;
class HistoryRetentionPolicy;
class IntentAwareIterator;
class IntentAwareIteratorIf;
class IntentIterator;
class KeyBytes;
class KeyEntryValue;
class ManualHistoryRetentionPolicy;
class PgsqlWriteOperation;
class PrimitiveValue;
class QLWriteOperation;
class RedisWriteOperation;
class RowPacker;
class ScanChoices;
class SchemaPacking;
class SchemaPackingStorage;
class SharedLockManager;
class TransactionStatusCache;
class WaitQueue;
class YQLRowwiseIteratorIf;
class YQLStorageIf;

struct ApplyTransactionState;
struct ColumnPackingData;
struct CompactionSchemaPacking;
struct DocDB;
struct DocReadContext;
struct IntentKeyValueForCDC;
struct KeyBounds;
struct LockBatchEntry;
struct ValueControlFields;

using DocKeyHash = uint16_t;
using LockBatchEntries = std::vector<LockBatchEntry>;
using DocReadContextPtr = std::shared_ptr<DocReadContext>;
using ScanChoicesPtr = std::unique_ptr<ScanChoices>;

using IndexRequests = std::vector<std::pair<const IndexInfo*, QLWriteRequestPB>>;

enum class KeyEntryType;
enum class ValueEntryType;

YB_STRONGLY_TYPED_BOOL(PartialRangeKeyIntents);

YB_STRONGLY_TYPED_BOOL(SkipFlush);

// "Weak" intents are written for ancestor keys of a key that's being modified. For example, if
// we're writing a.b.c with snapshot isolation, we'll write weak snapshot isolation intents for
// keys "a" and "a.b".
//
// "Strong" intents are written for keys that are being modified. In the example above, we will
// write a strong snapshot isolation intent for the key a.b.c itself.
YB_DEFINE_ENUM(IntentStrength, (kWeak)(kStrong));

}  // namespace docdb
}  // namespace yb
