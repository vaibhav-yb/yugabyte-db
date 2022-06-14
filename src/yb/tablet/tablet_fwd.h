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
#ifndef YB_TABLET_TABLET_FWD_H
#define YB_TABLET_TABLET_FWD_H

#include <memory>

#include "yb/gutil/ref_counted.h"

#include "yb/tablet/tablet.fwd.h"

#include "yb/util/enums.h"
#include "yb/util/math_util.h"
#include "yb/util/strongly_typed_bool.h"

namespace yb {
namespace tablet {

class AbstractTablet;

class OperationDriver;
typedef scoped_refptr<OperationDriver> OperationDriverPtr;

class RaftGroupMetadata;
typedef scoped_refptr<RaftGroupMetadata> RaftGroupMetadataPtr;

class Tablet;
typedef std::shared_ptr<Tablet> TabletPtr;

struct TableInfo;
typedef std::shared_ptr<TableInfo> TableInfoPtr;

class TabletPeer;
typedef std::shared_ptr<TabletPeer> TabletPeerPtr;

class ChangeMetadataOperation;
class Operation;
class OperationFilter;
class SnapshotCoordinator;
class SnapshotOperation;
class SplitOperation;
class TabletSnapshots;
class TabletSplitter;
class TabletStatusListener;
class TransactionIntentApplier;
class TransactionCoordinator;
class TransactionCoordinatorContext;
class TransactionParticipant;
class TransactionParticipantContext;
class TransactionStatePB;
class TruncateOperation;
class TruncatePB;
class UpdateTxnOperation;
class WriteOperation;
class WriteQuery;
class WriteQueryContext;

struct CreateSnapshotData;
struct DocDbOpIds;
struct PgsqlReadRequestResult;
struct QLReadRequestResult;
struct RemoveIntentsData;
struct TabletInitData;
struct TabletMetrics;
struct TransactionApplyData;
struct TransactionStatusInfo;

YB_DEFINE_ENUM(FlushMode, (kSync)(kAsync));
YB_DEFINE_ENUM(RequireLease, (kFalse)(kTrue)(kFallbackToFollower));
YB_STRONGLY_TYPED_BOOL(Destroy);
YB_STRONGLY_TYPED_BOOL(DisableFlushOnShutdown);
YB_STRONGLY_TYPED_BOOL(IsSysCatalogTablet);
YB_STRONGLY_TYPED_BOOL(ShouldAbortActiveTransactions);
YB_STRONGLY_TYPED_BOOL(TransactionsEnabled);

// kIntents - Used to indicate that a transaction-related operation has already been applied to
// intents RocksDB.
// kRegular - Used to indicate that a transaction-related operation has already been applied to
// regular RocksDB (which was flushed) but the corresponding deletion of intents from the intents
// RocksDB has not been flushed and was therefore lost.
// kNone - Used to indicate that a transaction has not been applied to either regular or intents
// RocksDB.
YB_DEFINE_ENUM(ApplyPhase, (kNone)(kRegular)(kIntents));

enum class FlushFlags {
  kNone = 0,

  kRegular = 1,
  kIntents = 2,

  kAll = kRegular | kIntents
};

}  // namespace tablet
}  // namespace yb

#endif  // YB_TABLET_TABLET_FWD_H
