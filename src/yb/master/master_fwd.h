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

#include <map>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/optional/optional.hpp>

#include "yb/common/entity_ids_types.h"

#include "yb/gutil/ref_counted.h"

#include "yb/master/master_backup.fwd.h"
#include "yb/master/master_replication.pb.h"
#include "yb/master/tablet_split_fwd.h"

#include "yb/util/enums.h"
#include "yb/util/math_util.h"
#include "yb/util/monotime.h"
#include "yb/util/strongly_typed_bool.h"

namespace yb {

class HostPort;
struct HostPortHash;

namespace vtables {

class YQLPartitionsVTable;
class YQLVirtualTable;

}  // namespace vtables

namespace master {

class TSDescriptor;
typedef std::shared_ptr<TSDescriptor> TSDescriptorPtr;
typedef std::vector<TSDescriptorPtr> TSDescriptorVector;

class EncryptionManager;

class CatalogManager;
class CatalogManagerIf;
class CatalogManagerBgTasks;
class CDCConsumerSplitDriverIf;
class CDCRpcTasks;
class CDCSplitDriverIf;
class ClusterConfigInfo;
class ClusterLoadBalancer;
class FlushManager;
class Master;
class MasterBackupProxy;
class MasterOptions;
class MasterPathHandlers;
class MasterAdminProxy;
class MasterClientProxy;
class MasterClusterProxy;
class MasterDclProxy;
class MasterDdlProxy;
class MasterEncryptionProxy;
class MasterHeartbeatProxy;
class MasterReplicationProxy;
class NamespaceInfo;
class PermissionsManager;
class RetryingTSRpcTask;
class SnapshotCoordinatorContext;
class SnapshotState;
class SysCatalogTable;
class SysConfigInfo;
class SysRowEntries;
class TSDescriptor;
class TSManager;
class UDTypeInfo;
class XClusterSafeTimeService;
class YsqlTablegroupManager;
class YsqlTablespaceManager;
class YsqlTransactionDdl;

struct CDCConsumerStreamInfo;
struct TableDescription;
struct TabletReplica;
struct TabletReplicaDriveInfo;

class AsyncTabletSnapshotOp;
using AsyncTabletSnapshotOpPtr = std::shared_ptr<AsyncTabletSnapshotOp>;

class TableInfo;
using TableInfoPtr = scoped_refptr<TableInfo>;

class TabletInfo;
using TabletInfoPtr = scoped_refptr<TabletInfo>;
using TabletInfos = std::vector<TabletInfoPtr>;

struct SnapshotScheduleRestoration;
using SnapshotScheduleRestorationPtr = std::shared_ptr<SnapshotScheduleRestoration>;

YB_STRONGLY_TYPED_BOOL(RegisteredThroughHeartbeat);

YB_STRONGLY_TYPED_BOOL(IncludeInactive);
YB_STRONGLY_TYPED_BOOL(IncludeDeleted);

YB_DEFINE_ENUM(
    CollectFlag,
    (kAddIndexes)(kIncludeParentColocatedTable)(kSucceedIfCreateInProgress)(kAddUDTypes));
using CollectFlags = EnumBitSet<CollectFlag>;

using TableToTablespaceIdMap = std::unordered_map<TableId, boost::optional<TablespaceId>>;
using TablespaceIdToReplicationInfoMap = std::unordered_map<
    TablespaceId, boost::optional<ReplicationInfoPB>>;

using LeaderStepDownFailureTimes = std::unordered_map<TabletServerId, MonoTime>;
using TabletReplicaMap = std::unordered_map<std::string, TabletReplica>;
using TabletToTabletServerMap = std::unordered_map<TabletId, TabletServerId>;
using TabletInfoMap = std::map<TabletId, scoped_refptr<TabletInfo>>;
struct cloud_hash;
struct cloud_equal_to;
using AffinitizedZonesSet = std::unordered_set<CloudInfoPB, cloud_hash, cloud_equal_to>;
using BlacklistSet = std::unordered_set<HostPort, HostPortHash>;
using RetryingTSRpcTaskPtr = std::shared_ptr<RetryingTSRpcTask>;

// Use ordered map to make computing fingerprint of the map easier.
using DbOidToCatalogVersionMap = std::map<uint32_t, std::pair<uint64_t, uint64_t>>;
using RelIdToAttributesMap = std::unordered_map<uint32_t, std::vector<PgAttributePB>>;
using RelTypeOIDMap = std::unordered_map<uint32_t, uint32_t>;
namespace enterprise {

class CatalogManager;

} // namespace enterprise

} // namespace master
} // namespace yb
