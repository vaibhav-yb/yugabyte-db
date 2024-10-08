// Copyright (c) YugabyteDB, Inc.
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

// This file contains the gFlags that are common across yb-master and yb-tserver processes.

#include "yb/util/flags.h"

// User specified identifier for this cluster. On the first master leader setup, this is stored in
// the cluster_config. if not specified, a random UUID is generated.
// Changing the value after setup is not recommended.
DEFINE_NON_RUNTIME_string(cluster_uuid, "", "Cluster UUID to be used by this cluster");
TAG_FLAG(cluster_uuid, hidden);

DEFINE_RUNTIME_AUTO_PG_FLAG(
    bool, yb_enable_replication_slot_consumption, kLocalPersisted, false, true,
    "Enable consumption of changes via replication slots."
    "Requires yb_enable_replication_commands to be true.");
