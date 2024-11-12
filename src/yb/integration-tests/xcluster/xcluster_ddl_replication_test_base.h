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

#pragma once

#include "yb/integration-tests/xcluster/xcluster_ysql_test_base.h"

namespace yb {

class XClusterDDLReplicationTestBase : public XClusterYsqlTestBase {
 public:
  XClusterDDLReplicationTestBase() = default;
  ~XClusterDDLReplicationTestBase() = default;

  virtual void SetUp() override;

  bool UseAutomaticMode() override {
    // All these tests use automatic.
    return true;
  }

  Status SetUpClusters(bool is_colocated = false);

  Status EnableDDLReplicationExtension();

  virtual Status CheckpointReplicationGroup(
      const xcluster::ReplicationGroupId& replication_group_id = kReplicationGroupId) override {
    return XClusterYsqlTestBase::CheckpointReplicationGroup(replication_group_id);
  }

  // Unlike the previous method, this one does not fail if bootstrap is required.
  Status CheckpointReplicationGroupWithoutRequiringNoBootstrapNeeded(
      const std::vector<NamespaceName>& namespace_names);

  Result<std::shared_ptr<client::YBTable>> GetProducerTable(
      const client::YBTableName& producer_table_name);

  Result<std::shared_ptr<client::YBTable>> GetConsumerTable(
      const client::YBTableName& producer_table_name);

  void InsertRowsIntoProducerTableAndVerifyConsumer(const client::YBTableName& producer_table_name);

  Status WaitForSafeTimeToAdvanceToNowWithoutDDLQueue();

  Status PrintDDLQueue(Cluster& cluster);
};

}  // namespace yb
