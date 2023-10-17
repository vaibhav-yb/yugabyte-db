// Copyright (c) YugaByte, Inc.
//
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

#include <gtest/gtest.h>
#include "yb/cdc/cdc_service.proxy.h"
#include "yb/common/common_types.pb.h"
#include "yb/rpc/rpc_controller.h"
#include "yb/tools/yb-admin-test-base.h"
#include "yb/util/flags.h"

#include "yb/client/client.h"
#include "yb/client/ql-dml-test-base.h"
#include "yb/client/schema.h"
#include "yb/client/table.h"
#include "yb/client/table_info.h"

#include "yb/integration-tests/cdcsdk_test_base.h"

#include "yb/master/master_backup.proxy.h"
#include "yb/master/master_defaults.h"
#include "yb/master/master_replication.proxy.h"

#include "yb/rpc/secure_stream.h"

#include "yb/tools/admin-test-base.h"
#include "yb/tools/yb-admin_util.h"
#include "yb/tools/yb-admin_client.h"

#include "yb/tserver/mini_tablet_server.h"
#include "yb/client/table_alterer.h"
#include "yb/tserver/tablet_server.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/date_time.h"
#include "yb/util/env_util.h"
#include "yb/util/monotime.h"
#include "yb/util/path_util.h"
#include "yb/util/result.h"
#include "yb/util/subprocess.h"
#include "yb/util/test_macros.h"
#include "yb/util/tsan_util.h"

#include "yb/yql/pgwrapper/pg_wrapper_test_base.h"

namespace yb {
namespace tools {

using std::string;

const client::YBTableName kCdcsdkTableName(YQL_DATABASE_PGSQL, "my_namespace", "test_table");

class CDCSDKAdminCliTest : public pgwrapper::PgCommandTestBase {
 protected:
  std::unique_ptr<cdc::CDCServiceProxy> cdc_proxy_;

  CDCSDKAdminCliTest() : pgwrapper::PgCommandTestBase(false, false) {}

  void SetUp() override {
    pgwrapper::PgCommandTestBase::SetUp();
    ASSERT_OK(CreateClient());

    // Create test namespace.
    ASSERT_OK(client_->CreateNamespaceIfNotExists(
        kCdcsdkTableName.namespace_name(), kCdcsdkTableName.namespace_type()));

    cdc_proxy_ = std::make_unique<cdc::CDCServiceProxy>(
        &client_->proxy_cache(), cluster_->tablet_server(0)->bound_rpc_hostport());
  }

  Result<string> GetStreamIdFromParsedOutput(string output) {
    const int kStreamUuidLength = 36;
    const string find_stream_id = "CDC Stream ID: ";
    string::size_type stream_id_pos = output.find(find_stream_id);

    return output.substr(stream_id_pos + find_stream_id.size(), kStreamUuidLength);
  }

  template <class... Args>
  Result<std::string> RunAdminToolCommand(Args&&... args) {
    return tools::RunAdminToolCommand(cluster_->GetMasterAddresses(), std::forward<Args>(args)...);
  }
};

TEST_F(CDCSDKAdminCliTest, TestCreateChangeDataStream) {
  string output = ASSERT_RESULT(RunAdminToolCommand(
      "create_change_data_stream", "ysql." + kCdcsdkTableName.namespace_name()));

  // Output is of the format: CDC Stream ID: <36 character UUID>
  string parsed_string = ASSERT_RESULT(GetStreamIdFromParsedOutput(output));
  LOG(INFO) << "Created CDC stream using yb-admin: " << parsed_string;
}

}  // namespace tools
}  // namespace yb
