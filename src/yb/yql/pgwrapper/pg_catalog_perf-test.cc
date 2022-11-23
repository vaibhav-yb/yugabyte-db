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

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include <gflags/gflags.h>

#include "yb/master/master.h"
#include "yb/master/mini_master.h"

#include "yb/tserver/tablet_server.h"
#include "yb/tserver/mini_tablet_server.h"

#include "yb/util/metrics.h"
#include "yb/util/result.h"
#include "yb/util/status.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_thread_holder.h"

#include "yb/yql/pgwrapper/libpq_utils.h"
#include "yb/yql/pgwrapper/pg_mini_test_base.h"

METRIC_DECLARE_histogram(handler_latency_yb_tserver_TabletServerService_Read);
METRIC_DECLARE_counter(pg_response_cache_queries);
METRIC_DECLARE_counter(pg_response_cache_hits);
DECLARE_bool(ysql_enable_read_request_caching);

namespace yb {
namespace pgwrapper {
namespace {

Status EnableCatcacheEventLogging(PGConn* conn) {
  return conn->Execute("SET yb_debug_log_catcache_events = ON");
}

template<bool CacheEnabled>
class ConfigurablePgCatalogPerfTest : public PgMiniTestBase {
 protected:
  void SetUp() override {
    FLAGS_ysql_enable_read_request_caching = CacheEnabled;
    PgMiniTestBase::SetUp();
    read_rpc_watcher_ = std::make_unique<MetricWatcher>(
        *cluster_->mini_master()->master(),
        METRIC_handler_latency_yb_tserver_TabletServerService_Read);
    auto& tserver = *cluster_->mini_tablet_server(0)->server();
    response_cache_queries_ = std::make_unique<MetricWatcher>(
        tserver, METRIC_pg_response_cache_queries);
    response_cache_hits_ = std::make_unique<MetricWatcher>(
        tserver, METRIC_pg_response_cache_hits);
  }

  size_t NumTabletServers() override {
    return 1;
  }

  Result<uint64_t> CacheRefreshRPCCount() {
    auto conn = VERIFY_RESULT(Connect());
    RETURN_NOT_OK(EnableCatcacheEventLogging(&conn));
    auto conn_aux = VERIFY_RESULT(Connect());
    RETURN_NOT_OK(EnableCatcacheEventLogging(&conn_aux));
    RETURN_NOT_OK(conn_aux.Execute("CREATE TABLE t (k INT)"));
    RETURN_NOT_OK(conn_aux.Execute("ALTER TABLE t ADD COLUMN v INT"));
    // Catalog version was increased by the conn_aux but conn may not detect this immediately.
    // So run simplest possible query which doesn't produce RPC in a loop until number of
    // RPC will be greater than 0.
    for (;;) {
      const auto result = VERIFY_RESULT(read_rpc_watcher_->Delta([&conn] {
        return conn.Execute("ROLLBACK");
      }));
      if (result) {
        return result;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
    return STATUS(RuntimeError, "Unreachable statement");
  }

  using AfterCacheRefreshFunctor = std::function<Status(PGConn*)>;
  Result<uint64_t> RPCCountAfterCacheRefresh(const AfterCacheRefreshFunctor& functor) {
    auto conn = VERIFY_RESULT(Connect());
    RETURN_NOT_OK(conn.Execute("CREATE TABLE cache_refresh_trigger (k INT)"));
    // Force version increment. Next new connection will do cache refresh on start.
    RETURN_NOT_OK(conn.Execute("ALTER TABLE cache_refresh_trigger ADD COLUMN v INT"));
    auto aux_conn = VERIFY_RESULT(Connect());
    return read_rpc_watcher_->Delta([&functor, &aux_conn] {
      return functor(&aux_conn);
    });
  }

  Result<std::pair<uint64_t, uint64_t>> ResponseCacheCountersDelta(
    const MetricWatcher::DeltaFunctor& functor) {
    uint64_t queries = 0;
    auto hits = VERIFY_RESULT(response_cache_hits_->Delta([this, &functor, &queries]() -> Status {
      queries = VERIFY_RESULT(response_cache_queries_->Delta([&functor] {
        return functor();
      }));
      return Status::OK();
    }));
    return std::make_pair(queries, hits);
  }

  std::unique_ptr<MetricWatcher> read_rpc_watcher_;
  std::unique_ptr<MetricWatcher> response_cache_queries_;
  std::unique_ptr<MetricWatcher> response_cache_hits_;
};

using PgCatalogPerfTest = ConfigurablePgCatalogPerfTest<false>;
using PgCatalogWithCachePerfTest = ConfigurablePgCatalogPerfTest<true>;

} // namespace

// Test checks the number of RPC for very first and subsequent connection to same t-server.
// Very first connection prepares local cache file while subsequent connections doesn't do this.
// As a result number of RPCs has huge difference.
// Note: Also subsequent connections doesn't preload the cache. This maybe changed in future.
//       Number of RPCs in all the tests are not the constants and they can be changed in future.
TEST_F(PgCatalogPerfTest, YB_DISABLE_TEST_IN_TSAN(StartupRPCCount)) {
  const auto connector = [this]() -> Status {
    RETURN_NOT_OK(Connect());
    return Status::OK();
  };

  const auto first_connect_rpc_count = ASSERT_RESULT(read_rpc_watcher_->Delta(connector));
  ASSERT_EQ(first_connect_rpc_count, 4);
  const auto subsequent_connect_rpc_count = ASSERT_RESULT(read_rpc_watcher_->Delta(connector));
  ASSERT_EQ(subsequent_connect_rpc_count, 1);
}

// Test checks number of RPC in case of cache refresh without partitioned tables.
TEST_F(PgCatalogPerfTest, YB_DISABLE_TEST_IN_TSAN(CacheRefreshRPCCountWithoutPartitionTables)) {
  const auto cache_refresh_rpc_count = ASSERT_RESULT(CacheRefreshRPCCount());
  ASSERT_EQ(cache_refresh_rpc_count, 3);
}

// Test checks number of RPC in case of cache refresh with partitioned tables.
TEST_F(PgCatalogPerfTest, YB_DISABLE_TEST_IN_TSAN(CacheRefreshRPCCountWithPartitionTables)) {
  auto conn = ASSERT_RESULT(Connect());
  for (size_t ti = 0; ti < 3; ++ti) {
    ASSERT_OK(conn.ExecuteFormat("CREATE TABLE t$0 (r INT, v INT) PARTITION BY RANGE(r)", ti));
    for (size_t pi = 0; pi < 3; ++pi) {
      ASSERT_OK(conn.ExecuteFormat(
          "CREATE TABLE t$0_p$1 PARTITION OF t$0 FOR VALUES FROM ($2) TO ($3)",
          ti, pi, 100 * pi + 1, 100 * (pi + 1)));
    }
  }
  const auto cache_refresh_rpc_count = ASSERT_RESULT(CacheRefreshRPCCount());
  ASSERT_EQ(cache_refresh_rpc_count, 6);
}

// Test checks number of RPC to a master caused by the first INSERT stmt into a table with primary
// key after cache refresh.
TEST_F(PgCatalogPerfTest, YB_DISABLE_TEST_IN_TSAN(AfterCacheRefreshRPCCountOnInsert)) {
  auto aux_conn = ASSERT_RESULT(Connect());
  ASSERT_OK(aux_conn.Execute("CREATE TABLE t (k INT PRIMARY KEY)"));
  auto master_rpc_count_for_insert = ASSERT_RESULT(RPCCountAfterCacheRefresh([](PGConn* conn) {
    return conn->Execute("INSERT INTO t VALUES(0)");
  }));
  ASSERT_EQ(master_rpc_count_for_insert, 1);
}

// Test checks number of RPC to a master caused by the first SELECT stmt from a table with primary
// key after cache refresh.
TEST_F(PgCatalogPerfTest, YB_DISABLE_TEST_IN_TSAN(AfterCacheRefreshRPCCountOnSelect)) {
  auto aux_conn = ASSERT_RESULT(Connect());
  ASSERT_OK(aux_conn.Execute("CREATE TABLE t (k INT PRIMARY KEY)"));
  auto master_rpc_count_for_select = ASSERT_RESULT(RPCCountAfterCacheRefresh([](PGConn* conn) {
    VERIFY_RESULT(conn->Fetch("SELECT * FROM t"));
    return static_cast<Status>(Status::OK());
  }));
  ASSERT_EQ(master_rpc_count_for_select, 3);
}

// The test checks number of hits in response cache in case of multiple connections and aggressive
// sys catalog changes. Which causes catalog cache refresh in each established connection.
TEST_F_EX(PgCatalogPerfTest,
          YB_DISABLE_TEST_IN_TSAN(ResponseCacheEfficiency),
          PgCatalogWithCachePerfTest) {
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.Execute("CREATE TABLE t (r INT PRIMARY KEY)"));
  auto aux_conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.Execute("ALTER TABLE t ADD COLUMN v INT"));
  std::vector<PGConn> conns;
  constexpr size_t kConnectionCount = 20;
  constexpr size_t kAlterTableCount = 10;
  for (size_t i = 0; i < kConnectionCount; ++i) {
    conns.push_back(ASSERT_RESULT(Connect()));
    ASSERT_RESULT(conns.back().Fetch("SELECT * FROM t"));
  }
  ASSERT_RESULT(aux_conn.Fetch("SELECT * FROM t"));
  size_t read_rpc_counter = 0;
  auto cache_counters = ASSERT_RESULT(ResponseCacheCountersDelta(
    [this, &conn, &conns, &read_rpc_counter] {
      read_rpc_counter = VERIFY_RESULT(read_rpc_watcher_->Delta([&conn, &conns] {
        for (size_t i = 0; i < kAlterTableCount; ++i) {
          RETURN_NOT_OK(conn.ExecuteFormat("ALTER TABLE t ADD COLUMN v_$0 INT", i));
          TestThreadHolder holder;
          size_t conn_idx = 0;
          for (auto& c : conns) {
            holder.AddThread([&c, idx = conn_idx, i] {
              ASSERT_OK(c.ExecuteFormat("INSERT INTO t VALUES($0)", idx * 100 + i));
            });
            ++conn_idx;
          }
        }
        return static_cast<Status>(Status::OK());
      }));
      return static_cast<Status>(Status::OK());
    }));
  const auto items_count = ASSERT_RESULT(conn.FetchValue<int64_t>("SELECT COUNT(*) FROM t"));
  constexpr size_t kExpectedColumnCount = kAlterTableCount + 2;
  const auto column_count = PQnfields(ASSERT_RESULT(conn.Fetch("SELECT * FROM t limit 1")).get());
  ASSERT_EQ(kExpectedColumnCount, column_count);
  const auto aux_column_count =
      PQnfields(ASSERT_RESULT(aux_conn.Fetch("SELECT * FROM t limit 1")).get());
  ASSERT_EQ(kExpectedColumnCount, aux_column_count);
  ASSERT_EQ(items_count, kAlterTableCount * kConnectionCount);
  constexpr size_t kUniqueQueriesPerRefresh = 3;
  const auto unique_queries = kAlterTableCount * kUniqueQueriesPerRefresh;
  const auto total_queries = kConnectionCount * unique_queries;
  ASSERT_EQ(cache_counters.first, total_queries);
  ASSERT_LE(cache_counters.second, total_queries - unique_queries);
  ASSERT_GE(cache_counters.second, total_queries - 2 * unique_queries);
  ASSERT_LE(read_rpc_counter, 720);
}

} // namespace pgwrapper
} // namespace yb
