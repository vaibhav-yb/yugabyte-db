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

#include "yb/consensus/raft_consensus.h"

#include "yb/integration-tests/cql_test_base.h"

#include "yb/master/mini_master.h"

#include "yb/tablet/tablet_peer.h"

#include "yb/util/random_util.h"
#include "yb/util/status_log.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_util.h"
#include "yb/util/tsan_util.h"

using namespace std::literals;

DECLARE_bool(TEST_timeout_non_leader_master_rpcs);
DECLARE_int64(cql_processors_limit);
DECLARE_int32(client_read_write_timeout_ms);

DECLARE_int32(partitions_vtable_cache_refresh_secs);
DECLARE_int32(client_read_write_timeout_ms);
DECLARE_bool(disable_truncate_table);

namespace yb {

class CqlTest : public CqlTestBase<MiniCluster> {
 public:
  virtual ~CqlTest() = default;
};

TEST_F(CqlTest, ProcessorsLimit) {
  constexpr int kSessions = 10;
  FLAGS_cql_processors_limit = 1;

  std::vector<CassandraSession> sessions;
  bool has_failures = false;
  for (int i = 0; i != kSessions; ++i) {
    auto session = EstablishSession(driver_.get());
    if (!session.ok()) {
      LOG(INFO) << "Establish session failure: " << session.status();
      ASSERT_TRUE(session.status().IsServiceUnavailable());
      has_failures = true;
    } else {
      sessions.push_back(std::move(*session));
    }
  }

  ASSERT_TRUE(has_failures);
}

// Execute delete in parallel to transactional update of the same row.
TEST_F(CqlTest, ConcurrentDeleteRowAndUpdateColumn) {
  constexpr int kIterations = 70;
  auto session1 = ASSERT_RESULT(EstablishSession(driver_.get()));
  auto session2 = ASSERT_RESULT(EstablishSession(driver_.get()));
  ASSERT_OK(session1.ExecuteQuery(
      "CREATE TABLE t (i INT PRIMARY KEY, j INT) WITH transactions = { 'enabled' : true }"));
  auto insert_prepared = ASSERT_RESULT(session1.Prepare("INSERT INTO t (i, j) VALUES (?, ?)"));
  for (int key = 1; key <= 2 * kIterations; ++key) {
    auto stmt = insert_prepared.Bind();
    stmt.Bind(0, key);
    stmt.Bind(1, key * 10);
    ASSERT_OK(session1.Execute(stmt));
  }
  auto update_prepared = ASSERT_RESULT(session1.Prepare(
      "BEGIN TRANSACTION "
      "  UPDATE t SET j = j + 1 WHERE i = ?;"
      "  UPDATE t SET j = j + 1 WHERE i = ?;"
      "END TRANSACTION;"));
  auto delete_prepared = ASSERT_RESULT(session1.Prepare("DELETE FROM t WHERE i = ?"));
  std::vector<CassandraFuture> futures;
  for (int i = 0; i < kIterations; ++i) {
    int k1 = i * 2 + 1;
    int k2 = i * 2 + 2;

    auto update_stmt = update_prepared.Bind();
    update_stmt.Bind(0, k1);
    update_stmt.Bind(1, k2);
    futures.push_back(session1.ExecuteGetFuture(update_stmt));
  }

  for (int i = 0; i < kIterations; ++i) {
    int k2 = i * 2 + 2;

    auto delete_stmt = delete_prepared.Bind();
    delete_stmt.Bind(0, k2);
    futures.push_back(session1.ExecuteGetFuture(delete_stmt));
  }

  for (auto& future : futures) {
    ASSERT_OK(future.Wait());
  }

  auto result = ASSERT_RESULT(session1.ExecuteWithResult("SELECT * FROM t"));
  auto iterator = result.CreateIterator();
  int num_rows = 0;
  int num_even = 0;
  while (iterator.Next()) {
    ++num_rows;
    auto row = iterator.Row();
    auto key = row.Value(0).As<int>();
    auto value = row.Value(1).As<int>();
    if ((key & 1) == 0) {
      LOG(ERROR) << "Even key: " << key;
      ++num_even;
    }
    ASSERT_EQ(value, key * 10 + 1);
    LOG(INFO) << "Row: " << key << " => " << value;
  }
  ASSERT_EQ(num_rows, kIterations);
  ASSERT_EQ(num_even, 0);
}

TEST_F(CqlTest, TestUpdateListIndexAfterOverwrite) {
  auto session = ASSERT_RESULT(EstablishSession(driver_.get()));
  auto cql = [&](const std::string query) { ASSERT_OK(session.ExecuteQuery(query)); };
  cql("CREATE TABLE test(h INT, v LIST<INT>, PRIMARY KEY(h))");
  cql("INSERT INTO test (h, v) VALUES (1, [1, 2, 3])");

  auto select = [&]() -> Result<string> {
    auto result = VERIFY_RESULT(session.ExecuteWithResult("SELECT * FROM test"));
    auto iter = result.CreateIterator();
    DFATAL_OR_RETURN_ERROR_IF(!iter.Next(), STATUS(NotFound, "Did not find result in test table."));
    auto row = iter.Row();
    auto key = row.Value(0).As<int>();
    EXPECT_EQ(key, 1);
    return row.Value(1).ToString();
  };

  cql("UPDATE test SET v = [4, 5, 6] where h = 1");
  cql("UPDATE test SET v[0] = 7 WHERE h = 1");
  auto res1 = ASSERT_RESULT(select());
  EXPECT_EQ(res1, "[7, 5, 6]");

  cql("INSERT INTO test (h, v) VALUES (1, [10, 11, 12])");
  cql("UPDATE test SET v[0] = 8 WHERE h = 1");
  auto res2 = ASSERT_RESULT(select());
  EXPECT_EQ(res2, "[8, 11, 12]");
}

TEST_F(CqlTest, Timeout) {
  FLAGS_client_read_write_timeout_ms = 5000 * kTimeMultiplier;

  auto session = ASSERT_RESULT(EstablishSession(driver_.get()));
  ASSERT_OK(session.ExecuteQuery(
      "CREATE TABLE t (i INT PRIMARY KEY, j INT) WITH transactions = { 'enabled' : true }"));

  auto peers = ListTabletPeers(cluster_.get(), ListPeersFilter::kAll);
  for (const auto& peer : peers) {
    peer->raft_consensus()->TEST_DelayUpdate(100ms);
  }

  auto prepared =
      ASSERT_RESULT(session.Prepare("BEGIN TRANSACTION "
                                    "  INSERT INTO t (i, j) VALUES (?, ?);"
                                    "END TRANSACTION;"));
  struct Request {
    CassandraFuture future;
    CoarseTimePoint start_time;
  };
  std::deque<Request> requests;
  constexpr int kOps = 50;
  constexpr int kKey = 42;
  int executed_ops = 0;
  for (;;) {
    while (!requests.empty() && requests.front().future.Ready()) {
      WARN_NOT_OK(requests.front().future.Wait(), "Insert failed");
      auto passed = CoarseMonoClock::now() - requests.front().start_time;
      ASSERT_LE(passed, FLAGS_client_read_write_timeout_ms * 1ms + 2s * kTimeMultiplier);
      requests.pop_front();
    }
    if (executed_ops >= kOps) {
      if (requests.empty()) {
        break;
      }
      std::this_thread::sleep_for(100ms);
      continue;
    }

    auto stmt = prepared.Bind();
    stmt.Bind(0, kKey);
    stmt.Bind(1, ++executed_ops);
    requests.push_back(Request{
        .future = session.ExecuteGetFuture(stmt),
        .start_time = CoarseMonoClock::now(),
    });
  }
}

TEST_F(CqlTest, RecreateTableWithInserts) {
  const auto kNumKeys = 4;
  const auto kNumIters = 2;
  auto session = ASSERT_RESULT(EstablishSession(driver_.get()));
  for (int i = 0; i != kNumIters; ++i) {
    SCOPED_TRACE(Format("Iteration: $0", i));
    ASSERT_OK(session.ExecuteQuery(
        "CREATE TABLE t (k INT PRIMARY KEY, v INT) WITH transactions = { 'enabled' : true }"));
    std::string expr = "BEGIN TRANSACTION ";
    for (int key = 0; key != kNumKeys; ++key) {
      expr += "INSERT INTO t (k, v) VALUES (?, ?); ";
    }
    expr += "END TRANSACTION;";
    auto prepared = ASSERT_RESULT(session.Prepare(expr));
    auto stmt = prepared.Bind();
    size_t idx = 0;
    for (int key = 0; key != kNumKeys; ++key) {
      stmt.Bind(idx++, RandomUniformInt<int32_t>(-1000, 1000));
      stmt.Bind(idx++, -key);
    }
    ASSERT_OK(session.Execute(stmt));
    ASSERT_OK(session.ExecuteQuery("DROP TABLE t"));
  }
}

class CqlThreeMastersTest : public CqlTest {
 public:
  void SetUp() override {
    FLAGS_partitions_vtable_cache_refresh_secs = 0;
    CqlTest::SetUp();
  }

  int num_masters() override { return 3; }
};

Status CheckNumAddressesInYqlPartitionsTable(CassandraSession* session, int expected_num_addrs) {
  const int kReplicaAddressesIndex = 5;
  auto result = VERIFY_RESULT(session->ExecuteWithResult("SELECT * FROM system.partitions"));
  auto iterator = result.CreateIterator();
  while (iterator.Next()) {
    auto replica_addresses = iterator.Row().Value(kReplicaAddressesIndex).ToString();
    ssize_t num_addrs = 0;
    if (replica_addresses.size() > std::strlen("{}")) {
      num_addrs = std::count(replica_addresses.begin(), replica_addresses.end(), ',') + 1;
    }

    EXPECT_EQ(num_addrs, expected_num_addrs);
  }
  return Status::OK();
}

TEST_F_EX(CqlTest, HostnameResolutionFailureInYqlPartitionsTable, CqlThreeMastersTest) {
  auto session = ASSERT_RESULT(EstablishSession(driver_.get()));
  ASSERT_OK(CheckNumAddressesInYqlPartitionsTable(&session, 3));

  // TEST_RpcAddress is 1-indexed.
  string hostname =
      server::TEST_RpcAddress(cluster_->LeaderMasterIdx() + 1, server::Private::kFalse);

  // Fail resolution of the old leader master's hostname.
  TEST_SetFailToFastResolveAddress(hostname);

  // Shutdown the master leader, and wait for new leader to get elected.
  ASSERT_RESULT(cluster_->GetLeaderMiniMaster())->Shutdown();
  ASSERT_RESULT(cluster_->GetLeaderMiniMaster());

  // Assert that a new call will succeed, but will be missing the shutdown master address.
  ASSERT_OK(CheckNumAddressesInYqlPartitionsTable(&session, 2));

  TEST_SetFailToFastResolveAddress("");
}

TEST_F_EX(CqlTest, NonRespondingMaster, CqlThreeMastersTest) {
  FLAGS_TEST_timeout_non_leader_master_rpcs = true;
  auto session = ASSERT_RESULT(EstablishSession(driver_.get()));
  ASSERT_OK(session.ExecuteQuery("CREATE TABLE t1 (i INT PRIMARY KEY, j INT)"));
  ASSERT_OK(session.ExecuteQuery("INSERT INTO t1 (i, j) VALUES (1, 1)"));
  ASSERT_OK(session.ExecuteQuery("CREATE TABLE t2 (i INT PRIMARY KEY, j INT)"));

  LOG(INFO) << "Prepare";
  auto prepared = ASSERT_RESULT(session.Prepare("INSERT INTO t2 (i, j) VALUES (?, ?)"));
  LOG(INFO) << "Step down";
  auto peer = ASSERT_RESULT(cluster_->GetLeaderMiniMaster())->tablet_peer();
  ASSERT_OK(StepDown(peer, std::string(), ForceStepDown::kTrue));
  LOG(INFO) << "Insert";
  FLAGS_client_read_write_timeout_ms = 5000;
  bool has_ok = false;
  for (int i = 0; i != 3; ++i) {
    auto stmt = prepared.Bind();
    stmt.Bind(0, i);
    stmt.Bind(1, 1);
    auto status = session.Execute(stmt);
    if (status.ok()) {
      has_ok = true;
      break;
    }
    ASSERT_NE(status.message().ToBuffer().find("timed out"), std::string::npos) << status;
  }
  ASSERT_TRUE(has_ok);
}

TEST_F(CqlTest, TestTruncateTable) {
  auto session = ASSERT_RESULT(EstablishSession(driver_.get()));
  auto cql = [&](const std::string query) { ASSERT_OK(session.ExecuteQuery(query)); };
  cql("CREATE TABLE users(userid INT PRIMARY KEY, fullname TEXT)");
  cql("INSERT INTO users(userid,fullname) values (1, 'yb');");
  cql("TRUNCATE TABLE users");
  auto result = ASSERT_RESULT(session.ExecuteWithResult("SELECT count(*) FROM users"));
  auto iterator = result.CreateIterator();
  iterator.Next();
  auto count = iterator.Row().Value(0).As<int64>();
  LOG(INFO) << "Count : " << count;
  EXPECT_EQ(count, 0);

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_disable_truncate_table) = true;
  ASSERT_NOK(session.ExecuteQuery("TRUNCATE TABLE users"));
}

TEST_F(CqlTest, ManyColumns) {
  constexpr int kNumRows = 10;
#ifndef NDEBUG
  constexpr int kColumns = RegularBuildVsSanitizers(100, 10);
#else
  constexpr int kColumns = 1000;
#endif

  auto session = ASSERT_RESULT(EstablishSession(driver_.get()));
  std::string expr = "CREATE TABLE t (id INT PRIMARY KEY";
  for (int i = 1; i <= kColumns; ++i) {
    expr += Format(", c$0 INT", i);
  }
  expr += ") WITH tablets = 1";
  ASSERT_OK(session.ExecuteQuery(expr));
  expr = "UPDATE t SET";
  for (int i = 2;; ++i) { // Don't set first column.
    expr += Format(" c$0 = ?", i);
    if (i == kColumns) {
      break;
    }
    expr += ",";
  }
  expr += " WHERE id = ?";
  auto insert_prepared = ASSERT_RESULT(session.Prepare(expr));

  for (int i = 1; i <= kNumRows; ++i) {
    auto stmt = insert_prepared.Bind();
    int idx = 0;
    for (int c = 2; c <= kColumns; ++c) {
      stmt.Bind(idx++, c);
    }
    stmt.Bind(idx++, i);
    ASSERT_OK(session.Execute(stmt));
  }
  auto start = CoarseMonoClock::Now();
  for (int i = 0; i <= 100; ++i) {
    auto value = ASSERT_RESULT(session.FetchValue<int64_t>("SELECT COUNT(c1) FROM t"));
    if (i == 0) {
      ASSERT_EQ(value, 0);
      start = CoarseMonoClock::Now();
    }
  }
  MonoDelta passed = CoarseMonoClock::Now() - start;
  LOG(INFO) << "Passed: " << passed;
}

}  // namespace yb
