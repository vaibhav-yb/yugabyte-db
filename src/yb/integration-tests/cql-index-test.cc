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

#include "yb/integration-tests/cql_test_base.h"
#include "yb/integration-tests/mini_cluster_utils.h"

#include "yb/util/atomic.h"
#include "yb/util/backoff_waiter.h"
#include "yb/util/logging.h"
#include "yb/util/random_util.h"
#include "yb/util/status_log.h"
#include "yb/util/test_thread_holder.h"
#include "yb/util/tsan_util.h"

using namespace std::literals;

DECLARE_bool(allow_index_table_read_write);
DECLARE_int32(cql_prepare_child_threshold_ms);
DECLARE_bool(disable_index_backfill);
DECLARE_bool(transactions_poll_check_aborted);
DECLARE_bool(TEST_disable_proactive_txn_cleanup_on_abort);
DECLARE_int32(client_read_write_timeout_ms);
DECLARE_int32(rpc_workers_limit);
DECLARE_uint64(transaction_manager_workers_limit);
DECLARE_uint64(TEST_inject_txn_get_status_delay_ms);
DECLARE_int64(transaction_abort_check_interval_ms);

namespace yb {

class CqlIndexTest : public CqlTestBase<MiniCluster> {
 public:
  virtual ~CqlIndexTest() = default;

  void TestTxnCleanup(size_t max_remaining_txns_per_tablet);
  void TestConcurrentModify2Columns(const std::string& expr);
};

YB_STRONGLY_TYPED_BOOL(UniqueIndex);

Status CreateIndexedTable(
    CassandraSession* session, UniqueIndex unique_index = UniqueIndex::kFalse) {
  RETURN_NOT_OK(
      session->ExecuteQuery("CREATE TABLE IF NOT EXISTS t (key INT PRIMARY KEY, value INT) WITH "
                            "transactions = { 'enabled' : true }"));
  return session->ExecuteQuery(
      Format("CREATE $0 INDEX IF NOT EXISTS idx ON T (value)", unique_index ? "UNIQUE" : ""));
}

TEST_F(CqlIndexTest, Simple) {
  constexpr int kKey = 1;
  constexpr int kValue = 2;

  auto session = ASSERT_RESULT(EstablishSession(driver_.get()));

  ASSERT_OK(CreateIndexedTable(&session));

  ASSERT_OK(session.ExecuteQuery("INSERT INTO t (key, value) VALUES (1, 2)"));
  auto result = ASSERT_RESULT(session.ExecuteWithResult("SELECT * FROM t WHERE value = 2"));
  auto iter = result.CreateIterator();
  ASSERT_TRUE(iter.Next());
  auto row = iter.Row();
  ASSERT_EQ(row.Value(0).As<cass_int32_t>(), kKey);
  ASSERT_EQ(row.Value(1).As<cass_int32_t>(), kValue);
  ASSERT_FALSE(iter.Next());
}

TEST_F(CqlIndexTest, MultipleIndex) {
  FLAGS_disable_index_backfill = false;
  auto session1 = ASSERT_RESULT(EstablishSession(driver_.get()));
  auto session2 = ASSERT_RESULT(EstablishSession(driver_.get()));

  WARN_NOT_OK(session1.ExecuteQuery(
      "CREATE TABLE t (key INT PRIMARY KEY, value INT) WITH transactions = { 'enabled' : true }"),
          "Create table failed.");
  auto future1 = session1.ExecuteGetFuture("CREATE INDEX idx1 ON T (value)");
  auto future2 = session2.ExecuteGetFuture("CREATE INDEX idx2 ON T (value)");

  constexpr auto kNamespace = "test";
  const client::YBTableName table_name(YQL_DATABASE_CQL, kNamespace, "t");
  const client::YBTableName index_table_name1(YQL_DATABASE_CQL, kNamespace, "idx1");
  const client::YBTableName index_table_name2(YQL_DATABASE_CQL, kNamespace, "idx2");

  LOG(INFO) << "Waiting for idx1 got " << future1.Wait();
  auto perm1 = ASSERT_RESULT(client_->WaitUntilIndexPermissionsAtLeast(
      table_name, index_table_name1, IndexPermissions::INDEX_PERM_READ_WRITE_AND_DELETE));
  CHECK_EQ(perm1, IndexPermissions::INDEX_PERM_READ_WRITE_AND_DELETE);
  LOG(INFO) << "Waiting for idx2 got " << future2.Wait();
  auto perm2 = ASSERT_RESULT(client_->WaitUntilIndexPermissionsAtLeast(
      table_name, index_table_name2, IndexPermissions::INDEX_PERM_READ_WRITE_AND_DELETE));
  CHECK_EQ(perm2, IndexPermissions::INDEX_PERM_READ_WRITE_AND_DELETE);
}

class CqlIndexSmallWorkersTest : public CqlIndexTest {
 public:
  void SetUp() override {
    FLAGS_rpc_workers_limit = 4;
    FLAGS_transaction_manager_workers_limit = 4;
    CqlIndexTest::SetUp();
  }
};

TEST_F_EX(CqlIndexTest, ConcurrentIndexUpdate, CqlIndexSmallWorkersTest) {
  constexpr int kThreads = 8;
  constexpr cass_int32_t kKeys = kThreads / 2;
  constexpr cass_int32_t kValues = kKeys;
  constexpr int kNumInserts = kThreads * 5;

  FLAGS_client_read_write_timeout_ms = 10000 * kTimeMultiplier;
  SetAtomicFlag(1000, &FLAGS_TEST_inject_txn_get_status_delay_ms);
  FLAGS_transaction_abort_check_interval_ms = 100000;

  auto session = ASSERT_RESULT(EstablishSession(driver_.get()));

  ASSERT_OK(LoggedWaitFor(
      [&session]() { return CreateIndexedTable(&session).ok(); }, 60s, "create table", 12s));
  constexpr auto kNamespace = "test";
  const client::YBTableName table_name(YQL_DATABASE_CQL, kNamespace, "t");
  const client::YBTableName index_table_name(YQL_DATABASE_CQL, kNamespace, "idx");
  ASSERT_OK(LoggedWaitFor(
      [this, table_name, index_table_name]() {
        auto result = client_->WaitUntilIndexPermissionsAtLeast(
            table_name, index_table_name, IndexPermissions::INDEX_PERM_READ_WRITE_AND_DELETE);
        return result.ok() && *result == IndexPermissions::INDEX_PERM_READ_WRITE_AND_DELETE;
      },
      90s,
      "wait for create index to complete",
      12s));

  TestThreadHolder thread_holder;
  std::atomic<int> inserts(0);
  for (int i = 0; i != kThreads; ++i) {
    thread_holder.AddThreadFunctor([this, &inserts, &stop = thread_holder.stop_flag()] {
      auto session = ASSERT_RESULT(EstablishSession(driver_.get()));
      auto prepared = ASSERT_RESULT(session.Prepare("INSERT INTO t (key, value) VALUES (?, ?)"));
      while (!stop.load(std::memory_order_acquire)) {
        auto stmt = prepared.Bind();
        stmt.Bind(0, RandomUniformInt<cass_int32_t>(1, kKeys));
        stmt.Bind(1, RandomUniformInt<cass_int32_t>(1, kValues));
        auto status = session.Execute(stmt);
        if (status.ok()) {
          ++inserts;
        } else {
          LOG(INFO) << "Insert failed: " << status;
        }
      }
    });
  }

  while (!thread_holder.stop_flag().load(std::memory_order_acquire)) {
    auto num_inserts = inserts.load(std::memory_order_acquire);
    if (num_inserts >= kNumInserts) {
      break;
    }
    YB_LOG_EVERY_N_SECS(INFO, 5) << "Num inserts " << num_inserts << " of " << kNumInserts;
    std::this_thread::sleep_for(100ms);
  }

  thread_holder.Stop();

  SetAtomicFlag(0, &FLAGS_TEST_inject_txn_get_status_delay_ms);
}

TEST_F(CqlIndexTest, TestSaturatedWorkers) {
  /*
   * (#11258) We set a very short timeout to force failure if child transaction
   * is not created quickly enough.

   * TODO: when switching to a fully asynchronous model, this failure will disappear.
   */
  FLAGS_cql_prepare_child_threshold_ms = 1;

  auto session = ASSERT_RESULT(EstablishSession(driver_.get()));
  ASSERT_OK(session.ExecuteQuery(
      "CREATE TABLE t (key INT PRIMARY KEY, v1 INT, v2 INT) WITH "
      "transactions = { 'enabled' : true }"));
  ASSERT_OK(session.ExecuteQuery(
      "CREATE INDEX i1 ON t(key, v1) WITH "
      "transactions = { 'enabled' : true }"));
  ASSERT_OK(session.ExecuteQuery(
      "CREATE INDEX i2 ON t(key, v2) WITH "
      "transactions = { 'enabled' : true }"));

  constexpr int kKeys = 10000;
  std::string expr = "BEGIN TRANSACTION ";
  for (int i = 0; i < kKeys; i++) {
    expr += Format("INSERT INTO t (key, v1, v2) VALUES ($0, $1, $2); ", i, i, i);
  }
  expr += "END TRANSACTION;";

  // We should expect to see timed out error
  auto status = session.ExecuteQuery(expr);
  ASSERT_FALSE(status.ok());
  ASSERT_NE(status.message().ToBuffer().find("Timed out waiting for prepare child status"),
            std::string::npos) << status;
}

YB_STRONGLY_TYPED_BOOL(CheckReady);

void CleanFutures(std::deque<CassandraFuture>* futures, CheckReady check_ready) {
  while (!futures->empty() && (!check_ready || futures->front().Ready())) {
    auto status = futures->front().Wait();
    if (!status.ok() && !status.IsTimedOut()) {
      auto msg = status.message().ToBuffer();
      if (msg.find("Duplicate value disallowed by unique index") == std::string::npos) {
        ASSERT_OK(status);
      }
    }
    futures->pop_front();
  }
}

void CqlIndexTest::TestTxnCleanup(size_t max_remaining_txns_per_tablet) {
  auto session = ASSERT_RESULT(EstablishSession(driver_.get()));

  ASSERT_OK(CreateIndexedTable(&session, UniqueIndex::kTrue));
  std::deque<CassandraFuture> futures;

  auto prepared = ASSERT_RESULT(session.Prepare("INSERT INTO t (key, value) VALUES (?, ?)"));

  for (int i = 0; i != RegularBuildVsSanitizers(100, 30); ++i) {
    ASSERT_NO_FATALS(CleanFutures(&futures, CheckReady::kTrue));

    auto stmt = prepared.Bind();
    stmt.Bind(0, i);
    stmt.Bind(1, RandomUniformInt<cass_int32_t>(1, 10));
    futures.push_back(session.ExecuteGetFuture(stmt));
  }

  ASSERT_NO_FATALS(CleanFutures(&futures, CheckReady::kFalse));

  AssertRunningTransactionsCountLessOrEqualTo(cluster_.get(), max_remaining_txns_per_tablet);
}

// Test proactive aborted transactions cleanup.
TEST_F(CqlIndexTest, TxnCleanup) {
  FLAGS_transactions_poll_check_aborted = false;

  TestTxnCleanup(/* max_remaining_txns_per_tablet= */ 5);
}

// Test poll based aborted transactions cleanup.
TEST_F(CqlIndexTest, TxnPollCleanup) {
  FLAGS_TEST_disable_proactive_txn_cleanup_on_abort = true;
  FLAGS_transaction_abort_check_interval_ms = 1000;

  TestTxnCleanup(/* max_remaining_txns_per_tablet= */ 0);
}

void CqlIndexTest::TestConcurrentModify2Columns(const std::string& expr) {
  FLAGS_allow_index_table_read_write = true;

  constexpr int kKeys = RegularBuildVsSanitizers(50, 10);

  auto session = ASSERT_RESULT(EstablishSession(driver_.get()));
  ASSERT_OK(session.ExecuteQuery(
      "CREATE TABLE t (key INT PRIMARY KEY, v1 INT, v2 INT) WITH "
      "transactions = { 'enabled' : true }"));
  ASSERT_OK(session.ExecuteQuery("CREATE INDEX v1_idx ON t (v1)"));

  // Wait for the table alterations to complete.
  std::this_thread::sleep_for(5000ms);

  auto prepared1 = ASSERT_RESULT(session.Prepare(Format(expr, "v1")));
  auto prepared2 = ASSERT_RESULT(session.Prepare(Format(expr, "v2")));

  std::vector<CassandraFuture> futures;

  for (int i = 0; i != kKeys; ++i) {
    for (auto* prepared : {&prepared1, &prepared2}) {
      auto statement = prepared->Bind();
      statement.Bind(0, i);
      statement.Bind(1, i);
      futures.push_back(session.ExecuteGetFuture(statement));
    }
  }

  int good = 0;
  int bad = 0;
  for (auto& future : futures) {
    auto status = future.Wait();
    if (status.ok()) {
      ++good;
    } else {
      ASSERT_TRUE(status.IsTimedOut()) << status;
      ++bad;
    }
  }

  ASSERT_GE(good, bad * 4);

  std::vector<bool> present(kKeys);

  int read_keys = 0;
  auto result = ASSERT_RESULT(session.ExecuteWithResult("SELECT * FROM v1_idx"));
  auto iter = result.CreateIterator();
  while (iter.Next()) {
    auto row = iter.Row();
    auto key = row.Value(0).As<int32_t>();
    auto value = row.Value(1);
    LOG(INFO) << key << ", " << value.ToString();

    ASSERT_FALSE(present[key]) << key;
    present[key] = true;
    ++read_keys;
    ASSERT_FALSE(value.IsNull());
    ASSERT_EQ(key, value.As<int32_t>());
  }

  ASSERT_EQ(read_keys, kKeys);
}

TEST_F(CqlIndexTest, ConcurrentInsert2Columns) {
  TestConcurrentModify2Columns("INSERT INTO t ($0, key) VALUES (?, ?)");
}

TEST_F(CqlIndexTest, ConcurrentUpdate2Columns) {
  TestConcurrentModify2Columns("UPDATE t SET $0 = ? WHERE key = ?");
}

} // namespace yb
