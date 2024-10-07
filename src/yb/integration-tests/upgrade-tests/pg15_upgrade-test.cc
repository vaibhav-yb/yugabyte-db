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

#include "yb/integration-tests/upgrade-tests/pg15_upgrade_test_base.h"

#include "yb/yql/pgwrapper/libpq_utils.h"

namespace yb {

class Pg15UpgradeTest : public Pg15UpgradeTestBase {
 public:
  Pg15UpgradeTest() = default;
};

TEST_F(Pg15UpgradeTest, CheckVersion) {
  const auto kSelectVersion = "SELECT version()";
  {
    auto conn = ASSERT_RESULT(cluster_->ConnectToDB());
    auto version = ASSERT_RESULT(conn.FetchRowAsString(kSelectVersion));
    ASSERT_STR_CONTAINS(version, old_version_info().version);
  }

  ASSERT_OK(UpgradeClusterToMixedMode());

  {
    auto conn = ASSERT_RESULT(CreateConnToTs(kMixedModeTserverPg15));
    auto version = ASSERT_RESULT(conn.FetchRowAsString(kSelectVersion));
    ASSERT_STR_CONTAINS(version, current_version_info().version_number());
  }
  {
    auto conn = ASSERT_RESULT(CreateConnToTs(kMixedModeTserverPg11));
    auto version = ASSERT_RESULT(conn.FetchRowAsString(kSelectVersion));
    ASSERT_STR_CONTAINS(version, old_version_info().version);
  }

  ASSERT_OK(FinalizeUpgradeFromMixedMode());

  {
    auto conn = ASSERT_RESULT(cluster_->ConnectToDB());
    auto version = ASSERT_RESULT(conn.FetchRowAsString(kSelectVersion));
    ASSERT_STR_CONTAINS(version, current_version_info().version_number());
  }
}

TEST_F(Pg15UpgradeTest, SimpleTable) {
  const size_t kRowCount = 100;
  // Create a table with 3 tablets and kRowCount rows so that each tablet has at least a few rows.
  ASSERT_OK(ExecuteStatements(
      {"CREATE TABLE t (a INT) SPLIT INTO 3 TABLETS",
       Format("INSERT INTO t VALUES(generate_series(1, $0))", kRowCount)}));
  static const auto kSelectFromTable = "SELECT * FROM t";

  ASSERT_OK(UpgradeClusterToMixedMode());

  {
    auto conn = ASSERT_RESULT(CreateConnToTs(kMixedModeTserverPg15));
    auto count = ASSERT_RESULT(conn.Fetch(kSelectFromTable));
    ASSERT_EQ(PQntuples(count.get()), kRowCount);
  }
  {
    auto conn = ASSERT_RESULT(CreateConnToTs(kMixedModeTserverPg11));
    auto count = ASSERT_RESULT(conn.Fetch(kSelectFromTable));
    ASSERT_EQ(PQntuples(count.get()), kRowCount);
  }

  ASSERT_OK(FinalizeUpgradeFromMixedMode());

  // Verify row count from a random tserver.
  {
    auto conn = ASSERT_RESULT(cluster_->ConnectToDB());
    auto count = ASSERT_RESULT(conn.Fetch(kSelectFromTable));
    ASSERT_EQ(PQntuples(count.get()), kRowCount);
  }
}

TEST_F(Pg15UpgradeTest, BackslashD) {
  ASSERT_OK(ExecuteStatement("CREATE TABLE t (a INT)"));
  static const auto kBackslashD = "\\d";
  static const auto kExpectedResult =
      "        List of relations\n"
      " Schema | Name | Type  |  Owner   \n"
      "--------+------+-------+----------\n"
      " public | t    | table | postgres\n"
      "(1 row)\n\n";

  auto result = ASSERT_RESULT(ExecuteViaYsqlsh(kBackslashD));
  ASSERT_EQ(result, kExpectedResult);

  ASSERT_OK(UpgradeClusterToMixedMode());

  result = ASSERT_RESULT(ExecuteViaYsqlshOnTs(kBackslashD, kMixedModeTserverPg15));
  ASSERT_EQ(result, kExpectedResult);
  result = ASSERT_RESULT(ExecuteViaYsqlshOnTs(kBackslashD, kMixedModeTserverPg11));
  ASSERT_EQ(result, kExpectedResult);

  ASSERT_OK(FinalizeUpgradeFromMixedMode());

  // Verify the result from a random tserver.
  result = ASSERT_RESULT(ExecuteViaYsqlsh(kBackslashD));
  ASSERT_EQ(result, kExpectedResult);
}

TEST_F(Pg15UpgradeTest, Comments) {
  const auto kPg11DatabaseComment = "PG11: [db] I came first!";
  const auto kPg11TableComment = "PG11: [table] I came first!";
  ASSERT_OK(ExecuteStatements(
      {"CREATE TABLE t (a int)",
       Format("COMMENT ON DATABASE yugabyte IS '$0'", kPg11DatabaseComment),
       Format("COMMENT ON TABLE t IS '$0'", kPg11TableComment)}));

  ASSERT_OK(UpgradeClusterToMixedMode());

  const auto kSelectDatabaseComment =
      "SELECT pg_catalog.shobj_description(d.oid, 'pg_database') FROM pg_catalog.pg_database d "
      "WHERE datname = 'yugabyte'";
  const auto kSelectTableComment =
      "SELECT description from pg_description JOIN pg_class on pg_description.objoid = "
      "pg_class.oid WHERE relname = 't'";

  auto check_pg11_comment = [&](pgwrapper::PGConn& conn) {
    auto comment = ASSERT_RESULT(conn.FetchRow<std::string>(kSelectDatabaseComment));
    ASSERT_EQ(comment, kPg11DatabaseComment);
    comment = ASSERT_RESULT(conn.FetchRow<std::string>(kSelectTableComment));
    ASSERT_EQ(comment, kPg11TableComment);
  };

  {
    auto conn = ASSERT_RESULT(CreateConnToTs(kMixedModeTserverPg15));
    ASSERT_NO_FATALS(check_pg11_comment(conn));
  }
  {
    auto conn = ASSERT_RESULT(CreateConnToTs(kMixedModeTserverPg11));
    ASSERT_NO_FATALS(check_pg11_comment(conn));
  }

  ASSERT_OK(FinalizeUpgradeFromMixedMode());

  // Check the comment from a random tserver.
  {
    auto conn = ASSERT_RESULT(cluster_->ConnectToDB());
    ASSERT_NO_FATALS(check_pg11_comment(conn));
  }

  // Update the comment.
  const auto kPg15DatabaseComment = "PG15: [db] I am better than you!";
  const auto kPg15TableComment = "PG15: [table] I am better than you!";
  ASSERT_OK(ExecuteStatements(
      {Format("COMMENT ON DATABASE yugabyte IS '$0'", kPg15DatabaseComment),
       Format("COMMENT ON TABLE t IS '$0'", kPg15TableComment)}));

  // Check the new comment from a random tserver.
  {
    auto conn = ASSERT_RESULT(cluster_->ConnectToDB());
    auto comment = ASSERT_RESULT(conn.FetchRow<std::string>(kSelectDatabaseComment));
    ASSERT_EQ(comment, kPg15DatabaseComment);
    comment = ASSERT_RESULT(conn.FetchRow<std::string>(kSelectTableComment));
    ASSERT_EQ(comment, kPg15TableComment);
  }
}

TEST_F(Pg15UpgradeTest, Schemas) {
  const auto kSchemaA = "schema_a";
  const auto kSchemaB = "schema_b";
  const auto kPublic = "public";

  const auto kSchemaATable = "s_schema_table_a";
  const auto kSchemaBTable = "s_schema_table_b";
  const auto kDefaultTable = "d_schema_table";
  const auto kDefaultTable2 = "d_schema_table_2";
  const auto kPublicTable = "p_schema_table";
  const auto kPublicTable2 = "p_schema_table_2";

  // This query returns rows in the format "schema.table"
  static const auto kGetTables =
      Format("SELECT nspname || '.' || relname FROM pg_class c "
             "JOIN pg_namespace n ON c.relnamespace = n.oid "
             "WHERE nspname IN ('$0', '$1', '$2') AND relname LIKE '%%schema_table%%' "
             "ORDER BY nspname, relname ASC",
             kSchemaA, kSchemaB, kPublic);

  // YB_TODO: When `CREATE SCHEMA` is the first command in this sequence, it fails with the error:
  // ERROR:  this ddl statement is currently not allowed
  // DETAIL:  The pg_yb_catalog_version table is not in per-database catalog version mode.
  // HINT:  Fix pg_yb_catalog_version table to per-database catalog version mode.
  // (This is before anything upgrade-related occurs)
  ASSERT_OK(ExecuteStatements(
      {Format("CREATE TABLE $0.$1 (a INT)", kPublic, kPublicTable),
       Format("CREATE SCHEMA $0", kSchemaA),
       Format("CREATE TABLE $0.$1 (a INT)", kSchemaA, kSchemaATable),
       Format("CREATE TABLE $0 (a INT)", kDefaultTable)}));

  ASSERT_OK(UpgradeClusterToMixedMode());

  auto check_tables = [&](pgwrapper::PGConn& conn) {
    const auto results = ASSERT_RESULT(conn.FetchRows<std::string>(kGetTables));
    ASSERT_EQ(results.size(), 3);
    ASSERT_STR_CONTAINS(results[0], Format("$0.$1", kPublic, kDefaultTable));
    ASSERT_STR_CONTAINS(results[1], Format("$0.$1", kPublic, kPublicTable));
    ASSERT_STR_CONTAINS(results[2], Format("$0.$1", kSchemaA, kSchemaATable));

    // Check that each table can be selected from (proving it's more than just an entry in pg_class)
    const auto joined_rows = ASSERT_RESULT(conn.FetchRow<pgwrapper::PGUint64>(
      Format("SELECT COUNT(*) FROM $0.$1, $2, $3",
             kSchemaA, kSchemaATable, kDefaultTable, kPublicTable)));
    ASSERT_EQ(joined_rows, 0);
  };

  {
    auto conn = ASSERT_RESULT(CreateConnToTs(kMixedModeTserverPg15));
    ASSERT_NO_FATALS(check_tables(conn));
  }
  {
    auto conn = ASSERT_RESULT(CreateConnToTs(kMixedModeTserverPg11));
    ASSERT_NO_FATALS(check_tables(conn));
  }

  ASSERT_OK(FinalizeUpgradeFromMixedMode());

  // Check the tables from a random tserver
  {
    auto conn = ASSERT_RESULT(cluster_->ConnectToDB());
    ASSERT_NO_FATALS(check_tables(conn));
  }

  // Create a new schema and tables
  ASSERT_OK(ExecuteStatements(
      {Format("CREATE SCHEMA $0", kSchemaB),
       Format("CREATE TABLE $0.$1 (a INT)", kPublic, kPublicTable2),
       Format("CREATE TABLE $0.$1 (a INT)", kSchemaB, kSchemaBTable),
       Format("CREATE TABLE $0 (a INT)", kDefaultTable2)}));

  // Check the new tables from a random tserver
  {
    auto conn = ASSERT_RESULT(cluster_->ConnectToDB());
    const auto results = ASSERT_RESULT(conn.FetchRows<std::string>(kGetTables));
    ASSERT_EQ(results.size(), 6);
    int idx = 0;
    ASSERT_STR_CONTAINS(results[idx++], Format("$0.$1", kPublic, kDefaultTable));
    ASSERT_STR_CONTAINS(results[idx++], Format("$0.$1", kPublic, kDefaultTable2));
    ASSERT_STR_CONTAINS(results[idx++], Format("$0.$1", kPublic, kPublicTable));
    ASSERT_STR_CONTAINS(results[idx++], Format("$0.$1", kPublic, kPublicTable2));
    ASSERT_STR_CONTAINS(results[idx++], Format("$0.$1", kSchemaA, kSchemaATable));
    ASSERT_STR_CONTAINS(results[idx++], Format("$0.$1", kSchemaB, kSchemaBTable));

    // Check that each table can be selected from (proving it's more than just an entry in pg_class)
    const auto joined_rows = ASSERT_RESULT(conn.FetchRow<pgwrapper::PGUint64>(
        Format("SELECT COUNT(*) FROM $0.$1, $2.$3, $4, $5, $6, $7",
               kSchemaA, kSchemaATable, kSchemaB, kSchemaBTable,
               kDefaultTable, kDefaultTable2, kPublicTable, kPublicTable2)));
    ASSERT_EQ(joined_rows, 0);
  }
}

}  // namespace yb
