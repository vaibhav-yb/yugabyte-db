// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations
// under the License.
//

package org.yb.pgsql;

import static org.yb.AssertionWrappers.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.yb.YBTestRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(value = YBTestRunner.class)
public class TestYbQueryDiagnostics extends BasePgSQLTest {
    public class QueryDiagnosticsParams {
        private final int diagnosticsInterval;
        private final int explainSampleRate;
        private final boolean explainAnalyze;
        private final boolean explainDist;
        private final boolean explainDebug;
        private final int bindVarQueryMinDuration;

        public QueryDiagnosticsParams(int diagnosticsInterval, int explainSampleRate,
                                      boolean explainAnalyze, boolean explainDist,
                                      boolean explainDebug, int bindVarQueryMinDuration) {
            this.diagnosticsInterval = diagnosticsInterval;
            this.explainSampleRate = explainSampleRate;
            this.explainAnalyze = explainAnalyze;
            this.explainDist = explainDist;
            this.explainDebug = explainDebug;
            this.bindVarQueryMinDuration = bindVarQueryMinDuration;
        }
    }

    private static final int ASH_SAMPLING_INTERVAL_MS = 500;
    private static final String noQueriesExecutedWarning = "No query executed;";
    private static final String pgssResetWarning =
        "pg_stat_statements was reset, query string not available;";
    private static final String permissionDeniedWarning =
        "Failed to create query diagnostics directory, Permission denied;";

    @Before
    public void setUp() throws Exception {
        /* Set Gflags and restart cluster */
        Map<String, String> flagMap = super.getTServerFlags();
        flagMap.put("TEST_yb_enable_query_diagnostics", "true");
        /* Enable ASH for active_session_history.csv */
        if (isTestRunningWithConnectionManager()) {
            flagMap.put("allowed_preview_flags_csv",
                    "ysql_yb_ash_enable_infra,ysql_yb_enable_ash,enable_ysql_conn_mgr");
        } else {
            flagMap.put("allowed_preview_flags_csv",
                        "ysql_yb_ash_enable_infra,ysql_yb_enable_ash");
        }
        flagMap.put("ysql_yb_ash_enable_infra", "true");
        flagMap.put("ysql_yb_enable_ash", "true");
        flagMap.put("ysql_yb_ash_sampling_interval_ms",
                    String.valueOf(ASH_SAMPLING_INTERVAL_MS));

        restartClusterWithFlags(Collections.emptyMap(), flagMap);

        setUpPreparedStatement();
    }

    public void setUpPreparedStatement() throws Exception {
        try (Statement statement = connection.createStatement()) {
            /* Creating test table and filling dummy data */
            statement.execute("CREATE TABLE test_table(a TEXT, b INT, c FLOAT)");
            statement.execute("PREPARE stmt(TEXT, INT, FLOAT) AS SELECT * FROM test_table " +
                              "WHERE a = $1 AND b = $2 AND c = $3;");
            statement.execute("EXECUTE stmt('var', 1, 1.1)");
        }
    }

    private String getQueryIdFromPgStatStatements(Statement statement, String pattern)
                                                  throws Exception {
        /* Get query id of the prepared statement */
        ResultSet resultSet = statement.executeQuery("SELECT queryid FROM pg_stat_statements " +
                                                     "WHERE query LIKE '" + pattern + "'");
        if (!resultSet.next())
            fail("Query id not found in pg_stat_statements");

        return resultSet.getString("queryid");
    }

    private Path runQueryDiagnostics(Statement statement, String queryId,
                                     QueryDiagnosticsParams params) throws Exception {
        /* Run query diagnostics on the prepared stmt */
        String query = "SELECT * FROM yb_query_diagnostics( " +
                       "query_id => " + queryId +
                       ",diagnostics_interval_sec => " + params.diagnosticsInterval +
                       ",bind_var_query_min_duration_ms =>" + params.bindVarQueryMinDuration +
                       ",explain_analyze => " + params.explainAnalyze +
                       ",explain_dist => " + params.explainDist +
                       ",explain_debug => " + params.explainDebug +
                       ",explain_sample_rate => " + params.explainSampleRate + ")";
        ResultSet resultSet = statement.executeQuery(query);

        if (!resultSet.next())
            fail("yb_query_diagnostics() function failed");

        /* Returns bundle path */
        return Paths.get(resultSet.getString("yb_query_diagnostics"));
    }

    private void assertQueryDiagnosticsStatus(ResultSet resultSet, Path expectedPath,
                                              String expectedStatus,
                                              String expectedDescription,
                                              QueryDiagnosticsParams expectedParams)
                                              throws SQLException {
        Path viewPath = Paths.get(resultSet.getString("path"));
        String status = resultSet.getString("status");
        String description = resultSet.getString("description");
        int diagnosticsInterval = resultSet.getInt("diagnostics_interval_sec");
        int bindVarQueryMinDuration = resultSet.getInt("bind_var_query_min_duration_ms");
        String explainParamsString = resultSet.getString("explain_params");
        JSONObject explainParams = new JSONObject(explainParamsString);

        assertEquals("yb_query_diagnostics_status returns wrong path",
                     expectedPath, viewPath);
        assertEquals("yb_query_diagnostics_status returns wrong status",
                     expectedStatus, status);
        assertEquals("yb_query_diagnostics_status returns wrong description",
                     expectedDescription, description);
        assertEquals("yb_query_diagnostics_status returns wrong diagnostics_interval_sec",
                     expectedParams.diagnosticsInterval, (Object) diagnosticsInterval);
        assertEquals("yb_query_diagnostics_status returns wrong" +
                     "bind_var_query_min_duration_ms",
                     expectedParams.bindVarQueryMinDuration, (Object) bindVarQueryMinDuration);
        assertEquals("yb_query_diagnostics_status returns wrong explain_params",
                     expectedParams.explainAnalyze, explainParams.getBoolean("explain_analyze"));
        assertEquals("yb_query_diagnostics_status returns wrong explain_params",
                     expectedParams.explainDebug, explainParams.getBoolean("explain_debug"));
        assertEquals("yb_query_diagnostics_status returns wrong explain_params",
                     expectedParams.explainDist, explainParams.getBoolean("explain_dist"));
        assertEquals("yb_query_diagnostics_status returns wrong explain_params",
                     expectedParams.explainSampleRate, explainParams.getInt("explain_sample_rate"));
    }

    @Test
    public void checkBindVariablesData() throws Exception {
        int diagnosticsInterval = 2;
        QueryDiagnosticsParams params = new QueryDiagnosticsParams(
            diagnosticsInterval,
            100 /* explainSampleRate */,
            true /* explainAnalyze */,
            true /* explainDist */,
            false /* explainDebug */,
            0 /* bindVarQueryMinDuration */);

        try (Statement statement = connection.createStatement()) {
            /* Run query diagnostics on the prepared stmt */
            String queryId = getQueryIdFromPgStatStatements(statement, "PREPARE%");
            Path bundleDataPath = runQueryDiagnostics(statement, queryId, params);

            /* Generate some data to be dumped */
            statement.execute("EXECUTE stmt('var1', 1, 1.1)");
            statement.execute("EXECUTE stmt('var2', 2, 2.2)");

            /*
             * Thread sleeps for diagnosticsInterval + 1 sec to ensure that the bundle has expired
            */
            Thread.sleep((diagnosticsInterval + 1) * 1000);

            Path bindVarPath = bundleDataPath.resolve("bind_variables.csv");

            assertTrue("Bind variables file does not exist", Files.exists(bindVarPath));
            assertGreaterThan("bind_variables.csv file size is not greater than 0",
                              Files.size(bindVarPath) , 0L);

            List<String> lines = Files.readAllLines(bindVarPath);
            assertEquals("Number of lines in bind_variables.csv is not as expected",
                         lines.size(), 2);
            assertTrue("bind_variables.csv does not contain expected data",
                       lines.get(0).contains("var1,1,1.1") &&
                       lines.get(1).contains("var2,2,2.2"));
        }
    }

    @Test
    public void testYbQueryDiagnosticsStatus() throws Exception {
        int diagnosticsInterval = 2;
        int sleep_time_s = diagnosticsInterval + 1;

        try (Statement statement = connection.createStatement()) {
            /* Run query diagnostics on the prepared stmt */
            String queryId = getQueryIdFromPgStatStatements(statement, "PREPARE%");
            QueryDiagnosticsParams successfulRunParams = new QueryDiagnosticsParams(
                diagnosticsInterval,
                100 /* explainSampleRate */,
                true /* explainAnalyze */,
                true /* explainDist */,
                false /* explainDebug */,
                0 /* bindVarQueryMinDuration */);

            QueryDiagnosticsParams fileErrorRunParams = new QueryDiagnosticsParams(
                diagnosticsInterval,
                50 /* explainSampleRate */,
                true /* explainAnalyze */,
                true /* explainDist */,
                true /* explainDebug */,
                10 /* bindVarQueryMinDuration */);

            QueryDiagnosticsParams inProgressRunParams = new QueryDiagnosticsParams(
                120 /* diagnosticsInterval */,
                75 /* explainSampleRate */,
                false /* explainAnalyze */,
                false /* explainDist */,
                false /* explainDebug */,
                15 /* bindVarQueryMinDuration */);

            /*
             * Trigger the successful bundle
             */
            Path successfulBundlePath = runQueryDiagnostics(statement, queryId,
                                                            successfulRunParams);

            /* Generate some data to be dumped */
            statement.execute("EXECUTE stmt('var1', 2, 2.2)");
            statement.execute("EXECUTE stmt('var2', 3, 3.3)");

            /*
             * Thread sleeps for diagnosticsInterval + 1 sec to ensure that the bundle has expired
            */
            statement.execute("SELECT pg_sleep(" + sleep_time_s + ")");

            /*
             * Trigger the bundle with file error
             */
            Path fileErrorBundlePath = runQueryDiagnostics(statement, queryId, fileErrorRunParams);

            /* Generate some data to be dumped */
            statement.execute("EXECUTE stmt('var1', 2, 2.2)");
            statement.execute("EXECUTE stmt('var2', 3, 3.3)");

            String query_diagnostics_path = fileErrorBundlePath.getParent().getParent().toString();

            /* Postgres cannot write to query-diagnostics folder */
            recreateFolderWithPermissions(query_diagnostics_path, 400);

            /*
             * Thread sleeps for diagnosticsInterval + 1 sec to ensure that the bundle has expired
            */
            statement.execute("SELECT pg_sleep(" + sleep_time_s + ")");

            /* Reset permissions to allow test cleanup */
            recreateFolderWithPermissions(query_diagnostics_path, 666);

            /*
             * Trigger the bundle for 120 seconds to ensure it remains in In Progress state
             */
            Path inProgressBundlePath = runQueryDiagnostics(statement, queryId,
                                                            inProgressRunParams);
            ResultSet resultSet = statement.executeQuery(
                                    "SELECT * FROM yb_query_diagnostics_status " +
                                    "WHERE status='Success'");

            if (!resultSet.next())
                fail("yb_query_diagnostics_status view does not have expected data");

            assertQueryDiagnosticsStatus(resultSet,
                                         successfulBundlePath /* expectedViewPath */,
                                         "Success" /* expectedStatus */,
                                         "" /* expectedDescription */,
                                         successfulRunParams);

            resultSet = statement.executeQuery(
                                    "SELECT * FROM yb_query_diagnostics_status " +
                                    "WHERE status='Error'");

            if (!resultSet.next())
                fail("yb_query_diagnostics_status view does not have expected data");

            assertQueryDiagnosticsStatus(resultSet,
                                         fileErrorBundlePath.getParent() /* expectedViewPath */,
                                         "Error" /* expectedStatus */,
                                         permissionDeniedWarning /* expectedDescription */,
                                         fileErrorRunParams);

            resultSet = statement.executeQuery(
                                    "SELECT * FROM yb_query_diagnostics_status " +
                                    "WHERE status='In Progress'");

            if (!resultSet.next())
                fail("yb_query_diagnostics_status view does not have expected data");

            assertQueryDiagnosticsStatus(resultSet,
                                         inProgressBundlePath /* expectedViewPath */,
                                         "In Progress" /* expectedStatus */,
                                         "" /* expectedDescription */,
                                         inProgressRunParams);
        }
    }

    private void recreateFolderWithPermissions(String query_diagnostics_path, int permissions)
                                               throws Exception {
        List<String> commands = Arrays.asList(
            "rm -rf " + query_diagnostics_path,
            "mkdir " + query_diagnostics_path,
            "chmod " + permissions + " " + query_diagnostics_path
        );
        execute(commands);
    }

    private static void execute(List<String> commands) throws Exception{
        for (String command : commands) {
            Process process = Runtime.getRuntime().exec(new String[]{"bash", "-c", command});
            process.waitFor();
        }
    }

    @Test
    public void testBufferSizeUpdateAfterShmemInit() throws Exception {
        try (Statement statement = connection.createStatement()) {
            try {
               statement.executeQuery("set yb_query_diagnostics_circular_buffer_size to 50");
            }
            catch (SQLException e) {
                assertTrue("Error message does not contain expected message",
                           e.getMessage().contains("parameter" +
                           " \"yb_query_diagnostics_circular_buffer_size\" " +
                           "cannot be changed without restarting the server"));
            }
        }
    }

    @Test
    public void testCircularBufferWrapAround() throws Exception {
        /* Set Gflags and restart cluster */
        Map<String, String> flagMap = super.getTServerFlags();
        flagMap.put("TEST_yb_enable_query_diagnostics", "true");
        flagMap.put("ysql_pg_conf_csv", "yb_query_diagnostics_circular_buffer_size=15");
        restartClusterWithFlags(Collections.emptyMap(), flagMap);

        try (Statement statement = connection.createStatement()) {
            /* running several bundles ensure buffer wraps around */
            runMultipleBundles(statement, 100);
        }
    }

    private void runMultipleBundles(Statement statement, int n) throws Exception {
        int diagnosticsInterval = 1;
        int queryId = 0;

        QueryDiagnosticsParams params = new QueryDiagnosticsParams(
            diagnosticsInterval,
            100 /* explainSampleRate */,
            false /* explainAnalyze */,
            false/* explainDist */,
            false/* explainDebug */,
            0 /* bindVarQueryMinDuration */);

        for (int i = 1; i <= n; i+=5) {

            /* Run in batches of 5 to save time */
            for (int j = 0; j < 5; j++)
            {
                queryId++;
                runQueryDiagnostics(statement, Integer.toString(queryId) , params);
            }

            /* Completing the bundles as circular buffer only stores completed bundles */
            Thread.sleep((diagnosticsInterval + 1) * 1000);

            /* If the buffer wrapped around then query_id 1 should not be present */
            ResultSet resultSet = statement.executeQuery(String.format("SELECT query_id " +
                                                 "FROM yb_query_diagnostics_status " +
                                                 "where query_id = '1'"));
            if (!resultSet.next()) {
                /* Check that the last 5 query_ids are present in the view */
                for (int k = queryId; k > queryId - 5; k--) {
                    resultSet = statement.executeQuery(String.format("SELECT query_id " +
                                                       "FROM yb_query_diagnostics_status " +
                                                       "WHERE query_id = '%d'", k));
                    if (!resultSet.next())
                        fail("could not find query_id " + k + " in the view");
                }
                return;
            }
        }
        fail("Buffer never wrapped around");
    }

    @Test
    public void checkAshData() throws Exception {
        int diagnosticsInterval = (5 * ASH_SAMPLING_INTERVAL_MS) / 1000; /* convert to seconds */
        QueryDiagnosticsParams params = new QueryDiagnosticsParams(
                diagnosticsInterval /* diagnosticsInterval */,
                100 /* explainSampleRate */,
                true /* explainAnalyze */,
                true /* explainDist */,
                false /* explainDebug */,
                0 /* bindVarQueryMinDuration */);

        /* sleep time is diagnosticsInterval + 1 sec to ensure that the bundle has expired */
        long sleep_time_s = diagnosticsInterval + 1;

        try (Statement statement = connection.createStatement()) {
            statement.execute("SELECT pg_sleep(0.5)");

            String queryId = getQueryIdFromPgStatStatements(statement, "%pg_sleep%");
            Path bundleDataPath = runQueryDiagnostics(statement, queryId, params);

            /* Protects from "No query executed;" warning */
            statement.execute("SELECT pg_sleep(0.1)");

            /* sleep for bundle expiry */
            statement.execute("SELECT pg_sleep(" + sleep_time_s + ")");
            Path ashPath = bundleDataPath.resolve("active_session_history.csv");

            assertTrue("active_session_history file does not exist",
                       Files.exists(ashPath));
            assertGreaterThan("active_session_history.csv file is empty",
                              Files.size(ashPath) , 0L);

            ResultSet resultSet = statement.executeQuery(
                                  "SELECT * FROM yb_query_diagnostics_status");
            if (!resultSet.next())
                fail("yb_query_diagnostics_status view does not have expected data");

            Timestamp startTime = resultSet.getTimestamp("start_time");
            long diagnosticsIntervalSec = resultSet.getLong("diagnostics_interval_sec");
            Timestamp endTime = new Timestamp(startTime.getTime() +
                                              (diagnosticsIntervalSec * 1000L));

            statement.execute("CREATE TABLE temp_ash_data" +
                              "(LIKE yb_active_session_history INCLUDING ALL)");
            String copyCmd = "COPY temp_ash_data FROM '" + ashPath.toString() +
                             "' WITH (FORMAT CSV, HEADER)";
            statement.execute(copyCmd);

            resultSet = statement.executeQuery("SELECT COUNT(*) FROM temp_ash_data");
            long validTimeEntriesCount = getSingleRow(statement,
                                         "SELECT COUNT(*) FROM temp_ash_data").getLong(0);

            assertGreaterThan("active_session_history.csv file is empty",
                              validTimeEntriesCount, 0L);

            long invalidTimeEntriesCount = getSingleRow(statement,
                                           "SELECT COUNT(*) FROM temp_ash_data " +
                                           "WHERE sample_time < '" + startTime + "' " +
                                           "OR sample_time > '" + endTime + "'").getLong(0);

            assertEquals("active_session_history.csv contains invalid time entries",
                         invalidTimeEntriesCount, 0);
        }
    }

    @Test
    public void checkPgssData() throws Exception {
        int diagnosticsInterval = (5 * ASH_SAMPLING_INTERVAL_MS) / 1000; /* convert to seconds */
        QueryDiagnosticsParams queryDiagnosticsParams = new QueryDiagnosticsParams(
            diagnosticsInterval,
            100 /* explainSampleRate */,
            true /* explainAnalyze */,
            true /* explainDist */,
            false /* explainDebug */,
            0 /* bindVarQueryMinDuration */);

        /* sleep time is diagnosticsInterval + 1 sec to ensure that the bundle has expired */
        long sleep_time_s = diagnosticsInterval + 1;

        try (Statement statement = connection.createStatement()) {
            statement.execute("SELECT pg_sleep(0.5)");

            String queryId = getQueryIdFromPgStatStatements(statement, "%pg_sleep%");
            Path bundleDataPath = runQueryDiagnostics(statement, queryId, queryDiagnosticsParams);

            statement.execute("SELECT pg_sleep(0.1)");
            statement.execute("SELECT * from pg_class");
            statement.execute("SELECT pg_sleep(0.2)");

            /* sleep for bundle expiry */
            statement.execute("SELECT pg_sleep(" + sleep_time_s + ")");

            Path pgssPath = bundleDataPath.resolve("pg_stat_statements.csv");

            assertTrue("pg_stat_statements file does not exist", Files.exists(pgssPath));
            assertGreaterThan("pg_stat_statements.csv file is empty",
                              Files.size(pgssPath) , 0L);

            /* Unit is ms */
            float epsilon = 10;
            int expectedTotalTime = 300;
            int expectedMinTime = 100;
            int expectedMaxTime = 200;
            int expectedMeanTime = 150;
            List<String> pgssData = Files.readAllLines(pgssPath);
            String[] tokens = pgssData.get(1).split(",");

            assertEquals("pg_stat_statements data size is not as expected",
                         2, pgssData.size());
            assertEquals("pg_stat_statements query is incorrect", queryId, tokens[0]);
            assertTrue("pg_stat_statements contains unnecessary data",
                       !tokens[1].contains("pg_class"));
            assertEquals("Number of calls are incorrect", "2", tokens[2]);
            /* pg_stat_statements outputs data in ms */
            assertLessThan("total_time is incorrect",
                           Math.abs(Float.parseFloat(tokens[3]) - expectedTotalTime), epsilon);
            assertLessThan("min_time is incorrect",
                           Math.abs(Float.parseFloat(tokens[4]) - expectedMinTime), epsilon);
            assertLessThan("max_time is incorrect",
                           Math.abs(Float.parseFloat(tokens[5]) - expectedMaxTime), epsilon);
            assertLessThan("mean_time is incorrect",
                           Math.abs(Float.parseFloat(tokens[6]) - expectedMeanTime), epsilon);
        }
    }

    private void runBundleWithQueries(Statement statement, String queryId,
                                      QueryDiagnosticsParams queryDiagnosticsParams,
                                      String[] queries, String warning) throws Exception {
        /* sleep time is diagnosticsInterval + 1 sec to ensure that the bundle has expired */
        long sleep_time_s = queryDiagnosticsParams.diagnosticsInterval + 1;

        Path bundleDataPath = runQueryDiagnostics(statement, queryId, queryDiagnosticsParams);

        for (String query : queries) {
            statement.execute(query);
        }

        /*
         * Sleep for the bundle expiry duration.
         * This also prevents from "No data available in ASH for the given time range;" warning.
         */
        statement.execute("SELECT pg_sleep(" + sleep_time_s + ")");

        /* Select the last executed bundle */
        ResultSet resultSet = statement.executeQuery("SELECT * " +
                                                     "FROM yb_query_diagnostics_status " +
                                                     "ORDER BY start_time DESC");
        if (!resultSet.next())
            fail("yb_query_diagnostics_status view does not have expected data");

        assertQueryDiagnosticsStatus(resultSet,
                                     bundleDataPath /* expectedViewPath */,
                                     "Success" /* expectedStatus */,
                                     warning /* expectedDescription */,
                                     queryDiagnosticsParams);

        if (!warning.equals(noQueriesExecutedWarning)) {
            Path pgssPath = bundleDataPath.resolve("pg_stat_statements.csv");
            assertTrue("pg_stat_statements file does not exist", Files.exists(pgssPath));
            assertGreaterThan("pg_stat_statements.csv file is empty",
            Files.size(pgssPath) , 0L);

            /* Read the pg_stat_statements.csv file */
            List<String> pgssData = Files.readAllLines(pgssPath);
            String[] tokens = pgssData.get(1).split(",");

            /* Ensure that the query string in pg_stat_statements is empty as expected */
            assertEquals("pg_stat_statements query is incorrect", "\"\"", tokens[1]);
        }
    }

    @Test
    public void testPgssResetBetweenDiagnostics() throws Exception {
        int diagnosticsInterval = (5 * ASH_SAMPLING_INTERVAL_MS) / 1000; /* convert to seconds */
        QueryDiagnosticsParams queryDiagnosticsParams = new QueryDiagnosticsParams(
            diagnosticsInterval,
            100 /* explainSampleRate */,
            true /* explainAnalyze */,
            true /* explainDist */,
            false /* explainDebug */,
            0 /* bindVarQueryMinDuration */);

        try (Statement statement = connection.createStatement()) {
            /*
             * If pg_stat_statements resets during the bundle creation process,
             * the query string in the pg_stat_statements output file will not be available.
             * A warning will be included in the description field of the catalog view
             * to indicate the same.
             */
            String queryId = getQueryIdFromPgStatStatements(statement, "PREPARE%");

            /* Test different scenarios of pgss reset */

            /* reset */
            runBundleWithQueries(statement, queryId, queryDiagnosticsParams, new String[] {
                "SELECT pg_stat_statements_reset()",
            }, noQueriesExecutedWarning);

            /* statement -> reset */
            runBundleWithQueries(statement, queryId, queryDiagnosticsParams, new String[] {
                "EXECUTE stmt('var1', 1, 1.1)",
                "SELECT pg_stat_statements_reset()",
            }, pgssResetWarning);

            /* reset -> statement */
            runBundleWithQueries(statement, queryId, queryDiagnosticsParams, new String[] {
                "SELECT pg_stat_statements_reset()",
                "EXECUTE stmt('var2', 2, 2.2)"
            }, pgssResetWarning);

            /*
             * statement -> reset -> statement
             *
             * Note that this also emits pgssResetWarning, although a statement is executed
             * after the reset. This is intentional as if we were to implement a check for
             * last_time_query_bundled against last_time_reset, it would require a
             * GetCurrentTimestamp() call per bundled query, which could be expensive.
             */
            runBundleWithQueries(statement, queryId, queryDiagnosticsParams, new String[] {
                "EXECUTE stmt('var1', 1, 1.1)",
                "SELECT pg_stat_statements_reset()",
                "EXECUTE stmt('var2', 2, 2.2)"
            }, pgssResetWarning);
        }
    }

    @Test
    public void emptyBundle() throws Exception {
        int diagnosticsInterval = (5 * ASH_SAMPLING_INTERVAL_MS) / 1000; /* convert to seconds */
        QueryDiagnosticsParams params = new QueryDiagnosticsParams(
            diagnosticsInterval,
            100 /* explainSampleRate */,
            true /* explainAnalyze */,
            true /* explainDist */,
            false /* explainDebug */,
            0 /* bindVarQueryMinDuration */);

        /* sleep time is diagnosticsInterval + 1 sec to ensure that the bundle has expired */
        long sleep_time_s = diagnosticsInterval + 1;

        try (Statement statement = connection.createStatement()) {
            /* Run query diagnostics on the prepared stmt */
            String queryId = getQueryIdFromPgStatStatements(statement, "PREPARE%");
            Path bundleDataPath = runQueryDiagnostics(statement, queryId, params);

            /* sleep for bundle expiry */
            statement.execute("SELECT pg_sleep(" + sleep_time_s + ")");

            /* Check that the bundle is empty */
            try (Stream<Path> files = Files.list(bundleDataPath)) {
                if (files.findAny().isPresent()) {
                    fail("The bundle directory is not empty, even though no queries were fired");
                }
            } catch (IOException e) {
                fail("Failed to list files in the bundle directory: " + e.getMessage());
            }

            /* Check that the bundle is empty in the view */
            ResultSet resultSet = statement.executeQuery(
                                  "SELECT * FROM yb_query_diagnostics_status");
            if (!resultSet.next())
                fail("yb_query_diagnostics_status view does not have expected data");

            assertQueryDiagnosticsStatus(resultSet,
                                         bundleDataPath /* expectedViewPath */,
                                         "Success" /* expectedStatus */,
                                         noQueriesExecutedWarning /* expectedDescription */,
                                         params);
        }
    }
}
