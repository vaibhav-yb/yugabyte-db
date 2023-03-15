package org.yb.pgsql;

import org.apache.ibatis.jdbc.ScriptRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.YBTestRunner;
import org.yb.client.GetTableSchemaResponse;
import org.yb.client.ListTablesResponse;
import org.yb.client.YBClient;
import org.yb.master.MasterDdlOuterClass;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import static org.yb.AssertionWrappers.assertEquals;
import static org.yb.AssertionWrappers.fail;

@RunWith(value = YBTestRunner.class)
public class TestGetTableSchema extends BasePgSQLTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestPgInsert.class);

  @Test
  public void testGetTableSchema() throws Exception {
    try (Statement st = connection.createStatement()) {
      // Create tables.
      runSqlScript(connection, "repro.sql");

      LOG.info("Getting client");
      YBClient ybClient = miniCluster.getClient();

      // Get all the table UUIDs.
      LOG.info("Getting tables list");
      ListTablesResponse resp = ybClient.getTablesList();
      Set<String> tableUUIDs = new HashSet<>();
      for (MasterDdlOuterClass.ListTablesResponsePB.TableInfo tableInfo : resp.getTableInfoList()) {
        tableUUIDs.add(tableInfo.getId().toStringUtf8());
      }

      // Now call getTableSchema on all the tables, it should pass with the changes
      LOG.info("Calling getTableSchemaByUUID");
      for (String tableId : tableUUIDs) {
        LOG.info("Getting schema for table ID " + tableId + " and name " +
          ybClient.openTableByUUID(tableId).getName());
        GetTableSchemaResponse response = ybClient.getTableSchemaByUUID(tableId);
      }
    } catch (Exception e) {
      fail("Test failed because of exception " + e);
    }
  }

  private void runSqlScript(Connection conn, String fileName) {
    LOG.info("Running the SQL script: " + fileName);
    ScriptRunner sr = new ScriptRunner(conn);
    sr.setAutoCommit(true);

    final String sqlFile = Paths.get("src", "test", "resources").toFile().getAbsolutePath()
                           + "/"+ fileName;
    try {
      Reader reader = new BufferedReader(new FileReader(sqlFile));
      sr.runScript(reader);
    } catch (FileNotFoundException f) {
      f.printStackTrace();
      fail();
    }
  }
}
