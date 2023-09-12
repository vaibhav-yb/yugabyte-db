---
title: Steps to perform live migration of your database using YugabyteDB Voyager
headerTitle: Live migration
linkTitle: Live migration
headcontent: Steps for a live migration using YugabyteDB Voyager
description: Run the steps to ensure a successful live migration using YugabyteDB Voyager.
menu:
  preview_yugabyte-voyager:
    identifier: migrate-live
    parent: migration-types
    weight: 103
techPreview: /preview/faq/general/#what-is-the-definition-of-the-beta-feature-tag
type: docs
rightNav:
  hideH4: true
---

This page describes the steps to perform and verify a successful live migration to YugabyteDB, including changes that continuously occur on the source.

## Live migration workflow

The following workflows illustrate how you can perform data migration including changes happening on the source simultaneously. With the export data command, you can first export a snapshot and then start continuously capturing changes occurring on the source to an event queue on the disk. Using the import data command, you similarly import the snapshot first, and then continuously apply the exported change events on the target.

Eventually, the migration process reaches a steady state where you can [cut over to a database](#cut-over-to-a-database). You can stop your applications from pointing to your source database, let all the remaining changes be applied on the target YugabyteDB database, and then restart your applications pointing to YugabyteDB.

The following illustration describes how the data export and import operations are simultaneously handled by YugabyteDB Voyager.

![Live migration short](/images/migrate/live-migration-short.png)

The following illustration shows the steps in a live migration using YugabyteDB Voyager.

![Live migration workflow](/images/migrate/live-migration-workflow.png)

| Step | Description |
| :--- | :---|
| [Install yb-voyager](../../install-yb-voyager/#install-yb-voyager) | yb-voyager supports RHEL, CentOS, Ubuntu, and macOS, as well as airgapped and Docker-based installations. |
| [Prepare source](#prepare-the-source-database) | Create a new database user with READ access to all the resources to be migrated. |
| [Prepare target](#prepare-the-target-database) | Deploy a YugabyteDB database and create a user with superuser privileges. |
| [Export schema](#export-schema) | Convert the database schema to PostgreSQL format using the `yb-voyager export schema` command. |
| [Analyze schema](#analyze-schema) | Generate a *Schema&nbsp;Analysis&nbsp;Report* using the `yb-voyager analyze-schema` command. The report suggests changes to the PostgreSQL schema to make it appropriate for YugabyteDB. |
| [Modify schema](#manually-edit-the-schema) | Using the report recommendations, manually change the exported schema. |
| Start | Start the phases: export data first, followed by import data and archive changes simultaneously. |
| [Export data](#export-data) | The export data command first exports a snapshot and then starts continuously capturing changes from the source.|
| [Import data](#import-data) | The import data command first imports the snapshot, and then continuously applies the exported change events on the target. |
| [Archive changes](#archive-changes) | Continuously archive migration changes to limit disk utilization. |
| [Initiate cutover](#cut-over-to-a-database) | Perform a cutover (stop streaming changes) when the migration process reaches a steady state where you can stop your applications from pointing to your source database, allow all the remaining changes to be applied on the target YugabyteDB database, and then restart your applications pointing to YugabyteDB. |
| [Wait for cutover to complete](#cut-over-to-a-database) | Monitor the wait status using the [cutover status](../../reference/yb-voyager-cli/#cutover-status) command. |
| [Import&nbsp;indexes&nbsp;and triggers](#import-indexes-and-triggers) | Import indexes and triggers to the target YugabyteDB database using the `yb-voyager import schema` command with an additional `--post-import-data` flag. |
| [Verify migration](#verify-migration) | Check if the live migration is successful. |

Before proceeding with migration, ensure that you have completed the following steps:

- [Install yb-voyager](../../install-yb-voyager/#install-yb-voyager).
- Check the [unsupported features](../../known-issues/#unsupported-features) and [known issues](../../known-issues/#known-issues).
- Review [data modeling](../../reference/data-modeling/) strategies.
- [Prepare the source database](#prepare-the-source-database).
- [Prepare the target database](#prepare-the-target-database).

## Prepare the source database

Prepare your source database by creating a new database user, and provide it with READ access to all the resources which need to be migrated.

<ul class="nav nav-tabs nav-tabs-yb">
  <li>
    <a href="#standalone-oracle" class="nav-link active" id="standalone-oracle-tab" data-toggle="tab" role="tab" aria-controls="oracle" aria-selected="true">
      <i class="icon-oracle" aria-hidden="true"></i>
      Standalone Oracle Container Database
    </a>
  </li>
    <li>
    <a href="#rds-oracle" class="nav-link" id="rds-oracle-tab" data-toggle="tab" role="tab" aria-controls="oracle" aria-selected="true">
      <i class="icon-oracle" aria-hidden="true"></i>
      RDS Oracle
    </a>
  </li>
</ul>

<div class="tab-content">
  <div id="standalone-oracle" class="tab-pane fade show active" role="tabpanel" aria-labelledby="standalone-oracle-tab">
  {{% includeMarkdown "./standalone-oracle.md" %}}
  </div>
    <div id="rds-oracle" class="tab-pane fade" role="tabpanel" aria-labelledby="rds-oracle-tab">
  {{% includeMarkdown "./rds-oracle.md" %}}
  </div>
</div>

If you want yb-voyager to connect to the source database over SSL, refer to [SSL Connectivity](../../reference/yb-voyager-cli/#ssl-connectivity).

{{< note title="Connecting to Oracle instances" >}}
You can use only one of the following arguments to connect to your Oracle instance.

- [`--source-db-schema`](../../reference/yb-voyager-cli/#source-db-schema)
- [`--oracle-db-sid`](../../reference/yb-voyager-cli/#oracle-db-sid)
- [`--oracle-tns-alias`](../../reference/yb-voyager-cli/#ssl-connectivity)
{{< /note >}}

## Prepare the target database

Prepare your target YugabyteDB database cluster by creating a database, and a user for your cluster.

### Create the target database

Create the target database in your YugabyteDB cluster. The database name can be the same or different from the source database name.

If you don't provide the target database name during import, yb-voyager assumes the target database name is `yugabyte`. To specify the target database name during import, use the `--target-db-name` argument with the `yb-voyager import` commands.

```sql
CREATE DATABASE target_db_name;
```

### Create a user

Create a user with [`SUPERUSER`](../../../api/ysql/the-sql-language/statements/dcl_create_role/#syntax) role.

- For a local YugabyteDB cluster or YugabyteDB Anywhere, create a user and role with the superuser privileges using the following command:

     ```sql
     CREATE USER ybvoyager SUPERUSER PASSWORD 'password';
     ```

- For YugabyteDB Managed, create a user with [`yb_superuser`](../../../yugabyte-cloud/cloud-secure-clusters/cloud-users/#admin-and-yb-superuser) role using the following command:

     ```sql
     CREATE USER ybvoyager PASSWORD 'password';
     GRANT yb_superuser TO ybvoyager;
     ```

If you want yb-voyager to connect to the target database over SSL, refer to [SSL Connectivity](../../reference/yb-voyager-cli/#ssl-connectivity).

{{< warning title="Deleting the ybvoyager user" >}}

After migration, all the migrated objects (tables, views, and so on) are owned by the `ybvoyager` user. You should transfer the ownership of the objects to some other user (for example, `yugabyte`) and then delete the `ybvoyager` user. Example steps to delete the user are:

```sql
REASSIGN OWNED BY ybvoyager TO yugabyte;
DROP OWNED BY ybvoyager;
DROP USER ybvoyager;
```

{{< /warning >}}

## Create an export directory

yb-voyager keeps all of its migration state, including exported schema and data, in a local directory called the *export directory*.

Before starting migration, you should create the export directory on a file system that has enough space to keep the entire source database. Next, you should provide the path of the export directory as a mandatory argument (`--export-dir`) to each invocation of the yb-voyager command in an environment variable.

```sh
mkdir $HOME/export-dir
export EXPORT_DIR=$HOME/export-dir
```

The export directory has the following sub-directories and files:

- `reports` directory contains the generated *Schema Analysis Report*.
- `schema` directory contains the source database schema translated to PostgreSQL. The schema is partitioned into smaller files by the schema object type such as tables, views, and so on.
- `data` directory contains CSV (Comma Separated Values) files that are passed to the COPY command on the target database.
- `metainfo` and `temp` directories are used by yb-voyager for internal bookkeeping.
- `logs` directory contains the log files for each command.

## Migrate your database to YugabyteDB

Proceed with schema and data migration using the following steps:

### Export and analyze schema

To begin, export the schema from the source database. Once exported, analyze the schema and apply any necessary manual changes.

#### Export schema

The `yb-voyager export schema` command extracts the schema from the source database, converts it into PostgreSQL format (if the source database is Oracle or MySQL), and dumps the SQL DDL files in the `EXPORT_DIR/schema/*` directories.

{{< note title="Usage for source_db_schema" >}}

The `source_db_schema` argument specifies the schema of the source database.

- For Oracle, `source-db-schema` can take only one schema name and you can migrate _only one_ schema at a time.

{{< /note >}}

An example invocation of the command is as follows:

```sh
# Replace the argument values with those applicable for your migration.
yb-voyager export schema --export-dir <EXPORT_DIR> \
        --source-db-type <SOURCE_DB_TYPE> \
        --source-db-host <SOURCE_DB_HOST> \
        --source-db-user <SOURCE_DB_USER> \
        --source-db-password <SOURCE_DB_PASSWORD> \ # Enclose the password in single quotes if it contains special characters.
        --source-db-name <SOURCE_DB_NAME> \
        --source-db-schema <SOURCE_DB_SCHEMA>

```

Refer to [export schema](../../reference/yb-voyager-cli/#export-schema) for details about the arguments.

#### Analyze schema

The schema exported in the previous step may not yet be suitable for importing into YugabyteDB. Even though YugabyteDB is PostgreSQL compatible, given its distributed nature, you may need to make minor manual changes to the schema.

The `yb-voyager analyze-schema` command analyses the PostgreSQL schema dumped in the [export schema](#export-schema) step, and prepares a report that lists the DDL statements which need manual changes. An example invocation of the command is as follows:

```sh
# Replace the argument values with those applicable for your migration.
yb-voyager analyze-schema --export-dir <EXPORT_DIR> --output-format <FORMAT>
```

The preceding command generates a report file under the `EXPORT_DIR/reports/` directory.

Refer to [analyze schema](../../reference/yb-voyager-cli/#analyze-schema) for details about the arguments.

#### Manually edit the schema

Fix all the issues listed in the generated schema analysis report by manually editing the SQL DDL files from the `EXPORT_DIR/schema/*`.

After making the manual changes, re-run the `yb-voyager analyze-schema` command. This generates a fresh report using your changes. Repeat these steps until the generated report contains no issues.

To learn more about modelling strategies using YugabyteDB, refer to [Data modeling](../../reference/data-modeling/).

{{< note title="Manual schema changes" >}}

- `CREATE INDEX CONCURRENTLY` is not currently supported in YugabyteDB. You should remove the `CONCURRENTLY` clause before trying to import the schema.

- Include the primary key definition in the `CREATE TABLE` statement. Primary Key cannot be added to a partitioned table using the `ALTER TABLE` statement.

{{< /note >}}

### Import schema

Import the schema using the `yb-voyager import schema` command.

{{< note title="Usage for target_db_schema" >}}

`yb-voyager` imports the source database into the `public` schema of the target database. By specifying `--target-db-schema` argument during import, you can instruct `yb-voyager` to create a non-public schema and use it for the schema/data import.

{{< /note >}}

An example invocation of the command is as follows:

```sh
# Replace the argument values with those applicable for your migration.
yb-voyager import schema --export-dir <EXPORT_DIR> \
        --target-db-host <TARGET_DB_HOST> \
        --target-db-user <TARGET_DB_USER> \
        --target-db-password <TARGET_DB_PASSWORD> \ # Enclose the password in single quotes if it contains special characters..
        --target-db-name <TARGET_DB_NAME> \
        --target-db-schema <TARGET_DB_SCHEMA>
```

Refer to [import schema](../../reference/yb-voyager-cli/#import-schema) for details about the arguments.

yb-voyager applies the DDL SQL files located in the `$EXPORT_DIR/schema` directory to the target database. If yb-voyager terminates before it imports the entire schema, you can rerun it by adding the `--ignore-exist` option.

{{< note title="Importing indexes and triggers" >}}

Because the presence of indexes and triggers can slow down the rate at which data is imported, by default `import schema` does not import indexes and triggers. You should complete the data import without creating indexes and triggers. Only after data import is complete, create indexes and triggers using the `import schema` command with an additional `--post-import-data` flag.

{{< /note >}}

### Export data

Begin exporting data from the source database into the `EXPORT_DIR/data` directory using the yb-voyager export data command as follows:

```sh
# Replace the argument values with those applicable for your migration.
yb-voyager export data --export-dir <EXPORT_DIR> \
        --source-db-type <SOURCE_DB_TYPE> \
        --source-db-host <SOURCE_DB_HOST> \
        --source-db-user <SOURCE_DB_USER> \
        --source-db-password <SOURCE_DB_PASSWORD> \ # Enclose the password in single quotes if it contains special characters.
        --source-db-name <SOURCE_DB_NAME> \
        --source-db-schema <SOURCE_DB_SCHEMA> \
        --export-type snapshot-and-changes
```

The export data command first ensures that it exports a snapshot of the data already present on the source database. Next, you start a streaming phase (CDC phase) where you begin capturing new changes made to the data on the source after the migration has started. Some important metrics such as the number of events, export rate, and so on, is displayed during the CDC phase similar to the following:

```output
| ---------------------------------------  |  ----------------------------- |
| Metric                                   |                          Value |
| ---------------------------------------  |  ----------------------------- |
| Total Exported Events                    |                         123456 |
| Total Exported Events (Current Run)      |                         123456 |
| Export Rate(Last 3 min)                  |                      22133/sec |
| Export Rate(Last 10 min)                 |                      21011/sec |
| ---------------------------------------  |  ----------------------------- |
```

Note that the CDC phase will start only after a snapshot of the entire table-set is completed.
Additionally, the CDC phase is restartable. So, if yb-voyager terminates when data export is in progress, it resumes from its current state after the CDC phase is restarted.

#### Caveats

- Some data types are unsupported. For a detailed list, refer to [datatype mappings](../../reference/datatype-mapping-oracle/).
- For Oracle where sequences are not attached to a column, resume value generation is unsupported.
- [--parallel-jobs](../../reference/yb-voyager-cli/#parallel-jobs) argument has no effect on live migration.

Refer to [export data](../../reference/yb-voyager-cli/#export-data) for details about the arguments, and [export data status](../../reference/yb-voyager-cli/#export-data-status) to track the status of an export operation.

The options passed to the command are similar to the [`yb-voyager export schema`](#export-schema) command. To export only a subset of the tables, pass a comma-separated list of table names in the `--table-list` argument.

### Import data

After you have successfully imported the schema in the target database, you can start importing the data using the yb-voyager import data command as follows:

```sh
# Replace the argument values with those applicable for your migration.
yb-voyager import data --export-dir <EXPORT_DIR> \
        --target-db-host <TARGET_DB_HOST> \
        --target-db-user <TARGET_DB_USER> \
        --target-db-password <TARGET_DB_PASSWORD> \ # Enclose the password in single quotes if it contains special characters.
        --target-db-name <TARGET_DB_NAME> \
        --target-db-schema <TARGET_DB_SCHEMA> \ # Oracle only.
        --parallel-jobs <NUMBER_OF_JOBS>
```

Refer to [import data](../../reference/yb-voyager-cli/#import-data) for details about the arguments.

For the snapshot exported, yb-voyager splits the data dump files (from the $EXPORT_DIR/data directory) into smaller batches. yb-voyager concurrently ingests the batches such that all nodes of the target YugabyteDB database cluster are used. After the snapshot is imported, a similar approach is employed for the CDC phase, where concurrent batches of change events are applied on the target YugabyteDB database cluster.

Some important metrics such as the number of events, ingestion rate, and so on, is displayed during the CDC phase similar to the following:

```output
| -----------------------------  |  ----------------------------- |
| Metric                         |                          Value |
| -----------------------------  |  ----------------------------- |
| Total Imported events          |                         272572 |
| Events Imported in this Run    |                         272572 |
| Ingestion Rate (last 3 mins)   |               14542 events/sec |
| Ingestion Rate (last 10 mins)  |               14542 events/sec |
| Time taken in this Run         |                      0.83 mins |
| Remaining Events               |                        4727427 |
| Estimated Time to catch up     |                          5m42s |
| -----------------------------  |  ----------------------------- |
```

The entire import process is designed to be _restartable_ if yb-voyager terminates when the data import is in progress. If restarted, the data import resumes from its current state.

{{< note title="Note">}}
[table-list](../../reference/yb-voyager-cli/#table-list) and [exclude-table-list](../../reference/yb-voyager-cli/#exclude-table-list) flags are not supported in live migration.
{{< /note >}}

{{< tip title="Importing large datasets" >}}

When importing a very large database, run the import data command in a `screen` session, so that the import is not terminated when the terminal session stops.

If the `yb-voyager import data` command terminates before completing the data ingestion, you can re-run it with the same arguments and the command will resume the data import operation.

{{< /tip >}}

#### Import data status

Run the `yb-voyager import data status --export-dir <EXPORT_DIR>` command to get an overall progress of the data import operation.

### Archive changes

As the migration continuously exports changes on the source database to the `EXPORT-DIR`, the disk utilization continues to grow indefinitely over time. To limit usage of all the disk space, you can use the `archive changes` command as follows:

```sh
yb-voyager archive changes --export-dir <EXPORT-DIR> --move-to <DESTINATION-DIR> --delete
```

Refer to [archive changes](../../reference/yb-voyager-cli/#archive-changes) for details about the arguments.

### Cut over to the target

Cutover is the last phase, where you switch your application over from the source database to the target YugabyteDB database.

Keep monitoring the metrics displayed for export data and import data processes. After you notice that the import of events is catching up to the exported events, you are ready to perform a cutover. You can use the "Remaining events" metric displayed in the import data process to help you determine the cutover.

Perform the following steps as part of the cutover process:

1. Quiesce your source database, that is stop application writes.
1. Perform a cutover after the exported events rate ("ingestion rate" in the metrics table) drops to 0 using the following command:

    ```sh
    yb-voyager cutover initiate --export-dir <EXPORT_DIR>
    ```

    Refer to [cutover initiate](../../reference/yb-voyager-cli/#cutover-initiate) for details about the arguments.

    The cutover initiate command stops the export data process, followed by the import data process after it has imported all the events to the target YugabyteDB database.

1. Wait for the cutover process to complete. Monitor the status of the cutover process using the following command:

    ```sh
    yb-voyager cutover status --export-dir <EXPORT_DIR>
    ```

    Refer to [cutover status](../../reference/yb-voyager-cli/#cutover-status) for details about the arguments.

### Import indexes and triggers

Import indexes and triggers using the `import schema` command with an additional `--post-import-data` flag as follows:

```sh
# Replace the argument values with those applicable for your migration.
yb-voyager import schema --export-dir <EXPORT_DIR> \
        --target-db-host <TARGET_DB_HOST> \
        --target-db-user <TARGET_DB_USER> \
        --target-db-password <TARGET_DB_PASSWORD> \ # Enclose the password in single quotes if it contains special characters.
        --target-db-name <TARGET_DB_NAME> \
        --target-db-user <TARGET_DB_USER> \
        --target-db-schema <TARGET_DB_SCHEMA> \
        --post-import-data
```

Refer to [import schema](../../reference/yb-voyager-cli/#import-schema) for details about the arguments.

### Verify migration

After the schema and data import is complete, the automated part of the database migration process is considered complete. You should manually run validation queries on both the source and target database to ensure that the data is correctly migrated. A sample query to validate the databases can include checking the row count of each table.

{{< warning title = "Caveat associated with rows reported by import data status" >}}

Suppose you have a scenario where,

- [import data](#import-data) or [import data file](#import-data-file) command fails.
- To resolve this issue, you delete some of the rows from the split files.
- After retrying, the import data command completes successfully.

In this scenario, [import data status](#import-data-status) command reports an incorrect number of imported rows, because it doesn't take into account the deleted rows.

For more details, refer to GitHub issue [#360](https://github.com/yugabyte/yb-voyager/issues/360).

{{< /warning >}}

After migration verification, stop [archiving changes](#archive-changes).

## Limitations

- Schema changes on the source Oracle database will not be recognized during the live migration.
- Tables without primary key are not supported.
- Truncating a table on the source database is not taken into account; you need to manually truncate tables on your YugabyteDB cluster.
- Some Oracle datatypes are unsupported - NCHAR, NVARCHAR, VARRAY, BLOB, CLOB, and NCLOB.
- Case-sensitive table names or column names are partially supported. yb-voyager converts them to case-insensitive names. For example, an "Accounts" table in a source Oracle database is migrated as `accounts` (case-insensitive) to a YugabyteDB database.
- Reserved keywords such as table name, or column names are unsupported.
