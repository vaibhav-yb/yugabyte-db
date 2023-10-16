---
title: import schema reference
headcontent: yb-voyager import schema
linkTitle: import schema
description: YugabyteDB Voyager import schema reference
menu:
  preview_yugabyte-voyager:
    identifier: voyager-import-schema
    parent: schema-migration
    weight: 50
type: docs
---

[Import the schema](../../../migrate/migrate-steps/#import-schema) to the YugabyteDB database.

During migration, run the import schema command twice, first without the [--post-import-data](#arguments) argument and then with the argument. The second invocation creates indexes and triggers in the target schema, and must be done after [import data](../../../migrate/migrate-steps/#import-data) is complete.

{{< note title="For Oracle migrations" >}}

For Oracle migrations using YugabyteDB Voyager v1.1 or later, the Orafce extension is installed on the target database by default. This enables you to use a subset of predefined functions, operators, and packages from Oracle. The extension is installed in the public schema, and when listing functions or views, extra objects will be visible on the target database which may confuse you. You can remove the extension using the [DROP EXTENSION](../../../../api/ysql/the-sql-language/statements/ddl_drop_extension) command.

{{< /note >}}

## Syntax

```text
Usage: yb-voyager import schema [ <arguments> ... ]
```

### Arguments

The valid *arguments* for import schema are described in the following table:

| Argument | Description/valid options |
| :------- | :------------------------ |
| --continue-on-error | Continues the import of all the exported schema even during an error and outputs all the erroneous DDLs in the `failed.sql` file under the `export-dir/schema` directory. (default: false) <br> Usage to set it: `yb-voyager import schema ... --continue-on-error`  |
| --enable-orafce | Enables Orafce extension on target (if source database type is Oracle). (default: true) |
| --exclude-object-list <objectTypes> | Comma-separated list of schema object types (object types are case-insensitive) to exclude while importing schema (ignored if --object-list is used).<br> Example: `--exclude-object-list 'FUNCTION,PROCEDURE'` |
| -e, --export-dir <path> | Path to the export directory. This directory is a workspace used to store exported schema DDL files, export data files, migration state, and a log file. |
| -h, --help | Command line help. |
| --ignore-exist | Ignore if an object already exists on the target database. (default: false)<br>Usage to set it: `yb-voyager import schema ... --ignore-exist` |
| --object-list <objectTypes> | Comma-separated list of schema object types (object types are case-insensitive) to include while importing schema.<br>Example: `--object-list 'TABLE,VIEW,MVIEW'` |
| --post-import-data | Imports indexes and triggers in the YugabyteDB database after data import is complete. This argument assumes that data import is already done and imports only indexes and triggers in the YugabyteDB database. (default: false) |
| --refresh-mviews | Refreshes the materialized views on target during the post-import-data phase (default: false) |
| --send-diagnostics| Send [diagnostics](../../../diagnostics-report/) information to Yugabyte. (default: true)|
| --start-clean | Starts a fresh schema import on the target yugabyteDB database for the schema present in the `schema` directory. |
| --straight-order | Imports the schema objects in the order specified via the `--object-list` flag (default: false)<br> Usage to set it: `yb-voyager import schema ... --object-list 'TYPE,TABLE,VIEW...'  --straight-order` |
| --target-db-host <hostname> | Domain name or IP address of the machine on which target database server is running. (default: 127.0.0.1)|
| --target-db-name <name> | Target database name. (default: yugabyte) |
| --target-db-password <password>| Target database password. Alternatively, you can also specify the password by setting the environment variable `TARGET_DB_PASSWORD`. If you don't provide a password via the CLI or environment variable during any migration phase, yb-voyager will prompt you at runtime for a password. If the password contains special characters that are interpreted by the shell (for example, # and $), enclose the password in single quotes. |
| --target-db-port <port> | Port number of the target database machine. (default: 5433) |
| --target-db-schema <schemaName> | Schema name of the target database. MySQL and Oracle migrations only. |
| --target-db-user <username> | Username of the target database. |
| [--target-ssl-cert](../../yb-voyager-cli/#ssl-connectivity) <certificateName> | Name of the certificate which is part of the SSL `<cert,key>` pair. |
| [--target-ssl-key](../../yb-voyager-cli/#ssl-connectivity) <keyName> | Name of the key which is part of the SSL `<cert,key>` pair. |
| [--target-ssl-crl](../../yb-voyager-cli/#ssl-connectivity) <path> | Path to a file containing the SSL certificate revocation list (CRL).|
| [--target-ssl-mode](../../yb-voyager-cli/#ssl-connectivity) <SSLmode> | One of `disable`, `allow`, `prefer`(default), `require`, `verify-ca`, or `verify-full`. |
| [--target-ssl-root-cert](../../yb-voyager-cli/#ssl-connectivity) <path> | Path to a file containing SSL certificate authority (CA) certificate(s). |
| --verbose | Display extra information in the output. (default: false) |
| -y, --yes | Answer yes to all prompts during the export schema operation. (default: false) |

## Examples

```sh
yb-voyager import schema --export-dir /dir/export-dir \
        --target-db-host 127.0.0.1 \
        --target-db-user ybvoyager \
        --target-db-password 'password' \
        --target-db-name target_db \
        --target-db-schema target_schema
```

Example of [post-import data](../../../migrate/migrate-steps/#import-indexes-and-triggers) phase is as follows:

```sh

yb-voyager import schema --export-dir /dir/export-dir \
        --target-db-host 127.0.0.1 \
        --target-db-user ybvoyager \
        --target-db-password 'password' \
        --target-db-name target_db \
        --target-db-schema target_schema
        --post-import-data
        --refresh-mviews
```
