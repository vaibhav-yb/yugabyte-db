---
title: fall-forward synchronize reference
headcontent: yb-voyager fall-forward synchronize
linkTitle: fall-forward synchronize
description: YugabyteDB Voyager fall-forward synchronize reference
menu:
  preview_yugabyte-voyager:
    identifier: voyager-fall-forward-synchronize
    parent: fall-forward
    weight: 100
type: docs
---

Exports new changes from the YugabyteDB database to import to the fall-forward database so that the fall-forward database can be in sync with the YugabyteDB database after cutover.

## Syntax

```text
Usage: yb-voyager fall-forward synchronize [ <arguments> ... ]
```

### Arguments

The valid *arguments* for fall-forward synchronize are described in the following table:

| Argument | Description/valid options |
| :------- | :------------------------ |
| --disable-pb | Use this argument to not display progress bars. For live migration, `--disable-pb` can also be used to hide metrics for export data. (default: false) |
| --exclude-table-list <tableNames> | Comma-separated list of tables to exclude while importing data (ignored if `--table-list` is used). |
| -e, --export-dir <path> | Path to the export directory. This directory is a workspace used to store exported schema DDL files, export data files, migration state, and a log file.|
| -h, --help | Command line help for synchronize. |
| --send-diagnostics | Send [diagnostics](../../../diagnostics-report/) information to Yugabyte. (default: true) |
| --table-list | Comma-separated list of the tables to export data. Do not use in conjunction with `--exclude-table-list.`|
| --target-db-host <hostname> | Domain name or IP address of the machine on which target database server is running. (default: 127.0.0.1)|
| --target-db-name <name> | Target database name on which import needs to be done.|
| --target-db-password <password>| Target database password. Alternatively, you can also specify the password by setting the environment variable `TARGET_DB_PASSWORD`. If you don't provide a password via the CLI during any migration phase, yb-voyager will prompt you at runtime for a password. If the password contains special characters that are interpreted by the shell (for example, # and $), enclose the password in single quotes. |
| --target-db-port <port> | Port number of the target database machine that runs the YugabyteDB YSQL API. (default: 5433)|
| --target-db-schema <schemaName> | Schema name of the target database. |
| --target-db-user <username> | Username of the target database. |
| [--target-ssl-cert](../../yb-voyager-cli/#ssl-connectivity) <certificateName> | Name of the certificate which is part of the SSL `<cert,key>` pair. |
| [--target-ssl-key](../../yb-voyager-cli/#ssl-connectivity) <keyName> | Name of the key which is part of the SSL `<cert,key>` pair. |
| [--target-ssl-crl](../../yb-voyager-cli/#ssl-connectivity) <path> | Path to a file containing the SSL certificate revocation list (CRL).|
| [--target-ssl-mode](../../yb-voyager-cli/#ssl-connectivity) <SSLmode> | One of `disable`, `allow`, `prefer`(default), `require`, `verify-ca`, or `verify-full`. |
| [--target-ssl-root-cert](../../yb-voyager-cli/#ssl-connectivity) <path> | Path to a file containing SSL certificate authority (CA) certificate(s). |
| --verbose | Display extra information in the output. (default: false) |
| -y, --yes| Answer yes to all prompts during migration. (default: false) |

## Example

```sh
yb-voyager fall-forward synchronize --export-dir /dir/export-dir \
        --target-db-host 127.0.0.1 \
        --target-db-user ybvoyager \
        --target-db-password 'password' \
        --target-db-name target_db \
        --target-db-schema target_schema
```
