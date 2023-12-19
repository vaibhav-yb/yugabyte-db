---
title: Session-level audit logging in YSQL
headerTitle: Session-level audit logging in YSQL
linkTitle: Session-level audit logging
description: Session-level audit logging in YSQL.
menu:
  v2.18:
    identifier: session-audit-logging-1-ysql
    parent: audit-logging
    weight: 760
type: docs
---

<ul class="nav nav-tabs-alt nav-tabs-yb">
  <li >
    <a href="../session-audit-logging-ysql/" class="nav-link active">
      <i class="icon-postgres" aria-hidden="true"></i>
      YSQL
    </a>
  </li>
</ul>

## Enable session-level audit

Session logging is enabled for per user session basis. Enable session logging for all DML and DDL statements and log all relations in DML statements.

```sql
set pgaudit.log = 'write, ddl';
set pgaudit.log_relation = on;
```

Enable session logging for all commands except MISC and raise audit log messages as NOTICE.

## Example

In this example session audit logging is used for logging DDL and SELECT statements. Note that the insert statement is not logged because the WRITE class is not enabled.

SQL statements are shown below.

### Step 1. Connect using `ysql`

Open the YSQL shell (ysqlsh), specifying the `yugabyte` user and prompting for the password.

```sh
$ ./ysqlsh -U yugabyte -W
```

When prompted for the password, enter the yugabyte password. You should be able to log in and see a response similar to the following:

```output
ysqlsh (11.2-YB-2.5.0.0-b0)
Type "help" for help.

yugabyte=#
```

### Step 2. Enable `pgaudit` extension

Enable `pgaudit` extension on the YugabyteDB cluster.

```sql
\c yugabyte yugabyte;
CREATE EXTENSION IF NOT EXISTS pgaudit;
```

### Step 3. Enable session audit logging

Enable session audit logging in YugabyteDB cluster.

```sql
set pgaudit.log = 'read, ddl';
```

### Step 4. Perform statements

```sql
create table account
(
    id int,
    name text,
    password text,
    description text
);

insert into account (id, name, password, description)
             values (1, 'user1', 'HASH1', 'blah, blah');

select *
    from account;
```

### Step 5. Verify output

You should output similar to the following in the logs:

```output
2020-11-09 19:19:09.262 UTC [3710] LOG:  AUDIT: SESSION,1,1,DDL,CREATE
TABLE,TABLE,public.account,"create table account
        (
            id int,
            name text,
            password text,
            description text
        );",<not logged>
2020-11-09 19:19:19.619 UTC [3710] LOG:  AUDIT: SESSION,2,1,READ,SELECT,,,"select *
            from account;",<not logged>
```
