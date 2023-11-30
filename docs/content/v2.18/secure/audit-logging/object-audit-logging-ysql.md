---
title: Object-level audit logging in YSQL
headerTitle: Object-level audit logging in YSQL
linkTitle: Object-level audit logging
description: Object-level audit logging in YSQL.
menu:
  v2.18:
    identifier: object-audit-logging-1-ysql
    parent: audit-logging
    weight: 765
type: docs
---

<ul class="nav nav-tabs-alt nav-tabs-yb">
  <li >
    <a href="../object-audit-logging-ysql/" class="nav-link active">
      <i class="icon-postgres" aria-hidden="true"></i>
      YSQL
    </a>
  </li>
</ul>

Object audit logging logs statements that affect a particular relation. Only SELECT, INSERT, UPDATE, and DELETE commands are supported. TRUNCATE is not included in object audit logging.

Object audit logging is intended to be a finer-grained replacement for `pgaudit.log = 'read, write'`. As such, it may not make sense to use them in conjunction but one possible scenario would be to use session logging to capture each statement and then supplement that with object logging to get more detail about specific relations.

In YugabyteDB, object-level audit logging is implemented by reusing the PG role system. The `pgaudit.role` setting defines the role that will be used for audit logging. A relation ( TABLE, VIEW, etc.) will be audit logged when the audit role has permissions for the command executed or inherits the permissions from another role. This allows you to effectively have multiple audit roles even though there is a single master role in any context.

In this example object audit logging is used to illustrate how a granular approach may be taken towards logging of SELECT and DML statements.

## Step 1. Connect using `ysql`

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

## Step 2. Enable `pgaudit`

Enable `pgaudit` extension on the YugabyteDB cluster.

```sql
\c yugabyte yugabyte;

CREATE EXTENSION IF NOT EXISTS pgaudit;
```

## Step 3. Enable object auditing

Set [pgaudit.role](https://github.com/pgaudit/pgaudit/blob/master/README.md#pgauditrole) to `auditor` and grant `SELECT` and `UPDATE` privileges on the `account` table. Any `SELECT` or `UPDATE` statements on the `account` table will now be logged. Note that logging on the `account` table is controlled by column-level permissions, while logging on the `account_role_map` table is table-level.

```sql
CREATE ROLE auditor;

set pgaudit.role = 'auditor';
```

## Step 4. Create a table

```sql
create table account
(
    id int,
    name text,
    password text,
    description text
);

grant select (password)
   on public.account
   to auditor;

select id, name
  from account;

select password
  from account;

grant update (name, password)
   on public.account
   to auditor;

update account
   set description = 'yada, yada';

update account
   set password = 'HASH2';

create table account_role_map
(
    account_id int,
    role_id int
);

grant select
   on public.account_role_map
   to auditor;

select account.password,
       account_role_map.role_id
  from account
       inner join account_role_map
            on account.id = account_role_map.account_id;
```

## Step 5. Verify output

You should see the following output in the logs:

```output
2020-11-09 19:46:42.633 UTC [3944] LOG:  AUDIT: OBJECT,1,1,READ,SELECT,TABLE,public.account,"select password
          from account;",<not logged>
2020-11-09 19:47:02.531 UTC [3944] LOG:  AUDIT: OBJECT,2,1,WRITE,UPDATE,TABLE,public.account,"update account
           set password = 'HASH2';",<not logged>
I1109 19:47:09.418772  3944 ybccmds.c:453] Creating Table yugabyte.public.account_role_map
I1109 19:47:09.418812  3944 pg_ddl.cc:310] PgCreateTable: creating a transactional table: yugabyte.account_role_map
I1109 19:47:09.538868  3944 table_creator.cc:307] Created table yugabyte.account_role_map of type PGSQL_TABLE_TYPE
2020-11-09 19:47:22.752 UTC [3944] LOG:  AUDIT: OBJECT,3,1,READ,SELECT,TABLE,public.account,"select account.password,
               account_role_map.role_id
          from account
               inner join account_role_map
                    on account.id = account_role_map.account_id;",<not logged>
2020-11-09 19:47:22.752 UTC [3944] LOG:  AUDIT: OBJECT,3,1,READ,SELECT,TABLE,public.account_role_map,"select account.password,
               account_role_map.role_id
          from account
               inner join account_role_map
                    on account.id = account_role_map.account_id;",<not logged>
```
