<!--
+++
private=true
+++
-->

Create a role and a database user, and provide the user with READ access to all the resources which need to be migrated.

1. Create a role that has the privileges as listed in the following table:

   | Permission | Object type in the source schema |
   | :--------- | :---------------------------------- |
   | `SELECT` | VIEW, SEQUENCE, TABLE PARTITION, TABLE, SYNONYM, MATERIALIZED VIEW |
   | `EXECUTE` | PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, TYPE |

   Change the `<SCHEMA_NAME>` appropriately in the following snippets, and run the following steps as a privileged user.

   ```sql
   CREATE ROLE <SCHEMA_NAME>_reader_role;

   BEGIN
       FOR R IN (SELECT owner, object_name FROM all_objects WHERE owner='<SCHEMA_NAME>' and object_type in ('VIEW','SEQUENCE','TABLE PARTITION','TABLE','SYNONYM','MATERIALIZED VIEW'))
       LOOP
           EXECUTE IMMEDIATE 'grant select on '||R.owner||'."'||R.object_name||'" to <SCHEMA_NAME>_reader_role';
       END LOOP;
   END;
   /

   BEGIN
       FOR R IN (SELECT owner, object_name FROM all_objects WHERE owner='<SCHEMA_NAME>' and object_type in ('PROCEDURE','FUNCTION','PACKAGE','PACKAGE BODY', 'TYPE'))
       LOOP
           EXECUTE IMMEDIATE 'grant execute on '||R.owner||'."'||R.object_name||'" to <SCHEMA_NAME>_reader_role';
       END LOOP;
   END;
   /
   ```

1. Create a user `ybvoyager` and grant `CONNECT` and `<SCHEMA_NAME>_reader_role` to the user:

   ```sql
   CREATE USER ybvoyager IDENTIFIED BY password;
   GRANT CONNECT TO ybvoyager;
   GRANT <SCHEMA_NAME>_reader_role TO ybvoyager;
   ```

1. [OPTIONAL] Grant `SELECT_CATALOG_ROLE` to `ybvoyager`. This role might be required in migration planning and debugging.

   ```sql
   GRANT SELECT_CATALOG_ROLE TO ybvoyager;
   ```

   The `ybvoyager` user can now be used for migration.
