---
--- Verify renaming on temp tables
---
CREATE TEMP TABLE temp_table(a int primary key, b int);
CREATE INDEX temp_table_b_idx ON temp_table(b);
ALTER INDEX temp_table_pkey RENAME TO temp_table_pkey_new;
ALTER INDEX temp_table_b_idx RENAME TO temp_table_b_idx_new;

---
--- Verify yb_db_admin role can ALTER table
---
CREATE TABLE foo(a INT UNIQUE);
CREATE TABLE bar(b INT);
ALTER TABLE bar ADD CONSTRAINT baz FOREIGN KEY (b) REFERENCES foo(a);
CREATE TABLE table_other(a int, b int);
CREATE INDEX index_table_other ON table_other(a);
CREATE USER regress_alter_table_user1;
SET SESSION AUTHORIZATION yb_db_admin;
ALTER TABLE table_other RENAME to table_new;
ALTER TABLE table_new OWNER TO regress_alter_table_user1;
ALTER TABLE bar DROP CONSTRAINT baz;
ALTER TABLE pg_database RENAME TO test; -- should fail
ALTER TABLE pg_tablespace OWNER TO regress_alter_table_user1; -- should fail
---
--- Verify yb_db_admin role can ALTER index
---
ALTER INDEX index_table_other RENAME TO index_table_other_new;
RESET SESSION AUTHORIZATION;
DROP TABLE foo;
DROP TABLE bar;
DROP TABLE table_new;
DROP USER regress_alter_table_user1;

---
--- Verify alter table which requires table rewrite
---

--- Table without primary key index
--- Empty table case
CREATE TABLE no_pk_tbl(k INT);
ALTER TABLE no_pk_tbl ADD COLUMN s1 TIMESTAMP DEFAULT clock_timestamp();
ALTER TABLE no_pk_tbl ADD COLUMN v1 SERIAL;
--- Non-empty case
INSERT INTO no_pk_tbl VALUES(1), (2), (3);
ALTER TABLE no_pk_tbl ADD COLUMN s2 TIMESTAMP DEFAULT clock_timestamp();
ALTER TABLE no_pk_tbl ADD COLUMN v2 SERIAL;
DROP TABLE no_pk_tbl;

--- Table with primary key index
--- Empty table case
CREATE TABLE pk_tbl(k INT PRIMARY KEY);
ALTER TABLE pk_tbl ADD COLUMN s1 TIMESTAMP DEFAULT clock_timestamp();
ALTER TABLE pk_tbl ADD COLUMN v1 SERIAL;
--- Non-empty case
INSERT INTO pk_tbl VALUES(1), (2), (3);
ALTER TABLE pk_tbl ADD COLUMN s2 TIMESTAMP DEFAULT clock_timestamp();
ALTER TABLE pk_tbl ADD COLUMN v2 SERIAL;
DROP TABLE pk_tbl;

-- Verify cache cleanup of table names when TABLE RENAME fails.
CREATE TABLE rename_test (id int);
SET yb_test_fail_next_ddl TO true;
ALTER TABLE rename_test RENAME TO foobar;
-- The table name must be unchanged.
SELECT * FROM rename_test;
-- The name 'foobar' must be invalid.
SELECT * FROM foobar;
-- Rename operation must succeed now.
ALTER TABLE rename_test RENAME TO foobar;
DROP TABLE foobar;

