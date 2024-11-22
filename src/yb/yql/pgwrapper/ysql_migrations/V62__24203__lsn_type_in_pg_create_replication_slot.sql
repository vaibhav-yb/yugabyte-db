BEGIN;
  SET LOCAL yb_non_ddl_txn_for_sys_tables_allowed TO true;

  -- Add a parameter to the pg_create_logical_replication_slot method
  DELETE FROM pg_catalog.pg_proc WHERE proname = 'pg_create_logical_replication_slot' AND
    pronamespace = 'pg_catalog'::regnamespace;
  INSERT INTO pg_catalog.pg_proc (
    oid, proname, pronamespace, proowner, prolang, procost, prorows, provariadic, protransform,
    prokind, prosecdef, proleakproof, proisstrict, proretset, provolatile, proparallel,
    pronargs, pronargdefaults, prorettype, proargtypes, proallargtypes, proargmodes,
    proargnames, proargdefaults, protrftypes, prosrc, probin, proconfig, proacl
  ) VALUES (
    3786, 'pg_create_logical_replication_slot', 11, 10, 12, 1, 0, 0, '-', 'f', false, false, true,
    false, 'v', 'u', 4, 2, 2249, '19 19 16 16 19', '{19,19,16,16,19,19,3220}',
    '{i,i,i,i,i,o,o}', '{slot_name,plugin,temporary,twophase,yb_lsn_type,slot_name,lsn}',
    NULL, NULL, 'pg_create_logical_replication_slot', NULL, NULL, NULL)
  ON CONFLICT DO NOTHING;
COMMIT;
