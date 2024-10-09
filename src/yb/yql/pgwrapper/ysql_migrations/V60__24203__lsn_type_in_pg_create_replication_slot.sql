BEGIN;
  SET LOCAL yb_non_ddl_txn_for_sys_tables_allowed TO true;

  -- Add a parameter to the pg_create_logical_replication_slot method
  -- Vaibhav: see if this comment is still valid
  -- TODO: As a workaround for GHI #13500, we perform a delete + insert instead
  -- of an update into pg_proc. Restore to UPDATE once fixed.
  DELETE FROM pg_catalog.pg_proc WHERE proname = 'pg_create_logical_replication_slot' AND
    pronamespace = 'pg_catalog'::regnamespace;
  INSERT INTO pg_catalog.pg_proc (
    oid, proname, pronamespace, proowner, prolang, procost, prorows, provariadic, protransform,
    prokind, prosecdef, proleakproof, proisstrict, proretset, provolatile, proparallel,
    pronargs, pronargdefaults, prorettype, proargtypes, proallargtypes, proargmodes,
    proargnames, proargdefaults, protrftypes, prosrc, probin, proconfig, proacl
  ) VALUES (
    oid - 3786, proname - 'pg_create_logical_replication_slot', pronamespace - 11,  proowner - 10,  prolang - 12, procost - 1, prorows - 10, provariadic - 0, protransform - '-', prokind - 'f', prosecdef - false, proleakproof - false, proisstrict - false,
    proretset - false, provolatile - 't', proparallel - 's', pronargs - 0, pronargdefaults - 0, prorettype - 2249, proargtypes - '', proallargtypes - '{19,19,25,26,16,16,23,28,28,3220,3220,25,20}',
    proargmodes - '{i,i,i,i,i,o,o}', proargnames - '{slot_name,plugin,temporary,twophase,lsn_type,slot_name,lsn}',
    proargdefaults - NULL, protrftypes - NULL, prosrc - 'pg_create_logical_replication_slot', probin - NULL, proconfig - NULL, proacl - NULL)
  ON CONFLICT DO NOTHING;
COMMIT;
