--
-- REPLICATION SLOT
--
CREATE ROLE regress_replicationslot_user LOGIN SUPERUSER;
CREATE ROLE regress_replicationslot_replication_user WITH REPLICATION;
CREATE ROLE regress_replicationslot_dummy;

SET SESSION AUTHORIZATION 'regress_replicationslot_user';

SELECT * FROM pg_create_logical_replication_slot('testslot1', 'yboutput', false);
SELECT * FROM pg_create_logical_replication_slot('testslot2', 'yboutput', false);

-- Cannot do SELECT * since yb_stream_id changes across runs.
SELECT slot_name, plugin, slot_type, database, temporary, active,
    active_pid, xmin, catalog_xmin, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots;

-- drop the replication slot and create with same name again.
SELECT * FROM pg_drop_replication_slot('testslot1');
-- TODO(#19263): Change the slot to temporary once supported.
SELECT * FROM pg_create_logical_replication_slot('testslot1', 'yboutput', false);

-- unsupported cases
SELECT * FROM pg_create_logical_replication_slot('testslot_unsupported_plugin', 'unsupported_plugin', false);
SELECT * FROM pg_create_logical_replication_slot('testslot_unsupported_temporary', 'yboutput', true);
SELECT * FROM pg_create_physical_replication_slot('testslot_unsupported_physical', true, false);

-- creating replication slot with same name fails.
SELECT * FROM pg_create_logical_replication_slot('testslot1', 'yboutput', false);

-- success since user has 'replication' role
SET ROLE regress_replicationslot_replication_user;
SELECT * FROM pg_create_logical_replication_slot('testslot3', 'yboutput', false);
RESET ROLE;

-- fail - must have replication or superuser role
SET ROLE regress_replicationslot_dummy;
SELECT * FROM pg_create_logical_replication_slot('testslot4', 'yboutput', false);
RESET ROLE;

-- drop replication slots
SELECT * FROM pg_drop_replication_slot('testslot1');
SELECT * FROM pg_drop_replication_slot('testslot2');
SELECT * FROM pg_drop_replication_slot('testslot3');
SELECT slot_name, plugin, slot_type, database, temporary, active,
    active_pid, xmin, catalog_xmin, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots;

-- drop non-existent replication slot
SELECT * FROM pg_drop_replication_slot('testslot_nonexistent');

RESET SESSION AUTHORIZATION;
DROP ROLE regress_replicationslot_user;
DROP ROLE regress_replicationslot_replication_user;
DROP ROLE regress_replicationslot_dummy;
