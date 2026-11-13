-- Rename product-prefixed tables after the Replicator -> Parom rename.
-- ALTER TABLE ... RENAME TO has the same syntax on H2 and PostgreSQL.
ALTER TABLE replicator_dr_snapshots RENAME TO parom_dr_snapshots;
ALTER TABLE replicator_streaming_leases RENAME TO parom_streaming_leases;
