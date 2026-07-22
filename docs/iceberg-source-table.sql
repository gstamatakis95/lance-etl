-- Iceberg source table for the lance-etl pipeline.
--
-- This is the fixed Iceberg source envelope. Each immutable dataset specification declares the
-- vector, text, and metadata keys projected from the three maps.
--
-- Adjust the catalog name, database, and table name to your environment.

CREATE TABLE bench.db.sift (
    org_id               STRING    NOT NULL              COMMENT 'Routing component: organization id',
    tenant_id            STRING    NOT NULL              COMMENT 'Routing component: tenant id',
    namespace            STRING    NOT NULL              COMMENT 'Routing component: namespace',
    record_id            STRING    NOT NULL              COMMENT 'Merge key, unique per logical record',
    op                   STRING    NOT NULL              COMMENT 'Change op: insert, update, or delete (drives merge, not stored)',
    ts                   TIMESTAMP NOT NULL              COMMENT 'The single canonical event and retention clock',
    vectors              MAP<STRING, ARRAY<FLOAT>>       COMMENT 'Named vectors declared by the active specification',
    texts                MAP<STRING, STRING>             COMMENT 'Named text fields declared by the active specification',
    metadata             MAP<STRING, STRING>             COMMENT 'Named scalar fields declared by the active specification'
)
USING iceberg
PARTITIONED BY (tenant_id, namespace, org_id, hours(ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.parquet.compression-codec' = 'zstd'
);

-- Notes
-- 1. Reads use the exact snapshot identities recorded by the control plane, never wall-clock
--    windows. A baseline uses snapshot-id. An append uses start-snapshot-id and end-snapshot-id.
--    The hours(ts) partition is physical source evidence and never becomes a replay cursor.
-- 2. op drives the replay-safe merge. Insert and update produce a live post-image. Delete produces
--    a tombstone that retention may later materialize. op itself is not written to Lance.
-- 3. Executors project only the keys frozen in the work item's immutable specification revision.
--    Undeclared source keys are ignored. Changing the target schema requires a new revision and a
--    rebuild, so there is no runtime schema discovery or grow-only dynamic pivot.
-- 4. Vector fields are cast to the exact fixed-size-list<float32> dimension declared by the
--    specification. Text and metadata fields become string columns. A missing declared key yields
--    NULL. A present vector with the wrong dimension rejects the work item.
-- 5. Retention is not a per-row column. Expiry derives from ts plus the retention window on the spec
--    revision (record_retention_seconds). When that window is set, the maintenance job deletes every
--    row whose ts is before now minus the window. When it is unset (the default), no record expires.
