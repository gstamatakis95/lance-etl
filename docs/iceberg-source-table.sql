-- Iceberg source table for the lance-etl pipeline.
--
-- This is the schema the IcebergToLanceETL job reads from. Column names map to ETLConfig:
--   key_col        = record_id              (merge key, one row per id wins last-write)
--   op_col         = op                     (insert / update / delete change marker, not stored in Lance)
--   ts_col         = ts                     (the single canonical clock, also the read window and Iceberg partition)
--   vectors_col    = vectors                (map of named vectors)
--   texts_col      = texts                  (map of named text fields)
--   metadata_col   = metadata               (map of string metadata)
--   partition_cols = org_id, tenant_id, namespace (Lance dataset routing)
--
-- Adjust the catalog name, database, and table name to your environment.

CREATE TABLE bench.db.sift (
    org_id               STRING    NOT NULL              COMMENT 'Routing component: organization id',
    tenant_id            STRING    NOT NULL              COMMENT 'Routing component: tenant id',
    namespace            STRING    NOT NULL              COMMENT 'Routing component: namespace',
    record_id            STRING    NOT NULL              COMMENT 'Merge key, unique per logical record',
    op                   STRING    NOT NULL              COMMENT 'Change op: insert, update, or delete (drives merge, not stored)',
    ts                   TIMESTAMP NOT NULL              COMMENT 'The single canonical clock, the read window, and the Iceberg partition (ETLConfig.ts_col)',
    vectors              MAP<STRING, ARRAY<FLOAT>>       COMMENT 'Named vectors, every key pivots into a concrete fixed-size-list column',
    texts                MAP<STRING, STRING>             COMMENT 'Named text fields, every key pivots into a concrete string column',
    metadata             MAP<STRING, STRING>             COMMENT 'Arbitrary string metadata, every key pivots into a concrete string column'
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
-- 1. Incremental reads resolve a wall-clock window to start-snapshot-id / end-snapshot-id from the
--    {table}.snapshots metadata, not start-timestamp / end-timestamp (Iceberg 1.10 rejects those on
--    batch scans). The hours(ts) partition lets Iceberg prune files, and the ts window filter narrows
--    rows to the batch.
-- 2. op drives merge_insert: insert and update upsert the row, delete removes the key. op itself is
--    not written to the Lance dataset. Delete markers default to delete / DELETE / d.
-- 3. Lance has no map type, so the ETL expands every map column into concrete per-key columns before
--    write. The pivot runs per-dataset on the executor (not globally in Spark) so each org's dataset
--    contains only the keys that org actually uses. For each map column, every distinct key present in
--    the group's data becomes a column: the key is used as the column name as-is and the value is
--    the column value. Keys that collide with an existing or reserved column name are silently skipped
--    and counted in the dataset.invalid_map_keys metric. A key absent from a row yields NULL for that
--    column in that row.
-- 4. COLUMN TYPES after pivot. Vector columns (from the vectors map) arrive as list<float32> and are
--    cast automatically to a fixed-size-list using the first non-null entry to infer the dimension.
--    If the inner type is float64 (Spark may widen ARRAY<FLOAT> in some environments), it is
--    normalized to float32 before the FSL cast. Text columns (from the texts map) become STRING
--    columns. Metadata columns (from the metadata map) become STRING columns and are filter-eligible
--    downstream (a BTREE or BITMAP scalar index can cover any metadata key that has sufficient
--    cardinality). New keys appearing in a later ETL window are absorbed by the existing add_columns
--    schema evolution in apply_merge without operator intervention. The per-dataset Lance schema is
--    grow-only: columns are never removed, so existing readers and indexes are never broken.
-- 5. Retention is not a per-row column. Expiry derives from ts plus the retention window on the spec
--    revision (record_retention_seconds). When that window is set, the maintenance job deletes every
--    row whose ts is before now minus the window. When it is unset (the default), no record expires.
