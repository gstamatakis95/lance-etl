-- Iceberg source table for the lance-etl pipeline.
--
-- This is the schema the IcebergToLanceETL job reads from. Column names map to ETLConfig:
--   key_col        = vector_id           (merge key, one row per id wins last-write)
--   op_col         = op                  (insert / update / delete change marker, not stored in Lance)
--   ts_col         = event_timestamp     (event time, the single canonical clock)
--   window_column  = processing_timestamp (incremental read window, also the Iceberg partition)
--   vectors_col    = vectors             (map of named vectors)
--   metadata_col   = metadata            (map of string metadata)
--   partition_cols = org_id, tenant_id, namespace (Lance dataset routing)
--
-- Adjust the catalog name, database, and table name to your environment.

CREATE TABLE bench.db.sift (
    org_id               STRING    NOT NULL              COMMENT 'Routing component: organization id',
    tenant_id            STRING    NOT NULL              COMMENT 'Routing component: tenant id',
    namespace            STRING    NOT NULL              COMMENT 'Routing component: namespace',
    vector_id            STRING    NOT NULL              COMMENT 'Merge key, unique per logical record',
    op                   STRING    NOT NULL              COMMENT 'Change op: insert, update, or delete (drives merge, not stored)',
    event_timestamp      TIMESTAMP NOT NULL              COMMENT 'Event time, the canonical clock (ETLConfig.ts_col)',
    processing_timestamp TIMESTAMP NOT NULL              COMMENT 'Pipeline processing time, the read window and Iceberg partition (ETLConfig.window_column)',
    vectors              MAP<STRING, ARRAY<FLOAT>>       COMMENT 'Named vectors, one entry per vector field',
    texts                MAP<STRING, STRING>             COMMENT 'Named text fields, one entry per text field',
    metadata             MAP<STRING, STRING>             COMMENT 'Arbitrary string metadata',
    ttl                  BIGINT                          COMMENT 'Optional per-row lifetime in seconds; cast to a Duration column for row-level TTL'
)
USING iceberg
PARTITIONED BY (tenant_id, namespace, org_id, hours(processing_timestamp))
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
--    batch scans). The processing_timestamp partition lets Iceberg prune files, and the window_column
--    filter narrows rows to the batch.
-- 2. op drives merge_insert: insert and update upsert the row, delete removes the key. op itself is
--    not written to the Lance dataset. Delete markers default to delete / DELETE / d.
-- 3. Lance has no map type, so the ETL unpacks the maps before write. The named vectors and texts are
--    PIVOTED: each key declared in ETLConfig.vector_fields becomes a concrete column holding
--    vectors[key], and each key in ETLConfig.text_fields becomes a concrete column holding texts[key].
--    Undeclared keys are dropped and a declared key absent from a row yields NULL for that column. The
--    metadata map stays stored-only payload, flattened into the positional list columns metadata_keys
--    and metadata_values (see the etl.py module docstring).
-- 4. INDEXABLE vs PAYLOAD. The IVF_RQ vector index needs a fixed-size-list<float32,dim> column and
--    the INVERTED full-text index needs a string column. The pivot in note 3 produces exactly those:
--    a named vector becomes one concrete fixed-size-list column the IVF_RQ index targets (pair it with
--    a column_types cast) and a named text field becomes one concrete string column the INVERTED index
--    targets. Declare every searchable or vector-indexed field in vector_fields or text_fields so it
--    lands as a concrete column. Leave stored-only data in the metadata map, which stays flattened.
-- 5. ttl is optional. Row-level TTL only runs when the maintenance job is given --ttl-column. The ETL
--    passes it through and casts BIGINT seconds to an Arrow Duration; the delete predicate is
--    `event_timestamp + ttl < now`. Omit this column if you do not use row-level TTL.
