# Iceberg-to-Lance Multi-Tenant Synchronization

> Historical design input. ADR 0042 and the repository README supersede this independently
> scalable S3 and worker design. The implemented project is local-first, uses one PostgreSQL
> control plane and one local Spark-backed reconciler, and has no Kubernetes or Airflow runtime
> dependency.

**Project state:** Superseded design input
**Target:** One S3-backed Lance dataset per tenant
**Source:** One shared Apache Iceberg table containing all tenants
**Control plane:** PostgreSQL
**Execution:** PySpark extraction plus independently scalable Lance applicator workers
**Delivery semantics:** At-least-once processing with replay-safe state reconciliation
**Document date:** 2026-07-16

---

## 1. Executive summary

This system synchronizes tenant-scoped state from a shared Apache Iceberg table into one Lance dataset per tenant on S3.

The expected operating profile is:

- Approximately one million tenants.
- The majority of tenants normally share the same Iceberg cursor.
- A small minority may fail or lag independently.
- The synchronization runs hourly or daily.
- The Iceberg source receives inserts, updates, and deletes through CDC.
- Each tenant has a durable PostgreSQL cursor containing its last successfully synchronized Iceberg snapshot ID.
- The target Lance dataset may contain multiple fragments and versions.
- Destination operations may be replayed.
- A failure for one tenant must not block unrelated tenants.

The final architecture deliberately separates:

1. **Source-range discovery**
   - Freeze a start and end Iceberg snapshot.
   - Scan the shared Iceberg changelog once for a cursor cohort.
   - Use the changelog to identify affected tenants and keys.

2. **Authoritative state extraction**
   - Read the source Iceberg table at the frozen end snapshot.
   - Build the complete final state for every affected tenant.
   - Materialize immutable, checksum-protected tenant artifacts on S3.

3. **Tenant-isolated application**
   - Claim tenant work through PostgreSQL leases.
   - Reconcile one tenant artifact into one Lance dataset.
   - Advance only that tenant’s cursor after Lance succeeds.
   - Retry ambiguous failures by replaying the same immutable artifact.

4. **Bulk advancement of unchanged tenants**
   - After an extraction run is verified complete, advance cursor rows for tenants that had no source changes.
   - Perform updates in bounded PostgreSQL transactions.

The key safety property is:

> A tenant cursor advances only after the tenant’s Lance dataset is known to represent the frozen Iceberg end snapshot, or after a verified extraction proves that the tenant had no changes in the range.

This is not a distributed exactly-once transaction. It is an at-least-once state-reconciliation design that converges safely under retries.

---

## 2. Final architecture

```text
                               Shared Iceberg table
                                        │
                     freeze cohort start and current head
                                        │
                                        ▼
                         Shared Iceberg changelog scan
                           (one scan per cursor cohort)
                                        │
                         changed tenants / changed keys
                                        │
                                        ▼
                       Iceberg time-travel read at frozen
                                end snapshot
                                        │
                          complete state per changed tenant
                                        │
                                        ▼
                  Immutable tenant artifacts and run manifest on S3
                                        │
                                        ▼
                           PostgreSQL tenant work queue
                                        │
          ┌─────────────────────────────┼─────────────────────────────┐
          ▼                             ▼                             ▼
   Lance worker 1                Lance worker 2                Lance worker N
          │                             │                             │
 tenant-A/data.lance            tenant-B/data.lance            tenant-C/data.lance
          │                             │                             │
 cursor A advances              B retries independently         cursor C advances
```

### 2.1 Component responsibilities

| Component | Responsibility |
|---|---|
| Iceberg table | Authoritative multi-tenant source state and snapshot history |
| PySpark extractor | Freezes ranges, validates lineage, discovers affected tenants, extracts complete tenant state |
| S3 artifact store | Durable replayable tenant snapshots and extraction manifests |
| PostgreSQL | Per-tenant cursors, extraction-run state, work queue, leases, errors, maintenance metadata |
| Lance workers | Reconcile one tenant state artifact into one tenant Lance dataset |
| Reconciliation service | Detect silent drift between Iceberg and Lance |
| Maintenance workers | Compact and clean Lance datasets while holding the same tenant lease |

---

## 3. Non-negotiable invariants

These invariants define correctness.

1. **One Lance dataset per tenant.**
2. **One active mutating worker per tenant dataset.**
3. **Every extraction run freezes one immutable Iceberg end snapshot.**
4. **A tenant cursor never moves backward.**
5. **Snapshot IDs are identifiers, not numeric offsets.**
6. **Cursor chronology is validated against Iceberg ancestry.**
7. **A tenant cursor advances only after successful Lance reconciliation or verified absence of tenant changes.**
8. **A failed or ambiguous tenant application leaves the cursor unchanged.**
9. **Retries use the same deterministic extraction identity and immutable artifact.**
10. **Lance merge keys are unique inside each tenant dataset.**
11. **The source table UUID is persisted and checked to detect drop-and-recreate events.**
12. **Schema compatibility is verified before mutation.**
13. **Maintenance obtains the same per-tenant lease as synchronization.**
14. **Iceberg snapshots required by unfinished extraction are not expired.**
15. **No tenant is marked unchanged until the complete extraction run passes its publication barrier.**
16. **Every cursor change is auditable by extraction run and target Lance version.**
17. **Continuous reconciliation is required. CDC correctness is not trusted indefinitely.**

---

## 4. Delivery and consistency semantics

### 4.1 What the system guarantees

The design provides:

- Independent tenant failure handling.
- At-least-once application.
- Replay-safe convergence to the frozen Iceberg end state.
- No cursor advancement on known or ambiguous target failure.
- Bounded source scans through cursor cohorts.
- Durable recovery after Spark or worker process loss.
- Detection of rollback, expired history, table replacement, schema incompatibility, duplicate keys, and corrupted artifacts.

### 4.2 What it does not guarantee

The design does not guarantee:

- Original source-database CDC event order.
- Visibility of every intermediate source update.
- A cross-system atomic transaction between Lance and PostgreSQL.
- Zero downtime for every external dependency.
- Exactly-once side effects outside the Lance state store.
- Recovery from expired Iceberg history unless extraction artifacts already exist or the tenant is rebuilt.

### 4.3 Why replay is safe

Every changed tenant is reconciled from a complete, immutable representation of its state at a frozen Iceberg snapshot.

A retry therefore applies:

```text
desired tenant state at snapshot S
```

rather than attempting to continue a partially completed sequence of row-level side effects.

The final state is deterministic even when:

- Lance committed but PostgreSQL did not.
- A worker timed out after a successful S3 commit.
- A worker lease expired and another worker took over.
- The same work item is applied multiple times.
- A preceding application partially completed.

---

## 5. Apache Iceberg assumptions and constraints

### 5.1 Changelog range semantics

Iceberg’s Spark `create_changelog_view` supports:

- `start-snapshot-id`: exclusive.
- `end-snapshot-id`: inclusive.
- `_change_type`.
- `_change_ordinal`.
- `_commit_snapshot_id`.

Use snapshot IDs for durable cursors, not timestamps.

```sql
CALL prod.system.create_changelog_view(
    table => 'app.source_table',
    changelog_view => 'source_changes',
    options => map(
        'start-snapshot-id', '123',
        'end-snapshot-id',   '456'
    ),
    identifier_columns => array('tenant_id', 'record_id'),
    compute_updates => true,
    net_changes => false
);
```

### 5.2 Hard limitation: delete files

The current Iceberg incremental changelog implementation rejects snapshots containing delete manifests/delete files.

This commonly affects merge-on-read row-level operations.

For this design, the source must use copy-on-write for relevant mutations:

```sql
ALTER TABLE prod.app.source_table SET TBLPROPERTIES (
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode'  = 'copy-on-write'
);
```

This changes future writes only. Historical snapshots inside the recovery window must also be tested.

### 5.3 Changelog purpose in this design

Do not use changelog row order to reconstruct final tenant state.

Use the changelog only to discover:

```text
affected tenant IDs
affected primary keys
source snapshots represented by the range
```

Then read the authoritative end state through Iceberg time travel.

This avoids relying on intra-snapshot row ordering, which is not an original CDC ordering guarantee.

### 5.4 Snapshot retention

Required retention must exceed:

```text
maximum extractor outage
+ maximum retry and repair duration
+ deployment rollback window
+ operational safety margin
```

For example:

```text
maximum outage:       3 days
repair duration:      2 days
deployment rollback:  1 day
safety margin:        2 days
--------------------------------
minimum retention:    8 days
```

At about 1,000 snapshots per day, eight days means approximately 8,000 retained snapshots.

Retention policy must be tested for:

- metadata size.
- manifest planning latency.
- maintenance duration.
- catalog behavior.
- S3 request volume.

### 5.5 Table replacement detection

Persist the Iceberg table UUID in all control records.

If the table name remains the same but its UUID changes:

```text
STOP incremental synchronization
mark affected tenants REBUILD_REQUIRED
```

---

## 6. Cursor and cohort strategy

### 6.1 Explicit cursor per tenant

Every tenant has an explicit snapshot cursor:

> All source changes through this snapshot have been successfully reflected in the tenant’s Lance dataset.

At one million tenants, explicit cursor storage is reasonable.

Hourly full cursor updates average about 278 rows per second, although actual writes occur as bursts and create PostgreSQL WAL and MVCC churn. Use bulk, bounded, set-based updates rather than one statement per tenant.

### 6.2 Dominant cohort

Most tenants are expected to share one snapshot ID.

Example:

```text
999,950 tenants → snapshot S100
30 tenants      → snapshot S90
20 tenants      → snapshot S40
current head    → snapshot S150
```

The regular run:

1. Identifies the dominant cohort at `S100`.
2. Scans Iceberg once for `(S100, S150]`.
3. Processes changed tenants.
4. Advances unchanged successful tenants to `S150`.
5. Leaves failed tenants at `S100`.

Lagging tenants are handled by a separate retry flow.

### 6.3 Do not order snapshot IDs numerically

This is invalid:

```python
snapshot_id > previous_snapshot_id
```

Use Iceberg lineage/ancestry.

### 6.4 Cohorts for lagging tenants

Group lagging tenants by exact cursor where practical.

If there are too many distinct cursors, group by bounded ancestry rank ranges, but apply per-tenant filtering so a tenant receives only changes after its own cursor.

The normal dominant-cohort path must not wait for severe laggards.

---

## 7. PostgreSQL schema

The following is a recommended baseline. Types and indexes should be adapted to deployment conventions.

### 7.1 Tenant cursor

```sql
CREATE TYPE tenant_sync_status AS ENUM (
    'ACTIVE',
    'RETRYING',
    'PAUSED',
    'REBUILD_REQUIRED',
    'RECONCILIATION_REQUIRED',
    'DISABLED'
);

CREATE TABLE tenant_iceberg_cursor (
    source_table_uuid UUID NOT NULL,
    tenant_id TEXT NOT NULL,

    snapshot_id BIGINT NOT NULL,
    lance_uri TEXT NOT NULL,
    lance_version BIGINT,

    status tenant_sync_status NOT NULL DEFAULT 'ACTIVE',

    schema_version INTEGER NOT NULL,
    schema_fingerprint TEXT NOT NULL,

    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ,

    last_attempt_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,

    last_error_code TEXT,
    last_error TEXT,

    last_extraction_run_id UUID,

    fragment_count INTEGER,
    deleted_row_ratio DOUBLE PRECISION,
    last_compacted_at TIMESTAMPTZ,
    last_cleaned_at TIMESTAMPTZ,
    maintenance_due_at TIMESTAMPTZ,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (source_table_uuid, tenant_id),

    CHECK (snapshot_id > 0),
    CHECK (schema_version > 0)
);

CREATE INDEX tenant_cursor_cohort_idx
    ON tenant_iceberg_cursor (
        source_table_uuid,
        snapshot_id
    )
    WHERE status IN ('ACTIVE', 'RETRYING');

CREATE INDEX tenant_cursor_retry_idx
    ON tenant_iceberg_cursor (
        source_table_uuid,
        next_retry_at
    )
    WHERE status = 'RETRYING';

CREATE INDEX tenant_maintenance_due_idx
    ON tenant_iceberg_cursor (
        maintenance_due_at
    )
    WHERE maintenance_due_at IS NOT NULL;
```

Avoid indexing frequently updated timestamp and error columns without a concrete query requirement.

### 7.2 Extraction run

```sql
CREATE TYPE extraction_run_status AS ENUM (
    'CREATED',
    'EXTRACTING',
    'VALIDATING',
    'EXTRACTED',
    'APPLYING',
    'COMPLETED',
    'FAILED',
    'CANCELLED'
);

CREATE TABLE iceberg_extraction_run (
    run_id UUID PRIMARY KEY,

    source_table_uuid UUID NOT NULL,
    source_table_identifier TEXT NOT NULL,
    source_branch TEXT NOT NULL DEFAULT 'main',

    start_snapshot_id BIGINT NOT NULL,
    end_snapshot_id BIGINT NOT NULL,

    cohort_tenant_count BIGINT NOT NULL,

    staging_root_uri TEXT NOT NULL,
    run_manifest_uri TEXT,

    schema_version INTEGER NOT NULL,
    schema_fingerprint TEXT NOT NULL,

    status extraction_run_status NOT NULL,

    changed_key_count BIGINT,
    changed_tenant_count BIGINT,
    artifact_count BIGINT,

    manifest_checksum TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    extracted_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    last_error_code TEXT,
    last_error TEXT,

    UNIQUE (
        source_table_uuid,
        start_snapshot_id,
        end_snapshot_id
    ),

    CHECK (start_snapshot_id <> end_snapshot_id)
);
```

### 7.3 Tenant work

```sql
CREATE TYPE tenant_work_status AS ENUM (
    'READY',
    'RUNNING',
    'SUCCEEDED',
    'RETRYING',
    'PERMANENT_FAILURE',
    'CANCELLED'
);

CREATE TABLE tenant_sync_work (
    run_id UUID NOT NULL
        REFERENCES iceberg_extraction_run(run_id),

    source_table_uuid UUID NOT NULL,
    tenant_id TEXT NOT NULL,

    expected_snapshot_id BIGINT NOT NULL,
    target_snapshot_id BIGINT NOT NULL,

    artifact_uri TEXT NOT NULL,
    artifact_checksum TEXT NOT NULL,
    source_row_count BIGINT NOT NULL,

    schema_version INTEGER NOT NULL,
    schema_fingerprint TEXT NOT NULL,

    status tenant_work_status NOT NULL DEFAULT 'READY',

    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    lease_owner TEXT,
    lease_token BIGINT NOT NULL DEFAULT 0,
    lease_expires_at TIMESTAMPTZ,

    lance_version_before BIGINT,
    lance_version_after BIGINT,

    last_error_code TEXT,
    last_error TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,

    PRIMARY KEY (run_id, tenant_id),

    CHECK (source_row_count >= 0)
);

CREATE INDEX tenant_sync_work_ready_idx
    ON tenant_sync_work (
        next_attempt_at,
        run_id
    )
    WHERE status IN ('READY', 'RETRYING');
```

### 7.4 Cursor history and audit

```sql
CREATE TABLE tenant_cursor_history (
    source_table_uuid UUID NOT NULL,
    tenant_id TEXT NOT NULL,

    previous_snapshot_id BIGINT NOT NULL,
    new_snapshot_id BIGINT NOT NULL,

    run_id UUID NOT NULL,
    lance_version BIGINT,

    changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (
        source_table_uuid,
        tenant_id,
        new_snapshot_id
    )
);
```

History retention may be shorter than operational cursor retention, but should cover incident investigation and reconciliation periods.

---

## 8. S3 layout

### 8.1 Lance datasets

Avoid a flat prefix with one million direct children.

```text
s3://tenant-data/lance/
    ab/
      cd/
        <encoded-tenant-id>/
          data.lance/
```

Use a deterministic hash prefix.

```python
import hashlib
import urllib.parse


def tenant_lance_uri(base_uri: str, tenant_id: str) -> str:
    digest = hashlib.sha256(tenant_id.encode("utf-8")).hexdigest()
    encoded = urllib.parse.quote(tenant_id, safe="")
    return (
        f"{base_uri.rstrip('/')}/"
        f"{digest[:2]}/{digest[2:4]}/"
        f"{encoded}/data.lance"
    )
```

Persist the derived URI at tenant provisioning time. Do not discover tenant datasets through S3 listing.

### 8.2 Extraction artifacts

```text
s3://sync-control/iceberg-to-lance/
    source-table=<table-uuid>/
      end-snapshot=<snapshot-id>/
        run=<run-id>/
          run-manifest.json
          tenant-bucket=0000/
          ...
          tenant-bucket=4095/
          tenants/
            ab/cd/<encoded-tenant-id>/state.arrow
            ab/cd/<encoded-tenant-id>/manifest.json
```

### 8.3 Immutable artifact identity

A tenant artifact is uniquely identified by:

```text
source table UUID
tenant ID
expected start snapshot
target end snapshot
schema fingerprint
```

Never overwrite the same identity with different content.

### 8.4 Tenant artifact manifest

```json
{
  "format_version": 1,
  "run_id": "f1ad84c3-55b4-4fab-9c5f-fadcfb658e31",
  "source_table_uuid": "6ec810c2-2e6d-41af-9243-bc917180e727",
  "tenant_id": "tenant-123",
  "expected_snapshot_id": 123,
  "target_snapshot_id": 456,
  "schema_version": 3,
  "schema_fingerprint": "sha256:...",
  "row_count": 991,
  "content_checksum": "sha256:...",
  "artifact_uri": "s3://.../state.arrow",
  "created_at": "2026-07-16T12:00:00Z"
}
```

---

## 9. PySpark extraction

### 9.1 Spark configuration

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("iceberg-to-lance-extractor")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    # Catalog configuration omitted because it depends on Glue, REST,
    # Hadoop, or another catalog implementation.
    .getOrCreate()
)
```

### 9.2 Read current main head

```python
from typing import Optional


CATALOG = "prod"
TABLE = "app.source_table"
FULL_TABLE = f"{CATALOG}.{TABLE}"


def current_main_snapshot_id() -> Optional[int]:
    row = spark.sql(
        f"""
        SELECT snapshot_id
        FROM {FULL_TABLE}.refs
        WHERE name = 'main'
        """
    ).first()

    return None if row is None else int(row["snapshot_id"])
```

### 9.3 Build exact lineage

At about thousands to tens of thousands of retained snapshots, collecting snapshot metadata to the driver is normally acceptable. Do not collect source data.

```python
from pyspark.sql import DataFrame


def load_exact_lineage(head_snapshot_id: int) -> DataFrame:
    rows = spark.sql(
        f"""
        SELECT snapshot_id, parent_id, committed_at
        FROM {FULL_TABLE}.snapshots
        """
    ).collect()

    by_id = {
        int(row["snapshot_id"]): (
            None if row["parent_id"] is None else int(row["parent_id"])
        )
        for row in rows
    }

    newest_first: list[int] = []
    visited: set[int] = set()
    current: int | None = head_snapshot_id

    while current is not None:
        if current in visited:
            raise RuntimeError(
                f"Cycle detected in snapshot lineage at {current}"
            )

        if current not in by_id:
            raise RuntimeError(
                f"Snapshot {current} is not retained"
            )

        visited.add(current)
        newest_first.append(current)
        current = by_id[current]

    oldest_first = list(reversed(newest_first))

    return spark.createDataFrame(
        [
            (snapshot_id, rank)
            for rank, snapshot_id in enumerate(oldest_first)
        ],
        schema="snapshot_id LONG, snapshot_rank LONG",
    )
```

### 9.4 Validate cohort cursor

```python
def assert_is_ancestor(
    lineage: DataFrame,
    start_snapshot_id: int,
    end_snapshot_id: int,
) -> None:
    ids = {
        int(row["snapshot_id"])
        for row in lineage.select("snapshot_id").collect()
    }

    if end_snapshot_id not in ids:
        raise RuntimeError(
            f"End snapshot {end_snapshot_id} is not in the active lineage"
        )

    if start_snapshot_id not in ids:
        raise RuntimeError(
            f"Start snapshot {start_snapshot_id} is not an ancestor of "
            f"end snapshot {end_snapshot_id}"
        )
```

### 9.5 Create shared changelog view

```python
from uuid import uuid4
from pyspark.sql import DataFrame


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def create_changelog_view(
    start_snapshot_id: int,
    end_snapshot_id: int,
) -> tuple[str, DataFrame]:
    view_name = f"tenant_cdc_{uuid4().hex}"

    spark.sql(
        f"""
        CALL {CATALOG}.system.create_changelog_view(
            table => {sql_literal(TABLE)},
            changelog_view => {sql_literal(view_name)},
            options => map(
                'start-snapshot-id', '{start_snapshot_id}',
                'end-snapshot-id',   '{end_snapshot_id}'
            ),
            identifier_columns => array(
                'tenant_id',
                'record_id'
            ),
            compute_updates => true,
            net_changes => false
        )
        """
    )

    return view_name, spark.table(view_name)
```

### 9.6 Discover affected tenants and keys

```python
from pyspark.sql import functions as F


def changed_keys(changelog: DataFrame) -> DataFrame:
    return (
        changelog
        .select("tenant_id", "record_id")
        .where(
            F.col("tenant_id").isNotNull()
            & F.col("record_id").isNotNull()
        )
        .distinct()
    )
```

Reject null keys rather than silently continuing.

### 9.7 Read authoritative state at end snapshot

For changed keys:

```python
def read_end_state(end_snapshot_id: int) -> DataFrame:
    return (
        spark.read
        .format("iceberg")
        .option("snapshot-id", end_snapshot_id)
        .load(FULL_TABLE)
    )
```

For a complete tenant reconciliation, produce the full state for every affected tenant:

```python
def affected_tenant_state(
    end_state: DataFrame,
    affected_tenants: DataFrame,
) -> DataFrame:
    return (
        end_state.alias("state")
        .join(
            affected_tenants.alias("tenant"),
            F.col("state.tenant_id") == F.col("tenant.tenant_id"),
            "inner",
        )
        .select("state.*")
    )
```

This can be more expensive than reading changed keys only. It is chosen because it enables a single authoritative tenant reconciliation and eliminates multi-operation partial-state windows.

For extremely large individual tenants, an alternate keyed-delta strategy may be required, but it must preserve atomicity or tolerate intermediate visibility explicitly.

### 9.8 Validate unique tenant keys

```python
def duplicate_keys(tenant_state: DataFrame) -> DataFrame:
    return (
        tenant_state
        .groupBy("tenant_id", "record_id")
        .count()
        .where(F.col("count") > 1)
    )
```

Any duplicate is a permanent data-quality failure for that tenant because Lance does not enforce a primary key and merge-insert uses a join key that should be unique.

### 9.9 Bucket extraction output

Do not generate one Spark partition per tenant.

```python
STAGING_BUCKETS = 4096


def with_tenant_bucket(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "_tenant_bucket",
        F.pmod(
            F.xxhash64("tenant_id"),
            F.lit(STAGING_BUCKETS),
        ).cast("int"),
    )
```

Write an intermediate bucketed dataset:

```python
def write_bucketed_state(df: DataFrame, uri: str) -> None:
    (
        with_tenant_bucket(df)
        .repartition(STAGING_BUCKETS, "_tenant_bucket")
        .write
        .mode("overwrite")
        .partitionBy("_tenant_bucket")
        .parquet(uri)
    )
```

A downstream artifact-publishing stage can transform bucketed rows into immutable Arrow IPC tenant artifacts.

### 9.10 Extraction publication barrier

An extraction run may transition to `EXTRACTED` only after:

- Source table UUID matches.
- Start is an ancestor of end.
- Changelog completed.
- No unsupported delete-file snapshot was encountered.
- Duplicate key checks passed or affected tenants were quarantined.
- All artifact uploads completed.
- Artifact checksums verify.
- Tenant work rows were inserted.
- Run-manifest checksum verifies.
- A final immutable run marker was published.

Only after this barrier may unchanged tenants be advanced.

---

## 10. Bulk advancement of unchanged tenants

Tenants absent from the changed-tenant work list can advance directly to the frozen end snapshot, but only after the extraction publication barrier.

Use bounded batches to control transaction duration, WAL, locks, and replica lag.

Example PostgreSQL function pattern:

```sql
WITH candidates AS (
    SELECT cursor.source_table_uuid, cursor.tenant_id
    FROM tenant_iceberg_cursor AS cursor
    WHERE cursor.source_table_uuid = :table_uuid
      AND cursor.snapshot_id = :start_snapshot_id
      AND cursor.status = 'ACTIVE'
      AND NOT EXISTS (
          SELECT 1
          FROM tenant_sync_work AS work
          WHERE work.run_id = :run_id
            AND work.tenant_id = cursor.tenant_id
      )
    ORDER BY cursor.tenant_id
    LIMIT :batch_size
    FOR UPDATE SKIP LOCKED
)
UPDATE tenant_iceberg_cursor AS cursor
SET
    snapshot_id = :end_snapshot_id,
    last_success_at = now(),
    last_extraction_run_id = :run_id,
    updated_at = now()
FROM candidates
WHERE cursor.source_table_uuid = candidates.source_table_uuid
  AND cursor.tenant_id = candidates.tenant_id
  AND cursor.snapshot_id = :start_snapshot_id
RETURNING cursor.tenant_id;
```

Recommended initial batch size:

```text
50,000 to 200,000 rows per transaction
```

Benchmark under actual:

- synchronous replication settings.
- WAL archiving.
- autovacuum configuration.
- storage class.
- replica count.
- connection pool.
- transaction timeout.

---

## 11. Tenant work leasing

### 11.1 Claim work

```sql
WITH selected AS (
    SELECT run_id, tenant_id
    FROM tenant_sync_work
    WHERE status IN ('READY', 'RETRYING')
      AND next_attempt_at <= now()
      AND (
          lease_expires_at IS NULL
          OR lease_expires_at < now()
      )
    ORDER BY next_attempt_at, tenant_id
    FOR UPDATE SKIP LOCKED
    LIMIT :limit
)
UPDATE tenant_sync_work AS work
SET
    status = 'RUNNING',
    lease_owner = :worker_id,
    lease_token = work.lease_token + 1,
    lease_expires_at = now() + interval '15 minutes',
    attempt_count = work.attempt_count + 1,
    updated_at = now()
FROM selected
WHERE work.run_id = selected.run_id
  AND work.tenant_id = selected.tenant_id
RETURNING work.*;
```

### 11.2 Lease heartbeat

Long-running tenants should extend their lease periodically:

```sql
UPDATE tenant_sync_work
SET
    lease_expires_at = now() + interval '15 minutes',
    updated_at = now()
WHERE run_id = :run_id
  AND tenant_id = :tenant_id
  AND status = 'RUNNING'
  AND lease_owner = :worker_id
  AND lease_token = :lease_token;
```

Require one affected row. Zero means the worker has lost ownership and must not commit control-plane state.

### 11.3 Fencing limitation

A PostgreSQL fencing token cannot prevent an already-running process from writing to S3 by itself.

The primary defense is:

- short bounded work.
- lease heartbeat.
- one worker claim per tenant.
- replay-safe complete-state reconciliation.
- compare-and-set cursor updates.

Even if a stale worker completes a Lance commit, it cannot advance the cursor after losing the lease. A subsequent reconciliation corrects the dataset.

---

## 12. Lance dataset application

### 12.1 Lance API assumptions

The examples target the Lance Python API corresponding to the 8.x generation and must be compiled and integration-tested against the exact pinned `pylance==8.0.0` build.

Relevant APIs include:

```python
lance.dataset(uri)
lance.write_dataset(...)
dataset.merge_insert(key)
builder.when_matched_update_all()
builder.when_not_matched_insert_all()
builder.when_not_matched_by_source_delete(filter_expression)
builder.execute(pyarrow_table)
```

Do not treat documentation examples as a substitute for an integration test against the pinned package.

### 12.2 Canonical Arrow schema

All tenant datasets must use one controlled canonical schema.

```python
import pyarrow as pa


CANONICAL_SCHEMA = pa.schema([
    pa.field("record_id", pa.string(), nullable=False),
    pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=True),
    pa.field("payload", pa.string(), nullable=True),
    # Add the real source fields here.
])
```

Do not include `tenant_id` in the physical tenant dataset unless query or audit requirements justify the duplication.

### 12.3 Normalize and validate an artifact

```python
import pyarrow as pa
import pyarrow.compute as pc


def validate_tenant_table(table: pa.Table) -> pa.Table:
    if table.schema != CANONICAL_SCHEMA:
        # Replace this with controlled additive-schema evolution if supported.
        raise ValueError(
            f"Schema mismatch: expected={CANONICAL_SCHEMA}, "
            f"actual={table.schema}"
        )

    if table["record_id"].null_count:
        raise ValueError("record_id contains null values")

    distinct = pc.count_distinct(table["record_id"]).as_py()
    if distinct != table.num_rows:
        raise ValueError("record_id is not unique")

    return table
```

### 12.4 Existing dataset reconciliation

The desired operation is one merge-insert commit that:

- updates matched rows.
- inserts unmatched source rows.
- deletes target rows absent from the complete source artifact.

Official documentation demonstrates filtered `when_not_matched_by_source_delete`. For a tenant-wide replacement, verify the exact 8.0.0 binding signature in tests.

Illustrative code:

```python
import lance
import pyarrow as pa


def reconcile_existing_dataset(
    uri: str,
    desired_state: pa.Table,
) -> int:
    desired_state = validate_tenant_table(desired_state)

    dataset = lance.dataset(uri)
    version_before = int(dataset.version)

    builder = (
        dataset
        .merge_insert("record_id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
    )

    # Confirm the exact pylance 8.0.0 method signature.
    #
    # The intent is: delete all target rows that are not present in
    # the complete source artifact.
    builder = builder.when_not_matched_by_source_delete("true")

    builder.execute(desired_state)

    reopened = lance.dataset(uri)
    version_after = int(reopened.version)

    if version_after <= version_before:
        raise RuntimeError(
            f"Lance version did not advance: "
            f"before={version_before}, after={version_after}"
        )

    return version_after
```

### 12.5 Dataset creation

Creation must distinguish a genuine “not found” from permissions, network, credential, and corruption errors.

Illustrative code:

```python
def create_tenant_dataset(
    uri: str,
    desired_state: pa.Table,
) -> int:
    desired_state = validate_tenant_table(desired_state)

    dataset = lance.write_dataset(
        desired_state,
        uri,
        mode="create",
    )

    return int(dataset.version)
```

If two workers race:

1. One create succeeds.
2. The other receives an already-exists/conflict response.
3. The losing worker reopens the dataset.
4. It performs normal reconciliation.

Do not catch every exception and assume the dataset is missing.

### 12.6 Empty tenant state

An empty artifact means the tenant has no source rows at the frozen snapshot.

The operation must still produce an empty Lance dataset state.

Options:

1. Use merge-insert with a canonical empty Arrow table and tenant-wide `not matched by source delete`.
2. Overwrite the dataset with an empty canonical-schema table if overwrite semantics are acceptable and tested.
3. For a never-created tenant with no rows, create an empty canonical dataset or use a documented “empty dataset” convention.

This path requires explicit integration tests.

### 12.7 Complete applicator function

```python
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class TenantWork:
    run_id: str
    source_table_uuid: str
    tenant_id: str
    expected_snapshot_id: int
    target_snapshot_id: int
    lance_uri: str
    artifact_uri: str
    artifact_checksum: str
    lease_owner: str
    lease_token: int


class PermanentTenantError(RuntimeError):
    pass


class RetryableTenantError(RuntimeError):
    pass


def apply_tenant_state(
    work: TenantWork,
    desired_state: pa.Table,
    dataset_exists: bool,
) -> tuple[Optional[int], int]:
    desired_state = validate_tenant_table(desired_state)

    if dataset_exists:
        ds = lance.dataset(work.lance_uri)
        before = int(ds.version)
        after = reconcile_existing_dataset(
            work.lance_uri,
            desired_state,
        )
        return before, after

    after = create_tenant_dataset(
        work.lance_uri,
        desired_state,
    )
    return None, after
```

The real implementation must classify concrete Lance and object-store exceptions into:

- retryable conflict.
- retryable network/S3 failure.
- authentication/authorization failure.
- dataset not found.
- dataset already exists.
- corrupt manifest.
- incompatible schema.
- permanent source-data error.

---

## 13. Retry policy

### 13.1 Retryable cases

Typically retry:

- S3 timeout.
- transient DNS/network errors.
- S3 throttling.
- Lance optimistic-concurrency conflict.
- PostgreSQL failover.
- temporary credential refresh failure.
- process interruption.
- unknown completion state.

### 13.2 Permanent or quarantined cases

Do not endlessly retry:

- duplicate primary keys in source state.
- incompatible schema.
- corrupt artifact checksum.
- source table UUID mismatch.
- non-ancestor snapshot cursor.
- expired source history without an artifact.
- persistent permission denial.
- corrupted Lance dataset requiring rebuild.
- invalid tenant URI or tenant identity.

### 13.3 Exponential backoff with jitter

```python
import random
import time
from collections.abc import Callable
from typing import TypeVar


T = TypeVar("T")


def retry(
    operation: Callable[[], T],
    *,
    attempts: int = 6,
    base_seconds: float = 1.0,
    maximum_seconds: float = 60.0,
) -> T:
    last_error: Exception | None = None

    for attempt in range(attempts):
        try:
            return operation()
        except PermanentTenantError:
            raise
        except Exception as error:
            last_error = error

            if attempt + 1 == attempts:
                break

            delay = min(
                maximum_seconds,
                base_seconds * (2 ** attempt),
            )
            delay *= random.uniform(0.75, 1.25)
            time.sleep(delay)

    raise RetryableTenantError(
        f"Operation failed after {attempts} attempts"
    ) from last_error
```

Use bounded worker concurrency to avoid creating a retry storm during a regional or credential outage.

---

## 14. Successful cursor commit

After Lance succeeds, update work state, cursor, and audit history in one PostgreSQL transaction.

```sql
BEGIN;

UPDATE tenant_sync_work
SET
    status = 'SUCCEEDED',
    lance_version_before = :lance_version_before,
    lance_version_after = :lance_version_after,
    lease_owner = NULL,
    lease_expires_at = NULL,
    completed_at = now(),
    updated_at = now()
WHERE run_id = :run_id
  AND tenant_id = :tenant_id
  AND status = 'RUNNING'
  AND lease_owner = :worker_id
  AND lease_token = :lease_token;

-- Require exactly one affected row.

WITH updated AS (
    UPDATE tenant_iceberg_cursor
    SET
        snapshot_id = :target_snapshot_id,
        lance_version = :lance_version_after,
        schema_version = :schema_version,
        schema_fingerprint = :schema_fingerprint,
        status = 'ACTIVE',
        consecutive_failures = 0,
        next_retry_at = NULL,
        last_success_at = now(),
        last_error_code = NULL,
        last_error = NULL,
        last_extraction_run_id = :run_id,
        updated_at = now()
    WHERE source_table_uuid = :source_table_uuid
      AND tenant_id = :tenant_id
      AND snapshot_id = :expected_snapshot_id
    RETURNING
        source_table_uuid,
        tenant_id,
        snapshot_id
)
INSERT INTO tenant_cursor_history (
    source_table_uuid,
    tenant_id,
    previous_snapshot_id,
    new_snapshot_id,
    run_id,
    lance_version
)
SELECT
    source_table_uuid,
    tenant_id,
    :expected_snapshot_id,
    snapshot_id,
    :run_id,
    :lance_version_after
FROM updated;

-- Require exactly one inserted history row.

COMMIT;
```

If the cursor compare-and-set affects zero rows:

- another worker may have advanced the tenant.
- the work item may be stale.
- the tenant may have been rebuilt.
- an operator may have paused or changed the cursor.

Do not overwrite current state blindly.

---

## 15. Failure handling matrix

| Failure | Detection | Result | Recovery |
|---|---|---|---|
| Spark fails before artifact publication | Run never reaches `EXTRACTED` | No cursor advancement | Re-run extraction |
| Spark writes partial bucket output | Missing run barrier/checksum | No cursor advancement | Delete/reuse run prefix safely and retry |
| Changelog encounters delete files | Extraction exception | Run fails | Enforce COW or use alternate extraction |
| Start snapshot expired | Lineage validation fails | Cohort cannot incrementally recover | Rebuild tenants or use retained artifact |
| Iceberg rollback | Start not ancestor of end | Incremental run blocked | Reconcile/rebuild affected tenants |
| Table recreated | UUID mismatch | Incremental run blocked | Full rebuild |
| Duplicate tenant key | Validation fails | Only affected tenant quarantined | Correct source or define deterministic rule |
| Worker crashes before Lance commit | Lease expires | Cursor unchanged | Reclaim and retry |
| Lance succeeds and worker times out | Ambiguous result | Cursor unchanged | Replay complete tenant state |
| Lance succeeds and PostgreSQL fails | Cursor unchanged | Dataset may already be correct | Replay and then CAS cursor |
| PostgreSQL cursor succeeds and response is lost | Cursor already advanced | Retry sees stale expected cursor | Treat as completed after verification |
| Lease expires during work | Heartbeat/CAS fails | Worker cannot update control state | New owner replays |
| Two Lance writers race | Conflict or multiple versions | One may retry | Enforce lease, reopen, and reconcile |
| Compaction races with sync | Lance conflict | Operation retries | Maintenance uses same tenant lease |
| S3 regional outage | Request failures | Work remains pending/retrying | Back off and fail over if designed |
| PostgreSQL outage | Claim/commit failure | No control-plane progress | Multi-AZ failover and replay |
| Artifact corrupted | Checksum mismatch | Work blocked | Re-extract artifact |
| Lance manifest corrupt | Open/read failure | Tenant isolated | Rebuild tenant dataset |
| Schema mismatch | Fingerprint/schema validation | Tenant isolated | Migrate or rebuild |
| Poison tenant | Repeated permanent error | Other tenants continue | Quarantine |
| One million-row cursor update fails | PostgreSQL rollback | Batch remains old | Retry bounded batch |
| Reader sees intermediate version | Avoided by one reconciliation commit | Consistent version | Confirm merge semantics in tests |

---

## 16. Lance concurrency and transaction policy

Lance uses versioned manifests and optimistic concurrency.

Production policy:

- Serialize synchronization, compaction, cleanup, and index maintenance per tenant through the same lease.
- Reopen the Lance dataset before every retry.
- Never retain a stale `LanceDataset` object across a long queue delay.
- Record `version_before` and `version_after`.
- Verify that a successful mutation created or selected the expected newer version.
- Avoid multiple commits per synchronization where possible.
- Treat unknown commit state as replayable, not as success.
- Keep source artifacts until cursor advancement and a safety period have passed.

---

## 17. Lance fragments, versions, and maintenance

It is acceptable for each tenant dataset to have multiple fragments and versions.

However, unbounded growth degrades metadata planning and mutation performance.

### 17.1 Maintenance signals

Track:

- fragment count.
- small-fragment count.
- deletion ratio.
- current Lance version.
- versions since compaction.
- total dataset bytes.
- read latency.
- merge latency.
- index state.
- last compaction time.
- last cleanup time.

### 17.2 Example thresholds

Initial thresholds for testing, not universal constants:

```text
compact when fragment_count > 64
or deleted_row_ratio > 0.15
or small fragments exceed 25% of fragments
```

Large tenants may need different thresholds.

### 17.3 Compaction

Illustrative API:

```python
import lance


def compact_tenant(uri: str) -> None:
    dataset = lance.dataset(uri)
    dataset.optimize.compact_files(
        target_rows_per_fragment=1_000_000
    )
```

Verify the exact pinned API and return type.

### 17.4 Old-version cleanup

Illustrative API:

```python
from datetime import timedelta


def cleanup_tenant(uri: str) -> None:
    dataset = lance.dataset(uri)
    dataset.cleanup_old_versions(
        older_than=timedelta(days=7),
        delete_unverified=False,
    )
```

Never clean versions needed for:

- incident rollback.
- active readers pinned to versions.
- in-flight operations.
- audit requirements.
- reconciliation investigation.

### 17.5 Scheduling

Do not scan one million S3 prefixes to discover maintenance candidates.

Use PostgreSQL `maintenance_due_at` and claim tenant maintenance tasks with the same lease protocol.

---

## 18. Schema evolution

### 18.1 Canonical schema registry

Maintain:

- integer schema version.
- Arrow schema.
- field IDs or stable logical identifiers.
- compatibility rules.
- migration procedure.
- schema fingerprint.

### 18.2 Compatibility policy

| Change | Default policy |
|---|---|
| Add nullable field | Supported after tested migration |
| Add required field | Reject without default/backfill |
| Rename field | Map through registry and do not infer only by name |
| Widen integer safely | Allow after validation |
| Narrow type | Reject |
| Change timestamp timezone/unit | Explicit migration |
| Remove field | Preserve or rebuild according to policy |
| Nested schema change | Explicit compatibility test |

### 18.3 Fingerprint

Compute a canonical schema fingerprint, for example:

```text
sha256(canonical Arrow schema serialization)
```

Store it in:

- extraction run.
- tenant artifact.
- tenant cursor.
- worker logs.

---

## 19. Reconciliation

Continuous reconciliation is mandatory.

### 19.1 What to compare

For a tenant at cursor snapshot `S`:

- Iceberg time-travel state at `S`.
- Current Lance dataset state associated with the cursor’s Lance version.

Compare:

- row count.
- distinct primary-key count.
- schema fingerprint.
- deterministic row checksum.
- selected column statistics.
- optionally full records for small tenants.

### 19.2 Canonical checksums

Canonicalization must define:

- column order.
- null encoding.
- floating-point normalization.
- timestamp timezone and unit.
- nested field ordering.
- string encoding.
- binary encoding.

Example conceptual row hash:

```text
SHA256(
  record_id
  || canonical(field_1)
  || canonical(field_2)
  || ...
)
```

Aggregate hashes in an order-independent way or sort by primary key.

### 19.3 Reconciliation schedule

Recommended starting policy:

- Every extraction run: structural and artifact checks.
- Daily: random tenant sample.
- Weekly: larger stratified sample.
- Monthly or quarterly: full population reconciliation, depending on cost.
- After any incident: targeted full reconciliation.
- New release canary: compare all canary tenants.

### 19.4 Mismatch handling

Set:

```text
status = RECONCILIATION_REQUIRED
```

Then:

1. Freeze a current or historical source snapshot.
2. Produce a complete tenant artifact.
3. Reconcile or recreate the Lance dataset.
4. Verify checksums.
5. Advance/reset the cursor through an audited repair operation.

---

## 20. Observability

### 20.1 Extraction metrics

- extraction run duration.
- snapshot count in range.
- changed key count.
- changed tenant count.
- artifact count.
- bytes read from Iceberg.
- bytes written to artifact S3.
- manifest planning time.
- duplicate-key tenant count.
- extraction failure count.
- current dominant-cursor lag.
- oldest tenant-cursor lag.

### 20.2 Worker metrics

- work claimed.
- work succeeded.
- retry count.
- permanent failure count.
- tenant application latency.
- artifact read latency.
- Lance open latency.
- Lance merge latency.
- Lance version delta.
- S3 throttling count.
- lease expiration count.
- stale-worker CAS failure.
- schema mismatch count.
- checksum failure count.

### 20.3 PostgreSQL metrics

- cursor update rows/second.
- transaction duration.
- WAL bytes.
- replica lag.
- dead tuples.
- autovacuum duration.
- index size.
- lock wait time.
- work-queue depth.
- oldest retry age.

### 20.4 Alerts

Alert on:

- extraction run stuck beyond SLA.
- no successful extraction in two schedule intervals.
- oldest tenant lag near retention boundary.
- failed run publication barrier.
- UUID mismatch.
- non-ancestor cursor.
- unsupported Iceberg delete-file snapshot.
- retry queue growth.
- elevated S3 throttling.
- PostgreSQL replica lag.
- worker lease churn.
- reconciliation mismatch.
- compaction backlog.
- artifact checksum mismatch.
- permanent failures above threshold.

---

## 21. Security and IAM

### 21.1 Separation of privileges

Use separate roles for:

- Spark source reader.
- artifact writer.
- Lance applicator.
- maintenance worker.
- reconciliation reader.
- PostgreSQL control-plane access.

### 21.2 Least privilege

Examples:

- Extractor can read Iceberg source and write only its run artifact prefix.
- Lance worker can read artifact prefixes and mutate tenant Lance prefixes.
- Maintenance worker can mutate Lance metadata and delete eligible old objects.
- Reconciliation service can read source and Lance but does not necessarily mutate.
- Database users receive only required table operations.

### 21.3 Encryption

Use:

- S3 server-side encryption.
- TLS for PostgreSQL and object storage access.
- KMS policies scoped to workloads.
- secrets manager or workload identity.
- short-lived credentials.

### 21.4 Tenant identifiers in paths

Hash and URL-encode tenant identifiers.

Avoid exposing sensitive raw identifiers in logs and metrics. Consider storing only a safe opaque tenant key in S3 paths.

---

## 22. Capacity and scaling

### 22.1 One million cursors

One million PostgreSQL rows is not inherently problematic.

The important costs are:

- hourly MVCC row versions.
- WAL volume.
- index maintenance on `snapshot_id`.
- autovacuum.
- replication lag.
- burst transaction size.

Use set-based updates and benchmark.

### 22.2 Worker parallelism

One dataset per tenant enables broad parallelism across tenants.

Concurrency must be bounded by:

- S3 request rate and throttling.
- worker CPU/memory.
- PostgreSQL queue throughput.
- artifact read bandwidth.
- typical Lance dataset size.
- merge latency.
- credential/session limits.

Start conservatively and use adaptive concurrency.

### 22.3 Heavy tenants

Classify tenants by expected state size:

```text
small
medium
large
very large
```

Use separate queues or worker pools so very large tenants do not occupy all general workers.

### 22.4 Changed-tenant count

Do not create work rows for one million tenants if only a small percentage changed.

Create tenant work only for changed tenants. Advance unchanged tenants in bulk.

---

## 23. Run lifecycle

### 23.1 Normal dominant-cohort run

1. Acquire extractor singleton/lease.
2. Read Iceberg table UUID.
3. Query dominant tenant cursor cohort.
4. Freeze current main snapshot as end.
5. Validate start and end lineage.
6. Insert deterministic extraction run.
7. Create changelog view for `(start, end]`.
8. Discover changed tenant IDs and keys.
9. Read full affected-tenant state at end snapshot.
10. Validate schema and key uniqueness.
11. Publish immutable tenant artifacts.
12. Insert tenant work rows.
13. Publish verified run manifest.
14. Transition run to `EXTRACTED`.
15. Bulk-advance unchanged tenants in bounded transactions.
16. Transition run to `APPLYING`.
17. Workers reconcile changed tenants independently.
18. When all work is terminal, mark run `COMPLETED` or completed-with-failures according to policy.
19. Retain artifacts through the configured replay window.
20. Release extractor lease.

### 23.2 Retry-cohort run

1. Select retryable tenants by cursor.
2. Freeze a target end snapshot, usually current head or a bounded intermediate snapshot.
3. Group by cursor or lineage window.
4. Extract and process without delaying the dominant cohort.
5. On success, tenants may rejoin the dominant cohort naturally.

---

## 24. Run completion semantics

A run may be considered:

### `EXTRACTED`

- All source discovery completed.
- Artifacts are durable and verified.
- Work rows exist.
- Unchanged tenants may advance.

### `COMPLETED`

- Every changed-tenant work row is `SUCCEEDED`, `PERMANENT_FAILURE`, or explicitly cancelled according to policy.
- Operationally, a run with permanent failures should be clearly distinguished from a completely successful run.

Recommended additional statuses:

```text
COMPLETED_SUCCESS
COMPLETED_WITH_FAILURES
```

Do not leave run meaning ambiguous.

---

## 25. Disaster recovery

### 25.1 PostgreSQL

Require:

- Multi-AZ deployment.
- point-in-time recovery.
- WAL archiving.
- tested restore procedures.
- backup monitoring.
- schema migration rollback strategy.

### 25.2 S3 artifacts

Configure lifecycle rules that retain artifacts longer than:

```text
maximum worker retry window
+ incident response window
+ safety margin
```

Consider cross-region replication if regional recovery is required.

### 25.3 Lance datasets

Lance datasets can be rebuilt from Iceberg if the required snapshot remains available.

If the historical cursor snapshot has expired:

- rebuild from a newer/current source snapshot.
- update cursor through an audited repair.
- accept that historical point-in-time parity cannot be reconstructed unless preserved elsewhere.

### 25.4 Runbooks

Maintain tested runbooks for:

- rebuild one tenant.
- rebuild a cursor cohort.
- recover after Iceberg rollback.
- recover after table recreation.
- repair a corrupt artifact.
- repair a corrupt Lance dataset.
- pause/resume a tenant.
- force reconciliation.
- rotate credentials.
- handle prolonged S3 outage.
- restore PostgreSQL control state.

---

## 26. Testing strategy

### 26.1 Unit tests

- URI derivation.
- snapshot lineage construction.
- cohort selection.
- schema fingerprint.
- checksum generation.
- duplicate-key detection.
- retry classification.
- CAS update behavior.
- artifact identity determinism.
- lease token behavior.

### 26.2 Integration tests

Run against:

- real Iceberg Spark runtime.
- pinned `pylance==8.0.0`.
- S3 or a behaviorally representative environment.
- PostgreSQL with the production schema.

Test:

- insert-only range.
- update range.
- delete range.
- delete and reinsert.
- multiple updates in one Iceberg snapshot.
- tenant becomes empty.
- new tenant dataset.
- existing tenant reconciliation.
- schema addition.
- schema incompatibility.
- Lance conflict.
- S3 throttling.
- PostgreSQL failover.
- lease expiry.
- worker crash after Lance commit.
- worker crash before PostgreSQL commit.
- duplicate artifact application.
- extraction driver loss.
- rollback/non-ancestor cursor.
- expired snapshot.
- table UUID change.
- compaction race.
- cleanup safety.

### 26.3 Fault injection

Deliberately terminate processes at every state transition:

```text
before artifact upload
after artifact upload
before run publication
after run publication
before Lance commit
after Lance commit
before cursor update
after cursor update
during lease heartbeat
during compaction
```

### 26.4 Load testing

Model:

- one million cursor rows.
- hourly updates.
- dominant cohort near one million.
- realistic changed-tenant distribution.
- worst-case burst after outage.
- heavy-tenant tail.
- S3 latency and throttling.
- PostgreSQL synchronous replication.
- production WAL retention.
- autovacuum.

---

## 27. Deployment gates

Production deployment is blocked until all are true.

- [ ] Source table is configured and verified for copy-on-write changes.
- [ ] Recovery-window snapshots contain no unsupported delete manifests.
- [ ] Iceberg table UUID is available and persisted.
- [ ] Snapshot ancestry validation is tested.
- [ ] `pylance==8.0.0` is pinned.
- [ ] Exact `when_not_matched_by_source_delete` semantics are integration-tested.
- [ ] Empty tenant reconciliation is tested.
- [ ] Dataset creation conflict is tested.
- [ ] One mutation per tenant produces the expected atomic visible state.
- [ ] Lance-success/PostgreSQL-failure replay is tested.
- [ ] Stale lease owner cannot advance the cursor.
- [ ] Artifact checksums are validated.
- [ ] Duplicate keys quarantine only the affected tenant.
- [ ] Cursor bulk updates are benchmarked with production PostgreSQL settings.
- [ ] Snapshot retention exceeds tested worst-case lag.
- [ ] Lance compaction and cleanup use the tenant lease.
- [ ] Reconciliation detects injected drift.
- [ ] Tenant rebuild is automated and tested.
- [ ] Metrics, dashboards, and alerts are deployed.
- [ ] Incident runbooks are reviewed.
- [ ] Canary rollout completes without reconciliation mismatches.

---

## 28. Decisions and trade-offs

### Decision: one Lance dataset per tenant

**Benefits**

- Strong failure isolation.
- Independent versions and fragments.
- Easy tenant rebuild.
- Parallelism across tenants.
- Tenant-specific lifecycle and maintenance.

**Costs**

- One million dataset prefixes/manifests.
- Need deterministic direct addressing.
- Maintenance scheduling cannot rely on listing.
- Many small datasets may create S3 request overhead.
- Operational metadata must live in PostgreSQL.

### Decision: explicit cursor per tenant

**Benefits**

- Precise independent recovery.
- Simple audit and lag reporting.
- Failed tenants stay behind without blocking others.

**Costs**

- Bulk PostgreSQL update churn.
- WAL and autovacuum requirements.
- Cohort index write amplification.

### Decision: complete tenant reconciliation

**Benefits**

- Avoids dependence on intra-snapshot changelog ordering.
- One Lance commit can reconcile insert/update/delete state.
- Retries are deterministic.
- Handles unknown previous partial attempts.

**Costs**

- Reads complete state for changed tenants.
- Large tenants require more extraction bandwidth.
- Artifact size can be larger than a delta.

### Decision: immutable S3 artifacts

**Benefits**

- Driver-independent recovery.
- Replay without rescanning Iceberg.
- Auditable checksums.
- Tenant workers decoupled from Spark lifetime.

**Costs**

- Additional S3 writes and lifecycle management.
- Artifact publication logic.
- Temporary storage.

---

## 29. Alternatives rejected

### One Iceberg scan per tenant

Rejected because:

- one million independent scans are operationally infeasible.
- repeated manifest and file planning.
- lagging tenants require long source retention.
- poor cost and latency characteristics.

### One global Lance dataset for all tenants

Rejected by requirement and because:

- wider failure and contention domain.
- shared manifest/version history.
- maintenance coupling.
- tenant isolation is weaker.

### Direct Spark writes into Lance datasets without durable artifacts

Rejected because:

- Spark task retries can duplicate side effects.
- driver loss destroys in-memory progress.
- long-lived external writes do not map cleanly to Spark partition semantics.
- independent tenant retry is harder.

### Changelog-order-based final-state reconstruction

Rejected because:

- `_change_ordinal` orders snapshot change groups, not original row events inside a snapshot.
- original CDC order may have been collapsed during Iceberg ingestion.
- complete end-state extraction is safer.

### Separate Lance delete then upsert

Rejected for the final design because:

- two visible Lance versions.
- worker crash can expose an intermediate tenant state.
- replay repairs eventually but readers may observe partial state.

---

## 30. Reference implementation structure

```text
iceberg_lance_sync/
├── pyproject.toml
├── README.md
├── config/
│   ├── schema.yaml
│   └── environments/
├── migrations/
│   └── postgres/
├── src/
│   └── iceberg_lance_sync/
│       ├── config.py
│       ├── models.py
│       ├── iceberg/
│       │   ├── metadata.py
│       │   ├── lineage.py
│       │   ├── changelog.py
│       │   └── extraction.py
│       ├── artifacts/
│       │   ├── writer.py
│       │   ├── reader.py
│       │   ├── checksum.py
│       │   └── manifest.py
│       ├── postgres/
│       │   ├── cursors.py
│       │   ├── runs.py
│       │   ├── work_queue.py
│       │   └── leases.py
│       ├── lance_target/
│       │   ├── paths.py
│       │   ├── schema.py
│       │   ├── apply.py
│       │   ├── errors.py
│       │   └── maintenance.py
│       ├── workers/
│       │   ├── extractor.py
│       │   ├── applicator.py
│       │   ├── reconciliation.py
│       │   └── maintenance.py
│       ├── observability/
│       │   ├── metrics.py
│       │   └── logging.py
│       └── cli.py
└── tests/
    ├── unit/
    ├── integration/
    ├── fault_injection/
    └── load/
```

---

## 31. Recommended initial configuration

These are starting points requiring benchmarks.

```yaml
schedule:
  dominant_cohort: hourly
  retry_cohorts: every_15_minutes
  reconciliation_sample: daily
  maintenance_scan: hourly

extractor:
  staging_buckets: 4096
  maximum_snapshots_per_run: 2000
  artifact_format: arrow_ipc
  immutable_artifacts: true

postgres:
  unchanged_cursor_update_batch: 100000
  work_claim_batch: 100
  lease_duration_minutes: 15

workers:
  initial_concurrency: 100
  maximum_attempts: 6
  retry_base_seconds: 1
  retry_max_seconds: 3600

lance:
  package: "pylance==8.0.0"
  compact_fragment_threshold: 64
  compact_deleted_ratio: 0.15
  old_version_retention_days: 7

iceberg:
  required_write_mode: copy-on-write
  snapshot_retention_days: 8
```

Do not adopt these numbers without load testing.

---

## 32. Sources and implementation references

1. Apache Iceberg Spark procedures, including `create_changelog_view`:
   https://iceberg.apache.org/docs/latest/spark-procedures/

2. Apache Iceberg incremental changelog implementation and delete-file limitation:
   https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/BaseIncrementalChangelogScan.java

3. Lance read/write and merge-insert documentation:
   https://lance.org/guide/read_and_write/

4. Lance transaction and conflict-resolution specification:
   https://lance.org/format/table/transaction/

5. Lance table-format overview:
   https://lance.org/format/table/

6. Lance performance guidance:
   https://lance.org/guide/performance/

7. PostgreSQL `SELECT`, row locking, and `SKIP LOCKED`:
   https://www.postgresql.org/docs/current/sql-select.html

8. PostgreSQL `UPDATE`:
   https://www.postgresql.org/docs/current/sql-update.html

9. PostgreSQL `COPY`:
   https://www.postgresql.org/docs/current/sql-copy.html

10. PostgreSQL routine vacuuming:
    https://www.postgresql.org/docs/current/routine-vacuuming.html

---

## 33. Final acceptance statement

The project is production-ready only after the deployment gates and failure-injection tests pass.

The final operating model is:

```text
shared Iceberg cohort scan
→ changed-tenant discovery
→ authoritative state read at frozen snapshot
→ immutable tenant artifact
→ one tenant-isolated Lance reconciliation
→ PostgreSQL cursor advancement
→ replay on uncertainty
→ periodic reconciliation
```

This design minimizes shared failure domains while preserving one explicit Iceberg snapshot cursor and one independently versioned Lance dataset per tenant.
