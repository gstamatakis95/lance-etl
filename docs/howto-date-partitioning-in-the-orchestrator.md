# How-to: date and time lifecycle at the orchestrator layer

*This document predates the unified architecture (ADR 0028) and needs a refresh.*

This document is for teams that previously relied on by-date physical partitioning (one Lance dataset
per calendar day) and need to understand how to achieve equivalent data-lifecycle goals now that the
ETL is date-agnostic.

---

## Why by-date partitioning was removed from the ETL

By-date partitioning was introduced in ADR-0004 (generic `partition_cols` routing plus
`partition_derivations` to derive an `event_date` column from a timestamp). ADR-0006 added the
serving-side counterpart: a `DateRange` field in the gRPC `DatasetTarget` message that fanned a
single request out across N per-day datasets and merged the results.

ADR-0014 removed both sides. The reasons are summarised below.

**Write-side problems:**

- `partition_derivations` required translating strftime directives into Spark `date_format` dialect.
  Unsupported directives silently produced wrong partition values.
- The `lance_etl_partition_derive` and `lance_etl_window_column` Airflow Variables were implicit
  contracts between the writer and the server. A mismatch caused silent mis-routing.

**Serving-side problems:**

- A query spanning N days opened N dataset handles, ran N full queries, and executed an O(N x k)
  dedup merge pass in `domain/merge.rs` before truncation to k. Overhead grew linearly with the
  date range regardless of how many results were actually returned.
- The `DateRange` proto field carried YYYY-MM-DD strings that the server had to parse and validate,
  with per-day 404 semantics that were asymmetric from all other errors.
- The `Prewarm` and `Clusters` RPCs only accepted a single-day target, creating an asymmetry with
  the multi-day `Search` RPCs.

**What replaces it:**

Every deployment already applied scalar timestamp filters at query time. The typed `Filter` AST in
ADR-0005 (`filter_to_expr` in `src/lance/filter.rs`) pushes those predicates down to the Lance
scanner. A BTREE scalar index on the event-timestamp column makes range filters efficient without
any physical partitioning by date. Each search target resolves to exactly one dataset. `domain/merge.rs`
is deleted. The fan-out env knobs (`SEARCH_API_FANOUT_CONCURRENCY`, `SEARCH_API_ID_COLUMN`) are gone.

For the full decision record see `docs/adr/0014-drop-by-date-partitioning.md`.

---

## Option A (recommended, default): one dataset per routing key, event-time range filters at query time

This is the simplest path. No orchestrator changes are needed beyond the default DAG.

### How data is laid out

The ETL writes one Lance dataset per distinct combination of `partition_cols` values (default
`org_id`, `tenant_id`, `namespace`):

```
s3://my-bucket/lance/<org_id>/<tenant_id>/<namespace>.lance
```

All events for that routing key, across all time, land in the same dataset. Each scheduled run
ingests one bounded window of Iceberg changes into that dataset via an idempotent keyed
`merge_insert`. The event timestamp of each row is stored as a regular column (default column name:
`timestamp`, configured via `ETLConfig.ts_col`).

### How date-range queries work

Clients express a time range as a scalar `Filter` on the event-timestamp column. The gRPC
`Filter` AST (defined in `search.proto`) accepts a `Between` predicate or a pair of `Comparison`
predicates combined with `And`. Column names are validated against the dataset schema. No raw SQL
strings are accepted anywhere in the gRPC or domain layers.

Conceptual filter for "events between 2026-01-01 and 2026-02-01":

```proto
filter {
  and {
    filters {
      comparison {
        column: "timestamp"
        op: COMPARE_OP_GE
        value { int64_value: 1735689600 }  # 2026-01-01T00:00:00Z as epoch seconds
      }
    }
    filters {
      comparison {
        column: "timestamp"
        op: COMPARE_OP_LT
        value { int64_value: 1738368000 }  # 2026-02-01T00:00:00Z as epoch seconds
      }
    }
  }
}
```

Alternatively, use the `Between` predicate:

```proto
filter {
  between {
    column: "timestamp"
    low  { int64_value: 1735689600 }
    high { int64_value: 1738367999 }
  }
}
```

### What makes the filter efficient

The `indexing` step in the default DAG builds a BTREE scalar index on the event-timestamp column
(configured via the `lance_etl_index_flags` Airflow Variable, for example
`--scalar-column timestamp`). The BTREE index makes open-ended and bounded range predicates
efficient: the Lance scanner prunes fragments before reading any row data. No cross-dataset fan-out
or merge pass is required.

### Orchestrator setup for Option A

The default DAG in `airflow/lance_etl_dag.py` already implements this path:

- `lance_etl_schedule` controls how frequently the pipeline runs (default: `@daily`).
- Each scheduled run reads the data-interval window from the Iceberg source and upserts it into
  the appropriate per-routing-key datasets.
- No `lance_etl_partition_by` override is needed.
- No date component is added to the namespace or any other routing column.

**No additional Airflow Variables or DAG changes are required for Option A.**

---

## Option B: physically separate per-date datasets via orchestrator-injected routing

Choose this option only when the team genuinely needs to drop a whole day cheaply (deleting one
dataset path is cheaper than deleting rows from a large dataset), or when hard tenant-per-day
isolation is a compliance requirement.

Note that Option B reintroduces multi-dataset serving. The system deliberately dropped that
capability in ADR-0014 because the fan-out complexity was unsustainable. Choosing Option B means
owning the fan-out at the application layer (or living without cross-day search).

### How it works

Instead of adding a date-derived column inside the ETL (which ADR-0014 removed), the orchestrator
stamps a date literal into the `namespace` (or adds a new trailing partition column) before each
scheduled run. The ETL receives a different routing-key value for each day and writes to a
different dataset path.

Two sub-approaches are available:

**Sub-approach B1: date in `namespace`**

The orchestrator templates the date into the `namespace` value. The resulting path is:

```
s3://my-bucket/lance/<org_id>/<tenant_id>/<namespace>-2026-01-15.lance
```

This works when the application already uses `namespace` as a logical grouping that can absorb a
date suffix.

**Sub-approach B2: date as an extra trailing partition column**

The orchestrator extends `partition_cols` with a literal date column and sets the `namespace` (or
a new `event_date` column) to the run date. The resulting path is:

```
s3://my-bucket/lance/<org_id>/<tenant_id>/<namespace>/2026-01-15.lance
```

This is cleaner when `namespace` must remain stable for other consumers.

### Concrete Airflow example (Sub-approach B2: extra date partition column)

The key idea: each daily DAG run templates the `data_interval_start` date into the ETL window
arguments AND into the `lance_etl_partition_by` Variable (or into a `dag_run.conf` override) so
each run writes to its own date-scoped dataset.

Create a separate daily DAG for per-date ingestion. Save it alongside `lance_etl_dag.py` in
`airflow/`:

```python
"""Per-date Lance ETL DAG: one dataset per day per routing key.

Each daily run writes to a date-scoped dataset path so individual days can be
dropped cheaply by deleting the dataset at that path.
"""

from __future__ import annotations

import shlex
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DAG_ID = "lance_etl_per_date"
LANCE_ETL_CLI = "/opt/lance-etl/src/lance_etl/cli.py"

dag_params: dict[str, str | int] = {
    "iceberg_table": "prod.vectors.events",
    "lance_base_uri": "s3://my-bucket/lance-by-date",
    "datasets_file": "/opt/lance/datasets-by-date.txt",
    "dd_service": "lance-pipeline",
    "dd_env": "prod",
    "dd_tags": "",
    "executor_instances": 8,
    "executor_memory": "8g",
    "driver_memory": "4g",
    "spark_conf_overrides": "{}",
}

default_args: dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def build_per_date_etl_args(params: dict[str, str | int]) -> list[str]:
    """Build ETL CLI args that inject the run date as the final partition column.

    The Jinja template ``{{ data_interval_start.strftime('%Y-%m-%d') }}``
    evaluates to the date string for the slot being processed. That date string
    is passed both as the ``--window-start`` / ``--window-end`` bounds and as
    the ``namespace`` column value via ``--partition-by``.

    Args:
        params: DAG-run params dict.

    Returns:
        Argument list starting with the ``etl`` subcommand token.
    """
    return [
        "etl",
        "--table",
        str(params["iceberg_table"]),
        "--start",
        "{{ data_interval_start | string }}",
        "--end",
        "{{ data_interval_end | string }}",
        "--base-uri",
        str(params["lance_base_uri"]),
        "--dd-service",
        str(params["dd_service"]),
        "--dd-env",
        str(params["dd_env"]),
        "--window-start",
        "{{ data_interval_start | string }}",
        "--window-end",
        "{{ data_interval_end | string }}",
        "--partition-by",
        "org_id,tenant_id,namespace,event_date",
    ]


with DAG(
    dag_id=DAG_ID,
    description="Per-date Iceberg to Lance ETL: one dataset per day per routing key",
    schedule="@daily",
    start_date=datetime(2026, 6, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params=dag_params,
    tags=["lance", "etl", "vector-db", "per-date"],
) as dag:
    etl_task = SparkSubmitOperator(
        task_id="etl",
        conn_id=Variable.get("lance_etl_spark_conn_id", default_var="spark_default"),
        application=LANCE_ETL_CLI,
        application_args=build_per_date_etl_args(dag_params),
        name="lance-etl-per-date-etl",
        conf={
            "spark.executor.instances": str(dag_params["executor_instances"]),
            "spark.executor.memory": str(dag_params["executor_memory"]),
            "spark.driver.memory": str(dag_params["driver_memory"]),
        },
        py_files="",
        verbose=False,
        do_xcom_push=False,
        env_vars={"PYTHONPATH": "/opt/lance-etl/src"},
        spark_binary="spark-submit",
        driver_class_path="",
        jars="",
        packages="",
        exclude_packages="",
        keytab="",
        principal="",
        proxy_user="",
    )
```

**Important:** the Iceberg source must carry an `event_date` column, or the ETL will fail schema
validation. One way to produce it is to add a derived date column upstream in the source pipeline.
The ETL itself no longer derives columns from timestamps, so the column must exist in the source.

### Resulting dataset paths for Option B

With `--partition-by org_id,tenant_id,namespace,event_date` and the slot for 2026-01-15:

```
s3://my-bucket/lance-by-date/acme/prod/search/2026-01-15.lance
s3://my-bucket/lance-by-date/acme/prod/search/2026-01-16.lance
s3://my-bucket/lance-by-date/acme/staging/search/2026-01-15.lance
...
```

Each dataset is an independent Lance dataset. The serving layer must be configured to target the
correct date-scoped URI when serving queries for a specific day.

---

## Backfill: replaying historical windows

The ETL's `merge_insert` is idempotent and keyed by `vector_id` (or whatever `key_col` is
configured to). Replaying a window that was already processed updates rows in place rather than
duplicating them. This makes backfill straightforward.

### Airflow native backfill

Set `catchup=False` in the DAG definition (as the default DAG does) to prevent automatic catch-up
on deploy. When an explicit backfill is needed, use the Airflow CLI:

```bash
airflow dags backfill lance_etl_pipeline \
    --start-date 2026-01-01 \
    --end-date 2026-02-01
```

Airflow submits one DAG run per interval slot. Each run reads the bounded Iceberg snapshot window
for that slot and upserts into the target datasets. Because each window is idempotent, retrying a
failed slot is safe. Rows that already exist are updated to their latest values rather than
duplicated.

### Parallelism during backfill

The default DAG sets `max_active_runs=1`. This serializes runs to prevent overlapping index
maintenance commits on the same dataset (the losing concurrent build is silently discarded, and
concurrent compaction commits on the same dataset also race). For backfills where higher throughput
is needed:

- Increase `max_active_runs` temporarily via a DAG code change.
- Ensure no two concurrent runs target the same dataset (possible when backfilling non-overlapping
  routing-key subsets in parallel DAGs).
- After the backfill, restore `max_active_runs=1` and run the index step manually over all
  affected datasets to rebuild any indices whose build was discarded.

### Manual window override

Any run can be triggered with an explicit window that overrides the Airflow data interval. Use the
Trigger DAG dialog or the REST API:

```json
{
    "start": "2026-01-01T00:00:00+00:00",
    "end":   "2026-01-08T00:00:00+00:00"
}
```

The `build_etl_application_args` function in `lance_etl_dag.py` resolves `dag_run.conf['start']`
and `dag_run.conf['end']` with highest precedence, falling back to the data interval. The Iceberg
snapshot bounds continue to use the data interval for partition pruning, so the window filter is an
additional narrowing on top of the Iceberg read.

---

## Retention and cleanup at the orchestrator layer

### Option A: row-level retention

For Option A (one dataset per routing key), there is no cheap whole-dataset drop because all time
periods share one dataset. Row-level retention requires a TTL job that deletes rows whose
event-timestamp falls before the retention horizon. A reference TTL job design will be described in
a forthcoming ADR. In the meantime, a scheduled job can call `dataset.delete(predicate)` with a
predicate on the event-timestamp column.

### Option B: whole-dataset drop

For Option B (one dataset per day), dropping an old day is a cheap object-store operation. Delete
the entire dataset path for the date that has aged out:

```bash
# Drop the 2025-12-31 slice for one routing key
aws s3 rm --recursive \
    s3://my-bucket/lance-by-date/acme/prod/search/2025-12-31.lance/
```

Or in Python using `pyarrow.fs`:

```python
from pyarrow import fs as pa_fs

filesystem, root_path = pa_fs.FileSystem.from_uri(
    "s3://my-bucket/lance-by-date/acme/prod/search/2025-12-31.lance"
)
filesystem.delete_dir(root_path)
```

Automate this with an Airflow sensor or a separate retention DAG that computes the cutoff date and
deletes all dataset paths older than the retention window. The `discover_datasets` helper in
`src/lance_etl/cloud_storage.py` can enumerate all `*.lance` paths under a base URI for an
automated sweep.

---

## Tradeoffs: Option A vs Option B

| Dimension | Option A (one dataset per routing key) | Option B (one dataset per day per routing key) |
|---|---|---|
| Query simplicity | Single gRPC call. Date range expressed as a `Between` or `And` filter on the event-timestamp column. | Multi-call fan-out in the application layer. The serving API addresses exactly one dataset per request, so the caller must iterate over date URIs. |
| Drop-a-day cost | Expensive: requires row-level deletes over a potentially large dataset and a subsequent compaction run. | Cheap: delete one directory at the object-store level. |
| Number of datasets | Low: one dataset per routing key regardless of time span. | High: one dataset per routing key per day. Index builds, compaction runs, and cache warming all scale with dataset count. |
| Index maintenance | One BTREE index per dataset maintained incrementally by the default DAG. | One BTREE index per per-date dataset. Each day's dataset requires its own build and compaction cycle. The `datasets_file` passed to the `index` and `compact` steps must be regenerated each run. |
| Serving fan-out | None. One dataset per request. | Inherent. A query spanning D days requires D separate search calls and client-side result merging. The system removed server-side fan-out in ADR-0014. |
| Backfill | Natural: replay slots via `airflow dags backfill`. Idempotent merges converge. | Same replay mechanism but each slot targets a different dataset path. Existing day datasets are updated in place, not duplicated. |
| Operational complexity | Low: matches the default DAG exactly. | Higher: requires managing a growing collection of per-date datasets, a retention sweep, and a dynamic `datasets_file` updated on each run. |

**Recommendation:** start with Option A. Choose Option B only after confirming that the drop-a-day
cost or isolation requirement cannot be met at the row level, and that the team is prepared to own
multi-dataset fan-out at the application layer.
