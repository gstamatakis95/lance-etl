# ADR 0027 — Unified pipeline: prune, maintenance, index, stamp in one DAG

**Status**: Accepted

**Date**: 2026-06-11

---

## Context

The compaction and indexing jobs ran as two independent Airflow DAGs
(``lance_etl_maintenance`` and ``lance_etl_index``) with the correct
compact-before-index ordering enforced only by schedule-stagger convention.  That
ordering dependency is real.  The bench end-to-end flow was reordered to
ETL → compact → index → tag for exactly this reason.  Running the two jobs
concurrently was the root cause of the stale-fragment replan loop and was why the index
DAG required ``max_active_runs=1`` in the first place.

Three additional costs came with the two-DAG split.

**Two cluster spin-ups per hour.**  Each DAG launches a separate Spark cluster, meaning
maintenance and indexing each pay the full cluster bootstrap cost.  A unified job can
share the session across phases at no extra overhead.

**No home for interval tagging.**  Tags mark the Lance dataset version that corresponds
to a completed ETL window.  They pin that version against cleanup and allow the search
API to pin its read version.  Neither the maintenance nor the indexing job knew about ETL
windows.  The stamp had to be injected separately, and there was no enforcement that the
stamp came after indexing.

**Unbounded tag accumulation.**  Tags pin versions against cleanup.  Hourly tagging
without a retention policy would grow disk usage unboundedly.  Retention logic had no
production home.

The approved resolution is to collapse maintenance, indexing, and stamping into a single
serialized job (``lance_etl.pipeline``) run by a single DAG.

---

## Decision

### New package: ``lance_etl.pipeline``

A new Python package ``lance_etl.pipeline`` implements ``PipelineJob``, which runs the
following phases in series over the dataset fleet:

1. **Prune** (skipped when ``tag_keep_last`` is ``None``): delete interval tags older
   than the keep-last window.  Pruning runs first so the same run's cleanup step in the
   maintenance phase reclaims the versions that were pinned by the dropped tags.

2. **Maintenance**: run ``MaintenanceJob.run`` — TTL expiration, two-tier compaction,
   version cleanup.

3. **Index**: run ``LanceIndexer.run`` — IVF_RQ vector indices and scalar/FTS indices
   with derived-state skip.

4. **Stamp**: when ``tag_stamp`` is set, write an interval tag named in colon-free UTC
   form (``%Y%m%dT%H%M%SZ``) via ``update_serving_tags``.  When ``serve_tag`` is true,
   also advance the ``HEAD`` tag to the same version.

### Tag retention

Interval tag names match the ``%Y%m%dT%H%M%SZ`` strptime pattern.  Names that do not
match (e.g. ``HEAD``, operator-set labels) are never touched.  Matching tags are sorted
descending by parsed datetime and the newest ``tag_keep_last`` are kept.  The rest are
deleted.  The default keep-last value is 48, which equals two days at hourly cadence.

### Tag stamp format

The stamp string is derived from the Airflow ``data_interval_end`` value passed through
the ``--tag-stamp`` CLI flag and parsed by ``parse_window_tag`` in ``cliutil.py``.
The parser accepts Airflow-rendered datetime strings (e.g. ``2026-06-11 12:00:00+00:00``)
and ISO-8601 forms.  It always normalises to colon-free UTC (e.g. ``20260611T120000Z``).

### Scheduler pool

A single ``lance-pipeline`` pool is applied to both the maintenance and indexing sub-job
configs inside ``PipelineConfig.__post_init__``.  The pool is a knob for Spark scheduler
fairness between datasets within a phase.  It has no effect on Airflow-level concurrency,
which is controlled by ``max_active_runs=1``.

### DAG changes

``lance_etl_maintenance_dag.py`` and ``lance_etl_index_dag.py`` are deleted.  A single
new DAG ``lance_etl_pipeline_dag.py`` (DAG id ``lance_etl_pipeline``) replaces them.
The ETL DAG (``lance_etl_etl_dag.py``) is unchanged except for its COEXISTENCE docstring,
which is updated from a three-DAG model to a two-DAG model.

``lance_etl_common.py`` exports ``APPLICATION_PIPELINE`` and ``pipeline_dag_params``
in place of the deleted ``APPLICATION_INDEXING``, ``APPLICATION_MAINTENANCE``,
``index_dag_params``, and ``maintenance_dag_params`` constants.

The recommended schedule is a cron offset such as ``15 * * * *`` on the pipeline DAG so
it trails the ETL DAG within the same clock hour without requiring explicit task
dependencies between DAGs.

### Standalone CLIs retained

The ``lance_etl.maintenance`` and ``lance_etl.indexing`` packages and their CLIs
(``lance-etl-maintenance``, ``lance-etl-index``) are kept as standalone operator tools.
They are not scheduled and are not invoked by the pipeline DAG.  Operators can still call
them directly for one-off rebuilds or diagnostics without triggering a full pipeline run.

---

## Consequences

**Two DAGs removed.**  ``lance_etl_maintenance`` and ``lance_etl_index`` are gone.
Operators managing schedules, SLAs, or alerting rules for those DAGs must migrate to
``lance_etl_pipeline``.

**Whole pipeline serialized.**  ``max_active_runs=1`` on ``lance_etl_pipeline`` means
at most one run is in flight at any time.  A slow index build or a large compaction will
delay the start of the next run.  This is the same constraint the index DAG already had.

**Correct compact-before-index ordering is now structural.**  The phases run in sequence
inside a single Spark job.  No schedule stagger or external coordination is needed to
avoid concurrent compaction and indexing.

**One cluster spin-up per pipeline run.**  The single job pays the cluster bootstrap cost
once regardless of the number of phases.

**New Airflow Variables.**  ``lance_etl_pipeline_schedule``, ``lance_etl_tag_keep_last``,
and ``lance_etl_pipeline_serve_tag`` are new.  Existing ``lance_etl_datasets_file``,
``lance_etl_ttl_column``, ``lance_etl_index_flags``, ``lance_etl_dd_service``,
``lance_etl_dd_env``, and ``lance_etl_dd_tags`` are reused by the pipeline DAG with the
same semantics as before.

---

## Future work

The current implementation serializes phases fleet-wide: all datasets finish maintenance
before any dataset begins indexing.  A future revision could interleave phases per
dataset (maintenance on dataset A, then index dataset A, then move to dataset B) to
reduce the time between a dataset's compaction completing and its index becoming current.
This was deferred because fleet-level serialization is simpler to reason about and the
correctness properties are easier to verify.
