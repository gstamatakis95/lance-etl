# ADR 0032: Hourly interval tags at ETL write time and per-query tag pinning

Status: Accepted

Date: 2026-07-03

## Context

ADR 0027 introduced interval tags (`%Y%m%dT%H%M%SZ` names) stamped by the unified pipeline job
after compaction and indexing, with a keep-last-48 prune and the version-cleanup exemption for
tagged versions. That leaves a gap on both ends. On the write side, a dataset version produced
by the Spark ETL carries no marker until the pipeline runs, so there is no stable name for
"what this dataset looked like in hour X" at data-visibility time. On the read side, the
search service resolved a query to the serve policy or the latest version only, with tag and
version pins reserved for the prewarm path.

The operator wants every ETL write to stamp the produced dataset with a tag named after the
truncated hour it was produced, and the query service to optionally pin a query to such a tag,
while the latest version stays the fast default path and caching keeps working well.

## Decision

### The ETL stamps the hour tag at write time

`ETLConfig.tag_stamp` carries a pre-formatted tag name, produced by the new
`cliutil.parse_hour_tag`: an ISO 8601 instant truncated to its UTC hour and formatted as
`%Y%m%dT%H%M%SZ` (`2026-06-11T12:34:56` becomes `20260611T120000Z`). The ETL DAG passes
`--tag-stamp {{ data_interval_end | string }}`, so the tag names the hourly window that
produced the data.

The stamp runs once on the driver after every batch of the run has committed
(`IcebergToLanceETL.stamp_interval_tags`), fanning `update_serving_tags` out over exactly the
datasets the run wrote (`seen_datasets`). It never runs inside the executor-side merges, which
touch one dataset from multiple partitions and would stamp redundantly and racily.

`update_serving_tag` is create-or-move, so a second ETL run within the same hour advances that
hour's tag to the newest version. The tag therefore means "the latest version produced in hour
X", which is the natural read for an hourly checkpoint.

### Stamping ownership is shared and convergent

The pipeline job keeps its post-index stamp. Both stampers write the same tag name for the
same hourly window, and both use create-or-move, so they converge: the ETL stamp makes the
window addressable as soon as data is visible, and the pipeline stamp later moves the same tag
onto the compacted and indexed version. The pipeline's prune phase (keep newest 48 interval
tags) and the cleanup exemption for tagged versions already bound and protect ETL-created tags
with no changes.

### The query service pins per request through `version_ref`

Every search request (vector, text, hybrid) carries an optional `version_ref` oneof (a version
id or a tag name). Unset means `DatasetRef::Serve`, the existing serve policy — latest, or the
resolved serve tag when serve-by-tag is on — so the common latest-version path is untouched. A
hybrid pin opens both legs at the same resolved snapshot, keeping fusion dedup consistent.

One correction ships with this: open INTENT is no longer inferred from the reference. The
provider previously classified `Tag`/`Version`/`Latest` opens as prewarm intent, which was
true when only prewarm used them. A serving search pinned to a tag would then skip the
`serve.cold_open` metric and pollute the last-prewarmed bookkeeping. `DatasetProvider` now has
an explicit `dataset_for_prewarm` entry point (used only by the prewarm path), and both trait
methods route through one internal open that takes the intent as a parameter.

### Caching

No new cache machinery is needed because every layer already keys on the resolved version:

- The open-handle LRU keys on `(uri, resolved version)`, so a pinned-tag handle coexists with
  the serve handle instead of evicting it.
- Tag-to-version resolutions are cached per `(uri, tag)` with the serve-tag TTL (10 s) and
  coalesced, so steady traffic against one tag costs at most one live tag read per TTL window
  per process.
- The Lance index and metadata caches are version- and manifest-scoped (ADR 0007), so a pinned
  query reuses exactly the entries a prewarm of that version populated, on disk or Redis
  (ADR 0031) alike.

Caveats worth monitoring rather than engineering around now: each distinct pinned version is
its own weighted handle in the LRU, and each distinct tag costs one live tag-JSON read per TTL
window, so very high pinned-tag cardinality would pressure the handle cache and the object
store. The expected workload — latest most of the time, occasional hour pins — is far from
that regime.

## Consequences

- Every hourly window is addressable by name the moment its data lands, pinned against version
  cleanup until the pipeline prunes it (about two days at the default keep-last 48).
- Clients time-travel per query (`version_ref.tag = "20260611T120000Z"`) without any
  server-side configuration change, and the default request shape and its performance are
  unchanged.
- Tag-pinned serving traffic now reports `serve.cold_open` correctly instead of being counted
  as prewarm.
- Replayed or backfilled ETL windows re-stamp their hour's tag deterministically, matching the
  idempotent-merge semantics of the ETL itself.
