# Prewarm + tag-based blue-green serving: correct composition plan

Checkout-verified design for making the gRPC Prewarm RPC and the planned `prod`-tag blue-green serving scheme
(task #12) compose so that prewarming warms the exact version that will be served. All evidence is cited as
`path:line`. The lance checkout at `/Users/gstamatakis/IdeaProjects/lance` is read-only ground truth. The Rust
crate is treated as read-only for this task. This document is a plan, not a change.

---

## 0. The risk in one paragraph

Prewarm warms a dataset's serialized index cache, metadata byte cache, and an open dataset handle. Both disk
caches are keyed by dataset URI plus the per-index UUID and the version-numbered manifest path. A green rebuild
mints fresh random index UUIDs (`Uuid::new_v4()`, lance `rust/lance/src/dataset/index.rs:221-222`,
`rust/lance/src/index.rs:497`) and writes a new versioned manifest, so warming the live blue version then
flipping `prod -> green` leaves every green index and manifest COLD. Worse, the provider's open-handle LRU is
keyed by URI alone and opens at latest with no TTL (`provider.rs:135`, `provider.rs:202-220`), so a tag flip is
never observed by an already-open handle and the service can serve stale blue indefinitely. The fix is to make
Prewarm target an explicit version (or an arbitrary tag, not just `prod`), and to make the provider resolve a
serve tag to a concrete `version_id`, open at that version, and key the handle LRU on the resolved version with a
bounded refresh so a flip is picked up promptly.

---

## 1. Current Prewarm behavior (evidence)

What Prewarm resolves and warms today:

- The handler reads `PrewarmRequest`, takes the target, builds the spec, and calls `backend.prewarm`
  (`grpc/mod.rs:240-250`). The spec is `metadata`, `all_indexes`, `index_names`, `fts_with_position` only
  (`grpc/convert.rs:36-43`, proto `search.proto:47-59`).
- The backend opens the dataset via `self.provider.dataset(target, date)` (`lance/prewarm.rs:37`). `date` comes
  from `target.single_date()` (`lance/prewarm.rs:36`), which is `None` for the rangeless dataset or one day
  (`domain/target.rs:91-99`). There is NO version or tag input anywhere on this path.
- After opening, it `load_indices()`, filters out system indexes, resolves target names, and calls
  `dataset.prewarm_index` / `prewarm_index_with_options` per index with bounded concurrency
  (`lance/prewarm.rs:99-152`).

So today Prewarm warms whatever the provider resolves, which is always the LATEST manifest of the URI. A caller
CANNOT target a specific version or tag. Confirmed: the proto `DatasetTarget` carries only org/tenant/namespace
plus an optional `date_range` (`search.proto:36-45`, `domain/target.rs:57-66`), and the provider opens by URI
with no version (`provider.rs:215`).

Cache tiers populated and how they are keyed:

- Dataset handle: a Moka `Cache<String, Arc<Dataset>>` keyed by the dataset URI string
  (`provider.rs:72`, `provider.rs:205-220`). The key is `{base}/{org}/{tenant}/{namespace}.lance` or the
  per-day variant (`provider.rs:166-173`). No version, no tag. Capacity-only LRU, no TTL (`provider.rs:135`).
- Disk index cache (serialized index pages plus an in-memory hot tier): keyed by Lance's `InternalCacheKey`
  whose `prefix()` the backend hashes into a per-prefix directory (`cache/disk_cache.rs:82-89`). Lance prefixes
  these keys with the dataset URI and the index UUID, as the module doc states and the prefix-invalidation test
  proves: `s3://bucket/ds.lance/` for dataset-scoped entries and `s3://bucket/ds.lance/uuid-1/` for
  index-scoped entries (`cache/disk_cache.rs:26-30`, `cache/disk_cache.rs:514-534`, `provider.rs:45-51`).
- Metadata byte cache (manifest, transaction, small `_indices/` ranges): keyed by `store_prefix` plus the object
  path (`cache/store_cache.rs:91-93`). Manifests live at `_versions/{n}.manifest` and are treated as immutable
  and cacheable, while the mutable latest pointer `_latest.manifest` is never cached (`cache/store_cache.rs:44`,
  `cache/store_cache.rs:60-78`, `cache/store_cache.rs:556-564`).

Net: every populated tier is URI + UUID + version-path keyed. Warming version A populates entries that a query
against version B (different UUIDs and a different manifest path) will not hit. The disk index cache hot tier and
the metadata byte cache for version A are simply never read by version B.

---

## 2. Current version/tag resolution in the provider (evidence)

- The provider opens with `DatasetBuilder::from_uri(&open_uri).with_session(session)` and optional store params,
  then `.load()` (`provider.rs:215-219`). No `with_version`, no `with_tag`. This always resolves the latest
  manifest (lance `builder.rs:698` `None => (None, None)`, then `builder.rs:782-790`
  `resolve_latest_location`).
- There is NO tag support in the provider today. The word "tag" does not appear in `provider.rs` or `config.rs`.
- The handle LRU is keyed by the URI string only and coalesces concurrent opens via `try_get_with`
  (`provider.rs:211-221`). It is `Cache::new(capacity)` with no `time_to_live` or `time_to_idle`
  (`provider.rs:135`).

Would a tag flip be observed, and after how long? With the current code there is no tag, so the question maps to
"when does a cached handle pick up a newer latest version". Answer: never, for as long as the handle stays
resident. A `Cache<String, Arc<Dataset>>` entry is created once on a cold open and reused for every later request
to that URI (`provider.rs:211-228`). The `Dataset` is a snapshot of one manifest version. There is no
`checkout_latest`, no TTL, and no invalidation, so the handle serves its original version until LRU capacity
pressure evicts it (default capacity 1024, `config.rs:13`). On a low-cardinality tenant that handle can live for
the whole process lifetime. This is the stale-after-flip failure waiting to happen once a `prod` tag is opened
through this same path.

Recall capture already reads `dataset.version_id()` for served queries (`backend.rs:194-196`, lance
`dataset.rs:2074`), so the served version is observable on the trace, which we exploit in the telemetry design.

---

## 3. Lance tag and version APIs (checkout evidence)

Creating, updating, resolving tags:

- Rust `Tags`: `create(tag, reference)`, `update(tag, reference)`, `delete(tag)`, `get(tag) -> TagContents`,
  `get_version(tag) -> u64`, `list()` (`rust/lance/src/dataset/refs.rs:148-282`, accessor
  `Dataset::tags()` at `rust/lance/src/dataset.rs:452`). `update` overwrites the tag JSON via a single object
  `put` (`refs.rs:255-282`), so a flip is one small-object write, which is the O(1) cutover the task wants.
  `get_version` reads the tag JSON and returns its `version` field (`refs.rs:196-198`).
- Python `Tags`: `ds.tags.create(tag, reference)`, `ds.tags.update(tag, reference)`, `ds.tags.delete(tag)`,
  `ds.tags.get_version(tag) -> Optional[int]`, `ds.tags.list()` (`python/python/lance/dataset.py:6802-6919`,
  accessor `Dataset.tags` at `dataset.py:873`). `reference` accepts an int version, a tag name string, or a
  `(branch, version)` tuple (`dataset.py:6890-6908`).
- Tag storage: a tag is a JSON file at `{base}/_refs/tags/{tag}.json` containing the version and manifest size
  (`refs.rs:893-904`). It is NOT under `_versions/`, `_transactions/`, or `_indices/`, so the metadata byte
  cache `classify()` returns `None` and tag reads always pass through to the real store
  (`cache/store_cache.rs:60-78`). That is correct: tag resolution must never be served from a stale cache.

Opening at a specific version or tag:

- Rust `DatasetBuilder::with_version(u64)` and `with_tag(&str)` exist
  (`rust/lance/src/dataset/builder.rs:236-252`). They set a `Ref` (`refs.rs:31-40`:
  `VersionNumber(u64)`, `Version(branch, version)`, `Tag(String)`).
- The Rust open path can take a tag directly. `with_tag` resolves the tag during `load()`: it constructs `Refs`,
  calls `refs.tags().get(&tag_name)`, reads the version, then loads that versioned manifest
  (`builder.rs:680-735`). So a tag open is internally tag -> version_id -> versioned manifest. The provider can
  either pass `with_tag("prod")` and let lance resolve, or resolve `prod -> version_id` itself with
  `dataset.tags().get_version` and then `with_version(id)`. The design below resolves explicitly so the resolved
  id becomes the cache key.
- Python: `lance.dataset(uri, version=...)` accepts `int | str` where a string is a tag
  (`python/python/lance/dataset.py:715`), and `Dataset.checkout_version(int | str | (branch, version))`
  (`dataset.py:2850-2864`).

Version pinning so prewarm-by-version and serve-by-resolved-version reference the identical manifest and index
UUIDs:

- A version number resolves to exactly one immutable manifest file: V1 `_versions/{version}.manifest`, V2
  `_versions/{u64::MAX - version:020}.manifest` (`rust/lance-table/src/io/commit.rs:83-110`). Both schemes live
  under `_versions/` and end in `.manifest`, so the metadata byte cache treats both as cacheable immutable
  manifests (`cache/store_cache.rs:60-78`).
- The manifest pins the full index list with their UUIDs (index UUIDs are random per build,
  `index.rs:221-222`, `index.rs:497`). Therefore opening version N twice, or prewarming version N then serving a
  tag that resolves to version N, references byte-for-byte the same manifest path and the same index UUIDs, so
  every cache key matches. This is the property the whole plan rests on: warm by version_id, serve by the same
  version_id.

---

## 4. The correct design

### 4a. Prewarm RPC target: accept an explicit version or tag (Rust change)

Make Prewarm able to warm green BEFORE the flip. Add an optional ref selector to the proto. This is an additive,
non-breaking change (new optional fields, old clients keep latest-resolution semantics).

Proto change in `proto/lance_etl/search/v1/search.proto`, `PrewarmRequest` (currently `search.proto:47-59`):

```proto
message PrewarmRequest {
  DatasetTarget target = 1;
  bool metadata = 2;
  bool all_indexes = 3;
  repeated string index_names = 4;
  bool fts_with_position = 5;
  // New: pin the version to warm. Exactly one of version / tag may be set. When both
  // are unset, warm the latest version (today's behavior).
  oneof ref {
    uint64 version = 6;
    string tag = 7;
  }
}
```

Because the same selector is needed by serving (4b) and by Clusters, put it on `DatasetTarget` instead if a
single shared selector is preferred. Either placement is additive. The minimal blue-green requirement is that
Prewarm can name the green version explicitly, so the version variant is mandatory and the tag variant is a
convenience for warming a non-`prod` staging tag.

Domain change in `domain/prewarm.rs` `PrewarmSpec` (currently `domain/prewarm.rs:8-18`): the version selector is
a property of the dataset to open, not of what-to-warm, so model it as a `DatasetRef` carried alongside the
target rather than inside `PrewarmSpec`. Add to `domain/target.rs`:

```rust
pub enum DatasetRef { Latest, Version(u64), Tag(String) }
```

and thread an `Option<DatasetRef>` (defaulting to `Latest`) into the provider call.

Provider trait change in `lance/provider.rs` (currently `provider.rs:33-37`): extend `dataset` to take the ref:

```rust
fn dataset(&self, target: &DatasetTarget, date: Option<NaiveDate>, reference: DatasetRef)
    -> impl Future<Output = Result<Arc<Dataset>, SearchError>> + Send;
```

Handler change in `grpc/mod.rs` Prewarm (currently `grpc/mod.rs:240-250`): map the proto `ref` oneof to
`DatasetRef`, pass it down, and annotate the span with the resolved version (see 4d).

Backend change in `lance/prewarm.rs` (currently `lance/prewarm.rs:34-79`): pass the `DatasetRef` to
`self.provider.dataset(...)` and record `prewarm.resolved_version = dataset.version_id()` on the span and in the
report so callers can confirm which version was warmed.

### 4b. Provider tag resolution keyed on resolved version_id (Rust change)

Two coupled changes: resolve the serve ref to a concrete `version_id`, and key the handle LRU and the implicit
disk-cache scoping on that resolved id, with a bounded refresh.

1. Serve ref configuration. Add to `Config` (`config.rs:76-145`) a serve selector, for example
   `SEARCH_API_SERVE_TAG` (default `prod`) and a feature switch `SEARCH_API_SERVE_BY_TAG` (default off so the
   current latest behavior is preserved until rollout). When serve-by-tag is on, search opens the dataset at the
   resolved serve tag instead of latest.

2. Resolve tag -> version_id, then open by version. In `provider.dataset`, when the ref is a tag, resolve it
   once via lance `dataset.tags().get_version(tag)` (`refs.rs:196-198`) or by letting `with_tag` resolve, then
   open with `with_version(resolved_id)` so the open is pinned and reproducible. Resolution reads
   `_refs/tags/{tag}.json`, which is always a live read because that path is not cacheable
   (`cache/store_cache.rs:60-78`, `refs.rs:901-904`).

3. Key the handle LRU on the resolved version, not the tag string. Change the key type from `String` to a
   composite, for example `format!("{uri}@{version_id}")`, or a typed key
   `struct HandleKey { uri: String, version: u64 }`. Today the key is the bare URI (`provider.rs:72`,
   `provider.rs:205`). Keying on the resolved version means:
   - Blue (version A) and green (version B) handles coexist as distinct entries, so a flip never mutates an
     existing entry, it selects a different one.
   - A flip is observed as soon as tag resolution returns the new version_id, because that produces a different
     handle key and a fresh open of the green manifest.

4. Bound how long a tag resolution is trusted so flips are picked up promptly without a manifest read per
   request. Add a small tag-resolution cache `Cache<(uri, tag), (version_id, Instant)>` with a short
   `time_to_live`, for example `SEARCH_API_SERVE_TAG_TTL_SECS` default 5 to 15 seconds. Within the TTL, requests
   reuse the resolved version_id and hit the warm handle. After the TTL, the next request re-reads the tag JSON
   (one small `_refs/tags/{tag}.json` GET) and, if the version changed, opens and caches the new handle. The old
   blue handle ages out of the handle LRU by capacity or by an added `time_to_idle`. This bounds staleness to at
   most the TTL while keeping the steady-state per-request cost at zero extra manifest reads.

   Data-structure summary for `CachingDatasetProvider` (`provider.rs:69-77`): keep `datasets` but rekey it on
   `(uri, version_id)`, and add `tag_versions: Cache<(String, String), u64>` built with
   `time_to_live(serve_tag_ttl)`. The disk index cache and metadata byte cache need NO key change: they are
   already version- and UUID-scoped through lance's `InternalCacheKey` prefixes and the `_versions/{n}.manifest`
   path (`cache/disk_cache.rs:26-30`, `cache/store_cache.rs:60-78`), so once the provider opens the correct
   version, those tiers self-segregate blue from green.

Explicit invalidation alternative to the TTL: a small admin RPC or signal that drops the `tag_versions` entry
for a `(uri, tag)` immediately after a flip. The TTL is the simpler default and needs no new control plane. Both
can coexist (TTL as the safety net, explicit invalidation for instant cutover).

### 4c. The safe operational sequence (python helper + Rust + runbook)

Where each step lives:

1. Build green. The distributed ETL plus index build commits a new version N on the dataset URI (existing
   pipeline, `src/lance_etl/etl.py`, `src/lance_etl/indexing.py`). No tag is moved yet, so `prod` still points
   at blue version M and all serving stays on blue. Python.
2. Prewarm green by version. An operator or the DAG calls Prewarm with `version = N` against the serving
   replicas. This warms the green manifest, its index UUIDs, and the metadata byte ranges into every replica's
   caches while traffic still serves blue. Rust serves the Prewarm. Trigger is python or an operator.
3. Flip the prod tag. A new python tagging helper writes `ds.tags.update("prod", N)`
   (`python/python/lance/dataset.py:6890-6908`) and logs the sequence (old version M, new version N, timestamp,
   replica set prewarmed). This is the O(1) cutover (`refs.rs:255-282`). Python.
4. Blue drains. Within the serve-tag TTL (4b step 4) replicas re-resolve `prod` to N, open and serve green from
   the already-warm caches, and the blue handle ages out of the handle LRU. No cold first query because step 2
   pre-populated version N. Rust.

The python tagging helper is the new artifact and belongs next to the ETL or compaction CLI
(`src/lance_etl/cli.py`). It should: resolve and log the current `prod` version, refuse to flip to a version
that has no committed indexes if recall safety is required, perform `tags.update`, and emit a structured log line
plus a Datadog event so the flip is correlatable with the cold-query metric in 4d. It must NOT itself prewarm,
because warming runs inside the Rust serving processes, not the driver.

### 4d. Telemetry to make a flip-without-prewarm observable

The provider already emits dataset-open cold/warm latency and handle-cache hits (`metrics.rs:278-288`,
`provider.rs:223-227`), and search records `dataset.version_id()` on the recall span
(`backend.rs:194-196`, `recall.rs:102-135`). Add:

- Prewarm resolved version. Record `prewarm.resolved_version` on the Prewarm span and return it in
  `PrewarmResponse` (extend `proto` `PrewarmResponse` `search.proto:89-101` and
  `grpc/convert.rs:72-88`). Keep a process gauge `prewarm.last_version` (new `metrics.rs` method) holding the
  most recently warmed version per dataset.
- Served vs warmed gap. On each cold open for serving, compare the opened `version_id` against the last warmed
  version for that URI. Emit a counter `serve.cold_open_unwarmed` tagged by whether the opened version equals
  the last prewarmed version. A nonzero `served_version != last_prewarmed_version` cold open is exactly the
  flip-without-prewarm signal. Implement as a new `Metrics` method following the existing typed-facade pattern
  (`metrics.rs:278-288`), with no org tag (cardinality policy, `metrics.rs:168-172`).
- Tag-resolution refresh count. Counter `serve.tag_resolved` incremented whenever the `tag_versions` TTL lapses
  and a tag JSON is re-read, tagged `changed:true|false` when the resolved version differs from the prior. This
  shows flip propagation latency across the fleet.
- Cold-first-query-after-flip. A distribution `serve.first_query_after_flip_ms` recorded for the first query on
  a newly resolved version, sourced from the existing dataset-open cold timing (`metrics.rs:278-283`) tagged
  `post_flip:true`. A spike here with `serve.cold_open_unwarmed` confirms a missed prewarm.

All four are pure Rust metric additions in `telemetry/metrics.rs` plus call sites in `lance/provider.rs`,
`lance/prewarm.rs`, and `grpc/mod.rs`. They respect the existing tag policy (no `org_id` on metrics, org detail
stays on traces, `metrics.rs:168-172`).

### 4e. Failure modes to test

| # | Scenario | Expected observable |
|---|----------|---------------------|
| 1 | Flip prod to N, then prewarm N | First green query is observably COLD (high `dataset.open` cold latency, `serve.cold_open_unwarmed` increments) until the prewarm completes. Proves ordering matters. |
| 2 | Prewarm N, then flip prod to N | First green query is WARM (handle hit or disk-cache hit, `serve.cold_open_unwarmed` stays zero). The success path. |
| 3 | Tag moved mid-prewarm | Prewarm pins version N at call time via the resolved id, so it completes warming N regardless of a concurrent flip. A later flip to a third version is a separate, detectable event. Assert prewarm warms exactly the version it was asked for. |
| 4 | Provider serving stale after flip | With the version-keyed handle LRU plus serve-tag TTL, a query after `TTL` resolves the new version. Assert the served `dataset.version_id()` changes within the TTL and never exceeds it. Regression guard against the current never-observe behavior (`provider.rs:135`). |
| 5 | Cleanup deletes a tagged version mid-prewarm | Lance `cleanup_old_versions(error_if_tagged_old_versions=True)` protects tagged versions by default (`python/python/lance/dataset.py:2917-2972`). Test that an untagged in-flight green version being warmed is NOT deleted by a concurrent cleanup before the flip, and document that green must be tagged (even a transient `staging` tag) before any cleanup runs, or cleanup must exclude versions newer than the current `prod`. |

---

## 5. Interaction with stable row ids (#15) and V2 manifest paths (#12 A3)

Stable row ids (#15, in flight, see `market-research/stable-row-ids-plan.md`): stable row ids change how
`_rowid` values persist across compaction and rewrites, not how tags or versions resolve and not how index UUIDs
are assigned. Index UUIDs remain random per build (`index.rs:497`) and remain pinned per manifest version, so the
cache-keying argument in sections 1 and 3 is unaffected. The one coupling worth noting: stable row ids make
`_rowid` comparable across a blue-green flip, which improves recall scoring across the cutover
(`recall.rs:102-135`, `backend.rs:194-196`) because the same logical row keeps the same id in blue and green.
Without stable row ids, post-flip recall comparisons must dedup on the logical id column
(`backend.rs:148`, `config.rs:42-43`) rather than `_rowid`. No change to the prewarm or provider design.

V2 manifest paths (#12 A3): V2 changes the manifest filename to `_versions/{u64::MAX - version:020}.manifest`
but keeps it under `_versions/` and ending in `.manifest` (`rust/lance-table/src/io/commit.rs:93-110`). The
metadata byte cache classifies both V1 and V2 manifests as immutable cacheable Manifest entries
(`cache/store_cache.rs:60-78`), so version pinning and cache segregation hold under V2. One adjustment to flag:
the byte cache hard-codes the V1 mutable pointer name `_latest.manifest` for never-cache
(`cache/store_cache.rs:44`, `cache/store_cache.rs:72`). V2 uses a different latest pointer,
`latest_version_hint.json` under `_versions/` (`commit.rs:73-79`). That file does not end in `.manifest`, so the
current `classify()` already passes it through uncached by falling into the `Some(PathKind::Manifest) => None`
arm. Correct today by accident of the suffix check, but when #12 A3 lands the never-cache rule should be made
explicit for `latest_version_hint.json` so a future refactor cannot regress it into a cached, stale latest
pointer. This is the only manifest-path coupling that touches the cache correctness of this plan.

---

## 6. Summary: Rust vs python vs runbook

Rust changes (the serving crate):

- Proto: add `oneof ref { uint64 version; string tag; }` to `PrewarmRequest` (additive, non-breaking), and add
  `resolved_version` to `PrewarmResponse` (`search.proto:47-59`, `search.proto:89-101`).
- Domain and handler: add `DatasetRef`, thread it through `prewarm_spec`/handler, map the proto oneof
  (`domain/target.rs`, `grpc/convert.rs:36-43`, `grpc/mod.rs:240-250`).
- Provider: extend `dataset(...)` to take `DatasetRef`, resolve a serve tag to a concrete `version_id`, open by
  version, rekey the handle LRU on `(uri, version_id)`, and add a short-TTL `tag_versions` resolution cache
  (`provider.rs:33-37`, `provider.rs:69-77`, `provider.rs:135`, `provider.rs:202-220`). Disk index cache and
  metadata byte cache need NO key change.
- Config: `SEARCH_API_SERVE_BY_TAG`, `SEARCH_API_SERVE_TAG` (default `prod`), `SEARCH_API_SERVE_TAG_TTL_SECS`
  (`config.rs:76-145`).
- Telemetry: `prewarm.last_version`, `serve.cold_open_unwarmed`, `serve.tag_resolved`,
  `serve.first_query_after_flip_ms` (`telemetry/metrics.rs`, call sites in provider/prewarm/grpc).

Python changes (the ETL/control side):

- A tagging helper in `src/lance_etl/cli.py` that logs the current `prod` version, calls
  `ds.tags.update("prod", N)` (`python/python/lance/dataset.py:6890-6908`), and emits a structured log plus a
  Datadog event. It must not prewarm.
- Ensure cleanup runs with `error_if_tagged_old_versions=True` or excludes versions at or newer than current
  `prod`, and that green is tagged (even transiently) before cleanup (`dataset.py:2917-2972`).

Operational runbook items (no code):

- The four-step sequence: build green, prewarm green by version against every serving replica, flip `prod`, let
  blue drain within the serve-tag TTL.
- Rollout gate: keep `SEARCH_API_SERVE_BY_TAG` off until prewarm-by-version is verified, then enable it so
  serving opens `prod` instead of latest.

Direct answers to the three asked questions:

- Key risks: (1) cache keys are URI + index-UUID + version-manifest scoped, so a flip to a freshly built green
  version with new random UUIDs is cold unless warmed by that exact version; (2) the handle LRU is URI-keyed
  with no TTL and no checkout-latest, so it serves stale blue indefinitely after a flip
  (`provider.rs:135`, `provider.rs:205-220`); (3) cleanup could delete an untagged in-flight green version
  mid-prewarm unless it is tag-protected.
- Exact proto change: add `oneof ref { uint64 version = 6; string tag = 7; }` to `PrewarmRequest`, and
  `uint64 resolved_version` to `PrewarmResponse`.
- Exact provider change: resolve the serve tag to a `version_id`, open the dataset with `with_version(id)`, rekey
  the handle LRU on `(uri, version_id)`, and add a short-TTL tag-resolution cache so flips are observed within a
  bounded window without per-request manifest reads.
- Can the current Prewarm RPC already target a version? No. The proto has no version or tag field and the backend
  always opens latest (`search.proto:47-59`, `lance/prewarm.rs:37`, `provider.rs:215`). It needs the additive
  proto change above. The change is non-breaking (new optional fields), so it is NOT a breaking proto change.
