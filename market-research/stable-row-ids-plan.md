# Move-stable row IDs + Fragment Reuse Index: implementation plan

Checkout-verified plan for enabling move-stable row IDs across the lance-etl pipeline so compaction stops
needing inline index remap. All `path:line` references were verified against the lance checkout at
`/Users/gstamatakis/IdeaProjects/lance` (pylance 8.0.0-beta.6) and the lance-etl sources during this pass.

## Verdict: GO

Stable row IDs are the correct structural endgame and the pinned build supports them end to end. The single
load-bearing production change is one keyword argument at dataset bootstrap. The change makes the tier-B
`Compaction.commit` binding gap irrelevant (not merely worked around), lets us delete the IVF_RQ remap-corruption
mitigation entirely, and shrinks the tier-B commit window to near nothing. One caveat keeps this from being an
unqualified GO without a test pass: the in-tree lance coverage exercises stable-row-id compaction with IVF_PQ and
with the single-process `compact_files` path, not with IVF_RQ and not through the distributed `Compaction.commit`
binding our tier B uses. Both gaps are closed by the proof test below, which must pass before fleet rollout. The
fragment-level orphan-race guard another agent is adding to `indexing.py` MUST STAY — stable row IDs do not
eliminate it.

## 1. The exact write parameter and its semantics

The parameter is `enable_stable_row_ids: bool`.

- pylance entry point: `lance.write_dataset(..., enable_stable_row_ids: bool = False, ...)`
  (`python/python/lance/dataset.py:7035`, threaded into the params dict at `dataset.py:7282`; docstring at
  `dataset.py:7098-7104`). Also accepted by `LanceDataset.commit` / `commit_batch`
  (`python/src/dataset.rs:2543,2586,2634-2635` → `builder.use_stable_row_ids(enable)`) and by
  `LanceFragment` writers (`python/python/lance/fragment.py:1011,1061,1236`).
- Rust plumbing: `WriteParams.enable_stable_row_ids` → `CommitBuilder.use_stable_row_ids`
  (`rust/lance/src/dataset/write/commit.rs:40,83-84,337-340,360`) → `ManifestWriteConfig.use_stable_row_ids`
  → manifest reader feature flag `FLAG_STABLE_ROW_IDS`. The runtime predicate is
  `Manifest::uses_stable_row_ids()` (`rust/lance-table/src/format/manifest.rs:492-494`), surfaced to Python as
  `LanceDataset.has_stable_row_ids` (`python/python/lance/dataset.py:1351-1355`,
  `python/src/dataset.rs:913-915`).

Creation-time only — confirmed:

- On `append`, the user's `enable_stable_row_ids` is IGNORED and forced to match the existing manifest:
  `rust/lance/src/dataset/write/insert.rs:301-307` ("Ignoring user provided stable row ids setting ...,
  dataset already has it set to ..."). The commit builder does the same: if a dataset already exists it adopts
  `ds.manifest.uses_stable_row_ids()`, otherwise it uses the caller's value
  (`commit.rs:337-340`). The docstring for the sibling `auto_cleanup_options` confirms the general rule: these
  flags "only take effect when creating a new dataset" (`dataset.py:7115-7117`).
- Conclusion: it cannot be turned on later through append/merge/delete. To enable on existing data, recreate the
  dataset (a fresh `mode="create"`/first write with the flag). Pre-release, this is acceptable (datasets can be
  recreated; no migration burden).

What changes for our write paths when it is on (all handled natively on the pinned build):

- `merge_insert` (our upsert): supported. The merger is constructed with the manifest's stable-row-id flag
  (`rust/lance/src/dataset/write/merge_insert.rs:1695`) and refreshes row-level latest-update version metadata on
  full-fragment overrides and partial-row updates (`merge_insert.rs:1033-1039,1127-1135`).
- `append`: inherits the flag (above). No behavior change beyond row-id assignment.
- `delete`: supported (`rust/lance/src/dataset/write/delete.rs:314`, parameterized stable/non-stable concurrency
  tests at `delete.rs:852-894`).
- segment-API index builds (`create_index_uncommitted` / `commit_existing_index_segments`,
  FTS `index_uuid` + `merge_index_metadata`): still work. Indices key on row IDs; compaction reserves new
  fragment IDs and updates each index's fragment bitmap rather than remapping row addresses
  (`rust/lance/src/dataset/optimize.rs:1992-2003`). The `fragment_ids=` shard argument continues to select
  source fragments — fragment IDs are unaffected by the stable-row-id flag.

The "stable after compaction, but NOT after updates" caveat (`dataset.py:7100-7104`) is not a problem for us. An
update is internally a delete + insert, so an updated row's logical row ID is retired and the new value is an
unindexed new row — exactly the index-invalidation behavior we already have without stable row IDs. The only
guarantee we depend on, and the only one stable row IDs add, is stability across COMPACTION. Verified at the
Python level: `python/python/tests/test_dataset.py:394-425` (`test_enable_stable_row_ids`) shows `_rowid` values
unchanged (0,1,2,3) across `compact_files()` while `_rowaddr` changes.

## 2. Fragment Reuse Index lifecycle and why the binding gap dissolves

The pivot is one line: `commit_compaction` computes

```
let needs_remapping = !dataset.manifest.uses_stable_row_ids() && !options.defer_index_remap;
```

at `rust/lance/src/dataset/optimize.rs:1893`. The three branches that follow (`optimize.rs:1940-2010`):

- `needs_remapping == true` (no stable row IDs, no deferral): the inline eager remap path —
  `index_remapper.remap_indices(row_id_map, ...)` (`optimize.rs:1971-1986`). This is the path that silently
  corrupts IVF_RQ on the pinned build (recorded at `compaction.py:66-69` and
  `market-research/concurrency-and-coexistence.md:293-302`).
- `needs_remapping == false && options.defer_index_remap == true`: builds the FRI. The rewrite tasks carry
  `row_addrs`, collected into `FragReuseGroup`s and written as the `__lance_frag_reuse` system index via
  `build_new_frag_reuse_index` (`optimize.rs:1951-1968,2006-2010`). The FRI is applied lazily whenever an index
  loads (`rust/lance/src/index/vector/ivf/v2.rs:808,878`); pruning (`cleanup_frag_reuse_index`) is Rust-only with
  no Python binding (`rust/lance/src/dataset/index/frag_reuse.rs:28`).
- `needs_remapping == false && options.defer_index_remap == false` (THE STABLE-ROW-ID PATH): the `else if
  !options.defer_index_remap && !has_address_style` branch at `optimize.rs:1992-2003` simply reserves fragment
  IDs "so that the fragment bitmap can be updated for each index" and produces NO remap and NO frag-reuse index
  (`frag_reuse_index = None`, `optimize.rs:2006-2010`). Queries stay correct because the row IDs the indices
  reference never moved.

Therefore the binding gap is dissolved, not worked around. The distributed Python `Compaction.commit` binding
hard-codes `CompactionOptions::default()` (`python/src/dataset/optimize.rs:567-568`, the "TODO: pass compaction
option" path), i.e. `defer_index_remap=false`. But `needs_remapping` is derived from the MANIFEST, not from the
options. On a stable-row-id dataset `uses_stable_row_ids()` is true, so `needs_remapping` is false regardless of
what options the binding passes, and the commit takes the cheap reserve-fragment-ids branch. The hard-coded
options no longer matter for the remap concern. (`defer_index_remap` itself becomes irrelevant on stable-row-id
datasets — there is nothing to defer.)

Confirmed by the in-tree test `test_stable_row_indices()` (`optimize.rs:2800-2890`): a stable-row-id dataset with
a BTREE scalar index and an IVF_PQ vector index, deletions applied so row IDs differ from row addresses, is
compacted; the index UUID set is asserted UNCHANGED and both vector and scalar query results are asserted
identical before/after — with no rebuild. The distributed path is separately covered by `test_compact_distributed`
(`optimize.rs:2717-2796`, parameterized `use_stable_row_id ∈ {false,true}`), which drives
plan/execute/`commit_compaction` — but that test carries no indices.

## 3. Per-file change list (exactly where stable row IDs must be set)

### `src/lance_etl/etl.py` — the one load-bearing production change

- Bootstrap write at `apply_merge` (`etl.py:460-461`):
  `lance.write_dataset(upserts.schema.empty_table(), uri, mode="append", storage_options=...)`. This call is the
  dataset-creation moment (first writer on a routing key). Because the dataset does not yet exist, the append
  path does NOT force-match (`insert.rs:301` only runs for an existing `WriteDestination::Dataset`), so the
  caller's `enable_stable_row_ids` IS honored on this first write. Change: add
  `enable_stable_row_ids=config.enable_stable_row_ids`. Every subsequent merge_insert / append / delete on that
  URI then inherits the flag automatically.
- `ETLConfig` (`etl.py:173-241`): add `enable_stable_row_ids: bool = True` (breaking-change OK; default on).
  Document that it is honored only at bootstrap and that flipping it requires recreating the dataset.
- No change needed in the merge_insert builder or the delete path — both inherit the manifest flag.

### `src/lance_etl/compaction.py` — simplify, the deferral machinery becomes moot

- `CompactionConfig.defer_index_remap` (`compaction.py:122`) and its docstring (`compaction.py:80-84`): on
  stable-row-id datasets this flag has no effect (nothing to defer). Recommended: delete the field, delete the
  `plan_options()` special-casing and warning (`compaction.py:168-187`), and drop `defer_index_remap` from
  `execute_options()` (`compaction.py:160`). If kept for the transition, it is harmless dead config on
  stable-row-id datasets. Either way it stops being a correctness lever.
- Module docstring (`compaction.py:1-36`) and `commit_rewrites` / `compact_one` docstrings
  (`compaction.py:315-334,392-413`): rewrite the "binding hard-codes default options, so remap happens inline"
  narrative — on stable-row-id datasets the tier-B commit no longer remaps; it reserves fragment IDs and updates
  bitmaps cheaply. The re-plan-on-conflict loop, `large_commit_retries`, and the cleanup-horizon floor all STAY.
- No new parameter is set here; compaction reads the flag from the dataset manifest.

### `src/lance_etl/indexing.py` — delete the IVF_RQ remap mitigation; KEEP the orphan guard

(Read-only here; another agent owns this file. Coordinate — list provided for them.)

Delete (becomes dead once remap never runs):
- `VectorIndexHandler.remap_requires_rebuild` (`indexing.py:1166-1187`).
- `VectorIndexHandler.record_coverage` (`indexing.py:1189-1206`) and the base no-op override hook usage
  (`indexing.py:889-899`), plus the `record_coverage` call site in `build_and_commit_segments`
  (`indexing.py:649`).
- The `covered_fragment_ids` manifest field (written in `record_coverage`) and the `remap_requires_rebuild`
  branch inside `VectorIndexHandler.target_fragments` (`indexing.py:1229-1236`).

Keep:
- `growth_requires_retrain` and its `target_fragments` branch (`indexing.py:1149-1164,1224-1228`) — independent
  of remap.
- The fragment-level stale-segment guard: `is_stale_fragment_error` / `STALE_FRAGMENT_MARKERS`
  (`indexing.py:482-511`), the stale-segment drop in `commit_segments` (`indexing.py:553-576`), the re-plan loop
  in `build_and_commit_segments` (`indexing.py:621-659`), and `FtsIndexHandler.commit_index`'s missing-fragment
  check (`indexing.py:1542-1551`). These guard against a concurrent compaction rewriting fragments between a
  segment build and its commit. Compaction still creates new fragments with NEW fragment IDs and removes the old
  ones even with stable row IDs — only the row IDs are stable, not the fragment IDs — so segments built against
  compacted-away fragment IDs are still stale and must still be dropped/replanned. This is the orphan-race guard
  the other agent is adding; it MUST STAY.

### `bench/ingest.py` — inherits the default

- `etl_config(...)` (`bench/ingest.py:55-69`) constructs `ETLConfig`; with the new `enable_stable_row_ids=True`
  default it is on automatically. No change required unless an explicit bench flag is wanted.

### `src/lance_etl/cli.py` — optional flag exposure

- The `etl` subcommand builds `ETLConfig` at `cli.py:210`. Optionally add a `--enable-stable-row-ids /
  --no-stable-row-ids` flag; otherwise the `True` default applies.

### Test fixtures that create datasets (set the flag where compaction correctness is asserted)

- `tests/conftest.py:73-84` `write_fragmented_dataset` (shared fixture) — add `enable_stable_row_ids=True` (or a
  parameter) so index/compaction tests run on stable-row-id datasets.
- `tests/test_compaction_fri.py:22-43` `write_indexed_dataset` — this file specifically pins the NON-stable FRI
  path; either keep it as the explicit non-stable regression or update/retire it alongside the proof test.
- `tests/test_recall_scoring.py:204,287,332`, `tests/test_index_maintenance.py:121`,
  `tests/test_schema_evolution.py:40`, `tests/test_compaction_replan.py:113` — add the flag where compaction is
  exercised.
- The former concurrent-writer stress prototype was retired when PostgreSQL dataset lanes made that race unsupported.
  to its dataset bootstrap.

## 4. What becomes deletable vs. what must stay

Deletable once stable row IDs are on (all datasets recreated):
- The entire IVF_RQ remap-corruption mitigation: `remap_requires_rebuild`, `record_coverage`, the
  `covered_fragment_ids` sidecar field, and the corresponding `target_fragments` branch in `indexing.py`. With no
  remap there is no corruption to detect.
- `CompactionConfig.defer_index_remap` and the `plan_options()` warning/scoping in `compaction.py` — moot on
  stable-row-id datasets. The `__lance_frag_reuse` path is never taken.
- The "per-dataset compact-then-index ordering is MANDATORY" rule in
  `market-research/concurrency-and-coexistence.md:184-193,300-302` downgrades from mandatory to advisory: it was
  mandatory because inline remap corrupted IVF_RQ. With stable row IDs there is no remap, so concurrent
  index-vs-compact no longer risks correctness (it may still be ordered for efficiency).

Must stay (NOT addressed by stable row IDs):
- The fragment-level orphan-race / stale-segment guard in `indexing.py` (see §3). Fragment IDs still change on
  compaction.
- The tier-B re-plan-on-conflict loop, `large_commit_retries`, and the cleanup-horizon floor
  (`compaction.py`) — these are about commit conflicts and cleanup, orthogonal to remap.
- `growth_requires_retrain` IVF centroid retrain trigger.
- `commit_with_retries` string-matching and the general conflict-retry budgets.

## 5. Proof test (the test that would have caught the original bug) + pinned-build result

Design: a Python test in the lance-etl suite (e.g. `tests/test_stable_row_id_compaction.py`) that, on a
stable-row-id dataset, builds all three production index types and asserts search is correct after compaction with
NO index rebuild.

1. Create a dataset with `enable_stable_row_ids=True`, dimension divisible by 8 (IVF_RQ requirement,
   `indexing.py:1132`), enough rows to clear `vector_min_rows`, split into several fragments. Apply some deletes
   so row IDs diverge from row addresses (mirrors `test_stable_row_indices` and `test_enable_stable_row_ids`).
2. Build IVF_RQ (vector), BTREE (scalar), and INVERTED (FTS) indices through the production handlers in
   `indexing.py` (segment API for vector/scalar, `index_uuid`+`merge_index_metadata` for FTS).
3. Capture: index UUID set / `describe_indices()` names, exact top-k vector neighbors for fixed queries, scalar
   filter results, FTS hits, and `count_rows()`.
4. Compact through the production tier-B path (`LanceCompactor.compact_one` / `Compaction.plan` →
   distributed execute → `Compaction.commit`) so the hard-coded-default-options binding is exercised, AND through
   `compact_small_dataset` for the small tier.
5. Assert post-compaction, with NO rebuild call: index UUID set unchanged (or at least no rebuild ran),
   `num_indexed_fragments` covers all live fragments / `num_unindexed_fragments == 0`, and vector/scalar/FTS query
   results byte-identical to the pre-compaction capture. A negative control on a `enable_stable_row_ids=False`
   dataset with IVF_RQ should reproduce the original recall collapse, proving the test has teeth.

Why this catches the original bug: the recorded failure (40/40 → 22/40 top-1 recall after one
`Compaction.execute`, with `num_indexed_fragments`/`num_indexed_rows` still clean) had no maintenance trigger that
could fire (`concurrency-and-coexistence.md:293-302`). Asserting exact search results before/after compaction,
not coverage statistics, is the only assertion that would have caught it.

Pinned-build verification result:
- Stable-row-id compaction keeps indices valid with no rebuild: PROVEN in-tree for BTREE + IVF_PQ via
  `test_stable_row_indices()` (`optimize.rs:2800-2890`) and for row-id stability via Python
  `test_enable_stable_row_ids` (`test_dataset.py:394-425`).
- Distributed `commit_compaction` honors the manifest flag (no remap) regardless of options: PROVEN by branch
  logic at `optimize.rs:1893,1992-2003` and exercised (without indices) by `test_compact_distributed`
  (`optimize.rs:2717-2796`, `use_stable_row_id=true`).
- Gaps to close with our proof test (the reason this is GO, not GO-with-no-work):
  1. IVF_RQ specifically — the in-tree vector coverage is IVF_PQ, not the IVF_RQ our pipeline builds. The
     original corruption was observed on IVF_RQ, so an IVF_RQ assertion is mandatory.
  2. INVERTED/FTS under stable-row-id compaction — not covered in-tree alongside compaction.
  3. The distributed `Compaction.commit` binding combined WITH indices present (in-tree distributed test has no
     indices; in-tree indexed test uses single-process `compact_files`).

## Risks

1. IVF_RQ-specific behavior is unproven in-tree; gated by the proof test above. If IVF_RQ unexpectedly fails
   where IVF_PQ passes, this becomes GO-WITH-CAVEATS (keep the remap mitigation until fixed upstream). Expectation
   from the code path is that it passes, since no remap runs on either index type.
2. Recreation cost: every existing dataset must be recreated to flip the flag (append cannot flip it). Acceptable
   pre-release; must be an explicit fleet step, not an in-place migration.
3. Storage/format: stable row IDs set a manifest reader feature flag (`FLAG_STABLE_ROW_IDS`); older readers that
   don't understand it cannot open the dataset. The Rust search-api uses the same pinned lance crates, so the
   serving path is fine. Verify no external reader predates the flag.
4. The fragment-level orphan guard must not be deleted by mistake during the IVF_RQ-mitigation cleanup — they
   live near each other in `indexing.py` but address different failure modes (§3/§4).
5. `has_address_style` tasks: the cheap stable-row-id branch is `!options.defer_index_remap && !has_address_style`
   (`optimize.rs:1992`). Our distributed rewrite tasks capture `row_addrs` (the comment at `compaction.py:13-14`
   notes the tasks carry the addresses deferral needs). Confirm in the proof test that tier-B stable-row-id
   commits actually take the no-remap branch (assert no `__lance_frag_reuse` index appears and index UUIDs are
   unchanged), since address-style tasks would otherwise route differently.
