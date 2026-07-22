# `lance_etl.etl`

Shared Lance mutation and ingestion primitives. This package is a library, not a job: the
production ingestion path is the PostgreSQL-backed reconciler (`lance_etl.reconciler`, primarily
`reconciler/workers.py`), which imports these modules directly inside its executor closures. See
the [package README](../README.md) for the reconciliation cycle these primitives feed and
[AGENTS.md](../AGENTS.md) for the Python rules and pylance API ground truth.

Two ingestion primitives coexist in this package, and which one a caller reaches for depends on
what durability contract it needs:

- `replay_sink.py` is the one the reconciler uses. It stores a source sequence and an event digest
  per row so a redelivered Iceberg snapshot converges to the same state instead of merely
  overwriting on timestamp.
- `sink.py` (`apply_merge`, `ETLConfig`) is the older whole-table upsert/delete merge sink, keyed
  only on `record_id` with a `source.ts >= target.ts` last-write-wins guard. It has no source
  sequence or digest, so it cannot detect an equal-sequence conflicting redelivery. It remains a
  fully tested library primitive, exercised directly by the concurrency and chunking test suites
  (`tests/test_etl_concurrency.py`, `tests/test_etl_streaming_merge.py`,
  `tests/test_merge_conflict_metric.py`, `tests/test_btree_delta_coexistence.py`,
  `tests/test_v2_manifest_paths.py`) and by `bench/qualification.py`, which reuses `ETLConfig` for
  its deterministic shuffle-width and mutation-collapse scale-gate evidence. It is not wired into
  the reconciler workers.

## Modules

| Module | Responsibility |
|---|---|
| `digest.py` | Canonical binary encoding and SHA-256 digests for one mutation (`canonical_event_digest`) and one source-window's terminal state (`canonical_source_digest`) |
| `mutation.py` | Operation normalization (`insert`/`update`/`upsert`/`i`/`u` to `upsert`, `delete`/`d` to `delete`) and per-snapshot terminal-mutation collapse |
| `completion.py` | Monotonic per-dataset completion marker stored in Lance dataset config, with crash-between-commit-and-control-plane reconciliation |
| `pivot.py` | Map-column pivot to concrete columns, fixed-size-list vector casts, routing-key grouping, and `ETLConfig` |
| `replay_sink.py` | Source-sequenced, event-digest-verified replay-safe tombstone merge — the reconciler's ingestion primitive |
| `sink.py` | Executor-side content-routed idempotent Lance merge sink (`apply_merge`), keyed on `record_id` and `ts` only |

## Replay-safe ingestion (`replay_sink.py`)

Every terminal mutation row carries four system columns beyond the payload:
`lance_etl_window_seq`, `lance_etl_source_sequence`, `lance_etl_event_digest` (32-byte SHA-256),
and `is_deleted`. `validate_replay_table` enforces their types and non-nullability, and requires
every tombstone row to null out its payload columns while keeping `ts` populated (`ts` carries the
delete mutation's event time so retention can bound the tombstone's lifetime, see the tombstone
note in [AGENTS.md](../AGENTS.md)).

`replay_safe_merge` is the entry point:

1. `expected_key_states` extracts one `(source_sequence, event_digest)` per key from the incoming
   table, rejecting a table that itself carries inconsistent duplicate keys for the same key
   (`ReplayConflict`).
2. Inside `commit_with_retries`, each attempt reopens the dataset, loads the stored
   `(source_sequence, event_digest)` for the affected keys (`load_key_states`, batched in
   `VERIFY_KEY_BATCH`-sized `IN (...)` filters), and checks `detect_same_sequence_conflicts`: an
   equal stored sequence with a different digest raises `ReplayConflict` rather than silently
   picking a winner.
3. `requires_replay_merge` short-circuits to a no-op (counted as `dataset.replay_noop`) when every
   incoming state is already stored at an equal-or-greater sequence, which makes an exact replay
   free.
4. Otherwise the merge runs as a single `merge_insert` with
   `when_matched_update_all(condition=replay_update_condition())` plus
   `when_not_matched_insert_all()`. The condition is
   `target.lance_etl_source_sequence < source.lance_etl_source_sequence` — deliberately strict
   (not `<=`), so a later exact duplicate still advances the stored sequence and an expired worker
   replaying an intermediate snapshot cannot regress state after a duplicate window has already
   completed.
5. After the commit, `verify_reconciled_states` reopens the dataset and confirms every incoming key
   is present at a sequence greater than or equal to what was submitted, with an equal sequence
   requiring an identical digest. Any violation raises `ReplayConflict`.

Together this gives three replay properties: a later sequence always wins, an exact redelivery is
a no-op, and an equal sequence with different content is rejected rather than silently applied.
`replay_table_chunks` splits an oversized terminal table by both row count and approximate byte
budget without reordering rows, for callers that need to commit a window in pieces.

## Completion marker (`completion.py`)

`finalize_completion_marker` records `(window_seq, source_digest)` in the dataset's config KV
(`LAST_APPLIED_WINDOW_KEY`, `LAST_APPLIED_DIGEST_KEY`) after a source window's Lance writes are
durable, so a process that crashes between the Lance commit and the PostgreSQL transition can
determine on restart whether the write actually landed. `completion_is_desired` treats an
already-stored equal-or-greater window as proof of durability, and raises `CompletionConflict` if
the same window sequence is associated with a different digest, mirroring the replay-sink
conflict rule at the marker level. The commit goes through `commit_with_retries`. If every retry
raises, the function re-reads the dataset once more before propagating — an ambiguous commit that
actually landed is still recognized as durable (`dataset.completion_ambiguous_success`) rather than
retried or reported as a failure.

## Digests (`digest.py`)

`canonical_event_digest` hashes routing, `record_id`, normalized operation, `ts`, and payload with
a length-prefixed, sorted-key binary encoding (`encode_value`, `encode_mapping`, `encode_sequence`)
so the digest is independent of dict iteration order and of any delivery metadata (Iceberg
sequence, snapshot id, file, work id) — an exact redelivery of the same logical mutation always
hashes identically. `canonical_source_digest` folds a whole source window's terminal
`(record_id, source_sequence, event_digest)` triples into one digest, sorted by encoded record id
so the result is partition-independent.

## Mutation collapse (`mutation.py`)

`collapse_snapshot_mutations` reduces one Iceberg snapshot's rows to one `TerminalMutation` per
`(target, record_id)`, keyed by the routing tuple plus `record_id`. Two rows with the same key and
the same digest collapse silently (an exact intra-snapshot duplicate). Two rows with the same key
and different digests raise `MutationConflict` — the snapshot itself is internally ambiguous and
cannot be applied. `materialize_post_image` fills every dataset-declared field from the payload,
setting omitted fields to null, and rejects a payload field outside the declared schema.

## Pivot and cast (`pivot.py`)

`pivot_map_columns` expands the `vectors`, `texts`, and `metadata` map columns into concrete
per-key columns for one dataset group, recording each new column's role (`vector`/`text`/`scalar`)
for the sink to persist into the dataset's `lance-etl.columns` config. `apply_fsl_cast` casts a
vector column to `fixed_size_list<float32, dim>`, inferring `dim` from the first non-null value or
using a caller-supplied dimension (the latter path exists so every parallel bulk-append task casts
to the one dimension derived once on the driver). `enforce_map_key_bound` fails a run loudly when a
source map column emits more distinct keys than `ETLConfig.max_keys_per_map`, protecting against a
runaway grow-only schema. `group_run_starts` and `stream_routing_groups` provide `O(rows)` routing-
key grouping over Arrow batches already sorted by the routing columns, bounding executor memory to
one flush's worth of rows rather than a whole partition. `tests/conftest.py` keeps a
non-streaming equivalence oracle (`group_by_routing`) purely as a test fixture, never a production
symbol.

## The `ts_col` contract

Every mutation carries exactly one canonical timestamp, `ts` (`ETLConfig.ts_col` defaults to
`"ts"`), used uniformly as the collapse ordering key, the retention clock, and the range-query
column. There is no separate ingestion or processing timestamp.

## Tests

`tests/test_replay_sink.py` and `tests/test_replay_sink_faults.py` cover the replay contract
(later-sequence-wins, exact-replay no-op, equal-sequence-different-digest rejection, and
failure-injection around Lance and object-store boundaries) against real local Lance datasets.
`tests/test_completion_marker.py`, `tests/test_mutation_identity.py`, and
`tests/test_etl_streaming_merge.py` cover the completion marker, digest/collapse identity, and the
streaming router respectively as pure-Python/PyArrow unit tests with no Spark or Lance dependency
where possible. `tests/test_etl_concurrency.py`, `tests/test_btree_delta_coexistence.py`,
`tests/test_merge_conflict_metric.py`, `tests/test_v2_manifest_paths.py`, and
`tests/test_scale_qualification.py` (in `bench/`) exercise `sink.py`'s `apply_merge` path,
including a concurrent merge-plus-compaction stress test that asserts no data is lost.
