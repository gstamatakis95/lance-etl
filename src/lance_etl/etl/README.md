# `lance_etl.etl`

Replay-safe Lance mutation and ingestion primitives. This package is a library, not a job. The
production ingestion path is the PostgreSQL-backed reconciler (`lance_etl.reconciler`, primarily
`reconciler/workers.py`), which imports these modules directly inside its executor closures. See
the [package README](../README.md) for the reconciliation cycle these primitives feed and
[AGENTS.md](../AGENTS.md) for the Python rules and pylance API ground truth.

`replay_sink.py` is the only write primitive. It stores a source sequence and event digest per row,
so redelivered Iceberg snapshots converge and equal-sequence conflicts fail closed.

## Modules

| Module | Responsibility |
|---|---|
| `digest.py` | Canonical binary encoding and SHA-256 digests for one mutation (`canonical_event_digest`) and one source-window's terminal state (`canonical_source_digest`) |
| `mutation.py` | Operation normalization (`insert`/`update`/`upsert`/`i`/`u` to `upsert`, `delete`/`d` to `delete`) and per-snapshot terminal-mutation collapse |
| `completion.py` | Monotonic per-dataset completion marker stored in Lance dataset config, with crash-between-commit-and-control-plane reconciliation |
| `arrow.py` | Fixed-size-list vector normalization for executor materialization |
| `storage.py` | Required Lance storage format and strict missing-dataset classification |
| `replay_sink.py` | Source-sequenced, event-digest-verified replay-safe tombstone merge |

## Replay-safe ingestion (`replay_sink.py`)

Every terminal mutation row carries four system columns beyond the payload:
`lance_etl_window_seq`, `lance_etl_source_sequence`, `lance_etl_event_digest` (32-byte SHA-256),
and `is_deleted`. `validate_replay_table` enforces their types and non-nullability, and requires
every tombstone row to null out its payload columns while keeping `ts` populated (`ts` carries the
delete mutation's event time so retention can bound the tombstone's lifetime, see the tombstone
note in [AGENTS.md](../AGENTS.md)).

`replay_safe_merge` is the entry point:

1. `expected_key_states` extracts one `(source_sequence, event_digest)` per key from the incoming
   table and rejects every duplicate key before the first dataset open (`ReplayConflict`). Even
   exact duplicate source rows would otherwise both take the not-matched insert path and create
   duplicate physical keys in a new dataset.
2. Inside `commit_with_retries`, each attempt reopens the dataset, loads the stored
   `(source_sequence, event_digest)` for the affected keys (`load_key_states`, batched in
   `VERIFY_KEY_BATCH`-sized typed Arrow filters), rejects duplicate stored keys, and checks
   `detect_same_sequence_conflicts`: an equal stored sequence with a different digest raises
   `ReplayConflict` rather than silently picking a winner.
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
so the result is partition-independent. Duplicate record ids and negative source sequences fail
closed instead of producing an input-order-dependent digest.

## Mutation collapse (`mutation.py`)

`collapse_snapshot_mutations` reduces one Iceberg snapshot's rows to one `TerminalMutation` per
`(target, record_id)`, keyed by the routing tuple plus `record_id`. Two rows with the same key and
the same digest collapse silently (an exact intra-snapshot duplicate). Two rows with the same key
and different digests raise `MutationConflict` — the snapshot itself is internally ambiguous and
cannot be applied. `materialize_post_image` fills every dataset-declared field from the payload,
setting omitted fields to null, and rejects a payload field outside the declared schema.

## Arrow normalization and the `ts` contract

`apply_fsl_cast` in `arrow.py` enforces each specification-declared vector dimension and converts
values to `fixed_size_list<float32, dim>`. Every mutation carries exactly one canonical timestamp,
`ts`, used uniformly as the event clock, retention clock, and range-query column. There is no
separate ingestion or processing timestamp.

## Tests

`tests/test_replay_sink.py` and `tests/test_replay_sink_faults.py` cover the replay contract
(later-sequence-wins, exact-replay no-op, equal-sequence-different-digest rejection, and
failure-injection around Lance and object-store boundaries) against real local Lance datasets.
`tests/test_completion_marker.py`, `tests/test_mutation_identity.py`, and
`tests/test_arrow_normalization.py` cover the completion marker, digest/collapse identity, and
vector normalization. `tests/test_btree_delta_coexistence.py`, `tests/test_merge_conflict_metric.py`,
and `tests/test_v2_manifest_paths.py` exercise the replay-safe production merge path.
