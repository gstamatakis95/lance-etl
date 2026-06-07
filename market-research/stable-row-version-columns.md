# Stable-row-ID version columns: verification against lance main @ 466405f47

## 1. Existence and exact names

Two pseudo-columns exist. Their canonical string names, defined at
`rust/lance-core/src/lib.rs:27-29`, are:

- `_row_created_at_version`
- `_row_last_updated_at_version`

Both are declared as `ArrowField::new(<name>, DataType::UInt64, true)` (lines 44-48 of the same
file), meaning nullable `uint64`. They are listed as system columns alongside `_rowid`,
`_rowaddr`, and `_rowoffset` in `is_system_column()` (line 59-63).

They are **computed-on-read** (virtual). No file on disk stores a column with that name; instead,
per-row version sequences are stored as fragment-level metadata (`created_at_version_meta` /
`last_updated_at_version_meta` on each `Fragment`) and re-materialised when a scanner or take
call requests them. The projection layer in
`rust/lance-datafusion/src/projection.rs:96-99` intercepts the string names and sets
`with_row_created_at_version` / `with_row_last_updated_at_version` flags on the physical
projection rather than resolving them as physical fields.

## 2. Type and semantics

**Type: monotonic `uint64` version counter, not a wall-clock timestamp.**

Each value is the lance dataset version number at which the event occurred. Version numbers are
integers that start at 1 and increment by 1 with every committed write. They carry no intrinsic
time information.

**Mapping version number to wall-clock time** is possible but indirect. `dataset.versions()` in
`python/python/lance/dataset.py:2756-2769` returns a list of dicts; each dict has a `"timestamp"`
key which is a Python `datetime` object (derived from the manifest's nanosecond-precision
`timestamp()` field, `rust/lance/src/dataset.rs:213`). Given a version integer `v` from
`_row_created_at_version`, one must fetch `dataset.versions()`, find the entry where
`entry["version"] == v`, and read its `"timestamp"`. This is an O(number of versions) call, not
a per-row attribute.

**Stability across compaction.** Both columns are preserved correctly through compaction.
`recalc_versions_for_rewritten_fragments` in `rust/lance/src/dataset/optimize.rs:1778-1874`
loads the per-row version sequences from the old fragments, masks out deleted rows, rechunks the
sequences to align with new fragment boundaries, and writes them back. `created_at_version` is
therefore stable through compaction (the original creation version is kept intact). Equally,
`last_updated_at_version` does **not** change on a compaction-only rewrite because the rewritten
fragments inherit the existing `last_updated_at_version` runs from the old fragments; the
compaction commit version is not stamped onto unchanged rows.

**Correctness for updates.** `resolve_update_version_metadata` in
`rust/lance/src/dataset/transaction.rs:87-216` preserves `created_at` from the source fragment
for any row that already existed (the "UPDATE branch") and stamps it at `new_version` only for
newly inserted rows (the "INSERT branch"). `last_updated_at_version_meta` is always set to
`new_version` for modified fragments.

## 3. Availability condition

The version metadata is only populated when the dataset was created with
`enable_stable_row_ids=True`. The manifest flag is `FLAG_STABLE_ROW_IDS = 2` (bit 1) in
`rust/lance-table/src/feature_flags.rs:14`. The transaction logic checks
`manifest.uses_stable_row_ids()` before assigning row IDs and version metadata
(`rust/lance/src/dataset/transaction.rs:1875-1884`).

However, the **columns can be projected on any dataset regardless of the flag.** When no version
metadata exists (i.e., `last_updated_at_version_meta` / `created_at_version_meta` are `None` on
the fragment), the stream layer defaults all values to `1u64` (see
`rust/lance-table/src/utils/stream.rs:355-357` and `372-374`: "Default to version 1 if sequence
not provided"). This means projecting `_row_created_at_version` on a non-stable-row-id dataset
returns `1` for every row, which is meaningless but does not error. Filtering on these columns also
works syntactically but produces useless results.

The Python test at `python/python/tests/test_dataset.py:765-802` confirms this: it writes a
dataset without `enable_stable_row_ids` yet projects `_row_created_at_version` and
`_row_last_updated_at_version`, asserting they all equal `1`.

Semantically meaningful values require `enable_stable_row_ids=True` at dataset creation time;
that is not the current default.

## 4. gRPC filterability through the search-api domain layer

**Syntax: allowed.** The `is_plain_identifier` function in
`rust/search-api/src/lance/filter.rs:106-113` accepts names matching `[A-Za-z_][A-Za-z0-9_]*`.
The leading underscore is explicitly listed as a valid first character (`first == '_'`), so
`_row_created_at_version` and `_row_last_updated_at_version` both pass the syntactic check.

**Schema check: blocked in the current implementation.** `schema_columns` in
`rust/search-api/src/lance/backend.rs:336-338` builds the allowlist from
`dataset.schema().fields`, which is the physical manifest schema (`rust/lance/src/dataset.rs:2158-2160`).
System columns (`_rowid`, `_row_created_at_version`, etc.) are computed-on-read and are **not**
part of the manifest schema; they do not appear in `dataset.schema().fields`. Consequently
`allowed_columns.contains("_row_created_at_version")` returns `false`, and the `column_ref`
function at `rust/search-api/src/lance/filter.rs:97-100` rejects the column with
`SearchError::InvalidArgument("unknown filter column")`.

In other words: the identifier regex would accept the names, but the schema allowlist check
rejects them because system columns are invisible to `dataset.schema()`. Filtering by
`_row_last_updated_at_version > N` through the search-api gRPC path is **blocked** without a
code change to the allowlist (e.g., explicitly adding known system column names to
`schema_columns`, or checking `lance_core::is_system_column` before rejecting).

## 5. Verdict on redundancy with `_ingested_at`

The claim that these columns would make a manually-stamped `_ingested_at` redundant is **no**
for the following reasons:

**a) Wall-clock ingestion time is not directly recoverable.**
`_row_created_at_version` stores a version integer, not a timestamp. Recovering a wall-clock time
requires a separate `dataset.versions()` round-trip, mapping the integer to a manifest timestamp.
That timestamp is the commit time of the version, which is close to but not identical to when
Spark stamped `F.current_timestamp()`. The `_ingested_at` column stores the Spark driver's
`current_timestamp()` at the moment the Spark job ran, which may differ from the manifest commit
timestamp (e.g., due to retry latency, compaction lag, or clock skew). The two values are not
equivalent.

**b) It does not work for incremental/last-updated semantics without stable row IDs.**
`last_updated_at_version` contains meaningful per-row values only when
`enable_stable_row_ids=True`. This flag is not the current default and must be set at dataset
creation time — it cannot be added retroactively to existing datasets. Any dataset written with
the current ETL (`etl.py`) without that flag stores `None` for all version metadata, and the
columns return the meaningless default `1` for all rows.

**c) gRPC filterability is absent in the current search-api implementation.**
System columns are not in `dataset.schema()`, so `schema_columns()` returns a set that does not
include them, and `filter_to_expr` rejects any reference. A `_row_last_updated_at_version > N`
filter through the gRPC API currently returns `SearchError::InvalidArgument`. Enabling this would
require a targeted code change to the allowlist logic.

**Summary table:**

| Requirement | `_ingested_at` (TimestampType, physical) | `_row_created_at_version` (uint64, virtual) |
|---|---|---|
| Wall-clock time, directly | Yes | No — requires `versions()` lookup |
| Available without stable row IDs | Yes | Syntactically yes; semantically no (always `1`) |
| Filterable via gRPC search-api today | Yes (physical column in schema) | No (not in `dataset.schema()`, allowlist rejects) |
| Stable across compaction | Yes (value does not change) | Yes (`recalc_versions` preserves it) |
| Incremental delta semantics | Yes | Yes — only if `enable_stable_row_ids=True` |

**Verdict:** The version columns are a useful complement to `_ingested_at` once stable row IDs are
universally enabled, but they cannot replace it today. The missing pieces are: (1) not the default,
(2) wall-clock time requires a separate lookup, (3) the gRPC filter path does not expose system
columns. Dropping `_ingested_at` would lose the only directly queryable, always-present,
wall-clock ingestion timestamp. The correct long-term posture is to enable stable row IDs by
default on new datasets and update `schema_columns` in the search-api to include named system
columns, at which point `_row_created_at_version` can replace `_ingested_at` for incremental-ETL
bookkeeping — but wall-clock conversion would still require a `versions()` lookup or a separate
materialized timestamp column.
