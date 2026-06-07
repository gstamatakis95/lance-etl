@AGENTS.md

The canonical agent instructions for this repository live in AGENTS.md above. Read that file in
full before making any change.

The four rules most likely to cause a review failure if missed:

1. No `#` inline comments anywhere in `src/`, `tests/`, or `airflow/` — use docstrings only.
   No leading underscores on any defined name in those directories either. Note that `__version__`
   was removed from `src/lance_etl/__init__.py` for exactly this reason.
2. Lance indexes are built exclusively via the segment API (`create_index_uncommitted` /
   `merge_existing_index_segments` / `commit_existing_index_segments` for vector/scalar, and the
   `index_uuid` + `merge_index_metadata` path for FTS/INVERTED only). Never use
   `create_scalar_index(fragment_ids=)` for BTREE or BITMAP — it raises on current lance main.
3. No raw SQL strings in the gRPC filter API — use the typed `Filter` AST in `domain/filter.rs`.
   All column names are validated and literals are typed DataFusion `lit` expressions.
4. No stable row IDs anywhere. `enable_stable_row_ids` was evaluated and rejected (silent data
   corruption on release builds with concurrent merge + compaction). See
   `docs/adr/0010-stable-row-ids-rejected.md`. Do not offer it as an option.
