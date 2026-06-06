@AGENTS.md

The canonical agent instructions for this repository live in AGENTS.md above. Read that file in
full before making any change.

The three rules most likely to cause a review failure if missed:

1. No `#` inline comments anywhere in `src/`, `tests/`, or `airflow/` — use docstrings only.
2. Lance indexes are built exclusively via the segment API (`create_index_uncommitted` /
   `merge_existing_index_segments` / `commit_existing_index_segments` for vector/scalar, and the
   `index_uuid` + `merge_index_metadata` path for FTS/INVERTED only). Never use
   `create_scalar_index(fragment_ids=)` for BTREE or BITMAP — it raises on current lance main.
3. No raw SQL strings in the gRPC filter API — use the typed `Filter` AST in `domain/filter.rs`.
   All column names are validated and literals are typed DataFusion `lit` expressions.
