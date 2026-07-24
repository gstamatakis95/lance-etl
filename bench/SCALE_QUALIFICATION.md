# Scale and failure qualification

Run the bounded local cohort without downloading a corpus:

```bash
python -m bench qualify \
  --qualification-rows 25000 \
  --workspace /tmp/lance-etl-qualification/workspace \
  --results-root /tmp/lance-etl-qualification/results \
  --run-id local-qualification
```

The run writes `capacity.json` and `qualify.json`. The qualification artifact records the exact
commit, pylance version, host capacity, cache state, deterministic cohort digest, planted scenario
counts, collapse materialization, measured peak Python allocation, routing shuffle width, and the
internal width cap. The cohort has a hot target and a long tail. It includes exact redeliveries,
late event time in newer processing-hour partitions, deletes followed by recreation, 32 payload
fields, and periodic 8 KiB text values.

The local command rejects more than one million synthetic base rows unless
`--allow-large-qualification` is present. That flag permits the allocation. It does not turn a
synthetic run into a BIGANN qualification or satisfy an external scale gate.

Real PostgreSQL queue contention is an integration test against a disposable schema:

```bash
LANCE_ETL_TEST_DATABASE_URL=postgresql+psycopg://user:password@host/database \
.venv/bin/pytest tests/test_postgres_queue_load.py -m integration -q
```

The default load is 256 target lanes with eight concurrent drainers. Set
`LANCE_ETL_QUEUE_LOAD_TARGETS` for an approved larger run. The test rejects values above 50,000.

Every qualification artifact keeps 100M and 1B as `NOT_RUN`. Each gate records its command,
resource floors, exact local shortfall, and required evidence. A claim changes only after the real
BIGANN command completes on declared infrastructure and its capacity, checksum, phase, and cold
cache artifacts are retained.
