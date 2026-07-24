# `lance_etl.tools`

Operator tools CLI: recall audit and Iceberg source-table optimization. This package is deliberately
**not** registered as a console script in `pyproject.toml`. The only
installed entry point in this repository is `lance-etl-reconcile`
(`lance_etl.reconciler.launcher:main`, see the [package README](../README.md)). Reach these tools with:

```bash
uv run python -m lance_etl.tools.cli --help
```

`cli.py` carries a `if __name__ == "__main__": raise SystemExit(main())` guard, so
`python -m lance_etl.tools.cli` is a complete, working entry point without any packaging changes.
`main()` is also importable directly (`from lance_etl.tools.cli import main`) for tests or scripts
that want to drive it in-process.

## Module

| Module | Responsibility |
|---|---|
| `cli.py` | `build_parser` for the `recall` and `optimize-iceberg` subcommands, their dispatchers, and `main` |

Everything else the CLI needs is bundled from elsewhere in `lance_etl` rather than reimplemented
here:

- `recall` builds a `RecallJobConfig` and a `DatadogSpanSource`, then runs
  [`lance_etl.recall.RecallAuditJob`](../recall/README.md).
- `optimize-iceberg` builds an `IcebergOptimizeConfig` and runs
  `lance_etl.iceberg_optimize.IcebergOptimizer`.

Argument parsing, Spark session construction, storage-option and telemetry-config parsing, and the
shared exit-code convention all come from `lance_etl.cliutil` (`add_common_arguments`, `build_spark`,
`build_telemetry_config`, `parse_storage_options`, `run_with_spark`, `run_cli_main`), the same
helpers the reconciler CLI uses.

## Subcommands

### `recall`

Replays Datadog-sampled queries as exact brute-force/BM25 scans against the dataset versions that
served them and reports recall@k, nDCG@k, and MRR. See
[`lance_etl.recall`](../recall/README.md) for the full replay pipeline.

```bash
uv run python -m lance_etl.tools.cli recall \
  --from 2026-07-20T00:00:00 --to 2026-07-21T00:00:00 \
  --base-uri s3://lance-etl/datasets \
  --dd-site datadoghq.com --max-samples 10000 --vector-column vector
```

`DD_API_KEY` and `DD_APP_KEY` must be set in the environment. `DatadogSpanSource` raises
`ValueError` immediately if either is missing.

### `optimize-iceberg`

Runs Iceberg's own source-table maintenance procedures against the upstream Iceberg table the ETL
reads from — distinct from the `maintenance` package, which optimizes the Lance datasets the ETL
writes to. `rewrite_data_files` and `rewrite_manifests` run by default.
The destructive `--remove-orphan-files` is opt-in. Snapshot expiration is deliberately not exposed
by this operator command. Only the PostgreSQL-backed reconciler can prove the oldest exact source
snapshot unfinished work still needs, so age-only expiration would violate the durable retention
gate.

```bash
uv run python -m lance_etl.tools.cli optimize-iceberg \
  --table local.default.events
```

## Tests

`tests/test_tools_cli_parsing.py` covers both subcommands and the `cliutil` tag parsers, stopping at
`parser.parse_args(...)`. No subcommand handler runs and no Spark session is built.
`tests/test_recall.py` covers the recall configuration. `tests/test_iceberg_optimize.py` covers the
optimizer directly, including a local end-to-end Spark run.
