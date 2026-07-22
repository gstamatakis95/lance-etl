# `lance_etl.tools`

Operator tools CLI: recall audit, namespace migration, and Iceberg source-table optimization. This
package is deliberately **not** registered as a console script in `pyproject.toml` — the only
installed entry point in this repository is `lance-etl-reconcile`
(`lance_etl.reconciler.cli:main`, see the [package README](../README.md)). Reach these tools with:

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
| `cli.py` | `build_parser` (the `recall`/`migrate-namespace`/`optimize-iceberg` argparse subcommands), one `run_*` dispatcher per subcommand, and `main` |

Everything else the CLI needs is bundled from elsewhere in `lance_etl` rather than reimplemented
here:

- `recall` builds a `RecallJobConfig` and a `DatadogSpanSource`, then runs
  [`lance_etl.recall.RecallAuditJob`](../recall/README.md).
- `migrate-namespace` builds a `MigrateConfig` (and an `IndexJobConfig` when any index-column flag
  is supplied) and runs `lance_etl.migrate_namespace.NamespaceMigrator`.
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

### `migrate-namespace`

Copies every dataset whose namespace path component equals `--source-namespace` to the same address
with the namespace component swapped to `--target-namespace`. Source datasets are never deleted, so
an operator can verify the new namespace and flip serving through the blue-green tag helpers before
removing the source (ADR 0019, see
[rejected-and-operator-tools.md](../../../docs/adr/rejected-and-operator-tools.md)). Targets are
recompacted and reindexed after the copy unless `--no-recompact`/`--no-reindex` is passed, reusing
`MaintenanceJob` and `LanceIndexer` rather than reimplementing compaction or the segment-API index
flows. An existing target dataset fails the run unless `--overwrite-target` is set.

```bash
uv run python -m lance_etl.tools.cli migrate-namespace \
  --source-namespace prod --target-namespace prod-v2 \
  --base-uri s3://lance-etl/datasets --partition-by org_id,tenant_id,namespace
```

### `optimize-iceberg`

Runs Iceberg's own source-table maintenance procedures against the upstream Iceberg table the ETL
reads from — distinct from the `maintenance` package, which optimizes the Lance datasets the ETL
writes to. `rewrite_data_files` and `rewrite_manifests` run by default.
`--expire-snapshots` and the destructive `--remove-orphan-files` are opt-in, and snapshot expiration
runs only after the durable source-retention gate authorizes it.

```bash
uv run python -m lance_etl.tools.cli optimize-iceberg \
  --table local.default.events --expire-snapshots \
  --expire-retain-last 5 --expire-older-than-days 7
```

## Tests

`tests/test_tools_cli_parsing.py` covers `build_parser`'s argparse structure for all three
subcommands and the `cliutil` tag parsers, stopping at `parser.parse_args(...)` — no subcommand
handler runs and no Spark session is built. `tests/test_recall.py` covers the `recall` subcommand's
config wiring against `lance_etl.recall`. `tests/test_migrate_namespace.py` and
`tests/test_iceberg_optimize.py` cover the underlying `NamespaceMigrator` and `IcebergOptimizer`
libraries directly, including local end-to-end Spark runs, rather than going through the CLI layer.
`tests/test_partition_routing.py` covers `parse_partition_cols`.
