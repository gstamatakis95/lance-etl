# Architecture decisions

The decisions behind lance-etl are consolidated into seven thematic documents. Inside each
document, every still-relevant decision keeps its original ADR number as a section heading, so
a reference like "ADR 0030" anywhere in the code or docs resolves through the table below.
Superseded decisions are one-line notes in their home document.

The seven documents:

| Document | Covers |
|---|---|
| [etl-and-data-model.md](etl-and-data-model.md) | Iceberg reads, routing, the event-time clock, the dynamic map pivot, exact source identities |
| [fleet-orchestration-and-maintenance.md](fleet-orchestration-and-maintenance.md) | Local PostgreSQL reconciliation, historical workflow evolution, task-based execution, retention, coexistence, Iceberg upkeep |
| [indexing.md](indexing.md) | Segment-API flows, vector artifact storage, role auto-indexing, the streaming k-means bootstrap, the object-store centroid cache |
| [serving-filters-and-tags.md](serving-filters-and-tags.md) | Crate layering, the typed filter AST, time ranges, blue-green serving, per-query version pinning |
| [caching-and-observability.md](caching-and-observability.md) | The persistent cache and its pluggable backends, Prewarm, the Lance trace bridge, recall auditing |
| [rejected-and-operator-tools.md](rejected-and-operator-tools.md) | The stable-row-id rejection, V2 manifest paths, knob reduction, and retired operator paths |
| [postgresql-dataset-control-plane.md](postgresql-dataset-control-plane.md) | Exact 9-table control plane, immutable dataset specifications, durable work, immutable publications, and the local runtime |

Index of every original ADR number:

| ADR | Title | Home | Status |
|---|---|---|---|
| 0001 | Distributed indexing via the Lance segment API | [indexing.md](indexing.md) | Accepted (vector training portion superseded by 0030) |
| 0002 | Two-tier compaction orchestration | [fleet-orchestration-and-maintenance.md](fleet-orchestration-and-maintenance.md) | Superseded by 0028 |
| 0003 | Incremental Iceberg reads via snapshot-id bounds | [etl-and-data-model.md](etl-and-data-model.md) | Accepted |
| 0004 | Dataset routing and duplicate semantics | [etl-and-data-model.md](etl-and-data-model.md) | Accepted |
| 0005 | Rust gRPC service layering and the typed filter AST | [serving-filters-and-tags.md](serving-filters-and-tags.md) | Accepted |
| 0006 | Date-range fan-out search with dedup-keep-best | [serving-filters-and-tags.md](serving-filters-and-tags.md) | Superseded by 0014 |
| 0007 | Persistent index/metadata cache and the Prewarm RPC | [caching-and-observability.md](caching-and-observability.md) | Accepted |
| 0008 | Datadog observability and recall auditing | [caching-and-observability.md](caching-and-observability.md) | Accepted |
| 0009 | Stale index-plan safety | [fleet-orchestration-and-maintenance.md](fleet-orchestration-and-maintenance.md) | Accepted |
| 0010 | Move-stable row IDs | [rejected-and-operator-tools.md](rejected-and-operator-tools.md) | Rejected |
| 0011 | The `_ingested_at` ingestion-timestamp column | [etl-and-data-model.md](etl-and-data-model.md) | Superseded by 0016 |
| 0012 | V2 manifest paths fleet-wide | [rejected-and-operator-tools.md](rejected-and-operator-tools.md) | Accepted |
| 0013 | Tag-based blue-green serving | [serving-filters-and-tags.md](serving-filters-and-tags.md) | Serving authority superseded by 0042, tags retained as mirrors |
| 0014 | One dataset per target, time queries as scalar filters | [serving-filters-and-tags.md](serving-filters-and-tags.md) | Accepted |
| 0015 | CLI and config knob reduction | [rejected-and-operator-tools.md](rejected-and-operator-tools.md) | Accepted |
| 0016 | Event-time canonical clock | [etl-and-data-model.md](etl-and-data-model.md) | Accepted |
| 0017 | Rust intake service with a pluggable record sink | [rejected-and-operator-tools.md](rejected-and-operator-tools.md) | Superseded |
| 0018 | Retention-window expiry inside maintenance | [fleet-orchestration-and-maintenance.md](fleet-orchestration-and-maintenance.md) | Accepted |
| 0019 | Namespace copy/migrate utility | [rejected-and-operator-tools.md](rejected-and-operator-tools.md) | Superseded |
| 0020 | Static declared-field map pivot | [etl-and-data-model.md](etl-and-data-model.md) | Superseded by 0024 |
| 0021 | Event-time range on the search RPCs | [serving-filters-and-tags.md](serving-filters-and-tags.md) | Accepted |
| 0022 | Lance trace-event bridge | [caching-and-observability.md](caching-and-observability.md) | Accepted |
| 0023 | Iceberg source-table optimization job | [fleet-orchestration-and-maintenance.md](fleet-orchestration-and-maintenance.md) | Accepted |
| 0024 | Dynamic per-dataset map pivot | [etl-and-data-model.md](etl-and-data-model.md) | Accepted |
| 0025 | Sidecar-free vector artifacts | [indexing.md](indexing.md) | Superseded by 0040 |
| 0026 | Job isolation: separate packages and CLIs | [fleet-orchestration-and-maintenance.md](fleet-orchestration-and-maintenance.md) | Superseded for runtime invocation |
| 0027 | Unified pipeline: prune, maintenance, index, stamp | [fleet-orchestration-and-maintenance.md](fleet-orchestration-and-maintenance.md) | Superseded by local reconciliation |
| 0028 | Unified task-based fleet orchestration, format 2.1, column roles | [fleet-orchestration-and-maintenance.md](fleet-orchestration-and-maintenance.md) | Execution model accepted, control-plane schema superseded by 0042 |
| 0029 | Every index builds distributed through segments | [indexing.md](indexing.md) | Accepted |
| 0030 | Streaming k-means bootstrap for IVF_RQ | [indexing.md](indexing.md) | Accepted |
| 0031 | Pluggable cache backend (disk, redis, memory) | [caching-and-observability.md](caching-and-observability.md) | Accepted |
| 0032 | Hourly interval tags and per-query tag pinning | [etl-and-data-model.md](etl-and-data-model.md) + [serving-filters-and-tags.md](serving-filters-and-tags.md) | Hour-tag source identity superseded |
| 0033 | ZONEMAP scalar indexes via merged segment commits | [indexing.md](indexing.md) | Accepted |
| 0034 | Adaptive salted routing, streaming merge, and spark_batches removal | [etl-and-data-model.md](etl-and-data-model.md) | Accepted |
| 0035 | Per-dataset failure isolation, partial-failure exit codes, and stamp gating | [fleet-orchestration-and-maintenance.md](fleet-orchestration-and-maintenance.md) | Isolation accepted, scheduler behavior superseded |
| 0036 | Idle-dataset cleanup rotation | [fleet-orchestration-and-maintenance.md](fleet-orchestration-and-maintenance.md) | Accepted |
| 0037 | Cluster-aware parallelism and discoverable partition counts | [fleet-orchestration-and-maintenance.md](fleet-orchestration-and-maintenance.md) | Accepted |
| 0038 | Ingestion and the maintenance/indexing pipeline must not overlap per dataset | [fleet-orchestration-and-maintenance.md](fleet-orchestration-and-maintenance.md) | Superseded by PostgreSQL dataset lanes |
| 0039 | Commit-retry and idempotency hardening for bootstrap and tag operations | [fleet-orchestration-and-maintenance.md](fleet-orchestration-and-maintenance.md) | Accepted |
| 0040 | Object-store centroid cache for distributed vector builds | [indexing.md](indexing.md) | Accepted (supersedes 0025) |
| 0041 | Clustered rewrite: centroid-locality dataset reorganization | [fleet-orchestration-and-maintenance.md](fleet-orchestration-and-maintenance.md) | Accepted |
| 0042 | First-class PostgreSQL dataset control plane | [postgresql-dataset-control-plane.md](postgresql-dataset-control-plane.md) | Accepted, exact 9-table local design |
