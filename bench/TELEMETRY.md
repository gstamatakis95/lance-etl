# Local telemetry capture

This document describes what telemetry is captured, where each file lands, the exact environment
variables that wire each emitter to the local listeners, how to run the Rust search server with
capture enabled, and a self-improvement loop recipe for iterating on pipeline performance.

---

## What gets captured and where

All capture output lands under `{workspace}/telemetry/` (default `bench/workspace/telemetry/`).

| File | Contents | Source |
|---|---|---|
| `metrics.jsonl` | One JSON line per DogStatsD metric datagram received | Python pipeline jobs and Rust search service |
| `traces.jsonl` | One JSON line per OTLP span exported | Rust search service only (see note on Python traces below) |
| `search-api.log` | JSON structured logs with trace correlation | Rust search service stdout (redirect manually or via e2e wiring) |

### Python pipeline traces

The Python side uses `ddtrace` (Datadog APM) which sends traces over the Datadog Agent wire
protocol, not OTLP.  No local `ddtrace` receiver is provided.  The Python side emits DogStatsD
metrics (captured in `metrics.jsonl`) and structured logs (captured to stdout or a file you
redirect).  Lance internal trace events are bridged into DogStatsD counters and gauges through
`attach_lance_event_bridge` and appear in `metrics.jsonl`.

---

## Environment variables

### Python pipeline jobs

The Python `TelemetryConfig` reads these variables (set automatically by
`--capture-telemetry`):

| Variable | Purpose | Default when absent |
|---|---|---|
| `LANCE_BENCH_STATSD_HOST` | DogStatsD destination host | `localhost` |
| `LANCE_BENCH_STATSD_PORT` | DogStatsD destination port | `8125` |

`bench_telemetry_config()` in `bench/spark_session.py` reads both variables.  Because Spark
executors inherit the driver process environment, setting them before `build_spark()` is called
is sufficient to route executor metrics to the local listener.

### Rust search service

| Variable | Purpose | Default when absent |
|---|---|---|
| `SEARCH_API_STATSD_ADDR` | DogStatsD UDP target address (`host:port`) | `127.0.0.1:8125` (or `{DD_AGENT_HOST}:8125`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC endpoint for trace export | Falls back to `http://{DD_AGENT_HOST}:4317` |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Alternative OTLP trace-only endpoint | Same fallback as above |
| `SEARCH_API_TELEMETRY_DISABLED` | Set to `true` to disable all telemetry | `false` |
| `OTEL_SERVICE_NAME` / `DD_SERVICE` | Service name on traces and metrics | `search-api` |
| `DD_ENV` | Environment tag on traces and metrics | unset |
| `DD_VERSION` | Version tag on traces and metrics | unset |
| `RUST_LOG` | Log level filter for JSON stdout logs | `info` |

The capture infrastructure uses two non-standard ports to avoid clashing with a real Datadog
Agent that might be running locally.  The defaults are:

- DogStatsD listener: `127.0.0.1:19125` (flag `--statsd-port`)
- OTLP gRPC receiver: `127.0.0.1:14317` (flag `--otlp-port`)

---

## Running the benchmark with capture

Pass `--capture-telemetry` to any `e2e` invocation.  The listeners start before the first batch
and stop after the last recall sweep.

```bash
python -m bench e2e \
  --dataset synthetic \
  --limit 1000000 \
  --batches 2 \
  --no-text \
  --capture-telemetry \
  --statsd-port 19125 \
  --otlp-port 14317 \
  --workspace bench/workspace \
  --results-root bench/results
```

Output files appear under `bench/workspace/telemetry/` when the run completes.

---

## Running the Rust search server with capture

When `--capture-telemetry` is active the Python e2e orchestrator sets `SEARCH_API_STATSD_ADDR`
and `OTEL_EXPORTER_OTLP_ENDPOINT` in its own process environment.  If you start the Rust server
as a child process or in a separate terminal you must set these variables manually.

### User-managed server (separate terminal)

```bash
export LANCE_ETL_BASE_URI="bench/workspace/lance"
export SEARCH_API_PORT=50051
export SEARCH_API_STATSD_ADDR="127.0.0.1:19125"
export OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:14317"

./rust/search-api/target/release/search-api \
  2>&1 | tee bench/workspace/telemetry/search-api.log
```

The `tee` command writes JSON log lines to `search-api.log` while also printing to the terminal.
If you want logs only in the file (no terminal output) replace `tee` with a redirect:

```bash
./rust/search-api/target/release/search-api \
  >> bench/workspace/telemetry/search-api.log 2>&1
```

### Start the server before the benchmark

Build the binary once:

```bash
cd rust/search-api && cargo build --release && cd ../..
```

Then run the server with capture variables set, capturing its stdout to the telemetry directory.
Start the `--capture-telemetry` benchmark run in a second terminal (the listeners start before
the first batch, so start the server only after the Python run has logged "DogStatsD listener
bound" and "OTLP gRPC receiver bound").

---

## Installing the bench dependency group

`opentelemetry-proto` is required for the OTLP gRPC receiver and is declared in the `bench`
dependency group.  Install it with:

```bash
uv pip install --group bench
```

or equivalently:

```bash
uv pip install opentelemetry-proto grpcio grpcio-tools numpy pandas matplotlib
```

---

## jq recipes

All examples assume the telemetry directory is `bench/workspace/telemetry/`.

### Slowest spans

```bash
jq -r '[.span.name, (.span.end_time_unix_nano - .span.start_time_unix_nano | . / 1e6 | floor | tostring) + "ms"] | @tsv' \
  bench/workspace/telemetry/traces.jsonl \
  | sort -t$'\t' -k2 -rn | head -20
```

### Spans with s3 attributes (query leg object-store stats)

The Rust search service attaches `s3.*` attributes sourced from Lance execution-stats events.
These appear in the span `attributes` array.

```bash
jq 'select(.span.attributes != null)
  | .span
  | {name, attributes: [.attributes[] | select(.key | startswith("s3."))]}
  | select(.attributes | length > 0)' \
  bench/workspace/telemetry/traces.jsonl
```

### Commit retry events (Python side via metrics.jsonl)

```bash
jq 'select(.name == "lance.pipeline.errors" or (.name | startswith("lance.pipeline")))
  | [.received_at, .name, .value, (.tags | join(","))] | @tsv' \
  bench/workspace/telemetry/metrics.jsonl
```

### Cache hit rate (Rust search service)

```bash
jq 'select(.name == "search_api.cache.lookup")
  | {cache: (.tags[] | select(startswith("cache:")) | ltrimstr("cache:")),
     tier: (.tags[] | select(startswith("tier:")) | ltrimstr("tier:")),
     outcome: (.tags[] | select(startswith("outcome:")) | ltrimstr("outcome:")),
     count: .value}' \
  bench/workspace/telemetry/metrics.jsonl \
  | jq -s 'group_by(.cache + ":" + .tier + ":" + .outcome)
    | map({key: .[0].cache + "/" + .[0].tier + "/" + .[0].outcome, value: map(.count) | add})
    | from_entries'
```

### RPC latency distribution by rpc type

```bash
jq 'select(.name == "search_api.rpc.duration_ms")
  | {rpc: (.tags[] | select(startswith("rpc:")) | ltrimstr("rpc:")), ms: .value}' \
  bench/workspace/telemetry/metrics.jsonl \
  | jq -s 'group_by(.rpc)
    | map({rpc: .[0].rpc,
           count: length,
           mean_ms: (map(.ms) | add / length | floor),
           max_ms: (map(.ms) | max | floor)})
    | sort_by(-.mean_ms)'
```

### Lance throttle events

```bash
jq 'select(.name == "search_api.throttle.new_rate" or .name == "search_api.throttle.errors")
  | [(.received_at | todate), .name, .value] | @tsv' \
  bench/workspace/telemetry/metrics.jsonl
```

### Dataset lifecycle events (open/commit/compact)

```bash
jq 'select(.name == "search_api.lance.dataset_events")
  | {event: (.tags[] | select(startswith("event:")) | ltrimstr("event:")), count: .value}' \
  bench/workspace/telemetry/metrics.jsonl \
  | jq -s 'group_by(.event)
    | map({event: .[0].event, total: (map(.count) | add)})'
```

### Prewarm duration by index kind

```bash
jq 'select(.name == "search_api.prewarm.index.duration_ms")
  | {kind: (.tags[] | select(startswith("kind:")) | ltrimstr("kind:")), ms: .value}' \
  bench/workspace/telemetry/metrics.jsonl \
  | jq -s 'group_by(.kind)
    | map({kind: .[0].kind, mean_ms: (map(.ms) | add / length | floor), max_ms: (map(.ms) | max | floor)})'
```

### JSON log lines from the search server containing a trace id

```bash
jq 'select(.trace_id != null) | {timestamp, level, message, trace_id, span_id}' \
  bench/workspace/telemetry/search-api.log | head -40
```

---

## Self-improvement loop

The intended workflow for iterating on pipeline performance:

1. Run the benchmark with capture enabled.  Both the Python pipeline and the Rust service emit
   telemetry to the local listeners for the duration of the run.

2. Read `bench/workspace/telemetry/metrics.jsonl` to identify the most expensive operations.
   Use the cache hit rate recipe to check whether the index and metadata caches are saturated.
   Use the RPC latency recipe to see which search legs are slowest.  Use the Lance IO event
   recipe to count cold index opens and partition loads.

3. Read `bench/workspace/telemetry/traces.jsonl` to drill into individual requests.  The `span`
   field carries the full OTLP span including attributes set by the Rust service.  Slow spans
   appear with large `end_time_unix_nano - start_time_unix_nano` values.

4. Cross-reference slow spans with the JSON log lines in `search-api.log` by `trace_id`.  The
   Rust service writes one JSON log line per event with the `trace_id` and `span_id` fields
   populated when an active OTel span is present.

5. Adjust the relevant knobs (index cache budget, nprobes, refine factor, shard count, compaction
   target rows) in `bench/config.py` flags or the Rust service env vars and re-run.  The
   `bench/results/` directory keeps a separate artifact directory per `run_id` so previous runs
   are never overwritten.

6. Compare `metrics.jsonl` across runs by filtering on metric name and computing aggregate
   statistics with the jq recipes above.  The `e2e.json` artifact in the run directory records
   per-batch ETL, index, and compaction wall times alongside gRPC recall scores.
