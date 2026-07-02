# ADR 0030: Streaming k-means bootstrap for IVF_RQ vector indexes

## Status

Accepted. Amends ADR 0029 (the vector build path) and depends on lance ``>=8.0.0``.

## Context

IVF centroid training previously loaded a ``num_partitions x sample_rate`` vector sample into
one executor's heap, which required two memory guards: a training memory budget and a
partition-count cap derived from it. The cap reduced recall exactly where it matters most,
because the largest datasets are the ones whose derived partition counts exceeded the budget.
lance 8.0.0 introduced streaming k-means: incremental training that loads at most
``num_partitions x streaming_sample_rate`` vectors per step, compresses chunks into a weighted
coreset above 256 partitions, and refines with streaming Lloyd passes.

Two constraints were verified against the released library before adoption. The streaming
parameters are exposed only through the committed ``create_index`` path, and the distributed
segment path (``create_index_uncommitted`` with ``fragment_ids``) refuses internal training and
hard-requires precomputed centroids. A committed ``create_index`` accepts an explicit
``rabitq_model`` alongside the streaming parameters, and ``get_ivf_model`` reads the
streaming-trained centroids back afterwards.

## Decision

Vector builds split into two modes at plan time:

- **Bootstrap** (index absent, ``rebuild``, or the artifact triggers: missing config, config
  mismatch, growth past ``retrain_growth_factor``): one task runs a committed ``create_index``
  with the streaming k-means parameters and a freshly minted RaBitQ rotation, then stores the
  artifact config. ``replace=True`` makes a growth retrain a wholesale index replacement.
- **Increment** (committed index with reusable config): unchanged — parallel shard fan-out
  through the segment API, with centroids read back from the committed index on the executor
  and the rotation from the stored config, so every delta stays on one model.

The in-heap trainer, its semaphore, the training memory budget, and the partition-count memory
cap are deleted. The sticky full-rebuild threading across replan rounds is deleted too: the
plan phase re-derives the bootstrap decision from stored state each round.

## Consequences

- Training memory is bounded by the streaming chunk size regardless of partition count, so
  partition counts follow the size policy alone and large datasets regain recall headroom.
- A fresh or retrain build runs as one task instead of a train step plus shard fan-out. The
  steady-state majority (incremental deltas) keeps full shard parallelism.
- The committed ``create_index`` bootstrap is the one sanctioned exception to the
  segment-API-only rule of ADR 0029, and only because it carries an explicit rotation and
  stores the artifact config. Artifact-less plain builds remain forbidden.
- Model coherence across deltas is preserved by construction: the bootstrap's rotation is
  minted by the pipeline and stored, and increments reuse both it and the committed centroids.
