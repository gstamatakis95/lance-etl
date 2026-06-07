# 0007. Disk-backed index/metadata cache and the Prewarm RPC

Status: Accepted

## Context

The service serves up to 30k datasets. Cold opens pay object-store round trips for manifests, index metadata,
and index data. Lance ships only an in-memory Moka cache backend, no disk backend. We want index data and all
metadata (manifests, transactions, index files) cached on local disk at a configurable path, with raw table
data excluded.

## Decision

Inject a hybrid disk-plus-memory `CacheBackend` into the shared Lance `Session` index cache, backed by a
versioned cache directory (default `/tmp/rust-search/cache`) with TTL and LRU sweeping by a janitor. Add a
metadata byte cache as a `WrappingObjectStore` that caches versioned manifests, transactions, and small index
files only, with raw `data/` reads always passing through. Add a Prewarm RPC that opens a dataset through the
shared session (warming manifest and metadata) and calls `load_indices` plus per-index `prewarm_index`, so a
caller can warm an org before traffic arrives.

## Consequences

Cache keys are URI plus index-UUID plus version-manifest-path, so entries are version-correct. Raw data is
provably excluded by the path classifier. One shared session safely spans all datasets because keys are
URI-scoped. The metadata cache must never cache the latest-version pointer (`_latest.manifest` in V1, the hint
file in V2) or a flip would be invisible. The Prewarm-versus-serving-version interaction is subtle and is
addressed separately by [0013](0013-blue-green-serving.md): warming the live version then flipping a serve tag
to a freshly built version leaves the new version cold, so Prewarm must target an explicit version before the
flip. Datadog requires a retention filter for the recall sampling spans described in
[0008](0008-observability-and-recall-audit.md).
