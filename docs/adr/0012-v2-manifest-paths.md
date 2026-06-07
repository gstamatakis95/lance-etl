# 0012. V2 manifest paths fleet-wide

Status: Accepted

## Context

V1 names a manifest `_versions/{version}.manifest`, so locating the latest version costs a directory LIST that
grows with the version count. Across 30k datasets opened repeatedly by the search service, that is a large and
avoidable object-store cost.

## Decision

Create every dataset with V2 manifest paths (`enable_v2_manifest_paths`, default True). V2 names the manifest
so the latest version sorts first and is found with a single head or list, turning every dataset open into one
object-store request regardless of history depth. The flag is honored only at dataset bootstrap. Existing
datasets migrate one-shot through `migrate_dataset_manifest_paths`.

## Consequences

This is a creation-time naming choice with no concurrency or correctness caveat, unlike stable row IDs, so it
defaults on. The only documented caveat is that a V2 dataset is unreadable by Lance older than 0.17.0, which the
pinned build is well past. The Rust byte cache must explicitly never-cache the V2 latest-version hint file (not
just V1 `_latest.manifest`), tracked as part of [0013](0013-blue-green-serving.md), or a stale latest-version
pointer could be served.
