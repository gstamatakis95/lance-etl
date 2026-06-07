# 0013. Tag-based blue/green serving

Status: Proposed (not yet implemented)

## Context

Operators want an O(1) serving cutover between dataset versions: build a new (green) version offline, then flip
serving to it atomically, with a clean rollback. Lance tags (`tags.update`) provide the atomic pointer. The
subtlety is the interaction with the disk cache and Prewarm ([0007](0007-disk-cache-and-prewarm.md)): cache
entries are keyed by version, so warming the live (blue) version and then flipping a `prod` tag to green leaves
green cold, and a provider that caches the tag-to-version resolution too long serves stale blue after a flip.

## Decision

Maintain a `prod` tag updated via `tags.update` for O(1) cutover. The correct operational sequence is build
green, prewarm green by explicit version, then flip the tag, never flip then warm. To make that safe:

- The Prewarm RPC must accept an explicit version or a specific tag (not just the live latest) so green can be
  warmed before the flip, and must return the resolved version.
- The provider must resolve a serve tag to a concrete version, key the open-dataset-handle LRU and the caches on
  the resolved version (not the tag string), and bound the tag-to-version resolution with a short TTL so a flip
  is observed promptly without per-request manifest reads.
- The byte cache must never-cache the latest-version pointer for both V1 and V2 layouts.
- Telemetry must make a flip-without-prewarm observable (served version not equal to the most-recently-prewarmed
  version).
- The Python tagging helper only writes the tag and logs the safe sequence. It never assumes the serving layer
  auto-refreshes.
- Cleanup must not delete a tagged version (`error_if_tagged_old_versions`), and green must be tagged before any
  cleanup runs.

## Consequences

Status is Proposed because the Rust serving-side implementation is not done: an agent started the additive proto
fields and provider config but was interrupted by a spend limit, and the incomplete declarations were backed out
to keep the crate compiling. The full design is in `market-research/prewarm-blue-green-plan.md` and the
implementation is tracked as a pending task. The proto changes are additive and non-breaking, so the bench
client keeps working. Stable row IDs would have improved cross-flip recall comparison but are unrelated to the
cutover mechanism and were rejected separately ([0010](0010-stable-row-ids-rejected.md)).
