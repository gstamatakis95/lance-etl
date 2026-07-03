//! Persistent two-tier caching for Lance datasets: a hybrid index cache, a path-filtered
//! metadata byte cache, a pluggable persistence seam with disk and Redis backends, and the
//! background janitor.
//!
//! This layer plugs into Lance through two seams — [`lance_core::cache::CacheBackend`] for the
//! session index cache and [`lance_io::object_store::WrappingObjectStore`] for the byte cache —
//! and exposes nothing upward except construction, sweeping, and size accounting. It references
//! Lance cache and IO traits but never datasets, queries, or domain types. Beneath both tiers
//! sits one local seam, [`entry_store::EntryStore`], so the persistence backend (local disk or
//! shared Redis) swaps without touching any cache semantics.
//!
//! Submodules:
//! - [`layout`]: versioned stamp naming, key hashing, framing, atomic writes, and the
//!   TTL/budget sweep of the disk backend.
//! - [`entry_store`]: the persistent byte-store trait both tiers compose over.
//! - [`disk_store`]: local-disk [`EntryStore`](entry_store::EntryStore), the default backend.
//! - [`redis_store`]: shared-Redis [`EntryStore`](entry_store::EntryStore) for fleet-wide warm
//!   caches.
//! - [`index_cache`]: hybrid memory + persistent
//!   [`CacheBackend`](lance_core::cache::CacheBackend) for the Lance index cache.
//! - [`store_cache`]: read-through byte cache for immutable metadata reads of wrapped object
//!   stores.
//! - [`janitor`]: periodic sweep loop enforcing TTL and byte budgets over the disk tiers.

pub mod disk_store;
pub mod entry_store;
pub mod index_cache;
pub mod janitor;
pub mod layout;
pub mod redis_store;
pub mod store_cache;
