//! Persistent two-tier caching for Lance datasets: a disk-backed index cache, a path-filtered
//! metadata byte cache, their shared on-disk layout, and the background janitor.
//!
//! This layer plugs into Lance through two seams — [`lance_core::cache::CacheBackend`] for the
//! session index cache and [`lance_io::object_store::WrappingObjectStore`] for the byte cache —
//! and exposes nothing upward except construction, sweeping, and size accounting. It references
//! Lance cache and IO traits but never datasets, queries, or domain types.
//!
//! Submodules:
//! - [`layout`]: versioned stamp directory, key hashing, atomic writes, and the TTL/budget sweep.
//! - [`disk_cache`]: hybrid disk + memory [`CacheBackend`](lance_core::cache::CacheBackend) for
//!   the Lance index cache.
//! - [`store_cache`]: read-through disk cache for immutable metadata reads of wrapped object
//!   stores.
//! - [`janitor`]: periodic sweep loop enforcing TTL and byte budgets over both tiers.

pub mod disk_cache;
pub mod janitor;
pub mod layout;
pub mod store_cache;
