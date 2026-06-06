//! Lance-backed implementations of the domain traits.
//!
//! This is the only layer that touches Lance, Arrow, and DataFusion types; everything it exposes
//! upward speaks domain types from [`crate::domain`].

pub mod backend;
pub mod cache_layout;
pub mod disk_cache;
pub mod error;
pub mod filter;
pub mod index_reader;
pub mod janitor;
pub mod prewarm;
pub mod provider;
pub mod rows;
pub mod store_cache;
pub mod text;

pub use backend::LanceSearchBackend;
pub use disk_cache::DiskIndexCacheBackend;
pub use janitor::CacheJanitor;
pub use provider::{CachingDatasetProvider, DatasetProvider};
pub use store_cache::MetadataByteCache;
