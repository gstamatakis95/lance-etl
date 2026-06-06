//! Lance-backed implementations of the domain traits.
//!
//! This is the only layer that touches Lance datasets, Arrow, and DataFusion types. Everything it
//! exposes upward speaks domain types from [`crate::domain`]. Persistent caching lives in
//! [`crate::cache`] and is wired in through the provider.
//!
//! Submodules:
//! - [`provider`]: target-to-dataset resolution with the shared session and the handle LRU.
//! - [`backend`]: [`SearchBackend`](crate::domain::SearchBackend) over Lance scanners, including
//!   date-range fan-out.
//! - [`filter`]: typed filter AST to DataFusion expression translation.
//! - [`text`]: domain text query tree to Lance FTS query translation.
//! - [`rows`]: Arrow record batch to JSON row conversion.
//! - [`prewarm`]: [`Prewarmer`](crate::domain::Prewarmer) over the Lance prewarm APIs.
//! - [`index_reader`]: IVF centroid extraction and the
//!   [`ClusterReader`](crate::domain::ClusterReader) implementation.
//! - [`error`]: Lance error classification into the domain error type.

pub mod backend;
pub mod error;
pub mod filter;
pub mod index_reader;
pub mod prewarm;
pub mod provider;
pub mod rows;
pub mod text;

pub use backend::LanceSearchBackend;
pub use provider::{CachingDatasetProvider, DatasetProvider};
