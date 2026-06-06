//! Lance-backed implementations of the domain traits.
//!
//! This is the only layer that touches Lance, Arrow, and DataFusion types; everything it exposes
//! upward speaks domain types from [`crate::domain`].

pub mod backend;
pub mod error;
pub mod filter;
pub mod provider;
pub mod rows;
pub mod text;

pub use backend::LanceSearchBackend;
pub use provider::{CachingDatasetProvider, DatasetProvider};
