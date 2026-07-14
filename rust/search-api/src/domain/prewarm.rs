//! Cache prewarming: domain types and the trait transports call to warm an org's caches.

use std::time::Duration;

use crate::domain::catalog::ServingRoute;
use crate::domain::error::SearchError;
use crate::domain::target::{DatasetRef, DatasetTarget};

/// What a prewarm call should pull into the local caches.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrewarmSpec {
    /// Warm dataset metadata (manifest, transaction, index listing). Implied by warming any index,
    /// since opening the dataset warms its metadata. When false and no indexes are requested, the
    /// prewarm is a no-op that never opens the dataset.
    pub metadata: bool,
    /// Warm all indexes of the dataset. Ignored when `index_names` is non-empty.
    pub all_indexes: bool,
    /// Warm only these named indexes.
    pub index_names: Vec<String>,
    /// Also pull FTS position data for inverted indexes (needed for phrase queries).
    pub fts_with_position: bool,
}

impl PrewarmSpec {
    /// Returns true when the spec asks for any index warming at all.
    pub fn wants_indexes(&self) -> bool {
        self.all_indexes || !self.index_names.is_empty()
    }

    /// Returns true when the spec asks for no work at all.
    ///
    /// Opening the dataset always warms its metadata as a side effect, so `metadata = false` with
    /// no index targets is the only spec that warrants skipping the dataset open entirely. The
    /// prewarm implementation short-circuits such a spec into an empty report.
    pub fn is_noop(&self) -> bool {
        !self.metadata && !self.wants_indexes()
    }

    /// Resolves the index names to warm against the names available in the dataset.
    ///
    /// Explicit `index_names` win (deduplicated, original order preserved, unknown names kept so
    /// the caller can report a per-index error). Otherwise `all_indexes` selects every available
    /// name. Otherwise nothing is warmed.
    pub fn resolve_targets(&self, available: &[String]) -> Vec<String> {
        let source: &[String] = if !self.index_names.is_empty() {
            &self.index_names
        } else if self.all_indexes {
            available
        } else {
            return Vec::new();
        };
        let mut seen = std::collections::HashSet::new();
        source
            .iter()
            .filter(|name| seen.insert(name.as_str()))
            .cloned()
            .collect()
    }
}

/// Outcome of prewarming one index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrewarmedIndex {
    /// Index name as listed in the dataset manifest.
    pub name: String,
    /// Time spent prewarming this index (all delta segments).
    pub duration: Duration,
    /// `None` on success. A client-safe reason when this index was skipped or failed.
    pub error: Option<String>,
}

/// Outcome of one prewarm call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrewarmReport {
    /// True when dataset metadata was loaded into the cache.
    pub metadata_warmed: bool,
    /// Per-index outcomes in completion order.
    pub indexes: Vec<PrewarmedIndex>,
    /// Time spent opening the dataset and loading metadata.
    pub metadata_duration: Duration,
    /// Wall-clock time for the whole call.
    pub total_duration: Duration,
    /// Approximate bytes resident in the shared index cache after the call.
    pub index_cache_size_bytes: u64,
    /// The concrete committed version id that was warmed. With a tag target this is the version
    /// the tag resolved to at warm time, so a caller can confirm the green version is warm before
    /// flipping the serve tag onto it.
    pub resolved_version: u64,
}

/// Cache prewarming abstraction. Transports stay generic over this trait next to `SearchBackend`.
pub trait Prewarmer: Send + Sync + 'static {
    /// Warms the targeted dataset's caches and reports what was loaded.
    ///
    /// `reference` selects which committed version to warm: [`DatasetRef::Latest`] (the default
    /// for clients that send no version), an explicit [`DatasetRef::Version`], or a
    /// [`DatasetRef::Tag`] resolved at warm time. Pinning a version is what lets an operator warm
    /// a green build into every replica before flipping the serve tag onto it.
    fn prewarm(
        &self,
        target: &DatasetTarget,
        spec: PrewarmSpec,
        reference: DatasetRef,
    ) -> impl Future<Output = Result<PrewarmReport, SearchError>> + Send;
}

/// Authenticated internal prewarm contract over one explicit immutable serving route.
pub trait ExactPrewarmer: Send + Sync + 'static {
    /// Warms metadata and every committed user index for exactly `route` on the local process.
    fn prewarm_exact(
        &self,
        target: &DatasetTarget,
        route: ServingRoute,
    ) -> impl Future<Output = Result<PrewarmReport, SearchError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a `Vec<String>` from string literals.
    fn names(items: &[&str]) -> Vec<String> {
        items.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn explicit_names_win_and_are_deduplicated_in_order() {
        let spec = PrewarmSpec {
            all_indexes: true,
            index_names: names(&["b", "a", "b", "missing"]),
            ..Default::default()
        };
        assert_eq!(
            spec.resolve_targets(&names(&["a", "b", "c"])),
            names(&["b", "a", "missing"])
        );
    }

    #[test]
    fn all_indexes_selects_every_available_name() {
        let spec = PrewarmSpec {
            all_indexes: true,
            ..Default::default()
        };
        assert_eq!(spec.resolve_targets(&names(&["a", "b", "a"])), names(&["a", "b"]));
    }

    #[test]
    fn metadata_only_spec_selects_nothing() {
        let spec = PrewarmSpec {
            metadata: true,
            ..Default::default()
        };
        assert!(!spec.wants_indexes());
        assert!(spec.resolve_targets(&names(&["a"])).is_empty());
        assert!(!spec.is_noop());
    }

    #[test]
    fn empty_spec_is_a_noop() {
        let spec = PrewarmSpec::default();
        assert!(spec.is_noop());
    }

    #[test]
    fn index_spec_without_metadata_is_not_a_noop() {
        let spec = PrewarmSpec {
            metadata: false,
            index_names: names(&["a"]),
            ..Default::default()
        };
        assert!(!spec.is_noop());
    }
}
