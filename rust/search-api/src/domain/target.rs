//! Dataset addressing: the target every request names.

use crate::domain::error::SearchError;

/// Selects which committed version of a dataset to open.
///
/// `Serve` resolves the fixed production `HEAD` tag. The other variants pin an explicit version,
/// opt out of production serving to open `Latest`, or name a tag to resolve. Pinning to a
/// concrete version id (directly or via tag resolution) is what lets blue and green versions of
/// one dataset coexist in the handle cache and lets prewarm warm the exact version that will be
/// served.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum DatasetRef {
    /// Resolve the fixed production `HEAD` tag. Used by serving.
    #[default]
    Serve,
    /// The latest committed version, ignoring `HEAD`.
    Latest,
    /// A specific committed version id.
    Version(u64),
    /// A tag resolved to its committed version id at open time.
    Tag(String),
}

/// Addresses the dataset a request operates on.
///
/// The target names the single dataset at `{base}/{org_id}/{tenant_id}/{namespace}.lance`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatasetTarget {
    /// Organization id. Must match `[A-Za-z0-9_-]+`.
    pub org_id: String,
    /// Tenant id. Must match `[A-Za-z0-9_-]+`.
    pub tenant_id: String,
    /// Namespace. Must match `[A-Za-z0-9_-]+`.
    pub namespace: String,
}

impl DatasetTarget {
    /// Builds a target, for tests and embedded callers.
    pub fn new(org_id: impl Into<String>, tenant_id: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            org_id: org_id.into(),
            tenant_id: tenant_id.into(),
            namespace: namespace.into(),
        }
    }

    /// Validates every path segment of the target.
    pub fn validate(&self) -> Result<(), SearchError> {
        validate_path_segment(&self.org_id, "org_id")?;
        validate_path_segment(&self.tenant_id, "tenant_id")?;
        validate_path_segment(&self.namespace, "namespace")
    }
}

/// Rejects path segments that are empty or contain characters outside `[A-Za-z0-9_-]`.
pub fn validate_path_segment(value: &str, field: &str) -> Result<(), SearchError> {
    let valid = !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if valid {
        Ok(())
    } else {
        Err(SearchError::invalid_argument(format!(
            "{field} must be non-empty and match [A-Za-z0-9_-]+"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_validation_rejects_traversal_and_empties() {
        for bad in ["", "../escape", "a/b", "a b", "a.b"] {
            assert!(validate_path_segment(bad, "org_id").is_err(), "accepted {bad:?}");
        }
        assert!(validate_path_segment("org-1_A", "org_id").is_ok());
        let mut target = DatasetTarget::new("org1", "tenant1", "ns1");
        assert!(target.validate().is_ok());
        target.namespace = "../x".to_string();
        assert!(target.validate().is_err());
    }
}
