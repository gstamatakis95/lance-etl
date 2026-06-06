//! Classification of Lance errors into the domain error type.

use crate::domain::SearchError;

/// Classifies a Lance error by reference into the closest domain error.
pub fn classify_lance_error(err: &lance::Error) -> SearchError {
    match err {
        lance::Error::DatasetNotFound { .. } | lance::Error::NotFound { .. } => SearchError::not_found(err.to_string()),
        lance::Error::InvalidInput { .. } | lance::Error::IndexNotFound { .. } => {
            SearchError::invalid_argument(err.to_string())
        }
        other => SearchError::internal(other.to_string()),
    }
}
