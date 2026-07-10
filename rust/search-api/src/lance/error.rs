//! Classification of Lance errors into the domain error type.

use crate::domain::SearchError;

/// Generic client-facing message for an unclassified Lance failure.
///
/// The raw Lance error (which can carry object-store paths, schema detail, or other internal
/// state) is logged server-side via `tracing::error!` at the call site instead of being forwarded
/// to the client, so a caller never sees more than "an internal error occurred."
const GENERIC_INTERNAL_MESSAGE: &str = "an internal error occurred while executing the request";

/// Classifies a Lance error by reference into the closest domain error.
///
/// `DatasetNotFound`/`NotFound` and `InvalidInput`/`IndexNotFound` carry client-safe detail and are
/// forwarded as-is. Every other Lance error is logged in full server-side (`tracing::error!`) and
/// mapped to a generic [`SearchError::Internal`] message, so internal engine detail never reaches
/// a client through the gRPC status.
pub fn classify_lance_error(err: &lance::Error) -> SearchError {
    match err {
        lance::Error::DatasetNotFound { .. } | lance::Error::NotFound { .. } => SearchError::not_found(err.to_string()),
        lance::Error::InvalidInput { .. } | lance::Error::IndexNotFound { .. } => {
            SearchError::invalid_argument(err.to_string())
        }
        other => {
            tracing::error!(error = %other, "unclassified lance error");
            SearchError::internal(GENERIC_INTERNAL_MESSAGE)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unclassified_errors_are_generic_and_do_not_leak_internal_detail() {
        let raw = lance::Error::internal("corrupt file at /secret/internal/path.lance");
        let err = classify_lance_error(&raw);
        match err {
            SearchError::Internal(message) => {
                assert_eq!(message, GENERIC_INTERNAL_MESSAGE);
                assert!(!message.contains("/secret/internal/path.lance"));
            }
            other => panic!("expected an internal error, got {other:?}"),
        }
    }

    #[test]
    fn invalid_input_is_forwarded_with_detail() {
        let raw = lance::Error::invalid_input("bad filter");
        let err = classify_lance_error(&raw);
        assert!(matches!(err, SearchError::InvalidArgument(_)));
    }
}
