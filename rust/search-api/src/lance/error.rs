//! Classification of Lance errors into the domain error type.

use crate::domain::SearchError;

/// Generic client-facing message for an unclassified Lance failure.
///
/// The raw Lance error (which can carry object-store paths, schema detail, or other internal
/// state) is logged server-side via `tracing::error!` at the call site instead of being forwarded
/// to the client, so a caller never sees more than "an internal error occurred."
const GENERIC_INTERNAL_MESSAGE: &str = "an internal error occurred while executing the request";

/// Client-facing message for a missing dataset.
///
/// Lance's `DatasetNotFound` display includes the full dataset URI (bucket, org, tenant paths),
/// which must never reach a client. The full error is logged server-side instead, and the RPC
/// failure log already carries the dataset target triplet for correlation.
const DATASET_NOT_FOUND_MESSAGE: &str = "Dataset not found";

/// Client-facing message for a missing non-dataset resource.
///
/// Lance's generic `NotFound` display also embeds the object URI, so it gets the same
/// log-full-return-sanitized treatment as `DatasetNotFound`.
const RESOURCE_NOT_FOUND_MESSAGE: &str = "a required resource was not found";

/// Client-facing message for an engine-internal timeout.
const ENGINE_TIMEOUT_MESSAGE: &str = "the storage engine timed out executing the request";

/// Classifies a Lance error by reference into the closest domain error.
///
/// Not-found conditions map to [`SearchError::NotFound`]: `DatasetNotFound` and `NotFound` carry
/// object-store URIs in their display form, so they are logged in full server-side and returned
/// with a sanitized message, while `RefNotFound` and `VersionNotFound` messages only echo the
/// client-supplied tag or version and are forwarded as-is. `InvalidInput`/`IndexNotFound` carry
/// client-safe detail and are forwarded as [`SearchError::InvalidArgument`]. `Timeout` becomes a
/// retriable [`SearchError::Unavailable`] with a generic message. Every other Lance error is
/// logged in full server-side (`tracing::error!`) and mapped to a generic
/// [`SearchError::Internal`] message, so internal engine detail never reaches a client through
/// the gRPC status.
pub fn classify_lance_error(err: &lance::Error) -> SearchError {
    match err {
        lance::Error::DatasetNotFound { .. } => {
            tracing::warn!(error = %err, "dataset not found");
            SearchError::not_found(DATASET_NOT_FOUND_MESSAGE)
        }
        lance::Error::NotFound { .. } => {
            tracing::warn!(error = %err, "resource not found");
            SearchError::not_found(RESOURCE_NOT_FOUND_MESSAGE)
        }
        lance::Error::RefNotFound { .. } | lance::Error::VersionNotFound { .. } => {
            SearchError::not_found(err.to_string())
        }
        lance::Error::InvalidInput { .. } | lance::Error::IndexNotFound { .. } => {
            SearchError::invalid_argument(err.to_string())
        }
        lance::Error::Timeout { .. } => {
            tracing::warn!(error = %err, "lance operation timed out");
            SearchError::unavailable(ENGINE_TIMEOUT_MESSAGE)
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

    #[test]
    fn dataset_not_found_is_not_found_with_a_sanitized_message() {
        let raw = lance::Error::dataset_not_found("s3://secret-bucket/org-42/tenant-7/ns.lance", "no manifest".into());
        let err = classify_lance_error(&raw);
        match err {
            SearchError::NotFound(message) => {
                assert_eq!(message, DATASET_NOT_FOUND_MESSAGE);
                assert!(!message.contains("secret-bucket"), "URIs must never reach the client");
            }
            other => panic!("expected not found, got {other:?}"),
        }
    }

    #[test]
    fn generic_not_found_is_not_found_with_a_sanitized_message() {
        let raw = lance::Error::not_found("s3://secret-bucket/org-42/ns.lance/_versions/3.manifest");
        let err = classify_lance_error(&raw);
        match err {
            SearchError::NotFound(message) => {
                assert_eq!(message, RESOURCE_NOT_FOUND_MESSAGE);
                assert!(!message.contains("secret-bucket"));
            }
            other => panic!("expected not found, got {other:?}"),
        }
    }

    #[test]
    fn ref_and_version_not_found_are_not_found_with_client_safe_detail() {
        let raw = lance::Error::RefNotFound {
            message: "tag 20260611T120000Z does not exist".to_string(),
        };
        let err = classify_lance_error(&raw);
        match err {
            SearchError::NotFound(message) => assert!(message.contains("20260611T120000Z")),
            other => panic!("expected not found, got {other:?}"),
        }

        let raw = lance::Error::VersionNotFound {
            message: "version 17 does not exist".to_string(),
        };
        let err = classify_lance_error(&raw);
        match err {
            SearchError::NotFound(message) => assert!(message.contains("17")),
            other => panic!("expected not found, got {other:?}"),
        }
    }

    #[test]
    fn timeout_is_a_retriable_unavailable_with_a_generic_message() {
        let raw = lance::Error::timeout("read of s3://secret-bucket/part-3 exceeded 120s");
        let err = classify_lance_error(&raw);
        match err {
            SearchError::Unavailable(message) => {
                assert_eq!(message, ENGINE_TIMEOUT_MESSAGE);
                assert!(!message.contains("secret-bucket"));
            }
            other => panic!("expected unavailable, got {other:?}"),
        }
    }
}
