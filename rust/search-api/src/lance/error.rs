//! Classification of Lance errors into the domain error type.

use crate::domain::SearchError;

/// Generic client-facing message for an unclassified Lance failure.
///
/// Raw Lance errors can carry object-store paths, schema detail, or other private state, so only
/// a closed error class is logged and clients receive this bounded message.
const GENERIC_INTERNAL_MESSAGE: &str = "an internal error occurred while executing the request";

/// Client-facing message for a missing dataset.
///
/// Lance's `DatasetNotFound` display includes the full dataset URI (bucket, org, tenant paths),
/// which must never reach either clients or normal telemetry.
const DATASET_NOT_FOUND_MESSAGE: &str = "Dataset not found";

/// Client-facing message for a missing non-dataset resource.
///
/// Lance's generic `NotFound` display also embeds the object URI, so it gets the same
/// bounded-class logging treatment as `DatasetNotFound`.
const RESOURCE_NOT_FOUND_MESSAGE: &str = "a required resource was not found";

/// Client-facing message for an engine-internal timeout.
const ENGINE_TIMEOUT_MESSAGE: &str = "the storage engine timed out executing the request";

/// Returns whether a failed dataset open proves the selected dataset, reference, or version is
/// absent rather than exposing a transient missing object inside an otherwise live dataset.
pub fn is_definitive_open_absence(err: &lance::Error) -> bool {
    matches!(
        err,
        lance::Error::DatasetNotFound { .. } | lance::Error::RefNotFound { .. } | lance::Error::VersionNotFound { .. }
    )
}

/// Classifies a Lance error by reference into the closest domain error.
///
/// Not-found conditions map to [`SearchError::NotFound`]: `DatasetNotFound` and `NotFound` carry
/// object-store URIs in their display form, so only their closed class is logged and they are
/// returned with a sanitized message, while `RefNotFound` and `VersionNotFound` messages echo the
/// client-supplied tag or version and are forwarded as-is. `InvalidInput`/`IndexNotFound` carry
/// client-safe detail and are forwarded as [`SearchError::InvalidArgument`]. `Timeout` becomes a
/// retriable [`SearchError::Unavailable`] with a generic message. Every other Lance error is
/// logged by closed class and mapped to a generic
/// [`SearchError::Internal`] message, so internal engine detail never reaches a client through
/// the gRPC status.
pub fn classify_lance_error(err: &lance::Error) -> SearchError {
    match err {
        lance::Error::DatasetNotFound { .. } => {
            tracing::warn!(error_class = "dataset_not_found", "lance request failed");
            SearchError::not_found(DATASET_NOT_FOUND_MESSAGE)
        }
        lance::Error::NotFound { .. } => {
            tracing::warn!(error_class = "resource_not_found", "lance request failed");
            SearchError::not_found(RESOURCE_NOT_FOUND_MESSAGE)
        }
        lance::Error::RefNotFound { .. } | lance::Error::VersionNotFound { .. } => {
            SearchError::not_found(err.to_string())
        }
        lance::Error::InvalidInput { .. } | lance::Error::IndexNotFound { .. } => {
            SearchError::invalid_argument(err.to_string())
        }
        lance::Error::Timeout { .. } => {
            tracing::warn!(error_class = "timeout", "lance request failed");
            SearchError::unavailable(ENGINE_TIMEOUT_MESSAGE)
        }
        _ => {
            tracing::error!(error_class = "internal", "lance request failed");
            SearchError::internal(GENERIC_INTERNAL_MESSAGE)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    use super::*;

    /// Cloneable writer collecting one subscriber's formatted events.
    #[derive(Clone)]
    struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for CaptureWriter {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

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
    fn only_dataset_reference_and_version_absence_are_definitive_open_misses() {
        let dataset = lance::Error::dataset_not_found("memory://missing", "no manifest".into());
        let reference = lance::Error::RefNotFound {
            message: "tag HEAD does not exist".to_string(),
        };
        let version = lance::Error::VersionNotFound {
            message: "version 17 does not exist".to_string(),
        };
        let transient_object = lance::Error::not_found("memory://live/_versions/17.manifest");
        assert!(is_definitive_open_absence(&dataset));
        assert!(is_definitive_open_absence(&reference));
        assert!(is_definitive_open_absence(&version));
        assert!(!is_definitive_open_absence(&transient_object));
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

    #[test]
    fn emitted_logs_never_contain_raw_uri_or_engine_detail() {
        let tracing_guard = crate::telemetry::TRACING_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let writer = CaptureWriter(bytes.clone());
        let subscriber = tracing_subscriber::fmt().with_writer(move || writer.clone()).finish();
        tracing::subscriber::with_default(subscriber, || {
            tracing::callsite::rebuild_interest_cache();
            classify_lance_error(&lance::Error::dataset_not_found(
                "s3://secret-bucket/private-target.lance",
                "private detail".into(),
            ));
            classify_lance_error(&lance::Error::internal(
                "corrupt s3://secret-bucket/private-target.lance",
            ));
        });
        let output = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(output.contains("dataset_not_found"));
        assert!(output.contains("error_class=\"internal\""));
        assert!(!output.contains("secret-bucket"));
        assert!(!output.contains("private detail"));
        drop(tracing_guard);
    }
}
