//! The single domain error type shared by every layer below the transport.

use std::fmt;

/// Domain error classifying every failure the search core can produce.
///
/// The transport layer owns the mapping onto wire status codes. The domain only records the
/// failure class and a client-safe message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchError {
    /// The request is malformed or references unknown columns, operators, or parameters.
    InvalidArgument(String),
    /// The requested dataset (or an entity within it) does not exist.
    NotFound(String),
    /// A transient engine or storage condition (e.g. an internal timeout) that a client may
    /// safely retry with backoff.
    Unavailable(String),
    /// Any other failure inside the engine.
    Internal(String),
}

impl SearchError {
    /// Builds an `InvalidArgument` error.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument(message.into())
    }

    /// Builds a `NotFound` error.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    /// Builds an `Unavailable` error.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable(message.into())
    }

    /// Builds an `Internal` error.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }

    /// Returns the client-facing message.
    pub fn message(&self) -> &str {
        match self {
            Self::InvalidArgument(message)
            | Self::NotFound(message)
            | Self::Unavailable(message)
            | Self::Internal(message) => message,
        }
    }
}

impl fmt::Display for SearchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument(message) => write!(f, "invalid argument: {message}"),
            Self::NotFound(message) => write!(f, "not found: {message}"),
            Self::Unavailable(message) => write!(f, "unavailable: {message}"),
            Self::Internal(message) => write!(f, "internal: {message}"),
        }
    }
}

impl std::error::Error for SearchError {}
