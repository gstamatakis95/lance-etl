//! Record intake: domain types and the sink seam transports hand validated batches to.
//!
//! Nothing here references protobuf, tonic, or Lance. The transport ([`crate::grpc`]) validates
//! protobuf requests into the [`IntakeBatch`] type and hands the batch to a [`RecordSink`]. The
//! sink is the only place a write destination appears, so swapping the destination (stdout now,
//! Kafka later) touches nothing else.

use std::collections::BTreeMap;
use std::fmt;

use crate::domain::target::DatasetTarget;

/// The single domain error type for the intake path.
///
/// The transport layer owns the mapping onto wire status codes. The domain only records the
/// failure class and a client-safe message, mirroring [`crate::domain::error::SearchError`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntakeError {
    /// The batch is malformed (bad target, missing id, an empty named vector, a fully empty
    /// upsert, ...).
    InvalidArgument(String),
    /// The sink could not accept the batch right now and the caller may retry.
    Unavailable(String),
    /// Any other failure inside the sink.
    Internal(String),
}

impl IntakeError {
    /// Builds an `InvalidArgument` error.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument(message.into())
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
            Self::InvalidArgument(message) | Self::Unavailable(message) | Self::Internal(message) => message,
        }
    }
}

impl fmt::Display for IntakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument(message) => write!(f, "invalid argument: {message}"),
            Self::Unavailable(message) => write!(f, "unavailable: {message}"),
            Self::Internal(message) => write!(f, "internal: {message}"),
        }
    }
}

impl std::error::Error for IntakeError {}

/// The kind of mutation applied to a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationOp {
    /// Create the record if absent, otherwise replace it.
    Upsert,
    /// Delete the record by id.
    Delete,
}

impl MutationOp {
    /// Low-cardinality tag value for this operation.
    pub fn as_tag(self) -> &'static str {
        match self {
            Self::Upsert => "upsert",
            Self::Delete => "delete",
        }
    }
}

/// One record to create, update, or (by id) delete.
///
/// The shape mirrors the ETL and search data model: a string id, an event-time clock, a metadata
/// map, zero or more named fixed-dimension vectors, and zero or more named text fields. The
/// addressing (org, tenant, namespace) lives on the [`DatasetTarget`] of the enclosing
/// [`IntakeBatch`] and is never duplicated here.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    /// Client-assigned vector id. Transported verbatim and never interpreted as an expression.
    pub id: String,
    /// Source event timestamp in epoch milliseconds. The canonical ETL clock; there is no separate
    /// ingestion timestamp.
    pub event_timestamp_ms: i64,
    /// Record metadata keyed by name. Lands as an Arrow `Map<Utf8, Utf8>` column downstream. Keys
    /// and values are transported verbatim and never interpreted. May be empty.
    pub metadata: BTreeMap<String, String>,
    /// Named vector columns, each a fixed-length float array keyed by column name. Optional: may be
    /// empty on an upsert. When an entry is present its values must be non-empty. Empty (and
    /// ignored) for a delete.
    pub vectors: BTreeMap<String, Vec<f32>>,
    /// Named text fields keyed by text column name, mirroring the search side's full-text
    /// convention: a field under key `body` lands in the `body` column, the same column a
    /// `TextSearch` queries by naming it. Values are transported verbatim. May be empty.
    pub texts: BTreeMap<String, String>,
}

/// One validated mutation: an operation paired with the record it acts on.
///
/// An [`MutationOp::Upsert`] carries the full record. A [`MutationOp::Delete`] reads only the id of
/// the record, so its other fields are left at their defaults.
#[derive(Debug, Clone, PartialEq)]
pub enum Mutation {
    /// Create or replace the record.
    Upsert(Record),
    /// Delete the record with this id.
    Delete {
        /// Id of the record to delete.
        id: String,
    },
}

impl Mutation {
    /// The operation kind of this mutation.
    pub fn op(&self) -> MutationOp {
        match self {
            Self::Upsert(_) => MutationOp::Upsert,
            Self::Delete { .. } => MutationOp::Delete,
        }
    }

    /// The id of the record this mutation acts on.
    pub fn id(&self) -> &str {
        match self {
            Self::Upsert(record) => &record.id,
            Self::Delete { id } => id,
        }
    }
}

/// A batch of mutations addressed to a single dataset, ready for a sink to accept.
#[derive(Debug, Clone, PartialEq)]
pub struct IntakeBatch {
    /// Dataset the mutations belong to.
    pub target: DatasetTarget,
    /// Mutations to apply, in client order.
    pub mutations: Vec<Mutation>,
}

impl IntakeBatch {
    /// Builds a batch from a target and its mutations.
    pub fn new(target: DatasetTarget, mutations: Vec<Mutation>) -> Self {
        Self { target, mutations }
    }

    /// Number of upserts in the batch.
    pub fn upsert_count(&self) -> u64 {
        self.mutations
            .iter()
            .filter(|mutation| mutation.op() == MutationOp::Upsert)
            .count() as u64
    }

    /// Number of deletes in the batch.
    pub fn delete_count(&self) -> u64 {
        self.mutations
            .iter()
            .filter(|mutation| mutation.op() == MutationOp::Delete)
            .count() as u64
    }
}

/// One rejected mutation, carrying the offending record id and a client-safe reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntakeItemError {
    /// Record id the error refers to. Empty when the id itself was missing.
    pub id: String,
    /// Client-safe reason the mutation was rejected.
    pub message: String,
}

/// Outcome of accepting one (or several, when aggregated) intake batch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IntakeReport {
    /// Number of mutations accepted by the sink.
    pub accepted: u64,
    /// Number of mutations rejected by validation or the sink.
    pub rejected: u64,
    /// Per-item errors for the rejected mutations, in encounter order.
    pub errors: Vec<IntakeItemError>,
}

impl IntakeReport {
    /// Records one accepted mutation.
    pub fn accept(&mut self) {
        self.accepted += 1;
    }

    /// Records one rejected mutation with its id and reason.
    pub fn reject(&mut self, id: impl Into<String>, message: impl Into<String>) {
        self.rejected += 1;
        self.errors.push(IntakeItemError {
            id: id.into(),
            message: message.into(),
        });
    }

    /// Folds another report into this one, summing counts and concatenating errors.
    pub fn merge(&mut self, other: IntakeReport) {
        self.accepted += other.accepted;
        self.rejected += other.rejected;
        self.errors.extend(other.errors);
    }
}

/// The write seam: a destination any intake batch is handed to.
///
/// Implementations receive an already-validated [`IntakeBatch`] and report what they accepted.
/// The transport stays generic over this trait and never names a concrete destination. This is the
/// extension point: [`StdoutSink`] is the placeholder, and a future `KafkaSink` drops in here as a
/// second implementation without touching the proto, the transport, or the domain types.
pub trait RecordSink: Send + Sync + 'static {
    /// Accepts a validated batch and reports the per-batch outcome.
    fn accept(&self, batch: IntakeBatch) -> impl Future<Output = Result<IntakeReport, IntakeError>> + Send;
}

/// Placeholder [`RecordSink`] that prints each mutation as one structured line to stdout.
///
/// This exists so the intake service has a working, observable destination before the real one
/// lands. The future `KafkaSink` will implement [`RecordSink`] the same way and replace this at the
/// construction site in `main`; nothing else changes because the seam is the only place the
/// destination is named.
#[derive(Debug, Clone, Default)]
pub struct StdoutSink;

impl RecordSink for StdoutSink {
    async fn accept(&self, batch: IntakeBatch) -> Result<IntakeReport, IntakeError> {
        let target = &batch.target;
        let mut report = IntakeReport::default();
        for mutation in &batch.mutations {
            match mutation {
                Mutation::Upsert(record) => {
                    println!(
                        "intake upsert org={} tenant={} namespace={} id={} ts_ms={} vectors=[{}] texts=[{}] metadata_keys={}",
                        target.org_id,
                        target.tenant_id,
                        target.namespace,
                        record.id,
                        record.event_timestamp_ms,
                        format_vector_dims(&record.vectors),
                        format_text_names(&record.texts),
                        record.metadata.len(),
                    );
                }
                Mutation::Delete { id } => {
                    println!(
                        "intake delete org={} tenant={} namespace={} id={}",
                        target.org_id, target.tenant_id, target.namespace, id,
                    );
                }
            }
            report.accept();
        }
        Ok(report)
    }
}

/// Renders the named vectors as a comma-separated `name:dim` list in column order.
fn format_vector_dims(vectors: &BTreeMap<String, Vec<f32>>) -> String {
    vectors
        .iter()
        .map(|(name, values)| format!("{name}:{}", values.len()))
        .collect::<Vec<_>>()
        .join(",")
}

/// Renders the named text fields as a comma-separated list of field names in column order.
fn format_text_names(texts: &BTreeMap<String, String>) -> String {
    texts.keys().cloned().collect::<Vec<_>>().join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds an upsert mutation for the given id with one named vector and one text field.
    fn upsert(id: &str) -> Mutation {
        Mutation::Upsert(Record {
            id: id.to_string(),
            event_timestamp_ms: 1_700_000_000_000,
            metadata: BTreeMap::from([("source".to_string(), "test".to_string())]),
            vectors: BTreeMap::from([("vector".to_string(), vec![1.0, 0.0, 0.0, 0.0])]),
            texts: BTreeMap::from([("body".to_string(), "hello".to_string())]),
        })
    }

    #[test]
    fn batch_counts_upserts_and_deletes() {
        let batch = IntakeBatch::new(
            DatasetTarget::new("org1", "tenant1", "ns1"),
            vec![upsert("a"), upsert("b"), Mutation::Delete { id: "c".to_string() }],
        );
        assert_eq!(batch.upsert_count(), 2);
        assert_eq!(batch.delete_count(), 1);
        assert_eq!(batch.mutations[2].op(), MutationOp::Delete);
        assert_eq!(batch.mutations[2].id(), "c");
    }

    #[tokio::test]
    async fn stdout_sink_accepts_every_mutation() {
        let batch = IntakeBatch::new(
            DatasetTarget::new("org1", "tenant1", "ns1"),
            vec![upsert("a"), Mutation::Delete { id: "b".to_string() }],
        );
        let report = StdoutSink.accept(batch).await.unwrap();
        assert_eq!(report.accepted, 2);
        assert_eq!(report.rejected, 0);
        assert!(report.errors.is_empty());
    }

    #[test]
    fn report_merge_sums_counts_and_concatenates_errors() {
        let mut left = IntakeReport::default();
        left.accept();
        left.reject("x", "bad");
        let mut right = IntakeReport::default();
        right.accept();
        right.reject("y", "worse");
        left.merge(right);
        assert_eq!(left.accepted, 2);
        assert_eq!(left.rejected, 2);
        assert_eq!(left.errors.len(), 2);
        assert_eq!(left.errors[1].id, "y");
    }
}
