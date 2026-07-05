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

/// The kind of write applied to a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOp {
    /// Create the record if absent, otherwise replace it.
    Upsert,
    /// Delete the record by id.
    Delete,
}

impl WriteOp {
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

/// One validated record write: an operation paired with the record it acts on.
///
/// An [`WriteOp::Upsert`] carries the full record. A [`WriteOp::Delete`] reads only the id of the
/// record, so its other fields are left at their defaults.
#[derive(Debug, Clone, PartialEq)]
pub enum RecordWrite {
    /// Create or replace the record.
    Upsert(Record),
    /// Delete the record with this id.
    Delete {
        /// Id of the record to delete.
        id: String,
    },
}

impl RecordWrite {
    /// The operation kind of this record write.
    pub fn op(&self) -> WriteOp {
        match self {
            Self::Upsert(_) => WriteOp::Upsert,
            Self::Delete { .. } => WriteOp::Delete,
        }
    }

    /// The id of the record this write acts on.
    pub fn id(&self) -> &str {
        match self {
            Self::Upsert(record) => &record.id,
            Self::Delete { id } => id,
        }
    }
}

/// A batch of record writes addressed to a single dataset, ready for a sink to accept.
#[derive(Debug, Clone, PartialEq)]
pub struct IntakeBatch {
    /// Dataset the record writes belong to.
    pub target: DatasetTarget,
    /// Record writes to apply, in client order.
    pub writes: Vec<RecordWrite>,
}

impl IntakeBatch {
    /// Builds a batch from a target and its record writes.
    pub fn new(target: DatasetTarget, writes: Vec<RecordWrite>) -> Self {
        Self { target, writes }
    }

    /// Number of upserts in the batch.
    pub fn upsert_count(&self) -> u64 {
        self.writes.iter().filter(|write| write.op() == WriteOp::Upsert).count() as u64
    }

    /// Number of deletes in the batch.
    pub fn delete_count(&self) -> u64 {
        self.writes.iter().filter(|write| write.op() == WriteOp::Delete).count() as u64
    }
}

/// Outcome of accepting one (or several, when aggregated) intake batch.
///
/// The outcome is expressed purely as record ids: those the sink accepted and those that failed
/// validation or sink acceptance. A record whose id is itself empty or invalid cannot be reported
/// by id, so it never appears in `failed_ids`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IntakeReport {
    /// Ids of records successfully accepted by the sink, in encounter order.
    pub succeeded_ids: Vec<String>,
    /// Ids of records that failed validation or sink acceptance, in encounter order.
    pub failed_ids: Vec<String>,
}

impl IntakeReport {
    /// Records one successfully accepted record id.
    pub fn succeed(&mut self, id: impl Into<String>) {
        self.succeeded_ids.push(id.into());
    }

    /// Records one failed record id.
    pub fn fail(&mut self, id: impl Into<String>) {
        self.failed_ids.push(id.into());
    }

    /// Folds another report into this one, concatenating both id lists.
    pub fn merge(&mut self, other: IntakeReport) {
        self.succeeded_ids.extend(other.succeeded_ids);
        self.failed_ids.extend(other.failed_ids);
    }
}

/// The write seam: a destination any intake batch is handed to.
///
/// Implementations receive an already-validated [`IntakeBatch`] and report which record ids they
/// accepted and which failed. The transport stays generic over this trait and never names a
/// concrete destination. This is the extension point: [`StdoutSink`] is the placeholder, and a
/// future `KafkaSink` drops in here as a second implementation without touching the proto, the
/// transport, or the domain types.
pub trait RecordSink: Send + Sync + 'static {
    /// Accepts a validated batch and reports the per-batch outcome.
    fn accept(&self, batch: IntakeBatch) -> impl Future<Output = Result<IntakeReport, IntakeError>> + Send;
}

/// Placeholder [`RecordSink`] that prints each record write as one structured line to stdout.
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
        for write in &batch.writes {
            match write {
                RecordWrite::Upsert(record) => {
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
                RecordWrite::Delete { id } => {
                    println!(
                        "intake delete org={} tenant={} namespace={} id={}",
                        target.org_id, target.tenant_id, target.namespace, id,
                    );
                }
            }
            report.succeed(write.id().to_string());
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

    /// Builds an upsert record write for the given id with one named vector and one text field.
    fn upsert(id: &str) -> RecordWrite {
        RecordWrite::Upsert(Record {
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
            vec![upsert("a"), upsert("b"), RecordWrite::Delete { id: "c".to_string() }],
        );
        assert_eq!(batch.upsert_count(), 2);
        assert_eq!(batch.delete_count(), 1);
        assert_eq!(batch.writes[2].op(), WriteOp::Delete);
        assert_eq!(batch.writes[2].id(), "c");
    }

    #[tokio::test]
    async fn stdout_sink_succeeds_every_record_write() {
        let batch = IntakeBatch::new(
            DatasetTarget::new("org1", "tenant1", "ns1"),
            vec![upsert("a"), RecordWrite::Delete { id: "b".to_string() }],
        );
        let report = StdoutSink.accept(batch).await.unwrap();
        assert_eq!(report.succeeded_ids, vec!["a".to_string(), "b".to_string()]);
        assert!(report.failed_ids.is_empty());
    }

    #[test]
    fn report_merge_concatenates_id_lists() {
        let mut left = IntakeReport::default();
        left.succeed("a");
        left.fail("x");
        let mut right = IntakeReport::default();
        right.succeed("b");
        right.fail("y");
        left.merge(right);
        assert_eq!(left.succeeded_ids, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(left.failed_ids, vec!["x".to_string(), "y".to_string()]);
    }
}
