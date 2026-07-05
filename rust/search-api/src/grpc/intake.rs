//! Thin gRPC transport for the intake service: proto <-> domain mapping over a [`RecordSink`].
//!
//! This layer never references Lance types. It validates protobuf requests into domain
//! [`IntakeBatch`]es, hands each batch to the sink, and converts the domain report back to
//! protobuf. Per-RPC observability lives here: the tower layer in `main` opens the server span,
//! and the handlers annotate it with the dataset target, emit `intake.*` metrics, and log failures
//! with the target context. The sink is the only seam where a write destination appears, so the
//! transport is identical whether the sink prints to stdout or writes to Kafka.

use std::sync::Arc;
use std::time::Instant;

use tonic::{Code, Request, Response, Status, Streaming};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::domain::{DatasetTarget, IntakeBatch, IntakeError, IntakeReport, RecordSink, RecordWrite};
use crate::grpc::code_tag;
use crate::grpc::intake_convert::{record_write_from_proto, report_to_proto, target_from_proto, write_id};
use crate::pb::intake_service_server::IntakeService;
use crate::pb::{WriteRecordsRequest, WriteRecordsResponse};
use crate::telemetry::{IntakeRpc, Metrics};

/// gRPC service adapter over any domain [`RecordSink`].
pub struct IntakeGrpc<S> {
    sink: Arc<S>,
    metrics: Arc<Metrics>,
}

impl<S> IntakeGrpc<S> {
    /// Creates the adapter emitting per-RPC metrics through the given facade.
    pub fn with_metrics(sink: Arc<S>, metrics: Arc<Metrics>) -> Self {
        Self { sink, metrics }
    }
}

/// The outcome of processing one request's batch: the merged report plus the operation counts the
/// batch metric needs.
struct BatchOutcome {
    /// Validation failures merged with the sink's report.
    report: IntakeReport,
    /// Upserts handed to the sink.
    upserts: u64,
    /// Deletes handed to the sink.
    deletes: u64,
}

impl<S: RecordSink> IntakeGrpc<S> {
    /// Validates one request into a batch, hands the valid record writes to the sink, and merges the
    /// per-item validation failures with the sink's report.
    ///
    /// A bad target fails the whole request. A bad individual record write fails per-item and does
    /// not stop the rest of the batch; its id is recorded as failed unless the id is itself empty,
    /// in which case the write cannot be reported by id and is skipped. A sink failure fails the
    /// whole request.
    async fn process_request(&self, request: WriteRecordsRequest) -> Result<BatchOutcome, IntakeError> {
        let target = target_from_proto(request.target)?;
        annotate_target(&target);
        let mut report = IntakeReport::default();
        let mut valid: Vec<RecordWrite> = Vec::with_capacity(request.writes.len());
        for write in request.writes {
            let id = write_id(&write);
            match record_write_from_proto(write) {
                Ok(write) => valid.push(write),
                Err(_) => {
                    if !id.is_empty() {
                        report.fail(id);
                    }
                }
            }
        }
        let batch = IntakeBatch::new(target, valid);
        let upserts = batch.upsert_count();
        let deletes = batch.delete_count();
        let sink_report = self.sink.accept(batch).await?;
        report.merge(sink_report);
        Ok(BatchOutcome {
            report,
            upserts,
            deletes,
        })
    }

    /// Emits the per-batch metric for one processed request.
    fn record_batch(&self, rpc: IntakeRpc, submitted: u64, outcome: &BatchOutcome) {
        self.metrics.intake_batch(
            rpc,
            outcome.upserts,
            outcome.deletes,
            outcome.report.failed_ids.len() as u64,
            submitted,
        );
    }

    /// Records the RPC outcome: `intake.*` request/latency/error metrics and a warn-level event
    /// with the target context on failure.
    fn record_rpc(&self, rpc: IntakeRpc, started: Instant, result: &Result<WriteRecordsResponse, Status>) {
        let code = match result {
            Ok(_) => Code::Ok,
            Err(status) => status.code(),
        };
        tracing::Span::current().set_attribute("rpc.grpc.status_code", code as i64);
        self.metrics.intake_rpc(rpc, code_tag(code), started.elapsed());
        if let Err(status) = result {
            tracing::warn!(
                rpc = rpc.as_tag(),
                status = code_tag(code),
                message = status.message(),
                "intake rpc failed"
            );
        }
    }
}

#[tonic::async_trait]
impl<S: RecordSink> IntakeService for IntakeGrpc<S> {
    /// Applies one batch of record writes addressed to a single dataset.
    async fn write(&self, request: Request<WriteRecordsRequest>) -> Result<Response<WriteRecordsResponse>, Status> {
        let started = Instant::now();
        let request = request.into_inner();
        let submitted = request.writes.len() as u64;
        let result = match self.process_request(request).await {
            Ok(outcome) => {
                self.record_batch(IntakeRpc::Write, submitted, &outcome);
                Ok(report_to_proto(outcome.report))
            }
            Err(error) => Err(status_from_intake_error(error)),
        };
        self.record_rpc(IntakeRpc::Write, started, &result);
        result.map(Response::new)
    }

    /// Applies a stream of record-write batches, returning one aggregated response on half-close.
    async fn write_stream(
        &self,
        request: Request<Streaming<WriteRecordsRequest>>,
    ) -> Result<Response<WriteRecordsResponse>, Status> {
        let started = Instant::now();
        let mut stream = request.into_inner();
        let mut total = IntakeReport::default();
        let result = async {
            while let Some(request) = stream.message().await? {
                let submitted = request.writes.len() as u64;
                let outcome = self.process_request(request).await.map_err(status_from_intake_error)?;
                self.record_batch(IntakeRpc::WriteStream, submitted, &outcome);
                total.merge(outcome.report);
            }
            Ok(report_to_proto(std::mem::take(&mut total)))
        }
        .await;
        self.record_rpc(IntakeRpc::WriteStream, started, &result);
        result.map(Response::new)
    }
}

/// Maps a domain intake error onto the corresponding gRPC status.
pub fn status_from_intake_error(err: IntakeError) -> Status {
    match err {
        IntakeError::InvalidArgument(message) => Status::invalid_argument(message),
        IntakeError::Unavailable(message) => Status::unavailable(message),
        IntakeError::Internal(message) => Status::internal(message),
    }
}

/// Annotates the current (server) span with the canonical dataset-target attributes. Target
/// identifiers are allowed on traces but never on metrics.
fn annotate_target(target: &DatasetTarget) {
    let span = tracing::Span::current();
    span.set_attribute("org_id", target.org_id.clone());
    span.set_attribute("tenant_id", target.tenant_id.clone());
    span.set_attribute("namespace", target.namespace.clone());
}
