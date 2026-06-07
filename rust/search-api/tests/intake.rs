//! Integration tests for the intake gRPC service: proto <-> domain conversion round-trips,
//! validation rejections, the [`StdoutSink`] domain seam, and the unary Write RPC happy path
//! served over a local TCP port with a tonic client.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use search_api::domain::{DatasetTarget, IntakeBatch, Record, RecordSink, RecordWrite, StdoutSink, WriteOp};
use search_api::grpc::IntakeGrpc;
use search_api::grpc::intake_convert::{record_write_from_proto, report_to_proto, target_from_proto};
use search_api::pb::intake_service_client::IntakeServiceClient;
use search_api::pb::intake_service_server::IntakeServiceServer;
use search_api::pb::{
    DatasetTarget as PbTarget, FloatVector as PbFloatVector, Record as PbRecord, RecordWrite as PbRecordWrite,
    WriteOp as PbOp, WriteRecordsRequest,
};
use search_api::telemetry::Metrics;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;
use tonic::transport::{Channel, Server};

/// Builds a proto target for `org1/tenant1/ns1`.
fn pb_target() -> Option<PbTarget> {
    Some(PbTarget {
        org_id: "org1".to_string(),
        tenant_id: "tenant1".to_string(),
        namespace: "ns1".to_string(),
    })
}

/// Builds a proto named-vector map from `name -> values` pairs.
fn pb_vectors(entries: &[(&str, Vec<f32>)]) -> HashMap<String, PbFloatVector> {
    entries
        .iter()
        .map(|(name, values)| (name.to_string(), PbFloatVector { values: values.clone() }))
        .collect()
}

/// Builds a proto string map from `key -> value` pairs.
fn pb_strings(entries: &[(&str, &str)]) -> HashMap<String, String> {
    entries
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

/// Builds a proto upsert record write carrying one named vector, one text field, and one metadata key.
fn pb_upsert(id: &str, vector: Vec<f32>) -> PbRecordWrite {
    PbRecordWrite {
        op: PbOp::Upsert as i32,
        record: Some(PbRecord {
            id: id.to_string(),
            event_timestamp_ms: 1_700_000_000_000,
            metadata: pb_strings(&[("k", "1")]),
            vectors: pb_vectors(&[("vector", vector)]),
            texts: pb_strings(&[("body", "hello world")]),
        }),
    }
}

/// Builds a proto delete record write for the given id.
fn pb_delete(id: &str) -> PbRecordWrite {
    PbRecordWrite {
        op: PbOp::Delete as i32,
        record: Some(PbRecord {
            id: id.to_string(),
            event_timestamp_ms: 0,
            metadata: HashMap::new(),
            vectors: HashMap::new(),
            texts: HashMap::new(),
        }),
    }
}

#[test]
fn target_round_trips_and_rejects_bad_segments() {
    let target = target_from_proto(pb_target()).unwrap();
    assert_eq!(target, DatasetTarget::new("org1", "tenant1", "ns1"));
    let bad = PbTarget {
        org_id: "../escape".to_string(),
        tenant_id: "tenant1".to_string(),
        namespace: "ns1".to_string(),
    };
    assert!(target_from_proto(Some(bad)).is_err());
    assert!(target_from_proto(None).is_err());
}

#[test]
fn multi_vector_multi_text_upsert_round_trips_through_conversion() {
    let write = PbRecordWrite {
        op: PbOp::Upsert as i32,
        record: Some(PbRecord {
            id: "a".to_string(),
            event_timestamp_ms: 1_700_000_000_000,
            metadata: pb_strings(&[("source", "web"), ("lang", "en")]),
            vectors: pb_vectors(&[("dense", vec![1.0, 0.0, 0.0, 0.0]), ("sparse", vec![0.5, 0.5])]),
            texts: pb_strings(&[("title", "green pear"), ("body", "a ripe green pear")]),
        }),
    };
    match record_write_from_proto(write).unwrap() {
        RecordWrite::Upsert(record) => {
            assert_eq!(record.id, "a");
            assert_eq!(record.event_timestamp_ms, 1_700_000_000_000);
            assert_eq!(
                record.metadata,
                BTreeMap::from([
                    ("source".to_string(), "web".to_string()),
                    ("lang".to_string(), "en".to_string()),
                ])
            );
            assert_eq!(record.vectors["dense"], vec![1.0, 0.0, 0.0, 0.0]);
            assert_eq!(record.vectors["sparse"], vec![0.5, 0.5]);
            assert_eq!(record.texts["title"], "green pear");
            assert_eq!(record.texts["body"], "a ripe green pear");
        }
        other => panic!("expected upsert, got {other:?}"),
    }
}

#[test]
fn delete_round_trips_through_conversion() {
    let delete = record_write_from_proto(pb_delete("b")).unwrap();
    assert_eq!(delete.op(), WriteOp::Delete);
    assert_eq!(delete.id(), "b");
}

#[test]
fn metadata_only_upsert_is_accepted() {
    let write = PbRecordWrite {
        op: PbOp::Upsert as i32,
        record: Some(PbRecord {
            id: "meta".to_string(),
            event_timestamp_ms: 7,
            metadata: pb_strings(&[("only", "metadata")]),
            vectors: HashMap::new(),
            texts: HashMap::new(),
        }),
    };
    match record_write_from_proto(write).unwrap() {
        RecordWrite::Upsert(record) => {
            assert_eq!(record.metadata["only"], "metadata");
            assert!(record.vectors.is_empty());
            assert!(record.texts.is_empty());
        }
        other => panic!("expected upsert, got {other:?}"),
    }
}

#[test]
fn validation_rejects_bad_record_writes() {
    let empty_named_vector = PbRecordWrite {
        op: PbOp::Upsert as i32,
        record: Some(PbRecord {
            id: "a".to_string(),
            event_timestamp_ms: 0,
            metadata: HashMap::new(),
            vectors: pb_vectors(&[("dense", vec![])]),
            texts: HashMap::new(),
        }),
    };
    assert!(record_write_from_proto(empty_named_vector).is_err());

    let fully_empty_upsert = PbRecordWrite {
        op: PbOp::Upsert as i32,
        record: Some(PbRecord {
            id: "a".to_string(),
            event_timestamp_ms: 0,
            metadata: HashMap::new(),
            vectors: HashMap::new(),
            texts: HashMap::new(),
        }),
    };
    assert!(record_write_from_proto(fully_empty_upsert).is_err());

    let empty_id = record_write_from_proto(pb_upsert("", vec![1.0]));
    assert!(empty_id.is_err());

    let unspecified = record_write_from_proto(PbRecordWrite {
        op: PbOp::Unspecified as i32,
        record: Some(PbRecord {
            id: "a".to_string(),
            event_timestamp_ms: 0,
            metadata: pb_strings(&[("k", "v")]),
            vectors: HashMap::new(),
            texts: HashMap::new(),
        }),
    });
    assert!(unspecified.is_err());

    let no_record = record_write_from_proto(PbRecordWrite {
        op: PbOp::Delete as i32,
        record: None,
    });
    assert!(no_record.is_err());
}

#[tokio::test]
async fn stdout_sink_reports_succeeded_ids() {
    let batch = IntakeBatch::new(
        DatasetTarget::new("org1", "tenant1", "ns1"),
        vec![
            RecordWrite::Upsert(Record {
                id: "a".to_string(),
                event_timestamp_ms: 1,
                metadata: BTreeMap::from([("k".to_string(), "v".to_string())]),
                vectors: BTreeMap::from([("dense".to_string(), vec![1.0, 0.0])]),
                texts: BTreeMap::new(),
            }),
            RecordWrite::Delete { id: "b".to_string() },
        ],
    );
    let report = StdoutSink.accept(batch).await.unwrap();
    assert_eq!(report.succeeded_ids, vec!["a".to_string(), "b".to_string()]);
    assert!(report.failed_ids.is_empty());
    let proto = report_to_proto(report);
    assert_eq!(proto.succeeded_ids, vec!["a".to_string(), "b".to_string()]);
    assert!(proto.failed_ids.is_empty());
}

/// Serves the intake gRPC API on an ephemeral local port and returns a connected channel.
async fn serve() -> Channel {
    let metrics = Arc::new(Metrics::disabled());
    let intake = IntakeGrpc::with_metrics(Arc::new(StdoutSink), metrics);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        Server::builder()
            .add_service(IntakeServiceServer::new(intake))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

#[tokio::test]
async fn write_rpc_happy_path_accepts_a_batch() {
    let mut client = IntakeServiceClient::new(serve().await);
    let response = client
        .write(WriteRecordsRequest {
            target: pb_target(),
            writes: vec![pb_upsert("a", vec![1.0, 0.0, 0.0, 0.0]), pb_delete("b")],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.succeeded_ids, vec!["a".to_string(), "b".to_string()]);
    assert!(response.failed_ids.is_empty());
}

#[tokio::test]
async fn write_rpc_partitions_succeeded_and_failed_ids() {
    let mut client = IntakeServiceClient::new(serve().await);
    let response = client
        .write(WriteRecordsRequest {
            target: pb_target(),
            writes: vec![pb_upsert("good", vec![1.0]), pb_upsert("bad", vec![])],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.succeeded_ids, vec!["good".to_string()]);
    assert_eq!(response.failed_ids, vec!["bad".to_string()]);
}

#[tokio::test]
async fn write_rpc_rejects_a_bad_target() {
    let mut client = IntakeServiceClient::new(serve().await);
    let status = client
        .write(WriteRecordsRequest {
            target: Some(PbTarget {
                org_id: "../escape".to_string(),
                tenant_id: "tenant1".to_string(),
                namespace: "ns1".to_string(),
            }),
            writes: vec![pb_upsert("a", vec![1.0])],
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
}
