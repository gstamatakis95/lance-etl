//! Integration tests: builds tiny Lance datasets in a tempdir (single and date-partitioned),
//! creates INVERTED and IVF vector indexes, serves the gRPC API on a local TCP port, and
//! exercises every RPC plus the standard health service with a tonic client.

use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{FixedSizeListArray, Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_index::IndexType;
use lance_index::scalar::InvertedIndexParams;
use lance_linalg::distance::DistanceType as LanceDistanceType;
use prost_types::value::Kind;
use search_api::config::Config;
use search_api::grpc::SearchGrpc;
use search_api::lance::{CachingDatasetProvider, LanceSearchBackend};
use search_api::pb::search_service_client::SearchServiceClient;
use search_api::pb::search_service_server::SearchServiceServer;
use search_api::pb::{
    BooleanQuery, ClustersRequest, CompareOp, Comparison, DatasetTarget, DateRange, DistanceType, Filter, FtsQuery,
    Fusion, HybridSearchRequest, InList, LiteralValue, MatchQuery, PhraseQuery, PrewarmRequest, RrfFusion, TextQuery,
    TextSearchRequest, VectorQuery, VectorSearchRequest, filter, fts_query, fusion, literal_value, text_query,
};
use search_api::telemetry::{self, Metrics};
use tempfile::TempDir;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;
use tonic::transport::{Channel, Server};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_tracing_opentelemetry::middleware::filters::reject_healthcheck;
use tonic_tracing_opentelemetry::middleware::server::OtelGrpcLayer;

const DIM: i32 = 4;

/// Backend type wired by the tests: Lance over the caching provider.
type Backend = LanceSearchBackend<CachingDatasetProvider>;

/// Builds the proto target for `{org}/tenant1/ns1` without a date range.
fn target(org: &str) -> Option<DatasetTarget> {
    Some(DatasetTarget {
        org_id: org.to_string(),
        tenant_id: "tenant1".to_string(),
        namespace: "ns1".to_string(),
        date_range: None,
    })
}

/// Builds the proto target for `{org}/tenant1/ns1` with an inclusive date range.
fn dated_target(org: &str, start: &str, end: &str) -> Option<DatasetTarget> {
    Some(DatasetTarget {
        date_range: Some(DateRange {
            start_date: start.to_string(),
            end_date: end.to_string(),
        }),
        ..target(org).unwrap()
    })
}

/// The schema shared by every test dataset: id, text, vector(DIM), plus a `vector_id` logical id.
fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("vector_id", DataType::Int32, false),
        Field::new("text", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), DIM),
            false,
        ),
    ]))
}

/// Writes one dataset at `uri` with the given rows.
async fn write_rows(uri: &str, rows: &[(i32, i32, &str, [f32; 4])]) -> Dataset {
    let schema = test_schema();
    let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        rows.iter()
            .map(|(_, _, _, vector)| Some(vector.iter().map(|value| Some(*value)).collect::<Vec<_>>())),
        DIM,
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|(id, _, _, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|(_, vid, _, _)| *vid).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|(_, _, text, _)| *text).collect::<Vec<_>>(),
            )),
            Arc::new(vectors),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    Dataset::write(reader, uri, None).await.unwrap()
}

/// Writes the standard four-row dataset at `uri` and creates an INVERTED index with positions on
/// `text` so phrase queries work.
async fn build_test_dataset(uri: &str) {
    let mut dataset = write_rows(
        uri,
        &[
            (1, 1, "red apple pie", [1.0, 0.0, 0.0, 0.0]),
            (2, 2, "green pear tart", [0.0, 1.0, 0.0, 0.0]),
            (3, 3, "blue fish stew", [0.0, 0.0, 1.0, 0.0]),
            (4, 4, "yellow lemon cake", [0.0, 0.0, 0.0, 1.0]),
        ],
    )
    .await;
    dataset
        .create_index(
            &["text"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default().with_position(true),
            true,
        )
        .await
        .unwrap();
}

/// Writes three day-partitioned datasets under `{root}/org1/tenant1/ns1/` where `vector_id = 1`
/// recurs on every day with a different vector: an exact match for `[1,0,0,0]` on 2026-06-02 and
/// progressively worse copies on the other days.
async fn build_dated_datasets(root: &std::path::Path) {
    let base = root.join("org1/tenant1/ns1");
    write_rows(
        &format!("{}/2026-06-01.lance", base.display()),
        &[
            (10, 1, "day one copy", [0.0, 1.0, 0.0, 0.0]),
            (11, 2, "day one other", [0.0, 0.0, 1.0, 0.0]),
        ],
    )
    .await;
    write_rows(
        &format!("{}/2026-06-02.lance", base.display()),
        &[
            (20, 1, "day two copy", [1.0, 0.0, 0.0, 0.0]),
            (21, 3, "day two other", [0.0, 0.0, 0.0, 1.0]),
        ],
    )
    .await;
    write_rows(
        &format!("{}/2026-06-03.lance", base.display()),
        &[(30, 1, "day three copy", [0.5, 0.5, 0.0, 0.0])],
    )
    .await;
}

/// Serves the gRPC API on an ephemeral local port and returns a connected channel.
///
/// The server stack mirrors production: telemetry is initialized in disabled (log-only) mode and
/// every request flows through the OpenTelemetry tower layer, so each test doubles as a
/// pass-through assertion for the instrumentation layer.
async fn serve(tmp: &TempDir) -> Channel {
    serve_with_metrics(tmp, Arc::new(Metrics::disabled())).await
}

/// Like [`serve`] but emitting per-RPC metrics through the given facade.
async fn serve_with_metrics(tmp: &TempDir, metrics: Arc<Metrics>) -> Channel {
    drop(telemetry::init_tracing(true));
    let config = Config {
        base_uri: tmp.path().display().to_string(),
        dataset_cache_capacity: 16,
        index_cache_bytes: 64 * 1024 * 1024,
        metadata_cache_bytes: 64 * 1024 * 1024,
        port: 0,
        cache_dir: tmp.path().join("disk-cache"),
        disk_index_cache_bytes: 64 * 1024 * 1024,
        disk_store_cache_bytes: 64 * 1024 * 1024,
        disk_cache_ttl_secs: 3600,
        store_cache_max_range_bytes: 4 * 1024 * 1024,
        disk_cache_sweep_secs: 300,
        disk_cache_disabled: false,
        prewarm_concurrency: 4,
        fanout_concurrency: 4,
        id_column: "vector_id".to_string(),
        statsd_addr: "127.0.0.1:8125".to_string(),
        telemetry_disabled: true,
    };
    let provider = CachingDatasetProvider::with_telemetry(&config, metrics.clone());
    let backend = Arc::new(
        LanceSearchBackend::new(provider)
            .with_fanout_concurrency(config.fanout_concurrency)
            .with_id_column(config.id_column.clone())
            .with_metrics(metrics.clone()),
    );
    let service = SearchGrpc::with_metrics(backend, metrics);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<SearchServiceServer<SearchGrpc<Backend>>>()
        .await;
    tokio::spawn(
        Server::builder()
            .layer(OtelGrpcLayer::default().filter(reject_healthcheck))
            .add_service(health_service)
            .add_service(SearchServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

/// The on-disk path of the rangeless `org1/tenant1/ns1` dataset under the tempdir.
fn org1_uri(tmp: &TempDir) -> String {
    format!("{}/org1/tenant1/ns1.lance", tmp.path().display())
}

/// Reads a numeric field out of a result row struct.
fn row_number(row: &Option<prost_types::Struct>, key: &str) -> f64 {
    match row
        .as_ref()
        .unwrap()
        .fields
        .get(key)
        .and_then(|value| value.kind.as_ref())
    {
        Some(Kind::NumberValue(number)) => *number,
        other => panic!("expected number for {key}, got {other:?}"),
    }
}

/// Builds an int64 literal.
fn int_literal(number: i64) -> LiteralValue {
    LiteralValue {
        kind: Some(literal_value::Kind::Int64Value(number)),
    }
}

/// Builds a `column <op> int` comparison filter.
fn compare_filter(column: &str, op: CompareOp, number: i64) -> Filter {
    Filter {
        predicate: Some(filter::Predicate::Comparison(Comparison {
            column: column.to_string(),
            op: op as i32,
            value: Some(int_literal(number)),
        })),
    }
}

/// Builds a default vector query for the given query vector and k.
fn vector_query(vector: Vec<f32>, k: u32) -> VectorQuery {
    VectorQuery {
        vector,
        k,
        ..Default::default()
    }
}

/// Builds a simple match-string text query for the given terms and k.
fn simple_text_query(terms: &str, k: u32) -> TextQuery {
    TextQuery {
        input: Some(text_query::Input::Simple(terms.to_string())),
        columns: vec!["text".to_string()],
        k,
        ..Default::default()
    }
}

#[tokio::test]
async fn search_rpcs_return_sensible_results() {
    let tmp = TempDir::new().unwrap();
    build_test_dataset(&org1_uri(&tmp)).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel.clone());

    let mut health = HealthClient::new(channel);
    let status = health
        .check(HealthCheckRequest { service: String::new() })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.status(), ServingStatus::Serving);

    let response = client
        .vector_search(VectorSearchRequest {
            target: target("org1"),
            query: Some(vector_query(vec![1.0, 0.0, 0.0, 0.0], 2)),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.results.len(), 2);
    assert_eq!(row_number(&response.results[0].row, "id"), 1.0);
    assert!(response.results[0].distance < response.results[1].distance + 1e-6);

    let response = client
        .text_search(TextSearchRequest {
            target: target("org1"),
            query: Some(simple_text_query("lemon", 3)),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.results.len(), 1);
    assert_eq!(row_number(&response.results[0].row, "id"), 4.0);
    assert!(response.results[0].score > 0.0);

    let response = client
        .hybrid_search(HybridSearchRequest {
            target: target("org1"),
            vector: Some(vector_query(vec![0.0, 1.0, 0.0, 0.0], 0)),
            text: Some(simple_text_query("pear", 0)),
            k: 2,
            fusion: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.results.len(), 2);
    assert_eq!(row_number(&response.results[0].row, "id"), 2.0);
    let top_score = response.results[0].fused_score;
    let next_score = response.results[1].fused_score;
    assert!(top_score > next_score);
    let expected_top = 2.0 / 61.0;
    assert!((top_score - expected_top).abs() < 1e-9);
}

#[tokio::test]
async fn typed_filters_replace_sql_strings() {
    let tmp = TempDir::new().unwrap();
    build_test_dataset(&org1_uri(&tmp)).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let mut query = vector_query(vec![0.0, 1.0, 0.0, 0.0], 4);
    query.filter = Some(compare_filter("id", CompareOp::Gt, 2));
    let response = client
        .vector_search(VectorSearchRequest {
            target: target("org1"),
            query: Some(query),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!response.results.is_empty());
    assert!(response.results.iter().all(|hit| row_number(&hit.row, "id") > 2.0));

    let mut query = vector_query(vec![0.0, 1.0, 0.0, 0.0], 4);
    query.filter = Some(Filter {
        predicate: Some(filter::Predicate::And(search_api::pb::FilterList {
            filters: vec![
                compare_filter("id", CompareOp::Ge, 2),
                Filter {
                    predicate: Some(filter::Predicate::InList(InList {
                        column: "id".to_string(),
                        values: vec![int_literal(2), int_literal(3)],
                        negated: false,
                    })),
                },
            ],
        })),
    });
    let response = client
        .vector_search(VectorSearchRequest {
            target: target("org1"),
            query: Some(query),
        })
        .await
        .unwrap()
        .into_inner();
    let ids: Vec<f64> = response.results.iter().map(|hit| row_number(&hit.row, "id")).collect();
    assert_eq!(response.results.len(), 2);
    assert!(ids.contains(&2.0) && ids.contains(&3.0));

    let mut query = vector_query(vec![0.0, 1.0, 0.0, 0.0], 4);
    query.filter = Some(compare_filter("id; DROP TABLE users", CompareOp::Gt, 0));
    let status = client
        .vector_search(VectorSearchRequest {
            target: target("org1"),
            query: Some(query),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("column"), "unexpected error: {status}");
}

#[tokio::test]
async fn vector_knobs_distance_type_row_id_and_offset() {
    let tmp = TempDir::new().unwrap();
    build_test_dataset(&org1_uri(&tmp)).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let mut query = vector_query(vec![1.0, 0.0, 0.0, 0.0], 2);
    query.distance_type = DistanceType::Cosine as i32;
    query.with_row_id = true;
    query.projection = vec!["id".to_string()];
    let response = client
        .vector_search(VectorSearchRequest {
            target: target("org1"),
            query: Some(query),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.results.len(), 2);
    assert_eq!(row_number(&response.results[0].row, "id"), 1.0);
    assert!(response.results[0].distance.abs() < 1e-6);
    assert!((response.results[1].distance - 1.0).abs() < 1e-6);
    let row = response.results[0].row.as_ref().unwrap();
    assert!(row.fields.contains_key("_rowid"));
    assert!(!row.fields.contains_key("text"));

    let mut query = vector_query(vec![1.0, 0.0, 0.0, 0.0], 2);
    query.offset = Some(1);
    let response = client
        .vector_search(VectorSearchRequest {
            target: target("org1"),
            query: Some(query),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.results.len(), 2);
    assert!(response.results.iter().all(|hit| row_number(&hit.row, "id") != 1.0));
}

#[tokio::test]
async fn fts_phrase_and_boolean_queries() {
    let tmp = TempDir::new().unwrap();
    build_test_dataset(&org1_uri(&tmp)).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let phrase = TextQuery {
        input: Some(text_query::Input::Fts(FtsQuery {
            query: Some(fts_query::Query::Phrase(PhraseQuery {
                terms: "green pear".to_string(),
                column: Some("text".to_string()),
                slop: 0,
            })),
        })),
        k: 4,
        ..Default::default()
    };
    let response = client
        .text_search(TextSearchRequest {
            target: target("org1"),
            query: Some(phrase),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.results.len(), 1);
    assert_eq!(row_number(&response.results[0].row, "id"), 2.0);

    let boolean = TextQuery {
        input: Some(text_query::Input::Fts(FtsQuery {
            query: Some(fts_query::Query::Boolean(BooleanQuery {
                should: vec![
                    FtsQuery {
                        query: Some(fts_query::Query::Match(MatchQuery {
                            terms: "pear".to_string(),
                            column: Some("text".to_string()),
                            ..Default::default()
                        })),
                    },
                    FtsQuery {
                        query: Some(fts_query::Query::Match(MatchQuery {
                            terms: "lemon".to_string(),
                            column: Some("text".to_string()),
                            ..Default::default()
                        })),
                    },
                ],
                must: vec![],
                must_not: vec![],
            })),
        })),
        k: 4,
        ..Default::default()
    };
    let response = client
        .text_search(TextSearchRequest {
            target: target("org1"),
            query: Some(boolean),
        })
        .await
        .unwrap()
        .into_inner();
    let ids: Vec<f64> = response.results.iter().map(|hit| row_number(&hit.row, "id")).collect();
    assert_eq!(response.results.len(), 2);
    assert!(ids.contains(&2.0) && ids.contains(&4.0));
}

#[tokio::test]
async fn hybrid_fusion_config_is_applied() {
    let tmp = TempDir::new().unwrap();
    build_test_dataset(&org1_uri(&tmp)).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let response = client
        .hybrid_search(HybridSearchRequest {
            target: target("org1"),
            vector: Some(vector_query(vec![0.0, 1.0, 0.0, 0.0], 0)),
            text: Some(simple_text_query("pear", 0)),
            k: 2,
            fusion: Some(Fusion {
                strategy: Some(fusion::Strategy::Rrf(RrfFusion { rrf_k: Some(1.0) })),
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.results.len(), 2);
    assert_eq!(row_number(&response.results[0].row, "id"), 2.0);
    assert!((response.results[0].fused_score - 1.0).abs() < 1e-9);

    let status = client
        .hybrid_search(HybridSearchRequest {
            target: target("org1"),
            vector: Some(vector_query(vec![0.0, 1.0, 0.0, 0.0], 0)),
            text: Some(simple_text_query("pear", 0)),
            k: 2,
            fusion: Some(Fusion {
                strategy: Some(fusion::Strategy::Rrf(RrfFusion { rrf_k: Some(-3.0) })),
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn date_range_fanout_dedups_by_best_score_and_skips_missing_days() {
    let tmp = TempDir::new().unwrap();
    build_dated_datasets(tmp.path()).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let response = client
        .vector_search(VectorSearchRequest {
            target: dated_target("org1", "2026-06-01", "2026-06-04"),
            query: Some(vector_query(vec![1.0, 0.0, 0.0, 0.0], 3)),
        })
        .await
        .unwrap()
        .into_inner();
    let vids: Vec<f64> = response
        .results
        .iter()
        .map(|hit| row_number(&hit.row, "vector_id"))
        .collect();
    assert_eq!(
        vids.iter().filter(|vid| **vid == 1.0).count(),
        1,
        "duplicate vector_id must be deduplicated: {vids:?}"
    );
    assert_eq!(row_number(&response.results[0].row, "vector_id"), 1.0);
    assert_eq!(
        row_number(&response.results[0].row, "id"),
        20.0,
        "dedup must keep the best (minimum-distance) copy, which lives on 2026-06-02"
    );
    assert!(response.results[0].distance.abs() < 1e-6);
    assert!(
        response
            .results
            .windows(2)
            .all(|pair| pair[0].distance <= pair[1].distance + 1e-6),
        "merged results must stay ordered nearest-first"
    );
}

#[tokio::test]
async fn date_range_fanout_respects_explicit_projection_without_leaking_the_id_column() {
    let tmp = TempDir::new().unwrap();
    build_dated_datasets(tmp.path()).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let mut query = vector_query(vec![1.0, 0.0, 0.0, 0.0], 3);
    query.projection = vec!["id".to_string()];
    let response = client
        .vector_search(VectorSearchRequest {
            target: dated_target("org1", "2026-06-01", "2026-06-03"),
            query: Some(query),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        row_number(&response.results[0].row, "id"),
        20.0,
        "dedup must work even when the projection omits the id column"
    );
    for hit in &response.results {
        let row = hit.row.as_ref().unwrap();
        assert!(
            !row.fields.contains_key("vector_id"),
            "the internally projected id column must be stripped from responses"
        );
    }
}

#[tokio::test]
async fn date_range_with_zero_existing_datasets_is_not_found() {
    let tmp = TempDir::new().unwrap();
    build_dated_datasets(tmp.path()).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let status = client
        .vector_search(VectorSearchRequest {
            target: dated_target("org1", "2026-07-01", "2026-07-03"),
            query: Some(vector_query(vec![1.0, 0.0, 0.0, 0.0], 2)),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::NotFound);

    let status = client
        .vector_search(VectorSearchRequest {
            target: dated_target("org1", "2026-06-03", "2026-06-01"),
            query: Some(vector_query(vec![1.0, 0.0, 0.0, 0.0], 2)),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument, "inverted ranges are rejected");

    let status = client
        .vector_search(VectorSearchRequest {
            target: dated_target("org1", "2026-06-XX", "2026-06-03"),
            query: Some(vector_query(vec![1.0, 0.0, 0.0, 0.0], 2)),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument, "malformed dates are rejected");
}

#[tokio::test]
async fn prewarm_rpc_warms_metadata_and_indexes() {
    let tmp = TempDir::new().unwrap();
    build_test_dataset(&org1_uri(&tmp)).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let response = client
        .prewarm(PrewarmRequest {
            target: target("org1"),
            metadata: true,
            all_indexes: true,
            index_names: vec![],
            fts_with_position: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(response.metadata_warmed);
    assert_eq!(response.indexes.len(), 1);
    assert_eq!(response.indexes[0].name, "text_idx");
    assert_eq!(response.indexes[0].error, "");

    let response = client
        .text_search(TextSearchRequest {
            target: target("org1"),
            query: Some(simple_text_query("lemon", 3)),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.results.len(), 1);
}

#[tokio::test]
async fn prewarm_rpc_reports_per_index_errors_and_status_codes() {
    let tmp = TempDir::new().unwrap();
    build_test_dataset(&org1_uri(&tmp)).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let response = client
        .prewarm(PrewarmRequest {
            target: target("org1"),
            metadata: false,
            all_indexes: false,
            index_names: vec!["text_idx".into(), "no_such_index".into()],
            fts_with_position: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(response.metadata_warmed);
    assert_eq!(response.indexes.len(), 2);
    let by_name = |name: &str| response.indexes.iter().find(|index| index.name == name).unwrap();
    assert_eq!(by_name("text_idx").error, "");
    assert!(!by_name("no_such_index").error.is_empty());

    let status = client
        .prewarm(PrewarmRequest {
            target: target("absent"),
            metadata: true,
            all_indexes: false,
            index_names: vec![],
            fts_with_position: false,
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::NotFound);

    let status = client
        .prewarm(PrewarmRequest {
            target: target("../escape"),
            metadata: true,
            all_indexes: false,
            index_names: vec![],
            fts_with_position: false,
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);

    let status = client
        .prewarm(PrewarmRequest {
            target: dated_target("org1", "2026-06-01", "2026-06-03"),
            metadata: true,
            all_indexes: false,
            index_names: vec![],
            fts_with_position: false,
        })
        .await
        .unwrap_err();
    assert_eq!(
        status.code(),
        Code::InvalidArgument,
        "prewarm must reject multi-day ranges"
    );
}

#[tokio::test]
async fn clusters_rpc_returns_ivf_centroids() {
    let tmp = TempDir::new().unwrap();
    let uri = org1_uri(&tmp);
    let rows: Vec<(i32, i32, String, [f32; 4])> = (0..64)
        .map(|index| {
            let mut vector = [0.0f32; 4];
            vector[(index % 4) as usize] = 1.0 + (index as f32) / 100.0;
            (index, index, format!("row {index}"), vector)
        })
        .collect();
    let borrowed: Vec<(i32, i32, &str, [f32; 4])> = rows
        .iter()
        .map(|(id, vid, text, vector)| (*id, *vid, text.as_str(), *vector))
        .collect();
    let mut dataset = write_rows(&uri, &borrowed).await;
    dataset
        .create_index(
            &["vector"],
            IndexType::Vector,
            Some("vector_idx".to_string()),
            &VectorIndexParams::ivf_flat(4, LanceDistanceType::L2),
            true,
        )
        .await
        .unwrap();
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let response = client
        .clusters(ClustersRequest {
            target: target("org1"),
            index_name: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.num_partitions, 4);
    assert_eq!(response.clusters.len(), 4, "centroid count must equal num_partitions");
    assert_eq!(response.dimension, DIM as u32);
    assert_eq!(response.index_name, "vector_idx");
    for (id, cluster) in response.clusters.iter().enumerate() {
        assert_eq!(cluster.id, id as u32);
        assert_eq!(cluster.centroid.len(), DIM as usize);
    }

    let named = client
        .clusters(ClustersRequest {
            target: target("org1"),
            index_name: Some("vector_idx".to_string()),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(named.clusters.len(), 4);
}

#[tokio::test]
async fn clusters_rpc_not_found_and_invalid_cases() {
    let tmp = TempDir::new().unwrap();
    build_test_dataset(&org1_uri(&tmp)).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let status = client
        .clusters(ClustersRequest {
            target: target("absent"),
            index_name: None,
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::NotFound, "missing dataset must be NotFound");

    let status = client
        .clusters(ClustersRequest {
            target: target("org1"),
            index_name: None,
        })
        .await
        .unwrap_err();
    assert_eq!(
        status.code(),
        Code::NotFound,
        "dataset without a vector index must be NotFound"
    );

    let status = client
        .clusters(ClustersRequest {
            target: target("org1"),
            index_name: Some("no_such_index".to_string()),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::NotFound);

    let status = client
        .clusters(ClustersRequest {
            target: dated_target("org1", "2026-06-01", "2026-06-03"),
            index_name: None,
        })
        .await
        .unwrap_err();
    assert_eq!(
        status.code(),
        Code::InvalidArgument,
        "clusters must reject multi-day ranges"
    );

    let status = client
        .clusters(ClustersRequest {
            target: target("org1"),
            index_name: Some("text_idx".to_string()),
        })
        .await
        .unwrap_err();
    assert_eq!(
        status.code(),
        Code::InvalidArgument,
        "naming a non-vector index must be InvalidArgument"
    );
}

#[tokio::test]
async fn missing_dataset_and_bad_target_return_proper_status_codes() {
    let tmp = TempDir::new().unwrap();
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let status = client
        .vector_search(VectorSearchRequest {
            target: target("absent"),
            query: Some(vector_query(vec![1.0, 0.0, 0.0, 0.0], 1)),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::NotFound);
    assert!(status.message().contains("Dataset"), "unexpected error: {status}");

    let status = client
        .text_search(TextSearchRequest {
            target: target("../escape"),
            query: Some(simple_text_query("x", 1)),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("org_id"));

    let status = client
        .text_search(TextSearchRequest {
            target: None,
            query: Some(simple_text_query("x", 1)),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("target"));
}

#[tokio::test]
async fn instrumented_server_emits_rpc_metrics_and_passes_requests_through() {
    let tmp = TempDir::new().unwrap();
    build_dated_datasets(tmp.path()).await;
    build_test_dataset(&org1_uri(&tmp)).await;
    let (receiver, sink) = cadence::SpyMetricSink::new();
    let channel = serve_with_metrics(&tmp, Arc::new(Metrics::from_sink(sink))).await;
    let mut client = SearchServiceClient::new(channel.clone());

    let response = client
        .vector_search(VectorSearchRequest {
            target: target("org1"),
            query: Some(vector_query(vec![1.0, 0.0, 0.0, 0.0], 2)),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.results.len(), 2, "instrumentation must not alter results");

    let response = client
        .vector_search(VectorSearchRequest {
            target: dated_target("org1", "2026-06-01", "2026-06-04"),
            query: Some(vector_query(vec![1.0, 0.0, 0.0, 0.0], 3)),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!response.results.is_empty());

    let status = client
        .text_search(TextSearchRequest {
            target: target("absent"),
            query: Some(simple_text_query("x", 1)),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::NotFound);

    let mut health = HealthClient::new(channel);
    let health_status = health
        .check(HealthCheckRequest { service: String::new() })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        health_status.status(),
        ServingStatus::Serving,
        "health checks must pass through the span filter"
    );

    let mut lines = Vec::new();
    while let Ok(packet) = receiver.try_recv() {
        lines.push(String::from_utf8(packet).unwrap());
    }
    assert!(
        lines.iter().any(|line| line.starts_with("search_api.rpc.requests:1|c")
            && line.contains("rpc:vector_search")
            && line.contains("status:ok")),
        "missing ok request count: {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("search_api.rpc.duration_ms:") && line.contains("rpc:vector_search")),
        "missing latency distribution: {lines:?}"
    );
    assert!(
        lines.iter().any(|line| line.starts_with("search_api.rpc.errors:1|c")
            && line.contains("rpc:text_search")
            && line.contains("status:not_found")),
        "missing error count: {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("search_api.fanout.legs:3|d") && line.contains("leg:vector")),
        "missing fan-out width distribution: {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("search_api.fanout.leg.duration_ms:") && line.contains("leg:vector")),
        "missing per-leg latency distribution: {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("search_api.fanout.dedup.dropped:2|c") && line.contains("leg:vector")),
        "missing dedup-drop counter: {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("search_api.dataset.open.duration_ms:") && line.contains("cold:true")),
        "missing dataset open distribution: {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("search_api.cache.handles.entries:")),
        "missing handle cache gauge: {lines:?}"
    );
    assert!(
        !lines.iter().any(|line| line.contains("org_id")),
        "org_id must never appear on metrics: {lines:?}"
    );
}
