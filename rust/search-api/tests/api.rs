//! Integration tests: builds tiny Lance datasets in a tempdir, creates INVERTED and IVF vector
//! indexes, serves the gRPC API on a local TCP port, and
//! exercises every RPC plus the standard health service with a tonic client.

use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{FixedSizeListArray, Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_index::IndexType;
use lance_index::scalar::InvertedIndexParams;
use lance_linalg::distance::DistanceType as LanceDistanceType;
use prost_types::value::Kind;
use search_api::config::Config;
use search_api::grpc::{RouteTimeoutLayer, SearchGrpc};
use search_api::lance::{CachingDatasetProvider, LanceSearchBackend};
use search_api::pb::search_service_client::SearchServiceClient;
use search_api::pb::search_service_server::SearchServiceServer;
use search_api::pb::{
    BooleanQuery, ClustersRequest, CompareOp, Comparison, DatasetTarget, DistanceType, Filter, FtsQuery, Fusion,
    HybridSearchRequest, IdentityRerank, InList, LiteralValue, MatchQuery, PhraseQuery, PrewarmRequest, Rerank,
    RrfFusion, TextQuery, TextSearchRequest, VectorQuery, VectorSearchRequest, WeightedFusion, filter, fts_query,
    fusion, literal_value, rerank, text_query,
};
use search_api::telemetry::{self, Metrics, RecallCapture, RecallQueryType, RecallRecord};
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

/// Builds the proto target for `{org}/tenant1/ns1`.
fn target(org: &str) -> Option<DatasetTarget> {
    Some(DatasetTarget {
        org_id: org.to_string(),
        tenant_id: "tenant1".to_string(),
        namespace: "ns1".to_string(),
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
    let head_version = dataset.version_id();
    dataset
        .tags()
        .create(search_api::config::PRODUCTION_SERVE_TAG, head_version)
        .await
        .unwrap();
}

/// Serves the gRPC API on an ephemeral local port and returns a connected channel.
///
/// The server stack mirrors production: telemetry is initialized in disabled (log-only) mode and
/// every request flows through the OpenTelemetry tower layer plus the production per-route
/// timeout layer, so each test doubles as a pass-through assertion for both layers (every search
/// here completes inside the default budget).
async fn serve(tmp: &TempDir) -> Channel {
    serve_with_metrics(tmp, Arc::new(Metrics::disabled())).await
}

/// Like [`serve`] but emitting per-RPC metrics through the given facade.
async fn serve_with_metrics(tmp: &TempDir, metrics: Arc<Metrics>) -> Channel {
    serve_full(tmp, metrics, None).await
}

/// Like [`serve_with_metrics`] but optionally enabling sampled-query recall capture.
async fn serve_full(tmp: &TempDir, metrics: Arc<Metrics>, recall: Option<RecallCapture>) -> Channel {
    drop(telemetry::init_tracing(true, metrics.clone()));
    let config = Config {
        base_uri: tmp.path().display().to_string(),
        dataset_cache_capacity: 16,
        index_cache_bytes: 64 * 1024 * 1024,
        metadata_cache_bytes: 64 * 1024 * 1024,
        port: 0,
        cache_dir: tmp.path().join("disk-cache"),
        disk_index_cache_bytes: 64 * 1024 * 1024,
        disk_store_cache_bytes: 64 * 1024 * 1024,
        cache_backend: search_api::config::CacheBackendKind::Disk,
        redis_url: None,
        redis_namespace: search_api::config::DEFAULT_REDIS_NAMESPACE.to_string(),
        statsd_addr: "127.0.0.1:8125".to_string(),
        telemetry_disabled: true,
        serve_tag_ttl_secs: search_api::config::DEFAULT_SERVE_TAG_TTL_SECS,
    };
    let provider = CachingDatasetProvider::with_telemetry(&config, metrics.clone()).await;
    let backend = Arc::new(LanceSearchBackend::new(provider).with_metrics(metrics.clone()));
    let mut service = SearchGrpc::with_metrics(backend, metrics.clone());
    if let Some(recall) = recall {
        service = service.with_recall(recall);
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<SearchServiceServer<SearchGrpc<Backend>>>()
        .await;
    tokio::spawn(
        Server::builder()
            .layer(OtelGrpcLayer::default().filter(reject_healthcheck))
            .layer(RouteTimeoutLayer::from_defaults(metrics.clone()))
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
            rerank: None,
            time_range: None,
            version_ref: None,
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
            rerank: None,
            time_range: None,
            version_ref: None,
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
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            vector: Some(vector_query(vec![0.0, 1.0, 0.0, 0.0], 0)),
            text: Some(simple_text_query("pear", 0)),
            k: 2,
            fusion: None,
            filter: None,
            filter_mode: 0,
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
            rerank: None,
            time_range: None,
            version_ref: None,
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
            rerank: None,
            time_range: None,
            version_ref: None,
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
            rerank: None,
            time_range: None,
            version_ref: None,
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
            rerank: None,
            time_range: None,
            version_ref: None,
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
            rerank: None,
            time_range: None,
            version_ref: None,
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
            rerank: None,
            time_range: None,
            version_ref: None,
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
            rerank: None,
            time_range: None,
            version_ref: None,
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
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            vector: Some(vector_query(vec![0.0, 1.0, 0.0, 0.0], 0)),
            text: Some(simple_text_query("pear", 0)),
            k: 2,
            fusion: Some(Fusion {
                strategy: Some(fusion::Strategy::Rrf(RrfFusion { rrf_k: Some(1.0) })),
            }),
            filter: None,
            filter_mode: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.results.len(), 2);
    assert_eq!(row_number(&response.results[0].row, "id"), 2.0);
    assert!((response.results[0].fused_score - 1.0).abs() < 1e-9);

    let status = client
        .hybrid_search(HybridSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            vector: Some(vector_query(vec![0.0, 1.0, 0.0, 0.0], 0)),
            text: Some(simple_text_query("pear", 0)),
            k: 2,
            fusion: Some(Fusion {
                strategy: Some(fusion::Strategy::Rrf(RrfFusion { rrf_k: Some(-3.0) })),
            }),
            filter: None,
            filter_mode: 0,
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
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
            version_ref: None,
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
            rerank: None,
            time_range: None,
            version_ref: None,
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
            version_ref: None,
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
            version_ref: None,
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
            version_ref: None,
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
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
    let head_version = dataset.version_id();
    dataset
        .tags()
        .create(search_api::config::PRODUCTION_SERVE_TAG, head_version)
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
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("absent"),
            query: Some(vector_query(vec![1.0, 0.0, 0.0, 0.0], 1)),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::NotFound);
    assert!(status.message().contains("Dataset"), "unexpected error: {status}");

    let status = client
        .text_search(TextSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("../escape"),
            query: Some(simple_text_query("x", 1)),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("org_id"));

    let status = client
        .text_search(TextSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: None,
            query: Some(simple_text_query("x", 1)),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("target"));
}

#[tokio::test]
async fn k_and_offset_above_the_configured_ceiling_are_rejected() {
    let tmp = TempDir::new().unwrap();
    build_test_dataset(&org1_uri(&tmp)).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);
    let max_k = search_api::config::DEFAULT_SEARCH_MAX_K as u32;

    let status = client
        .vector_search(VectorSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            query: Some(vector_query(vec![1.0, 0.0, 0.0, 0.0], max_k + 1)),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains('k'), "unexpected error: {status}");

    let status = client
        .text_search(TextSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            query: Some(simple_text_query("lemon", max_k + 1)),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);

    let status = client
        .hybrid_search(HybridSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            vector: Some(vector_query(vec![1.0, 0.0, 0.0, 0.0], 0)),
            text: Some(simple_text_query("lemon", 0)),
            k: max_k + 1,
            fusion: None,
            filter: None,
            filter_mode: 0,
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);

    let mut query = vector_query(vec![1.0, 0.0, 0.0, 0.0], 5);
    query.offset = Some(u64::MAX);
    let status = client
        .vector_search(VectorSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            query: Some(query),
        })
        .await
        .unwrap_err();
    assert_eq!(
        status.code(),
        Code::InvalidArgument,
        "an absurd offset must be rejected, not overflow: {status}"
    );
}

#[tokio::test]
async fn recall_capture_samples_vector_searches() {
    let tmp = TempDir::new().unwrap();
    build_test_dataset(&org1_uri(&tmp)).await;
    let (receiver, sink) = cadence::SpyMetricSink::new();
    let metrics = Arc::new(Metrics::from_sink(sink));
    let captured: Arc<std::sync::Mutex<Vec<RecallRecord>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let records_sink = captured.clone();
    let recall = RecallCapture::new(1.0, "vector_id", metrics.clone()).with_hook(Arc::new(move |record| {
        records_sink.lock().unwrap().push(record.clone());
    }));
    let channel = serve_full(&tmp, metrics, Some(recall)).await;
    let mut client = SearchServiceClient::new(channel);

    let mut query = vector_query(vec![1.0, 0.0, 0.0, 0.0], 2);
    query.filter = Some(compare_filter("id", CompareOp::Ge, 1));
    let response = client
        .vector_search(VectorSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            query: Some(query),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.results.len(), 2, "capture must not alter results");

    {
        let records = captured.lock().unwrap();
        assert_eq!(records.len(), 1, "rate 1.0 must sample every eligible request");
        let record = &records[0];
        assert_eq!(record.query_type, RecallQueryType::Vector);
        assert_eq!(record.org_id, "org1");
        assert_eq!(record.tenant_id, "tenant1");
        assert_eq!(record.namespace, "ns1");
        assert_eq!(record.k, 2);
        assert!(!record.sample_id.is_empty());
        assert!(record.captured_at_unix_ms > 0);
        assert!(
            record.dataset_version.is_some(),
            "the served dataset version must be captured"
        );
        assert_eq!(record.query_vector_json.as_deref(), Some("[1.0,0.0,0.0,0.0]"));
        assert_eq!(record.text_query_json, None, "vector samples carry no text query");
        assert_eq!(
            record.result_scores_json, None,
            "vector samples carry distances, not scores"
        );
        assert_eq!(
            record.filter_json.as_deref(),
            Some(r#"{"compare":{"column":"id","op":"ge","value":{"int":1}}}"#)
        );
        let ids: Vec<serde_json::Value> = serde_json::from_str(&record.result_ids_json).unwrap();
        assert_eq!(ids.len(), 2, "one id per served hit, in rank order");
        assert_eq!(ids[0], serde_json::json!(1), "rank 1 must be the exact match");
        let distances: Vec<f64> = serde_json::from_str(record.result_distances_json.as_deref().unwrap()).unwrap();
        assert_eq!(distances.len(), 2);
        assert!(distances[0] <= distances[1]);
    }

    let response = client
        .vector_search(VectorSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            query: Some(vector_query(vec![0.0, 1.0, 0.0, 0.0], 1)),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.results.len(), 1);
    {
        let records = captured.lock().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].filter_json, None, "unfiltered requests carry no filter");
        let ids: Vec<serde_json::Value> = serde_json::from_str(&records[1].result_ids_json).unwrap();
        assert_eq!(ids, vec![serde_json::json!(2)]);
    }

    let mut lines = Vec::new();
    while let Ok(packet) = receiver.try_recv() {
        lines.push(String::from_utf8(packet).unwrap());
    }
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("search_api.recall.samples:1|c")
                && line.contains("query_type:vector")
                && line.contains("filtered:true")),
        "missing filtered recall sample count: {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("search_api.recall.samples:1|c")
                && line.contains("query_type:vector")
                && line.contains("filtered:false")),
        "missing unfiltered recall sample count: {lines:?}"
    );
    assert_eq!(
        lines.iter().filter(|line| line.contains("recall.samples")).count(),
        2,
        "exactly the two eligible vector requests must be counted: {lines:?}"
    );
}

#[tokio::test]
async fn recall_capture_samples_text_and_hybrid_with_new_attributes() {
    let tmp = TempDir::new().unwrap();
    build_test_dataset(&org1_uri(&tmp)).await;
    let (receiver, sink) = cadence::SpyMetricSink::new();
    let metrics = Arc::new(Metrics::from_sink(sink));
    let captured: Arc<std::sync::Mutex<Vec<RecallRecord>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let records_sink = captured.clone();
    let recall = RecallCapture::new(1.0, "vector_id", metrics.clone()).with_hook(Arc::new(move |record| {
        records_sink.lock().unwrap().push(record.clone());
    }));
    let channel = serve_full(&tmp, metrics, Some(recall)).await;
    let mut client = SearchServiceClient::new(channel);

    client
        .text_search(TextSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            query: Some(simple_text_query("lemon", 3)),
        })
        .await
        .unwrap();
    client
        .hybrid_search(HybridSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            vector: Some(vector_query(vec![0.0, 1.0, 0.0, 0.0], 0)),
            text: Some(simple_text_query("pear", 0)),
            k: 2,
            fusion: Some(Fusion {
                strategy: Some(fusion::Strategy::Weighted(search_api::pb::WeightedFusion {
                    vector_weight: Some(0.7),
                })),
            }),
            filter: None,
            filter_mode: 0,
        })
        .await
        .unwrap();

    let records = captured.lock().unwrap();
    assert_eq!(
        records.len(),
        2,
        "rate 1.0 must sample both the text and hybrid requests"
    );

    let text = &records[0];
    assert_eq!(text.query_type, RecallQueryType::Text);
    assert!(
        text.dataset_version.is_some(),
        "text samples must capture the served version"
    );
    assert_eq!(text.query_vector_json, None, "text samples carry no query vector");
    assert_eq!(text.fusion_json, None, "text samples carry no fusion");
    let text_query: serde_json::Value = serde_json::from_str(text.text_query_json.as_deref().unwrap()).unwrap();
    assert_eq!(text_query["match"]["terms"], serde_json::json!("lemon"));
    let text_columns: Vec<String> = serde_json::from_str(text.text_columns_json.as_deref().unwrap()).unwrap();
    assert_eq!(text_columns, vec!["text".to_string()]);
    let ids: Vec<serde_json::Value> = serde_json::from_str(&text.result_ids_json).unwrap();
    assert_eq!(ids, vec![serde_json::json!(4)], "lemon matches only row 4");
    let scores: Vec<f64> = serde_json::from_str(text.result_scores_json.as_deref().unwrap()).unwrap();
    assert_eq!(scores.len(), 1);
    assert!(scores[0] > 0.0, "text relevance score must be positive");
    assert_eq!(
        text.result_distances_json, None,
        "text samples carry scores, not distances"
    );

    let hybrid = &records[1];
    assert_eq!(hybrid.query_type, RecallQueryType::Hybrid);
    assert!(hybrid.dataset_version.is_some());
    assert_eq!(hybrid.query_vector_json.as_deref(), Some("[0.0,1.0,0.0,0.0]"));
    let hybrid_query: serde_json::Value = serde_json::from_str(hybrid.text_query_json.as_deref().unwrap()).unwrap();
    assert_eq!(hybrid_query["match"]["terms"], serde_json::json!("pear"));
    assert_eq!(
        hybrid.fusion_json.as_deref(),
        Some(r#"{"weighted":{"vector_weight":0.7}}"#)
    );
    let hybrid_scores: Vec<f64> = serde_json::from_str(hybrid.result_scores_json.as_deref().unwrap()).unwrap();
    assert!(!hybrid_scores.is_empty(), "hybrid samples must record fused scores");
    assert_eq!(hybrid.result_distances_json, None);

    let mut lines = Vec::new();
    while let Ok(packet) = receiver.try_recv() {
        lines.push(String::from_utf8(packet).unwrap());
    }
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("search_api.recall.samples:1|c") && line.contains("query_type:text")),
        "missing text recall sample count: {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("search_api.recall.samples:1|c") && line.contains("query_type:hybrid")),
        "missing hybrid recall sample count: {lines:?}"
    );
}

#[tokio::test]
async fn instrumented_server_emits_rpc_metrics_and_passes_requests_through() {
    let tmp = TempDir::new().unwrap();
    build_test_dataset(&org1_uri(&tmp)).await;
    let (receiver, sink) = cadence::SpyMetricSink::new();
    let channel = serve_with_metrics(&tmp, Arc::new(Metrics::from_sink(sink))).await;
    let mut client = SearchServiceClient::new(channel.clone());

    let response = client
        .vector_search(VectorSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            query: Some(vector_query(vec![1.0, 0.0, 0.0, 0.0], 2)),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.results.len(), 2, "instrumentation must not alter results");

    let status = client
        .text_search(TextSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
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
        lines
            .iter()
            .any(|line| line.starts_with("search_api.query.iops:") && line.contains("rpc:vector_search")),
        "missing per-query iops distribution from the lance execution-stats callback: {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("search_api.query.bytes_read:") && line.contains("rpc:vector_search")),
        "missing per-query bytes_read distribution: {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("search_api.query.parts_loaded:") && line.contains("rpc:vector_search")),
        "missing per-query parts_loaded distribution: {lines:?}"
    );
    assert!(
        !lines.iter().any(|line| line.contains("org_id")),
        "org_id must never appear on metrics: {lines:?}"
    );
}

/// Builds a string literal for use in filter predicates.
fn string_literal(text: &str) -> LiteralValue {
    LiteralValue {
        kind: Some(literal_value::Kind::StringValue(text.to_string())),
    }
}

/// Builds a `column = "string"` equality filter.
fn string_eq_filter(column: &str, value: &str) -> Filter {
    Filter {
        predicate: Some(filter::Predicate::Comparison(Comparison {
            column: column.to_string(),
            op: CompareOp::Eq as i32,
            value: Some(string_literal(value)),
        })),
    }
}

#[tokio::test]
async fn vector_search_string_equality_filter_returns_matching_rows_only() {
    let tmp = TempDir::new().unwrap();
    build_test_dataset(&org1_uri(&tmp)).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let mut query = vector_query(vec![0.0, 1.0, 0.0, 0.0], 4);
    query.filter = Some(string_eq_filter("text", "red apple pie"));
    let response = client
        .vector_search(VectorSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            query: Some(query),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.results.len(),
        1,
        "string equality filter must return only the matching row"
    );
    assert_eq!(
        row_number(&response.results[0].row, "id"),
        1.0,
        "only row 1 has text = 'red apple pie'"
    );

    let mut query = vector_query(vec![1.0, 0.0, 0.0, 0.0], 4);
    query.filter = Some(string_eq_filter("text", "no such text value"));
    let response = client
        .vector_search(VectorSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            query: Some(query),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        response.results.is_empty(),
        "a string equality filter with no matching value must return zero hits"
    );
}

#[tokio::test]
async fn hybrid_request_level_filter_applies_to_both_legs() {
    let tmp = TempDir::new().unwrap();
    build_test_dataset(&org1_uri(&tmp)).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let response = client
        .hybrid_search(HybridSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            vector: Some(vector_query(vec![0.0, 1.0, 0.0, 0.0], 0)),
            text: Some(simple_text_query("pear", 0)),
            k: 4,
            fusion: None,
            filter: Some(string_eq_filter("text", "green pear tart")),
            filter_mode: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.results.len(),
        1,
        "request-level string equality filter must restrict both legs to the matching row"
    );
    assert_eq!(
        row_number(&response.results[0].row, "id"),
        2.0,
        "only row 2 has text = 'green pear tart'"
    );

    let unfiltered = client
        .hybrid_search(HybridSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            vector: Some(vector_query(vec![0.0, 1.0, 0.0, 0.0], 0)),
            text: Some(simple_text_query("pear", 0)),
            k: 4,
            fusion: None,
            filter: None,
            filter_mode: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        unfiltered.results.len() >= response.results.len(),
        "removing the request-level filter must not reduce the result count"
    );

    let response_combined = client
        .hybrid_search(HybridSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            vector: Some({
                let mut q = vector_query(vec![0.0, 1.0, 0.0, 0.0], 0);
                q.filter = Some(compare_filter("id", CompareOp::Le, 3));
                q
            }),
            text: Some(simple_text_query("pear", 0)),
            k: 4,
            fusion: None,
            filter: Some(string_eq_filter("text", "green pear tart")),
            filter_mode: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_combined.results.len(),
        1,
        "per-leg filter ANDed with request-level filter must still return only the matching row"
    );
    assert_eq!(
        row_number(&response_combined.results[0].row, "id"),
        2.0,
        "the combined AND predicate must resolve to row 2 only"
    );
}

/// Builds an identity rerank spec proto, optionally truncating to `top_n`.
fn identity_rerank(top_n: Option<u64>) -> Option<Rerank> {
    Some(Rerank {
        strategy: Some(rerank::Strategy::Identity(IdentityRerank { top_n })),
    })
}

#[tokio::test]
async fn rerank_top_n_truncates_and_absent_top_n_leaves_results_unchanged() {
    let tmp = TempDir::new().unwrap();
    build_test_dataset(&org1_uri(&tmp)).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let baseline = client
        .vector_search(VectorSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            query: Some(vector_query(vec![1.0, 0.0, 0.0, 0.0], 4)),
        })
        .await
        .unwrap()
        .into_inner();
    let baseline_ids: Vec<f64> = baseline.results.iter().map(|hit| row_number(&hit.row, "id")).collect();
    assert_eq!(baseline_ids.len(), 4, "without a rerank spec no truncation must occur");

    let truncated = client
        .vector_search(VectorSearchRequest {
            rerank: identity_rerank(Some(2)),
            time_range: None,
            version_ref: None,
            target: target("org1"),
            query: Some(vector_query(vec![1.0, 0.0, 0.0, 0.0], 4)),
        })
        .await
        .unwrap()
        .into_inner();
    let truncated_ids: Vec<f64> = truncated.results.iter().map(|hit| row_number(&hit.row, "id")).collect();
    assert_eq!(
        truncated_ids,
        baseline_ids[..2],
        "an identity rerank spec with top_n must keep the leading candidates in order"
    );
}

#[tokio::test]
async fn weighted_fusion_proto_variant_is_applied() {
    let tmp = TempDir::new().unwrap();
    build_test_dataset(&org1_uri(&tmp)).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let response = client
        .hybrid_search(HybridSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            vector: Some(vector_query(vec![0.0, 1.0, 0.0, 0.0], 0)),
            text: Some(simple_text_query("pear", 0)),
            k: 2,
            fusion: Some(Fusion {
                strategy: Some(fusion::Strategy::Weighted(WeightedFusion {
                    vector_weight: Some(1.0),
                })),
            }),
            filter: None,
            filter_mode: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.results.len(), 2);
    assert_eq!(
        row_number(&response.results[0].row, "id"),
        2.0,
        "row 2 is both the nearest vector and the pear match, so it tops weighted fusion"
    );
    assert!(response.results[0].fused_score >= response.results[1].fused_score);

    let status = client
        .hybrid_search(HybridSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            vector: Some(vector_query(vec![0.0, 1.0, 0.0, 0.0], 0)),
            text: Some(simple_text_query("pear", 0)),
            k: 2,
            fusion: Some(Fusion {
                strategy: Some(fusion::Strategy::Weighted(WeightedFusion {
                    vector_weight: Some(1.5),
                })),
            }),
            filter: None,
            filter_mode: 0,
        })
        .await
        .unwrap_err();
    assert_eq!(
        status.code(),
        Code::InvalidArgument,
        "vector_weight outside [0,1] must be rejected"
    );
}

/// Appends one extra row at `uri`, producing a new committed version beyond any tagged one.
async fn append_row(uri: &str, row: (i32, i32, &str, [f32; 4])) {
    let schema = test_schema();
    let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        vec![Some(row.3.iter().map(|value| Some(*value)).collect::<Vec<_>>())],
        DIM,
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![row.0])),
            Arc::new(Int32Array::from(vec![row.1])),
            Arc::new(StringArray::from(vec![row.2])),
            Arc::new(vectors),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    Dataset::write(
        reader,
        uri,
        Some(WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn version_ref_pins_a_search_to_a_tagged_or_explicit_snapshot() {
    let tmp = TempDir::new().unwrap();
    let uri = org1_uri(&tmp);
    build_test_dataset(&uri).await;
    let dataset = Dataset::open(&uri).await.unwrap();
    let tagged = dataset.version_id();
    dataset.tags().create("20260611T120000Z", tagged).await.unwrap();
    append_row(&uri, (9, 9, "purple grape jam", [0.9, 0.9, 0.0, 0.0])).await;

    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let served = client
        .vector_search(VectorSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: None,
            target: target("org1"),
            query: Some(vector_query(vec![0.9, 0.9, 0.0, 0.0], 5)),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        served.results.len(),
        4,
        "an unpinned search must resolve HEAD rather than the later commit"
    );
    assert!(
        served.results.iter().all(|hit| row_number(&hit.row, "id") != 9.0),
        "the post-HEAD append must be invisible to production serving"
    );

    let at_tag = client
        .vector_search(VectorSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: Some(search_api::pb::vector_search_request::VersionRef::Tag(
                "20260611T120000Z".to_string(),
            )),
            target: target("org1"),
            query: Some(vector_query(vec![0.9, 0.9, 0.0, 0.0], 5)),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        at_tag.results.len(),
        4,
        "a tag-pinned search must only see the tagged snapshot's rows"
    );
    assert!(
        at_tag.results.iter().all(|hit| row_number(&hit.row, "id") != 9.0),
        "the appended row must be invisible at the tagged snapshot"
    );

    let at_version = client
        .vector_search(VectorSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: Some(search_api::pb::vector_search_request::VersionRef::Version(tagged)),
            target: target("org1"),
            query: Some(vector_query(vec![0.9, 0.9, 0.0, 0.0], 5)),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        at_version.results.len(),
        4,
        "an explicit version pin behaves like its tag"
    );

    let text_at_tag = client
        .text_search(TextSearchRequest {
            rerank: None,
            time_range: None,
            version_ref: Some(search_api::pb::text_search_request::VersionRef::Tag(
                "20260611T120000Z".to_string(),
            )),
            target: target("org1"),
            query: Some(simple_text_query("lemon", 3)),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        text_at_tag.results.len(),
        1,
        "a tag-pinned text search must serve FTS hits from the tagged snapshot"
    );
}
