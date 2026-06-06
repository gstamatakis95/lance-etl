//! Integration tests: builds a tiny Lance dataset in a tempdir, creates an INVERTED index (with
//! positions) on the text column, serves the gRPC API on a local TCP port, and exercises all
//! three RPCs plus the standard health service with a tonic client.

use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{FixedSizeListArray, Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use lance::index::DatasetIndexExt;
use lance_index::IndexType;
use lance_index::scalar::InvertedIndexParams;
use prost_types::value::Kind;
use search_api::config::Config;
use search_api::grpc::SearchGrpc;
use search_api::lance::{CachingDatasetProvider, LanceSearchBackend};
use search_api::pb::search_service_client::SearchServiceClient;
use search_api::pb::search_service_server::SearchServiceServer;
use search_api::pb::{
    BooleanQuery, CompareOp, Comparison, DistanceType, Filter, FtsQuery, Fusion, HybridSearchRequest, InList,
    LiteralValue, MatchQuery, PhraseQuery, PrewarmRequest, RrfFusion, TextQuery, TextSearchRequest, VectorQuery,
    VectorSearchRequest, filter, fts_query, fusion, literal_value, text_query,
};
use tempfile::TempDir;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;
use tonic::transport::{Channel, Server};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

const DIM: i32 = 4;

/// Backend type wired by the tests: Lance over the caching provider.
type Backend = LanceSearchBackend<CachingDatasetProvider>;

/// Writes a four-row dataset (id, text, vector) at `uri` and creates an INVERTED index with
/// positions on `text` so phrase queries work.
async fn build_test_dataset(uri: &str) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("text", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), DIM),
            false,
        ),
    ]));
    let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        vec![
            Some(vec![Some(1.0), Some(0.0), Some(0.0), Some(0.0)]),
            Some(vec![Some(0.0), Some(1.0), Some(0.0), Some(0.0)]),
            Some(vec![Some(0.0), Some(0.0), Some(1.0), Some(0.0)]),
            Some(vec![Some(0.0), Some(0.0), Some(0.0), Some(1.0)]),
        ],
        DIM,
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                "red apple pie",
                "green pear tart",
                "blue fish stew",
                "yellow lemon cake",
            ])),
            Arc::new(vectors),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let mut dataset = Dataset::write(reader, uri, None).await.unwrap();
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

/// Serves the gRPC API on an ephemeral local port and returns a connected channel.
async fn serve(tmp: &TempDir) -> Channel {
    let config = Config {
        base_uri_template: format!("{}/{{org_id}}.lance", tmp.path().display()),
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
    };
    let provider = CachingDatasetProvider::new(&config);
    let backend = Arc::new(LanceSearchBackend::new(provider));
    let service = SearchGrpc::new(backend);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<SearchServiceServer<SearchGrpc<Backend>>>()
        .await;
    tokio::spawn(
        Server::builder()
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
    let uri = format!("{}/org1.lance", tmp.path().display());
    build_test_dataset(&uri).await;
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
            org_id: "org1".into(),
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
            org_id: "org1".into(),
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
            org_id: "org1".into(),
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
    let uri = format!("{}/org1.lance", tmp.path().display());
    build_test_dataset(&uri).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let mut query = vector_query(vec![0.0, 1.0, 0.0, 0.0], 4);
    query.filter = Some(compare_filter("id", CompareOp::Gt, 2));
    let response = client
        .vector_search(VectorSearchRequest {
            org_id: "org1".into(),
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
            org_id: "org1".into(),
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
            org_id: "org1".into(),
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
    let uri = format!("{}/org1.lance", tmp.path().display());
    build_test_dataset(&uri).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let mut query = vector_query(vec![1.0, 0.0, 0.0, 0.0], 2);
    query.distance_type = DistanceType::Cosine as i32;
    query.with_row_id = true;
    query.projection = vec!["id".to_string()];
    let response = client
        .vector_search(VectorSearchRequest {
            org_id: "org1".into(),
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
            org_id: "org1".into(),
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
    let uri = format!("{}/org1.lance", tmp.path().display());
    build_test_dataset(&uri).await;
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
            org_id: "org1".into(),
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
            org_id: "org1".into(),
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
    let uri = format!("{}/org1.lance", tmp.path().display());
    build_test_dataset(&uri).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let response = client
        .hybrid_search(HybridSearchRequest {
            org_id: "org1".into(),
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
            org_id: "org1".into(),
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
async fn prewarm_rpc_warms_metadata_and_indexes() {
    let tmp = TempDir::new().unwrap();
    let uri = format!("{}/org1.lance", tmp.path().display());
    build_test_dataset(&uri).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let response = client
        .prewarm(PrewarmRequest {
            org_id: "org1".into(),
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
            org_id: "org1".into(),
            query: Some(simple_text_query("lemon", 3)),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.results.len(), 1);
}

#[tokio::test]
async fn prewarm_rpc_reports_per_index_errors_and_org_status_codes() {
    let tmp = TempDir::new().unwrap();
    let uri = format!("{}/org1.lance", tmp.path().display());
    build_test_dataset(&uri).await;
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let response = client
        .prewarm(PrewarmRequest {
            org_id: "org1".into(),
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
            org_id: "absent".into(),
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
            org_id: "../escape".into(),
            metadata: true,
            all_indexes: false,
            index_names: vec![],
            fts_with_position: false,
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn missing_dataset_and_bad_org_return_proper_status_codes() {
    let tmp = TempDir::new().unwrap();
    let channel = serve(&tmp).await;
    let mut client = SearchServiceClient::new(channel);

    let status = client
        .vector_search(VectorSearchRequest {
            org_id: "absent".into(),
            query: Some(vector_query(vec![1.0, 0.0, 0.0, 0.0], 1)),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::NotFound);
    assert!(status.message().contains("Dataset"), "unexpected error: {status}");

    let status = client
        .text_search(TextSearchRequest {
            org_id: "../escape".into(),
            query: Some(simple_text_query("x", 1)),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("org_id"));
}
