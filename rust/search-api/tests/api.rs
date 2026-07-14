//! Production public-surface and exact-catalog integration tests.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use arrow_array::types::Float32Type;
use arrow_array::{BooleanArray, FixedSizeListArray, Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use common::{
    AllowTestAuthorizer, FakeServingCatalog, build_indexed_dataset, test_admission, test_config, test_target,
};
use lance::Dataset;
use search_api::domain::{DatasetRef, DatasetTarget};
use search_api::grpc::auth::{RequestAuthorizer, RequiredRole};
use search_api::grpc::{RouteTimeoutLayer, SearchGrpc};
use search_api::lance::{CachingDatasetProvider, DatasetProvider, LanceSearchBackend};
use search_api::pb::search_service_client::SearchServiceClient;
use search_api::pb::search_service_server::SearchServiceServer;
use search_api::pb::{
    CompareOp, Comparison, DatasetTarget as ProtoTarget, Filter, HybridFusionMode, HybridSearchRequest, LiteralValue,
    TextQuery, TextSearchRequest, VectorQuery, VectorSearchRequest, filter, literal_value, text_query,
};
use search_api::telemetry::Metrics;
use tempfile::TempDir;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};

/// Converts the shared logical target to protobuf.
fn proto_target() -> ProtoTarget {
    ProtoTarget {
        org_id: "org1".to_string(),
        tenant_id: "tenant1".to_string(),
        namespace: "ns1".to_string(),
    }
}

/// Starts the public service over one exact fake catalog route.
async fn serve(
    data_root: &TempDir,
    cache_root: &TempDir,
    uri: &str,
    version: u64,
) -> (Channel, Arc<FakeServingCatalog>) {
    serve_with_authorizer(data_root, cache_root, uri, version, Arc::new(AllowTestAuthorizer)).await
}

/// Starts the public service with an explicit authorization seam.
async fn serve_with_authorizer(
    data_root: &TempDir,
    cache_root: &TempDir,
    uri: &str,
    version: u64,
    authorizer: Arc<dyn RequestAuthorizer>,
) -> (Channel, Arc<FakeServingCatalog>) {
    let config = test_config(data_root.path(), cache_root.path());
    let catalog = Arc::new(FakeServingCatalog::new(test_target(), uri, version));
    let provider = CachingDatasetProvider::with_catalog_and_inner_store_wrapper(&config, catalog.clone(), None).await;
    let metrics = Arc::new(Metrics::disabled());
    let backend = Arc::new(LanceSearchBackend::new(provider).with_metrics(metrics.clone()));
    let service = SearchGrpc::with_metrics(backend, metrics.clone(), authorizer, test_admission());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = TcpListenerStream::new(listener);
    tokio::spawn(
        Server::builder()
            .layer(RouteTimeoutLayer::from_defaults(metrics))
            .add_service(SearchServiceServer::new(service))
            .serve_with_incoming(incoming),
    );
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    (channel, catalog)
}

/// Authorizer that rejects every otherwise valid target.
struct DenyTestAuthorizer;

#[async_trait::async_trait]
impl RequestAuthorizer for DenyTestAuthorizer {
    async fn authorize(
        &self,
        _metadata: &tonic::metadata::MetadataMap,
        _target: &DatasetTarget,
        _required_role: RequiredRole,
    ) -> Result<(), tonic::Status> {
        Err(tonic::Status::permission_denied("denied by test policy"))
    }
}

/// Writes a dataset containing duplicate logical IDs and one deleted nearest neighbor.
async fn build_duplicate_dataset(uri: &str) -> u64 {
    let schema = Arc::new(Schema::new(vec![
        Field::new("vector_id", DataType::Utf8, false),
        Field::new("is_deleted", DataType::Boolean, false),
        Field::new("id", DataType::Int32, false),
        Field::new("text", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            false,
        ),
    ]));
    let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        vec![
            Some(vec![Some(1.0), Some(0.0), Some(0.0), Some(0.0)]),
            Some(vec![Some(0.99), Some(0.01), Some(0.0), Some(0.0)]),
            Some(vec![Some(0.9), Some(0.1), Some(0.0), Some(0.0)]),
            Some(vec![Some(0.8), Some(0.2), Some(0.0), Some(0.0)]),
        ],
        4,
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["deleted", "dup", "dup", "unique"])),
            Arc::new(BooleanArray::from(vec![true, false, false, false])),
            Arc::new(Int32Array::from(vec![0, 1, 2, 3])),
            Arc::new(StringArray::from(vec!["hidden", "apple", "apple", "apple"])),
            Arc::new(vectors),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    Dataset::write(reader, uri, None).await.unwrap().version_id()
}

/// Builds an equality filter over one integer column.
fn id_filter(id: i64) -> Filter {
    Filter {
        predicate: Some(filter::Predicate::Comparison(Comparison {
            column: "id".to_string(),
            op: CompareOp::Eq as i32,
            value: Some(LiteralValue {
                kind: Some(literal_value::Kind::Int64Value(id)),
            }),
        })),
    }
}

#[tokio::test]
async fn all_public_searches_return_typed_ids_and_exact_served_version() {
    let data = TempDir::new().unwrap();
    let cache = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/org1/tenant1/ns1.lance", data.path().display());
    build_indexed_dataset(&uri).await;
    let version = Dataset::open(&uri).await.unwrap().version_id();
    let (channel, _) = serve(&data, &cache, &uri, version).await;
    let mut client = SearchServiceClient::new(channel);

    let vector = client
        .vector_search(VectorSearchRequest {
            target: Some(proto_target()),
            query: Some(VectorQuery {
                vector: vec![1.0, 0.0, 0.0, 0.0],
            }),
            k: 2,
            filter: None,
            projection: vec!["id".to_string()],
            time_range: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(vector.served_version, version);
    assert_eq!(vector.results[0].vector_id, "v1");
    assert_eq!(vector.results[0].projection[0].name, "id");

    let text = client
        .text_search(TextSearchRequest {
            target: Some(proto_target()),
            query: Some(TextQuery {
                columns: vec!["text".to_string()],
                input: Some(text_query::Input::Simple("lemon".to_string())),
            }),
            k: 1,
            filter: None,
            projection: vec!["text".to_string()],
            time_range: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(text.served_version, version);
    assert_eq!(text.results[0].vector_id, "v4");

    let hybrid = client
        .hybrid_search(HybridSearchRequest {
            target: Some(proto_target()),
            vector: Some(VectorQuery {
                vector: vec![0.0, 1.0, 0.0, 0.0],
            }),
            text: Some(TextQuery {
                columns: vec!["text".to_string()],
                input: Some(text_query::Input::Simple("pear".to_string())),
            }),
            k: 2,
            time_range: None,
            filter: None,
            fusion_mode: HybridFusionMode::Balanced as i32,
            projection: vec!["id".to_string()],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(hybrid.served_version, version);
    assert_eq!(hybrid.results[0].vector_id, "v2");
}

#[tokio::test]
async fn target_validation_happens_before_catalog_access() {
    let data = TempDir::new().unwrap();
    let cache = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/org1/tenant1/ns1.lance", data.path().display());
    build_indexed_dataset(&uri).await;
    let version = Dataset::open(&uri).await.unwrap().version_id();
    let (channel, catalog) = serve(&data, &cache, &uri, version).await;
    let mut client = SearchServiceClient::new(channel);
    let status = client
        .vector_search(VectorSearchRequest {
            target: Some(ProtoTarget {
                org_id: "../escape".to_string(),
                tenant_id: "tenant1".to_string(),
                namespace: "ns1".to_string(),
            }),
            query: Some(VectorQuery { vector: vec![1.0; 4] }),
            k: 1,
            filter: None,
            projection: Vec::new(),
            time_range: None,
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(catalog.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cross_target_authorization_fails_before_catalog_or_object_store_access() {
    let data = TempDir::new().unwrap();
    let cache = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/org1/tenant1/ns1.lance", data.path().display());
    build_indexed_dataset(&uri).await;
    let version = Dataset::open(&uri).await.unwrap().version_id();
    let (channel, catalog) = serve_with_authorizer(&data, &cache, &uri, version, Arc::new(DenyTestAuthorizer)).await;
    let status = SearchServiceClient::new(channel)
        .vector_search(VectorSearchRequest {
            target: Some(proto_target()),
            query: Some(VectorQuery { vector: vec![1.0; 4] }),
            k: 1,
            filter: None,
            projection: Vec::new(),
            time_range: None,
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert_eq!(catalog.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn duplicate_rows_are_deduplicated_and_deleted_rows_never_surface() {
    let data = TempDir::new().unwrap();
    let cache = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/org1/tenant1/ns1.lance", data.path().display());
    let version = build_duplicate_dataset(&uri).await;
    let (channel, _) = serve(&data, &cache, &uri, version).await;
    let mut client = SearchServiceClient::new(channel);
    let response = client
        .vector_search(VectorSearchRequest {
            target: Some(proto_target()),
            query: Some(VectorQuery {
                vector: vec![1.0, 0.0, 0.0, 0.0],
            }),
            k: 3,
            filter: None,
            projection: vec!["id".to_string()],
            time_range: None,
        })
        .await
        .unwrap()
        .into_inner();
    let ids: Vec<&str> = response
        .results
        .iter()
        .map(|result| result.vector_id.as_str())
        .collect();
    assert_eq!(ids, vec!["dup", "unique"]);
    assert!(!ids.contains(&"deleted"));
    assert!(response.partial);
    assert_eq!(
        response.warnings,
        vec![search_api::pb::SearchWarning::ResultsUnderfilled as i32]
    );
}

#[tokio::test]
async fn request_filter_is_applied_with_the_mandatory_live_row_filter() {
    let data = TempDir::new().unwrap();
    let cache = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/org1/tenant1/ns1.lance", data.path().display());
    build_indexed_dataset(&uri).await;
    let version = Dataset::open(&uri).await.unwrap().version_id();
    let (channel, _) = serve(&data, &cache, &uri, version).await;
    let mut client = SearchServiceClient::new(channel);
    let response = client
        .vector_search(VectorSearchRequest {
            target: Some(proto_target()),
            query: Some(VectorQuery { vector: vec![1.0; 4] }),
            k: 2,
            filter: Some(id_filter(3)),
            projection: vec!["id".to_string()],
            time_range: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!response.partial);
    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].vector_id, "v3");
}

#[tokio::test]
async fn catalog_uri_outside_the_allowlisted_base_fails_before_open() {
    let data = TempDir::new().unwrap();
    let cache = TempDir::new().unwrap();
    let config = test_config(data.path(), cache.path());
    let catalog = Arc::new(FakeServingCatalog::new(
        test_target(),
        "/outside/org1/tenant1/ns1.lance",
        1,
    ));
    let provider = CachingDatasetProvider::with_catalog_and_inner_store_wrapper(&config, catalog, None).await;
    let error = provider.dataset(&test_target(), DatasetRef::Serve).await.unwrap_err();
    assert!(error.to_string().contains("outside the allowed base URI"));
}

#[test]
fn public_proto_has_no_admin_or_execution_selector_surface() {
    let proto = include_str!("../proto/lance_etl/v1/lance_etl.proto");
    for forbidden in [
        "rpc Prewarm",
        "rpc Clusters",
        "google.protobuf.Struct",
        "message RrfFusion",
        "message WeightedFusion",
        "with_row_id =",
        "version_ref",
        "nprobes =",
        "wand_factor =",
        "offset =",
    ] {
        assert!(!proto.contains(forbidden), "public proto still contains {forbidden}");
    }
}

#[test]
fn target_type_is_hashable_for_catalog_cache_keys() {
    let mut values = std::collections::HashSet::new();
    values.insert(DatasetTarget::new("org1", "tenant1", "ns1"));
    assert_eq!(values.len(), 1);
}
