//! Integration tests: query-at-tag/version pins the correct dataset snapshot.
//!
//! These tests verify that `VectorQuery::reference` routes to the right committed version,
//! exercising the full backend stack (LanceSearchBackend + CachingDatasetProvider + real tags)
//! without spinning up a gRPC server. The convert-level unit tests for the proto oneofs live in
//! the `#[cfg(test)]` section of `src/grpc/convert.rs`.

mod common;

use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{FixedSizeListArray, Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use common::{DIM, TEST_DATASET_PATH, build_indexed_dataset, test_config, test_target};
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use search_api::config::Config;
use search_api::domain::{DatasetRef, HybridQuery, SearchBackend, TextQuery, VectorQuery};
use search_api::lance::{CachingDatasetProvider, DatasetProvider, LanceSearchBackend};
use tempfile::TempDir;

/// Like [`test_config`] but serving through the `prod` tag.
fn serve_by_tag_config(data_root: &std::path::Path, cache_root: &std::path::Path) -> Config {
    let mut config = test_config(data_root, cache_root);
    config.serve_by_tag = true;
    config.serve_tag = "prod".to_string();
    config
}

/// A small nearest-neighbor probe using flat search (no index required), identical to the probe
/// in `blue_green.rs` so the dataset helpers can be shared without modification.
fn probe_with_ref(reference: DatasetRef) -> VectorQuery {
    VectorQuery {
        vector: vec![1.0, 0.0, 0.0, 0.0],
        k: 4,
        bypass_vector_index: true,
        reference,
        ..Default::default()
    }
}

/// Appends two extra rows to an existing dataset at `uri`, producing a new committed version.
async fn append_rows(uri: &str) {
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
            Some(vec![Some(0.5), Some(0.5), Some(0.0), Some(0.0)]),
            Some(vec![Some(0.0), Some(0.5), Some(0.5), Some(0.0)]),
        ],
        DIM,
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![5, 6])),
            Arc::new(StringArray::from(vec!["extra row five", "extra row six"])),
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
async fn vector_search_at_tag_pins_the_tagged_version() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;

    let dataset = Dataset::open(&uri).await.unwrap();
    let v1 = 1u64;
    let latest = dataset.version_id();
    assert!(latest >= 2, "index builds must produce several versions, got {latest}");
    dataset.tags().create("t1", v1).await.unwrap();

    let config = test_config(data_tmp.path(), cache_tmp.path());
    let provider = CachingDatasetProvider::with_inner_store_wrapper(&config, None).await;
    let backend = LanceSearchBackend::new(provider);
    let target = test_target();

    let at_tag = backend
        .vector_search(&target, probe_with_ref(DatasetRef::Tag("t1".to_string())))
        .await
        .unwrap();
    assert_eq!(
        at_tag.dataset_version,
        Some(v1),
        "tag-pinned search must open the version the tag resolves to"
    );

    let at_serve = backend
        .vector_search(&target, probe_with_ref(DatasetRef::Serve))
        .await
        .unwrap();
    assert_eq!(
        at_serve.dataset_version,
        Some(latest),
        "Serve with no serve-by-tag enabled must return the latest version"
    );

    assert!(
        at_tag.dataset_version != at_serve.dataset_version,
        "the tag and the latest version must differ for this test to be meaningful"
    );
}

#[tokio::test]
async fn vector_search_at_explicit_version_pins_it() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;

    let dataset = Dataset::open(&uri).await.unwrap();
    let latest = dataset.version_id();
    assert!(latest >= 2, "index builds must produce several versions, got {latest}");

    let config = test_config(data_tmp.path(), cache_tmp.path());
    let provider = CachingDatasetProvider::with_inner_store_wrapper(&config, None).await;
    let backend = LanceSearchBackend::new(provider);
    let target = test_target();

    let at_v1 = backend
        .vector_search(&target, probe_with_ref(DatasetRef::Version(1)))
        .await
        .unwrap();
    assert_eq!(
        at_v1.dataset_version,
        Some(1),
        "version-pinned search must open exactly that committed version"
    );

    let at_latest = backend
        .vector_search(&target, probe_with_ref(DatasetRef::Latest))
        .await
        .unwrap();
    assert_eq!(
        at_latest.dataset_version,
        Some(latest),
        "Latest reference must open the most recently committed version"
    );

    assert!(
        at_v1.dataset_version != at_latest.dataset_version,
        "version 1 and latest must differ for this test to be meaningful"
    );
}

#[tokio::test]
async fn older_tag_remains_queryable_after_a_later_commit() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;

    let dataset = Dataset::open(&uri).await.unwrap();
    let after_first_build = dataset.version_id();
    assert!(
        after_first_build >= 2,
        "index builds must produce several versions, got {after_first_build}"
    );
    dataset.tags().create("t1", 1u64).await.unwrap();

    let plain_uri = format!("{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    append_rows(&plain_uri).await;
    let dataset2 = Dataset::open(&plain_uri).await.unwrap();
    let after_append = dataset2.version_id();
    assert!(after_append > after_first_build, "append must produce a newer version");

    let config = test_config(data_tmp.path(), cache_tmp.path());
    let provider = CachingDatasetProvider::with_inner_store_wrapper(&config, None).await;
    let backend = LanceSearchBackend::new(provider);
    let target = test_target();

    let at_old_tag = backend
        .vector_search(&target, probe_with_ref(DatasetRef::Tag("t1".to_string())))
        .await
        .unwrap();
    assert_eq!(
        at_old_tag.dataset_version,
        Some(1),
        "the older tag must still resolve to version 1 after a later commit"
    );
    assert!(
        !at_old_tag.hits.is_empty(),
        "the old tagged version must still return hits"
    );

    let at_latest = backend
        .vector_search(&target, probe_with_ref(DatasetRef::Latest))
        .await
        .unwrap();
    assert_eq!(
        at_latest.dataset_version,
        Some(after_append),
        "Latest must resolve to the most recent committed version after the append"
    );

    assert!(
        at_old_tag.dataset_version != at_latest.dataset_version,
        "the old tag and latest must resolve to different versions"
    );
}

#[tokio::test]
async fn text_search_at_tag_pins_the_tagged_version() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;

    let dataset = Dataset::open(&uri).await.unwrap();
    let tagged = dataset.version_id();
    dataset.tags().create("t1", tagged).await.unwrap();
    let plain_uri = format!("{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    append_rows(&plain_uri).await;
    let latest = Dataset::open(&plain_uri).await.unwrap().version_id();
    assert!(latest > tagged, "append must produce a newer version");

    let config = test_config(data_tmp.path(), cache_tmp.path());
    let provider = CachingDatasetProvider::with_inner_store_wrapper(&config, None).await;
    let backend = LanceSearchBackend::new(provider);
    let target = test_target();

    let mut query = TextQuery::simple("lemon", 4);
    query.reference = DatasetRef::Tag("t1".to_string());
    let at_tag = backend.text_search(&target, query).await.unwrap();
    assert_eq!(
        at_tag.dataset_version,
        Some(tagged),
        "tag-pinned text search must open the tagged snapshot"
    );
    assert_eq!(at_tag.hits.len(), 1, "the tagged snapshot must serve FTS hits");

    let at_serve = backend
        .text_search(&target, TextQuery::simple("lemon", 4))
        .await
        .unwrap();
    assert_eq!(
        at_serve.dataset_version,
        Some(latest),
        "an unset reference must serve the latest version"
    );
}

#[tokio::test]
async fn hybrid_search_at_tag_opens_both_legs_at_the_pinned_snapshot() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;

    let dataset = Dataset::open(&uri).await.unwrap();
    let tagged = dataset.version_id();
    dataset.tags().create("t1", tagged).await.unwrap();
    let plain_uri = format!("{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    append_rows(&plain_uri).await;

    let config = test_config(data_tmp.path(), cache_tmp.path());
    let provider = CachingDatasetProvider::with_inner_store_wrapper(&config, None).await;
    let backend = LanceSearchBackend::new(provider);
    let target = test_target();

    let query = HybridQuery {
        vector: probe_with_ref(DatasetRef::Serve),
        text: TextQuery::simple("lemon", 4),
        k: 4,
        fusion: Default::default(),
        reference: DatasetRef::Tag("t1".to_string()),
    };
    let outcome = backend.hybrid_search(&target, query).await.unwrap();
    assert_eq!(
        outcome.dataset_version,
        Some(tagged),
        "a hybrid search pinned to a tag must fuse both legs at the tagged snapshot"
    );
    assert!(!outcome.hits.is_empty(), "the pinned snapshot must produce fused hits");
}

#[tokio::test]
async fn serve_by_tag_resolves_the_serve_tag_while_an_explicit_tag_overrides_it() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;

    let dataset = Dataset::open(&uri).await.unwrap();
    let prod_version = dataset.version_id();
    dataset.tags().create("prod", prod_version).await.unwrap();
    dataset.tags().create("t1", 1u64).await.unwrap();
    let plain_uri = format!("{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    append_rows(&plain_uri).await;

    let config = serve_by_tag_config(data_tmp.path(), cache_tmp.path());
    let provider = CachingDatasetProvider::with_inner_store_wrapper(&config, None).await;
    let backend = LanceSearchBackend::new(provider);
    let target = test_target();

    let at_serve = backend
        .vector_search(&target, probe_with_ref(DatasetRef::Serve))
        .await
        .unwrap();
    assert_eq!(
        at_serve.dataset_version,
        Some(prod_version),
        "Serve with serve-by-tag on must resolve the prod tag, not latest"
    );

    let at_pin = backend
        .vector_search(&target, probe_with_ref(DatasetRef::Tag("t1".to_string())))
        .await
        .unwrap();
    assert_eq!(
        at_pin.dataset_version,
        Some(1),
        "an explicit tag pin must override the serve policy"
    );
}

#[tokio::test]
async fn pinned_tag_and_serve_handles_coexist_in_the_handle_cache() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;

    let dataset = Dataset::open(&uri).await.unwrap();
    dataset.tags().create("t1", 1u64).await.unwrap();

    let config = test_config(data_tmp.path(), cache_tmp.path());
    let provider = CachingDatasetProvider::with_inner_store_wrapper(&config, None).await;
    let target = test_target();

    provider.dataset(&target, DatasetRef::Serve).await.unwrap();
    provider
        .dataset(&target, DatasetRef::Tag("t1".to_string()))
        .await
        .unwrap();
    let (entries, _) = provider.handle_cache_stats().await;
    assert_eq!(
        entries, 2,
        "the serve handle and the tag-pinned handle must coexist under distinct version keys"
    );

    provider.dataset(&target, DatasetRef::Serve).await.unwrap();
    provider
        .dataset(&target, DatasetRef::Tag("t1".to_string()))
        .await
        .unwrap();
    let (entries_after, _) = provider.handle_cache_stats().await;
    assert_eq!(entries_after, 2, "repeat opens must reuse the cached handles");
}

#[tokio::test]
async fn unset_reference_follows_serve_policy() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;

    let dataset = Dataset::open(&uri).await.unwrap();
    let latest = dataset.version_id();

    let config = test_config(data_tmp.path(), cache_tmp.path());
    let provider = CachingDatasetProvider::with_inner_store_wrapper(&config, None).await;
    let backend = LanceSearchBackend::new(provider);
    let target = test_target();

    let default_query = VectorQuery {
        vector: vec![1.0, 0.0, 0.0, 0.0],
        k: 2,
        bypass_vector_index: true,
        ..Default::default()
    };
    assert_eq!(
        default_query.reference,
        DatasetRef::Serve,
        "default-constructed VectorQuery must carry DatasetRef::Serve"
    );

    let outcome = backend.vector_search(&target, default_query).await.unwrap();
    assert_eq!(
        outcome.dataset_version,
        Some(latest),
        "an unset reference (Serve) with no serve-by-tag config must open the latest version"
    );
}
