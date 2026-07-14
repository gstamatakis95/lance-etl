//! Integration test for the short negative cache of failed dataset opens: a repeated request for
//! a nonexistent dataset must be answered from the negative cache without touching the object
//! store, and the TTL must bound how long a freshly created dataset stays invisible.

mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use search_api::domain::{DatasetRef, SearchError};
use search_api::lance::{CachingDatasetProvider, DatasetProvider};

async fn write_unpublished_dataset(uri: &str) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    Dataset::write(reader, uri, None).await.unwrap();
}

#[tokio::test]
async fn missing_dataset_opens_are_negatively_cached_and_expire_with_the_ttl() {
    let dataset_root = tempfile::TempDir::new().unwrap();
    let cache_dir = tempfile::TempDir::new().unwrap();
    let config = common::test_config(dataset_root.path(), cache_dir.path());
    let counts = Arc::new(common::ReadCounts::default());
    let wrapper = Arc::new(common::CountingWrapper { counts: counts.clone() });
    let provider = CachingDatasetProvider::with_inner_store_wrapper(&config, Some(wrapper)).await;
    let target = common::test_target();

    let err = provider.dataset(&target, DatasetRef::Latest).await.unwrap_err();
    assert!(matches!(err, SearchError::NotFound(_)), "unexpected error: {err:?}");
    let operations_after_first = counts.total();
    assert!(
        operations_after_first > 0,
        "the first probe of a missing dataset must reach the store"
    );

    for _ in 0..5 {
        let err = provider.dataset(&target, DatasetRef::Latest).await.unwrap_err();
        assert!(matches!(err, SearchError::NotFound(_)));
    }
    assert_eq!(
        counts.total(),
        operations_after_first,
        "repeat probes within the negative-cache TTL must not touch the store"
    );

    let uri = format!("{}/{}", dataset_root.path().display(), common::TEST_DATASET_PATH);
    common::build_indexed_dataset(&uri).await;
    tokio::time::sleep(Duration::from_secs(
        search_api::config::DEFAULT_NEGATIVE_OPEN_TTL_SECS + 1,
    ))
    .await;
    let dataset = provider
        .dataset(&target, DatasetRef::Latest)
        .await
        .expect("after the TTL a freshly created dataset must open normally");
    assert!(dataset.version_id() > 0);
}

#[tokio::test]
async fn missing_tag_opens_are_negatively_cached_within_the_ttl() {
    let dataset_root = tempfile::TempDir::new().unwrap();
    let cache_dir = tempfile::TempDir::new().unwrap();
    let config = common::test_config(dataset_root.path(), cache_dir.path());
    let counts = Arc::new(common::ReadCounts::default());
    let wrapper = Arc::new(common::CountingWrapper { counts: counts.clone() });
    let provider = CachingDatasetProvider::with_inner_store_wrapper(&config, Some(wrapper)).await;
    let target = common::test_target();
    let reference = DatasetRef::Tag("HEAD".to_string());

    let err = provider.dataset(&target, reference.clone()).await.unwrap_err();
    assert!(matches!(err, SearchError::NotFound(_)), "unexpected error: {err:?}");
    let operations_after_first = counts.total();
    assert!(
        operations_after_first > 0,
        "the first probe of a tag on a missing dataset must reach the store"
    );

    for _ in 0..5 {
        let err = provider.dataset(&target, reference.clone()).await.unwrap_err();
        assert!(matches!(err, SearchError::NotFound(_)));
    }
    assert_eq!(
        counts.total(),
        operations_after_first,
        "repeat tag probes within the negative-cache TTL must not touch the store"
    );
}

#[tokio::test]
async fn serve_fails_closed_when_existing_dataset_has_no_head_tag() {
    let dataset_root = tempfile::TempDir::new().unwrap();
    let cache_dir = tempfile::TempDir::new().unwrap();
    let uri = format!("{}/{}", dataset_root.path().display(), common::TEST_DATASET_PATH);
    write_unpublished_dataset(&uri).await;

    let config = common::test_config(dataset_root.path(), cache_dir.path());
    let provider = CachingDatasetProvider::new(&config).await;
    let err = provider
        .dataset(&common::test_target(), DatasetRef::Serve)
        .await
        .unwrap_err();

    assert!(matches!(err, SearchError::NotFound(_)), "unexpected error: {err:?}");
}
