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
use search_api::domain::{DatasetRef, SearchBackend, VectorQuery};
use search_api::lance::{CachingDatasetProvider, LanceSearchBackend};
use tempfile::TempDir;

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
    let provider = CachingDatasetProvider::with_inner_store_wrapper(&config, None);
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
    let provider = CachingDatasetProvider::with_inner_store_wrapper(&config, None);
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
    let provider = CachingDatasetProvider::with_inner_store_wrapper(&config, None);
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
async fn unset_reference_follows_serve_policy() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;

    let dataset = Dataset::open(&uri).await.unwrap();
    let latest = dataset.version_id();

    let config = test_config(data_tmp.path(), cache_tmp.path());
    let provider = CachingDatasetProvider::with_inner_store_wrapper(&config, None);
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
