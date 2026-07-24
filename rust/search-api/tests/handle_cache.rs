//! Integration tests for the weighted open-dataset-handle cache.
//!
//! These exercise the provider's Moka weigher: tiny handles weigh one unit and coexist in bulk,
//! while whale handles are clamped so a few of them cannot consume the whole budget. The handle
//! cache is private, so the tests read its settled `(entry_count, weighted_size)` through
//! [`CachingDatasetProvider::handle_cache_stats`], which drains Moka's pending maintenance first.

mod common;

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use common::test_config;
use lance::Dataset;
use lance::dataset::WriteParams;
use search_api::config::Config;
use search_api::domain::{DatasetRef, DatasetTarget};
use search_api::lance::CachingDatasetProvider;
use search_api::lance::DatasetProvider;
use search_api::lance::provider::MAX_HANDLE_WEIGHT;
use tempfile::TempDir;

/// Writes a single-column dataset with exactly `fragments` fragments by capping each data file at
/// one row, so the open handle's `count_fragments` (and thus its cache weight) equals `fragments`.
async fn write_dataset(uri: &str, fragments: usize) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let ids: Vec<i32> = (0..fragments as i32).collect();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(ids))]).unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let params = WriteParams {
        max_rows_per_file: 1,
        ..Default::default()
    };
    let dataset = Dataset::write(reader, uri, Some(params)).await.unwrap();
    let head_version = dataset.version_id();
    dataset
        .tags()
        .create(search_api::config::PRODUCTION_SERVE_TAG, head_version)
        .await
        .unwrap();
}

/// Builds a memory-only config (no disk tiers) over fresh temp dirs with the given handle-cache
/// weighted capacity, plus the dataset root used to write fixtures the provider then opens.
fn handle_cache_config(capacity: u64) -> (Config, TempDir, TempDir) {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let mut config = test_config(data_tmp.path(), cache_tmp.path());
    config.dataset_cache_capacity = capacity;
    config.cache_backend = search_api::config::CacheBackendKind::Memory;
    (config, data_tmp, cache_tmp)
}

/// Dataset target whose URI is unique per `name` so each handle keys a distinct cache slot.
fn target(name: &str) -> DatasetTarget {
    DatasetTarget::new("org1", "tenant1", name)
}

/// Resolves the on-disk URI the provider will compute for `target` under `data_root`.
fn uri_for(data_root: &std::path::Path, namespace: &str) -> String {
    format!(
        "file-object-store://{}/org1/tenant1/{namespace}.lance",
        data_root.display()
    )
}

#[tokio::test]
async fn weigher_assigns_clamped_fragment_weight() {
    let (config, data_tmp, _cache_tmp) = handle_cache_config(100_000);
    write_dataset(&uri_for(data_tmp.path(), "tiny"), 1).await;
    write_dataset(&uri_for(data_tmp.path(), "whale"), MAX_HANDLE_WEIGHT as usize + 8).await;
    let provider = CachingDatasetProvider::new(&config).await;

    provider.dataset(&target("tiny"), DatasetRef::Serve).await.unwrap();
    provider.dataset(&target("whale"), DatasetRef::Serve).await.unwrap();

    let (entries, weighted) = provider.handle_cache_stats().await;
    assert_eq!(entries, 2, "both handles fit under the generous capacity");
    assert_eq!(
        weighted,
        1 + MAX_HANDLE_WEIGHT as u64,
        "tiny weighs one unit and the whale is clamped to MAX_HANDLE_WEIGHT"
    );
}

#[tokio::test]
async fn many_tiny_coexist_then_capacity_bounds_total_weight() {
    let capacity = MAX_HANDLE_WEIGHT as u64;
    let (config, data_tmp, _cache_tmp) = handle_cache_config(capacity);
    let tiny_count = capacity as usize;
    let overflow = 24usize;
    for index in 0..tiny_count + overflow {
        write_dataset(&uri_for(data_tmp.path(), &format!("tiny{index}")), 1).await;
    }
    write_dataset(&uri_for(data_tmp.path(), "whale"), MAX_HANDLE_WEIGHT as usize * 2).await;
    let provider = CachingDatasetProvider::new(&config).await;

    for index in 0..tiny_count {
        provider
            .dataset(&target(&format!("tiny{index}")), DatasetRef::Serve)
            .await
            .unwrap();
    }
    let (entries, weighted) = provider.handle_cache_stats().await;
    assert_eq!(
        entries, capacity,
        "all {capacity} unit-weight tiny handles fit when their total weight equals the budget"
    );
    assert_eq!(weighted, capacity, "each tiny handle contributes exactly one unit");

    for index in tiny_count..tiny_count + overflow {
        provider
            .dataset(&target(&format!("tiny{index}")), DatasetRef::Serve)
            .await
            .unwrap();
    }
    provider.dataset(&target("whale"), DatasetRef::Serve).await.unwrap();
    let (entries_after, weighted_after) = provider.handle_cache_stats().await;
    assert!(
        weighted_after <= capacity,
        "weighted size never exceeds the configured capacity under overload, got {weighted_after}"
    );
    assert!(
        entries_after <= capacity,
        "a heavy whale plus overflow tiny handles cannot push resident weight past the budget, got {entries_after}"
    );
}

/// Appends `rows` more rows to an existing dataset, committing a new version.
async fn append_rows(uri: &str, rows: usize) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let ids: Vec<i32> = (0..rows as i32).collect();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(ids))]).unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let params = WriteParams {
        mode: lance::dataset::WriteMode::Append,
        ..Default::default()
    };
    Dataset::write(reader, uri, Some(params)).await.unwrap();
}

/// An unpinned (latest-tracking) handle must refresh within the serve-tag TTL, so a commit that
/// lands after the first open becomes visible without waiting for capacity eviction. A
/// version-pinned handle opened before the commit keeps serving its immutable snapshot.
#[tokio::test]
async fn latest_handle_refreshes_after_freshness_ttl() {
    let (mut config, data_tmp, _cache_tmp) = handle_cache_config(100_000);
    config.serve_tag_ttl_secs = 1;
    let uri = uri_for(data_tmp.path(), "fresh");
    write_dataset(&uri, 4).await;
    let provider = CachingDatasetProvider::new(&config).await;

    let first = provider.dataset(&target("fresh"), DatasetRef::Latest).await.unwrap();
    let pinned_version = first.version().version;
    assert_eq!(first.count_rows(None).await.unwrap(), 4);

    append_rows(&uri, 3).await;
    let stale = provider.dataset(&target("fresh"), DatasetRef::Latest).await.unwrap();
    assert_eq!(
        stale.count_rows(None).await.unwrap(),
        4,
        "within the freshness TTL the cached latest handle is still served"
    );

    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    let refreshed = provider.dataset(&target("fresh"), DatasetRef::Latest).await.unwrap();
    assert_eq!(
        refreshed.count_rows(None).await.unwrap(),
        7,
        "past the TTL the unpinned handle reopens and sees the new commit"
    );

    let pinned = provider
        .dataset(&target("fresh"), DatasetRef::Version(pinned_version))
        .await
        .unwrap();
    assert_eq!(
        pinned.count_rows(None).await.unwrap(),
        4,
        "a version-pinned handle never expires by time and keeps its snapshot"
    );
}
