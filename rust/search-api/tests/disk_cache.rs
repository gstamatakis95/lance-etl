//! Disk round-trip across two simulated cold processes: provider A prewarms an org into a cache
//! directory, then a fresh provider B over the same directory serves searches without re-reading
//! index or manifest bytes from the (counted) backing store, while raw `data/` reads always pass
//! through.

mod common;

use std::sync::Arc;

use common::{
    CountingWrapper, ReadCounts, TEST_DATASET_PATH, bin_file_count, build_indexed_dataset, test_config, test_target,
};
use search_api::domain::{PrewarmSpec, Prewarmer, SearchBackend, TextQuery, VectorQuery};
use search_api::lance::{CachingDatasetProvider, LanceSearchBackend};
use tempfile::TempDir;

/// Builds a backend over a fresh provider with its own counting wrapper.
fn build_backend(config: &search_api::config::Config) -> (LanceSearchBackend<CachingDatasetProvider>, Arc<ReadCounts>) {
    let counts = Arc::new(ReadCounts::default());
    let provider = CachingDatasetProvider::with_inner_store_wrapper(
        config,
        Some(Arc::new(CountingWrapper { counts: counts.clone() })),
    );
    (LanceSearchBackend::new(provider), counts)
}

/// One flat vector query matching the first row.
fn vector_query() -> VectorQuery {
    VectorQuery {
        vector: vec![1.0, 0.0, 0.0, 0.0],
        k: 2,
        ..Default::default()
    }
}

#[tokio::test]
async fn cold_process_serves_searches_from_disk_caches() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;
    let config = test_config(data_tmp.path(), cache_tmp.path());

    let (backend_a, counts_a) = build_backend(&config);
    let report = backend_a
        .prewarm(
            &test_target(),
            PrewarmSpec {
                metadata: true,
                all_indexes: true,
                index_names: vec![],
                fts_with_position: true,
            },
        )
        .await
        .unwrap();
    assert!(report.metadata_warmed);
    assert_eq!(report.indexes.len(), 2, "expected text_idx and id_idx: {report:?}");
    assert!(report.indexes.iter().all(|index| index.error.is_none()), "{report:?}");

    let hits = backend_a
        .text_search(&test_target(), TextQuery::simple("lemon", 3))
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    let hits = backend_a
        .vector_search(&test_target(), vector_query())
        .await
        .unwrap()
        .hits;
    assert_eq!(hits.len(), 2);
    let (_, _, data_a) = counts_a.snapshot();
    assert!(
        data_a > 0,
        "raw data reads must pass through to the store, never the cache"
    );

    let index_tier = cache_tmp
        .path()
        .join(search_api::cache::layout::stamp_dir_name())
        .join("index");
    assert!(
        bin_file_count(&index_tier) > 0,
        "prewarm must persist serialized index entries to disk"
    );

    drop(backend_a);

    let (backend_b, counts_b) = build_backend(&config);
    let hits = backend_b
        .text_search(&test_target(), TextQuery::simple("lemon", 3))
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    let (indices_b, manifests_b, _) = counts_b.snapshot();
    assert_eq!(
        indices_b, 0,
        "cold process must serve all _indices reads from the disk caches"
    );
    assert_eq!(
        manifests_b, 0,
        "cold process must serve manifest bytes from the disk cache"
    );

    let hits = backend_b
        .vector_search(&test_target(), vector_query())
        .await
        .unwrap()
        .hits;
    assert_eq!(hits.len(), 2);
    let (_, _, data_b) = counts_b.snapshot();
    assert!(data_b > 0, "flat vector scans must read data/ from the real store");
}

#[tokio::test]
async fn tiny_budget_sweep_keeps_cache_within_bounds_and_searches_correct() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;
    let mut config = test_config(data_tmp.path(), cache_tmp.path());
    config.disk_index_cache_bytes = 4096;
    config.disk_store_cache_bytes = 4096;

    let provider = CachingDatasetProvider::new(&config);
    let janitor = provider.janitor(&config).expect("disk caches enabled");
    let index_cache = provider.disk_index_cache().unwrap().clone();
    let store_cache = provider.store_cache().unwrap().clone();
    let backend = LanceSearchBackend::new(provider);
    backend
        .prewarm(
            &test_target(),
            PrewarmSpec {
                metadata: true,
                all_indexes: true,
                index_names: vec![],
                fts_with_position: true,
            },
        )
        .await
        .unwrap();

    janitor.sweep_once().await;
    assert!(
        index_cache.disk_size_bytes() <= config.disk_index_cache_bytes,
        "index tier must respect its byte budget after a sweep"
    );
    assert!(
        store_cache.approx_size_bytes() <= config.disk_store_cache_bytes,
        "store tier must respect its byte budget after a sweep"
    );

    let hits = backend
        .text_search(&test_target(), TextQuery::simple("lemon", 3))
        .await
        .unwrap();
    assert_eq!(
        hits.len(),
        1,
        "searches must still be correct after eviction (misses reload)"
    );
    let hits = backend
        .vector_search(&test_target(), vector_query())
        .await
        .unwrap()
        .hits;
    assert_eq!(hits.len(), 2);
}
