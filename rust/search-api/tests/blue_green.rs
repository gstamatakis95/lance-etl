//! Blue-green serving: the backend opens the version the fixed `HEAD` tag points at, not the
//! latest, and a `HEAD` move is observed within the tag TTL. Prewarm-by-tag reports the resolved
//! version. These exercise the version-keyed handle cache and the short-TTL tag-resolution cache
//! end to end against real Lance tags.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{CountingWrapper, ReadCounts, TEST_DATASET_PATH, build_indexed_dataset, test_config, test_target};
use lance::Dataset;
use search_api::config::Config;
use search_api::domain::{DatasetRef, PrewarmSpec, Prewarmer, SearchBackend, TextQuery, VectorQuery};
use search_api::lance::{CachingDatasetProvider, LanceSearchBackend};
use tempfile::TempDir;

/// Builds a config over the data/cache roots with a selected fixed tag TTL.
fn head_config(data_root: &std::path::Path, cache_root: &std::path::Path, ttl_secs: u64) -> Config {
    let mut config = test_config(data_root, cache_root);
    config.serve_tag_ttl_secs = ttl_secs;
    config
}

/// A small nearest-neighbor probe that returns hits at any version (flat search when unindexed).
fn probe() -> VectorQuery {
    VectorQuery {
        vector: vec![1.0, 0.0, 0.0, 0.0],
        k: 2,
        ..Default::default()
    }
}

#[tokio::test]
async fn serve_opens_head_version_and_observes_a_move() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;

    let dataset = Dataset::open(&uri).await.unwrap();
    let latest = dataset.version_id();
    assert!(latest >= 2, "index builds must produce several versions, got {latest}");
    dataset
        .tags()
        .update(search_api::config::PRODUCTION_SERVE_TAG, 1u64)
        .await
        .unwrap();

    let config = head_config(data_tmp.path(), cache_tmp.path(), 1);
    let provider = CachingDatasetProvider::with_inner_store_wrapper(&config, None).await;
    let backend = LanceSearchBackend::new(provider);
    let target = test_target();

    let outcome = backend.vector_search(&target, probe()).await.unwrap();
    assert_eq!(
        outcome.dataset_version,
        Some(1),
        "production serving must open the version HEAD points at, not latest"
    );

    dataset
        .tags()
        .update(search_api::config::PRODUCTION_SERVE_TAG, latest)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let outcome = backend.vector_search(&target, probe()).await.unwrap();
    assert_eq!(
        outcome.dataset_version,
        Some(latest),
        "a HEAD move must be observed within the tag TTL"
    );
}

#[tokio::test]
async fn prewarm_by_tag_reports_the_resolved_version() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;

    let dataset = Dataset::open(&uri).await.unwrap();
    dataset.tags().create("staging", 1u64).await.unwrap();

    let config = test_config(data_tmp.path(), cache_tmp.path());
    let provider = CachingDatasetProvider::with_inner_store_wrapper(&config, None).await;
    let backend = LanceSearchBackend::new(provider);

    let report = backend
        .prewarm(
            &test_target(),
            PrewarmSpec {
                metadata: true,
                all_indexes: false,
                index_names: vec![],
                fts_with_position: false,
            },
            DatasetRef::Tag("staging".to_string()),
        )
        .await
        .unwrap();
    assert_eq!(
        report.resolved_version, 1,
        "prewarm must report the tag-resolved version"
    );
}

#[tokio::test]
async fn prewarm_by_version_serves_warm_through_a_head_move_in_a_cold_process() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;

    let dataset = Dataset::open(&uri).await.unwrap();
    let green = dataset.version_id();
    assert!(
        green >= 2,
        "the indexed build must produce several versions, got {green}"
    );
    dataset
        .tags()
        .update(search_api::config::PRODUCTION_SERVE_TAG, 1u64)
        .await
        .unwrap();

    let config = head_config(data_tmp.path(), cache_tmp.path(), 10);

    let counts_a = Arc::new(ReadCounts::default());
    let provider_a = CachingDatasetProvider::with_inner_store_wrapper(
        &config,
        Some(Arc::new(CountingWrapper {
            counts: counts_a.clone(),
        })),
    )
    .await;
    let backend_a = LanceSearchBackend::new(provider_a);
    let report = backend_a
        .prewarm(
            &test_target(),
            PrewarmSpec {
                metadata: true,
                all_indexes: true,
                index_names: vec![],
                fts_with_position: true,
            },
            DatasetRef::Version(green),
        )
        .await
        .unwrap();
    assert_eq!(report.resolved_version, green, "prewarm pinned the green version by id");
    assert_eq!(report.indexes.len(), 2, "{report:?}");
    assert!(report.indexes.iter().all(|index| index.error.is_none()), "{report:?}");
    drop(backend_a);

    dataset
        .tags()
        .update(search_api::config::PRODUCTION_SERVE_TAG, green)
        .await
        .unwrap();

    let counts_b = Arc::new(ReadCounts::default());
    let provider_b = CachingDatasetProvider::with_inner_store_wrapper(
        &config,
        Some(Arc::new(CountingWrapper {
            counts: counts_b.clone(),
        })),
    )
    .await;
    let backend_b = LanceSearchBackend::new(provider_b);

    let text = backend_b
        .text_search(&test_target(), TextQuery::simple("lemon", 3))
        .await
        .unwrap();
    assert_eq!(text.hits.len(), 1);
    assert_eq!(
        text.dataset_version,
        Some(green),
        "production serving must resolve HEAD onto the prewarmed green version"
    );
    let vector = backend_b.vector_search(&test_target(), probe()).await.unwrap();
    assert_eq!(vector.dataset_version, Some(green));
    assert_eq!(vector.hits.len(), 2);

    let (indices_b, manifests_b, data_b) = counts_b.snapshot();
    assert_eq!(
        indices_b, 0,
        "a cold process serving HEAD must hit the disk index cache prewarm wrote by version, not the store"
    );
    assert_eq!(
        manifests_b, 0,
        "the green manifest prewarmed by version must be served from the disk byte cache once the tag resolves onto it"
    );
    assert!(data_b > 0, "raw data reads always pass through to the store");
}
