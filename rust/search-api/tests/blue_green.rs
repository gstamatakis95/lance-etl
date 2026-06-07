//! Blue-green serving: with serve-by-tag enabled the backend opens the version the serve tag
//! points at, not the latest, and a tag flip is observed within the serve-tag TTL. Prewarm-by-tag
//! reports the resolved version. These exercise the version-keyed handle cache and the short-TTL
//! tag-resolution cache end to end against real Lance tags.

mod common;

use std::time::Duration;

use common::{TEST_DATASET_PATH, build_indexed_dataset, test_config, test_target};
use lance::Dataset;
use search_api::config::Config;
use search_api::domain::{DatasetRef, PrewarmSpec, Prewarmer, SearchBackend, VectorQuery};
use search_api::lance::{CachingDatasetProvider, LanceSearchBackend};
use tempfile::TempDir;

/// Builds a config over the data/cache roots with serve-by-tag enabled and a short tag TTL.
fn serve_by_tag_config(data_root: &std::path::Path, cache_root: &std::path::Path, ttl_secs: u64) -> Config {
    let mut config = test_config(data_root, cache_root);
    config.serve_by_tag = true;
    config.serve_tag = "prod".to_string();
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
async fn serve_by_tag_opens_tagged_version_and_observes_a_flip() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;

    let dataset = Dataset::open(&uri).await.unwrap();
    let latest = dataset.version_id();
    assert!(latest >= 2, "index builds must produce several versions, got {latest}");
    dataset.tags().create("prod", 1u64).await.unwrap();

    let config = serve_by_tag_config(data_tmp.path(), cache_tmp.path(), 1);
    let provider = CachingDatasetProvider::with_inner_store_wrapper(&config, None);
    let backend = LanceSearchBackend::new(provider).with_id_column(config.id_column.clone());
    let target = test_target();

    let outcome = backend.vector_search(&target, probe()).await.unwrap();
    assert_eq!(
        outcome.dataset_version,
        Some(1),
        "serve-by-tag must open the version prod points at, not the latest"
    );

    dataset.tags().update("prod", latest).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let outcome = backend.vector_search(&target, probe()).await.unwrap();
    assert_eq!(
        outcome.dataset_version,
        Some(latest),
        "a tag flip must be observed within the serve-tag TTL"
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
    let provider = CachingDatasetProvider::with_inner_store_wrapper(&config, None);
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
