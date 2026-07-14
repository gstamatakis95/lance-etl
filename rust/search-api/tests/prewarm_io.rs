//! Prewarm-then-search IO assertions on a single provider: after Prewarm, vector, text, and
//! hybrid searches trigger no further reads under `_indices/` or of versioned manifests, while
//! `data/` reads stay nonzero — proving raw table data is served by the store, never the cache.

mod common;

use std::sync::Arc;

use common::{
    CountingWrapper, ReadCounts, TEST_DATASET_PATH, bin_file_count, build_indexed_dataset, test_config, test_target,
};
use search_api::domain::{
    DatasetRef, FusionSpec, HybridQuery, PrewarmSpec, Prewarmer, SearchBackend, TextQuery, VectorQuery,
};
use search_api::lance::{CachingDatasetProvider, LanceSearchBackend};
use tempfile::TempDir;

#[tokio::test]
async fn searches_after_prewarm_do_no_index_or_manifest_io() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;
    let config = test_config(data_tmp.path(), cache_tmp.path());
    let target = test_target();

    let counts = Arc::new(ReadCounts::default());
    let provider = CachingDatasetProvider::with_inner_store_wrapper(
        &config,
        Some(Arc::new(CountingWrapper { counts: counts.clone() })),
    )
    .await;
    let backend = LanceSearchBackend::new(provider);
    let report = backend
        .prewarm(
            &target,
            PrewarmSpec {
                metadata: true,
                all_indexes: true,
                index_names: vec![],
                fts_with_position: true,
            },
            DatasetRef::Latest,
        )
        .await
        .unwrap();
    assert!(report.metadata_warmed);
    assert_eq!(report.indexes.len(), 2);
    assert!(report.indexes.iter().all(|index| index.error.is_none()), "{report:?}");

    let stamp = cache_tmp.path().join(search_api::cache::layout::stamp_dir_name());
    assert!(
        bin_file_count(&stamp.join("index")) > 0,
        "prewarm must populate the disk index cache tier that vector/text scans read from"
    );
    assert!(
        bin_file_count(&stamp.join("store")) > 0,
        "prewarm must populate the metadata byte (store) cache tier that manifest/index-reopen reads consult"
    );

    let (indices_before, manifests_before, data_before) = counts.snapshot();

    let vector = VectorQuery {
        vector: vec![0.0, 1.0, 0.0, 0.0],
        k: 2,
        ..Default::default()
    };
    let outcome = backend.vector_search(&target, vector.clone()).await.unwrap();
    assert_eq!(outcome.hits.len(), 2);
    let text = backend
        .text_search(&target, TextQuery::simple("pear", 3))
        .await
        .unwrap();
    assert_eq!(text.hits.len(), 1);
    let fused = backend
        .hybrid_search(
            &target,
            HybridQuery {
                vector,
                text: TextQuery::simple("pear", 0),
                k: 2,
                fusion: FusionSpec::default(),
            },
        )
        .await
        .unwrap();
    assert_eq!(fused.hits.len(), 2);

    let (indices_after, manifests_after, data_after) = counts.snapshot();
    assert_eq!(
        indices_after, indices_before,
        "searches after prewarm must not read _indices/ from the store"
    );
    assert_eq!(
        manifests_after, manifests_before,
        "searches after prewarm must not re-read versioned manifests from the store"
    );
    assert!(
        data_after > data_before,
        "result materialization must read data/ from the store, proving raw data is never cached"
    );
}
