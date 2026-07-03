//! Lance trace-event bridge end to end.
//!
//! Installs the [`LanceEventMetricsLayer`] as the global tracing subscriber over a spy-backed
//! metrics facade, then builds, opens, and searches a real local dataset. Asserts the layer turned
//! the Lance `dataset_events` and `io_events` that fired during open and the indexed search into
//! `search_api.lance.*` DogStatsD metrics. The dataset open emits a `loading` dataset event and the
//! full-text search opens the inverted index, emitting an `open_scalar_index` IO event.

mod common;

use std::sync::Arc;

use cadence::SpyMetricSink;
use common::{TEST_DATASET_PATH, build_indexed_dataset, test_config, test_target};
use search_api::domain::{SearchBackend, TextQuery};
use search_api::lance::{CachingDatasetProvider, LanceSearchBackend};
use search_api::telemetry::{LanceEventMetricsLayer, Metrics};
use tempfile::TempDir;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[tokio::test]
async fn lance_event_layer_captures_open_and_io_events_from_a_real_scan() {
    let (receiver, sink) = SpyMetricSink::new();
    let metrics = Arc::new(Metrics::from_sink(sink));
    tracing_subscriber::registry()
        .with(LanceEventMetricsLayer::new(metrics))
        .init();

    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;

    let config = test_config(data_tmp.path(), cache_tmp.path());
    let backend = LanceSearchBackend::new(CachingDatasetProvider::new(&config).await);

    let mut query = TextQuery::simple("apple", 10);
    query.columns = vec!["text".to_string()];
    backend.text_search(&test_target(), query).await.unwrap();

    let mut lines = Vec::new();
    while let Ok(packet) = receiver.try_recv() {
        lines.push(String::from_utf8(packet).unwrap());
    }

    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("search_api.lance.dataset_events:1|c") && line.contains("event:loading")),
        "expected at least one dataset loading event from the open: {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("search_api.lance.io_events:1|c")),
        "expected at least one IO event from opening the inverted index for the scan: {lines:?}"
    );
}
