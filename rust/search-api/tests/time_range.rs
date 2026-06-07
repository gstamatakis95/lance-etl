//! Event-time range search and object-store scan-stats capture.
//!
//! Builds a tiny dataset with an `event_timestamp` column (one row per day), then drives the Lance
//! backend directly to assert that a request time range restricts results to the window on the
//! event-timestamp column (start inclusive, end exclusive, either bound optional), that an absent
//! range behaves as before, that the range ANDs with a caller filter, and that the scan-stats
//! capture path records `object_store.*` stats.

mod common;

use std::sync::{Arc, Mutex};

use arrow_array::types::Float32Type;
use arrow_array::{
    FixedSizeListArray, Int32Array, RecordBatch, RecordBatchIterator, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use common::{TEST_DATASET_PATH, test_config, test_target};
use lance::Dataset;
use lance::index::DatasetIndexExt;
use lance_index::IndexType;
use lance_index::scalar::InvertedIndexParams;
use search_api::domain::{
    CompareOp, Filter, HybridQuery, Literal, SearchBackend, SearchError, TextQuery, TimeRange, VectorQuery,
};
use search_api::lance::{CachingDatasetProvider, LanceSearchBackend, ScanIoStats};
use tempfile::TempDir;

/// Vector dimension of the test dataset.
const DIM: i32 = 4;

/// Epoch-milliseconds base for row 0 (2023-11-14T22:13:20Z), an arbitrary fixed instant.
const BASE_MS: i64 = 1_700_000_000_000;

/// Milliseconds in one day, the spacing between consecutive rows' event timestamps.
const DAY_MS: i64 = 86_400_000;

/// Event timestamp in epoch milliseconds for row `index` (one row per day).
fn event_ms(index: i64) -> i64 {
    BASE_MS + index * DAY_MS
}

/// Writes a four-row dataset (id, text, vector, event_timestamp) and creates an INVERTED index on
/// `text` so the text and hybrid legs run, with one row per day on the timestamp column.
async fn build_timestamped_dataset(uri: &str) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("text", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), DIM),
            false,
        ),
        Field::new(
            "event_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]));
    let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        vec![
            Some(vec![Some(1.0), Some(0.0), Some(0.0), Some(0.0)]),
            Some(vec![Some(0.9), Some(0.1), Some(0.0), Some(0.0)]),
            Some(vec![Some(0.8), Some(0.2), Some(0.0), Some(0.0)]),
            Some(vec![Some(0.7), Some(0.3), Some(0.0), Some(0.0)]),
        ],
        DIM,
    );
    let timestamps = TimestampMicrosecondArray::from((0..4).map(|index| event_ms(index) * 1_000).collect::<Vec<_>>())
        .with_timezone("UTC");
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                "red apple pie",
                "green pear tart",
                "blue fish stew",
                "yellow lemon cake",
            ])),
            Arc::new(vectors),
            Arc::new(timestamps),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let mut dataset = Dataset::write(reader, uri, None).await.unwrap();
    dataset
        .create_index(
            &["text"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default().with_position(true),
            true,
        )
        .await
        .unwrap();
}

/// Builds a backend over a fresh provider rooted at `data_root`, with the given cache dir.
fn build_backend(
    data_root: &std::path::Path,
    cache_root: &std::path::Path,
) -> LanceSearchBackend<CachingDatasetProvider> {
    let config = test_config(data_root, cache_root);
    LanceSearchBackend::new(CachingDatasetProvider::new(&config))
}

/// A vector query over all four rows with an optional event-time window.
fn ranged_vector_query(time_range: Option<TimeRange>) -> VectorQuery {
    VectorQuery {
        vector: vec![1.0, 0.0, 0.0, 0.0],
        k: 10,
        time_range,
        ..Default::default()
    }
}

/// Reads the integer `id` of a hit row.
fn hit_id(row: &serde_json::Map<String, serde_json::Value>) -> i64 {
    row.get("id").and_then(serde_json::Value::as_i64).expect("id column")
}

/// Sorted ids of a set of vector hits.
fn sorted_ids(hits: &[search_api::domain::Hit]) -> Vec<i64> {
    let mut ids: Vec<i64> = hits.iter().map(|hit| hit_id(&hit.row)).collect();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn vector_time_range_restricts_to_the_window() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_timestamped_dataset(&uri).await;
    let backend = build_backend(data_tmp.path(), cache_tmp.path());
    let target = test_target();

    let all = backend.vector_search(&target, ranged_vector_query(None)).await.unwrap();
    assert_eq!(
        sorted_ids(&all.hits),
        vec![1, 2, 3, 4],
        "an absent range searches all rows"
    );

    let start_only = backend
        .vector_search(
            &target,
            ranged_vector_query(Some(TimeRange {
                start_ms: Some(event_ms(2)),
                end_ms: None,
            })),
        )
        .await
        .unwrap();
    assert_eq!(sorted_ids(&start_only.hits), vec![3, 4], "start is inclusive");

    let end_only = backend
        .vector_search(
            &target,
            ranged_vector_query(Some(TimeRange {
                start_ms: None,
                end_ms: Some(event_ms(2)),
            })),
        )
        .await
        .unwrap();
    assert_eq!(sorted_ids(&end_only.hits), vec![1, 2], "end is exclusive");

    let window = backend
        .vector_search(
            &target,
            ranged_vector_query(Some(TimeRange {
                start_ms: Some(event_ms(1)),
                end_ms: Some(event_ms(3)),
            })),
        )
        .await
        .unwrap();
    assert_eq!(sorted_ids(&window.hits), vec![2, 3], "both bounds clip to [day1, day3)");
}

#[tokio::test]
async fn time_range_ands_with_a_caller_filter() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_timestamped_dataset(&uri).await;
    let backend = build_backend(data_tmp.path(), cache_tmp.path());

    let mut query = ranged_vector_query(Some(TimeRange {
        start_ms: Some(event_ms(1)),
        end_ms: None,
    }));
    query.filter = Some(Filter::Compare {
        column: "id".to_string(),
        op: CompareOp::Le,
        value: Literal::Int(3),
    });
    let hits = backend.vector_search(&test_target(), query).await.unwrap();
    assert_eq!(
        sorted_ids(&hits.hits),
        vec![2, 3],
        "the window (id in {{2,3,4}}) ANDed with id <= 3 leaves {{2,3}}"
    );
}

#[tokio::test]
async fn text_and_hybrid_time_range_restricts_the_window() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_timestamped_dataset(&uri).await;
    let backend = build_backend(data_tmp.path(), cache_tmp.path());
    let target = test_target();

    let mut text = TextQuery::simple("apple", 10);
    text.columns = vec!["text".to_string()];
    text.time_range = Some(TimeRange {
        start_ms: Some(event_ms(1)),
        end_ms: None,
    });
    let excluded = backend.text_search(&target, text.clone()).await.unwrap();
    assert!(
        excluded.hits.is_empty(),
        "apple is row 1 at day0, excluded by a day1 lower bound"
    );

    text.time_range = None;
    let included = backend.text_search(&target, text.clone()).await.unwrap();
    assert_eq!(included.hits.len(), 1, "without a window apple matches row 1");
    assert_eq!(hit_id(&included.hits[0].row), 1);

    let hybrid = HybridQuery {
        vector: ranged_vector_query(Some(TimeRange {
            start_ms: Some(event_ms(2)),
            end_ms: None,
        })),
        text: {
            let mut leg = TextQuery::simple("fish", 10);
            leg.columns = vec!["text".to_string()];
            leg.time_range = Some(TimeRange {
                start_ms: Some(event_ms(2)),
                end_ms: None,
            });
            leg
        },
        k: 10,
        fusion: search_api::domain::FusionSpec::default(),
    };
    let fused = backend.hybrid_search(&target, hybrid).await.unwrap();
    let mut ids: Vec<i64> = fused.hits.iter().map(|hit| hit_id(&hit.row)).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![3, 4], "both hybrid legs honor the day2 lower bound");
}

#[tokio::test]
async fn time_range_against_a_missing_event_column_is_rejected() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_timestamped_dataset(&uri).await;
    let backend = build_backend(data_tmp.path(), cache_tmp.path()).with_event_timestamp_column("no_such_column");

    let err = backend
        .vector_search(
            &test_target(),
            ranged_vector_query(Some(TimeRange {
                start_ms: Some(event_ms(1)),
                end_ms: None,
            })),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, SearchError::InvalidArgument(_)),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn scan_stats_capture_records_object_store_stats() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_timestamped_dataset(&uri).await;
    let config = test_config(data_tmp.path(), cache_tmp.path());
    let captured: Arc<Mutex<Vec<ScanIoStats>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = captured.clone();
    let backend = LanceSearchBackend::new(CachingDatasetProvider::new(&config))
        .with_scan_stats_hook(Arc::new(move |stats| sink.lock().unwrap().push(*stats)));

    backend
        .vector_search(&test_target(), ranged_vector_query(None))
        .await
        .unwrap();

    let stats = captured.lock().unwrap();
    assert_eq!(stats.len(), 1, "one scan must report one stats snapshot");
    assert!(
        stats[0].bytes_read > 0,
        "the scan must read bytes from the object store: {:?}",
        stats[0]
    );
    assert!(
        stats[0].requests > 0 || stats[0].iops > 0,
        "the scan must record object-store requests or iops: {:?}",
        stats[0]
    );
}
