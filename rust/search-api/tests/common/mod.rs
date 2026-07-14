//! Shared helpers for the integration tests: a tiny indexed dataset builder and a counting
//! object-store wrapper that records which reads reach the real store.
//!
//! Each test binary that includes this module uses only a subset of the helpers (for example the
//! blue-green tests need the dataset builder and config but not the counting store), so the module
//! allows dead code rather than forcing every binary to touch every helper.
#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::types::Float32Type;
use arrow_array::{FixedSizeListArray, Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use lance::Dataset;
use lance::index::DatasetIndexExt;
use lance_index::IndexType;
use lance_index::scalar::{InvertedIndexParams, ScalarIndexParams};
use lance_io::object_store::WrappingObjectStore;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use search_api::config::Config;
use search_api::domain::DatasetTarget;

/// Vector dimension of the test dataset.
pub const DIM: i32 = 4;

/// The org/tenant/namespace every disk-cache test dataset lives under.
pub fn test_target() -> DatasetTarget {
    DatasetTarget::new("org1", "tenant1", "ns1")
}

/// Relative dataset path of [`test_target`] under the base URI.
pub const TEST_DATASET_PATH: &str = "org1/tenant1/ns1.lance";

/// Reads that reached the real (wrapped) object store, bucketed by path class.
#[derive(Debug, Default)]
pub struct ReadCounts {
    /// Non-head reads under `_indices/`.
    pub indices_reads: AtomicU64,
    /// Non-head reads of immutable `_versions/{n}.manifest` files.
    pub manifest_reads: AtomicU64,
    /// Non-head reads under `data/`.
    pub data_reads: AtomicU64,
    /// Every store operation that reached the real store: all reads (including head and paths
    /// outside the buckets above) plus list calls. Lets a test assert that an operation touched
    /// the store at all, e.g. that a negatively cached open issues zero requests.
    pub total_operations: AtomicU64,
}

impl ReadCounts {
    /// Snapshot of `(indices, manifests, data)` read counters.
    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.indices_reads.load(Ordering::SeqCst),
            self.manifest_reads.load(Ordering::SeqCst),
            self.data_reads.load(Ordering::SeqCst),
        )
    }

    /// Total store operations observed so far.
    pub fn total(&self) -> u64 {
        self.total_operations.load(Ordering::SeqCst)
    }
}

/// Wrapper installing a [`CountingStore`] around the real store. Chain it *inside* the metadata
/// byte cache so it only sees reads that the cache did not serve from disk.
#[derive(Debug)]
pub struct CountingWrapper {
    /// Shared counters observed by the test.
    pub counts: Arc<ReadCounts>,
}

impl WrappingObjectStore for CountingWrapper {
    fn wrap(&self, store_prefix: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        let _ = store_prefix;
        Arc::new(CountingStore {
            inner: original,
            counts: self.counts.clone(),
        })
    }
}

/// Pass-through store recording non-head reads per path class.
#[derive(Debug)]
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    counts: Arc<ReadCounts>,
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

impl CountingStore {
    /// Buckets one non-head read by path class.
    fn record(&self, location: &ObjectPath) {
        let parts: Vec<String> = location.parts().map(|part| part.as_ref().to_string()).collect();
        let filename = location.filename().unwrap_or("");
        if parts.iter().any(|part| part == "_indices") {
            self.counts.indices_reads.fetch_add(1, Ordering::SeqCst);
        } else if parts.iter().any(|part| part == "_versions")
            && filename.ends_with(".manifest")
            && filename != "_latest.manifest"
        {
            self.counts.manifest_reads.fetch_add(1, Ordering::SeqCst);
        } else if parts.iter().any(|part| part == "data") {
            self.counts.data_reads.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.counts.total_operations.fetch_add(1, Ordering::SeqCst);
        if !options.head {
            self.record(location);
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[std::ops::Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        self.counts.total_operations.fetch_add(1, Ordering::SeqCst);
        for _ in ranges {
            self.record(location);
        }
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<ObjectPath>>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.counts.total_operations.fetch_add(1, Ordering::SeqCst);
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.counts.total_operations.fetch_add(1, Ordering::SeqCst);
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> ObjectStoreResult<ListResult> {
        self.counts.total_operations.fetch_add(1, Ordering::SeqCst);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &ObjectPath, to: &ObjectPath, options: CopyOptions) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Writes a four-row dataset (id, text, vector) at `uri` and creates an INVERTED index with
/// positions on `text` plus a BTree index on `id`, so prewarm has both FTS and scalar targets.
pub async fn build_indexed_dataset(uri: &str) {
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
            Some(vec![Some(1.0), Some(0.0), Some(0.0), Some(0.0)]),
            Some(vec![Some(0.0), Some(1.0), Some(0.0), Some(0.0)]),
            Some(vec![Some(0.0), Some(0.0), Some(1.0), Some(0.0)]),
            Some(vec![Some(0.0), Some(0.0), Some(0.0), Some(1.0)]),
        ],
        DIM,
    );
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
    dataset
        .create_index(&["id"], IndexType::BTree, None, &ScalarIndexParams::default(), true)
        .await
        .unwrap();
    let head_version = dataset.version_id();
    dataset
        .tags()
        .create(search_api::config::PRODUCTION_SERVE_TAG, head_version)
        .await
        .unwrap();
}

/// Builds a config over the given dataset root and cache dir, using the `file-object-store`
/// scheme so every read flows through the object-store wrappers (the plain `file` scheme has
/// optimized paths that bypass them).
pub fn test_config(dataset_root: &std::path::Path, cache_dir: &std::path::Path) -> Config {
    Config {
        base_uri: format!("file-object-store://{}", dataset_root.display()),
        dataset_cache_capacity: 16,
        index_cache_bytes: 64 * 1024 * 1024,
        metadata_cache_bytes: 64 * 1024 * 1024,
        port: 0,
        cache_dir: cache_dir.to_path_buf(),
        disk_index_cache_bytes: 1024 * 1024 * 1024,
        disk_store_cache_bytes: 1024 * 1024 * 1024,
        cache_backend: search_api::config::CacheBackendKind::Disk,
        redis_url: None,
        redis_namespace: search_api::config::DEFAULT_REDIS_NAMESPACE.to_string(),
        statsd_addr: "127.0.0.1:8125".to_string(),
        telemetry_disabled: true,
        serve_tag_ttl_secs: search_api::config::DEFAULT_SERVE_TAG_TTL_SECS,
        prewarm_targets_path: None,
    }
}

/// Like [`test_config`] but selecting the Redis cache backend at the given URL.
pub fn redis_test_config(dataset_root: &std::path::Path, cache_dir: &std::path::Path, redis_url: &str) -> Config {
    let mut config = test_config(dataset_root, cache_dir);
    config.cache_backend = search_api::config::CacheBackendKind::Redis;
    config.redis_url = Some(redis_url.to_string());
    config
}

/// A locally spawned `redis-server` child on a free port, killed on drop.
pub struct RedisServerGuard {
    child: std::process::Child,
    /// The connection URL of the spawned server.
    pub url: String,
}

impl RedisServerGuard {
    /// Spawns a throwaway `redis-server` on a free localhost port and waits for it to answer
    /// `PING`. Returns `None` (after an explanatory eprintln) when the binary is not installed,
    /// so redis-backed tests skip gracefully on machines without Redis.
    pub async fn spawn() -> Option<Self> {
        let port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
            listener.local_addr().ok()?.port()
        };
        let child = match std::process::Command::new("redis-server")
            .arg("--port")
            .arg(port.to_string())
            .arg("--save")
            .arg("")
            .arg("--appendonly")
            .arg("no")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
        {
            Ok(child) => child,
            Err(_) => {
                eprintln!("redis-server binary not found, skipping redis cache test");
                return None;
            }
        };
        let url = format!("redis://127.0.0.1:{port}");
        let guard = Self { child, url };
        for _ in 0..50 {
            if let Ok(client) = redis::Client::open(guard.url.as_str())
                && let Ok(mut conn) = client.get_multiplexed_async_connection().await
                && redis::cmd("PING").query_async::<String>(&mut conn).await.is_ok()
            {
                return Some(guard);
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        eprintln!("spawned redis-server did not answer PING in time, skipping redis cache test");
        None
    }
}

impl RedisServerGuard {
    /// Kills the server immediately, simulating a mid-run Redis outage.
    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for RedisServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Recursively counts `.bin` entry files under `root`.
///
/// Not every integration test that includes this module uses it, hence the dead-code allowance.
#[allow(dead_code)]
pub fn bin_file_count(root: &std::path::Path) -> usize {
    let mut count = 0;
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                count += bin_file_count(&path);
            } else if path.extension().is_some_and(|ext| ext == "bin") {
                count += 1;
            }
        }
    }
    count
}
