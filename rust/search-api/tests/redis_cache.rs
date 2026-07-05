//! Redis-backend integration tests over a locally spawned `redis-server`.
//!
//! Every test self-skips (with an explanatory message) when the `redis-server` binary is not
//! installed, so the suite stays green on machines without Redis. The end-to-end case mirrors
//! the disk backend's two-cold-process round trip: provider A prewarms into the shared server,
//! then a fresh provider B serves searches with zero index or manifest reads on the counted
//! backing store — the fleet-wide warm-cache property that motivates the Redis backend.

mod common;

use std::sync::Arc;

use common::{
    CountingWrapper, ReadCounts, RedisServerGuard, TEST_DATASET_PATH, build_indexed_dataset, redis_test_config,
    test_target,
};
use lance_core::Result as LanceResult;
use lance_core::cache::{CacheBackend, CacheCodec, CacheCodecImpl, InternalCacheKey};
use redis::AsyncCommands;
use search_api::cache::index_cache::HybridIndexCacheBackend;
use search_api::cache::layout::{hash_hex, stamp_dir_name};
use search_api::cache::redis_store::RedisEntryStore;
use search_api::domain::{DatasetRef, PrewarmSpec, Prewarmer, SearchBackend, TextQuery, VectorQuery};
use search_api::lance::{CachingDatasetProvider, LanceSearchBackend};
use search_api::telemetry::{CacheName, Metrics};
use tempfile::TempDir;

/// Toy serializable payload exercising the codec path.
#[derive(Debug, PartialEq, Eq)]
struct Payload(Vec<u8>);

impl CacheCodecImpl for Payload {
    const TYPE_ID: &'static str = "search-api.test.Payload";
    const CURRENT_VERSION: u32 = 1;

    fn serialize(&self, writer: &mut lance_core::cache::CacheEntryWriter<'_>) -> LanceResult<()> {
        writer.write_raw(&self.0)
    }

    fn deserialize(reader: &mut lance_core::cache::CacheEntryReader<'_>) -> LanceResult<Self> {
        Ok(Payload(reader.read_raw()?.to_vec()))
    }
}

/// Builds a key under the given prefix.
fn key(prefix: &str, name: &str) -> InternalCacheKey {
    InternalCacheKey::new(Arc::from(prefix), Arc::from(name), "Payload")
}

/// Connects one index-tier Redis store to the given server.
async fn index_store(url: &str) -> Arc<RedisEntryStore> {
    Arc::new(
        RedisEntryStore::connect(
            url,
            "test-ns",
            "index",
            std::time::Duration::from_secs(3600),
            CacheName::Index,
            Arc::new(Metrics::disabled()),
        )
        .await
        .unwrap(),
    )
}

/// A hybrid index backend over a fresh connection to the given server.
async fn index_backend(url: &str) -> HybridIndexCacheBackend {
    HybridIndexCacheBackend::new(index_store(url).await, 1024 * 1024, Arc::new(Metrics::disabled()))
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
async fn redis_round_trip_survives_a_cold_instance() {
    let Some(server) = RedisServerGuard::spawn().await else {
        return;
    };
    let codec = CacheCodec::from_impl::<Payload>();
    let cache_key = key("s3://bucket/ds.lance/", "page-0");
    let warm = index_backend(&server.url).await;
    warm.insert(&cache_key, Arc::new(Payload(vec![7u8; 32])), 32, Some(codec))
        .await;

    let cold = index_backend(&server.url).await;
    let fetched = cold.get(&cache_key, Some(codec)).await.unwrap();
    assert_eq!(fetched.downcast_ref::<Payload>().unwrap().0, vec![7u8; 32]);
}

#[tokio::test]
async fn invalidate_prefix_covers_entries_written_by_a_sibling_replica() {
    let Some(server) = RedisServerGuard::spawn().await else {
        return;
    };
    let codec = CacheCodec::from_impl::<Payload>();
    let purged = key("s3://bucket/ds.lance/", "page-0");
    let kept = key("s3://bucket/other.lance/", "page-0");
    let replica_a = index_backend(&server.url).await;
    for cache_key in [&purged, &kept] {
        replica_a
            .insert(cache_key, Arc::new(Payload(vec![9u8; 8])), 8, Some(codec))
            .await;
    }

    let replica_b = index_backend(&server.url).await;
    replica_b.invalidate_prefix("s3://bucket/ds.lance/").await;

    let cold = index_backend(&server.url).await;
    assert!(
        cold.get(&purged, Some(codec)).await.is_none(),
        "a sibling replica's invalidation must cover entries this replica never wrote"
    );
    assert!(cold.get(&kept, Some(codec)).await.is_some());
}

#[tokio::test]
async fn corrupt_redis_value_is_a_miss_and_gets_deleted() {
    let Some(server) = RedisServerGuard::spawn().await else {
        return;
    };
    let codec = CacheCodec::from_impl::<Payload>();
    let cache_key = key("s3://bucket/ds.lance/", "page-corrupt");
    let dir_key = format!(
        "test-ns:{}:index:{}",
        stamp_dir_name(),
        hash_hex(cache_key.prefix(), 32)
    );
    let file = format!(
        "{}-{}.bin",
        hash_hex(cache_key.key(), 32),
        hash_hex(cache_key.type_name(), 16)
    );
    let client = redis::Client::open(server.url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: u64 = conn.hset(&dir_key, &file, b"garbage".as_slice()).await.unwrap();

    let backend = index_backend(&server.url).await;
    assert!(
        backend.get(&cache_key, Some(codec)).await.is_none(),
        "a value failing the frame checksum must miss"
    );
    let leftover: Option<Vec<u8>> = conn.hget(&dir_key, &file).await.unwrap();
    assert!(leftover.is_none(), "the corrupt value must be deleted from redis");
}

#[tokio::test]
async fn cold_process_serves_searches_from_the_shared_redis_cache() {
    let Some(server) = RedisServerGuard::spawn().await else {
        return;
    };
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;
    let config = redis_test_config(data_tmp.path(), cache_tmp.path(), &server.url);

    let counts_a = Arc::new(ReadCounts::default());
    let provider_a = CachingDatasetProvider::with_inner_store_wrapper(
        &config,
        Some(Arc::new(CountingWrapper {
            counts: counts_a.clone(),
        })),
    )
    .await;
    assert!(
        provider_a.janitor(&config).is_none(),
        "the redis backend needs no disk janitor"
    );
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
            DatasetRef::Latest,
        )
        .await
        .unwrap();
    assert!(report.metadata_warmed);
    assert!(report.indexes.iter().all(|index| index.error.is_none()), "{report:?}");
    drop(backend_a);

    let client = redis::Client::open(server.url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("search-api:{}:*", stamp_dir_name()))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(!keys.is_empty(), "prewarm must persist entries into redis");
    let dir_key = keys
        .iter()
        .find(|key| !key.ends_with(":prefixes"))
        .expect("prewarm must persist at least one dir hash beside the registry");
    let ttl: i64 = redis::cmd("TTL").arg(dir_key).query_async(&mut conn).await.unwrap();
    assert!(ttl > 0, "dir keys must carry a TTL, got {ttl}");

    let counts_b = Arc::new(ReadCounts::default());
    let provider_b = CachingDatasetProvider::with_inner_store_wrapper(
        &config,
        Some(Arc::new(CountingWrapper {
            counts: counts_b.clone(),
        })),
    )
    .await;
    let backend_b = LanceSearchBackend::new(provider_b);
    let hits = backend_b
        .text_search(&test_target(), TextQuery::simple("lemon", 3))
        .await
        .unwrap()
        .hits;
    assert_eq!(hits.len(), 1);
    let (indices_b, manifests_b, _) = counts_b.snapshot();
    assert_eq!(
        indices_b, 0,
        "a cold process must serve all _indices reads from the shared redis cache"
    );
    assert_eq!(
        manifests_b, 0,
        "a cold process must serve manifest bytes from the shared redis cache"
    );

    let hits = backend_b
        .vector_search(&test_target(), vector_query())
        .await
        .unwrap()
        .hits;
    assert_eq!(hits.len(), 2);
    let (_, _, data_b) = counts_b.snapshot();
    assert!(data_b > 0, "raw data reads must pass through to the real store");
}

#[tokio::test]
async fn redis_death_mid_run_degrades_to_misses_without_failing() {
    let Some(mut server) = RedisServerGuard::spawn().await else {
        return;
    };
    let codec = CacheCodec::from_impl::<Payload>();
    let cache_key = key("s3://bucket/ds.lance/", "page-0");
    let backend = index_backend(&server.url).await;
    backend
        .insert(&cache_key, Arc::new(Payload(vec![7u8; 32])), 32, Some(codec))
        .await;

    server.kill();

    let cold_key = key("s3://bucket/ds.lance/", "page-never-written");
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        backend.get(&cold_key, Some(codec)).await
    })
    .await
    .expect("a dead redis must degrade to a fast miss, not a hang");
    assert!(outcome.is_none(), "a dead redis must read as a miss");
    assert!(
        backend.get(&cache_key, Some(codec)).await.is_some(),
        "the memory hot tier must keep serving entries it already holds"
    );
}

#[tokio::test]
async fn unreachable_redis_falls_back_to_memory_only_and_still_serves() {
    let data_tmp = TempDir::new().unwrap();
    let cache_tmp = TempDir::new().unwrap();
    let uri = format!("file-object-store://{}/{TEST_DATASET_PATH}", data_tmp.path().display());
    build_indexed_dataset(&uri).await;
    let config = redis_test_config(data_tmp.path(), cache_tmp.path(), "redis://127.0.0.1:1");

    let provider = CachingDatasetProvider::new(&config).await;
    assert!(
        provider.index_cache().is_none(),
        "an unreachable redis must fall back to memory-only caching"
    );
    assert!(provider.janitor(&config).is_none());
    let backend = LanceSearchBackend::new(provider);
    let hits = backend
        .text_search(&test_target(), TextQuery::simple("lemon", 3))
        .await
        .unwrap()
        .hits;
    assert_eq!(hits.len(), 1, "searches must succeed memory-only");
}
