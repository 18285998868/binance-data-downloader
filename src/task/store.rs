use cached::ConcurrentCached;
use cached::stores::DiskCache;
use serde::{Serialize, de::DeserializeOwned};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// File-backed cache using `cached::DiskCache` (sled).
pub struct Store {
    inner: DiskCache<String, String>,
}

impl Store {
    pub fn open(dir: &Path) -> Result<Self, crate::AppError> {
        std::fs::create_dir_all(dir)?;
        let cache_name = dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("data-downloader");
        let inner = DiskCache::new(cache_name)
            .disk_directory(dir)
            .build()
            .map_err(|e| crate::AppError::Data(format!("cache init: {e}")))?;
        Ok(Self { inner })
    }

    pub fn get_cached<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, crate::AppError> {
        match self.inner.cache_get(&key.to_string()) {
            Ok(Some(val)) => serde_json::from_str(&val)
                .map(Some)
                .map_err(|e| crate::AppError::Data(format!("cache deserialize: {e}"))),
            Ok(None) => Ok(None),
            Err(e) => Err(crate::AppError::Data(format!("cache get: {e}"))),
        }
    }

    pub fn set_cached<T: Serialize>(
        &self,
        key: &str,
        value: &T,
        _ttl: Duration,
    ) -> Result<(), crate::AppError> {
        let json = serde_json::to_string_pretty(value)
            .map_err(|e| crate::AppError::Data(format!("cache serialize: {e}")))?;
        self.inner
            .cache_set(key.to_string(), json)
            .map_err(|e| crate::AppError::Data(format!("cache set: {e}")))?;
        Ok(())
    }

    /// Opens store in `spawn_blocking`, returns `Arc` for sharing across async tasks.
    pub async fn open_async(dir: &Path) -> Result<Arc<Self>, crate::AppError> {
        let dir = dir.to_path_buf();
        tokio::task::spawn_blocking(move || Self::open(&dir))
            .await
            .map_err(|e| crate::AppError::Join(format!("spawn_blocking: {e}")))?
            .map(Arc::new)
    }
}

// ---------------------------------------------------------------------------
// Async wrappers that reuse a single `Arc<Store>` (sled opened once).
// ---------------------------------------------------------------------------

/// Async cache read, offloads blocking sled I/O via `spawn_blocking`.
pub async fn get_cached<T>(store: &Arc<Store>, key: &str) -> Result<Option<T>, crate::AppError>
where
    T: DeserializeOwned + Send + 'static,
{
    let store = Arc::clone(store);
    let key = key.to_string();
    tokio::task::spawn_blocking(move || store.get_cached(&key))
        .await
        .map_err(|e| crate::AppError::Join(format!("spawn_blocking: {e}")))?
}

/// Async cache write, offloads blocking sled I/O via `spawn_blocking`.
pub async fn set_cached<T>(
    store: &Arc<Store>,
    key: &str,
    value: T,
    ttl: Duration,
) -> Result<(), crate::AppError>
where
    T: Serialize + Send + 'static,
{
    let store = Arc::clone(store);
    let key = key.to_string();
    tokio::task::spawn_blocking(move || store.set_cached(&key, &value, ttl))
        .await
        .map_err(|e| crate::AppError::Join(format!("spawn_blocking: {e}")))?
}
