use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// File-backed cache using sled, with TTL via stored expiration timestamps.
pub struct Store {
    db: sled::Db,
}

impl Store {
    /// Opens or creates a sled database at `dir`.
    pub fn open(dir: &Path) -> Result<Self, crate::AppError> {
        std::fs::create_dir_all(dir)?;
        let db = sled::open(dir)
            .map_err(|e| crate::AppError::Data(format!("sled open: {e}")))?;
        Ok(Self { db })
    }

    /// Retrieves a cached value if it exists and hasn't expired.
    pub fn get_cached<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, crate::AppError> {
        let raw = self.db
            .get(key)
            .map_err(|e| crate::AppError::Data(format!("sled get: {e}")))?;

        let bytes = match raw {
            Some(b) => b,
            None => return Ok(None),
        };

        let entry: CachedEntry<serde_json::Value> = serde_json::from_slice(&bytes)
            .map_err(|e| crate::AppError::Data(format!("cache deserialize: {e}")))?;

        if entry.is_expired() {
            let _ = self.db.remove(key);
            return Ok(None);
        }

        let value: T = serde_json::from_value(entry.value)
            .map_err(|e| crate::AppError::Data(format!("cache value deserialize: {e}")))?;
        Ok(Some(value))
    }

    /// Stores a value with a TTL. Uses `serde_json::Value` as intermediate to avoid
    /// serializing the expiration twice.
    pub fn set_cached<T: Serialize>(
        &self,
        key: &str,
        value: &T,
        ttl: Duration,
    ) -> Result<(), crate::AppError> {
        let json_value = serde_json::to_value(value)
            .map_err(|e| crate::AppError::Data(format!("cache serialize: {e}")))?;

        let expires_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            + ttl.as_secs();

        let entry = CachedEntry { value: json_value, expires_at };
        let bytes = serde_json::to_vec(&entry)
            .map_err(|e| crate::AppError::Data(format!("cache entry serialize: {e}")))?;

        self.db
            .insert(key, bytes)
            .map_err(|e| crate::AppError::Data(format!("sled insert: {e}")))?;

        // Flush to ensure durability after write
        let _ = self.db.flush();
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

#[derive(Serialize, Deserialize)]
struct CachedEntry<V> {
    value: V,
    expires_at: u64,
}

impl<V> CachedEntry<V> {
    fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now >= self.expires_at
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn temp_store() -> (Store, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        (store, dir)
    }

    #[test]
    fn set_and_get() {
        let (store, _dir) = temp_store();
        store.set_cached("key1", &"hello".to_string(), Duration::from_secs(60)).unwrap();
        let val: Option<String> = store.get_cached("key1").unwrap();
        assert_eq!(val, Some("hello".into()));
    }

    #[test]
    fn expired_entry_returns_none() {
        let (store, _dir) = temp_store();
        store.set_cached("exp", &"gone".to_string(), Duration::from_secs(0)).unwrap();
        std::thread::sleep(Duration::from_millis(10));
        let val: Option<String> = store.get_cached("exp").unwrap();
        assert!(val.is_none());
    }

    #[test]
    fn complex_type_roundtrip() {
        let (store, _dir) = temp_store();
        let data = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        store.set_cached("symbols", &data, Duration::from_secs(300)).unwrap();
        let loaded: Option<Vec<String>> = store.get_cached("symbols").unwrap();
        assert_eq!(loaded, Some(data));
    }

    #[test]
    fn missing_key() {
        let (store, _dir) = temp_store();
        let val: Option<String> = store.get_cached("nonexistent").unwrap();
        assert!(val.is_none());
    }
}
