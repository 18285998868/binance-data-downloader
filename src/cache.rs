use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use redb::{Database, ReadableDatabase, TableDefinition};

use crate::AppError;

const TABLE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("data");

/// A redb-backed cache rooted at one output directory (`{output_dir}/.cache`).
///
/// One instance per run, created in `app::run`; databases are opened lazily
/// per domain and shared through the instance. No global state, so tests are
/// independent and parallel-safe.
#[derive(Clone)]
pub struct Cache {
    base: PathBuf,
    dbs: Arc<Mutex<HashMap<String, Arc<Database>>>>,
}

impl Cache {
    /// Root the cache under `output_dir` (databases are created on first use).
    #[must_use]
    pub fn new(output_dir: &Path) -> Self {
        Self {
            base: output_dir.join(".cache"),
            dbs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn db_path(&self, domain: &str) -> PathBuf {
        self.base.join(format!("{domain}/{domain}.redb"))
    }

    fn ensure_db(&self, domain: &str) -> Result<Arc<Database>, AppError> {
        let path = self.db_path(domain);
        let key = path.to_string_lossy().into_owned();
        let mut map = self
            .dbs
            .lock()
            .map_err(|e| AppError::Cache(format!("mutex poisoned: {e}")))?;
        if let Some(db) = map.get(&key) {
            return Ok(Arc::clone(db));
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(AppError::Io)?;
        }
        let db = Database::create(&path).map_err(|e| AppError::Cache(e.to_string()))?;
        let db = Arc::new(db);
        map.insert(key, Arc::clone(&db));
        Ok(db)
    }
}

impl Cache {
    /// Read `key` from `domain`.
    ///
    /// Returns `Ok(None)` when the entry is missing or expired. Returns
    /// `Err` when the cache cannot be opened or read, so callers can
    /// distinguish a miss from a cache failure.
    pub fn read(
        &self,
        domain: &str,
        key: &str,
        ttl_secs: Option<u64>,
    ) -> Result<Option<Vec<u8>>, AppError> {
        let db = self.ensure_db(domain)?;
        let read_tx = db
            .begin_read()
            .map_err(|e| AppError::Cache(e.to_string()))?;
        let table = match read_tx.open_table(TABLE) {
            Ok(t) => t,
            Err(_) => return Ok(None),
        };
        let guard = match table.get(key).map_err(|e| AppError::Cache(e.to_string()))? {
            Some(g) => g,
            None => return Ok(None),
        };
        let raw = guard.value();

        if raw.len() < 8 {
            return Ok(None);
        }
        let (ts_bytes, data) = raw.split_at(8);
        let Ok(ts_arr) = ts_bytes.try_into() else {
            return Ok(None);
        };
        let cached_at = u64::from_le_bytes(ts_arr);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if let Some(ttl) = ttl_secs
            && cached_at + ttl <= now
        {
            tracing::debug!(domain, key, "cache entry expired");
            return Ok(None);
        }
        Ok(Some(data.to_vec()))
    }

    /// Store `data` under `key`, stamped with the current UNIX time.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Cache`] when the database cannot be opened or the
    /// write transaction fails, and [`AppError::Io`] when the cache directory
    /// cannot be created.
    pub fn write(&self, domain: &str, key: &str, data: &[u8]) -> Result<(), AppError> {
        let db = self.ensure_db(domain)?;
        let cached_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut buf = Vec::with_capacity(8 + data.len());
        buf.extend_from_slice(&cached_at.to_le_bytes());
        buf.extend_from_slice(data);

        let write_tx = db
            .begin_write()
            .map_err(|e| AppError::Cache(e.to_string()))?;
        {
            let mut table = write_tx
                .open_table(TABLE)
                .map_err(|e| AppError::Cache(e.to_string()))?;
            table
                .insert(key, buf)
                .map_err(|e| AppError::Cache(e.to_string()))?;
        }
        write_tx
            .commit()
            .map_err(|e| AppError::Cache(e.to_string()))?;
        Ok(())
    }

    /// Async wrapper around [`Self::read`] for use in async contexts.
    pub async fn read_async(
        &self,
        domain: &str,
        key: &str,
        ttl_secs: Option<u64>,
    ) -> Result<Option<Vec<u8>>, AppError> {
        let this = self.clone();
        let domain = domain.to_owned();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || this.read(&domain, &key, ttl_secs))
            .await
            .map_err(|e| AppError::Cache(format!("join error: {e}")))?
    }

    /// Async wrapper around [`Self::write`] for use in async contexts.
    pub async fn write_async(&self, domain: &str, key: &str, data: &[u8]) -> Result<(), AppError> {
        let this = self.clone();
        let domain = domain.to_owned();
        let key = key.to_owned();
        let data = data.to_vec();
        tokio::task::spawn_blocking(move || this.write(&domain, &key, &data))
            .await
            .map_err(|e| AppError::Cache(format!("join error: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_cache() -> (tempfile::TempDir, Cache) {
        let dir = tempfile::tempdir().unwrap();
        let cache = Cache::new(dir.path());
        (dir, cache)
    }

    #[test]
    fn write_then_read() {
        let (_dir, cache) = temp_cache();
        cache.write("test_domain", "key1", b"hello").unwrap();
        assert_eq!(
            cache.read("test_domain", "key1", None).unwrap(),
            Some(b"hello".to_vec())
        );
    }

    #[test]
    fn read_missing_key() {
        let (_dir, cache) = temp_cache();
        assert_eq!(
            cache.read("test_domain", "nonexistent", None).unwrap(),
            None
        );
    }

    #[test]
    fn read_expired_entry() {
        let (_dir, cache) = temp_cache();
        cache.write("test_domain", "key1", b"data").unwrap();
        // TTL = 0 → immediately expired
        assert_eq!(cache.read("test_domain", "key1", Some(0)).unwrap(), None);
    }
}
