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
pub struct Cache {
    base: PathBuf,
    dbs: Mutex<HashMap<String, Arc<Database>>>,
}

impl Cache {
    /// Root the cache under `output_dir` (databases are created on first use).
    #[must_use]
    pub fn new(output_dir: &Path) -> Self {
        Self {
            base: output_dir.join(".cache"),
            dbs: Mutex::new(HashMap::new()),
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

macro_rules! log_none {
    ($expr:expr, $domain:expr, $msg:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(domain = $domain, error = %e, "{}", $msg);
                return None;
            }
        }
    };
    ($expr:expr, $domain:expr, $key:expr, $msg:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(domain = $domain, key = $key, error = %e, "{}", $msg);
                return None;
            }
        }
    };
}

impl Cache {
    /// Read `key` from `domain`.
    ///
    /// Returns `None` when the entry is missing, expired, or the cache is
    /// unreadable (logged as a warning, never fatal).
    #[must_use]
    pub fn read(&self, domain: &str, key: &str, ttl_secs: Option<u64>) -> Option<Vec<u8>> {
        let db = match self.ensure_db(domain) {
            Ok(db) => db,
            Err(e) => {
                tracing::warn!(domain, "cache read ensure_db failed: {e}");
                return None;
            }
        };
        let read_tx = log_none!(db.begin_read(), domain, "cache read begin_read");
        let table = log_none!(read_tx.open_table(TABLE), domain, "cache read open_table");
        let guard = log_none!(table.get(key), domain, key, "cache read get")?;
        let raw = guard.value();

        if raw.len() < 8 {
            return None;
        }
        let (ts_bytes, data) = raw.split_at(8);
        let cached_at = u64::from_le_bytes(ts_bytes.try_into().ok()?);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if let Some(ttl) = ttl_secs
            && cached_at + ttl <= now
        {
            tracing::debug!(domain, key, "cache entry expired");
            return None;
        }
        Some(data.to_vec())
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
            cache.read("test_domain", "key1", None),
            Some(b"hello".to_vec())
        );
    }

    #[test]
    fn read_missing_key() {
        let (_dir, cache) = temp_cache();
        assert_eq!(cache.read("test_domain", "nonexistent", None), None);
    }

    #[test]
    fn read_expired_entry() {
        let (_dir, cache) = temp_cache();
        cache.write("test_domain", "key1", b"data").unwrap();
        // TTL = 0 → immediately expired
        assert_eq!(cache.read("test_domain", "key1", Some(0)), None);
    }
}
