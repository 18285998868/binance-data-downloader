//! Persistent key/value cache backed by `redb`, rooted at `{output_dir}/.cache`.
//!
//! Values carry an 8-byte little-endian UNIX timestamp so TTL can be checked on
//! read without a second source of truth.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use redb::{Database, ReadableDatabase, TableDefinition};

use crate::AppError;

const TABLE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("data");

/// How long a cached value stays fresh.
///
/// The archive's first-published date and the symbol's first kline date both
/// change at most once a year, so a month of freshness is plenty — and short
/// enough that a newly listed symbol is picked up promptly.
pub(crate) const CACHE_TTL_SECS: u64 = 30 * 24 * 60 * 60;

/// Shared, lazily-initialized database handle for one domain.
type DbCell = Arc<OnceLock<Arc<Database>>>;

/// A redb-backed cache rooted at one output directory (`{output_dir}/.cache`).
///
/// One instance per run, created in `app::run`; databases are opened lazily
/// per domain and shared through the instance. No global state, so tests are
/// independent and parallel-safe.
#[derive(Clone)]
pub(crate) struct Cache {
    base: PathBuf,
    dbs: Arc<Mutex<HashMap<String, DbCell>>>,
    create_lock: Arc<Mutex<()>>,
}

impl Cache {
    /// Root the cache under `output_dir` (databases are created on first use).
    #[must_use]
    pub(crate) fn new(output_dir: &Path) -> Self {
        Self {
            base: output_dir.join(".cache"),
            dbs: Arc::new(Mutex::new(HashMap::new())),
            create_lock: Arc::new(Mutex::new(())),
        }
    }

    fn db_path(&self, domain: &str) -> PathBuf {
        self.base.join(format!("{domain}/{domain}.redb"))
    }

    fn ensure_db(&self, domain: &str) -> Result<Arc<Database>, AppError> {
        let path = self.db_path(domain);
        let key = path.to_string_lossy().into_owned();

        // Fast path: a previously initialized cell is returned without taking
        // the creation lock.
        let cell = {
            let mut map = self
                .dbs
                .lock()
                .map_err(|e| AppError::Cache(format!("mutex poisoned: {e}")))?;
            Arc::clone(map.entry(key).or_insert_with(|| Arc::new(OnceLock::new())))
        };
        if let Some(db) = cell.get() {
            return Ok(Arc::clone(db));
        }

        // Slow path: serialize creation per instance to avoid concurrent
        // `Database::create` on the same path, but keep the critical section
        // limited to creation (not to lookups).
        let _guard = self
            .create_lock
            .lock()
            .map_err(|e| AppError::Cache(format!("mutex poisoned: {e}")))?;
        if let Some(db) = cell.get() {
            return Ok(Arc::clone(db));
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(AppError::Io)?;
        }
        let db = Database::create(&path).map_err(|e| AppError::Cache(e.to_string()))?;
        let db = Arc::new(db);
        if cell.set(Arc::clone(&db)).is_err() {
            // The cell was filled concurrently (the creation lock only
            // serializes *this* instance, not another one). The handle already
            // in the cell is the authoritative one.
            if let Some(existing) = cell.get() {
                return Ok(Arc::clone(existing));
            }
        }
        Ok(db)
    }
}

impl Cache {
    /// Read `key` from `domain`.
    ///
    /// Returns `Ok(None)` when the entry is missing, expired or malformed.
    /// Returns `Err` when the cache cannot be opened or read, so callers can
    /// distinguish a miss from a cache failure.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Cache`] when the database cannot be opened or read,
    /// and [`AppError::Io`] when the cache directory cannot be created.
    pub(crate) fn read(
        &self,
        domain: &str,
        key: &str,
        ttl_secs: Option<u64>,
    ) -> Result<Option<Vec<u8>>, AppError> {
        let db = self.ensure_db(domain)?;
        let read_tx = db
            .begin_read()
            .map_err(|e| AppError::Cache(e.to_string()))?;
        // A table that does not exist yet is an empty cache, not a failure.
        let Ok(table) = read_tx.open_table(TABLE) else {
            return Ok(None);
        };
        let Some(guard) = table.get(key).map_err(|e| AppError::Cache(e.to_string()))? else {
            return Ok(None);
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
            && cached_at.saturating_add(ttl) <= now
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
    pub(crate) fn write(&self, domain: &str, key: &str, data: &[u8]) -> Result<(), AppError> {
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

    /// Store several values in one transaction.
    ///
    /// One transaction means one fsync instead of one per value: a fresh run
    /// resolves the start date of every (symbol, timeframe), and writing each
    /// of them separately would fsync once per entry.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Cache`] when the database cannot be opened or the
    /// write transaction fails, and [`AppError::Io`] when the cache directory
    /// cannot be created.
    pub(crate) fn write_many(
        &self,
        domain: &str,
        entries: &[(String, Vec<u8>)],
    ) -> Result<(), AppError> {
        if entries.is_empty() {
            return Ok(());
        }
        let db = self.ensure_db(domain)?;
        let cached_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let write_tx = db
            .begin_write()
            .map_err(|e| AppError::Cache(e.to_string()))?;
        {
            let mut table = write_tx
                .open_table(TABLE)
                .map_err(|e| AppError::Cache(e.to_string()))?;
            for (key, data) in entries {
                let mut buf = Vec::with_capacity(8 + data.len());
                buf.extend_from_slice(&cached_at.to_le_bytes());
                buf.extend_from_slice(data);
                table
                    .insert(key.as_str(), buf)
                    .map_err(|e| AppError::Cache(e.to_string()))?;
            }
        }
        write_tx
            .commit()
            .map_err(|e| AppError::Cache(e.to_string()))?;
        Ok(())
    }

    /// Async wrapper around [`Self::read`] for use in async contexts.
    ///
    /// # Errors
    ///
    /// Propagates every error from [`Self::read`], plus [`AppError::Cache`] when
    /// the blocking task cannot be joined.
    pub(crate) async fn read_async(
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

    /// Async wrapper around [`Self::write_many`] for use in async contexts.
    ///
    /// # Errors
    ///
    /// Propagates every error from [`Self::write_many`], plus
    /// [`AppError::Cache`] when the blocking task cannot be joined.
    pub(crate) async fn write_many_async(
        &self,
        domain: &str,
        entries: &[(String, Vec<u8>)],
    ) -> Result<(), AppError> {
        let this = self.clone();
        let domain = domain.to_owned();
        let entries = entries.to_vec();
        tokio::task::spawn_blocking(move || this.write_many(&domain, &entries))
            .await
            .map_err(|e| AppError::Cache(format!("join error: {e}")))?
    }

    /// Async wrapper around [`Self::write`] for use in async contexts.
    ///
    /// # Errors
    ///
    /// Propagates every error from [`Self::write`], plus [`AppError::Cache`] when
    /// the blocking task cannot be joined.
    pub(crate) async fn write_async(
        &self,
        domain: &str,
        key: &str,
        data: &[u8],
    ) -> Result<(), AppError> {
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

    #[test]
    fn read_within_ttl_returns_value() {
        let (_dir, cache) = temp_cache();
        cache.write("test_domain", "key1", b"data").unwrap();
        assert_eq!(
            cache.read("test_domain", "key1", Some(3600)).unwrap(),
            Some(b"data".to_vec())
        );
    }

    #[test]
    fn domains_are_isolated() {
        let (_dir, cache) = temp_cache();
        cache.write("alpha", "k", b"a").unwrap();
        cache.write("beta", "k", b"b").unwrap();
        assert_eq!(cache.read("alpha", "k", None).unwrap(), Some(b"a".to_vec()));
        assert_eq!(cache.read("beta", "k", None).unwrap(), Some(b"b".to_vec()));
    }

    #[test]
    fn write_overwrites_existing_key() {
        let (_dir, cache) = temp_cache();
        cache.write("test_domain", "k", b"first").unwrap();
        cache.write("test_domain", "k", b"second").unwrap();
        assert_eq!(
            cache.read("test_domain", "k", None).unwrap(),
            Some(b"second".to_vec())
        );
    }

    #[test]
    fn write_many_stores_every_entry() {
        let (_dir, cache) = temp_cache();
        cache
            .write_many(
                "d",
                &[
                    ("a".to_owned(), b"1".to_vec()),
                    ("b".to_owned(), b"2".to_vec()),
                ],
            )
            .unwrap();
        assert_eq!(cache.read("d", "a", None).unwrap(), Some(b"1".to_vec()));
        assert_eq!(cache.read("d", "b", None).unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn write_many_without_entries_is_a_no_op() {
        let (_dir, cache) = temp_cache();
        cache.write_many("d", &[]).unwrap();
        assert_eq!(cache.read("d", "missing", None).unwrap(), None);
    }

    #[tokio::test]
    async fn write_many_async_stores_every_entry() {
        let (_dir, cache) = temp_cache();
        cache
            .write_many_async(
                "d",
                &[
                    ("a".to_owned(), b"1".to_vec()),
                    ("b".to_owned(), b"2".to_vec()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            cache.read_async("d", "a", None).await.unwrap(),
            Some(b"1".to_vec())
        );
        assert_eq!(
            cache.read_async("d", "b", None).await.unwrap(),
            Some(b"2".to_vec())
        );
    }

    #[test]
    fn value_shorter_than_timestamp_header_is_a_miss() {
        let (_dir, cache) = temp_cache();
        // Bypass the TTL-stamping writer to plant a truncated value.
        let db = cache.ensure_db("test_domain").unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut table = tx.open_table(TABLE).unwrap();
            table.insert("short", vec![1u8, 2, 3]).unwrap();
        }
        tx.commit().unwrap();
        assert_eq!(cache.read("test_domain", "short", None).unwrap(), None);
    }

    #[test]
    fn a_fresh_instance_reads_previously_written_data() {
        let dir = tempfile::tempdir().unwrap();
        Cache::new(dir.path())
            .write("d", "k", b"persisted")
            .unwrap();
        // A second instance over the same root must see the same database.
        assert_eq!(
            Cache::new(dir.path()).read("d", "k", None).unwrap(),
            Some(b"persisted".to_vec())
        );
    }

    #[tokio::test]
    async fn async_wrappers_round_trip() {
        let (_dir, cache) = temp_cache();
        cache.write_async("d", "k", b"async").await.unwrap();
        assert_eq!(
            cache.read_async("d", "k", None).await.unwrap(),
            Some(b"async".to_vec())
        );
        assert_eq!(cache.read_async("d", "missing", None).await.unwrap(), None);
    }

    #[tokio::test]
    async fn concurrent_writes_to_one_domain_all_land() {
        let (_dir, cache) = temp_cache();
        let tasks: Vec<_> = (0..32)
            .map(|i| {
                let cache = cache.clone();
                tokio::spawn(async move {
                    let key = format!("k{i}");
                    cache.write_async("d", &key, b"v").await.unwrap();
                    assert_eq!(
                        cache.read_async("d", &key, None).await.unwrap(),
                        Some(b"v".to_vec())
                    );
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }
    }
}
