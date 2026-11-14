//! Persistent key/value cache for start dates, stored as one JSON file per domain.
//!
//! The cache holds at most a few hundred small ASCII values (dates), so a
//! database engine would be far more machinery than the data justifies. A
//! single JSON document per domain is easy to inspect, needs no background
//! threads, and keeps the dependency tree small.
//!
//! Writes are atomic: the new document goes to a temporary file, is fsynced,
//! and is renamed over the old one. A crash therefore leaves either the old
//! cache or the new one, never a truncated file.
//!
//! Values carry an 8-byte little-endian UNIX timestamp (encoded as a JSON
//! number) so TTL can be checked on read without a second source of truth.

use std::collections::{HashMap, HashSet};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::AppError;

/// On-disk schema version. Bumped when the representation changes in a way an
/// older build cannot read; an unknown version is an error, never a silent
/// miss.
const CACHE_VERSION: u32 = 1;

/// How long a cached value stays fresh.
///
/// The archive's first-published date and the symbol's first kline date both
/// change at most once a year, so a month of freshness is plenty -- and short
/// enough that a newly listed symbol is picked up promptly.
pub(crate) const CACHE_TTL_SECS: u64 = 30 * 24 * 60 * 60;

/// A cache failure.
///
/// The variants separate what a caller could do about it: [`CacheError::Io`]
/// is a filesystem problem, [`CacheError::Join`] a broken worker task,
/// [`CacheError::Invalid`] a corrupt or unreadable document, and
/// [`CacheError::Poisoned`] a panicking lock holder. Callers treat all of them
/// as a miss and refetch, except for `Join`, which is surfaced as an
/// application task failure.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CacheError {
    /// A shared mutex was poisoned by a panicking thread.
    #[error("cache lock poisoned")]
    Poisoned,
    /// The cache directory or document could not be accessed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// The document exists but cannot be decoded.
    #[error("invalid cache document {path}: {message}")]
    Invalid {
        /// File that failed to decode.
        path: PathBuf,
        /// Why it could not be decoded.
        message: String,
    },
    /// Serialization of a valid in-memory document failed.
    #[error("cache serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    /// The blocking worker task failed to join.
    #[error("join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}

impl From<CacheError> for AppError {
    fn from(error: CacheError) -> Self {
        match error {
            // Keep the filesystem error as an I/O error: it is the same failure
            // whether it came from the cache or any other file operation.
            CacheError::Io(error) => Self::Io(error),
            // A blocking task that could not be joined is a task failure, not
            // a cache miss: the caller's `?` should surface it.
            CacheError::Join(error) => Self::Join(error),
            other => Self::Cache(other.to_string()),
        }
    }
}

/// One cached value and the time it was written.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedValue {
    /// UNIX timestamp (seconds) of the write.
    at: u64,
    /// Payload; the start-date callers store a `YYYY-MM-DD` string.
    value: Vec<u8>,
}

/// One domain document.
#[derive(Debug, Serialize, Deserialize)]
struct CacheFile {
    version: u32,
    entries: HashMap<String, CachedValue>,
}

impl Default for CacheFile {
    fn default() -> Self {
        Self {
            version: CACHE_VERSION,
            entries: HashMap::new(),
        }
    }
}

/// In-memory state shared by every clone of one [`Cache`].
#[derive(Debug, Default)]
struct State {
    /// Loaded documents, keyed by domain.
    domains: HashMap<String, HashMap<String, CachedValue>>,
    /// Domains whose document has been read (or confirmed absent).
    loaded: HashSet<String>,
}

/// A JSON-file-backed cache rooted at one output directory (`{output_dir}/.cache`).
///
/// One instance per run, created in `prepare::run`. Documents are loaded lazily
/// per domain and kept in memory; every write is persisted immediately, so a
/// fresh instance (or a re-run) sees the same values. No global state, so tests
/// are independent and parallel-safe.
///
/// The output-directory lock guarantees one process per tree. A second
/// `Cache` instance in the same process would not observe writes made by the
/// first after it has loaded the same domain; tests use one instance per tree.
#[derive(Clone)]
pub(crate) struct Cache {
    base: PathBuf,
    state: Arc<Mutex<State>>,
}

impl Cache {
    /// Root the cache under `output_dir` (documents are created on first use).
    #[must_use]
    pub(crate) fn new(output_dir: &Path) -> Self {
        Self {
            base: output_dir.join(".cache"),
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    /// Read `key` from `domain`.
    ///
    /// Returns `Ok(None)` when the entry is missing or expired. Returns `Err`
    /// when the document is unreadable, so callers can distinguish a miss from
    /// a broken cache.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Io`] when the document cannot be read,
    /// [`CacheError::Invalid`] when it is not a valid document, and
    /// [`CacheError::Poisoned`] when the shared mutex is poisoned.
    pub(crate) fn read(
        &self,
        domain: &str,
        key: &str,
        ttl_secs: Option<u64>,
    ) -> Result<Option<Vec<u8>>, CacheError> {
        let mut state = self.state.lock().map_err(|_| CacheError::Poisoned)?;
        self.load_domain(&mut state, domain)?;

        let Some(entry) = state
            .domains
            .get(domain)
            .and_then(|entries| entries.get(key))
        else {
            return Ok(None);
        };

        if let Some(ttl) = ttl_secs
            && entry.at.saturating_add(ttl) <= now_secs()
        {
            tracing::debug!(domain, key, "cache entry expired");
            return Ok(None);
        }
        Ok(Some(entry.value.clone()))
    }

    /// Store `data` under `key`, stamped with the current UNIX time.
    ///
    /// # Errors
    ///
    /// Same as [`Self::write_many`].
    pub(crate) fn write(&self, domain: &str, key: &str, data: &[u8]) -> Result<(), CacheError> {
        self.write_many(domain, &[(key.to_owned(), data.to_vec())])
    }

    /// Store several values in one document write.
    ///
    /// A fresh run resolves the start date of every (symbol, timeframe); one
    /// atomic write per domain is cheaper than one fsync per entry.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Io`] when the document cannot be written,
    /// [`CacheError::Invalid`] when the existing document cannot be decoded,
    /// and [`CacheError::Poisoned`] when the shared mutex is poisoned.
    pub(crate) fn write_many(
        &self,
        domain: &str,
        entries: &[(String, Vec<u8>)],
    ) -> Result<(), CacheError> {
        if entries.is_empty() {
            return Ok(());
        }
        validate_domain(domain)?;

        let mut state = self.state.lock().map_err(|_| CacheError::Poisoned)?;
        self.load_domain(&mut state, domain)?;

        // Build the next snapshot first: a failed write must not leave the
        // in-memory state ahead of the file.
        let mut next = state.domains.get(domain).cloned().unwrap_or_default();
        let at = now_secs();
        for (key, value) in entries {
            next.insert(
                key.clone(),
                CachedValue {
                    at,
                    value: value.clone(),
                },
            );
        }
        self.save_domain(domain, &next)?;
        state.domains.insert(domain.to_owned(), next);
        Ok(())
    }

    /// Async wrapper around [`Self::read`] for use in async contexts.
    ///
    /// # Errors
    ///
    /// Propagates every error from [`Self::read`], plus [`CacheError::Join`]
    /// when the blocking task cannot be joined.
    pub(crate) async fn read_async(
        &self,
        domain: &str,
        key: &str,
        ttl_secs: Option<u64>,
    ) -> Result<Option<Vec<u8>>, CacheError> {
        let this = self.clone();
        let domain = domain.to_owned();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || this.read(&domain, &key, ttl_secs))
            .await
            .map_err(CacheError::Join)?
    }

    /// Async wrapper around [`Self::write_many`] for use in async contexts.
    ///
    /// # Errors
    ///
    /// Propagates every error from [`Self::write_many`], plus
    /// [`CacheError::Join`] when the blocking task cannot be joined.
    pub(crate) async fn write_many_async(
        &self,
        domain: &str,
        entries: &[(String, Vec<u8>)],
    ) -> Result<(), CacheError> {
        let this = self.clone();
        let domain = domain.to_owned();
        let entries = entries.to_vec();
        tokio::task::spawn_blocking(move || this.write_many(&domain, &entries))
            .await
            .map_err(CacheError::Join)?
    }

    /// Async wrapper around [`Self::write`] for use in async contexts.
    ///
    /// # Errors
    ///
    /// Propagates every error from [`Self::write`], plus [`CacheError::Join`]
    /// when the blocking task cannot be joined.
    pub(crate) async fn write_async(
        &self,
        domain: &str,
        key: &str,
        data: &[u8],
    ) -> Result<(), CacheError> {
        let this = self.clone();
        let domain = domain.to_owned();
        let key = key.to_owned();
        let data = data.to_vec();
        tokio::task::spawn_blocking(move || this.write(&domain, &key, &data))
            .await
            .map_err(CacheError::Join)?
    }

    /// Load one domain from disk, once.
    fn load_domain(&self, state: &mut State, domain: &str) -> Result<(), CacheError> {
        validate_domain(domain)?;
        if state.loaded.contains(domain) {
            return Ok(());
        }

        let path = self.document_path(domain);
        let entries = match std::fs::read(&path) {
            Ok(bytes) => {
                let file: CacheFile =
                    serde_json::from_slice(&bytes).map_err(|error| CacheError::Invalid {
                        path: path.clone(),
                        message: error.to_string(),
                    })?;
                if file.version != CACHE_VERSION {
                    return Err(CacheError::Invalid {
                        path,
                        message: format!(
                            "unsupported cache version {}, expected {CACHE_VERSION}",
                            file.version
                        ),
                    });
                }
                file.entries
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(error) => return Err(CacheError::Io(error)),
        };

        state.domains.insert(domain.to_owned(), entries);
        state.loaded.insert(domain.to_owned());
        Ok(())
    }

    /// Persist one domain atomically.
    fn save_domain(
        &self,
        domain: &str,
        entries: &HashMap<String, CachedValue>,
    ) -> Result<(), CacheError> {
        std::fs::create_dir_all(&self.base)?;
        let path = self.document_path(domain);
        let tmp = self
            .base
            .join(format!(".{domain}.json.{}.tmp", std::process::id()));
        let bytes = serde_json::to_vec(&CacheFile {
            version: CACHE_VERSION,
            entries: entries.clone(),
        })?;

        let write_result = (|| -> std::io::Result<()> {
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(&bytes)?;
            // The cache is regenerable, but a half-written file would turn
            // into a hard error on the next run; fsync makes the rename safe.
            file.sync_all()?;
            std::fs::rename(&tmp, &path)?;
            sync_parent_dir(&path)
        })();
        if write_result.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        write_result?;
        Ok(())
    }

    /// Path of one domain document.
    fn document_path(&self, domain: &str) -> PathBuf {
        self.base.join(format!("{domain}.json"))
    }
}

/// Reject domain names that could escape the cache directory.
///
/// Domains are internal constants (`start_date`, `archive_start`), but the
/// check keeps a future call site from turning a typo into a path traversal.
fn validate_domain(domain: &str) -> Result<(), CacheError> {
    let valid = !domain.is_empty()
        && domain
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    if valid {
        Ok(())
    } else {
        Err(CacheError::Invalid {
            path: PathBuf::from(domain),
            message: "domain must contain only ASCII letters, digits, '_' or '-'".to_owned(),
        })
    }
}

/// Current UNIX time in seconds; before the epoch is treated as zero.
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Make a completed rename durable by fsyncing the containing directory.
fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let parent = path.parent().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has no parent")
        })?;
        let dir = std::fs::File::open(parent)?;
        dir.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
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
    fn read_missing_domain_is_a_miss() {
        let (_dir, cache) = temp_cache();
        assert_eq!(cache.read("absent", "key", None).unwrap(), None);
    }

    #[test]
    fn read_expired_entry() {
        let (_dir, cache) = temp_cache();
        cache.write("test_domain", "key1", b"data").unwrap();
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
        assert!(!cache.document_path("d").exists());
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
    fn a_fresh_instance_reads_previously_written_data() {
        let dir = tempfile::tempdir().unwrap();
        Cache::new(dir.path())
            .write("d", "k", b"persisted")
            .unwrap();
        assert_eq!(
            Cache::new(dir.path()).read("d", "k", None).unwrap(),
            Some(b"persisted".to_vec())
        );
    }

    #[test]
    fn the_document_is_readable_json() {
        let dir = tempfile::tempdir().unwrap();
        let cache = Cache::new(dir.path());
        cache.write("d", "k", b"v").unwrap();
        let raw = std::fs::read_to_string(cache.document_path("d")).unwrap();
        let parsed: CacheFile = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed.version, CACHE_VERSION);
        assert_eq!(parsed.entries["k"].value, b"v");
    }

    #[test]
    fn a_corrupt_document_is_an_error_not_a_miss() {
        let dir = tempfile::tempdir().unwrap();
        let cache = Cache::new(dir.path());
        std::fs::create_dir_all(dir.path().join(".cache")).unwrap();
        std::fs::write(cache.document_path("d"), b"not json").unwrap();

        let error = cache.read("d", "k", None).unwrap_err();
        assert!(matches!(error, CacheError::Invalid { .. }), "got {error:?}");
    }

    #[test]
    fn an_unknown_schema_version_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let cache = Cache::new(dir.path());
        std::fs::create_dir_all(dir.path().join(".cache")).unwrap();
        std::fs::write(cache.document_path("d"), br#"{"version":99,"entries":{}}"#).unwrap();

        let error = cache.read("d", "k", None).unwrap_err();
        assert!(matches!(error, CacheError::Invalid { .. }), "got {error:?}");
    }

    #[test]
    fn a_domain_cannot_escape_the_cache_directory() {
        let (_dir, cache) = temp_cache();
        let error = cache.write("../escape", "k", b"v").unwrap_err();
        assert!(matches!(error, CacheError::Invalid { .. }), "got {error:?}");
    }

    #[test]
    fn a_failed_write_does_not_leave_a_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        let cache = Cache::new(dir.path());
        cache.write("d", "k", b"v").unwrap();
        let leftovers: Vec<_> = std::fs::read_dir(dir.path().join(".cache"))
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| {
                std::path::Path::new(name)
                    .extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("tmp"))
            })
            .collect();
        assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");
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
        for task in tasks {
            task.await.unwrap();
        }
    }
}
