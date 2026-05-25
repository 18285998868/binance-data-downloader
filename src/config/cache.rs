use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// A lazily-loaded, write-on-request JSON cache file.
///
/// # Lifecycle
///
/// 1. [`JsonFileCache::new`] creates a handle pointing at
///    `<data_dir>/.cache/<name>.json`.
/// 2. [`JsonFileCache::load`] reads and deserialises the file.  If the file is
///    missing or corrupt, `T::default()` is used.
/// 3. Mutations via `data` must be followed by [`JsonFileCache::mark_changed`]
///    to ensure the write-back in step 4 is not a no-op.
/// 4. [`JsonFileCache::save`] writes the file to disk (only if changed).
pub struct JsonFileCache<T> {
    path: PathBuf,
    pub data: T,
    changed: bool,
}

impl<T: Default + DeserializeOwned + Serialize> JsonFileCache<T> {
    /// Creates a cache handle.  Does **not** touch the filesystem.
    pub fn new(data_dir: &Path, name: &str) -> Self {
        let path = data_dir.join(".cache").join(format!("{}.json", name));
        Self {
            path,
            data: T::default(),
            changed: false,
        }
    }

    /// Loads cached data from disk, falling back to `T::default()`.
    pub async fn load(&mut self) {
        match tokio::fs::read_to_string(&self.path).await {
            Ok(content) => match serde_json::from_str::<T>(&content) {
                Ok(data) => {
                    self.data = data;
                    log::debug!("loaded cache: {}", self.path.display());
                }
                Err(e) => {
                    log::warn!(
                        "corrupt cache file, recreating: {} error: {}",
                        self.path.display(),
                        e
                    );
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                log::debug!(
                    "cache file not found, starting fresh: {}",
                    self.path.display()
                );
            }
            Err(e) => {
                log::warn!(
                    "failed to read cache file, recreating: {} error: {}",
                    self.path.display(),
                    e
                );
            }
        }
    }

    /// Loads cached data only if the file is newer than `max_age`.
    /// Otherwise (missing, expired, or corrupt) resets to `T::default()`.
    pub async fn load_with_expiry(&mut self, max_age: Duration) {
        if self.is_expired(max_age).await {
            log::debug!("cache expired: {}", self.path.display());
            self.data = T::default();
            return;
        }
        self.load().await;
    }

    /// Checks whether the cache file is older than `max_age` or missing.
    async fn is_expired(&self, max_age: Duration) -> bool {
        let meta = match tokio::fs::metadata(&self.path).await {
            Ok(m) => m,
            Err(_) => return true,
        };
        let modified = match meta.modified() {
            Ok(t) => t,
            Err(_) => return true,
        };
        SystemTime::now()
            .duration_since(modified)
            .map(|age| age > max_age)
            .unwrap_or(true)
    }

    /// Writes the cache to disk only if [`mark_changed`](Self::mark_changed)
    /// has been called since the last load/save.
    pub async fn save(&self) {
        if !self.changed {
            return;
        }
        if let Some(parent) = self.path.parent()
            && let Err(e) = tokio::fs::create_dir_all(parent).await
        {
            log::warn!(
                "failed to create cache directory: {} error: {}",
                parent.display(),
                e
            );
            return;
        }
        match serde_json::to_string_pretty(&self.data) {
            Ok(json) => {
                if let Err(e) = tokio::fs::write(&self.path, json).await {
                    log::warn!(
                        "failed to write cache: {} error: {}",
                        self.path.display(),
                        e
                    );
                } else {
                    log::debug!("saved cache: {}", self.path.display());
                }
            }
            Err(e) => {
                log::warn!("failed to serialise cache: {}", e);
            }
        }
    }

    /// Marks the cache as dirty so the next [`save`](Self::save) actually
    /// writes to disk.
    pub fn mark_changed(&mut self) {
        self.changed = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_new_path() {
        let dir = tempfile::tempdir().unwrap();
        let cache = JsonFileCache::<Vec<String>>::new(dir.path(), "test");
        assert_eq!(cache.path, dir.path().join(".cache/test.json"));
        assert!(!cache.changed);
        assert!(cache.data.is_empty());
    }

    #[tokio::test]
    async fn test_cache_load_missing() {
        let dir = tempfile::tempdir().unwrap();
        let mut cache = JsonFileCache::<Vec<String>>::new(dir.path(), "missing");
        cache.load().await;
        assert!(cache.data.is_empty());
    }

    #[tokio::test]
    async fn test_cache_save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let mut cache = JsonFileCache::<Vec<String>>::new(dir.path(), "roundtrip");
        cache.data = vec!["BTCUSDT".into(), "ETHUSDT".into()];
        cache.mark_changed();
        cache.save().await;

        let mut loaded = JsonFileCache::<Vec<String>>::new(dir.path(), "roundtrip");
        loaded.load().await;
        assert_eq!(loaded.data, vec!["BTCUSDT", "ETHUSDT"]);
    }

    #[tokio::test]
    async fn test_cache_save_without_changed_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let cache = JsonFileCache::<Vec<String>>::new(dir.path(), "noop");
        cache.save().await;
        // no file should be created
        assert!(!cache.path.exists());
    }

    #[tokio::test]
    async fn test_cache_mark_changed() {
        let mut cache = JsonFileCache::<Vec<String>>::new(Path::new("/tmp"), "dirty");
        assert!(!cache.changed);
        cache.mark_changed();
        assert!(cache.changed);
    }

    #[tokio::test]
    async fn test_cache_corrupt_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".cache/corrupt.json");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "not valid json").unwrap();

        let mut cache = JsonFileCache::<Vec<String>>::new(dir.path(), "corrupt");
        cache.load().await;
        assert!(cache.data.is_empty());
    }

    #[tokio::test]
    async fn test_load_with_expiry_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let mut cache = JsonFileCache::<Vec<String>>::new(dir.path(), "fresh");
        cache.data = vec!["BTCUSDT".into()];
        cache.mark_changed();
        cache.save().await;

        let mut loaded = JsonFileCache::<Vec<String>>::new(dir.path(), "fresh");
        loaded.load_with_expiry(Duration::from_secs(3600)).await;
        assert_eq!(loaded.data, vec!["BTCUSDT"]);
    }

    #[tokio::test]
    async fn test_load_with_expiry_expired() {
        let dir = tempfile::tempdir().unwrap();
        let mut cache = JsonFileCache::<Vec<String>>::new(dir.path(), "stale");
        cache.data = vec!["BTCUSDT".into()];
        cache.mark_changed();
        cache.save().await;

        // Set file mtime to 2 weeks ago
        let old = SystemTime::now() - Duration::from_secs(14 * 86400);
        filetime::set_file_mtime(&cache.path, old.into()).ok();

        let mut loaded = JsonFileCache::<Vec<String>>::new(dir.path(), "stale");
        loaded.load_with_expiry(Duration::from_secs(7 * 86400)).await;
        assert!(loaded.data.is_empty());
    }

    #[tokio::test]
    async fn test_load_with_expiry_missing() {
        let dir = tempfile::tempdir().unwrap();
        let mut cache = JsonFileCache::<Vec<String>>::new(dir.path(), "missing");
        cache.load_with_expiry(Duration::from_secs(3600)).await;
        assert!(cache.data.is_empty());
    }
}
