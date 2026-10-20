//! Local output-tree scan: which files already exist, and tmp-file cleanup.

use std::collections::HashSet;
use std::hash::{BuildHasher, Hash, Hasher};
use std::path::Path;
use std::sync::OnceLock;

use crate::AppError;

/// Scan `data_dir` for existing `.zip` files.
/// Also removes stale tmp files this tool created (`{name}.zip.{pid}.tmp`)
/// during the same walk, except those that belong to a still-running process
/// (see [`has_live_owner`]). Foreign `.tmp` files are left alone.
///
/// Returns 128-bit hashes of the relative paths (see [`rel_path_hash`])
/// rather than the paths themselves: the set can hold millions of entries
/// (≈200-300 MB as `String`s, ≈35 MB as `u128`s for 1.8M files).
///
/// Entries are consumed in bounded batches rather than materialized up front —
/// `walkdir::DirEntry` owns a `PathBuf`, so collecting all 1.8M of them would
/// cost as much memory as the set this function exists to keep small.
///
/// # Errors
///
/// Returns [`AppError::Join`] if the blocking scanner task fails to join.
pub(crate) async fn scan_local_files(data_dir: &Path) -> Result<LocalFileSet, AppError> {
    let dir = data_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        if !dir.exists() {
            return Ok(LocalFileSet::default());
        }
        Ok::<_, AppError>(scan_with_batch_size(&dir, SCAN_BATCH_ENTRIES))
    })
    .await
    .map_err(AppError::from)?
}

/// Upper bound on directory entries held in memory before a flush.
const SCAN_BATCH_ENTRIES: usize = 16 * 1024;

/// Walk `root` and hash its `.zip` files, flushing every `batch_entries`.
///
/// The (I/O-bound) traversal stays single-threaded while hashing and tmp
/// cleanup are spread across cores, and peak memory is one batch rather than
/// the whole tree. `batch_entries` is a parameter (rather than a constant) so
/// tests can exercise the flush logic with a small fixture.
fn scan_with_batch_size(root: &Path, batch_entries: usize) -> LocalFileSet {
    let mut batch: Vec<walkdir::DirEntry> = Vec::with_capacity(batch_entries);
    let mut merged = LocalFileSet::default();
    for e in walkdir::WalkDir::new(root) {
        match e {
            Ok(e) => {
                batch.push(e);
                if batch.len() >= batch_entries {
                    merged.absorb(process_batch(&batch, root));
                    // Reuse the allocation for the next batch.
                    batch.clear();
                }
            }
            Err(err) => tracing::warn!(error = ?err, "walkdir entry failed"),
        }
    }
    if !batch.is_empty() {
        merged.absorb(process_batch(&batch, root));
    }
    merged
}

/// Hash (or clean up) one batch of entries, using one thread per core.
fn process_batch(entries: &[walkdir::DirEntry], root: &Path) -> LocalFileSet {
    let n_threads = std::thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get)
        .min(entries.len().max(1));
    let chunk_size = entries.len().div_ceil(n_threads).max(1);

    let mut partials: Vec<LocalFileSet> = Vec::with_capacity(n_threads);
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(n_threads);
        for chunk in entries.chunks(chunk_size) {
            let root = root.to_path_buf();
            let handle = scope.spawn(move || hash_chunk(chunk, &root));
            handles.push(handle);
        }
        for handle in handles {
            match handle.join() {
                Ok(files) => partials.push(files),
                Err(panic) => tracing::warn!(
                    "scan worker thread panicked; its files may be re-downloaded: {panic:?}"
                ),
            }
        }
    });

    let mut merged = LocalFileSet::default();
    for partial in partials {
        merged.absorb(partial);
    }
    merged
}

/// Hash the `.zip` entries of one chunk and reclaim stale `.tmp` entries.
fn hash_chunk(chunk: &[walkdir::DirEntry], root: &Path) -> LocalFileSet {
    let mut files = LocalFileSet::default();
    for e in chunk {
        if !e.file_type().is_file() {
            continue;
        }
        match e.path().extension().and_then(|x| x.to_str()) {
            Some("zip" | "tmp") => {}
            _ => continue,
        }
        let Ok(rel) = e.path().strip_prefix(root) else {
            // The entry is not below `root`, so its relative path is unknown
            // and can never match a download candidate. Hashing the absolute
            // path instead would silently produce a set that never matches,
            // so the entry is dropped instead.
            tracing::warn!(path = %e.path().display(), "entry is outside the scan root");
            continue;
        };
        let s = rel.to_string_lossy();

        if s.ends_with(".tmp") {
            if !is_our_tmp(&s) {
                continue;
            }
            if has_live_owner(e.path()) {
                continue;
            }
            if let Err(err) = std::fs::remove_file(e.path()) {
                tracing::warn!(
                    path = %e.path().display(),
                    error = ?err,
                    "failed to remove tmp file",
                );
            }
            continue;
        }

        files.insert_hash(rel_path_hash(&s));
    }
    files
}

/// Returns true for tmp files this tool creates: `{name}.zip.{pid}.tmp`.
///
/// Any other `.tmp` file in the output tree belongs to somebody else and must
/// not be deleted (writers create `{dest}.{pid}.tmp` before the atomic
/// rename).
fn is_our_tmp(rel: &str) -> bool {
    let Some(base) = rel.strip_suffix(".tmp") else {
        return false;
    };
    let Some((stem, pid)) = base.rsplit_once('.') else {
        return false;
    };
    // Case-sensitive on purpose: this tool only ever writes lowercase `.zip`,
    // so `X.ZIP.123.tmp` belongs to somebody else and must not be reclaimed.
    #[allow(clippy::case_sensitive_file_extension_comparisons)]
    {
        stem.ends_with(".zip") && !pid.is_empty() && pid.bytes().all(|b| b.is_ascii_digit())
    }
}

/// Age below which a `.tmp` file is treated as possibly still in flight.
const LIVE_TMP_WINDOW: std::time::Duration = std::time::Duration::from_mins(5);

/// Returns true when a `.tmp` file looks like an in-flight download of a
/// still-running process, judging by its recent modification time.
///
/// PID-based liveness checks are platform-specific and vulnerable to PID reuse,
/// so we treat any tmp file modified within the last few minutes as potentially
/// live and leave it untouched. Older tmp files are considered stale (the
/// writer crashed or finished) and may be removed by the caller.
fn has_live_owner(path: &Path) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    let Ok(modified) = meta.modified() else {
        return false;
    };
    modified.elapsed().is_ok_and(|age| age < LIVE_TMP_WINDOW)
}

/// A set of locally present files, keyed by [`rel_path_hash`].
///
/// Wraps a `HashSet<u128>` so callers do not depend on the hashing scheme: the
/// raw set stays private and every entry point goes through `*_hash` or takes
/// a relative path directly.
#[derive(Clone)]
pub(crate) struct LocalFileSet(HashSet<u128, ahash::RandomState>);

impl Default for LocalFileSet {
    /// A set with the same fixed seeds as [`rel_path_hash`], so the hash of a
    /// path is the same whatever produced it.
    fn default() -> Self {
        Self(HashSet::with_hasher(ahash::RandomState::with_seeds(
            0x517c_c1b7_2722_0a95,
            0x2545_f491_4f6c_dd1d,
            0x9e37_79b9_7f4a_7c15,
            0xbf58_476d_1ce4_e5b9,
        )))
    }
}

impl LocalFileSet {
    /// Whether a relative path is present in the set.
    #[must_use]
    pub(crate) fn contains_rel_path(&self, rel: &str) -> bool {
        self.contains_hash(rel_path_hash(rel))
    }

    /// Whether a hash produced by [`rel_path_hash`] is present in the set.
    #[must_use]
    pub(crate) fn contains_hash(&self, hash: u128) -> bool {
        self.0.contains(&hash)
    }

    /// Insert a hash produced by [`rel_path_hash`] into the set.
    pub(crate) fn insert_hash(&mut self, hash: u128) {
        self.0.insert(hash);
    }

    /// Move every entry of `other` into this set.
    pub(crate) fn absorb(&mut self, other: Self) {
        if other.is_empty() {
            return;
        }
        if self.is_empty() {
            self.0 = other.0;
            return;
        }
        self.0.extend(other.0);
    }

    /// Number of entries.
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the set is empty.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Hash of a relative path as stored in the scanned file set.
///
/// The set is queried once per candidate download and can hold millions of
/// entries, so only a 128-bit hash is kept instead of the path itself (see
/// [`scan_local_files`] for the memory trade-off). A collision would skip one
/// file; two independent passes over the path make this practically
/// impossible, and the downstream kline reader checks for gaps anyway.
///
/// The hasher is `ahash` rather than `SipHash`: the paths are built by this
/// crate from a validated symbol and a fixed date, so they are never
/// attacker-controlled and a cryptographic hash buys nothing. The seeds are
/// fixed, which keeps the hash reproducible across runs.
///
/// Both inserts and lookups happen within the same process, so the hash only
/// needs to be stable for the duration of a run.
#[must_use]
pub(crate) fn rel_path_hash(rel: &str) -> u128 {
    static STATE: OnceLock<ahash::RandomState> = OnceLock::new();
    let state = STATE.get_or_init(|| {
        ahash::RandomState::with_seeds(
            0x517c_c1b7_2722_0a95,
            0x2545_f491_4f6c_dd1d,
            0x9e37_79b9_7f4a_7c15,
            0xbf58_476d_1ce4_e5b9,
        )
    });

    let first = state.hash_one(rel);
    // A different prefix makes the second pass decorrelated from the first.
    let mut second = state.build_hasher();
    second.write_u8(0xa5);
    rel.hash(&mut second);
    (u128::from(first) << 64) | u128::from(second.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scan_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let files = scan_local_files(dir.path()).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn scan_missing_dir_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        let files = scan_local_files(&missing).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn scan_finds_zip_files() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("binance/data/um/daily/klines/BTCUSDT/1m");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("BTCUSDT-1m-2024-01-01.zip"), b"zip").unwrap();
        let files = scan_local_files(dir.path()).await.unwrap();
        assert_eq!(files.len(), 1);
        assert!(files.contains_rel_path(
            "binance/data/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip"
        ));
    }

    #[test]
    fn rel_path_hash_distinguishes_paths() {
        assert_ne!(rel_path_hash("a.zip"), rel_path_hash("b.zip"));
    }

    #[test]
    fn rel_path_hash_is_stable_across_calls() {
        // The hash is used as a key: a value that changed between the insert
        // and the lookup would make the file invisible.
        assert_eq!(rel_path_hash("a/b/c.zip"), rel_path_hash("a/b/c.zip"));
    }

    #[test]
    fn rel_path_hash_distinguishes_similar_paths() {
        // Paths that differ only in their tail or in a separator must not
        // collide: a collision would silently skip a file.
        let paths = [
            "binance/data/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip",
            "binance/data/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-02.zip",
            "binance/data/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2024-01-01.zip",
        ];
        for (i, a) in paths.iter().enumerate() {
            for (j, b) in paths.iter().enumerate() {
                if i != j {
                    assert_ne!(rel_path_hash(a), rel_path_hash(b), "{a} vs {b}");
                }
            }
        }
    }

    #[tokio::test]
    async fn scan_ignores_non_data_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("readme.txt"), b"hello").unwrap();
        let files = scan_local_files(dir.path()).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn scan_ignores_foreign_tmp_files() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("binance/data/um/daily/klines/BTCUSDT/1m");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("BTCUSDT-1m-2024-01-01.zip"), b"zip").unwrap();
        // Not our tmp shape (`{name}.zip.{pid}.tmp`): belongs to someone else.
        std::fs::write(data.join("BTCUSDT-1m-2024-01-01.tmp"), b"tmp").unwrap();
        std::fs::write(data.join("BTCUSDT-1m-2024-01-01.zip.tmp"), b"tmp").unwrap();
        let files = scan_local_files(dir.path()).await.unwrap();
        assert_eq!(files.len(), 1);
        assert!(files.contains_rel_path(
            "binance/data/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip"
        ));
        assert!(data.join("BTCUSDT-1m-2024-01-01.tmp").exists());
        assert!(data.join("BTCUSDT-1m-2024-01-01.zip.tmp").exists());
    }

    #[test]
    fn is_our_tmp_shape() {
        assert!(is_our_tmp("a/b/x.zip.123.tmp"));
        assert!(is_our_tmp("x.zip.0.tmp"));
        assert!(!is_our_tmp("a/b/x.tmp")); // bare, no pid
        assert!(!is_our_tmp("a/b/x.zip.tmp")); // no pid
        assert!(!is_our_tmp("a/b/x.zip.12a.tmp")); // non-numeric pid
        assert!(!is_our_tmp("a/b/x.txt")); // not a tmp file
    }

    #[tokio::test]
    async fn scan_removes_dead_process_tmp_files() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("binance/data/um/daily/klines/BTCUSDT/1m");
        std::fs::create_dir_all(&data).unwrap();
        // Age the tmp file beyond the liveness window so it is treated as stale.
        let stale = data.join("BTCUSDT-1m-2024-01-01.zip.0.tmp");
        std::fs::write(&stale, b"stale").unwrap();
        let f = std::fs::File::open(&stale).unwrap();
        let old_time = std::time::SystemTime::now() - std::time::Duration::from_mins(10);
        f.set_times(std::fs::FileTimes::new().set_modified(old_time))
            .unwrap();
        drop(f);
        let files = scan_local_files(dir.path()).await.unwrap();
        assert!(files.is_empty());
        assert!(!stale.exists());
    }

    #[tokio::test]
    async fn scan_keeps_tmp_files_of_live_processes() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("binance/data/um/daily/klines/BTCUSDT/1m");
        std::fs::create_dir_all(&data).unwrap();
        // Our own PID is definitely alive: simulate our in-flight download.
        let live = data.join(format!(
            "BTCUSDT-1m-2024-01-01.zip.{}.tmp",
            std::process::id()
        ));
        std::fs::write(&live, b"in-flight").unwrap();
        let files = scan_local_files(dir.path()).await.unwrap();
        assert!(files.is_empty());
        assert!(live.exists());
    }

    // — LocalFileSet —

    #[test]
    fn contains_hash_agrees_with_contains_rel_path() {
        let mut set = LocalFileSet::default();
        set.insert_hash(rel_path_hash("a/b/c.zip"));
        assert!(set.contains_rel_path("a/b/c.zip"));
        assert!(set.contains_hash(rel_path_hash("a/b/c.zip")));
        assert!(!set.contains_rel_path("a/b/d.zip"));
    }

    #[test]
    fn absorb_unions_two_sets() {
        let mut a = LocalFileSet::default();
        a.insert_hash(rel_path_hash("a.zip"));
        let mut b = LocalFileSet::default();
        b.insert_hash(rel_path_hash("b.zip"));

        a.absorb(b);
        assert_eq!(a.len(), 2);
        assert!(a.contains_rel_path("a.zip"));
        assert!(a.contains_rel_path("b.zip"));
    }

    #[test]
    fn is_empty_reflects_the_set_contents() {
        let mut set = LocalFileSet::default();
        assert!(set.is_empty());
        set.insert_hash(rel_path_hash("a.zip"));
        assert!(!set.is_empty());
    }

    #[test]
    fn absorb_into_empty_and_from_empty_are_both_no_ops() {
        let mut target = LocalFileSet::default();
        target.insert_hash(rel_path_hash("a.zip"));
        target.absorb(LocalFileSet::default());
        assert_eq!(target.len(), 1);

        let mut fresh = LocalFileSet::default();
        fresh.absorb(LocalFileSet::default());
        assert!(fresh.is_empty());
    }

    // — deep trees —

    #[tokio::test]
    async fn scan_finds_zips_across_deeply_nested_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let mut expected = 0;
        for (i, tf) in ["1m", "5m", "1d"].iter().enumerate() {
            let sub = dir
                .path()
                .join("binance/data/um/daily/klines")
                .join(format!("SYM{i}USDT"))
                .join(tf);
            std::fs::create_dir_all(&sub).unwrap();
            std::fs::write(sub.join(format!("SYM{i}USDT-{tf}-2024-01-01.zip")), b"zip").unwrap();
            expected += 1;
        }
        let files = scan_local_files(dir.path()).await.unwrap();
        assert_eq!(files.len(), expected);
    }

    /// Flushing in batches must not change the result set, whatever the size.
    #[tokio::test]
    async fn batching_does_not_change_the_result_set() {
        let dir = tempfile::tempdir().unwrap();
        let n = 500;
        let sub = dir.path().join("binance/data/um/daily/klines/BTCUSDT/1m");
        std::fs::create_dir_all(&sub).unwrap();
        for i in 0..n {
            std::fs::write(sub.join(format!("BTCUSDT-1m-{i:06}.zip")), b"z").unwrap();
        }

        // Batch sizes that all divide the file count unevenly, so every run
        // ends with a partial final batch.
        for batch in [1, 7, 64, n - 1, n, n + 1] {
            let files = scan_with_batch_size(dir.path(), batch);
            assert_eq!(files.len(), n, "batch size {batch} lost entries");
        }

        // The production path must agree with the helper.
        assert_eq!(scan_local_files(dir.path()).await.unwrap().len(), n);
    }

    #[tokio::test]
    async fn scan_with_batch_size_handles_an_empty_tree() {
        let dir = tempfile::tempdir().unwrap();
        assert!(scan_with_batch_size(dir.path(), 4).is_empty());
    }
}
