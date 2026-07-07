use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::{LazyLock, Mutex};

use redb::{Database, TableDefinition};

const TABLE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("data");

static DBS: LazyLock<Mutex<HashMap<String, Arc<Database>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn db_path(domain: &str) -> PathBuf {
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(format!(".cache/{domain}/{domain}.redb")),
        None => PathBuf::from(format!("/tmp/{domain}/{domain}.redb")),
    }
}

fn ensure_db(domain: &str) -> Option<Arc<Database>> {
    let mut map = DBS.lock().unwrap_or_else(|e| {
        tracing::warn!(domain, "cache mutex poisoned: {e}");
        e.into_inner()
    });
    if let Some(db) = map.get(domain) {
        return Some(Arc::clone(db));
    }
    let path = db_path(domain);
    if let Some(parent) = path.parent()
        && let Err(e) = std::fs::create_dir_all(parent)
    {
        tracing::error!(domain, path = %parent.display(), "cache ensure_db create_dir_all: {e}");
        return None;
    }
    let db = match Database::create(&path) {
        Ok(db) => Arc::new(db),
        Err(e) => {
            tracing::error!(domain, path = %path.display(), "cache ensure_db create: {e}");
            return None;
        }
    };
    map.insert(domain.to_string(), Arc::clone(&db));
    Some(db)
}

pub fn read(domain: &str, key: &str, ttl_secs: Option<u64>) -> Option<Vec<u8>> {
    let db = ensure_db(domain)?;
    let read_tx = match db.begin_read() {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(domain, "cache read begin_read: {e}");
            return None;
        }
    };
    let table = match read_tx.open_table(TABLE) {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(domain, "cache read open_table: {e}");
            return None;
        }
    };
    let guard = match table.get(key) {
        Ok(Some(g)) => g,
        Ok(None) => return None,
        Err(e) => {
            tracing::warn!(domain, key, "cache read get: {e}");
            return None;
        }
    };
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

pub fn write(domain: &str, key: &str, data: &[u8]) {
    let db = match ensure_db(domain) {
        Some(db) => db,
        None => {
            tracing::warn!(domain, "cache write skipped: ensure_db failed");
            return;
        }
    };
    let cached_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut buf = Vec::with_capacity(8 + data.len());
    buf.extend_from_slice(&cached_at.to_le_bytes());
    buf.extend_from_slice(data);
    if let Ok(write_tx) = db.begin_write() {
        if let Ok(mut table) = write_tx.open_table(TABLE) {
            if table.insert(key, buf).is_err() {
                tracing::error!(domain, key, "cache write insert failed");
            }
        } else {
            tracing::error!(domain, key, "cache write open_table failed");
        }
        if write_tx.commit().is_err() {
            tracing::error!(domain, key, "cache write commit failed");
        }
    } else {
        tracing::error!(domain, key, "cache write begin_write failed");
    }
}
