//! Content checks for a single downloaded archive.
//!
//! These run before anything is renamed into place: the leading bytes must be
//! the zip signature, the body must match `Content-Length` when the server
//! sent one, and optionally the finished file must open as a zip.

use std::path::Path;

use crate::download::DownloadError;

/// Bytes every non-empty zip archive begins with.
pub(super) const ZIP_MAGIC: &[u8; 4] = b"PK\x03\x04";

/// Upper bound on chunks read while hunting for the zip magic.
///
/// Guards against a server that dribbles empty chunks: without it a stream of
/// zero-length frames would spin forever inside the retry window.
const MAX_MAGIC_CHUNKS: u32 = 8;

/// Read the leading bytes and require the zip magic.
///
/// Returns the bytes read (at least [`ZIP_MAGIC`]), so the caller can write
/// them to disk without re-reading. The magic is accumulated across chunks
/// rather than read from the first one: with chunked transfer encoding the
/// four magic bytes can straddle a frame boundary, and mistaking that for "not
/// a zip" would burn the entire retry window on a perfectly good file.
///
/// # Errors
///
/// Returns [`DownloadError::Failed`] for an empty or non-zip body, and
/// [`DownloadError::Http`] when reading the body fails.
pub(super) async fn read_zip_head(
    resp: &mut reqwest::Response,
    url: &str,
) -> Result<Vec<u8>, DownloadError> {
    let mut head = Vec::with_capacity(ZIP_MAGIC.len());
    let mut chunks_read = 0u32;
    while head.len() < ZIP_MAGIC.len() {
        chunks_read += 1;
        if chunks_read > MAX_MAGIC_CHUNKS {
            return Err(not_a_zip(url));
        }
        match resp.chunk().await {
            Ok(Some(chunk)) => head.extend_from_slice(&chunk),
            Ok(None) => {
                return Err(DownloadError::Failed {
                    url: url.to_owned(),
                    reason: "empty response body".into(),
                });
            }
            Err(e) => return Err(DownloadError::from(e)),
        }
    }
    if &head[..ZIP_MAGIC.len()] != ZIP_MAGIC {
        return Err(not_a_zip(url));
    }
    Ok(head)
}

/// Require the body length to match `Content-Length` when the server sent one.
///
/// # Errors
///
/// Returns [`DownloadError::Failed`] when the body is shorter or longer than
/// the declared length.
pub(super) fn check_content_length(
    written: u64,
    expected: Option<u64>,
    url: &str,
) -> Result<(), DownloadError> {
    if let Some(expected) = expected
        && written != expected
    {
        return Err(DownloadError::Failed {
            url: url.to_owned(),
            reason: format!("incomplete download: {written}/{expected} bytes"),
        });
    }
    Ok(())
}

/// Optionally check that a finished file is a readable zip archive.
///
/// A corrupt archive is worth another attempt, so the caller treats this as
/// transient.
///
/// # Errors
///
/// Returns [`DownloadError::Failed`] when `verify` is set and the file does
/// not open as a zip with at least one entry.
pub(super) async fn check_archive(
    path: &Path,
    url: &str,
    verify: bool,
) -> Result<(), DownloadError> {
    if !verify {
        return Ok(());
    }
    if let Err(e) = crate::verify::verify_zip_archive(path).await {
        return Err(DownloadError::Failed {
            url: url.to_owned(),
            reason: format!("zip verification failed: {e}"),
        });
    }
    Ok(())
}

fn not_a_zip(url: &str) -> DownloadError {
    DownloadError::Failed {
        url: url.to_owned(),
        reason: "response is not a zip archive".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zip_magic_constant_is_the_local_file_signature() {
        assert_eq!(ZIP_MAGIC, b"PK\x03\x04");
        assert_eq!(ZIP_MAGIC.len(), 4);
    }
}
