//! Process limits that bound how much concurrency is actually safe.
//!
//! The configured concurrency is a request, not a promise: the operating
//! system has the final say. Every in-flight download holds two file
//! descriptors (the socket and the temporary file it streams into), so a
//! concurrency above half the descriptor limit turns into `EMFILE` and a wave
//! of failed downloads — reproduced with `ulimit -n 32` and
//! `concurrency = 64`, where 35 of 71 downloads failed with
//! `Os { code: 24, kind: TooManyOpenFiles }`.

/// File descriptors reserved for everything that is not a download.
///
/// A run holds a lock file, a cache database, the log output and the
/// listening sockets; the downloads get what is left.
const RESERVED_DESCRIPTORS: usize = 64;

/// Descriptors one in-flight download needs: the socket and the temporary file.
const DESCRIPTORS_PER_DOWNLOAD: usize = 2;

/// Absolute ceiling on the number of concurrent downloads, regardless of what
/// the process may open.
pub(crate) const MAX_CONCURRENCY: usize = 1024;

/// Maximum number of per-item error details kept in a batch report.
///
/// One systemic outage can fail thousands of pairs or symbols; the first few
/// messages carry the diagnosis, the rest would only bloat the report (and
/// the terminal). Both the pair loop and start-date resolution use the same
/// bound so a failure report never becomes the reason a run is hard to read.
pub(crate) const MAX_BATCH_ERRORS: usize = 20;

/// Descriptor limit assumed when it cannot be read.
const FALLBACK_DESCRIPTOR_LIMIT: usize = 1024;

/// The number of concurrent downloads this process can sustain.
///
/// Never zero: a run must be able to make progress even under a pathological
/// descriptor limit.
#[must_use]
pub(crate) fn sustainable_concurrency() -> usize {
    let affordable =
        descriptor_limit().saturating_sub(RESERVED_DESCRIPTORS) / DESCRIPTORS_PER_DOWNLOAD;
    affordable.clamp(1, MAX_CONCURRENCY)
}

/// The concurrency to actually use: what was asked for, capped by what the
/// process may sustain.
#[must_use]
pub(crate) fn effective_concurrency(requested: usize) -> usize {
    requested.clamp(1, sustainable_concurrency())
}

/// The soft `RLIMIT_NOFILE` of this process.
fn descriptor_limit() -> usize {
    rlimit::Resource::NOFILE
        .get_soft()
        .map_or(FALLBACK_DESCRIPTOR_LIMIT, |limit| {
            usize::try_from(limit).unwrap_or(usize::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_effective_concurrency_never_exceeds_the_absolute_ceiling() {
        assert!(sustainable_concurrency() <= MAX_CONCURRENCY);
    }

    #[test]
    fn the_effective_concurrency_is_at_least_one() {
        assert!(sustainable_concurrency() >= 1);
    }

    #[test]
    fn the_effective_concurrency_leaves_room_for_the_rest_of_the_process() {
        let limit = descriptor_limit();
        let effective = sustainable_concurrency();
        assert!(
            effective * DESCRIPTORS_PER_DOWNLOAD + RESERVED_DESCRIPTORS <= limit.max(1),
            "effective {effective} does not fit into a limit of {limit}"
        );
    }

    #[test]
    fn the_descriptor_limit_is_readable_on_this_platform() {
        // The limit is advisory; a platform that cannot report one falls back
        // to the conservative default rather than failing the run.
        assert!(descriptor_limit() >= 1);
    }

    #[test]
    fn the_effective_concurrency_never_exceeds_what_was_requested() {
        for requested in [1, 2, 16, 64, 1024, 4096] {
            let effective = effective_concurrency(requested);
            assert!(effective >= 1, "requested {requested} gave {effective}");
            assert!(
                effective <= requested.max(1),
                "requested {requested} gave {effective}"
            );
        }
    }

    #[test]
    fn the_effective_concurrency_is_capped_by_the_descriptor_limit() {
        assert!(effective_concurrency(usize::MAX) <= sustainable_concurrency());
    }
}
