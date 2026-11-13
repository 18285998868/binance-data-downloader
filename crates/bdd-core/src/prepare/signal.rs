//! Graceful shutdown: one Ctrl-C flag observed by the pipeline.

use crate::cancel::CancelToken;

/// Owns the Ctrl-C task and aborts it when the run ends.
///
/// Aborting on drop keeps the library usable by a long-lived host process:
/// the signal handler must not outlive the run that installed it.
pub(crate) struct SignalGuard(tokio::task::JoinHandle<()>);

impl Drop for SignalGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Install the Ctrl-C handler for one run.
///
/// The handler flips the shared [`CancelToken`]; every stage observes it, so a
/// long batch stops promptly instead of after the current pair.
pub(crate) fn install(cancel: CancelToken) -> SignalGuard {
    let task = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::warn!("cancellation requested; stopping in-flight downloads");
            cancel.cancel();
        }
    });
    SignalGuard(task)
}
