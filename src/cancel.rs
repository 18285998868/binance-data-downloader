//! Cooperative cancellation shared by the pipeline and its in-flight downloads.
//!
//! The Ctrl-C handler flips one flag; the pair loop checks it between pairs and
//! every in-flight download races it via [`tokio::select!`], so a long batch
//! stops promptly instead of after the current pair.

use std::sync::Arc;

use tokio::sync::watch;

/// A cheap, cloneable cancellation signal.
///
/// Clones share one signal. [`CancelToken::cancel`] wakes every current waiter;
/// tokens created (or subscribed) afterwards observe the flag immediately, so
/// there is no window where a late download misses the cancellation.
#[derive(Clone)]
pub(crate) struct CancelToken {
    tx: Arc<watch::Sender<bool>>,
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}

impl CancelToken {
    /// Create a token in the not-yet-cancelled state.
    #[must_use]
    pub(crate) fn new() -> Self {
        let (tx, _rx) = watch::channel(false);
        Self { tx: Arc::new(tx) }
    }

    /// Signal cancellation to every holder and waiter. Idempotent.
    pub(crate) fn cancel(&self) {
        // `send_replace`, not `send`: tokio's `send` is a no-op when the channel
        // has no receivers, which would silently drop the flag whenever the
        // ctrl-c handler fires before any download subscribes.
        self.tx.send_replace(true);
    }

    /// Whether cancellation has been signalled.
    #[must_use]
    pub(crate) fn is_cancelled(&self) -> bool {
        *self.tx.borrow()
    }

    /// Resolve once cancellation has been signalled.
    ///
    /// Returns immediately if the flag is already set. Never resolves while the
    /// token is live: if every sender is dropped, cancellation can no longer
    /// happen, so this parks instead of silently completing.
    pub(crate) async fn cancelled(&self) {
        let mut rx = self.tx.subscribe();
        loop {
            if *rx.borrow_and_update() {
                return;
            }
            if rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn starts_uncancelled() {
        let token = CancelToken::new();
        assert!(!token.is_cancelled());
    }

    #[tokio::test]
    async fn cancel_sets_the_flag() {
        let token = CancelToken::new();
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn cancel_is_idempotent() {
        let token = CancelToken::new();
        token.cancel();
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn cancelled_resolves_immediately_when_already_cancelled() {
        let token = CancelToken::new();
        token.cancel();
        tokio::time::timeout(Duration::from_secs(5), token.cancelled())
            .await
            .expect("already-cancelled token must resolve");
    }

    #[tokio::test]
    async fn cancelled_resolves_for_a_late_subscriber() {
        // A download that starts after Ctrl-C must still observe the signal.
        let token = CancelToken::new();
        token.cancel();
        let late = token.clone();
        tokio::time::timeout(Duration::from_secs(5), late.cancelled())
            .await
            .expect("late subscriber must observe cancellation");
    }

    #[tokio::test]
    async fn cancelled_waits_until_cancel_is_called() {
        let token = CancelToken::new();
        let waiter = token.clone();
        let handle = tokio::spawn(async move { waiter.cancelled().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!handle.is_finished(), "must not resolve before cancel");
        token.cancel();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("waiter must resolve after cancel")
            .unwrap();
    }

    #[tokio::test]
    async fn cancelling_one_clone_affects_the_others() {
        let a = CancelToken::new();
        let b = a.clone();
        a.cancel();
        assert!(b.is_cancelled());
    }

    #[tokio::test]
    async fn distinct_tokens_are_independent() {
        let a = CancelToken::new();
        let b = CancelToken::new();
        a.cancel();
        assert!(!b.is_cancelled());
    }
}
