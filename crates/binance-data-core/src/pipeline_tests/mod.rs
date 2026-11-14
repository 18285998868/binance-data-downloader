//! End-to-end pipeline tests over a mock exchange.
//!
//! These drive `run_with_endpoints`, so config loading, preparation,
//! start-date resolution, the pair loop, downloads, cancellation and the
//! final report are all exercised together.
//!
//! The module lives under `src/` rather than `tests/` on purpose:
//! `run_with_endpoints` and [`crate::config::for_tests`] are crate-private
//! seams, and exposing them as public API only for tests would widen the
//! surface the project has to keep stable.
//!
//! Tests are grouped by behaviour:
//!
//! * [`downloads`] — the download window, 404 tolerance, thresholds and skip
//!   rules;
//! * [`start_dates`] — API/archive resolution and its degradation rules;
//! * [`cancel`] — cooperative cancellation at every stage;
//! * [`prepare`] — the exclusive directory lock.
//!
//! [`harness`] holds the shared mock server and fixtures.

mod cancel;
mod downloads;
mod harness;
mod prepare;
mod start_dates;
