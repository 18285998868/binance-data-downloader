//! Integration tests for the two binaries in this package.
//!
//! They spawn the real executables built by Cargo, so argument parsing,
//! exit codes, and error rendering are covered end to end. No test touches
//! the network or an exchange endpoint.

use std::path::PathBuf;
use std::process::Command;

/// A path that must not exist, unique enough for a parallel test run.
fn missing_path(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "binance-data-cli-test-{}-{name}",
        std::process::id()
    ));
    assert!(
        !path.exists(),
        "test fixture unexpectedly exists: {}",
        path.display()
    );
    path
}

fn downloader() -> Command {
    Command::new(env!("CARGO_BIN_EXE_binance-data-downloader"))
}

fn verifier() -> Command {
    Command::new(env!("CARGO_BIN_EXE_binance-data-verify"))
}

#[test]
fn help_succeeds_and_documents_the_config_flag() {
    let output = downloader().arg("--help").output().unwrap();

    assert!(output.status.success(), "status: {}", output.status);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("-c, --config"), "stdout: {stdout}");
}

#[test]
fn a_missing_config_flag_is_rejected() {
    let output = downloader().output().unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--config"), "stderr: {stderr}");
}

#[test]
fn an_unreadable_config_file_reports_a_config_error() {
    let path = missing_path("config.toml");
    let output = downloader().arg("-c").arg(&path).output().unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("config error"), "stderr: {stderr}");
}

#[test]
fn verify_help_succeeds_and_documents_the_dir_flag() {
    let output = verifier().arg("--help").output().unwrap();

    assert!(output.status.success(), "status: {}", output.status);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--dir"), "stdout: {stdout}");
}

#[test]
fn verify_rejects_a_missing_directory() {
    let path = missing_path("tree");
    let output = verifier().arg("--dir").arg(&path).output().unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not a directory"), "stderr: {stderr}");
}
