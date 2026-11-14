//! Integration tests for the standalone verifier binary.
//!
//! Each test builds a small tree on disk and runs the real executable, so
//! argument parsing, thread-pool setup, failure rendering and exit codes are
//! covered end to end. No network access is involved.

use std::path::Path;
use std::process::{Command, Output};

fn verifier() -> Command {
    Command::new(env!("CARGO_BIN_EXE_binance-data-verify"))
}

fn run(dir: &Path) -> Output {
    verifier().arg("--dir").arg(dir).output().unwrap()
}

/// Write a structurally valid archive with a single entry.
fn write_valid_zip(path: &Path) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = zip::ZipWriter::new(file);
    writer
        .start_file("row.csv", zip::write::SimpleFileOptions::default())
        .unwrap();
    std::io::Write::write_all(&mut writer, b"open,high\n1,2\n").unwrap();
    writer.finish().unwrap();
}

#[test]
fn a_tree_without_archives_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("notes.txt"), b"not an archive").unwrap();

    let output = run(dir.path());

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("no .zip archives found"),
        "stdout: {stdout}"
    );
}

#[test]
fn a_valid_archive_is_reported_as_verified() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_zip(&dir.path().join("spot/BTCUSDT/1d/BTCUSDT-1d-2024-01-01.zip"));

    let output = run(dir.path());

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("verified 1 archive(s)"), "stdout: {stdout}");
}

#[test]
fn a_corrupt_archive_fails_and_names_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("broken.zip");
    std::fs::write(&path, b"not a zip").unwrap();

    let output = run(dir.path());

    assert!(!output.status.success());
    let stderr = stderr(&output);
    assert!(stderr.contains("invalid zip archive"), "stderr: {stderr}");
    assert!(
        stderr.contains(&path.display().to_string()),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("1 archive(s) failed verification (scanned 1)"),
        "stderr: {stderr}"
    );
}

#[test]
fn a_mixed_tree_reports_every_archive() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_zip(&dir.path().join("a/valid.zip"));
    std::fs::create_dir_all(dir.path().join("b")).unwrap();
    std::fs::write(dir.path().join("b/corrupt.zip"), b"not a zip").unwrap();

    let output = run(dir.path());

    assert!(!output.status.success());
    let stderr = stderr(&output);
    assert!(
        stderr.contains("1 archive(s) failed verification (scanned 2)"),
        "stderr: {stderr}"
    );
}

#[test]
fn a_missing_directory_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing");

    let output = run(&missing);

    assert!(!output.status.success());
    let stderr = stderr(&output);
    assert!(stderr.contains("not a directory"), "stderr: {stderr}");
}

#[test]
fn an_absurd_job_count_still_works() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_zip(&dir.path().join("valid.zip"));

    let output = verifier()
        .arg("--dir")
        .arg(dir.path())
        .arg("--jobs")
        .arg("1000000")
        .output()
        .unwrap();

    assert!(output.status.success(), "stderr: {}", stderr(&output));
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}
