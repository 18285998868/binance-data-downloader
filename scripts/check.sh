#!/usr/bin/env bash
# OS: CachyOS / Ubuntu CI
# TIME: 2026-11-13
# DOC: Run the local and CI quality gate for binance-data-downloader.
# LOGIC:
#   STEP 1: Parse arguments and locate the repository root.
#   STEP 2: Run fmt, check, and clippy (all features / no default features).
#   STEP 3: Run tests with nextest when available, then doctests.
#   STEP 4: Build docs with warnings denied.
#   STEP 5: Run cargo-deny and cargo-audit when installed; print skip otherwise.
# USAGE:
#   ./scripts/check.sh [--ci]
#
# Q:
#   Q1: nextest/deny/audit are optional locally. CI installs them and runs the
#       same script, so a missing tool prints `skip`, never a fake `ok`.
#   Q2: There is no MSRV job: rust-toolchain.toml deliberately tracks the
#       latest stable, and the project does not declare a lower bound.
#   Q3: CI is Ubuntu-only: rlimit uses libc/Unix APIs, so Windows does not
#       build by design.

if [[ ${BASH_SOURCE[0]} == "$0" ]]; then
	set -Eeuo pipefail
fi

info() { printf '%s\n' "$*" >&2; }
die() {
	printf '%s: %s\n' "${0##*/}" "$*" >&2
	exit 1
}

usage() {
	printf 'usage: %s [--ci]\n' "${0##*/}" >&2
}

run() {
	info "==> $*"
	"$@"
}

main() {
	local ci=0
	local nextest_profile=default
	local repo_root
	local -a cargo_flags=(--locked)

	while (($#)); do
		case $1 in
		--ci) ci=1 ;;
		-h | --help)
			usage
			return 0
			;;
		*) die "unknown argument: $1" ;;
		esac
		shift
	done

	if ((ci)); then
		nextest_profile=ci
	fi

	command -v cargo >/dev/null 2>&1 || die "cargo not found in PATH"

	repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
	cd -- "$repo_root" || die "cannot enter $repo_root"

	info "gate: fmt"
	run cargo fmt --all -- --check

	info "gate: check"
	run cargo check --workspace --all-targets "${cargo_flags[@]}"

	info "gate: clippy (all features)"
	run cargo clippy --workspace --all-targets --all-features "${cargo_flags[@]}" -- -D warnings

	info "gate: clippy (no default features)"
	run cargo clippy --workspace --all-targets --no-default-features "${cargo_flags[@]}" -- -D warnings

	info "gate: tests"
	if command -v cargo-nextest >/dev/null 2>&1; then
		run cargo nextest run --workspace --all-features "${cargo_flags[@]}" --profile "$nextest_profile"
	else
		info "skip cargo-nextest (not installed); falling back to cargo test"
		run cargo test --workspace --all-features "${cargo_flags[@]}"
	fi

	info "gate: doctests"
	run cargo test --workspace --doc --all-features "${cargo_flags[@]}"

	info "gate: docs"
	run env RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --all-features "${cargo_flags[@]}"

	info "gate: benches"
	run cargo bench --workspace --no-run "${cargo_flags[@]}"

	if command -v cargo-deny >/dev/null 2>&1; then
		info "gate: cargo-deny"
		run cargo deny check
	else
		info "skip cargo-deny (not installed)"
	fi

	if command -v cargo-audit >/dev/null 2>&1; then
		info "gate: cargo-audit"
		run cargo audit
	else
		info "skip cargo-audit (not installed)"
	fi

	info "all gates passed"
}

[[ ${BASH_SOURCE[0]} != "$0" ]] || main "$@"
