//! Library surface of the `binance-data-cli` package.
//!
//! The two binaries are thin wrappers around this library: they parse
//! arguments, call one function, render the result and map it to an exit code.
//! Everything that can be tested without spawning a process lives here.
//!
//! [`verify`] walks an existing output tree and validates every archive, for
//! the case where files were copied, restored from a backup, or written by an
//! older build.

pub mod verify;
