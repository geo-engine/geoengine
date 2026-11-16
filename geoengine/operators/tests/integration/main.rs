//! All integration tests in one binary, so the operators library is linked only once.
//!
//! Do not remove this directory: `cargo test` builds the `gdalsource-process` binary only because
//! this package has integration tests, and the GDAL tests in operators and services run it.

mod gdalsource_process;
mod streams;
