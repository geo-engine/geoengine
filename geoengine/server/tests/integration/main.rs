#![allow(clippy::unwrap_used, clippy::print_stdout, clippy::print_stderr)] // okay in tests

//! All integration tests in one binary, so the services library is linked only once.

mod drivers;
mod openapi;
mod startup;
