// configure default clippy lints
#![deny(clippy::correctness)]
#![warn(clippy::complexity, clippy::style, clippy::perf, clippy::pedantic)]
// disable some pedantic lints
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::default_trait_access,
    clippy::missing_errors_doc,
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::non_ascii_literal,
    clippy::option_if_let_else,
    clippy::result_large_err, // TODO: investigate this
    clippy::similar_names,
    clippy::single_match_else,
    clippy::type_repetition_in_bounds,
    clippy::wildcard_imports
)]
// enable some restriction lints
#![warn(clippy::print_stdout, clippy::print_stderr, clippy::dbg_macro)]

pub mod api;
#[cfg(not(feature = "pro"))]
pub mod apidoc;
pub mod contexts;
pub mod datasets;
pub mod error;
pub mod handlers;
pub mod layers;
pub mod ogc;
pub mod projects;
#[cfg(not(feature = "pro"))]
pub mod server;
pub mod stac;
#[macro_use]
pub mod util;
pub mod tasks;
pub mod workflows;

/// Compiles Geo Engine Pro
#[cfg(feature = "pro")]
pub mod pro;

pub use geoengine_datatypes::test_data;
