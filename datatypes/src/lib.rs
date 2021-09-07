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
    clippy::similar_names,
    clippy::single_match_else,
    clippy::type_repetition_in_bounds,
    clippy::wildcard_imports
)]
// enable some restriction lints
#![warn(clippy::print_stdout, clippy::print_stderr, clippy::dbg_macro)]

pub mod collections;
pub mod dataset;
pub mod error;
pub mod operations;
pub mod plots;
pub mod primitives;
pub mod raster;
pub mod spatial_reference;
pub mod util;

/// Compiles Geo Engine Pro
#[cfg(feature = "pro")]
pub mod pro;

#[macro_export]
macro_rules! test_data {
    ($name:expr) => {
        dbg!(std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .canonicalize() // get a full path
            .unwrap()
            .parent()
            .unwrap()
            .join("test_data/") // TODO: move somewhere else!
            .join($name)
            //.canonicalize() // strip all the ./ ../ and so on
            //.unwrap()
            .as_path())
    };
}
