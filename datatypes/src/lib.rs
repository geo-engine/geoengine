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
    clippy::trait_duplication_in_bounds, // TODO: reactive when not buggy
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

/// This macro resolves paths of files or folders in the `test_data` folder.
/// The `test_data` folder is located in the workspace root and has the same name as this macro.
/// To address data from the `test_data` folder you can use the macro like this:
///
/// Assuming a file "test.tiff" in `test_data` with the path `test_data/test.tiff` call the macro with `test_data!("test.tiff")`.
/// Assuming a file "more-data.json" in `test_data/vector/` with the path "test_data/vector/more-data.csv" call the macro with `test_data!("vector/move-data.csv")`.
///
/// # Panics
/// * if the path of the parent folder of `env!("CARGO_MANIFEST_DIR")` is unresolvable.
///
#[macro_export]
macro_rules! test_data {
    ($name:expr) => {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .canonicalize() // get a full path
            .unwrap()
            .parent()
            .unwrap()
            .join("test_data/")
            .join($name)
            .as_path()
    };
}
