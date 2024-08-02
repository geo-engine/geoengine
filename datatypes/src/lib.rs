pub mod collections;
pub mod dataset;
pub mod error;
pub mod operations;
pub mod plots;
pub mod primitives;
pub mod raster;
pub mod spatial_reference;
pub mod util;

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
            .expect("should be available during testing")
            .parent()
            .expect("should be available during testing")
            .join("test_data/")
            .join($name)
            .as_path()
    };
}
