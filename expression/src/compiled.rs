use crate::{
    error::{self, ExpressionExecutionError},
    ExpressionDependencies,
};
use libloading::{library_filename, Library, Symbol};
use snafu::ResultExt;
use std::{
    borrow::Cow,
    fs::File,
    io::Write,
    mem::ManuallyDrop,
    path::{Path, PathBuf},
    process::Command,
};
use tempfile::TempDir;

pub type Result<T, E = ExpressionExecutionError> = std::result::Result<T, E>;

/// Compiles and links an expression as a program and offers means to call it
pub struct LinkedExpression {
    library_folder: ManuallyDrop<TempDir>,
    library: ManuallyDrop<Library>,
    function_name: String,
}

impl LinkedExpression {
    pub fn new(
        function_name: &str,
        code: &str,
        dependencies: &ExpressionDependencies,
    ) -> Result<Self> {
        let library_folder =
            tempfile::tempdir().context(error::CannotGenerateSourceCodeDirectory)?;

        // format code in debug mode
        let mut code = Cow::from(code);
        if std::cfg!(debug_assertions) {
            code = syn::parse_file(&code)
                .map_or_else(
                    |e| {
                        // fallback to unformatted code
                        log::error!("Cannot parse expression: {e}");
                        code.to_string()
                    },
                    |file| prettyplease::unparse(&file),
                )
                .into();
        }

        let input_filename = create_source_code_file(library_folder.path(), &code)
            .context(error::CannotGenerateSourceCodeFile)?;

        let library_filename = compile_file(library_folder.path(), &input_filename, dependencies)
            .context(error::Compiler)?;

        let library = unsafe { Library::new(library_filename) }.context(error::LinkExpression)?;

        Ok(Self {
            library_folder: ManuallyDrop::new(library_folder),
            library: ManuallyDrop::new(library),
            function_name: function_name.to_string(),
        })
    }

    /// Returns a function with 1 input parameters
    ///
    /// # Safety
    ///
    /// The caller must ensure that the function is called with the correct type of input parameter
    ///
    #[allow(clippy::type_complexity)]
    pub unsafe fn function_1<A>(&self) -> Result<Symbol<fn(A) -> Option<f64>>> {
        self.library
            .get(self.function_name.as_bytes())
            .context(error::LinkedFunctionNotFound {
                name: self.function_name.clone(),
            })
    }
    /// Returns a function with 3 input parameters
    ///
    /// # Safety
    ///
    /// The caller must ensure that the function is called with the correct type of input parameter
    ///
    #[allow(clippy::type_complexity)]
    pub unsafe fn function_2<A, B>(&self) -> Result<Symbol<fn(A, B) -> Option<f64>>> {
        self.library
            .get(self.function_name.as_bytes())
            .context(error::LinkedFunctionNotFound {
                name: self.function_name.clone(),
            })
    }

    /// Returns an n-ary function
    ///
    /// # Safety
    ///
    /// The caller must ensure that the function is called with the correct type of input and output parameters
    ///
    #[allow(clippy::type_complexity)]
    pub unsafe fn function_nary<F>(&self) -> Result<Symbol<F>> {
        self.library
            .get(self.function_name.as_bytes())
            .context(error::LinkedFunctionNotFound {
                name: self.function_name.clone(),
            })
    }
}

impl Drop for LinkedExpression {
    fn drop(&mut self) {
        // first unlink program…
        unsafe { ManuallyDrop::drop(&mut self.library) };

        // …then delete files
        unsafe { ManuallyDrop::drop(&mut self.library_folder) };
    }
}

fn create_source_code_file(
    library_folder: &Path,
    source_code: &str,
) -> Result<PathBuf, std::io::Error> {
    let input_filename = library_folder.join("expression.rs");

    let mut file = File::create(&input_filename)?;
    file.write_all(source_code.as_bytes())?;

    Ok(input_filename)
}

fn compile_file(
    library_folder: &Path,
    input_filename: &Path,
    dependencies: &ExpressionDependencies,
) -> Result<PathBuf, std::io::Error> {
    let output_filename = library_folder.join(library_filename("expression"));

    let mut command = Command::new("rustc");
    command
        .args(["--edition", "2021"])
        .args(["--crate-type", "cdylib"])
        .args(["-C", "opt-level=3"])
        .arg("-L")
        .arg(dependencies.linker_path());

    if !std::cfg!(debug_assertions) {
        command.args(["-A", "warnings"]);
    }

    command
        .arg("-o")
        .arg(&output_filename)
        .arg(input_filename)
        .status()?;

    Ok(output_filename)
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    #[test]
    #[allow(clippy::float_cmp)]
    fn it_compiles_hello_world() {
        let dependencies = ExpressionDependencies::new().unwrap();

        let source_code = quote! {
            extern crate geo;

            use geo::{Area, polygon};

            #[no_mangle]
            pub extern "Rust" fn area_of_polygon() -> f64 {
                let polygon = polygon![
                    (x: 0., y: 0.),
                    (x: 5., y: 0.),
                    (x: 5., y: 6.),
                    (x: 0., y: 6.),
                    (x: 0., y: 0.),
                ];

                polygon.signed_area()
            }
        }
        .to_string();

        let linked_expression =
            LinkedExpression::new("area_of_polygon", &source_code, &dependencies).unwrap();

        assert_eq!(
            unsafe { linked_expression.function_nary::<fn() -> f64>().unwrap() }(),
            30.0
        );
    }

    // TODO: try out generated expression
}
