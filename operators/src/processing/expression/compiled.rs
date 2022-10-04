use std::{
    fs::File,
    io::Write,
    mem::ManuallyDrop,
    path::{Path, PathBuf},
    process::Command,
};

use libloading::{Library, Symbol};
use tempfile::TempDir;

use super::{codegen::ExpressionAst, error::ExpressionError};

type Result<T, E = ExpressionError> = std::result::Result<T, E>;

/// Compiles and links an expression as a program and offers means to call it
pub struct LinkedExpression {
    library_folder: ManuallyDrop<TempDir>,
    library: ManuallyDrop<Library>,
    function_name: String,
}

impl LinkedExpression {
    pub fn new(ast: &ExpressionAst) -> Result<Self> {
        let library_folder = tempfile::tempdir().map_err(|error| {
            ExpressionError::CannotGenerateSourceCodeDirectory {
                error: error.to_string(),
            }
        })?;

        // TODO: use `rustfmt` / `formatted_code` in debug mode
        let input_filename =
            create_source_code_file(library_folder.path(), &ast.code()).map_err(|error| {
                ExpressionError::CannotGenerateSourceCodeFile {
                    error: error.to_string(),
                }
            })?;

        let library_filename =
            compile_file(library_folder.path(), &input_filename).map_err(|error| {
                ExpressionError::CompileError {
                    error: error.to_string(),
                }
            })?;

        let library = unsafe { Library::new(&library_filename) }.map_err(|error| {
            ExpressionError::CompileError {
                error: error.to_string(),
            }
        })?;

        Ok(Self {
            library_folder: ManuallyDrop::new(library_folder),
            library: ManuallyDrop::new(library),
            function_name: ast.name().to_string(),
        })
    }

    /// Returns a function with 1 input parameters
    #[allow(clippy::type_complexity)]
    pub unsafe fn function_1<A>(&self) -> Result<Symbol<fn(A) -> Option<f64>>> {
        self.library
            .get(self.function_name.as_bytes())
            .map_err(|error| ExpressionError::LinkedFunctionNotFound {
                error: error.to_string(),
            })
    }
    /// Returns a function with 3 input parameters
    #[allow(clippy::type_complexity)]
    pub unsafe fn function_2<A, B>(&self) -> Result<Symbol<fn(A, B) -> Option<f64>>> {
        self.library
            .get(self.function_name.as_bytes())
            .map_err(|error| ExpressionError::LinkedFunctionNotFound {
                error: error.to_string(),
            })
    }

    /// Returns an n-ary function
    #[allow(clippy::type_complexity)]
    pub unsafe fn function_nary<F>(&self) -> Result<Symbol<F>> {
        self.library
            .get(self.function_name.as_bytes())
            .map_err(|error| ExpressionError::LinkedFunctionNotFound {
                error: error.to_string(),
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

fn compile_file(library_folder: &Path, input_filename: &Path) -> Result<PathBuf, std::io::Error> {
    let output_filename = library_folder.join("libexpression.so");

    Command::new("rustc")
        .args(["--crate-type", "cdylib"])
        .args(["-C", "opt-level=3"])
        .args(["-A", "warnings"]) // TODO: show warnings in debug mode
        .arg("-o")
        .arg(&output_filename)
        .arg(input_filename)
        .status()?;

    Ok(output_filename)
}
