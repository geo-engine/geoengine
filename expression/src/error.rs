use snafu::{Snafu, Whatever};

pub type Result<T, E = ExpressionError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum ExpressionError {
    #[snafu(display("Cannot create temporary directory"))]
    TempDir {
        source: std::io::Error,
    },

    #[snafu(display("Cannot create workspace for dependencies"))]
    DepsWorkspace {
        source: std::io::Error,
    },

    #[snafu(display("Cannot build dependencies"))]
    DepsBuild {
        /// [`cargo::util::errors::CargoResult`] has a lifetime that refers to a [`cargo::util::config::Config`].
        /// This is a debug print.
        source: Whatever,
    },

    CannotGenerateSourceCodeFile {
        error: String,
    },

    CannotGenerateSourceCodeDirectory {
        error: String,
    },
    CompileError {
        error: String,
    },
    Linker {
        error: String,
    },
    LinkedFunctionNotFound {
        error: String,
    },
}
