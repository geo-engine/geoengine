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

    #[snafu(display("Cannot store source code in temporary file"))]
    CannotGenerateSourceCodeFile {
        source: std::io::Error,
    },

    #[snafu(display("Cannot format source code"))]
    CannotFormatSourceCodeFile {
        source: std::io::Error,
    },

    #[snafu(display("Cannot store source code in temporary directory"))]
    CannotGenerateSourceCodeDirectory {
        source: std::io::Error,
    },

    #[snafu(display("Cannot compile expression"))]
    Compiler {
        source: std::io::Error,
    },

    #[snafu(display("Cannot load expression for execution"))]
    LinkExpression {
        source: libloading::Error,
    },

    Linker {
        error: String,
    },

    LinkedFunctionNotFound {
        error: String,
    },

    #[snafu(display("The expression must have a name"))]
    EmptyExpressionName,
}
