mod codegen;
mod compiled;
mod dependencies;
pub mod error;
mod functions;
mod parser;
mod util;

pub use codegen::{DataType, ExpressionAst, Parameter};
pub use compiled::LinkedExpression;
pub use dependencies::ExpressionDependencies;
pub use functions::FUNCTION_PREFIX;
pub use parser::ExpressionParser;
pub use util::write_minimal_toolchain_file;

pub use geoengine_expression_deps::*;

/// Checks if a string is a valid variable name
pub fn is_allowed_variable_name(name: &str) -> bool {
    name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.starts_with(FUNCTION_PREFIX)
}
