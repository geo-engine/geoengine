mod codegen;
mod compiled;
mod dependencies;
pub mod error;
mod functions;
mod parser;
mod toolchain;
mod util;

pub use codegen::{DataType, ExpressionAst, Parameter};
pub use compiled::LinkedExpression;
pub use dependencies::ExpressionDependencies;
pub use functions::FUNCTION_PREFIX;
pub use parser::ExpressionParser;
pub use toolchain::write_minimal_toolchain_file;

pub use geoengine_expression_deps::*;

/// Checks if a string is a valid variable name
pub fn is_allowed_variable_name(name: &str) -> bool {
    name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.starts_with(FUNCTION_PREFIX)
}

/// Keywords of the expression language, compared case-insensitively.
/// They cannot be used as variable names.
pub const KEYWORDS: &[&str] = &["if", "else", "let", "nodata", "is", "true", "false"];

/// Words that are reserved for future language features, compared case-insensitively.
/// They cannot be used as variable names.
pub const RESERVED_KEYWORDS: &[&str] = &[
    "and", "or", "not", "in", "for", "while", "loop", "break", "continue", "return", "fn", "match",
];

/// Checks if a string can be written as a variable in an expression.
///
/// In contrast to [`is_allowed_variable_name`], this also checks
/// that the name starts with a letter and is not a keyword.
pub fn is_valid_variable_name(name: &str) -> bool {
    name.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
        && is_allowed_variable_name(name)
        && !KEYWORDS
            .iter()
            .chain(RESERVED_KEYWORDS)
            .any(|keyword| keyword.eq_ignore_ascii_case(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_valid_variable_names() {
        for name in [
            "A", "nir", "band_1", "B04", "type", "None", "iff", "nodata2",
        ] {
            assert!(is_valid_variable_name(name), "{name} should be valid");
        }

        for name in [
            "",
            "_1",
            "1st",
            "nir-1",
            "nir 1",
            "bänd",
            "if",
            "NODATA",
            "Is",
            "true",
            "False",
            "let",
            "else",
            "expression_fn_foo",
            "while",
            "Return",
        ] {
            assert!(!is_valid_variable_name(name), "{name} should be invalid");
        }
    }
}
