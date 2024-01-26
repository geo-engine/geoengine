use std::fmt::Display;

use snafu::{Snafu, Whatever};

use crate::codegen::DataType;

// pub type Result<T, E = ExpressionExecutionError> = std::result::Result<T, E>;

/// An expression error type that concern the compilation, linkage and execution of an expression
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum ExpressionExecutionError {
    #[snafu(display("Cannot create temporary directory"))]
    TempDir { source: std::io::Error },

    #[snafu(display("Cannot create workspace for dependencies"))]
    DepsWorkspace { source: std::io::Error },

    #[snafu(display("Cannot build dependencies"))]
    DepsBuild {
        /// [`cargo::util::errors::CargoResult`] has a lifetime that refers to a [`cargo::util::config::Config`].
        /// This is a debug print.
        source: Whatever,
    },

    #[snafu(display("Cannot store source code in temporary file"))]
    CannotGenerateSourceCodeFile { source: std::io::Error },

    #[snafu(display("Cannot format source code"))]
    CannotFormatSourceCodeFile { source: std::io::Error },

    #[snafu(display("Cannot store source code in temporary directory"))]
    CannotGenerateSourceCodeDirectory { source: std::io::Error },

    #[snafu(display("Cannot compile expression"))]
    Compiler { source: std::io::Error },

    #[snafu(display("Cannot load expression for execution"))]
    LinkExpression { source: libloading::Error },

    #[snafu(display("Unknown function in expression: {name}"))]
    LinkedFunctionNotFound {
        source: libloading::Error,
        name: String,
    },
}

/// An expression error type that concern the parsing of user code
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum ExpressionParserError {
    #[snafu(display("The expression function must have a name"))]
    EmptyExpressionName,

    #[snafu(display("The variable {variable} was not defined"))]
    UnknownVariable {
        variable: String,
    },

    #[snafu(display("The variable {variable} was already defined"))]
    VariableShadowing {
        variable: String,
    },

    UnknownBooleanVariable {
        variable: String,
    },

    #[snafu(display("Unknown function: {function}"))]
    UnknownFunction {
        function: String,
    },

    #[snafu(display(
        "Invalid function arguments for function {name}: expected {expected}, got {actual}",
        expected = display_strings(expected),
        actual = display_strings(actual)
    ))]
    InvalidFunctionArguments {
        name: String,
        expected: Vec<String>,
        actual: Vec<DataType>,
    },

    UnexpectedBranchStructure,
    BranchStructureMalformed,

    DoesNotEndWithExpression,

    #[snafu(display("Unexpected rule: {rule}"))]
    UnexpectedRule {
        rule: String,
    },

    #[snafu(display("Unexpected operator: expected {expected}, found {found}"))]
    UnexpectedOperator {
        expected: &'static str,
        found: String,
    },

    UnexpectedComparator {
        comparator: String,
    },
    UnexpectedBooleanRule {
        rule: String,
    },
    UnexpectedBooleanOperator {
        operator: String,
    },
    ComparisonNeedsThreeParts,
    AssignmentNeedsTwoParts,
    Parser {
        source: crate::parser::PestError,
    },
    EmptyParameterName,
    DuplicateParameterName {
        parameter: String,
    },
    InvalidNumber {
        source: std::num::ParseFloatError,
    },
    MissingFunctionName,
    MissingIdentifier,
    MissingOutputNoDataValue,
    SourcesMustBeConsecutive,
}

fn display_strings<S: Display>(strings: &[S]) -> String {
    let mut output = String::new();
    output.push('[');
    for (i, string) in strings.iter().enumerate() {
        if i > 0 {
            output.push_str(", ");
        }
        output.push_str(&string.to_string());
    }
    output.push(']');
    output
}
