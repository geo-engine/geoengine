use crate::codegen::DataType;
use snafu::Snafu;
use std::fmt::{Debug, Display};

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
        debug: String,
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

#[derive(Clone, PartialEq, Eq)]
pub struct ExpressionParserError {
    source: crate::parser::PestError,
}

impl Display for ExpressionParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.source, f)
    }
}

impl Debug for ExpressionParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.source, f)
    }
}

impl std::error::Error for ExpressionParserError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.source()
    }
}

impl ExpressionParserError {
    pub fn from_syntactic_error(source: crate::parser::PestError) -> Self {
        Self { source }
    }

    pub fn from_semantic_error(source: &ExpressionSemanticError, span: pest::Span) -> Self {
        Self {
            source: crate::parser::PestError::new_from_span(
                pest::error::ErrorVariant::CustomError {
                    message: source.to_string(),
                },
                span,
            ),
        }
    }

    pub fn from_definition_error(source: &ExpressionSemanticError) -> Self {
        Self {
            source: crate::parser::PestError::new_from_pos(
                pest::error::ErrorVariant::CustomError {
                    message: source.to_string(),
                },
                // we cannot point to anything other than the start of the file
                pest::Position::from_start(""),
            ),
        }
    }
}

impl ExpressionSemanticError {
    pub fn into_parser_error(self, span: pest::Span) -> ExpressionParserError {
        ExpressionParserError::from_semantic_error(&self, span)
    }

    pub fn into_definition_parser_error(self) -> ExpressionParserError {
        ExpressionParserError::from_definition_error(&self)
    }
}

/// An expression error type that concern the parsing of user code
#[derive(Debug, Snafu, Clone, PartialEq, Eq)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum ExpressionSemanticError {
    #[snafu(display("The expression function must have a name"))]
    EmptyExpressionName,

    #[snafu(display("A parameter name must not be empty"))]
    EmptyParameterName,

    #[snafu(display("The parameter `{parameter}` was defined multiple times"))]
    DuplicateParameterName {
        parameter: String,
    },

    #[snafu(display("The variable `{variable}` was not defined"))]
    UnknownVariable {
        variable: String,
    },

    #[snafu(display("The variable `{variable}` was already defined"))]
    VariableShadowing {
        variable: String,
    },

    UnknownBooleanVariable {
        variable: String,
    },

    #[snafu(display("Unknown function `{function}`"))]
    UnknownFunction {
        function: String,
    },

    #[snafu(display(
        "Invalid function arguments for function `{name}`: expected {expected}, got {actual}",
        expected = display_strings(expected),
        actual = display_strings(actual)
    ))]
    InvalidFunctionArguments {
        name: String,
        expected: Vec<String>,
        actual: Vec<DataType>,
    },

    #[snafu(display("The if-then-else expression is missing a branch"))]
    MissingBranch,

    #[snafu(display("The if-then-else expression must have an else part"))]
    BranchStructureMalformed,

    #[snafu(display(
        "To output a value, the last statement must be an expression and not an assignment"
    ))]
    DoesNotEndWithExpression,

    #[snafu(display("Unexpected rule: {rule}"))]
    UnexpectedRule {
        rule: String,
    },

    #[snafu(display("Unexpected operator: expected (+, -, *, /, **), found {found}"))]
    UnexpectedOperator {
        found: String,
    },

    #[snafu(display("Unexpected operator: expected (<, <=, ==, !=, >=, >), found {comparator}"))]
    UnexpectedComparator {
        comparator: String,
    },

    #[snafu(display("Unexpected boolean rule: {rule}"))]
    UnexpectedBooleanRule {
        rule: String,
    },

    #[snafu(display("Unexpected boolean operator: {operator}"))]
    UnexpectedBooleanOperator {
        operator: String,
    },

    #[snafu(display("A comparison needs a left part, a comparator and a right part"))]
    ComparisonNeedsThreeParts,

    #[snafu(display("An assignment needs a variable name and an expression"))]
    AssignmentNeedsTwoParts,

    #[snafu(display("The constant `{constant}` is not a number"))]
    ConstantIsNotAdNumber {
        source: std::num::ParseFloatError,
        constant: String,
    },

    #[snafu(display("The function call is missing a function name"))]
    MalformedFunctionCall,

    #[snafu(display("The expression of form `A IS NODATA` is missing the left part"))]
    MalformedIdentifierIsNodata,

    #[snafu(display("All branches of an if-then-else expression must output the same type"))]
    AllBranchesMustOutputSameType,

    #[snafu(display("Comparisons can only be used with numbers"))]
    ComparisonsMustBeUsedWithNumbers,

    #[snafu(display("Operators can only be used with numbers"))]
    OperatorsMustBeUsedWithNumbers,

    #[snafu(display(
        "The expression was expected to output `{expected}`, but it outputs `{actual}`"
    ))]
    WrongOutputType {
        expected: DataType,
        actual: DataType,
    },
}

/// User-facing display of a list of strings
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

/// This module only ensures that the error types are `Send` and `Sync`.
mod send_sync_ensurance {
    use super::*;

    trait SendSyncEnsurance: Send + Sync {}

    impl SendSyncEnsurance for ExpressionExecutionError {}

    impl SendSyncEnsurance for ExpressionSemanticError {}

    impl SendSyncEnsurance for ExpressionParserError {}
}
