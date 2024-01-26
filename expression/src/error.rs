use crate::codegen::DataType;
use snafu::{Snafu, Whatever};
use std::fmt::Display;

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
    // TODO: use pest's error format for every parsing error?
    // We would get lines and columns with this.
    #[snafu(display("The expression is erroneous: {source}"))]
    Parser {
        source: crate::parser::PestError,
    },

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
