use std::num::ParseFloatError;

use super::parser::PestError;
use snafu::Snafu;

#[derive(Debug, Snafu, Clone, PartialEq)]
#[snafu(visibility = "pub(crate)")]
pub enum ExpressionError {
    UnknownVariable { variable: String },
    UnexpectedBranchStructure,
    BranchStructureMalformed,
    UnexpectedRule { rule: String },
    DoesNotEndWithExpression,
    UnexpectedOperator { operator: String },
    UnexpectedComparator { comparator: String },
    UnexpectedBooleanRule { rule: String },
    UnexpectedBooleanOperator { operator: String },
    ComparisonNeedsThreeParts,
    CannotAssignToParameter { parameter: String },
    AssignmentNeedsTwoParts,
    ParserError { source: PestError },
    EmptyExpressionName,
    EmptyParameterName,
    DuplicateParameterName { parameter: String },
    InvalidNumber { source: ParseFloatError },
    MissingFunctionName,
    RustFmtMissing,
    RustFmtError { error: String },
    RustFmtIoError { error: String },
    RustFmtConversionError { source: std::string::FromUtf8Error },
    CannotGenerateSourceCodeFile { source: String },
}
