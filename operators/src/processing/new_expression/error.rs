use std::num::ParseFloatError;

use super::parser::PestError;
use snafu::Snafu;

#[derive(Debug, Snafu, Clone, PartialEq)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum ExpressionError {
    UnknownVariable { variable: String },
    UnknownBooleanVariable { variable: String },
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
    Parser { source: PestError },
    EmptyExpressionName,
    EmptyParameterName,
    DuplicateParameterName { parameter: String },
    InvalidNumber { source: ParseFloatError },
    MissingFunctionName,
    CannotGenerateSourceCodeFile { error: String },
    CannotGenerateSourceCodeDirectory { error: String },
    CompileError { error: String },
    Linker { error: String },
    LinkedFunctionNotFound { error: String },
}
