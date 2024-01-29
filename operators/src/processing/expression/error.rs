use geoengine_expression::error::{ExpressionExecutionError, ExpressionParserError};
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum ExpressionError {
    #[snafu(display("{}", source), context(false))]
    Parser { source: ExpressionParserError },

    #[snafu(display("{}", source), context(false))]
    Execution { source: ExpressionExecutionError },
    // UnknownVariable {
    //     variable: String,
    // },
    // UnknownBooleanVariable {
    //     variable: String,
    // },
    // UnknownFunction {
    //     function: String,
    // },
    // InvalidFunctionArgumentCount {
    //     function: String,
    //     expected_min: usize,
    //     expected_max: usize,
    //     actual: usize,
    // },
    // UnexpectedBranchStructure,
    // BranchStructureMalformed,
    // UnexpectedRule {
    //     rule: String,
    // },
    // DoesNotEndWithExpression,
    // UnexpectedOperator {
    //     operator: String,
    // },
    // UnexpectedComparator {
    //     comparator: String,
    // },
    // UnexpectedBooleanRule {
    //     rule: String,
    // },
    // UnexpectedBooleanOperator {
    //     operator: String,
    // },
    // ComparisonNeedsThreeParts,
    // CannotAssignToParameter {
    //     parameter: String,
    // },
    // AssignmentNeedsTwoParts,
    // EmptyExpressionName,
    // EmptyParameterName,
    // DuplicateParameterName {
    //     parameter: String,
    // },
    // InvalidNumber {
    //     source: ParseFloatError,
    // },
    // MissingFunctionName,
    // CannotGenerateSourceCodeFile {
    //     error: String,
    // },
    // CannotGenerateSourceCodeDirectory {
    //     error: String,
    // },
    // CompileError {
    //     error: String,
    // },
    // Linker {
    //     error: String,
    // },
    // LinkedFunctionNotFound {
    //     error: String,
    // },
    // MissingIdentifier,
    // MissingOutputNoDataValue,
    // SourcesMustBeConsecutive,
}
/// This module only ensures that the error types are `Send` and `Sync`.
mod send_sync_ensurance {
    use super::*;

    trait SendSyncEnsurance: Send + Sync {}

    impl SendSyncEnsurance for ExpressionError {}
}
