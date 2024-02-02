use geoengine_expression::error::{ExpressionExecutionError, ExpressionParserError};
use snafu::Snafu;
use std::sync::Arc;

#[derive(Debug, Snafu)]
#[snafu(
    visibility(pub(crate)),
    context(suffix(false)), // disables default `Snafu` suffix
    module(raster))
]
pub enum RasterExpressionError {
    #[snafu(display("{}", source), context(false))]
    Parser { source: ExpressionParserError },

    #[snafu(display("Cannot generate dependencies: {source}."))]
    Dependencies {
        source: Arc<ExpressionExecutionError>,
    },

    #[snafu(display("{}", source), context(false))]
    Execution { source: ExpressionExecutionError },
}

#[derive(Debug, Snafu)]
#[snafu(
    visibility(pub(crate)),
    context(suffix(false)), // disables default `Snafu` suffix
    module(vector))
]
pub enum VectorExpressionError {
    #[snafu(display("Input column `{name}` does not exist."))]
    InputColumnNotExisting { name: String },

    #[snafu(display("Input column `{name}` is not numeric."))]
    InputColumnNotNumeric { name: String },

    #[snafu(display("Found {found} columns, but only up to {max} are allowed."))]
    TooManyInputColumns { max: usize, found: usize },

    #[snafu(display("The column `{name}` contains special characters."))]
    ColumnNameContainsSpecialCharacters { name: String },

    #[snafu(display("Output column `{name}` already exists."))]
    OutputColumnCollision { name: String },

    #[snafu(display("Cannot create `DataCollection`."))]
    CannotGenerateDataOutput,

    #[snafu(display("Cannot generate dependencies: {source}."))]
    Dependencies {
        source: Arc<geoengine_expression::error::ExpressionExecutionError>,
    },

    #[snafu(display("Cannot parse expression: {source}"), context(false))]
    Parsing {
        source: geoengine_expression::error::ExpressionParserError,
    },

    #[snafu(display("Compilation task failed"), context(false))]
    CompilationTask { source: tokio::task::JoinError },

    #[snafu(display("Cannot call expression function: {source}."), context(false))]
    Executing {
        source: geoengine_expression::error::ExpressionExecutionError,
    },

    #[snafu(display("Cannot add column {name}."))]
    AddColumn {
        name: String,
        source: geoengine_datatypes::error::Error,
    },

    #[snafu(display("Could not replace geometries."))]
    ReplaceGeometries {
        source: geoengine_datatypes::error::Error,
    },

    #[snafu(display("Cannot filter collection for empty output geometries."))]
    FilterEmptyGeometries {
        source: geoengine_datatypes::error::Error,
    },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub), context(suffix(false)))]
pub enum ExpressionDependenciesInitializationError {
    #[snafu(
        display("Could not compile expression dependencies: {source}"),
        context(false)
    )]
    Compilation { source: ExpressionExecutionError },

    #[snafu(display("Could not execute initialization task"), context(false))]
    Task { source: tokio::task::JoinError },
}
/// This module only ensures that the error types are `Send` and `Sync`.
mod send_sync_ensurance {
    use super::*;

    trait SendSyncEnsurance: Send + Sync {}

    impl SendSyncEnsurance for RasterExpressionError {}

    impl SendSyncEnsurance for VectorExpressionError {}

    impl SendSyncEnsurance for ExpressionDependenciesInitializationError {}
}
