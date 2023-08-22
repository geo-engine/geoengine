use snafu::Snafu;

#[cfg(feature = "xgboost")]
use xgboost_rs::XGBError;

use geoengine_datatypes::{error::ErrorSource, pro::MlModelId};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)), module(error))]
pub enum MachineLearningError {
    #[snafu(display("XG Booster instance could not complete training process.",))]
    #[cfg(feature = "xgboost")]
    BoosterTrainingError {
        source: XGBError,
    },

    #[snafu(display("The XGBoost library could not complete the operation successfully.",))]
    #[cfg(feature = "xgboost")]
    LibraryError {
        source: XGBError,
    },

    #[snafu(display("Couldn't parse the model file contents.",))]
    ModelFileParsingError {
        source: std::io::Error,
    },

    #[snafu(display("Could not write model to buffer.",))]
    #[cfg(feature = "xgboost")]
    ModelStorageError {
        source: XGBError,
    },

    #[snafu(display("Couldn't create a booster instance from the content of the model file.",))]
    #[cfg(feature = "xgboost")]
    LoadBoosterFromModelError {
        source: XGBError,
    },

    #[snafu(display("Couldn't generate a xgboost dmatrix from the given data.",))]
    #[cfg(feature = "xgboost")]
    CreateDMatrixError {
        source: XGBError,
    },

    #[snafu(display("Couldn't set the label data for the given dmatrix.",))]
    #[cfg(feature = "xgboost")]
    DMatrixSetLabelsError {
        source: XGBError,
    },

    #[snafu(display("Couldn't calculate predictions from the given data.",))]
    #[cfg(feature = "xgboost")]
    PredictionError {
        source: XGBError,
    },

    #[snafu(display("Couldn't get a base tile.",))]
    BaseTileError,

    #[snafu(display("No input data error. At least one raster is required.",))]
    NoInputData,

    #[snafu(display("There was an error with the creation of a new grid.",))]
    DataTypesError {
        source: geoengine_datatypes::error::Error,
    },

    #[snafu(display("There was an error with the joining of tokio tasks.",))]
    TokioJoinError {
        source: tokio::task::JoinError,
    },

    #[snafu(display(
        "There must be the same number of feature names and feature workflows provided. Got {} feature workflows and {} feature names instead.",
        n_feature_workflows,
        feature_names
    ))]
    WrongNumberOfFeatureNamesProvided {
        n_feature_workflows: usize,
        feature_names: usize,
    },

    #[snafu(display(
        "There must be the same number of label names and label workflows provided. Got {} label workflows and {} label names instead.",
        n_label_workflows,
        label_names
    ))]
    WrongNumberOfLabelNamesProvided {
        n_label_workflows: usize,
        label_names: usize,
    },

    MachineLearningMustHaveAtLeastOneFeatureAndOneLabel,
    MachineLearningFeatureDataNotAvailable,
    #[snafu(display("The aggregator at index {} could not be referenced", index))]
    CouldNotGetMlAggregatorRef {
        index: usize,
    },
    #[snafu(display("The feature name at index {} could not be found", index))]
    CouldNotGetMlFeatureName {
        index: usize,
    },
    CouldNotGetMlLabelKeyName,
    CouldNotGetMlModelPath,
    CouldNotGetMlModelUUID,

    CouldNotGetMlModelConfigPath {
        source: Box<dyn ErrorSource>,
    },

    // XGB Config Builder Errors
    // TODO: refactor these errors into a separate module?
    #[snafu(display("The number of distinct classes in the label data must be set!"))]
    UndefinedNumberOfClasses,

    #[snafu(display("Unknown or unsupported booster type: {}", s))]
    UnknownBoosterType {
        s: String,
    },

    #[snafu(display("Unknown or unsupported tree method: {}", s))]
    UnknownTreeMethod {
        s: String,
    },

    #[snafu(display("Unknown or unsupported process type: {}", s))]
    UnknownProcessType {
        s: String,
    },

    #[snafu(display("Unknown or unsupported objective function type: {}", s))]
    UnknownObjective {
        s: String,
    },

    #[snafu(display("Unknown or unsupported grow policy: {}", s))]
    UnknownGrowPolicy {
        s: String,
    },

    #[snafu(display("Unknown or unsupported predictor type: {}", s))]
    UnknownPredictorType {
        s: String,
    },

    #[snafu(display("Invalid refresh leaf input: {}. Only 0 | 1 is allowed as input", s))]
    InvalidRefreshLeafInput {
        s: String,
    },

    #[snafu(display("Unknown or unsupported config parameter for xgboost: {}", s))]
    UnknownXGBConfigParameter {
        s: String,
    },

    #[snafu(display(
        "Invalid 'eta' input for xgboost: {}. 'eta' should be between 0 and 1 (inclusive).",
        s
    ))]
    InvalidEtaInput {
        s: String,
    },

    #[snafu(display("Invalid 'max_leaves' input for xgboost: {}. 'max_leaves' should be a non-negative integer.", s))]
    InvalidMaxLeavesInput {
        s: String,
    },

    #[snafu(display("Invalid 'min_child_weight' input for xgboost: {}. 'min_child_weight' should be a non-negative value.", s))]
    InvalidMinChildWeightInput {
        s: String,
    },

    #[snafu(display("Invalid 'subsample' input for xgboost: {}. 'subsample' should be between 0 and 1 (inclusive).", s))]
    InvalidSubsampleInput {
        s: String,
    },

    #[snafu(display("Invalid 'scale_pos_weight' input for xgboost: {}. 'scale_pos_weight' should be a non-negative value.", s))]
    InvalidScalePosWeightInput {
        s: String,
    },

    #[snafu(display("Invalid 'base_score' input for xgboost: {}.", s))]
    InvalidBaseScoreInput {
        s: String,
    },

    #[snafu(display(
        "Invalid 'gamma' input for xgboost: {}. 'gamma' should be a non-negative value.",
        s
    ))]
    InvalidGammaInput {
        s: String,
    },

    #[snafu(display(
        "Invalid 'seed' input for xgboost: {}. 'seed' should be a non-negative integer.",
        s
    ))]
    InvalidSeedInput {
        s: String,
    },

    #[snafu(display(
        "Invalid 'num_class' input for xgboost: {}. 'num_class' should be a positive integer.",
        s
    ))]
    InvalidNumClassInput {
        s: String,
    },
    #[snafu(display(
        "Invalid 'silent' input for xgboost: {}. 'silent' should be either '0' or '1'.",
        s
    ))]
    InvalidSilentInput {
        s: String,
    },

    #[snafu(display(
        "Invalid 'max_depth' input for xgboost: {}. 'max_depth' should be a non-negative integer.",
        s
    ))]
    InvalidMaxDepthInput {
        s: String,
    },

    #[snafu(display("Invalid 'colsample_bylevel' input for xgboost: {}. 'colsample_bylevel' should be between 0 and 1 (inclusive).", s))]
    InvalidColsampleBylevelInput {
        s: String,
    },

    #[snafu(display("Invalid 'colsample_bytree' input for xgboost: {}. 'colsample_bytree' should be between 0 and 1 (inclusive).", s))]
    InvalidColsampleBytreeInput {
        s: String,
    },

    #[snafu(display("Invalid 'colsample_bynode' input for xgboost: {}. 'colsample_bynode' should be between 0 and 1 (inclusive).", s))]
    InvalidColsampleBynodeInput {
        s: String,
    },

    #[snafu(display(
        "Invalid 'sketch_eps' input for xgboost: {}. 'sketch_eps' should be a non-negative value.",
        s
    ))]
    InvalidSketchEpsInput {
        s: String,
    },

    #[snafu(display(
        "Invalid 'max_bin' input for xgboost: {}. 'max_bin' should be a positive integer.",
        s
    ))]
    InvalidMaxBinInput {
        s: String,
    },

    #[snafu(display("Invalid 'max_delta_step' input for xgboost: {}. 'max_delta_step' should be a non-negative value.", s))]
    InvalidMaxDeltaStepInput {
        s: String,
    },

    #[snafu(display("Invalid 'num_parallel_tree' input for xgboost: {}. 'num_parallel_tree' should be a positive integer.", s))]
    InvalidNumParallelTreeInput {
        s: String,
    },

    #[snafu(display(
        "Invalid 'lambda' input for xgboost: {}. 'lambda' should be a non-negative value.",
        s
    ))]
    InvalidLambdaInput {
        s: String,
    },

    #[snafu(display(
        "Invalid 'alpha' input for xgboost: {}. 'alpha' should be a non-negative value.",
        s
    ))]
    InvalidAlphaInput {
        s: String,
    },
    #[snafu(display(
        "Invalid 'verbosity' input for xgboost: {}. 'verbosity' should be either true or false.",
        s
    ))]
    InvalidDebugLevelInput {
        s: String,
    },

    #[snafu(display("Invalid 'colsample_bylevel' input for xgboost: {}. 'colsample_bylevel' should be between 0 and 1 (inclusive).", s))]
    InvalidColsampleByLevelInput {
        s: String,
    },

    #[snafu(display("Invalid 'colsample_bytree' input for xgboost: {}. 'colsample_bytree' should be between 0 and 1 (inclusive).", s))]
    InvalidColsampleByTreeInput {
        s: String,
    },

    #[snafu(display("Invalid 'colsample_bynode' input for xgboost: {}. 'colsample_bynode' should be between 0 and 1 (inclusive).", s))]
    InvalidColsampleByNodeInput {
        s: String,
    },
    #[snafu(display("Invalid booster parameters: {}", s))]
    InvalidBoosterParameters {
        s: String,
    },
    #[snafu(display("Invalid learning task parameters: {}", s))]
    InvalidLearningTaskParameters {
        s: String,
    },

    #[snafu(display("Invalid tree booster parameters: {}", s))]
    InvalidTreeBoosterParameters {
        s: String,
    },

    #[snafu(display("The model with id {} was not found in postgres", model_id))]
    UnknownModelIdInPostgres {
        model_id: MlModelId,
    },
}

impl From<std::io::Error> for MachineLearningError {
    fn from(source: std::io::Error) -> Self {
        Self::ModelFileParsingError { source }
    }
}

#[cfg(feature = "xgboost")]
impl From<XGBError> for MachineLearningError {
    fn from(source: XGBError) -> Self {
        Self::LibraryError { source }
    }
}
