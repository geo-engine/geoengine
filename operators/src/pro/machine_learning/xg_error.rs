use snafu::Snafu;
use xgboost_rs::XGBError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)), module(error))]
pub enum XGBoostModuleError {
    #[snafu(display("XG Booster instance could not complete training process.",))]
    BoosterTrainingError { source: XGBError },

    #[snafu(display("The XGBoost library could not complete the operation successfully.",))]
    LibraryError { source: XGBError },

    #[snafu(display("Couldn't parse the model file contents.",))]
    ModelFileParsingError { source: std::io::Error },

    #[snafu(display("Could not write model to buffer.",))]
    ModelStorageError { source: XGBError },

    #[snafu(display("Couldn't create a booster instance from the content of the model file.",))]
    LoadBoosterFromModelError { source: XGBError },

    #[snafu(display("Couldn't generate a xgboost dmatrix from the given data.",))]
    CreateDMatrixError { source: XGBError },

    #[snafu(display("Couldn't set the label data for the given dmatrix.",))]
    DMatrixSetLabelsError { source: XGBError },

    #[snafu(display("Couldn't calculate predictions from the given data.",))]
    PredictionError { source: XGBError },

    #[snafu(display("Couldn't get a base tile.",))]
    BaseTileError,

    #[snafu(display("No input data error. At least one raster is required.",))]
    NoInputData,

    #[snafu(display("There was an error with the creation of a new grid.",))]
    DataTypesError {
        source: geoengine_datatypes::error::Error,
    },

    #[snafu(display("There was an error with the joining of tokio tasks.",))]
    TokioJoinError { source: tokio::task::JoinError },
}

impl From<std::io::Error> for XGBoostModuleError {
    fn from(source: std::io::Error) -> Self {
        Self::ModelFileParsingError { source }
    }
}

impl From<XGBError> for XGBoostModuleError {
    fn from(source: XGBError) -> Self {
        Self::LibraryError { source }
    }
}
