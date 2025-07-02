use snafu::Snafu;
use strum::IntoStaticStr;

use super::MlModelName;

#[derive(Debug, Snafu, IntoStaticStr)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(MachineLearningError)), module(error))] // disables default `Snafu` suffix
pub enum MachineLearningError {
    CouldNotFindMlModelFile {
        source: Box<crate::error::Error>,
    },
    ModelNotFound {
        name: MlModelName,
    },
    DuplicateMlModelName {
        name: MlModelName,
    },
    InvalidModelNamespace {
        name: MlModelName,
    },
    #[snafu(display("An unexpected database error occurred."))]
    Postgres {
        source: tokio_postgres::Error,
    },
    #[snafu(display("An unexpected database error occurred."))]
    Bb8 {
        source: bb8_postgres::bb8::RunError<tokio_postgres::Error>,
    },
    #[snafu(display("An underlying MachineLearningError occured: {source}"))]
    MachineLearning {
        source: Box<geoengine_operators::machine_learning::MachineLearningError>,
    },
}

impl From<bb8_postgres::tokio_postgres::error::Error> for MachineLearningError {
    fn from(e: bb8_postgres::tokio_postgres::error::Error) -> Self {
        Self::Postgres { source: e }
    }
}

impl From<geoengine_operators::machine_learning::MachineLearningError> for MachineLearningError {
    fn from(e: geoengine_operators::machine_learning::MachineLearningError) -> Self {
        Self::MachineLearning {
            source: Box::new(e),
        }
    }
}
