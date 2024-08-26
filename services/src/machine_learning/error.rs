use snafu::Snafu;

use super::MlModelName;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(MachineLearningError)), module(error))] // disables default `Snafu` suffix
pub enum MachineLearningError {
    CouldNotFindMlModelFile {
        source: crate::error::Error,
    },
    ModelNotFound {
        name: MlModelName,
    },
    DuplicateMlModelName {
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
}

impl From<bb8_postgres::tokio_postgres::error::Error> for MachineLearningError {
    fn from(e: bb8_postgres::tokio_postgres::error::Error) -> Self {
        Self::Postgres { source: e }
    }
}
