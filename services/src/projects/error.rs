use geoengine_datatypes::error::ErrorSource;
use snafu::prelude::*;

use super::ProjectId;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(ProjectDbError)))]
pub enum ProjectDbError {
    #[snafu(display("Project {project} does not exist"))]
    ProjectNotFound { project: ProjectId },
    #[snafu(display("Updating project {project} failed"))]
    ProjectUpdateFailed { project: ProjectId },
    #[snafu(display("Accessing project {project} failed"))]
    AccessFailed {
        project: ProjectId,
        source: Box<dyn ErrorSource>,
    },
    #[snafu(display("An unexpected database error occurred."))]
    Postgres { source: tokio_postgres::Error },
    #[snafu(display("An unexpected database error occurred."))]
    Bb8 {
        source: bb8_postgres::bb8::RunError<tokio_postgres::Error>,
    },
}
