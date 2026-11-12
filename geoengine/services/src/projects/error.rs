use geoengine_datatypes::error::ErrorSource;
use snafu::prelude::*;

use super::{ProjectId, ProjectVersionId};
use crate::processing_graphs::processing_graph::ProcessingGraphId;
use geoengine_datatypes::dataset::LayerId;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(ProjectDbError)))]
pub enum ProjectDbError {
    #[snafu(display("Project {project} does not exist"))]
    ProjectNotFound { project: ProjectId },
    #[snafu(display("Version {version} of project {project} does not exist"))]
    ProjectVersionNotFound {
        project: ProjectId,
        version: ProjectVersionId,
    },
    #[snafu(display("Updating project {project} failed"))]
    ProjectUpdateFailed { project: ProjectId },
    #[snafu(display("Layer {layer} is not part of project {project}"))]
    UnknownLayer { project: ProjectId, layer: LayerId },
    #[snafu(display("Layer {layer} appears more than once in project {project}"))]
    DuplicateLayer { project: ProjectId, layer: LayerId },
    #[snafu(display("Layer id {layer} is not a valid UUID"))]
    InvalidLayerId { layer: LayerId },
    #[snafu(display("Processing graph {processing_graph} does not exist"))]
    ProcessingGraphNotFound { processing_graph: ProcessingGraphId },
    #[snafu(display("The processing graph of a project layer is invalid: {source}"))]
    InvalidProcessingGraph { source: serde_json::Error },
    #[snafu(display("Layer {layer} does not exist or is not accessible"))]
    LayerNotFound { layer: LayerId },
    #[snafu(display("Accessing project {project} failed: {source}"))]
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
