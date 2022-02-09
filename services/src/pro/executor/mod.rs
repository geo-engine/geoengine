use crate::handlers::plots::WrappedPlotOutput;
use crate::workflows::workflow::WorkflowId;
use float_cmp::approx_eq;
use geoengine_datatypes::primitives::{BoundingBox2D, TimeInterval};
use geoengine_operators::pro::executor::ExecutorTaskDescription;

#[derive(Clone, Debug)]
pub struct PlotDescription {
    pub id: WorkflowId,
    pub spatial_bounds: BoundingBox2D,
    pub temporal_bounds: TimeInterval,
}

impl ExecutorTaskDescription for PlotDescription {
    type KeyType = WorkflowId;
    type ResultType = crate::error::Result<WrappedPlotOutput>;

    fn primary_key(&self) -> &Self::KeyType {
        &self.id
    }

    fn can_join(&self, other: &Self) -> bool {
        self.temporal_bounds == other.temporal_bounds
            && approx_eq!(BoundingBox2D, self.spatial_bounds, other.spatial_bounds)
    }

    fn slice_result(&self, result: &Self::ResultType) -> Option<Self::ResultType> {
        Some(match result {
            Ok(wpo) => Ok(wpo.clone()),
            Err(_e) => Err(crate::error::Error::NotYetImplemented),
        })
    }
}
