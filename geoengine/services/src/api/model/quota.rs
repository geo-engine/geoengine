use crate::{api::model::processing_graphs::ProcessingGraphId, quota::ComputationId};
use geoengine_datatypes::primitives::DateTime;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ComputationQuota {
    pub timestamp: DateTime,
    pub computation_id: ComputationId,
    pub processing_graph_id: ProcessingGraphId,
    pub count: u64,
}

impl From<crate::quota::ComputationQuota> for ComputationQuota {
    fn from(value: crate::quota::ComputationQuota) -> Self {
        Self {
            timestamp: value.timestamp,
            computation_id: value.computation_id,
            processing_graph_id: value.workflow_id.into(),
            count: value.count,
        }
    }
}
