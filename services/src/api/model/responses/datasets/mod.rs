use crate::datasets::DatasetName;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub mod errors;

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(title = "Dataset Name Response")]
pub struct DatasetNameResponse {
    pub dataset_name: DatasetName,
}

impl From<DatasetName> for DatasetNameResponse {
    fn from(dataset_name: DatasetName) -> Self {
        Self { dataset_name }
    }
}
