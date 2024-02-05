use crate::datasets::DatasetName;
use serde::{Deserialize, Serialize};
use utoipa::ToResponse;

pub mod errors;

#[derive(Debug, Serialize, Deserialize, Clone, ToResponse)]
#[serde(rename_all = "camelCase")]
#[response(description = "Name of generated resource", example = json!({
    "name": "ns:name"
}))]
pub struct DatasetNameResponse {
    pub dataset_name: DatasetName,
}

impl From<DatasetName> for DatasetNameResponse {
    fn from(dataset_name: DatasetName) -> Self {
        Self { dataset_name }
    }
}
