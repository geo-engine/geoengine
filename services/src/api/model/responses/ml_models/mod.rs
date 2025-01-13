use serde::{Deserialize, Serialize};
use utoipa::ToResponse;

use crate::machine_learning::name::MlModelName;

#[derive(Debug, Serialize, Deserialize, Clone, ToResponse)]
#[serde(rename_all = "camelCase")]
#[response(description = "Name of generated resource", example = json!({
    "name": "ns:name"
}))]
pub struct MlModelNameResponse {
    pub dataset_name: MlModelName,
}

impl From<MlModelName> for MlModelNameResponse {
    fn from(dataset_name: MlModelName) -> Self {
        Self { dataset_name }
    }
}
