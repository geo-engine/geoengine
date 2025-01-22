use serde::{Deserialize, Serialize};
use utoipa::{ToResponse, ToSchema};

use crate::machine_learning::name::MlModelName;

#[derive(Debug, Serialize, Deserialize, Clone, ToResponse, ToSchema)]
#[serde(rename_all = "camelCase")]
#[response(description = "Name of generated resource", example = json!({
    "name": "ns:name"
}))]
pub struct MlModelNameResponse {
    pub ml_model_name: MlModelName,
}

impl From<MlModelName> for MlModelNameResponse {
    fn from(ml_model_name: MlModelName) -> Self {
        Self { ml_model_name }
    }
}
