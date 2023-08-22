use async_trait::async_trait;
use geoengine_datatypes::pro::MlModelId;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::Result;

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MlModel {
    pub model_id: MlModelId,
    pub model_content: String,
}

impl MlModel {}

/// Handling of ml models provided by geo engine
#[async_trait]
pub trait MlModelDb: Send + Sync {
    async fn load_ml_model(&self, model_id: MlModelId) -> Result<MlModel>;

    async fn store_ml_model(&self, model: MlModel) -> Result<()>;
}
