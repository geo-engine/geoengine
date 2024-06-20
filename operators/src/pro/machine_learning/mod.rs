use crate::util::Result;
use geoengine_datatypes::pro::MlModelId;

pub mod onnx;
pub mod xg_error;
pub mod xgboost;

#[async_trait::async_trait]
pub trait LoadMlModel: Send + Sync {
    // TODO: return a proper model type
    async fn load_ml_model_by_id(&self, model_id: MlModelId) -> Result<String>;
}

pub type MlModelAccess = Box<dyn LoadMlModel>;
