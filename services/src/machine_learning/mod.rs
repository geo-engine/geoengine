use crate::{
    config::{MachineLearning, get_config_element},
    datasets::upload::{UploadId, UploadRootPath},
    identifier,
    util::path_with_base_path,
};
use async_trait::async_trait;
use error::{MachineLearningError, error::CouldNotFindMlModelFileMachineLearningError};
use geoengine_operators::machine_learning::{MlModelLoadingInfo, MlModelMetadata};
use name::MlModelName;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::{borrow::Cow, path::PathBuf};
use utoipa::IntoParams;
use validator::{Validate, ValidationError};

pub mod error;
pub mod name;
mod postgres;

identifier!(MlModelId);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub struct MlModelIdAndName {
    pub id: MlModelId,
    pub name: MlModelName,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub struct MlModel {
    pub name: MlModelName,
    pub display_name: String,
    pub description: String,
    pub upload: UploadId,
    pub metadata: MlModelMetadata,
    pub file_name: String,
}

impl MlModel {
    pub fn model_path(&self) -> Result<PathBuf, MachineLearningError> {
        path_with_base_path(
            &self
                .upload
                .root_path()
                .map_err(Box::new)
                .context(CouldNotFindMlModelFileMachineLearningError)?,
            self.file_name.as_ref(),
        )
        .map_err(Box::new)
        .context(CouldNotFindMlModelFileMachineLearningError)
    }

    pub fn loading_info(&self) -> Result<MlModelLoadingInfo, MachineLearningError> {
        Ok(MlModelLoadingInfo {
            storage_path: self.model_path()?,
            metadata: self.metadata.clone(),
        })
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, IntoParams, Validate)]
#[into_params(parameter_in = Query)]
pub struct MlModelListOptions {
    #[param(example = 0)]
    pub offset: u32,
    #[param(example = 2)]
    #[validate(custom(function = "validate_list_limit"))]
    pub limit: u32,
}

fn validate_list_limit(value: u32) -> Result<(), ValidationError> {
    let limit = get_config_element::<MachineLearning>()
        .expect("should exist because it is defined in the default config")
        .list_limit;
    if value <= limit {
        return Ok(());
    }

    let mut err = ValidationError::new("limit (too large)");
    err.add_param::<u32>(Cow::Borrowed("max limit"), &limit);
    Err(err)
}

#[async_trait]
pub trait MlModelDb {
    async fn list_models(
        &self,
        options: &MlModelListOptions,
    ) -> Result<Vec<MlModel>, MachineLearningError>;

    async fn load_model(&self, name: &MlModelName) -> Result<MlModel, MachineLearningError>;

    async fn add_model(&self, model: MlModel) -> Result<MlModelIdAndName, MachineLearningError>;

    async fn resolve_model_name_to_id(
        &self,
        name: &MlModelName,
    ) -> Result<Option<MlModelId>, MachineLearningError>;
}
