use std::borrow::Cow;

use async_trait::async_trait;
use error::{error::CouldNotFindMlModelFileMachineLearningError, MachineLearningError};
use name::MlModelName;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Socket,
};
use utoipa::{IntoParams, ToSchema};
use validator::{Validate, ValidationError};

use crate::{
    api::model::datatypes::RasterDataType,
    contexts::PostgresDb,
    datasets::upload::{UploadId, UploadRootPath},
    identifier,
    util::{
        config::{get_config_element, MachineLearning},
        path_with_base_path,
    },
};

pub mod error;
pub mod name;

identifier!(MlModelId);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub struct MlModel {
    pub name: MlModelName,
    pub display_name: String,
    pub description: String,
    pub upload: UploadId,
    pub metadata: MlModelMetadata,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub struct MlModelMetadata {
    pub file_name: String,
    pub input_type: RasterDataType,
    pub num_input_bands: u32, // number of features per sample (bands per pixel)
    pub output_type: RasterDataType, // TODO: support multiple outputs, e.g. one band for the probability of prediction
                                     // TODO: output measurement, e.g. classification or regression, label names for classification. This would have to be provided by the model creator along the model file as it cannot be extracted from the model file(?)
}

impl MlModel {
    pub fn metadata_for_operator(
        &self,
    ) -> Result<geoengine_datatypes::machine_learning::MlModelMetadata, MachineLearningError> {
        Ok(geoengine_datatypes::machine_learning::MlModelMetadata {
            file_path: path_with_base_path(
                &self
                    .upload
                    .root_path()
                    .context(CouldNotFindMlModelFileMachineLearningError)?,
                self.metadata.file_name.as_ref(),
            )
            .context(CouldNotFindMlModelFileMachineLearningError)?,
            input_type: self.metadata.input_type.into(),
            num_input_bands: self.metadata.num_input_bands,
            output_type: self.metadata.output_type.into(),
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

    async fn load_model_metadata(
        &self,
        name: &MlModelName,
    ) -> Result<MlModelMetadata, MachineLearningError>;

    async fn add_model(&self, model: MlModel) -> Result<(), MachineLearningError>;

    async fn resolve_model_name_to_id(
        &self,
        name: &MlModelName,
    ) -> Result<Option<MlModelId>, MachineLearningError>;
}

#[async_trait]
impl<Tls> MlModelDb for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn list_models(
        &self,
        _options: &MlModelListOptions,
    ) -> Result<Vec<MlModel>, MachineLearningError> {
        unimplemented!()
    }

    async fn load_model(&self, _name: &MlModelName) -> Result<MlModel, MachineLearningError> {
        unimplemented!()
    }

    async fn load_model_metadata(
        &self,
        _name: &MlModelName,
    ) -> Result<MlModelMetadata, MachineLearningError> {
        unimplemented!()
    }

    async fn add_model(&self, _model: MlModel) -> Result<(), MachineLearningError> {
        unimplemented!()
    }

    async fn resolve_model_name_to_id(
        &self,
        _name: &MlModelName,
    ) -> Result<Option<MlModelId>, MachineLearningError> {
        unimplemented!()
    }
}
