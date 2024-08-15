use std::borrow::Cow;

use async_trait::async_trait;
use error::MachineLearningError;
use geoengine_datatypes::machine_learning::{MlModelMetadata, MlModelName};
use serde::{Deserialize, Serialize};
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Socket,
};
use utoipa::{IntoParams, ToSchema};
use validator::{Validate, ValidationError};

use crate::{
    api::handlers::machine_learning::MlModelListOptions,
    contexts::PostgresDb,
    util::config::{get_config_element, MachineLearning},
};

use super::api::handlers::machine_learning::MlModel;

pub mod error;

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
        options: &MlModelListOptions,
    ) -> Result<Vec<MlModel>, MachineLearningError> {
        todo!()
    }

    async fn load_model(&self, name: &MlModelName) -> Result<MlModel, MachineLearningError> {
        todo!()
    }

    async fn load_model_metadata(
        &self,
        name: &MlModelName,
    ) -> Result<MlModelMetadata, MachineLearningError> {
        todo!()
    }

    async fn add_model(&self, model: MlModel) -> Result<(), MachineLearningError> {
        todo!()
    }
}
