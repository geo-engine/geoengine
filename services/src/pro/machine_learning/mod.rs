use async_trait::async_trait;
use geoengine_datatypes::machine_learning::{MlModelMetadata, MlModelName};
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Socket,
};

use crate::{
    api::handlers::machine_learning::{MlModel, MlModelListOptions},
    machine_learning::{error::MachineLearningError, MlModelDb},
};

use super::contexts::ProPostgresDb;

#[async_trait]
impl<Tls> MlModelDb for ProPostgresDb<Tls>
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
