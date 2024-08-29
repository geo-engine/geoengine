use async_trait::async_trait;
use geoengine_datatypes::util::Identifier;
use snafu::ResultExt;
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Socket,
};

use crate::{
    machine_learning::{
        error::{
            error::{Bb8MachineLearningError, PostgresMachineLearningError},
            MachineLearningError,
        },
        name::MlModelName,
        MlModel, MlModelDb, MlModelId, MlModelListOptions, MlModelMetadata,
    },
    util::postgres::PostgresErrorExt,
};

use super::{contexts::ProPostgresDb, permissions::Permission};

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
        let conn = self
            .conn_pool
            .get()
            .await
            .context(Bb8MachineLearningError)?;

        let rows = conn
            .query(
                "
                SELECT
                    m.id,
                    m.name,
                    m.display_name,
                    m.description,
                    m.upload,
                    m.metadata
                FROM 
                    user_permitted_ml_models u JOIN ml_models m ON (u.ml_model_id = m.id)
                WHERE 
                    u.user_id = $1
                ORDER BY
                    name
                OFFSET
                    $2
                LIMIT 
                    $3",
                &[
                    &self.session.user.id,
                    &i64::from(options.offset),
                    &i64::from(options.limit),
                ],
            )
            .await
            .context(PostgresMachineLearningError)?;

        let models = rows
            .into_iter()
            .map(|row| MlModel {
                name: row.get(1),
                display_name: row.get(2),
                description: row.get(3),
                upload: row.get(4),
                metadata: row.get(5),
            })
            .collect();

        Ok(models)
    }

    async fn load_model(&self, name: &MlModelName) -> Result<MlModel, MachineLearningError> {
        let conn = self
            .conn_pool
            .get()
            .await
            .context(Bb8MachineLearningError)?;

        let Some(row) = conn
            .query_opt(
                "SELECT
                    m.id,
                    m.name,
                    m.display_name,
                    m.description,
                    m.upload,
                    m.metadata
                FROM 
                    user_permitted_ml_models u JOIN ml_models m ON (u.ml_model_id = m.id)
                WHERE 
                    u.user_id = $1 AND m.name = $2::\"MlModelName\"",
                &[&self.session.user.id, name],
            )
            .await
            .context(PostgresMachineLearningError)?
        else {
            return Err(MachineLearningError::ModelNotFound { name: name.clone() });
        };

        Ok(MlModel {
            name: row.get(1),
            display_name: row.get(2),
            description: row.get(3),
            upload: row.get(4),
            metadata: row.get(5),
        })
    }

    async fn load_model_metadata(
        &self,
        name: &MlModelName,
    ) -> Result<MlModelMetadata, MachineLearningError> {
        let conn = self
            .conn_pool
            .get()
            .await
            .context(Bb8MachineLearningError)?;

        let Some(row) = conn
            .query_opt(
                "SELECT
                    m.metadata
                FROM 
                    user_permitted_ml_models u JOIN ml_models m ON (u.ml_model_id = m.id)
                WHERE 
                    u.user_id = $1 AND m.name = $2",
                &[&self.session.user.id, name],
            )
            .await
            .context(PostgresMachineLearningError)?
        else {
            return Err(MachineLearningError::ModelNotFound { name: name.clone() });
        };

        Ok(row.get(1))
    }

    async fn add_model(&self, model: MlModel) -> Result<(), MachineLearningError> {
        self.check_ml_model_namespace(&model.name)?;

        let mut conn = self
            .conn_pool
            .get()
            .await
            .context(Bb8MachineLearningError)?;

        let tx = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresMachineLearningError)?;

        let id = MlModelId::new();

        tx.query_one(
            "INSERT INTO ml_models (
                    id,
                    name, 
                    display_name, 
                    description,
                    upload,
                    metadata
                ) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id;",
            &[
                &id,
                &model.name,
                &model.display_name,
                &model.description,
                &model.upload,
                &model.metadata,
            ],
        )
        .await
        .map_unique_violation("ml_models", "name", || {
            MachineLearningError::DuplicateMlModelName {
                name: model.name.clone(),
            }
        })?;

        let stmt = tx
            .prepare(
                "INSERT INTO permissions (role_id, permission, ml_model_id) VALUES ($1, $2, $3);",
            )
            .await
            .context(PostgresMachineLearningError)?;

        tx.execute(&stmt, &[&self.session.user.id, &Permission::Owner, &id])
            .await
            .context(PostgresMachineLearningError)?;

        tx.commit().await.context(PostgresMachineLearningError)?;

        Ok(())
    }
}
