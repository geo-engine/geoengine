use std::str::FromStr;

use crate::datasets::listing::ProvenanceOutput;
use crate::datasets::listing::SessionMetaDataProvider;
use crate::datasets::storage::DATASET_DB_LAYER_PROVIDER_ID;
use crate::datasets::storage::DATASET_DB_ROOT_COLLECTION_ID;
use crate::datasets::storage::{
    AddDataset, Dataset, DatasetDb, DatasetStore, DatasetStorer, MetaDataDefinition,
};
use crate::datasets::upload::FileId;
use crate::datasets::upload::{Upload, UploadDb, UploadId};
use crate::error::{self, Error, Result};
use crate::layers::layer::CollectionItem;
use crate::layers::layer::Layer;
use crate::layers::layer::LayerCollectionListOptions;
use crate::layers::layer::LayerListing;
use crate::layers::layer::ProviderLayerId;
use crate::layers::listing::LayerCollectionId;
use crate::layers::listing::LayerCollectionProvider;
use crate::layers::listing::LayerId;
use crate::pro::datasets::storage::UpdateDatasetPermissions;
use crate::pro::datasets::RoleId;
use crate::projects::Symbology;
use crate::util::user_input::Validated;
use crate::workflows::workflow::Workflow;
use crate::{
    datasets::listing::{DatasetListOptions, DatasetListing, DatasetProvider},
    pro::users::UserSession,
};
use async_trait::async_trait;
use bb8_postgres::bb8::Pool;
use bb8_postgres::tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use bb8_postgres::tokio_postgres::Socket;
use bb8_postgres::PostgresConnectionManager;
use geoengine_datatypes::dataset::{DatasetId, InternalDatasetId};
use geoengine_datatypes::primitives::RasterQueryRectangle;
use geoengine_datatypes::primitives::VectorQueryRectangle;
use geoengine_datatypes::util::Identifier;
use geoengine_operators::engine::RasterOperator;
use geoengine_operators::engine::TypedOperator;
use geoengine_operators::engine::VectorOperator;
use geoengine_operators::engine::{
    MetaData, RasterResultDescriptor, StaticMetaData, TypedResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSource;
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::mock::MockDatasetDataSourceParams;
use geoengine_operators::source::GdalSource;
use geoengine_operators::source::GdalSourceParameters;
use geoengine_operators::source::OgrSource;
use geoengine_operators::source::OgrSourceParameters;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};
use log::info;
use postgres_types::{FromSql, ToSql};
use snafu::{ensure, ResultExt};
use uuid::Uuid;

use super::{DatasetPermission, Permission};

pub struct PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    conn_pool: Pool<PostgresConnectionManager<Tls>>,
}

impl<Tls> PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new(conn_pool: Pool<PostgresConnectionManager<Tls>>) -> Self {
        Self { conn_pool }
    }
}

impl<Tls> DatasetDb<UserSession> for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
}

#[async_trait]
impl<Tls> DatasetProvider<UserSession> for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn list(
        &self,
        session: &UserSession,
        _options: Validated<DatasetListOptions>,
    ) -> Result<Vec<DatasetListing>> {
        // TODO: use options

        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "
            SELECT 
                d.id, 
                d.name, 
                d.description,
                d.tags,
                d.source_operator,
                d.result_descriptor,
                d.symbology
            FROM 
                user_permitted_datasets p JOIN datasets d 
                    ON (p.dataset_id = d.id)
            WHERE 
                p.user_id = $1",
            )
            .await?;

        let rows = conn.query(&stmt, &[&session.user.id]).await?;

        Ok(rows
            .iter()
            .map(|row| {
                Result::<DatasetListing>::Ok(DatasetListing {
                    id: DatasetId::Internal {
                        dataset_id: row.get(0),
                    },
                    name: row.get(1),
                    description: row.get(2),
                    tags: row.get::<_, Option<_>>(3).unwrap_or_default(),
                    source_operator: row.get(4),
                    result_descriptor: serde_json::from_value(row.get(5))?,
                    symbology: serde_json::from_value(row.get(6))?,
                })
            })
            .filter_map(Result::ok)
            .collect())
    }

    async fn load(&self, session: &UserSession, dataset: &DatasetId) -> Result<Dataset> {
        let id = dataset.internal().ok_or(Error::InvalidDatasetId)?;

        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "
            SELECT
                d.id,
                d.name,
                d.description,
                d.result_descriptor,
                d.source_operator,
                d.symbology,
                d.provenance
            FROM 
                user_permitted_datasets p JOIN datasets d 
                    ON (p.dataset_id = d.id)
            WHERE 
                p.user_id = $1 AND d.id = $2
            LIMIT 
                1",
            )
            .await?;

        // TODO: throw proper dataset does not exist/no permission error
        let row = conn.query_one(&stmt, &[&session.user.id, &id]).await?;

        Ok(Dataset {
            id: DatasetId::Internal {
                dataset_id: row.get(0),
            },
            name: row.get(1),
            description: row.get(2),
            result_descriptor: serde_json::from_value(row.get(3))?,
            source_operator: row.get(4),
            symbology: serde_json::from_value(row.get(5))?,
            provenance: serde_json::from_value(row.get(6))?,
        })
    }

    async fn provenance(
        &self,
        session: &UserSession,
        dataset: &DatasetId,
    ) -> Result<ProvenanceOutput> {
        let id = dataset.internal().ok_or(Error::InvalidDatasetId)?;

        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
            SELECT 
                d.provenance 
            FROM 
                user_permitted_datasets p JOIN datasets d
                    ON(p.dataset_id = d.id)
            WHERE 
                p.user_id = $1 AND d.id = $2",
            )
            .await?;

        let row = conn.query_one(&stmt, &[&session.user.id, &id]).await?;

        Ok(ProvenanceOutput {
            dataset: dataset.clone(),
            provenance: serde_json::from_value(row.get(0)).context(error::SerdeJson)?,
        })
    }
}

#[async_trait]
impl<Tls>
    SessionMetaDataProvider<
        UserSession,
        MockDatasetDataSourceLoadingInfo,
        VectorResultDescriptor,
        VectorQueryRectangle,
    > for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn session_meta_data(
        &self,
        _session: &UserSession,
        _dataset: &DatasetId,
    ) -> Result<
        Box<
            dyn MetaData<
                MockDatasetDataSourceLoadingInfo,
                VectorResultDescriptor,
                VectorQueryRectangle,
            >,
        >,
    > {
        Err(Error::NotYetImplemented)
    }
}

#[async_trait]
impl<Tls>
    SessionMetaDataProvider<
        UserSession,
        OgrSourceDataset,
        VectorResultDescriptor,
        VectorQueryRectangle,
    > for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn session_meta_data(
        &self,
        session: &UserSession,
        dataset: &DatasetId,
    ) -> Result<Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>>
    {
        let id = dataset.internal().ok_or(Error::InvalidDatasetId)?;

        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "
        SELECT 
            d.meta_data
        FROM 
            user_permitted_datasets p JOIN datasets d 
                ON (p.dataset_id = d.id)
        WHERE 
            d.id = $1 AND p.user_id = $2",
            )
            .await?;

        let row = conn.query_one(&stmt, &[&id, &session.user.id]).await?;

        let meta_data: StaticMetaData<
            OgrSourceDataset,
            VectorResultDescriptor,
            VectorQueryRectangle,
        > = serde_json::from_value(row.get(0))?;

        Ok(Box::new(meta_data))
    }
}

#[async_trait]
impl<Tls>
    SessionMetaDataProvider<
        UserSession,
        GdalLoadingInfo,
        RasterResultDescriptor,
        RasterQueryRectangle,
    > for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn session_meta_data(
        &self,
        session: &UserSession,
        dataset: &DatasetId,
    ) -> Result<Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>>
    {
        let id = dataset.internal().ok_or(Error::InvalidDatasetId)?;

        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "
        SELECT 
            d.meta_data
        FROM 
            user_permitted_datasets p JOIN datasets d 
                ON (p.dataset_id = d.id)
        WHERE 
            d.id = $1 AND p.user_id = $2",
            )
            .await?;

        let row = conn.query_one(&stmt, &[&id, &session.user.id]).await?;

        let meta_data: MetaDataDefinition = serde_json::from_value(row.get(0))?;

        Ok(match meta_data {
            MetaDataDefinition::GdalMetaDataRegular(m) => Box::new(m),
            MetaDataDefinition::GdalStatic(m) => Box::new(m),
            _ => return Err(Error::DatasetIdTypeMissMatch),
        })
    }
}

#[async_trait]
pub trait PostgresStorable<Tls>: Send + Sync
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn to_json(&self) -> Result<DatasetMetaDataJson>;
}

pub struct DatasetMetaDataJson {
    meta_data: serde_json::Value,
    result_descriptor: serde_json::Value,
}

impl<Tls> DatasetStorer for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type StorageType = Box<dyn PostgresStorable<Tls>>;
}

impl<Tls> PostgresStorable<Tls> for MetaDataDefinition
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn to_json(&self) -> Result<DatasetMetaDataJson> {
        match self {
            MetaDataDefinition::MockMetaData(d) => Ok(DatasetMetaDataJson {
                meta_data: serde_json::to_value(self)?,
                result_descriptor: serde_json::to_value(&TypedResultDescriptor::from(
                    d.result_descriptor.clone(),
                ))?,
            }),
            MetaDataDefinition::OgrMetaData(d) => Ok(DatasetMetaDataJson {
                meta_data: serde_json::to_value(self)?,
                result_descriptor: serde_json::to_value(&TypedResultDescriptor::from(
                    d.result_descriptor.clone(),
                ))?,
            }),
            MetaDataDefinition::GdalMetaDataRegular(d) => Ok(DatasetMetaDataJson {
                meta_data: serde_json::to_value(self)?,
                result_descriptor: serde_json::to_value(&TypedResultDescriptor::from(
                    d.result_descriptor.clone(),
                ))?,
            }),
            MetaDataDefinition::GdalStatic(d) => Ok(DatasetMetaDataJson {
                meta_data: serde_json::to_value(self)?,
                result_descriptor: serde_json::to_value(&TypedResultDescriptor::from(
                    d.result_descriptor.clone(),
                ))?,
            }),
            MetaDataDefinition::GdalMetadataNetCdfCf(d) => Ok(DatasetMetaDataJson {
                meta_data: serde_json::to_value(self)?,
                result_descriptor: serde_json::to_value(&TypedResultDescriptor::from(
                    d.result_descriptor.clone(),
                ))?,
            }),
            MetaDataDefinition::GdalMetaDataList(d) => Ok(DatasetMetaDataJson {
                meta_data: serde_json::to_value(self)?,
                result_descriptor: serde_json::to_value(&TypedResultDescriptor::from(
                    d.result_descriptor.clone(),
                ))?,
            }),
        }
    }
}

#[async_trait]
impl<Tls> DatasetStore<UserSession> for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn add_dataset(
        &self,
        session: &UserSession,
        dataset: Validated<AddDataset>,
        meta_data: Box<dyn PostgresStorable<Tls>>,
    ) -> Result<DatasetId> {
        let dataset = dataset.user_input;
        let id = dataset
            .id
            .unwrap_or_else(|| InternalDatasetId::new().into());
        let internal_id = id.internal().ok_or(Error::InvalidDatasetId)?;

        let meta_data_json = meta_data.to_json()?;

        let mut conn = self.conn_pool.get().await?;

        let tx = conn.build_transaction().start().await?;

        let stmt = tx
            .prepare(
                "
                INSERT INTO datasets (
                    id,
                    name,
                    description,
                    source_operator,
                    result_descriptor,
                    meta_data,
                    symbology,
                    provenance
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            )
            .await?;

        tx.execute(
            &stmt,
            &[
                &internal_id,
                &dataset.name,
                &dataset.description,
                &dataset.source_operator,
                &meta_data_json.result_descriptor,
                &meta_data_json.meta_data,
                &serde_json::to_value(&dataset.symbology)?,
                &serde_json::to_value(&dataset.provenance)?,
            ],
        )
        .await?;

        let stmt = tx
            .prepare(
                "
            INSERT INTO dataset_permissions (
                role_id,
                dataset_id,
                permission
            )
            VALUES ($1, $2, $3)",
            )
            .await?;

        tx.execute(
            &stmt,
            &[
                &RoleId::from(session.user.id),
                &internal_id,
                &Permission::Owner,
            ],
        )
        .await?;

        tx.commit().await?;

        Ok(id)
    }

    fn wrap_meta_data(&self, meta: MetaDataDefinition) -> Self::StorageType {
        Box::new(meta)
    }
}

#[async_trait]
impl<Tls> UpdateDatasetPermissions for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn add_dataset_permission(
        &self,
        session: &UserSession,
        permission: DatasetPermission,
    ) -> Result<()> {
        info!(
            "Add dataset permission session: {:?} permission: {:?}",
            session, permission
        );

        let internal_id = permission.dataset.internal().ok_or(
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(error::Error::DatasetIdTypeMissMatch),
            },
        )?;

        let mut conn = self.conn_pool.get().await?;

        let tx = conn.build_transaction().start().await?;

        let stmt = tx
            .prepare(
                "
            SELECT
                user_id 
            FROM 
                user_permitted_datasets 
            WHERE
                user_id = $1 AND dataset_id = $2 AND permission = $3",
            )
            .await?;

        let auth = tx
            .query_one(
                &stmt,
                &[
                    &RoleId::from(session.user.id),
                    &internal_id,
                    &Permission::Owner,
                ],
            )
            .await;

        ensure!(
            auth.is_ok(),
            error::UpateDatasetPermission {
                role: session.user.id.to_string(),
                dataset: permission.dataset,
                permission: format!("{:?}", permission.permission),
            }
        );

        let stmt = tx
            .prepare(
                "
            SELECT 
                COUNT(role_id) 
            FROM 
                dataset_permissions 
            WHERE 
                role_id = $1 AND dataset_id = $2 and permission = $3",
            )
            .await?;

        let duplicate = tx
            .query_one(
                &stmt,
                &[&permission.role, &internal_id, &permission.permission],
            )
            .await?;

        ensure!(
            duplicate.get::<usize, i64>(0) == 0,
            error::DuplicateDatasetPermission {
                role: session.user.id.to_string(),
                dataset: permission.dataset,
                permission: format!("{:?}", permission.permission),
            }
        );

        let stmt = tx
            .prepare(
                "
            INSERT INTO dataset_permissions (
                role_id,
                dataset_id,
                permission
            )
            VALUES ($1, $2, $3)",
            )
            .await?;

        tx.execute(
            &stmt,
            &[&permission.role, &internal_id, &permission.permission],
        )
        .await?;

        tx.commit().await?;

        Ok(())
    }
}

#[async_trait]
impl<Tls> UploadDb<UserSession> for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn get_upload(&self, session: &UserSession, upload: UploadId) -> Result<Upload> {
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare("SELECT id, files FROM uploads WHERE id = $1 AND user_id = $2")
            .await?;

        let row = conn.query_one(&stmt, &[&upload, &session.user.id]).await?;

        Ok(Upload {
            id: row.get(0),
            files: row
                .get::<_, Vec<FileUpload>>(1)
                .into_iter()
                .map(Into::into)
                .collect(),
        })
    }

    async fn create_upload(&self, session: &UserSession, upload: Upload) -> Result<()> {
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare("INSERT INTO uploads (id, user_id, files) VALUES ($1, $2, $3)")
            .await?;

        conn.execute(
            &stmt,
            &[
                &upload.id,
                &session.user.id,
                &upload
                    .files
                    .iter()
                    .map(FileUpload::from)
                    .collect::<Vec<_>>(),
            ],
        )
        .await?;
        Ok(())
    }
}

#[async_trait]
impl<Tls> LayerCollectionProvider for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn collection_items(
        &self,
        collection: &LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>> {
        ensure!(
            *collection == self.root_collection_id().await?,
            error::UnknownLayerCollectionId {
                id: collection.clone()
            }
        );

        let conn = self.conn_pool.get().await?;

        let options = options.user_input;

        // TODO: only list datasets that are accessible to the user as layer
        // for now they are listed, but cannot be accessed
        let stmt = conn
            .prepare(
                "
                SELECT 
                    concat(d.id, ''), 
                    d.name, 
                    d.description
                FROM 
                    datasets d
                ORDER BY d.name ASC
                LIMIT $1
                OFFSET $2;",
            )
            .await?;

        let rows = conn
            .query(
                &stmt,
                &[&i64::from(options.limit), &i64::from(options.offset)],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|row| {
                Result::<CollectionItem>::Ok(CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider: DATASET_DB_LAYER_PROVIDER_ID,
                        item: LayerId(row.get(0)),
                    },
                    name: row.get(1),
                    description: row.get(2),
                }))
            })
            .filter_map(Result::ok)
            .collect())
    }

    async fn root_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(LayerCollectionId(DATASET_DB_ROOT_COLLECTION_ID.to_string()))
    }

    async fn get_layer(&self, id: &LayerId) -> Result<Layer> {
        let dataset_id = DatasetId::Internal {
            dataset_id: InternalDatasetId::from_str(&id.0)?,
        };

        let conn = self.conn_pool.get().await?;

        // TODO: check permission to dataset
        // for now they dataset is returned, but cannot be accessed
        let stmt = conn
            .prepare(
                "
                SELECT 
                    d.name, 
                    d.description,
                    d.source_operator,
                    d.symbology
                FROM 
                    datasets d
                WHERE id = $1;",
            )
            .await?;

        let row = conn
            .query_one(
                &stmt,
                &[
                    &Uuid::from_str(&id.0).map_err(|_| error::Error::IdStringMustBeUuid {
                        found: id.0.clone(),
                    })?,
                ],
            )
            .await?;

        let name: String = row.get(0);
        let description: String = row.get(1);
        let source_operator: String = row.get(2);
        let symbology: Option<Symbology> = serde_json::from_value(row.get(3))?;

        let operator = match source_operator.as_str() {
            "OgrSource" => TypedOperator::Vector(
                OgrSource {
                    params: OgrSourceParameters {
                        dataset: dataset_id.clone(),
                        attribute_projection: None,
                        attribute_filters: None,
                    },
                }
                .boxed(),
            ),
            "GdalSource" => TypedOperator::Raster(
                GdalSource {
                    params: GdalSourceParameters {
                        dataset: dataset_id.clone(),
                    },
                }
                .boxed(),
            ),
            "MockDatasetDataSource" => TypedOperator::Vector(
                MockDatasetDataSource {
                    params: MockDatasetDataSourceParams {
                        dataset: dataset_id.clone(),
                    },
                }
                .boxed(),
            ),
            s => {
                return Err(crate::error::Error::UnknownOperator {
                    operator: s.to_owned(),
                })
            }
        };

        Ok(Layer {
            id: ProviderLayerId {
                provider: DATASET_DB_LAYER_PROVIDER_ID,
                item: id.clone(),
            },
            name,
            description,
            workflow: Workflow { operator },
            symbology,
        })
    }
}

#[derive(Debug, Clone, ToSql, FromSql)]
pub struct FileUpload {
    pub id: FileId,
    pub name: String,
    pub byte_size: i64,
}

impl From<crate::datasets::upload::FileUpload> for FileUpload {
    fn from(upload: crate::datasets::upload::FileUpload) -> Self {
        Self {
            id: upload.id,
            name: upload.name,
            byte_size: upload.byte_size as i64,
        }
    }
}

impl From<&crate::datasets::upload::FileUpload> for FileUpload {
    fn from(upload: &crate::datasets::upload::FileUpload) -> Self {
        Self {
            id: upload.id,
            name: upload.name.clone(),
            byte_size: upload.byte_size as i64,
        }
    }
}

impl From<FileUpload> for crate::datasets::upload::FileUpload {
    fn from(upload: FileUpload) -> Self {
        Self {
            id: upload.id,
            name: upload.name,
            byte_size: upload.byte_size as u64,
        }
    }
}
