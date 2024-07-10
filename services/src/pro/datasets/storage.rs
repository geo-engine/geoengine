use async_trait::async_trait;
use bb8_postgres::bb8::{ManageConnection, PooledConnection};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use tokio_postgres::Transaction;
use utoipa::ToSchema;

use geoengine_datatypes::dataset::DatasetId;
use geoengine_datatypes::primitives::DateTime;

use crate::datasets::storage::MetaDataDefinition;
use crate::datasets::upload::UploadId;
use crate::datasets::{AddDataset, DatasetIdAndName};
use crate::error::Result;
use crate::pro::datasets::storage::DatasetDeletionType::{DeleteData, DeleteRecordAndData};

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Expiration {
    pub deletion_timestamp: Option<DateTime>,
    pub deletion_type: DatasetDeletionType,
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema, FromSql, ToSql)]
pub enum DatasetDeletionType {
    DeleteRecordAndData,
    DeleteData,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ChangeDatasetExpiration {
    pub dataset_id: DatasetId,
    pub expiration_change: ExpirationChange,
}

impl ChangeDatasetExpiration {
    pub fn delete_fair(dataset_id: DatasetId) -> Self {
        ChangeDatasetExpiration {
            dataset_id,
            expiration_change: ExpirationChange::SetExpire(Expiration {
                deletion_timestamp: None,
                deletion_type: DeleteData,
            }),
        }
    }

    pub fn delete_full(dataset_id: DatasetId) -> Self {
        ChangeDatasetExpiration {
            dataset_id,
            expiration_change: ExpirationChange::SetExpire(Expiration {
                deletion_timestamp: None,
                deletion_type: DeleteRecordAndData,
            }),
        }
    }

    pub fn expire_fair(dataset_id: DatasetId, timestamp: DateTime) -> Self {
        ChangeDatasetExpiration {
            dataset_id,
            expiration_change: ExpirationChange::SetExpire(Expiration {
                deletion_timestamp: Some(timestamp),
                deletion_type: DeleteData,
            }),
        }
    }

    pub fn expire_full(dataset_id: DatasetId, timestamp: DateTime) -> Self {
        ChangeDatasetExpiration {
            dataset_id,
            expiration_change: ExpirationChange::SetExpire(Expiration {
                deletion_timestamp: Some(timestamp),
                deletion_type: DeleteRecordAndData,
            }),
        }
    }

    pub fn unset_expire(dataset_id: DatasetId) -> Self {
        ChangeDatasetExpiration {
            dataset_id,
            expiration_change: ExpirationChange::UnsetExpire,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum ExpirationChange {
    SetExpire(Expiration),
    UnsetExpire,
}

pub enum UploadedDatasetStatus {
    Available,
    Deleted,
}

#[derive(Debug, FromSql, ToSql)]
pub enum InternalUploadedDatasetStatus {
    Available,
    Expires,
    Expired,
    UpdateExpired,
    Deleted,
    DeletedWithError,
}

impl From<InternalUploadedDatasetStatus> for UploadedDatasetStatus {
    fn from(value: InternalUploadedDatasetStatus) -> Self {
        match value {
            InternalUploadedDatasetStatus::Available | InternalUploadedDatasetStatus::Expires => {
                UploadedDatasetStatus::Available
            }
            InternalUploadedDatasetStatus::Expired
            | InternalUploadedDatasetStatus::UpdateExpired
            | InternalUploadedDatasetStatus::Deleted
            | InternalUploadedDatasetStatus::DeletedWithError => UploadedDatasetStatus::Deleted,
        }
    }
}

/// internal functionality for transactional control of user-uploaded datasets db
///
/// In contrast to the `UploadedUserDatasetStore` this is not to be used by services but only by the `ProPostgresDb` internally.
/// This is because services do not know about database transactions.
#[async_trait]
pub trait TxUploadedUserDatasetStore<M: ManageConnection> {
    async fn validate_expiration_request_in_tx(
        &self,
        tx: &Transaction,
        dataset_id: &DatasetId,
        expiration: &Expiration,
    ) -> Result<()>;

    async fn uploaded_dataset_status_in_tx(
        &self,
        dataset_id: &DatasetId,
        tx: &Transaction,
    ) -> Result<UploadedDatasetStatus>;

    /// Updates the status of datasets, because some datasets might have reached expiration
    //TODO: Add some sort of periodic update for the status of datasets
    async fn lazy_dataset_store_updates(
        &self,
        conn: &mut PooledConnection<M>,
        dataset_id: Option<&DatasetId>,
    ) -> Result<()>;

    async fn update_dataset_status_in_tx(
        &self,
        tx: &Transaction,
        dataset_id: &DatasetId,
    ) -> Result<()>;

    async fn update_datasets_status_in_tx(&self, tx: &Transaction) -> Result<()>;

    async fn admin_update_datasets_status_in_tx(&self, tx: &Transaction) -> Result<()>;
    async fn set_expire_for_uploaded_dataset(
        &self,
        tx: &Transaction,
        dataset_id: &DatasetId,
        expiration: &Expiration,
    ) -> Result<()>;
    async fn unset_expire_for_uploaded_dataset(
        &self,
        tx: &Transaction,
        dataset_id: &DatasetId,
    ) -> Result<()>;
}

/// Storage of user-uploaded datasets
#[async_trait]
pub trait UploadedUserDatasetStore {
    async fn add_uploaded_dataset(
        &self,
        upload_id: UploadId,
        dataset: AddDataset,
        meta_data: MetaDataDefinition,
    ) -> Result<DatasetIdAndName>;

    async fn expire_uploaded_dataset(&self, expire_dataset: ChangeDatasetExpiration) -> Result<()>;

    async fn clear_expired_datasets(&self) -> Result<u64>;
}
