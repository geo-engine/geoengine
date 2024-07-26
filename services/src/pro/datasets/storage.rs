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
use crate::error::Error::IllegalDatasetStatus;
use crate::error::Result;
use crate::pro::datasets::storage::DatasetDeletionType::{DeleteData, DeleteRecordAndData};
use crate::pro::permissions::Permission;

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum UploadedDatasetStatus {
    Available,
    Expires(Expiration),
    Deleted(Expiration),
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

impl InternalUploadedDatasetStatus {
    pub fn convert_to_uploaded_dataset_status(
        &self,
        dataset_id: &DatasetId,
        expiration_timestamp: Option<DateTime>,
        dataset_deletion_type: Option<DatasetDeletionType>,
    ) -> Result<UploadedDatasetStatus> {
        if matches!(self, InternalUploadedDatasetStatus::Available)
            && expiration_timestamp.is_none()
            && expiration_timestamp.is_none()
        {
            return Ok(UploadedDatasetStatus::Available);
        } else if let Some(deletion_type) = dataset_deletion_type {
            let expiration = Expiration {
                deletion_timestamp: expiration_timestamp,
                deletion_type,
            };
            match self {
                InternalUploadedDatasetStatus::Available => {}
                InternalUploadedDatasetStatus::Expires => {
                    if expiration_timestamp.is_some() {
                        return Ok(UploadedDatasetStatus::Expires(expiration));
                    }
                }
                InternalUploadedDatasetStatus::Expired
                | InternalUploadedDatasetStatus::UpdateExpired
                | InternalUploadedDatasetStatus::Deleted
                | InternalUploadedDatasetStatus::DeletedWithError => {
                    return Ok(UploadedDatasetStatus::Deleted(expiration));
                }
            }
        }
        Err(IllegalDatasetStatus {
            dataset: (*dataset_id).into(),
            status: "InternalUploadedDatasetStatus is not a legal configuration".to_string(),
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DatasetType {
    UserUpload(UploadedDatasetStatus),
    NonUserUpload,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DatasetAccessStatus {
    pub id: DatasetId,
    pub dataset_type: DatasetType,
    pub permissions: Vec<Permission>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatasetAccessStatusResponse {
    pub id: DatasetId,
    pub is_user_upload: bool,
    pub is_available: bool,
    pub expiration: Option<Expiration>,
    pub permissions: Vec<Permission>,
}

impl From<DatasetAccessStatus> for DatasetAccessStatusResponse {
    fn from(value: DatasetAccessStatus) -> Self {
        let (is_user_upload, is_available, expiration) = match value.dataset_type {
            DatasetType::UserUpload(upload) => match upload {
                UploadedDatasetStatus::Available => (true, true, None),
                UploadedDatasetStatus::Expires(expiration) => (true, true, Some(expiration)),
                UploadedDatasetStatus::Deleted(expiration) => (true, false, Some(expiration)),
            },
            DatasetType::NonUserUpload => (false, true, None),
        };

        DatasetAccessStatusResponse {
            id: value.id,
            is_user_upload,
            is_available,
            expiration,
            permissions: value.permissions,
        }
    }
}

/// internal functionality for transactional control of user-uploaded datasets db
///
/// In contrast to the `UploadedUserDatasetStore` this is not to be used by services but only by the `ProPostgresDb` internally.
/// This is because services do not know about database transactions.
#[async_trait]
pub trait TxUploadedUserDatasetStore<M: ManageConnection> {
    async fn get_dataset_access_status_in_tx(
        &self,
        dataset_id: &DatasetId,
        tx: &Transaction,
    ) -> Result<DatasetAccessStatus>;

    async fn validate_expiration_request_in_tx(
        &self,
        dataset_id: &DatasetId,
        expiration: &Expiration,
        tx: &Transaction,
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

    async fn expire_uploaded_dataset_in_tx(
        &self,
        expire_dataset: ChangeDatasetExpiration,
        tx: &Transaction,
    ) -> Result<()>;

    async fn update_uploaded_datasets_status_in_tx(
        &self,
        dataset_id: Option<&DatasetId>,
        tx: &Transaction,
    ) -> Result<()>;

    async fn set_expire_for_uploaded_dataset(
        &self,
        dataset_id: &DatasetId,
        expiration: &Expiration,
        tx: &Transaction,
    ) -> Result<()>;

    async fn unset_expire_for_uploaded_dataset(
        &self,
        dataset_id: &DatasetId,
        tx: &Transaction,
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

    async fn get_dataset_access_status(
        &self,
        dataset_id: &DatasetId,
    ) -> Result<DatasetAccessStatus>;

    async fn clear_expired_datasets(&self) -> Result<u64>;
}
