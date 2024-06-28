use crate::datasets::storage::MetaDataDefinition;
use crate::datasets::upload::UploadId;
use crate::datasets::{AddDataset, DatasetIdAndName};
use crate::error::Result;
use async_trait::async_trait;
use geoengine_datatypes::dataset::DatasetId;
use geoengine_datatypes::primitives::DateTime;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use tokio_postgres::Transaction;
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct Expiration {
    pub deletion_timestamp: Option<DateTime>,
    pub delete_record: bool,
    pub delete_data: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ChangeDatasetExpiration {
    pub dataset_id: DatasetId,
    pub expiration_change: ExpirationChange,
}

impl ChangeDatasetExpiration {
    pub fn delete_meta(dataset_id: DatasetId) -> Self {
        ChangeDatasetExpiration {
            dataset_id,
            expiration_change: ExpirationChange::SetExpire(Expiration {
                deletion_timestamp: None,
                delete_record: true,
                delete_data: false,
            }),
        }
    }

    pub fn delete_fair(dataset_id: DatasetId) -> Self {
        ChangeDatasetExpiration {
            dataset_id,
            expiration_change: ExpirationChange::SetExpire(Expiration {
                deletion_timestamp: None,
                delete_record: false,
                delete_data: true,
            }),
        }
    }

    pub fn delete_full(dataset_id: DatasetId) -> Self {
        ChangeDatasetExpiration {
            dataset_id,
            expiration_change: ExpirationChange::SetExpire(Expiration {
                deletion_timestamp: None,
                delete_record: true,
                delete_data: true,
            }),
        }
    }

    pub fn delete_none(dataset_id: DatasetId) -> Self {
        ChangeDatasetExpiration {
            dataset_id,
            expiration_change: ExpirationChange::SetExpire(Expiration {
                deletion_timestamp: None,
                delete_record: false,
                delete_data: false,
            }),
        }
    }

    pub fn expire_meta(dataset_id: DatasetId, timestamp: DateTime) -> Self {
        ChangeDatasetExpiration {
            dataset_id,
            expiration_change: ExpirationChange::SetExpire(Expiration {
                deletion_timestamp: Some(timestamp),
                delete_record: true,
                delete_data: false,
            }),
        }
    }

    pub fn expire_fair(dataset_id: DatasetId, timestamp: DateTime) -> Self {
        ChangeDatasetExpiration {
            dataset_id,
            expiration_change: ExpirationChange::SetExpire(Expiration {
                deletion_timestamp: Some(timestamp),
                delete_record: false,
                delete_data: true,
            }),
        }
    }

    pub fn expire_full(dataset_id: DatasetId, timestamp: DateTime) -> Self {
        ChangeDatasetExpiration {
            dataset_id,
            expiration_change: ExpirationChange::SetExpire(Expiration {
                deletion_timestamp: Some(timestamp),
                delete_record: true,
                delete_data: true,
            }),
        }
    }

    pub fn expire_none(dataset_id: DatasetId, timestamp: DateTime) -> Self {
        ChangeDatasetExpiration {
            dataset_id,
            expiration_change: ExpirationChange::SetExpire(Expiration {
                deletion_timestamp: Some(timestamp),
                delete_record: false,
                delete_data: false,
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

    async fn update_dataset_status(&self, dataset_id: &DatasetId) -> Result<()>;

    async fn update_dataset_status_in_tx(
        &self,
        tx: &Transaction,
        dataset_id: &DatasetId,
    ) -> Result<()>;

    async fn update_datasets_status(&self) -> Result<()>;

    async fn update_datasets_status_in_tx(&self, tx: &Transaction) -> Result<()>;

    async fn clear_expired_datasets(&self) -> Result<usize>;
}
