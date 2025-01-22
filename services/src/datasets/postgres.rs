use super::DatasetName;
use crate::datasets::storage::MetaDataDefinition;
use crate::datasets::upload::FileId;
use crate::error::Result;
use async_trait::async_trait;
use bb8_postgres::bb8::PooledConnection;
use bb8_postgres::tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use bb8_postgres::tokio_postgres::Socket;
use bb8_postgres::PostgresConnectionManager;
use geoengine_datatypes::dataset::DatasetId;
use geoengine_operators::engine::TypedResultDescriptor;
use postgres_types::{FromSql, ToSql};

pub async fn resolve_dataset_name_to_id<Tls>(
    conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
    dataset_name: &DatasetName,
) -> Result<Option<DatasetId>>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let stmt = conn
        .prepare(
            "SELECT id
        FROM datasets
        WHERE name = $1::\"DatasetName\"",
        )
        .await?;

    let row_option = conn.query_opt(&stmt, &[&dataset_name]).await?;

    Ok(row_option.map(|row| row.get(0)))
}

#[async_trait]
pub trait PostgresStorable<Tls>: Send + Sync
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn to_typed_metadata(&self) -> Result<DatasetMetaData>;
}

pub struct DatasetMetaData<'m> {
    pub meta_data: &'m MetaDataDefinition,
    pub result_descriptor: TypedResultDescriptor,
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
