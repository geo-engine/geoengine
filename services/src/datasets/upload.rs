use actix_multipart::Multipart;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::identifier;
use crate::util::path_with_base_path;
use crate::{
    error,
    util::config::{self, get_config_element},
};
use async_trait::async_trait;
use futures_util::StreamExt;
use geoengine_datatypes::util::Identifier;
use serde::{Deserialize, Deserializer, Serialize};
use snafu::ResultExt;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use utoipa::ToSchema;

identifier!(UploadId);
identifier!(FileId);

pub trait AdjustFilePath {
    fn adjust_file_path(&self, file_path: &Path) -> Result<PathBuf>;
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Volume {
    pub name: VolumeName,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, ToSchema, Serialize, Hash)]
pub struct VolumeName(pub String);

impl Display for VolumeName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'de> Deserialize<'de> for VolumeName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;

        if s.chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            Ok(VolumeName(s))
        } else {
            Err(serde::de::Error::custom(
                "Volume name must only contain alphanumeric characters, underscores and dashes",
            ))
        }
    }
}

impl AdjustFilePath for Volume {
    fn adjust_file_path(&self, file_path: &Path) -> Result<PathBuf> {
        let _file_name = file_path.file_name().ok_or(error::Error::PathIsNotAFile)?;

        path_with_base_path(&self.path, file_path)
    }
}

#[derive(Debug, Clone)]
pub struct Volumes {
    pub volumes: Vec<Volume>,
}

impl Default for Volumes {
    fn default() -> Self {
        Self {
            volumes: crate::util::config::get_config_element::<crate::util::config::Data>()
                .expect("volumes should be defined, because they are in the default config")
                .volumes
                .into_iter()
                .map(|(name, path)| Volume { name, path })
                .collect::<Vec<_>>(),
        }
    }
}

pub trait UploadRootPath {
    fn root_path(&self) -> Result<PathBuf>;
}

impl UploadRootPath for UploadId {
    fn root_path(&self) -> Result<PathBuf> {
        let root = get_config_element::<config::Upload>()?.path;
        Ok(root.join(self.to_string()))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Upload {
    pub id: UploadId,
    pub files: Vec<FileUpload>,
}

impl AdjustFilePath for Upload {
    /// turns a user defined file path (pattern) into a path that points to the upload directory
    fn adjust_file_path(&self, file_path: &Path) -> Result<PathBuf> {
        let file_name = file_path.file_name().ok_or(error::Error::PathIsNotAFile)?;

        Ok(self.id.root_path()?.join(file_name))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct FileUpload {
    pub id: FileId,
    pub name: String,
    pub byte_size: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UploadListing {
    pub id: UploadId,
    pub num_files: usize,
}

pub async fn create_upload(mut body: Multipart) -> Result<(UploadId, Vec<FileUpload>)> {
    let upload_id = UploadId::new();

    let root = upload_id.root_path()?;

    fs::create_dir_all(&root).await.context(error::Io)?;

    let mut files: Vec<FileUpload> = vec![];
    while let Some(item) = body.next().await {
        let mut field = item?;
        let file_name = field
            .content_disposition()
            .get_filename()
            .ok_or(error::Error::UploadFieldMissingFileName)?
            .to_owned();

        let file_id = FileId::new();
        let mut file = fs::File::create(root.join(&file_name))
            .await
            .context(error::Io)?;

        let mut byte_size = 0_u64;
        while let Some(chunk) = field.next().await {
            let bytes = chunk?;
            file.write_all(&bytes).await.context(error::Io)?;
            byte_size += bytes.len() as u64;
        }
        file.flush().await.context(error::Io)?;

        files.push(FileUpload {
            id: file_id,
            name: file_name,
            byte_size,
        });
    }

    Ok((upload_id, files))
}

pub async fn delete_upload(upload_id: UploadId) -> Result<()> {
    let root = upload_id.root_path()?;
    log::debug!("Deleting {upload_id}");
    fs::remove_dir_all(&root).await.context(error::Io)?;
    Ok(())
}

#[async_trait]
pub trait UploadDb {
    async fn load_upload(&self, upload: UploadId) -> Result<Upload>;

    async fn create_upload(&self, upload: Upload) -> Result<()>;
}
