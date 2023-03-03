use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};

use crate::contexts::Session;
use crate::error::Result;
use crate::identifier;
use crate::util::path_with_base_path;
use crate::{
    error,
    util::config::{self, get_config_element},
};
use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize};
use utoipa::ToSchema;

identifier!(UploadId);
identifier!(FileId);

pub trait AdjustFilePath {
    fn adjust_file_path(&self, file_path: &Path) -> Result<PathBuf>;
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
pub struct Volume {
    pub name: VolumeName,
    #[schema(value_type = String)]
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

#[async_trait]
pub trait UploadDb {
    async fn load_upload(&self, upload: UploadId) -> Result<Upload>;

    async fn create_upload(&self, upload: Upload) -> Result<()>;
}
