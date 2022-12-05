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
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

identifier!(UploadId);
identifier!(FileId);
identifier!(VolumeId);

pub trait AdjustFilePath {
    fn adjust_file_path(&self, file_path: &Path) -> Result<PathBuf>;
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
pub struct Volume {
    pub id: VolumeId,
    #[schema(value_type = String)]
    pub path: PathBuf,
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
pub trait UploadDb<S: Session> {
    async fn get_upload(&self, session: &S, upload: UploadId) -> Result<Upload>;

    async fn create_upload(&self, session: &S, upload: Upload) -> Result<()>;
}
