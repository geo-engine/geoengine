use std::path::{Path, PathBuf};

use crate::contexts::Session;
use crate::error::Result;
use crate::storage::{ListOption, Listable, Storable};
use crate::util::user_input::UserInput;
use crate::{
    error,
    util::config::{self, get_config_element},
};
use async_trait::async_trait;
use geoengine_datatypes::identifier;
use serde::{Deserialize, Serialize};

identifier!(UploadId);
identifier!(FileId);

pub trait UploadRootPath {
    fn root_path(&self) -> Result<PathBuf>;
}

impl UploadRootPath for UploadId {
    fn root_path(&self) -> Result<PathBuf> {
        let root = get_config_element::<config::Upload>()?.path;
        Ok(root.join(self.to_string()))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Upload {
    pub id: UploadId,
    pub files: Vec<FileUpload>,
}

impl Upload {
    /// turns a user defined file path (pattern) into a path that points to the upload directory
    pub fn adjust_file_path(&self, file_path: &Path) -> Result<PathBuf> {
        let file_name = file_path.file_name().ok_or(error::Error::PathIsNotAFile)?;

        Ok(self.id.root_path()?.join(file_name))
    }
}

impl Storable for Upload {
    type Id = UploadId;
    type Item = Upload;
    type ItemListing = UploadListing;
    type ListOptions = UploadListOptions;
}

#[derive(Clone)]
pub struct UploadListOptions {
    pub offset: u32,
    pub limit: u32,
}

impl ListOption for UploadListOptions {
    type Item = Upload;

    fn offset(&self) -> u32 {
        self.offset
    }

    fn limit(&self) -> u32 {
        self.limit
    }

    fn compare_items(&self, a: &Self::Item, b: &Self::Item) -> std::cmp::Ordering {
        // TODO
        std::cmp::Ordering::Equal
    }

    fn retain(&self, item: &Self::Item) -> bool {
        // TODO
        true
    }
}

impl UserInput for UploadListOptions {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

impl Listable<Upload> for Upload {
    fn list(&self, id: &UploadId) -> UploadListing {
        UploadListing {
            id: *id,
            num_files: self.files.len(),
        }
    }
}

impl From<(UploadId, Upload)> for UploadListing {
    fn from(v: (UploadId, Upload)) -> Self {
        Self {
            id: v.0,
            num_files: v.1.files.len(),
        }
    }
}

impl UserInput for Upload {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
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

    async fn create_upload(&mut self, session: &S, upload: Upload) -> Result<()>;
}
