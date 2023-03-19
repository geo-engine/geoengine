mod hashmap_projectdb;
#[cfg(feature = "postgres")]
mod postgres_projectdb;
mod projectdb;

pub use hashmap_projectdb::ProHashMapProjectDbBackend;
pub use projectdb::{ProProjectDb, ProjectListOptions};

use crate::projects::ProjectVersionId;
use uuid::Uuid;

pub enum LoadVersion {
    Version(ProjectVersionId),
    Latest,
}

impl From<Option<Uuid>> for LoadVersion {
    fn from(id: Option<Uuid>) -> Self {
        id.map_or(LoadVersion::Latest, |id| {
            LoadVersion::Version(ProjectVersionId(id))
        })
    }
}
