use std::ffi::OsStr;
use std::{
    fs::{self, DirEntry, File},
    io::BufReader,
    path::PathBuf,
};

use crate::error::Result;
use crate::layers::external::DataProviderDefinition;
use crate::layers::storage::LayerProviderDb;
use crate::util::user_input::UserInput;
use crate::{contexts::MockableSession, datasets::storage::DatasetDb};

use super::storage::DatasetDefinition;

use log::warn;

pub async fn add_datasets_from_directory<S: MockableSession, D: DatasetDb<S>>(
    dataset_db: &mut D,
    file_path: PathBuf,
) {
    async fn add_dataset_definition_from_dir_entry<S: MockableSession, D: DatasetDb<S>>(
        db: &mut D,
        entry: &DirEntry,
    ) -> Result<()> {
        let def: DatasetDefinition =
            serde_json::from_reader(BufReader::new(File::open(entry.path())?))?;

        db.add_dataset(
            &S::mock(), // TODO: find suitable way to add public dataset
            def.properties.clone().validated()?,
            db.wrap_meta_data(def.meta_data.clone()),
        )
        .await?; // TODO: add as system user

        Ok(())
    }

    let dir = fs::read_dir(file_path);
    if dir.is_err() {
        warn!("Skipped adding datasets from directory because it can't be read");
        return;
    }
    let dir = dir.expect("checked");

    for entry in dir {
        match entry {
            Ok(entry) if entry.path().extension() == Some(OsStr::new("json")) => {
                if let Err(e) = add_dataset_definition_from_dir_entry(dataset_db, &entry).await {
                    warn!(
                        "Skipped adding dataset from directory entry: {:?} error: {}",
                        entry,
                        e.to_string()
                    );
                }
            }
            _ => {
                warn!("Skipped adding dataset from directory entry: {:?}", entry);
            }
        }
    }
}

// TODO: move to layers source dir
pub async fn add_providers_from_directory<D: LayerProviderDb>(db: &mut D, file_path: PathBuf) {
    async fn add_provider_definition_from_dir_entry<D: LayerProviderDb>(
        db: &mut D,
        entry: &DirEntry,
    ) -> Result<()> {
        let def: Box<dyn DataProviderDefinition> =
            serde_json::from_reader(BufReader::new(File::open(entry.path())?))?;

        db.add_layer_provider(def).await?; // TODO: add as system user
        Ok(())
    }

    let dir = fs::read_dir(file_path);
    if dir.is_err() {
        warn!("Skipped adding providers from directory because it can't be read");
        return;
    }
    let dir = dir.expect("checked");

    for entry in dir {
        if let Ok(entry) = entry {
            if entry.path().is_dir() {
                continue;
            }
            if let Err(e) = add_provider_definition_from_dir_entry(db, &entry).await {
                // TODO: log
                warn!(
                    "Skipped adding provider from directory entry: {:?} error: {}",
                    entry,
                    e.to_string()
                );
            }
        } else {
            // TODO: log
            warn!("Skipped adding provider from directory entry: {:?}", entry);
        }
    }
}
