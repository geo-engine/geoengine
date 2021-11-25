use std::{
    fs::{self, DirEntry, File},
    io::BufReader,
    path::PathBuf,
};

use crate::util::user_input::UserInput;
use crate::{contexts::MockableSession, datasets::storage::DatasetDb};
use crate::{datasets::storage::ExternalDatasetProviderDefinition, error::Result};

use super::storage::DatasetDefinition;

use log::warn;

pub async fn add_datasets_from_directory<S: MockableSession, D: DatasetDb<S>>(
    db: &mut D,
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
            def.properties.validated()?,
            db.wrap_meta_data(def.meta_data),
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
        if let Ok(entry) = entry {
            if let Err(e) = add_dataset_definition_from_dir_entry(db, &entry).await {
                warn!(
                    "Skipped adding dataset from directory entry: {:?} error: {}",
                    entry,
                    e.to_string()
                );
            }
        } else {
            warn!("Skipped adding dataset from directory entry: {:?}", entry);
        }
    }
}

pub async fn add_providers_from_directory<D: DatasetDb<S>, S: MockableSession>(
    db: &mut D,
    file_path: PathBuf,
) {
    async fn add_provider_definition_from_dir_entry<D: DatasetDb<S>, S: MockableSession>(
        db: &mut D,
        entry: &DirEntry,
    ) -> Result<()> {
        let def: Box<dyn ExternalDatasetProviderDefinition> =
            serde_json::from_reader(BufReader::new(File::open(entry.path())?))?;

        db.add_dataset_provider(&S::mock(), def).await?; // TODO: add as system user
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
