use std::{
    fs::{self, DirEntry, File},
    io::BufReader,
    path::PathBuf,
};

use geoengine_datatypes::util::Identifier;

use crate::datasets::storage::{DatasetDb, DatasetProviderDefinition};
use crate::error::Result;
use crate::users::user::UserId;
use crate::util::user_input::UserInput;

use super::storage::DatasetDefinition;

pub async fn add_datasets_from_directory<D: DatasetDb>(db: &mut D, file_path: PathBuf) {
    async fn add_dataset_definition_from_dir_entry<D: DatasetDb>(
        db: &mut D,
        entry: &DirEntry,
    ) -> Result<()> {
        let def: DatasetDefinition =
            serde_json::from_reader(BufReader::new(File::open(entry.path())?))?;

        db.add_dataset(
            UserId::new(),
            def.properties.validated()?,
            db.wrap_meta_data(def.meta_data),
        )
        .await?; // TODO: add as system user

        Ok(())
    }

    let dir = fs::read_dir(file_path);
    if dir.is_err() {
        // TODO: log
        eprintln!("Skipped adding datasets from directory because it can't be read");
    }
    let dir = dir.expect("checked");

    for entry in dir {
        if let Ok(entry) = entry {
            if let Err(e) = add_dataset_definition_from_dir_entry(db, &entry).await {
                // TODO: log
                eprintln!(
                    "Skipped adding dataset from directory entry: {:?} error: {}",
                    entry,
                    e.to_string()
                );
            }
        } else {
            // TODO: log
            eprintln!("Skipped adding dataset from directory entry: {:?}", entry);
        }
    }
}

pub async fn add_providers_from_directory<D: DatasetDb>(db: &mut D, file_path: PathBuf) {
    async fn add_provider_definition_from_dir_entry<D: DatasetDb>(
        db: &mut D,
        entry: &DirEntry,
    ) -> Result<()> {
        let def: Box<dyn DatasetProviderDefinition> =
            serde_json::from_reader(BufReader::new(File::open(entry.path())?))?;

        db.add_dataset_provider(UserId::new(), def).await?; // TODO: add as system user

        Ok(())
    }

    let dir = fs::read_dir(file_path);
    if dir.is_err() {
        // TODO: log
        eprintln!("Skipped adding providers from directory because it can't be read");
    }
    let dir = dir.expect("checked");

    for entry in dir {
        if let Ok(entry) = entry {
            if let Err(e) = add_provider_definition_from_dir_entry(db, &entry).await {
                // TODO: log
                eprintln!(
                    "Skipped adding provider from directory entry: {:?} error: {}",
                    entry,
                    e.to_string()
                );
            }
        } else {
            // TODO: log
            eprintln!("Skipped adding provider from directory entry: {:?}", entry);
        }
    }
}
