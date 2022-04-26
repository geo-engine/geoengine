use std::{
    fs::{self, DirEntry, File},
    io::BufReader,
    path::PathBuf,
};

use crate::{
    contexts::MockableSession,
    datasets::storage::{AddDatasetDefinition, DatasetDb},
};
use crate::{datasets::storage::ExternalDatasetProviderDefinition, error::Result};
use crate::{storage::Store, util::user_input::UserInput};

use super::storage::{Dataset, DatasetDefinition, MetaDataDefinition};

use geoengine_datatypes::dataset::InternalDatasetId;
use log::warn;

pub async fn add_datasets_from_directory<S: Store<Dataset> + Store<MetaDataDefinition>>(
    store: &mut S,
    file_path: PathBuf,
) {
    async fn add_dataset_definition_from_dir_entry<
        S: Store<Dataset> + Store<MetaDataDefinition>,
    >(
        store: &mut S,
        entry: &DirEntry,
    ) -> Result<()> {
        let def: AddDatasetDefinition =
            serde_json::from_reader(BufReader::new(File::open(entry.path())?))?;
        let dataset = def.dataset().await?;

        let dataset_store: &mut dyn Store<Dataset> = store;
        let id: InternalDatasetId = dataset_store.create(dataset.validated()?).await?;

        let metadata_store: &mut dyn Store<MetaDataDefinition> = store;
        metadata_store
            .create_with_id(&id, def.meta_data.validated()?)
            .await?;

        // TODO: delete dataset if metadata creation failed

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
            if let Err(e) = add_dataset_definition_from_dir_entry(store, &entry).await {
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

pub async fn add_providers_from_directory<S: Store<Box<dyn ExternalDatasetProviderDefinition>>>(
    store: &mut S,
    file_path: PathBuf,
) {
    async fn add_provider_definition_from_dir_entry<
        S: Store<Box<dyn ExternalDatasetProviderDefinition>>,
    >(
        store: &mut S,
        entry: &DirEntry,
    ) -> Result<()> {
        let def: Box<dyn ExternalDatasetProviderDefinition> =
            serde_json::from_reader(BufReader::new(File::open(entry.path())?))?;

        store.create(def.validated()?).await?;
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
            if let Err(e) = add_provider_definition_from_dir_entry(store, &entry).await {
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
