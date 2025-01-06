use crate::datasets::storage::{DatasetDb, DatasetDefinition};
use crate::error::Result;
use crate::layers::external::TypedDataProviderDefinition;
use crate::layers::storage::LayerProviderDb;
use crate::pro::permissions::{Permission, PermissionDb, Role};
use geoengine_datatypes::dataset::DatasetId;
use geoengine_datatypes::error::BoxedResultExt;
use geoengine_datatypes::util::helpers::ge_report;
use log::{error, info, warn};
use std::{
    fs::{self, DirEntry, File},
    io::BufReader,
    path::PathBuf,
};

pub async fn add_datasets_from_directory<D: DatasetDb + PermissionDb>(
    dataset_db: &mut D,
    file_path: PathBuf,
) {
    async fn add_dataset_definition_from_dir_entry<D: DatasetDb + PermissionDb>(
        db: &mut D,
        entry: &DirEntry,
    ) -> Result<()> {
        let def: DatasetDefinition =
            serde_json::from_reader(BufReader::new(File::open(entry.path())?))?;

        let dataset_id: DatasetId = db
            .add_dataset(def.properties.clone(), def.meta_data.clone())
            .await?
            .id;

        db.add_permission(
            Role::registered_user_role_id(),
            dataset_id,
            Permission::Read,
        )
        .await
        .boxed_context(crate::error::PermissionDb)?;

        db.add_permission(Role::anonymous_role_id(), dataset_id, Permission::Read)
            .await
            .boxed_context(crate::error::PermissionDb)?;

        Ok(())
    }

    let Ok(dir) = fs::read_dir(file_path) else {
        warn!("Skipped adding datasets from directory because it can't be read");
        return;
    };

    for entry in dir {
        if let Ok(entry) = entry {
            if let Err(e) = add_dataset_definition_from_dir_entry(dataset_db, &entry).await {
                warn!(
                    "Skipped adding dataset from directory entry: {:?} error: {}",
                    entry,
                    ge_report(e)
                );
            }
        } else {
            warn!("Skipped adding dataset from directory entry: {:?}", entry);
        }
    }
}

// TODO: move to layers source dir
pub async fn add_providers_from_directory<D: LayerProviderDb>(db: &mut D, base_path: PathBuf) {
    async fn add_provider_definition_from_dir_entry<D: LayerProviderDb>(
        db: &mut D,
        entry: &DirEntry,
    ) -> Result<()> {
        let def: TypedDataProviderDefinition =
            serde_json::from_reader(BufReader::new(File::open(entry.path())?))?;

        db.add_layer_provider(def).await?; // TODO: add as system user
        Ok(())
    }

    let Ok(dir) = fs::read_dir(&base_path) else {
        error!(
            "Skipped adding providers from directory `{:?}` because it can't be read",
            base_path
        );
        return;
    };

    for entry in dir {
        match entry {
            Ok(entry) if entry.path().is_file() => {
                match add_provider_definition_from_dir_entry(db, &entry).await {
                    Ok(()) => info!("Added provider from file `{:?}`", entry.path()),
                    Err(e) => {
                        warn!(
                            "Skipped adding provider from file `{:?}` error: `{:?}`",
                            entry.path(),
                            e
                        );
                    }
                }
            }
            Err(e) => {
                warn!("Skipped adding provider from directory entry `{:?}`", e);
            }
            _ => {}
        }
    }
}
