use std::{
    fs::{self, DirEntry, File},
    io::BufReader,
    path::PathBuf,
};

use crate::datasets::storage::DatasetDefinition;
use crate::{
    datasets::storage::DatasetDb,
    pro::permissions::{Permission, Role},
};
use crate::{error::Result, pro::permissions::PermissionDb};

use log::warn;

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

        let dataset_id = db
            .add_dataset(
                def.properties.clone(),
                db.wrap_meta_data(def.meta_data.clone()),
            )
            .await?
            .id;

        db.add_permission(
            Role::registered_user_role_id(),
            dataset_id,
            Permission::Read,
        )
        .await?;

        db.add_permission(Role::anonymous_role_id(), dataset_id, Permission::Read)
            .await?;

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
                    e.to_string()
                );
            }
        } else {
            warn!("Skipped adding dataset from directory entry: {:?}", entry);
        }
    }
}
