use std::{
    fs::{self, DirEntry, File},
    io::BufReader,
    path::PathBuf,
};

use crate::error::Result;
use crate::{
    datasets::storage::DatasetDb,
    pro::datasets::{DatasetPermission, Permission, Role},
};
use crate::{
    datasets::storage::DatasetDefinition, pro::users::UserSession, util::user_input::UserInput,
};

use log::warn;

use super::storage::UpdateDatasetPermissions;

pub async fn add_datasets_from_directory<D: DatasetDb<UserSession> + UpdateDatasetPermissions>(
    dataset_db: &mut D,
    file_path: PathBuf,
) {
    async fn add_dataset_definition_from_dir_entry<
        D: DatasetDb<UserSession> + UpdateDatasetPermissions,
    >(
        dataset_db: &mut D,
        entry: &DirEntry,
        system_session: &UserSession,
    ) -> Result<()> {
        let def: DatasetDefinition =
            serde_json::from_reader(BufReader::new(File::open(entry.path())?))?;

        let dataset_id = dataset_db
            .add_dataset(
                system_session,
                def.properties.clone().validated()?,
                dataset_db.wrap_meta_data(def.meta_data.clone()),
            )
            .await?;

        dataset_db
            .add_dataset_permission(
                system_session,
                DatasetPermission {
                    role: Role::user_role_id(),
                    dataset: dataset_id.clone(),
                    permission: Permission::Read,
                },
            )
            .await?;

        dataset_db
            .add_dataset_permission(
                system_session,
                DatasetPermission {
                    role: Role::anonymous_role_id(),
                    dataset: dataset_id.clone(),
                    permission: Permission::Read,
                },
            )
            .await?;

        Ok(())
    }

    let system_session = UserSession::system_session();

    let dir = fs::read_dir(file_path);
    if dir.is_err() {
        warn!("Skipped adding datasets from directory because it can't be read");
        return;
    }
    let dir = dir.expect("checked");

    for entry in dir {
        if let Ok(entry) = entry {
            if let Err(e) =
                add_dataset_definition_from_dir_entry(dataset_db, &entry, &system_session).await
            {
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
