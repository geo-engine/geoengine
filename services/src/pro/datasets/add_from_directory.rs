use std::{
    fs::{self, DirEntry, File},
    io::BufReader,
    path::PathBuf,
};

use crate::{
    datasets::add_from_directory::{add_dataset_as_layer, add_dataset_layer_collection},
    error::Result,
    layers::storage::LayerDb,
    workflows::registry::WorkflowRegistry,
};
use crate::{
    datasets::storage::DatasetDb,
    pro::datasets::{DatasetPermission, Permission, Role},
};
use crate::{
    datasets::storage::DatasetDefinition, pro::users::UserSession, util::user_input::UserInput,
};

use log::warn;

use super::storage::UpdateDatasetPermissions;

pub async fn add_datasets_from_directory<
    D: DatasetDb<UserSession> + UpdateDatasetPermissions,
    L: LayerDb,
    W: WorkflowRegistry,
>(
    dataset_db: &mut D,
    layer_db: &mut L,
    workflow_db: &mut W,
    file_path: PathBuf,
) {
    async fn add_dataset_definition_from_dir_entry<
        D: DatasetDb<UserSession> + UpdateDatasetPermissions,
        L: LayerDb,
        W: WorkflowRegistry,
    >(
        dataset_db: &mut D,
        layer_db: &mut L,
        workflow_db: &mut W,
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

        add_dataset_as_layer(def, dataset_id, layer_db, workflow_db).await?;

        Ok(())
    }

    let system_session = UserSession::system_session();

    let dir = fs::read_dir(file_path);
    if dir.is_err() {
        warn!("Skipped adding datasets from directory because it can't be read");
        return;
    }
    let dir = dir.expect("checked");

    let dataset_layer_collection = add_dataset_layer_collection(layer_db).await;
    if let Err(e) = dataset_layer_collection {
        warn!("Skipped adding dataset layer collection: {:?}", e);
    }

    for entry in dir {
        if let Ok(entry) = entry {
            if let Err(e) = add_dataset_definition_from_dir_entry(
                dataset_db,
                layer_db,
                workflow_db,
                &entry,
                &system_session,
            )
            .await
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
