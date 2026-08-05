use crate::{
    contexts::GeoEngineDb,
    datasets::storage::{DatasetDb, DatasetDefinition},
    error::Result,
    layers::{
        external::{DataProviderDefinition, TypedDataProviderDefinition},
        layer::{AddLayer, AddLayerCollection, LayerCollectionDefinition, LayerDefinition},
        listing::{LayerCollectionId, LayerCollectionProvider},
        storage::{LayerDb, LayerProviderDb},
    },
    permissions::{Permission, PermissionDb, Role},
};
use geoengine_datatypes::{dataset::DatasetId, error::BoxedResultExt, util::helpers::ge_report};
use std::{
    collections::HashMap,
    fs::{self, DirEntry, File},
    io::BufReader,
    path::PathBuf,
};
use tracing::{error, info, warn};
use uuid::Uuid;

pub const UNSORTED_COLLECTION_ID: Uuid = Uuid::from_u128(0xffb2_dd9e_f5ad_427c_b7f1_c9a0_c7a0_ae3f);

pub async fn add_layers_from_directory<L: LayerDb + PermissionDb>(db: &mut L, file_path: PathBuf) {
    async fn add_layer_from_dir_entry<L: LayerDb + PermissionDb>(
        db: &mut L,
        entry: &DirEntry,
    ) -> Result<()> {
        let def: LayerDefinition =
            serde_json::from_reader(BufReader::new(File::open(entry.path())?))?;

        db.add_layer_with_id(
            &def.id,
            AddLayer {
                name: def.name,
                description: def.description,
                workflow: def.workflow,
                symbology: def.symbology,
                metadata: def.metadata,
                properties: def.properties,
            },
            &LayerCollectionId(UNSORTED_COLLECTION_ID.to_string()),
        )
        .await?;

        // share with users
        db.add_permission(
            Role::registered_user_role_id(),
            def.id.clone(),
            Permission::Read,
        )
        .await
        .boxed_context(crate::error::PermissionDb)?;
        db.add_permission(Role::anonymous_role_id(), def.id.clone(), Permission::Read)
            .await
            .boxed_context(crate::error::PermissionDb)?;

        Ok(())
    }

    let Ok(dir) = fs::read_dir(file_path) else {
        warn!("Skipped adding layers from directory because it can't be read");
        return;
    };

    for entry in dir {
        let entry = match entry {
            Ok(entry) if is_json_file(&entry) => entry,
            _ => {
                warn!(
                    "Entry" = path_display_option(&entry),
                    "Error" = error_display_option(&entry),
                    "Skipped adding layer from directory entry"
                );
                continue;
            }
        };

        if let Err(e) = add_layer_from_dir_entry(db, &entry).await {
            warn!("Entry" = path_display(&entry), "Error" = %e, "Skipped adding layer from directory entry");
            continue;
        }

        info!("Entry" = %entry.path().display(), "Added layer from directory entry");
    }
}

///
/// # Panics
///
/// Panics if root collection cannot be resolved
///
pub async fn add_layer_collections_from_directory<
    L: LayerDb + LayerCollectionProvider + PermissionDb,
>(
    db: &mut L,
    file_path: PathBuf,
) {
    fn get_layer_collection_from_dir_entry(entry: &DirEntry) -> Result<LayerCollectionDefinition> {
        Ok(serde_json::from_reader(BufReader::new(File::open(
            entry.path(),
        )?))?)
    }

    async fn add_collection_to_db<L: LayerDb + PermissionDb>(
        db: &mut L,
        def: &LayerCollectionDefinition,
    ) -> Result<()> {
        let collection = AddLayerCollection {
            name: def.name.clone(),
            description: def.description.clone(),
            properties: def.properties.clone(),
        };

        db.add_layer_collection_with_id(
            &def.id,
            collection,
            &LayerCollectionId(UNSORTED_COLLECTION_ID.to_string()),
        )
        .await?;

        // share with users
        db.add_permission(
            Role::registered_user_role_id(),
            def.id.clone(),
            Permission::Read,
        )
        .await
        .boxed_context(crate::error::PermissionDb)?;
        db.add_permission(Role::anonymous_role_id(), def.id.clone(), Permission::Read)
            .await
            .boxed_context(crate::error::PermissionDb)?;

        for layer in &def.layers {
            db.add_layer_to_collection(layer, &def.id).await?;
        }

        Ok(())
    }

    let Ok(dir) = fs::read_dir(file_path) else {
        warn!("Skipped adding layer collections from directory because it can't be read");
        return;
    };

    let mut collection_defs = vec![];

    for entry in dir {
        let entry = match entry {
            Ok(entry) if is_json_file(&entry) => entry,
            _ => {
                warn!(
                    "Entry" = path_display_option(&entry),
                    "Error" = error_display_option(&entry),
                    "Skipped adding layer collection from directory entry"
                );
                continue;
            }
        };

        match get_layer_collection_from_dir_entry(&entry) {
            Ok(def) => collection_defs.push(def),
            Err(e) => {
                warn!(
                    "Entry" = path_display(&entry),
                    "Error" = %e,
                    "Skipped adding layer collection from directory entry"
                );
            }
        }
    }

    let root_id = db
        .get_root_layer_collection_id()
        .await
        .expect("root id must be resolved");
    let mut collection_children: HashMap<LayerCollectionId, Vec<LayerCollectionId>> =
        HashMap::new();

    for def in collection_defs {
        let ok = if def.id == root_id {
            Ok(())
        } else {
            add_collection_to_db(db, &def).await
        };

        match ok {
            Ok(()) => {
                collection_children.insert(def.id, def.collections);
            }
            Err(e) => {
                warn!("Skipped adding layer collection to db: {e}");
            }
        }
    }

    for (parent, children) in collection_children {
        for child in children {
            let op = db.add_collection_to_parent(&child, &parent).await;

            if let Err(e) = op {
                warn!("Skipped adding child collection to db: {e}");
            }
        }
    }
}

pub async fn add_providers_from_directory<D: LayerProviderDb + PermissionDb + GeoEngineDb>(
    db: &mut D,
    base_path: PathBuf,
) {
    async fn add_provider_definition_from_dir_entry<
        D: LayerProviderDb + PermissionDb + GeoEngineDb,
    >(
        db: &mut D,
        entry: &DirEntry,
    ) -> Result<()> {
        let def: TypedDataProviderDefinition =
            serde_json::from_reader(BufReader::new(File::open(entry.path())?))?;

        db.add_layer_provider(def.clone()).await?;

        let id = <TypedDataProviderDefinition as DataProviderDefinition<D>>::id(&def);

        // share with users
        db.add_permission(Role::registered_user_role_id(), id, Permission::Read)
            .await
            .boxed_context(crate::error::PermissionDb)?;
        db.add_permission(Role::anonymous_role_id(), id, Permission::Read)
            .await
            .boxed_context(crate::error::PermissionDb)?;

        Ok(())
    }

    let Ok(dir) = fs::read_dir(&base_path) else {
        error!(
            "Skipped adding providers from directory `{}` because it can't be read",
            base_path.display()
        );
        return;
    };

    for entry in dir {
        let entry = match entry {
            Ok(entry) if is_json_file(&entry) => entry,
            _ => {
                warn!(
                    "Entry" = entry.as_ref().ok().map(|e| e.path().display().to_string()),
                    "Error" = entry.as_ref().err().map(ToString::to_string),
                    "Skipped adding provider from directory entry"
                );
                continue;
            }
        };

        if let Err(e) = add_provider_definition_from_dir_entry(db, &entry).await {
            warn!(
                "File" = %entry.path().display(),
                "Error" = %e,
                "Skipped adding provider from file",
            );
        }

        info!("File" = %entry.path().display(), "Added provider from file");
    }
}

fn is_json_file(entry: &DirEntry) -> bool {
    entry.path().is_file() && entry.path().extension().is_some_and(|ext| ext == "json")
}

fn path_display_option(entry: &Result<DirEntry, std::io::Error>) -> Option<String> {
    entry.as_ref().ok().map(path_display)
}

fn path_display(entry: &DirEntry) -> String {
    entry.path().display().to_string()
}

fn error_display_option(entry: &Result<DirEntry, std::io::Error>) -> Option<String> {
    entry.as_ref().err().map(ToString::to_string)
}

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
            .add_dataset(def.properties.clone(), def.meta_data.clone(), None)
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
            warn!("Skipped adding dataset from directory entry: {entry:?}");
        }
    }
}
