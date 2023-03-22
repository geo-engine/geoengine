use std::{
    collections::HashMap,
    ffi::OsStr,
    fs::{self, DirEntry, File},
    io::BufReader,
    path::PathBuf,
};

use crate::{error::Result, layers::listing::LayerCollectionId};
use crate::{layers::storage::LayerDb, util::user_input::UserInput};
use crate::{
    layers::{
        add_from_directory::UNSORTED_COLLECTION_ID,
        layer::{AddLayer, AddLayerCollection, LayerCollectionDefinition, LayerDefinition},
        listing::LayerCollectionProvider,
    },
    pro::permissions::{Permission, PermissionDb, Role},
};

use log::{debug, info, warn};

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
            }
            .validated()?,
            &LayerCollectionId(UNSORTED_COLLECTION_ID.to_string()),
        )
        .await?;

        // share with users
        db.add_permission(
            Role::registered_user_role_id(),
            def.id.clone(),
            Permission::Read,
        )
        .await?;
        db.add_permission(Role::anonymous_role_id(), def.id.clone(), Permission::Read)
            .await?;

        Ok(())
    }

    let dir = fs::read_dir(file_path);
    if dir.is_err() {
        warn!("Skipped adding layers from directory because it can't be read");
        return;
    }
    let dir = dir.expect("checked");

    for entry in dir {
        match entry {
            Ok(entry) if entry.path().extension() == Some(OsStr::new("json")) => {
                match add_layer_from_dir_entry(db, &entry).await {
                    Ok(_) => info!("Added layer from directory entry: {:?}", entry),
                    Err(e) => warn!(
                        "Skipped adding layer from directory entry: {:?} error: {}",
                        entry,
                        e.to_string()
                    ),
                }
            }
            _ => {
                warn!("Skipped adding layer from directory entry: {:?}", entry);
            }
        }
    }
}

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
            properties: Default::default(),
        }
        .validated()?;

        db.add_layer_collection_with_id(
            &def.id,
            collection,
            &LayerCollectionId(UNSORTED_COLLECTION_ID.to_string()),
        )
        .await?;

        // share with users
        debug!("sharing collection");
        db.add_permission(
            Role::registered_user_role_id(),
            def.id.clone(),
            Permission::Read,
        )
        .await?;
        db.add_permission(Role::anonymous_role_id(), def.id.clone(), Permission::Read)
            .await?;

        for layer in &def.layers {
            db.add_layer_to_collection(layer, &def.id).await?;
        }

        Ok(())
    }

    let dir = fs::read_dir(file_path);
    if dir.is_err() {
        warn!("Skipped adding layer collections from directory because it can't be read");
        return;
    }
    let dir = dir.expect("checked");

    let mut collection_defs = vec![];

    for entry in dir {
        match entry {
            Ok(entry) if entry.path().extension() == Some(OsStr::new("json")) => {
                match get_layer_collection_from_dir_entry(&entry) {
                    Ok(def) => collection_defs.push(def),
                    Err(e) => {
                        warn!(
                            "Skipped adding layer collection from directory entry: {:?} error: {}",
                            entry,
                            e.to_string()
                        );
                    }
                }
            }
            _ => {
                warn!(
                    "Skipped adding layer collection from directory entry: {:?}",
                    entry
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
            Ok(_) => {
                collection_children.insert(def.id, def.collections);
            }
            Err(e) => {
                warn!("Skipped adding layer collection to db: {}", e);
            }
        }
    }

    for (parent, children) in collection_children {
        for child in children {
            let op = db.add_collection_to_parent(&child, &parent).await;

            if let Err(e) = op {
                warn!("Skipped adding child collection to db: {}", e);
            }
        }
    }
}
