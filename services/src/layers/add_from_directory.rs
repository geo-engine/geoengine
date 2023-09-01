use std::{
    collections::HashMap,
    ffi::OsStr,
    fs::{self, DirEntry, File},
    io::BufReader,
    path::PathBuf,
};

use crate::layers::storage::LayerDb;
use crate::layers::{
    layer::{AddLayer, AddLayerCollection, LayerCollectionDefinition, LayerDefinition},
    storage::INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
};
use crate::{error::Result, layers::listing::LayerCollectionId};

use log::{info, warn};
use uuid::Uuid;

pub const UNSORTED_COLLECTION_ID: Uuid = Uuid::from_u128(0xffb2_dd9e_f5ad_427c_b7f1_c9a0_c7a0_ae3f);

pub async fn add_layers_from_directory<L: LayerDb>(layer_db: &mut L, file_path: PathBuf) {
    async fn add_layer_from_dir_entry<L: LayerDb>(
        layer_db: &mut L,
        entry: &DirEntry,
    ) -> Result<()> {
        let def: LayerDefinition =
            serde_json::from_reader(BufReader::new(File::open(entry.path())?))?;

        layer_db
            .add_layer_with_id(
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

        Ok(())
    }

    let Ok(dir) = fs::read_dir(file_path) else {
        warn!("Skipped adding layers from directory because it can't be read");
        return;
    };

    for entry in dir {
        match entry {
            Ok(entry) if entry.path().extension() == Some(OsStr::new("json")) => {
                match add_layer_from_dir_entry(layer_db, &entry).await {
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

pub async fn add_layer_collections_from_directory<L: LayerDb>(db: &mut L, file_path: PathBuf) {
    fn get_layer_collection_from_dir_entry(entry: &DirEntry) -> Result<LayerCollectionDefinition> {
        Ok(serde_json::from_reader(BufReader::new(File::open(
            entry.path(),
        )?))?)
    }

    async fn add_collection_to_db<L: LayerDb>(
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

    let root_id = LayerCollectionId(INTERNAL_LAYER_DB_ROOT_COLLECTION_ID.to_string());
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
