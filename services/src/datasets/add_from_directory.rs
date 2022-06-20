use std::ffi::OsStr;
use std::{
    fs::{self, DirEntry, File},
    io::BufReader,
    path::PathBuf,
};

use crate::datasets::storage::MetaDataDefinition;
use crate::error::Result;
use crate::layers::external::ExternalLayerProviderDefinition;
use crate::layers::layer::{AddLayer, AddLayerCollection};
use crate::layers::listing::LayerCollectionId;
use crate::layers::storage::{LayerDb, LayerProviderDb};
use crate::util::user_input::UserInput;
use crate::workflows::workflow::Workflow;
use crate::{contexts::MockableSession, datasets::storage::DatasetDb};

use super::storage::DatasetDefinition;

use geoengine_datatypes::dataset::DatasetId;
use geoengine_operators::engine::{RasterOperator, TypedOperator, VectorOperator};
use geoengine_operators::mock::{MockDatasetDataSource, MockDatasetDataSourceParams};
use geoengine_operators::source::{
    GdalSource, GdalSourceParameters, OgrSource, OgrSourceParameters,
};
use log::warn;

const DATASET_LAYER_COLLECTION_ID: &str = "82825554-6b41-41e8-91c7-e562162c2a08";

pub async fn add_dataset_layer_collection<L: LayerDb>(layer_db: &mut L) -> Result<()> {
    let collection = AddLayerCollection {
        name: "Datasets".to_string(),
        description: "Available datasets".to_string(),
    }
    .validated()?;

    layer_db
        .add_collection_with_id(
            &LayerCollectionId(DATASET_LAYER_COLLECTION_ID.to_string()),
            collection,
        )
        .await?;

    Ok(())
}

pub async fn add_dataset_as_layer<L: LayerDb>(
    def: DatasetDefinition,
    dataset: DatasetId,
    layer_db: &mut L,
) -> Result<()> {
    let workflow = match def.meta_data {
        MetaDataDefinition::MockMetaData(_) => Workflow {
            operator: TypedOperator::Vector(
                MockDatasetDataSource {
                    params: MockDatasetDataSourceParams { dataset },
                }
                .boxed(),
            ),
        },
        MetaDataDefinition::OgrMetaData(_) => Workflow {
            operator: TypedOperator::Vector(
                OgrSource {
                    params: OgrSourceParameters {
                        dataset,
                        attribute_projection: None,
                        attribute_filters: None,
                    },
                }
                .boxed(),
            ),
        },
        MetaDataDefinition::GdalMetaDataRegular(_)
        | MetaDataDefinition::GdalStatic(_)
        | MetaDataDefinition::GdalMetadataNetCdfCf(_)
        | MetaDataDefinition::GdalMetaDataList(_) => Workflow {
            operator: TypedOperator::Raster(
                GdalSource {
                    params: GdalSourceParameters { dataset },
                }
                .boxed(),
            ),
        },
    };

    let layer = AddLayer {
        name: def.properties.name,
        description: def.properties.description,
        workflow,
        symbology: def.properties.symbology,
    }
    .validated()?;

    let layer = layer_db.add_layer(layer).await?;
    layer_db
        .add_layer_to_collection(
            &layer,
            &LayerCollectionId(DATASET_LAYER_COLLECTION_ID.to_string()),
        )
        .await?;

    Ok(())
}

pub async fn add_datasets_from_directory<S: MockableSession, D: DatasetDb<S>, L: LayerDb>(
    dataset_db: &mut D,
    layer_db: &mut L,
    file_path: PathBuf,
) {
    async fn add_dataset_definition_from_dir_entry<
        S: MockableSession,
        D: DatasetDb<S>,
        L: LayerDb,
    >(
        db: &mut D,
        layer_db: &mut L,
        entry: &DirEntry,
    ) -> Result<()> {
        let def: DatasetDefinition =
            serde_json::from_reader(BufReader::new(File::open(entry.path())?))?;

        let id = db
            .add_dataset(
                &S::mock(), // TODO: find suitable way to add public dataset
                def.properties.clone().validated()?,
                db.wrap_meta_data(def.meta_data.clone()),
            )
            .await?; // TODO: add as system user

        add_dataset_as_layer(def, id, layer_db).await?;

        Ok(())
    }

    let dir = fs::read_dir(file_path);
    if dir.is_err() {
        warn!("Skipped adding datasets from directory because it can't be read");
        return;
    }
    let dir = dir.expect("checked");

    add_dataset_layer_collection(layer_db)
        .await
        .expect("Adding dataset layer collection must work");

    for entry in dir {
        match entry {
            Ok(entry) if entry.path().extension() == Some(OsStr::new("json")) => {
                if let Err(e) =
                    add_dataset_definition_from_dir_entry(dataset_db, layer_db, &entry).await
                {
                    warn!(
                        "Skipped adding dataset from directory entry: {:?} error: {}",
                        entry,
                        e.to_string()
                    );
                }
            }
            _ => {
                warn!("Skipped adding dataset from directory entry: {:?}", entry);
            }
        }
    }
}

pub async fn add_providers_from_directory<D: LayerProviderDb>(db: &mut D, file_path: PathBuf) {
    async fn add_provider_definition_from_dir_entry<D: LayerProviderDb>(
        db: &mut D,
        entry: &DirEntry,
    ) -> Result<()> {
        let def: Box<dyn ExternalLayerProviderDefinition> =
            serde_json::from_reader(BufReader::new(File::open(entry.path())?))?;

        db.add_layer_provider(def).await?; // TODO: add as system user
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
