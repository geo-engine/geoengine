use std::ffi::OsStr;
use std::{
    fs::{self, DirEntry, File},
    io::BufReader,
    path::PathBuf,
};

use crate::datasets::storage::MetaDataDefinition;
use crate::layers::layer::{AddLayer, AddLayerCollection, LayerCollectionId};
use crate::layers::storage::LayerDb;
use crate::util::user_input::UserInput;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::Workflow;
use crate::{contexts::MockableSession, datasets::storage::DatasetDb};
use crate::{datasets::storage::ExternalDatasetProviderDefinition, error::Result};

use super::storage::DatasetDefinition;

use geoengine_datatypes::dataset::DatasetId;
use geoengine_operators::engine::{RasterOperator, TypedOperator, VectorOperator};
use geoengine_operators::mock::{MockDatasetDataSource, MockDatasetDataSourceParams};
use geoengine_operators::source::{
    GdalSource, GdalSourceParameters, OgrSource, OgrSourceParameters,
};
use log::warn;

const DATASET_LAYER_COLLECTION_ID: LayerCollectionId =
    LayerCollectionId::from_u128(0xa762_fc70_a23f_4957_bdb5_a12f_7405_9058);

pub async fn add_dataset_layer_collection<L: LayerDb>(layer_db: &mut L) -> Result<()> {
    let collection = AddLayerCollection {
        name: "Datasets".to_string(),
        description: "Available datasets".to_string(),
    }
    .validated()?;

    layer_db
        .add_collection_with_id(DATASET_LAYER_COLLECTION_ID, collection)
        .await?;

    Ok(())
}

pub async fn add_dataset_as_layer<L: LayerDb, W: WorkflowRegistry>(
    def: DatasetDefinition,
    dataset: DatasetId,
    layer_db: &mut L,
    workflow_db: &mut W,
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

    let workflow = workflow_db.register(workflow).await?;

    let layer = AddLayer {
        name: def.properties.name,
        description: def.properties.description,
        workflow,
        symbology: def.properties.symbology,
    }
    .validated()?;

    let layer = layer_db.add_layer(layer).await?;
    layer_db
        .add_layer_to_collection(layer, DATASET_LAYER_COLLECTION_ID)
        .await?;

    Ok(())
}

pub async fn add_datasets_from_directory<
    S: MockableSession,
    D: DatasetDb<S>,
    L: LayerDb,
    W: WorkflowRegistry,
>(
    dataset_db: &mut D,
    layer_db: &mut L,
    workflow_db: &mut W,
    file_path: PathBuf,
) {
    async fn add_dataset_definition_from_dir_entry<
        S: MockableSession,
        D: DatasetDb<S>,
        L: LayerDb,
        W: WorkflowRegistry,
    >(
        db: &mut D,
        layer_db: &mut L,
        workflow_db: &mut W,
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

        add_dataset_as_layer(def, id, layer_db, workflow_db).await?;

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
                    add_dataset_definition_from_dir_entry(dataset_db, layer_db, workflow_db, &entry)
                        .await
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

pub async fn add_providers_from_directory<D: DatasetDb<S>, S: MockableSession>(
    db: &mut D,
    file_path: PathBuf,
) {
    async fn add_provider_definition_from_dir_entry<D: DatasetDb<S>, S: MockableSession>(
        db: &mut D,
        entry: &DirEntry,
    ) -> Result<()> {
        let def: Box<dyn ExternalDatasetProviderDefinition> =
            serde_json::from_reader(BufReader::new(File::open(entry.path())?))?;

        db.add_dataset_provider(&S::mock(), def).await?; // TODO: add as system user
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
