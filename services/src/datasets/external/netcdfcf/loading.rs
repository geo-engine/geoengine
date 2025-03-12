use super::{
    error,
    metadata::{Creator, DataRange, NetCdfGroupMetadata, NetCdfOverviewMetadata},
    NetCdfEntity, Result,
};
use crate::{
    layers::{
        layer::{
            CollectionItem, Layer, LayerCollection, LayerCollectionListing, LayerListing, Property,
            ProviderLayerCollectionId, ProviderLayerId,
        },
        listing::LayerCollectionId,
    },
    projects::{RasterSymbology, Symbology},
    workflows::workflow::Workflow,
};
use geoengine_datatypes::{
    dataset::{DataProviderId, LayerId, NamedData},
    operations::image::{Colorizer, RasterColorizer},
    primitives::{CacheTtlSeconds, TimeInstance},
};
use geoengine_operators::{
    engine::{RasterOperator, RasterResultDescriptor, TypedOperator},
    source::{
        GdalDatasetParameters, GdalLoadingInfoTemporalSlice, GdalMetaDataList, GdalSource,
        GdalSourceParameters,
    },
};
use snafu::ResultExt;
use std::path::{Path, PathBuf};

pub struct LayerCollectionParts<'a, E: Iterator<Item = NetCdfEntity>> {
    pub provider_id: DataProviderId,
    pub collection_id: LayerCollectionId,
    pub group_path: &'a [String],
    pub overview: NetCdfOverviewMetadata,
    pub group: Option<NetCdfGroupMetadata>,
    pub subgroups: Vec<NetCdfGroupMetadata>,
    pub entities: E,
}

pub trait LayerCollectionIdFn: Send + Sync {
    fn layer_collection_id(&self, file_name: &Path, group_path: &[String]) -> LayerCollectionId;
    fn layer_id(&self, file_name: &Path, group_path: &[String], entity_id: usize) -> LayerId;
}

pub fn create_layer_collection_from_parts<ID: LayerCollectionIdFn>(
    LayerCollectionParts {
        provider_id,
        collection_id,
        group_path,
        overview,
        group,
        entities,
        subgroups,
    }: LayerCollectionParts<impl Iterator<Item = NetCdfEntity>>,
    id_fn: &ID,
) -> LayerCollection {
    let (name, description) = if let Some(group) = group {
        (group.title, group.description)
    } else {
        (overview.title, overview.summary)
    };
    let creator = if group_path.is_empty() {
        overview.creator
    } else {
        Creator::empty()
    };

    let items = if subgroups.is_empty() {
        entities
            .map(|entity| {
                CollectionItem::Layer(LayerListing {
                    r#type: Default::default(),
                    id: ProviderLayerId {
                        provider_id,
                        layer_id: id_fn.layer_id(
                            Path::new(overview.file_name.as_str()),
                            group_path,
                            entity.id,
                        ),
                    },
                    name: entity.name,
                    description: String::new(),
                    properties: vec![],
                })
            })
            .collect::<Vec<CollectionItem>>()
    } else {
        let group_path = group_path.to_owned();
        subgroups
            .into_iter()
            .map(|group| {
                let mut out_groups = group_path.clone();
                out_groups.push(group.name.clone());
                CollectionItem::Collection(LayerCollectionListing {
                    r#type: Default::default(),
                    id: ProviderLayerCollectionId {
                        provider_id,
                        collection_id: id_fn.layer_collection_id(
                            Path::new(overview.file_name.as_str()),
                            &out_groups,
                        ),
                    },
                    name: group.title.clone(),
                    description: group.description,
                    properties: Default::default(),
                })
            })
            .collect::<Vec<CollectionItem>>()
    };

    LayerCollection {
        id: ProviderLayerCollectionId {
            provider_id,
            collection_id,
        },
        name,
        description,
        items,
        entry_label: None,
        properties: create_properties_for_layer_collection(&creator),
    }
}

fn create_properties_for_layer_collection(creator: &Creator) -> Vec<Property> {
    let mut properties = Vec::new();

    if let Some(property) = creator.as_property() {
        properties.push(property);
    }

    properties
}

pub fn create_layer(
    provider_layer_id: ProviderLayerId,
    data_id: NamedData,
    netcdf_entity: NetCdfEntity,
    colorizer: Colorizer,
    creator: &Creator,
    time_steps: &[TimeInstance],
    data_range: DataRange,
) -> Result<Layer> {
    Ok(Layer {
        id: provider_layer_id,
        name: netcdf_entity.name.clone(),
        description: netcdf_entity.name,
        workflow: Workflow {
            operator: TypedOperator::Raster(
                GdalSource {
                    params: GdalSourceParameters { data: data_id },
                }
                .boxed(),
            ),
        },
        symbology: Some(Symbology::Raster(RasterSymbology {
            r#type: Default::default(),
            opacity: 1.0,
            raster_colorizer: RasterColorizer::SingleBand {
                band: 0,
                band_colorizer: colorizer,
            },
        })),
        properties: [creator.as_property()].into_iter().flatten().collect(),
        metadata: [
            (
                "timeSteps".to_string(),
                serde_json::to_string(&time_steps).context(error::CannotSerializeLayer)?,
            ),
            (
                "dataRange".to_string(),
                data_range.as_json().context(error::CannotSerializeLayer)?,
            ),
        ]
        .into_iter()
        .collect(),
    })
}

pub enum ParamModification {
    File {
        file_path: PathBuf,
        time_instance: TimeInstance,
    },
    Channel {
        channel: usize,
        time_instance: TimeInstance,
    },
}

pub fn create_loading_info(
    result_descriptor: RasterResultDescriptor,
    params_blueprint: &GdalDatasetParameters,
    modifications: impl Iterator<Item = ParamModification>,
    cache_ttl: CacheTtlSeconds,
) -> GdalMetaDataList {
    GdalMetaDataList {
        result_descriptor,
        params: modifications
            .map(|modification| create_loading_info_part(params_blueprint, modification, cache_ttl))
            .collect(),
    }
}

fn create_loading_info_part(
    params_blueprint: &GdalDatasetParameters,
    modification: ParamModification,
    cache_ttl: CacheTtlSeconds,
) -> GdalLoadingInfoTemporalSlice {
    let mut params = params_blueprint.clone();

    let time = match modification {
        ParamModification::File {
            file_path,
            time_instance,
        } => {
            params.file_path =
                file_path.with_file_name(time_instance.as_datetime_string_with_millis() + ".tiff");

            time_instance.into()
        }
        ParamModification::Channel {
            channel,
            time_instance,
        } => {
            params.rasterband_channel = channel;

            time_instance.into()
        }
    };

    GdalLoadingInfoTemporalSlice {
        time,
        params: Some(params),
        cache_ttl,
    }
}
