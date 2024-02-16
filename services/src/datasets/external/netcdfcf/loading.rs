use super::{error, NetCdfCf4DDatasetId, NetCdfEntity, Result};
use crate::{
    layers::{
        layer::{
            CollectionItem, Layer, LayerCollection, Property, ProviderLayerCollectionId,
            ProviderLayerId,
        },
        listing::LayerCollectionId,
    },
    projects::{RasterSymbology, Symbology},
    workflows::workflow::Workflow,
};
use geoengine_datatypes::{
    dataset::{DataProviderId, LayerId},
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
use std::path::PathBuf;

pub fn create_layer_collection(
    provider_id: DataProviderId,
    collection_id: LayerCollectionId,
    name: String,
    description: String,
    items: Vec<CollectionItem>,
    creator_name: Option<String>,
    creator_email: Option<String>,
    creator_institution: Option<String>,
) -> LayerCollection {
    let mut properties = Vec::new();

    if let Some(creator_name) = creator_name {
        let property = Property::from((
            "author".to_string(),
            format!(
                "{creator_name}, {}, {}",
                creator_email.unwrap_or_else(|| "unknown".to_string()),
                creator_institution.unwrap_or_else(|| "unknown".to_string())
            ),
        ));
        properties.push(property);
    }

    LayerCollection {
        id: ProviderLayerCollectionId {
            provider_id,
            collection_id,
        },
        name,
        description,
        items,
        entry_label: None,
        properties,
    }
}

pub fn create_layer(
    provider_id: DataProviderId,
    layer_id: &LayerId,
    dataset_id: &NetCdfCf4DDatasetId,
    netcdf_entity: NetCdfEntity,
    colorizer: Colorizer,
    creator_name: Option<String>,
    creator_email: Option<String>,
    creator_institution: Option<String>,
    time_steps: &[TimeInstance],
    data_range: (f64, f64),
) -> Result<Layer> {
    Ok(Layer {
        id: ProviderLayerId {
            provider_id,
            layer_id: layer_id.clone(),
        },
        name: netcdf_entity.name.clone(),
        description: netcdf_entity.name,
        workflow: Workflow {
            operator: TypedOperator::Raster(
                GdalSource {
                    params: GdalSourceParameters {
                        data: geoengine_datatypes::dataset::NamedData::with_system_provider(
                            provider_id.to_string(),
                            serde_json::to_string(dataset_id)
                                .context(error::CannotSerializeLayer)?,
                        ),
                    },
                }
                .boxed(),
            ),
        },
        symbology: Some(Symbology::Raster(RasterSymbology {
            opacity: 1.0,
            raster_colorizer: RasterColorizer::SingleBand {
                band: 0,
                band_colorizer: colorizer,
            },
        })),
        properties: [(
            "author".to_string(),
            format!(
                "{}, {}, {}",
                creator_name.unwrap_or_else(|| "unknown".to_string()),
                creator_email.unwrap_or_else(|| "unknown".to_string()),
                creator_institution.unwrap_or_else(|| "unknown".to_string())
            ),
        )
            .into()]
        .into_iter()
        .collect(),
        metadata: [
            (
                "timeSteps".to_string(),
                serde_json::to_string(&time_steps).context(error::CannotSerializeLayer)?,
            ),
            (
                "dataRange".to_string(),
                serde_json::to_string(&data_range).context(error::CannotSerializeLayer)?,
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
