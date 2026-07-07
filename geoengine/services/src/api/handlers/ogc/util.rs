use super::error;
use crate::{
    api::{
        handlers::{
            ogc::{OgcApiResult, error::OgcApiError},
            workflows::workflow_metadata,
        },
        model::{
            datatypes::{DataProviderId, LayerId},
            processing_graphs::{
                DeriveOutRasterSpecsSource, Interpolation, InterpolationMethod,
                InterpolationParameters, InterpolationResolution, RasterOperator, Reprojection,
                ReprojectionParameters, SingleRasterOrVectorOperator, SingleRasterOrVectorSource,
                SingleRasterSource, TypedOperator,
            },
        },
    },
    contexts::{ApplicationContext, SessionContext},
    layers::{
        layer::Layer,
        listing::LayerCollectionProvider,
        storage::{INTERNAL_PROVIDER_ID, LayerProviderDb},
    },
    workflows::workflow::Workflow,
};
use geoengine_datatypes::{
    error::BoxedResultExt,
    primitives::{AxisAlignedRectangle, Coordinate2D, SpatialPartition2D, SpatialResolution},
    spatial_reference::{SpatialReference, SpatialReferenceAuthority, SpatialReferenceOption},
};
use geoengine_operators::engine::{
    InitializedRasterOperator, RasterResultDescriptor, TypedResultDescriptor, WorkflowOperatorPath,
};
use ogcapi_types::common::{Authority, Bbox as OgcBbox, Crs, Datetime as OgcDatetime, Link};
use serde::{Deserialize, de::Error as _};
use snafu::ResultExt;
use std::{
    num::{NonZeroU16, NonZeroU64},
    str::FromStr,
};
use url::Url;

pub type LinkCreator = dyn Fn(&str, &'static str, &'static str) -> OgcApiResult<Link>;

pub fn link_creator(
    data_connector_id: DataProviderId,
    layer_id: LayerId,
) -> impl Fn(&str, &'static str, &'static str) -> OgcApiResult<Link> {
    move |path: &str, rel: &'static str, mediatype: &'static str| -> OgcApiResult<Link> {
        let base_url = ogc_base_url(data_connector_id, &layer_id)?;

        let href = base_url.join(path).map_err(crate::error::Error::from)?;

        Ok(Link::new(href.to_string(), rel).mediatype(mediatype))
    }
}

fn ogc_base_url(data_connector_id: DataProviderId, layer_id: &LayerId) -> OgcApiResult<Url> {
    let web_config = crate::config::get_config_element::<crate::config::Web>()?;
    let base = web_config.api_url()?;

    Ok(base
        .join(&format!("ogc/{data_connector_id}/{layer_id}/"))
        .map_err(crate::error::Error::from)?)
}

pub fn parse_datetime_option<'de, D>(deserializer: D) -> Result<Option<OgcDatetime>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Some(s) = Option::<String>::deserialize(deserializer)? else {
        return Ok(None);
    };

    OgcDatetime::from_str(&s)
        .map(Some)
        .map_err(D::Error::custom)
}

pub fn parse_bbox_option<'de, D>(deserializer: D) -> Result<Option<OgcBbox>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Some(s) = Option::<String>::deserialize(deserializer)? else {
        return Ok(None);
    };

    OgcBbox::from_str(&s).map(Some).map_err(D::Error::custom)
}

pub async fn raster_workflow_metadata<C: SessionContext>(
    processing_graph: Workflow,
    execution_context: C::ExecutionContext,
) -> OgcApiResult<RasterResultDescriptor> {
    let result_descriptor = workflow_metadata::<C>(processing_graph, execution_context).await?;
    match result_descriptor.into() {
        TypedResultDescriptor::Raster(descriptor) => Ok(descriptor),
        TypedResultDescriptor::Vector(_) => Err(OgcApiError::ExpectedRaster {
            found: "vector".to_string(),
        })?,
        TypedResultDescriptor::Plot(_) => Err(OgcApiError::ExpectedRaster {
            found: "plot".to_string(),
        })?,
    }
}

pub fn crs_from_spatial_reference_option(
    spatial_reference_option: SpatialReferenceOption,
) -> OgcApiResult<Crs> {
    let SpatialReferenceOption::SpatialReference(spatial_reference) = spatial_reference_option
    else {
        return Err(OgcApiError::MissingSpatialReference);
    };

    let authority = match spatial_reference.authority() {
        SpatialReferenceAuthority::Epsg => Authority::EPSG,
        SpatialReferenceAuthority::SrOrg
        | SpatialReferenceAuthority::Iau2000
        | SpatialReferenceAuthority::Esri => {
            return Err(OgcApiError::UnsupportedSpatialReferenceAuthority {
                from: (*spatial_reference.authority()).into(),
            });
        }
    };

    Ok(Crs::new(
        authority,
        0, // it is generally 0 by default, e.g., <https://www.opengis.net/def/crs/EPSG/0/4326>
        spatial_reference.code(),
    ))
}

pub fn to_ogc_bbox(spatial_bounds: SpatialPartition2D) -> OgcBbox {
    let lower_left = spatial_bounds.lower_left();
    let upper_right = spatial_bounds.upper_right();

    OgcBbox::Bbox2D([lower_left.x, lower_left.y, upper_right.x, upper_right.y])
}

pub async fn load_layer<C: ApplicationContext>(
    ctx: &C::SessionContext,
    data_connector_id: DataProviderId,
    layer_id: LayerId,
) -> OgcApiResult<Layer> {
    let data_connector_id = data_connector_id.into();
    let layer_id = layer_id.into();

    if data_connector_id == INTERNAL_PROVIDER_ID {
        return ctx
            .db()
            .load_layer(&layer_id)
            .await
            .context(error::LayerNotFound {
                data_connector_id,
                layer_id,
            });
    }

    ctx.db()
        .load_layer_provider(data_connector_id)
        .await?
        .load_layer(&layer_id)
        .await
        .context(error::LayerNotFound {
            data_connector_id,
            layer_id,
        })
}

/// Ensures that the layer exists in the given data connector. Returns an error if it does not exist.
pub async fn ensure_layer_exists<C: ApplicationContext>(
    app_ctx: &C,
    session: C::Session,
    data_connector_id: DataProviderId,
    layer_id: LayerId,
) -> OgcApiResult<()> {
    let ctx = app_ctx.session_context(session);
    let _layer = load_layer::<C>(&ctx, data_connector_id, layer_id).await?;

    Ok(())
}

pub async fn get_initialized_raster_operator<C: SessionContext>(
    layer: &Layer,
    execution_context: &C::ExecutionContext,
) -> OgcApiResult<Box<dyn InitializedRasterOperator>> {
    let operator = match layer.workflow.operator()? {
        geoengine_operators::engine::TypedOperator::Raster(operator) => operator,
        geoengine_operators::engine::TypedOperator::Vector(_) => {
            return Err(OgcApiError::ExpectedRaster {
                found: "vector".to_string(),
            });
        }
        geoengine_operators::engine::TypedOperator::Plot(_) => {
            return Err(OgcApiError::ExpectedRaster {
                found: "plot".to_string(),
            });
        }
    };

    operator
        .initialize(WorkflowOperatorPath::initialize_root(), execution_context)
        .await
        .boxed_context(error::InitializingProcessingGraph)
}

/// Reprojects the given processing graph to the target spatial reference
///
/// Modifies the processing graph in place.
pub fn processing_graph_with_reprojection(
    processing_graph: &mut Workflow,
    target_spatial_reference: SpatialReference,
) -> OgcApiResult<()> {
    const DERIVE_OUT_SPEC: DeriveOutRasterSpecsSource = DeriveOutRasterSpecsSource::DataBounds;

    *processing_graph = match processing_graph {
        Workflow::Typed { operator } => match operator {
            TypedOperator::Raster(operator) => Workflow::Typed {
                operator: TypedOperator::Raster(RasterOperator::Reprojection(Reprojection {
                    r#type: Default::default(),
                    params: ReprojectionParameters {
                        target_spatial_reference: target_spatial_reference.into(),
                        derive_out_spec: DERIVE_OUT_SPEC,
                    },
                    sources: Box::new(SingleRasterOrVectorSource {
                        source: SingleRasterOrVectorOperator::Raster(operator.clone()),
                    }),
                })),
            },
            TypedOperator::Vector(_) => Err(OgcApiError::ExpectedRaster {
                found: "vector".to_string(),
            })?,
            TypedOperator::Plot(_) => Err(OgcApiError::ExpectedRaster {
                found: "plot".to_string(),
            })?,
        },
        Workflow::Legacy { operator } => match operator {
            geoengine_operators::engine::TypedOperator::Raster(operator) => Workflow::Legacy {
                operator: geoengine_operators::engine::TypedOperator::Raster(Box::new(
                    geoengine_operators::processing::Reprojection {
                        params: geoengine_operators::processing::ReprojectionParams {
                            target_spatial_reference,
                            derive_out_spec: DERIVE_OUT_SPEC.into(),
                        },
                        sources: geoengine_operators::engine::SingleRasterOrVectorSource {
                            source:
                                geoengine_operators::util::input::RasterOrVectorOperator::Raster(
                                    operator.clone(),
                                ),
                        },
                    },
                )),
            },
            geoengine_operators::engine::TypedOperator::Vector(_) => {
                Err(OgcApiError::ExpectedRaster {
                    found: "vector".to_string(),
                })?
            }
            geoengine_operators::engine::TypedOperator::Plot(_) => {
                Err(OgcApiError::ExpectedRaster {
                    found: "plot".to_string(),
                })?
            }
        },
    };

    Ok(())
}

/// Resamples the given processing graph to the target spatial reference
///
/// Modifies the processing graph in place.
pub fn processing_graph_with_resampling(
    processing_graph: &mut Workflow,
    target_origin: Coordinate2D,
    target_resolution: SpatialResolution,
) -> OgcApiResult<()> {
    *processing_graph = match processing_graph {
        Workflow::Typed { operator } => match operator {
            TypedOperator::Raster(operator) => Workflow::Typed {
                operator: TypedOperator::Raster(RasterOperator::Interpolation(Interpolation {
                    r#type: Default::default(),
                    params: InterpolationParameters {
                        interpolation: InterpolationMethod::NearestNeighbor,
                        output_resolution: InterpolationResolution::Fraction {
                            x: target_resolution.x,
                            y: target_resolution.y,
                        },
                        output_origin_reference: Some(target_origin.into()),
                    },
                    sources: Box::new(SingleRasterSource {
                        raster: operator.clone(),
                    }),
                })),
            },
            TypedOperator::Vector(_) => Err(OgcApiError::ExpectedRaster {
                found: "vector".to_string(),
            })?,
            TypedOperator::Plot(_) => Err(OgcApiError::ExpectedRaster {
                found: "plot".to_string(),
            })?,
        },
        Workflow::Legacy { operator } => match operator {
            geoengine_operators::engine::TypedOperator::Raster(operator) => Workflow::Legacy {
                operator: geoengine_operators::engine::TypedOperator::Raster(Box::new(
                    geoengine_operators::processing::Interpolation {
                        params: geoengine_operators::processing::InterpolationParams {
                            interpolation: geoengine_operators::processing::InterpolationMethod::NearestNeighbor,
                            output_resolution: geoengine_operators::processing::InterpolationResolution::Fraction {
                                x: target_resolution.x,
                                y: target_resolution.y,
                            },
                            output_origin_reference: Some(target_origin),
                        },
                        sources: geoengine_operators::engine::SingleRasterSource {
                            raster: operator.clone(),
                        },
                    },
                )),
            },
            geoengine_operators::engine::TypedOperator::Vector(_) => {
                Err(OgcApiError::ExpectedRaster {
                    found: "vector".to_string(),
                })?
            }
            geoengine_operators::engine::TypedOperator::Plot(_) => {
                Err(OgcApiError::ExpectedRaster {
                    found: "plot".to_string(),
                })?
            }
        },
    };

    Ok(())
}

pub fn to_non_zero_u16(value: usize) -> NonZeroU16 {
    let value = u16::try_from(value).unwrap_or(u16::MAX);
    NonZeroU16::new(value.max(1)).unwrap_or(NonZeroU16::MIN)
}

pub fn to_non_zero_u64(value: usize) -> NonZeroU64 {
    NonZeroU64::try_from(value as u64).unwrap_or(NonZeroU64::MIN)
}

pub struct OriginAndResolution {
    pub origin: Coordinate2D,
    pub resolution: SpatialResolution,
}

/// Check if the layer's spatial reference matches the required spatial reference for the requested TMS.
///
/// If the layer's spatial reference does not match, modify the processing graph to include a reprojection operator
/// to the required spatial reference.
///
/// Then, reinitialize the raster operator to reflect the updated processing graph.
pub async fn reproject_if_necessary<C: SessionContext>(
    layer: &mut Layer,
    initialized_operator: &mut Box<dyn InitializedRasterOperator>,
    execution_context: &C::ExecutionContext,
    required_srs: Option<SpatialReference>,
    required_origin_and_resolution: Option<OriginAndResolution>,
) -> OgcApiResult<()> {
    let Some(layer_srs) = initialized_operator
        .result_descriptor()
        .spatial_reference
        .as_option()
    else {
        return Err(OgcApiError::MissingSpatialReference);
    };

    // If the required SRS is provided, check if it is different from the layer's SRS.
    let required_srs = required_srs.filter(|required_srs| layer_srs != *required_srs);

    if required_srs.is_none() && required_origin_and_resolution.is_none() {
        return Ok(());
    }

    if let Some(target_srs) = required_srs {
        processing_graph_with_reprojection(&mut layer.workflow, target_srs)?;
    }

    if let Some(target) = required_origin_and_resolution {
        processing_graph_with_resampling(&mut layer.workflow, target.origin, target.resolution)?;
    }

    *initialized_operator = get_initialized_raster_operator::<C>(layer, execution_context).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_converts_values_to_non_zero_u64() {
        // Normal cases
        assert_eq!(to_non_zero_u64(1), 1u64.try_into().unwrap());
        assert_eq!(to_non_zero_u64(1024), 1024u64.try_into().unwrap());

        // Edge case: zero becomes 1
        let result = to_non_zero_u64(0);
        assert_eq!(result.get(), 1);

        // Large value
        assert_eq!(to_non_zero_u64(1_000_000), 1_000_000u64.try_into().unwrap());

        // Common grid dimensions
        assert_eq!(to_non_zero_u64(2048), 2048u64.try_into().unwrap());
    }

    #[test]
    fn it_converts_values_to_non_zero_u16() {
        // Normal cases
        assert_eq!(to_non_zero_u16(1), 1u16.try_into().unwrap());
        assert_eq!(to_non_zero_u16(512), 512u16.try_into().unwrap());
        assert_eq!(to_non_zero_u16(65535), 65535u16.try_into().unwrap());

        // Edge case: zero becomes 1
        assert_eq!(to_non_zero_u16(0), 1u16.try_into().unwrap());

        // Edge case: overflow wraps to u16::MAX
        let result = to_non_zero_u16(usize::MAX);
        assert!(result.get() > 0);

        // Common tile size
        assert_eq!(to_non_zero_u16(256), 256u16.try_into().unwrap());
    }
}
