use super::error;
use crate::{
    api::{
        handlers::{
            ogc::{OgcApiResult, error::OgcApiError},
            workflows::workflow_metadata,
        },
        model::datatypes::{DataProviderId, LayerId},
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
    primitives::{AxisAlignedRectangle, SpatialPartition2D},
    spatial_reference::{SpatialReferenceAuthority, SpatialReferenceOption},
};
use geoengine_operators::engine::{
    InitializedRasterOperator, RasterResultDescriptor, TypedOperator, TypedResultDescriptor,
    WorkflowOperatorPath,
};
use ogcapi_types::common::{Authority, Bbox as OgcBbox, Crs, Datetime as OgcDatetime, Link};
use serde::{Deserialize, de::Error as _};
use snafu::ResultExt;
use std::str::FromStr;
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

// fn to_bounding_box2d(bbox: OgcBbox) -> OgcApiResult<BoundingBox2D> {
//     let coords = match bbox {
//         OgcBbox::Bbox2D(coords) => coords,
//         OgcBbox::Bbox3D(coords) => {
//             return Err(OgcApiError::Unsupported3DBoundingBox { coords })?;
//         }
//     };

//     BoundingBox2D::new(
//         Coordinate2D::new(coords[0], coords[1]),
//         Coordinate2D::new(coords[2], coords[3]),
//     )
//     .boxed_context(error::InvalidBoundingBox)
// }

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

pub async fn get_initialized_raster_operator<C: SessionContext>(
    layer: &Layer,
    execution_context: &C::ExecutionContext,
) -> OgcApiResult<Box<dyn InitializedRasterOperator>> {
    let operator = match layer.workflow.operator()? {
        TypedOperator::Raster(operator) => operator,
        TypedOperator::Vector(_) => {
            return Err(OgcApiError::ExpectedRaster {
                found: "vector".to_string(),
            });
        }
        TypedOperator::Plot(_) => {
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

pub async fn raster_operator_in_fitting_resolution<C: SessionContext>(
    initialized_operator: Box<dyn InitializedRasterOperator>,
    execution_context: &C::ExecutionContext,
    multiple_of_resolution: u32,
) -> OgcApiResult<Box<dyn InitializedRasterOperator>> {
    if multiple_of_resolution <= 1 {
        return Ok(initialized_operator);
    }

    let new_resolution = initialized_operator
        .result_descriptor()
        .spatial_grid
        .spatial_resolution()
        * f64::from(multiple_of_resolution);

    initialized_operator
        .optimize_and_reinitialize(new_resolution, execution_context)
        .await
        .boxed_context(error::InitializingProcessingGraph)
}
