use crate::{
    api::handlers::{
        ogc::{OgcApiResult, error::OgcApiError},
        workflows::workflow_metadata,
    },
    contexts::SessionContext,
    workflows::workflow::{Workflow, WorkflowId},
};
use geoengine_datatypes::spatial_reference::{SpatialReferenceAuthority, SpatialReferenceOption};
use geoengine_operators::engine::{RasterResultDescriptor, TypedResultDescriptor};
// use geoengine_datatypes::{
//     error::BoxedResultExt,
//     primitives::{BoundingBox2D, Coordinate2D},
// };
use ogcapi_types::common::{Authority, Bbox as OgcBbox, Crs, Datetime as OgcDatetime, Link};
use serde::{Deserialize, de::Error as _};
use std::str::FromStr;
use url::Url;

pub type LinkCreator = dyn Fn(&str, &'static str, &'static str) -> OgcApiResult<Link>;

pub fn link_creator(
    processing_graph_id: WorkflowId,
) -> impl Fn(&str, &'static str, &'static str) -> OgcApiResult<Link> {
    move |path: &str, rel: &'static str, mediatype: &'static str| -> OgcApiResult<Link> {
        let base_url = ogc_base_url(processing_graph_id)?;

        let href = base_url.join(path).map_err(crate::error::Error::from)?;

        Ok(Link::new(href.to_string(), rel).mediatype(mediatype))
    }
}

fn ogc_base_url(processing_graph_id: WorkflowId) -> OgcApiResult<Url> {
    let web_config = crate::config::get_config_element::<crate::config::Web>()?;
    let base = web_config.api_url()?;

    Ok(base
        .join(&format!("ogc/{processing_graph_id}/"))
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
