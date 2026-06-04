use crate::{api::handlers::ogc::OgcApiResult, workflows::workflow::WorkflowId};
// use geoengine_datatypes::{
//     error::BoxedResultExt,
//     primitives::{BoundingBox2D, Coordinate2D},
// };
use ogcapi_types::common::{Bbox as OgcBbox, Datetime as OgcDatetime, Link};
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
