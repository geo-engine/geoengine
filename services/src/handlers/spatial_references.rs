use std::str::FromStr;

use crate::error::Result;
use geoengine_datatypes::spatial_reference::SpatialReferenceAuthority;
use geoengine_datatypes::{primitives::BoundingBox2D, spatial_reference::SpatialReference};
use proj_sys::PJ_PROJ_STRING_TYPE_PJ_PROJ_4;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use warp::Filter;

use crate::{contexts::Session, error};
use crate::{
    error::Error,
    handlers::{authenticate, Context},
};

/// The specification of a spatial reference, where extent and axis labels are given
/// in natural order (x, y) = (east, north)
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SpatialReferenceSpecification {
    name: String,
    spatial_reference: SpatialReference,
    proj_string: String,
    extent: BoundingBox2D,
    axis_labels: Option<(String, String)>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ProjJson {
    name: String,
    coordinate_system: ProjJsonCoordinateSystem,
}

impl ProjJson {
    /// the axis order of the projection, if one is defined
    pub fn axis_order(&self) -> Option<AxisOrder> {
        match &self.coordinate_system.axis {
            Some(axes) => match *axes.as_slice() {
                [ProjJsonAxis {
                    direction: ProjJsonAxisDirection::North,
                    ..
                }, ProjJsonAxis {
                    direction: ProjJsonAxisDirection::East,
                    ..
                }] => Some(AxisOrder::NorthEast),
                [ProjJsonAxis {
                    direction: ProjJsonAxisDirection::East,
                    ..
                }, ProjJsonAxis {
                    direction: ProjJsonAxisDirection::North,
                    ..
                }] => Some(AxisOrder::EastNorth),
                _ => None,
            },
            _ => None,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ProjJsonCoordinateSystem {
    axis: Option<Vec<ProjJsonAxis>>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ProjJsonAxis {
    name: String,
    abbreviation: String,
    direction: ProjJsonAxisDirection,
    unit: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ProjJsonAxisDirection {
    East,
    North,
}

pub enum AxisOrder {
    NorthEast,
    EastNorth,
}

/// Get the proj json information for the given `srs_string` if it is known.
// TODO: expose method in proj crate instead
fn proj_json(srs_string: &str) -> Option<ProjJson> {
    unsafe {
        let c_definition = std::ffi::CString::new(srs_string).ok()?;

        let ctx = proj_sys::proj_context_create();
        let c_proj = proj_sys::proj_create(ctx, c_definition.as_ptr());

        let string = if c_proj.is_null() {
            None
        } else {
            let c_buf = proj_sys::proj_as_projjson(ctx, c_proj, std::ptr::null());

            std::ffi::CStr::from_ptr(c_buf)
                .to_str()
                .map(ToOwned::to_owned)
                .ok()
        };

        proj_sys::proj_destroy(c_proj);
        proj_sys::proj_context_destroy(ctx);
        proj_sys::proj_cleanup();

        string.and_then(|s| serde_json::from_str(&s).ok())
    }
}

/// Get the proj json information for the given `srs_string` if it is known.
// TODO: expose method in proj crate instead
fn proj_proj_string(srs_string: &str) -> Option<String> {
    unsafe {
        let c_definition = std::ffi::CString::new(srs_string).ok()?;

        let ctx = proj_sys::proj_context_create();
        let c_proj = proj_sys::proj_create(ctx, c_definition.as_ptr());

        let string = if c_proj.is_null() {
            None
        } else {
            let c_buf = proj_sys::proj_as_proj_string(
                ctx,
                c_proj,
                PJ_PROJ_STRING_TYPE_PJ_PROJ_4,
                std::ptr::null(),
            );

            std::ffi::CStr::from_ptr(c_buf)
                .to_str()
                .map(ToOwned::to_owned)
                .ok()
        };

        proj_sys::proj_destroy(c_proj);
        proj_sys::proj_context_destroy(ctx);
        proj_sys::proj_cleanup();

        string
    }
}

pub(crate) fn get_spatial_reference_specification_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("spatialReferenceSpecification" / String)
        .and(warp::get())
        .and(authenticate(ctx))
        .and_then(get_spatial_reference_specification)
}

#[allow(clippy::unused_async)] // the function signature of `Filter`'s `and_then` requires it
async fn get_spatial_reference_specification<S: Session>(
    srs_string: String,
    _session: S,
) -> Result<impl warp::Reply, warp::Rejection> {
    let spec = spatial_reference_specification(&srs_string)?;
    Ok(warp::reply::json(&spec))
}

/// custom spatial references not known by proj or that shall be overriden
fn custom_spatial_reference_specification(
    srs_string: &str,
) -> Option<SpatialReferenceSpecification> {
    // TODO: provide a generic storage for custom spatial reference specifications
    match srs_string.to_uppercase().as_str() {
        "SR-ORG:81" => Some(SpatialReferenceSpecification {
            name: "GEOS - GEOstationary Satellite".to_owned(),
            spatial_reference: SpatialReference::new(SpatialReferenceAuthority::SrOrg, 81),
            proj_string: "+proj=geos +lon_0=0 +h=-0 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs"
                .into(),
            extent: BoundingBox2D::new_unchecked(
                (-5_568_748.276, -5_568_748.276).into(),
                (5_568_748.276, 5_568_748.276).into(),
            ),
            axis_labels: None,
        }),
        _ => None,
    }
}

fn spatial_reference_specification(srs_string: &str) -> Result<SpatialReferenceSpecification> {
    if let Some(sref) = custom_spatial_reference_specification(srs_string) {
        return Ok(sref);
    }

    let spatial_reference = SpatialReference::from_str(srs_string).context(error::DataType)?;
    let json = proj_json(srs_string).ok_or_else(|| Error::UnknownSrsString {
        srs_string: srs_string.to_owned(),
    })?;
    let proj_string = proj_proj_string(srs_string).ok_or_else(|| Error::UnknownSrsString {
        srs_string: srs_string.to_owned(),
    })?;

    let extent = spatial_reference
        .area_of_use_projected()
        .context(error::DataType)?;

    let axis_labels = json.coordinate_system.axis.as_ref().map(|axes| {
        let a0 = axes.get(0).map_or("".to_owned(), |a| a.name.clone());
        let a1 = axes.get(1).map_or("".to_owned(), |a| a.name.clone());

        match json.axis_order() {
            None | Some(AxisOrder::EastNorth) => (a0, a1),
            Some(AxisOrder::NorthEast) => (a1, a0),
        }
    });
    let spec = SpatialReferenceSpecification {
        name: json.name,
        spatial_reference,
        proj_string,
        extent,
        axis_labels,
    };
    Ok(spec)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::contexts::InMemoryContext;
    use crate::contexts::SimpleContext;
    use crate::handlers::handle_rejection;
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::spatial_reference::SpatialReferenceAuthority;
    use serde_json;

    #[tokio::test]
    async fn get_spatial_reference() {
        let ctx = InMemoryContext::default();
        let session_id = ctx.default_session_ref().await.id();

        let response = warp::test::request()
            .method("GET")
            .path("/spatialReferenceSpecification/EPSG:4326")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session_id.to_string()),
            )
            .reply(&get_spatial_reference_specification_handler(ctx).recover(handle_rejection))
            .await;

        assert_eq!(response.status(), 200);

        let body: String = String::from_utf8(response.body().to_vec()).unwrap();
        let spec: SpatialReferenceSpecification = serde_json::from_str(&body).unwrap();
        assert_eq!(
            SpatialReferenceSpecification {
                name: "WGS 84".to_owned(),
                spatial_reference: SpatialReference::epsg_4326(),
                proj_string: "+proj=longlat +datum=WGS84 +no_defs +type=crs".to_owned(),
                extent: BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into()),
                axis_labels: Some((
                    "Geodetic longitude".to_owned(),
                    "Geodetic latitude".to_owned()
                )),
            },
            spec
        );
    }

    #[test]
    fn spec_webmercator() {
        let spec = spatial_reference_specification("EPSG:3857").unwrap();
        assert_eq!(SpatialReferenceSpecification {
                name: "WGS 84 / Pseudo-Mercator".to_owned(),
                spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857),
                proj_string: "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +nadgrids=@null +wktext +no_defs +type=crs".into(),
                extent: BoundingBox2D::new_unchecked((-20_037_508.342_789_244, -20_048_966.104_014_6).into(),  (20_037_508.342_789_244, 20_048_966.104_014_594).into()),
                axis_labels: Some(("Easting".to_owned(), "Northing".to_owned())),
            },
            spec
        );
    }

    #[test]
    fn spec_wgs84() {
        let spec = spatial_reference_specification("EPSG:4326").unwrap();
        assert_eq!(
            SpatialReferenceSpecification {
                name: "WGS 84".to_owned(),
                spatial_reference: SpatialReference::epsg_4326(),
                proj_string: "+proj=longlat +datum=WGS84 +no_defs +type=crs".to_owned(),
                extent: BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into()),
                axis_labels: Some((
                    "Geodetic longitude".to_owned(),
                    "Geodetic latitude".to_owned()
                )),
            },
            spec
        );
    }

    #[test]
    fn spec_utm32n() {
        let spec = spatial_reference_specification("EPSG:32632").unwrap();
        assert_eq!(
            SpatialReferenceSpecification {
                name: "WGS 84 / UTM zone 32N".to_owned(),
                spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632),
                proj_string: "+proj=utm +zone=32 +datum=WGS84 +units=m +no_defs +type=crs".into(),
                extent: BoundingBox2D::new_unchecked(
                    (166_021.443_080_539_64, 0.0).into(),
                    (833_978.556_919_460_4, 9_329_005.182_447_437).into()
                ),
                axis_labels: Some(("Easting".to_owned(), "Northing".to_owned())),
            },
            spec
        );
    }

    #[test]
    fn spec_geos() {
        let spec = spatial_reference_specification("SR-ORG:81").unwrap();
        assert_eq!(
            SpatialReferenceSpecification {
                name: "GEOS - GEOstationary Satellite".to_owned(),
                spatial_reference: SpatialReference::new(SpatialReferenceAuthority::SrOrg, 81),
                proj_string:
                    "+proj=geos +lon_0=0 +h=-0 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs".into(),
                extent: BoundingBox2D::new_unchecked(
                    (-5_568_748.276, -5_568_748.276).into(),
                    (5_568_748.276, 5_568_748.276).into()
                ),
                axis_labels: None,
            },
            spec
        );
    }
}
