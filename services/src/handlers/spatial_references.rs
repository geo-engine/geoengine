use crate::api::model::datatypes::{
    BoundingBox2D, Coordinate2D, SpatialReference, SpatialReferenceAuthority, StringPair,
};
use crate::handlers::Context;
use crate::{error, error::Error, error::Result};
use actix_web::{web, FromRequest, Responder};
use proj_sys::PJ_PROJ_STRING_TYPE_PJ_PROJ_4;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::str::FromStr;
use utoipa::ToSchema;

pub(crate) fn init_spatial_reference_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: Context,
    C::Session: FromRequest,
{
    cfg.service(
        web::resource("/spatialReferenceSpecification/{srs_string}")
            .route(web::get().to(get_spatial_reference_specification_handler::<C>)),
    );
}

/// The specification of a spatial reference, where extent and axis labels are given
/// in natural order (x, y) = (east, north)
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SpatialReferenceSpecification {
    pub name: String,
    pub spatial_reference: SpatialReference,
    pub proj_string: String,
    pub extent: BoundingBox2D,
    pub axis_labels: Option<StringPair>,
    pub axis_order: Option<AxisOrder>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
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

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct ProjJsonCoordinateSystem {
    axis: Option<Vec<ProjJsonAxis>>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct ProjJsonAxis {
    name: String,
    abbreviation: String,
    direction: ProjJsonAxisDirection,
    unit: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ProjJsonAxisDirection {
    East,
    North,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
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

#[allow(clippy::unused_async)] // the function signature of request handlers requires it
#[utoipa::path(
    tag = "Spatial References",
    get,
    path = "/spatialReferenceSpecification/{srs_string}",
    responses(
        (status = 200, description = "OK", body = SpatialReferenceSpecification,
            example = json!({
                "name": "WGS 84",
                "spatialReference": "EPSG:4326",
                "projString": "+proj=longlat +datum=WGS84 +no_defs +type=crs",
                "extent": {
                    "lowerLeftCoordinate": {
                        "x": -180.0,
                        "y": -90.0
                    },
                    "upperRightCoordinate": {
                        "x": 180.0,
                        "y": 90.0
                    }
                },
                "axisLabels": [
                    "Geodetic longitude",
                    "Geodetic latitude"
                ],
                "axisOrder": "northEast"
            })
        )
    ),
    params(
        ("srs_string", example = "EPSG:4326")
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn get_spatial_reference_specification_handler<C: Context>(
    srs_string: web::Path<String>,
    _session: C::Session,
) -> Result<impl Responder> {
    spatial_reference_specification(&srs_string).map(web::Json)
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
            extent: BoundingBox2D {
                lower_left_coordinate: Coordinate2D {
                    x: -5_568_748.276,
                    y: -5_568_748.276,
                },
                upper_right_coordinate: Coordinate2D {
                    x: 5_568_748.276,
                    y: 5_568_748.276,
                },
            },
            axis_labels: None,
            axis_order: Some(AxisOrder::EastNorth),
        }),
        _ => None,
    }
}

pub fn spatial_reference_specification(srs_string: &str) -> Result<SpatialReferenceSpecification> {
    if let Some(sref) = custom_spatial_reference_specification(srs_string) {
        return Ok(sref);
    }

    let spatial_reference =
        geoengine_datatypes::spatial_reference::SpatialReference::from_str(srs_string)
            .context(error::DataType)?;
    let json = proj_json(srs_string).ok_or_else(|| Error::UnknownSrsString {
        srs_string: srs_string.to_owned(),
    })?;
    let proj_string = proj_proj_string(srs_string).ok_or_else(|| Error::UnknownSrsString {
        srs_string: srs_string.to_owned(),
    })?;

    let extent: geoengine_datatypes::primitives::BoundingBox2D = spatial_reference
        .area_of_use_projected()
        .context(error::DataType)?;

    let axis_labels = json.coordinate_system.axis.as_ref().map(|axes| {
        let a0 = axes.get(0).map_or(String::new(), |a| a.name.clone());
        let a1 = axes.get(1).map_or(String::new(), |a| a.name.clone());

        match json.axis_order() {
            None | Some(AxisOrder::EastNorth) => (a0, a1),
            Some(AxisOrder::NorthEast) => (a1, a0),
        }
    });
    let spec = SpatialReferenceSpecification {
        axis_order: json.axis_order(),
        name: json.name,
        spatial_reference: spatial_reference.into(),
        proj_string,
        extent: extent.into(),
        axis_labels: axis_labels.map(Into::into),
    };
    Ok(spec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::SimpleContext;
    use crate::contexts::{InMemoryContext, Session};
    use crate::util::tests::send_test_request;
    use actix_web;
    use actix_web::http::header;
    use actix_web_httpauth::headers::authorization::Bearer;
    use float_cmp::approx_eq;
    use geoengine_datatypes::primitives::BoundingBox2D;
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceAuthority};
    use geoengine_datatypes::util::test::TestDefault;

    #[tokio::test]
    async fn get_spatial_reference() {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let req = actix_web::test::TestRequest::get()
            .uri("/spatialReferenceSpecification/EPSG:4326")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        assert_eq!(res.status(), 200);

        let spec: SpatialReferenceSpecification = actix_web::test::read_body_json(res).await;
        assert_eq!(
            SpatialReferenceSpecification {
                name: "WGS 84".to_owned(),
                spatial_reference: SpatialReference::epsg_4326().into(),
                proj_string: "+proj=longlat +datum=WGS84 +no_defs +type=crs".to_owned(),
                extent: BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into())
                    .into(),
                axis_labels: Some(
                    (
                        "Geodetic longitude".to_owned(),
                        "Geodetic latitude".to_owned()
                    )
                        .into()
                ),
                axis_order: Some(AxisOrder::NorthEast),
            },
            spec
        );
    }

    #[test]
    fn spec_webmercator() {
        let spec = spatial_reference_specification("EPSG:3857").unwrap();
        assert_eq!(spec.name, "WGS 84 / Pseudo-Mercator");
        assert_eq!(
            spec.spatial_reference,
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857).into()
        );
        assert_eq!(
            spec.proj_string,
            "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +nadgrids=@null +wktext +no_defs +type=crs",
        );
        assert!(approx_eq!(
            BoundingBox2D,
            spec.extent.into(),
            BoundingBox2D::new_unchecked(
                (-20_037_508.342_789_244, -20_048_966.104_014_6).into(),
                (20_037_508.342_789_244, 20_048_966.104_014_594).into()
            ),
            epsilon = 0.000_001
        ));
        assert_eq!(
            spec.axis_labels,
            Some(("Easting".to_owned(), "Northing".to_owned()).into())
        );
        assert_eq!(spec.axis_order, Some(AxisOrder::EastNorth));
    }

    #[test]
    fn spec_wgs84() {
        let spec = spatial_reference_specification("EPSG:4326").unwrap();
        assert_eq!(
            SpatialReferenceSpecification {
                name: "WGS 84".to_owned(),
                spatial_reference: SpatialReference::epsg_4326().into(),
                proj_string: "+proj=longlat +datum=WGS84 +no_defs +type=crs".to_owned(),
                extent: BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into())
                    .into(),
                axis_labels: Some(
                    (
                        "Geodetic longitude".to_owned(),
                        "Geodetic latitude".to_owned()
                    )
                        .into()
                ),
                axis_order: Some(AxisOrder::NorthEast),
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
                spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632)
                    .into(),
                proj_string: "+proj=utm +zone=32 +datum=WGS84 +units=m +no_defs +type=crs".into(),
                extent: BoundingBox2D::new_unchecked(
                    (166_021.443_080_539_64, 0.0).into(),
                    (833_978.556_919_460_4, 9_329_005.182_447_437).into()
                )
                .into(),
                axis_labels: Some(("Easting".to_owned(), "Northing".to_owned()).into()),
                axis_order: Some(AxisOrder::EastNorth),
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
                spatial_reference: SpatialReference::new(SpatialReferenceAuthority::SrOrg, 81)
                    .into(),
                proj_string:
                    "+proj=geos +lon_0=0 +h=-0 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs".into(),
                extent: BoundingBox2D::new_unchecked(
                    (-5_568_748.276, -5_568_748.276).into(),
                    (5_568_748.276, 5_568_748.276).into()
                )
                .into(),
                axis_labels: None,
                axis_order: Some(AxisOrder::EastNorth),
            },
            spec
        );
    }
}
