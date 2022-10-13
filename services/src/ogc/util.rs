use actix_web::guard::{Guard, GuardContext};
use geoengine_datatypes::primitives::{AxisAlignedRectangle, BoundingBox2D, DateTime};
use geoengine_datatypes::primitives::{Coordinate2D, SpatialResolution};
use geoengine_datatypes::spatial_reference::SpatialReference;
use reqwest::Url;
use serde::de::Error;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use std::str::FromStr;
use utoipa::openapi::{ObjectBuilder, SchemaType};
use utoipa::ToSchema;

use super::wcs::request::WcsBoundingbox;
use crate::api::model::datatypes::TimeInterval;
use crate::error::{self, Result};
use crate::handlers::spatial_references::{spatial_reference_specification, AxisOrder};
use crate::workflows::workflow::WorkflowId;

#[derive(PartialEq, Debug, Deserialize, Serialize, Clone, Copy)]
pub struct OgcBoundingBox {
    values: [f64; 4],
}

impl ToSchema for OgcBoundingBox {
    fn schema() -> utoipa::openapi::schema::Schema {
        ObjectBuilder::new().schema_type(SchemaType::String).into()
    }
}

impl OgcBoundingBox {
    pub fn new(a: f64, b: f64, c: f64, d: f64) -> Self {
        Self {
            values: [a, b, c, d],
        }
    }

    pub fn bounds<A: AxisAlignedRectangle>(self, spatial_reference: SpatialReference) -> Result<A> {
        rectangle_from_ogc_params(self.values, spatial_reference)
    }
}

/// Parse bbox, format is: "x1,y1,x2,y2"
pub fn parse_bbox<'de, D>(deserializer: D) -> Result<BoundingBox2D, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    let split: Vec<Result<f64, std::num::ParseFloatError>> = s.split(',').map(str::parse).collect();

    if let [Ok(x1), Ok(y1), Ok(x2), Ok(y2)] = *split.as_slice() {
        BoundingBox2D::new(Coordinate2D::new(x1, y1), Coordinate2D::new(x2, y2))
            .map_err(D::Error::custom)
    } else {
        Err(D::Error::custom("Invalid bbox"))
    }
}

/// Parse bbox, format is: "x1,y1,x2,y2"
pub fn parse_ogc_bbox<'de, D>(deserializer: D) -> Result<OgcBoundingBox, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    let split: Vec<Result<f64, std::num::ParseFloatError>> = s.split(',').map(str::parse).collect();

    if let [Ok(x1), Ok(y1), Ok(x2), Ok(y2)] = *split.as_slice() {
        Ok(OgcBoundingBox {
            values: [x1, y1, x2, y2],
        })
    } else {
        Err(D::Error::custom("Invalid bbox"))
    }
}

/// Parse the time string of a WMS request
/// time is specified in ISO8601, it can either be an instant (single datetime) or an interval
/// An interval is separated by "/". "Either the start value or the end value can be omitted to
/// indicate no restriction on time in that direction."
/// sources: - <http://docs.geoserver.org/2.8.x/en/user/services/wms/time.html#wms-time>
///          - <http://www.ogcnetwork.net/node/178>
pub fn parse_time_option<'de, D>(deserializer: D) -> Result<Option<TimeInterval>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    if s.is_empty() {
        return Ok(None);
    }

    parse_time_from_str::<D>(&s).map(Some)
}

/// Parse the time string of a WMS request
/// time is specified in ISO8601, it can either be an instant (single datetime) or an interval
/// An interval is separated by "/". "Either the start value or the end value can be omitted to
/// indicate no restriction on time in that direction."
/// sources: - <http://docs.geoserver.org/2.8.x/en/user/services/wms/time.html#wms-time>
///          - <http://www.ogcnetwork.net/node/178>
pub fn parse_time<'de, D>(deserializer: D) -> Result<TimeInterval, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // TODO: support relative time intervals

    let s = String::deserialize(deserializer)?;
    parse_time_from_str::<D>(&s)
}

fn parse_time_from_str<'de, D>(s: &str) -> Result<TimeInterval, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let split: Vec<_> = s.split('/').map(DateTime::from_str).collect();

    match *split.as_slice() {
        [Ok(time)] => geoengine_datatypes::primitives::TimeInterval::new(time, time)
            .map(Into::into)
            .map_err(D::Error::custom),
        [Ok(start), Ok(end)] => geoengine_datatypes::primitives::TimeInterval::new(start, end)
            .map(Into::into)
            .map_err(D::Error::custom),
        _ => Err(D::Error::custom(format!("Invalid time {}", s))),
    }
}

/// Parse a spatial resolution, format is: "resolution" or "xResolution,yResolution"
pub fn parse_spatial_resolution_option<'de, D>(
    deserializer: D,
) -> Result<Option<SpatialResolution>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    if s.is_empty() {
        return Ok(None);
    }

    let split: Vec<Result<f64, std::num::ParseFloatError>> = s.split(',').map(str::parse).collect();

    let spatial_resolution = match *split.as_slice() {
        [Ok(resolution)] => {
            SpatialResolution::new(resolution, resolution).map_err(D::Error::custom)?
        }
        [Ok(x_resolution), Ok(y_resolution)] => {
            SpatialResolution::new(x_resolution, y_resolution).map_err(D::Error::custom)?
        }
        _ => return Err(D::Error::custom("Invalid spatial resolution")),
    };

    Ok(Some(spatial_resolution))
}

/// Parse wcs 1.1.1 bbox, format is: "x1,y1,x2,y2,crs", crs format is like `urn:ogc:def:crs:EPSG::4326`
pub fn parse_wcs_bbox<'de, D>(deserializer: D) -> Result<WcsBoundingbox, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    let (bbox_str, crs_str) = match s.matches(',').count() {
        3 => (s.as_str(), None),
        4 => {
            let idx = s.rfind(',').expect("there is at least one ','");
            (&s[0..idx], Some(&s[idx + 1..s.len()]))
        }
        _ => {
            return Err(D::Error::custom(&format!(
                "cannot parse bbox from string: {}",
                s
            )))
        }
    };

    // TODO: more sophisticated crs parsing
    let spatial_reference = if let Some(crs_str) = crs_str {
        if let Some(crs) = crs_str.strip_prefix("urn:ogc:def:crs:") {
            Some(SpatialReference::from_str(&crs.replace("::", ":")).map_err(D::Error::custom)?)
        } else {
            return Err(D::Error::custom(&format!(
                "cannot parse crs from string: {}",
                crs_str
            )));
        }
    } else {
        None
    };

    let split: Vec<Result<f64, std::num::ParseFloatError>> =
        bbox_str.split(',').map(str::parse).collect();

    let bbox = if let [Ok(a), Ok(b), Ok(c), Ok(d)] = *split.as_slice() {
        [a, b, c, d]
    } else {
        return Err(D::Error::custom("Invalid bbox"));
    };

    Ok(WcsBoundingbox {
        bbox,
        spatial_reference,
    })
}

/// parse wcs 1.1.1, format is like `urn:ogc:def:crs:EPSG::4326`
pub fn parse_wcs_crs<'de, D>(deserializer: D) -> Result<SpatialReference, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    if let Some(crs) = s.strip_prefix("urn:ogc:def:crs:") {
        SpatialReference::from_str(&crs.replace("::", ":")).map_err(D::Error::custom)
    } else {
        Err(D::Error::custom("cannot parse crs"))
    }
}

/// parse coordinate, format is "x,y"
pub fn parse_coordinate<'de, D>(deserializer: D) -> Result<Coordinate2D, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    let split: Vec<Result<f64, std::num::ParseFloatError>> = s.split(',').map(str::parse).collect();

    match *split.as_slice() {
        [Ok(x), Ok(y)] => Ok(Coordinate2D::new(x, y)),
        _ => Err(D::Error::custom("Invalid coordinate")),
    }
}

/// create an axis aligned rectangle using the values "a,b,c,d" from OGC bbox-like parameters using the axis ordering for `spatial_reference`
pub fn rectangle_from_ogc_params<A: AxisAlignedRectangle>(
    values: [f64; 4],
    spatial_reference: SpatialReference,
) -> Result<A> {
    let [a, b, c, d] = values;
    match spatial_reference_specification(&spatial_reference.proj_string()?)?
        .axis_order
        .ok_or(error::Error::AxisOrderingNotKnownForSrs {
            srs_string: spatial_reference.srs_string(),
        })? {
        AxisOrder::EastNorth => {
            A::from_min_max((a, b).into(), (c, d).into()).context(error::DataType)
        }
        AxisOrder::NorthEast => {
            A::from_min_max((b, a).into(), (d, c).into()).context(error::DataType)
        }
    }
}

/// reorders the given tuple of coordinates, resolutions, etc. using the axis ordering for `spatial_reference` to give (x, y)
pub fn tuple_from_ogc_params(
    a: f64,
    b: f64,
    spatial_reference: SpatialReference,
) -> Result<(f64, f64)> {
    match spatial_reference_specification(&spatial_reference.proj_string()?)?
        .axis_order
        .ok_or(error::Error::AxisOrderingNotKnownForSrs {
            srs_string: spatial_reference.srs_string(),
        })? {
        AxisOrder::EastNorth => Ok((a, b)),
        AxisOrder::NorthEast => Ok((b, a)),
    }
}

#[derive(Clone, Copy)]
pub enum OgcProtocol {
    Wcs,
    Wms,
    Wfs,
}

impl OgcProtocol {
    fn url_path(self) -> &'static str {
        match self {
            OgcProtocol::Wcs => "wcs/",
            OgcProtocol::Wms => "wms/",
            OgcProtocol::Wfs => "wfs/",
        }
    }
}

pub fn ogc_endpoint_url(base: &Url, protocol: OgcProtocol, workflow: WorkflowId) -> Result<Url> {
    ensure!(base.path().ends_with('/'), error::BaseUrlMustEndWithSlash);

    base.join(protocol.url_path())?
        .join(&workflow.to_string())
        .map_err(Into::into)
}

pub struct OgcRequestGuard<'a> {
    request: &'a str,
}

impl<'a> OgcRequestGuard<'a> {
    pub fn new(request: &'a str) -> Self {
        Self { request }
    }
}

impl Guard for OgcRequestGuard<'_> {
    fn check(&self, ctx: &GuardContext<'_>) -> bool {
        ctx.head().uri.query().map_or(false, |q| {
            q.contains(&format!("request={}", self.request))
                || q.contains(&format!("REQUEST={}", self.request))
        })
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::spatial_reference::SpatialReferenceAuthority;
    use serde::de::value::StringDeserializer;
    use serde::de::IntoDeserializer;

    use super::*;

    #[test]
    fn parse_time_normal() {
        assert_eq!(
            parse_time(to_deserializer("1970-01-02T09:10:11.012+00:00")).unwrap(),
            geoengine_datatypes::primitives::TimeInterval::new_instant(
                DateTime::new_utc_with_millis(1970, 1, 2, 9, 10, 11, 12)
            )
            .unwrap()
            .into(),
        );
        assert_eq!(
            parse_time(to_deserializer("2020-12-31T23:59:59.999Z")).unwrap(),
            geoengine_datatypes::primitives::TimeInterval::new_instant(
                DateTime::new_utc_with_millis(2020, 12, 31, 23, 59, 59, 999)
            )
            .unwrap()
            .into(),
        );

        assert_eq!(
            parse_time(to_deserializer(
                "2019-01-01T00:00:00.000Z/2019-12-31T23:59:59.999Z"
            ))
            .unwrap(),
            geoengine_datatypes::primitives::TimeInterval::new(
                DateTime::new_utc_with_millis(2019, 1, 1, 0, 0, 0, 0),
                DateTime::new_utc_with_millis(2019, 12, 31, 23, 59, 59, 999)
            )
            .unwrap()
            .into(),
        );

        assert_eq!(
            parse_time(to_deserializer(
                "2019-01-01T00:00:00.000Z/2019-01-01T00:00:00.000Z"
            ))
            .unwrap(),
            geoengine_datatypes::primitives::TimeInterval::new_instant(
                DateTime::new_utc_with_millis(2019, 1, 1, 0, 0, 0, 0)
            )
            .unwrap()
            .into(),
        );

        assert_eq!(
            parse_time(to_deserializer("2014-04-01T12:00:00.000+00:00")).unwrap(),
            geoengine_datatypes::primitives::TimeInterval::new_instant(
                DateTime::new_utc_with_millis(2014, 4, 1, 12, 0, 0, 0)
            )
            .unwrap()
            .into(),
        );
    }

    #[test]
    fn parse_time_medieval() {
        assert_eq!(
            parse_time(to_deserializer("600-01-02T09:10:11.012+00:00")).unwrap(),
            geoengine_datatypes::primitives::TimeInterval::new_instant(
                DateTime::new_utc_with_millis(600, 1, 2, 9, 10, 11, 12)
            )
            .unwrap()
            .into(),
        );
        assert_eq!(
            parse_time(to_deserializer("600-01-02T09:10:11.012Z")).unwrap(),
            geoengine_datatypes::primitives::TimeInterval::new_instant(
                DateTime::new_utc_with_millis(600, 1, 2, 9, 10, 11, 12)
            )
            .unwrap()
            .into(),
        );
    }

    #[test]
    fn parse_time_bc() {
        assert_eq!(
            parse_time(to_deserializer("-600-01-02T09:10:11.012+00:00")).unwrap(),
            geoengine_datatypes::primitives::TimeInterval::new_instant(
                DateTime::new_utc_with_millis(-600, 1, 2, 9, 10, 11, 12)
            )
            .unwrap()
            .into(),
        );
        assert_eq!(
            parse_time(to_deserializer("-0600-01-02T09:10:11.012+00:00")).unwrap(),
            geoengine_datatypes::primitives::TimeInterval::new_instant(
                DateTime::new_utc_with_millis(-600, 1, 2, 9, 10, 11, 12)
            )
            .unwrap()
            .into(),
        );
        assert_eq!(
            parse_time(to_deserializer("-00600-01-02T09:10:11.012+00:00")).unwrap(),
            geoengine_datatypes::primitives::TimeInterval::new_instant(
                DateTime::new_utc_with_millis(-600, 1, 2, 9, 10, 11, 12)
            )
            .unwrap()
            .into(),
        );
        assert_eq!(
            parse_time(to_deserializer("-00600-01-02T09:10:11.0Z")).unwrap(),
            geoengine_datatypes::primitives::TimeInterval::new_instant(
                DateTime::new_utc_with_millis(-600, 1, 2, 9, 10, 11, 0)
            )
            .unwrap()
            .into(),
        );
    }

    #[test]
    fn parse_time_with_offset() {
        assert_eq!(
            parse_time(to_deserializer("-00600-01-02T09:10:11.0+01:00")).unwrap(),
            geoengine_datatypes::primitives::TimeInterval::new_instant(
                DateTime::new_utc_with_millis(-600, 1, 2, 8, 10, 11, 0)
            )
            .unwrap()
            .into(),
        );
    }

    fn to_deserializer(s: &str) -> StringDeserializer<serde::de::value::Error> {
        s.to_owned().into_deserializer()
    }

    #[test]
    fn it_parses_spatial_resolution_options() {
        assert_eq!(
            parse_spatial_resolution_option(to_deserializer("")).unwrap(),
            None
        );

        assert_eq!(
            parse_spatial_resolution_option(to_deserializer("0.1")).unwrap(),
            Some(SpatialResolution::zero_point_one())
        );
        assert_eq!(
            parse_spatial_resolution_option(to_deserializer("1")).unwrap(),
            Some(SpatialResolution::one())
        );

        assert_eq!(
            parse_spatial_resolution_option(to_deserializer("0.1,0.2")).unwrap(),
            Some(SpatialResolution::new(0.1, 0.2).unwrap())
        );

        assert!(parse_spatial_resolution_option(to_deserializer(",")).is_err());
        assert!(parse_spatial_resolution_option(to_deserializer("0.1,0.2,0.3")).is_err());
    }

    #[test]
    fn it_parses_wcs_bbox() {
        let s = "-162,-81,162,81,urn:ogc:def:crs:EPSG::3857";

        assert_eq!(
            parse_wcs_bbox(to_deserializer(s)).unwrap(),
            WcsBoundingbox {
                bbox: [-162., -81., 162., 81.],
                spatial_reference: Some(SpatialReference::new(
                    SpatialReferenceAuthority::Epsg,
                    3857
                )),
            }
        );
    }

    #[test]
    fn it_parses_wcs_crs() {
        let s = "urn:ogc:def:crs:EPSG::4326";

        assert_eq!(
            parse_wcs_crs(to_deserializer(s)).unwrap(),
            SpatialReference::epsg_4326(),
        );
    }

    #[test]
    fn it_parses_coordinate() {
        let s = "1.1,2.2";

        assert_eq!(
            parse_coordinate(to_deserializer(s)).unwrap(),
            Coordinate2D::new(1.1, 2.2)
        );
    }

    #[test]
    fn it_builds_correct_endpoint_urls() {
        assert_eq!(
            ogc_endpoint_url(
                &Url::parse("http://example.com/").unwrap(),
                OgcProtocol::Wms,
                WorkflowId::from_str("b9a7b1a0-efd6-4de9-9973-c3aeaf9282bd").unwrap(),
            )
            .unwrap(),
            Url::parse("http://example.com/wms/b9a7b1a0-efd6-4de9-9973-c3aeaf9282bd").unwrap()
        );

        assert_eq!(
            ogc_endpoint_url(
                &Url::parse("http://example.com/a/").unwrap(),
                OgcProtocol::Wms,
                WorkflowId::from_str("b9a7b1a0-efd6-4de9-9973-c3aeaf9282bd").unwrap(),
            )
            .unwrap(),
            Url::parse("http://example.com/a/wms/b9a7b1a0-efd6-4de9-9973-c3aeaf9282bd").unwrap()
        );

        assert_eq!(
            ogc_endpoint_url(
                &Url::parse("http://example.com/a/sub/folder/").unwrap(),
                OgcProtocol::Wms,
                WorkflowId::from_str("b9a7b1a0-efd6-4de9-9973-c3aeaf9282bd").unwrap(),
            )
            .unwrap(),
            Url::parse("http://example.com/a/sub/folder/wms/b9a7b1a0-efd6-4de9-9973-c3aeaf9282bd")
                .unwrap()
        );
    }
}
