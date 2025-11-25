use crate::api::model::datatypes::SpatialReference;
use actix_web::guard::{Guard, GuardContext};
use actix_web::{FromRequest, HttpRequest, dev::Payload};
use futures_util::future::{Ready, ready};
use geoengine_datatypes::primitives::Coordinate2D;
use geoengine_datatypes::primitives::{AxisAlignedRectangle, BoundingBox2D, DateTime};
use reqwest::Url;
use serde::de::DeserializeOwned;
use serde::de::Error;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};
use std::str::FromStr;
use utoipa::openapi::schema::{ObjectBuilder, SchemaType};
use utoipa::{PartialSchema, ToSchema};

use super::wcs::request::WcsBoundingbox;
use crate::api::handlers::spatial_references::{AxisOrder, spatial_reference_specification};
use crate::api::model::datatypes::TimeInterval;
use crate::error::{self, Result, UnableToParseQueryString, UnableToSerializeQueryString};
use crate::workflows::workflow::WorkflowId;

#[derive(PartialEq, Debug, Deserialize, Serialize, Clone, Copy)]
pub struct OgcBoundingBox {
    values: [f64; 4],
}

impl PartialSchema for OgcBoundingBox {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        ObjectBuilder::new()
            .schema_type(SchemaType::Type(utoipa::openapi::Type::String))
            .into()
    }
}

impl ToSchema for OgcBoundingBox {}

impl OgcBoundingBox {
    pub fn new(a: f64, b: f64, c: f64, d: f64) -> Self {
        Self {
            values: [a, b, c, d],
        }
    }

    pub fn bounds<A: AxisAlignedRectangle>(self, spatial_reference: SpatialReference) -> Result<A> {
        rectangle_from_ogc_params(self.values, spatial_reference)
    }

    pub fn bounds_naive<A: AxisAlignedRectangle>(self) -> Result<A> {
        A::from_min_max(
            (self.values[0], self.values[1]).into(),
            (self.values[2], self.values[3]).into(),
        )
        .map_err(Into::into)
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
        _ => Err(D::Error::custom(format!("Invalid time {s}"))),
    }
}

/// Parse wcs 1.1.1 bbox, format is: "x1,y1,x2,y2,crs", crs format is like `urn:ogc:def:crs:EPSG::4326`
#[allow(clippy::missing_panics_doc)]
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
            return Err(D::Error::custom(format!(
                "cannot parse bbox from string: {s}"
            )));
        }
    };

    // TODO: more sophisticated crs parsing
    let spatial_reference = if let Some(crs_str) = crs_str {
        if let Some(crs) = crs_str.strip_prefix("urn:ogc:def:crs:") {
            Some(SpatialReference::from_str(&crs.replace("::", ":")).map_err(D::Error::custom)?)
        } else {
            return Err(D::Error::custom(format!(
                "cannot parse crs from string: {crs_str}"
            )));
        }
    } else {
        None
    };

    let split: Vec<Result<f64, std::num::ParseFloatError>> =
        bbox_str.split(',').map(str::parse).collect();

    let [Ok(x1), Ok(y1), Ok(x2), Ok(y2)] = *split.as_slice() else {
        return Err(D::Error::custom("Invalid bbox"));
    };

    Ok(WcsBoundingbox {
        bbox: [x1, y1, x2, y2],
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
    let axis_order = spatial_reference_specification(spatial_reference)?
        .axis_order
        .ok_or(error::Error::AxisOrderingNotKnownForSrs {
            srs_string: spatial_reference.srs_string(),
        })?;
    match axis_order {
        AxisOrder::EastNorth => A::from_min_max((a, b).into(), (c, d).into()).map_err(Into::into),
        AxisOrder::NorthEast => A::from_min_max((b, a).into(), (d, c).into()).map_err(Into::into),
    }
}

/// reorders the given tuple of coordinates, resolutions, etc. using the axis ordering for `spatial_reference` to give (x, y)
pub fn tuple_from_ogc_params(
    a: f64,
    b: f64,
    spatial_reference: SpatialReference,
) -> Result<(f64, f64)> {
    match spatial_reference_specification(spatial_reference)?
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

// TODO: remove when all OGC handlers are using only one handler
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
        ctx.head().uri.query().is_some_and(|q| {
            q.contains(&format!("request={}", self.request))
                || q.contains(&format!("REQUEST={}", self.request))
        })
    }
}

/// a special query extractor for actix-web because some OGC clients use varying casing for query parameter keys
pub struct OgcQueryExtractor<T>(pub T);

impl<T> OgcQueryExtractor<T> {
    #[inline]
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> std::ops::Deref for OgcQueryExtractor<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for OgcQueryExtractor<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> FromRequest for OgcQueryExtractor<T>
where
    T: DeserializeOwned + 'static,
{
    type Error = crate::error::Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        // get raw query string
        let qs = req.query_string();

        // parse into key-value pairs
        let params: Result<Vec<(String, String)>> =
            serde_urlencoded::from_str(qs).context(UnableToParseQueryString);

        let res = params
            .and_then(|pairs| {
                // normalize keys to lowercase
                let normalized: Vec<(String, String)> = pairs
                    .into_iter()
                    .map(|(k, v)| (k.to_lowercase(), v))
                    .collect();

                // re-serialize then deserialize into target type T
                let new_qs = serde_urlencoded::to_string(&normalized)
                    .context(UnableToSerializeQueryString)?;
                let value =
                    serde_urlencoded::from_str::<T>(&new_qs).context(UnableToParseQueryString)?;
                Ok(value)
            })
            .map(OgcQueryExtractor);

        ready(res)
    }
}

#[cfg(test)]
mod tests {
    use crate::api::model::datatypes::SpatialReferenceAuthority;
    use serde::de::IntoDeserializer;
    use serde::de::value::StringDeserializer;

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
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 4326),
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

    #[test]
    fn ogc_query_extractor_normalizes_keys() {
        use actix_web::test;
        use serde::Deserialize;

        #[derive(Debug, Deserialize, PartialEq)]
        struct TestQuery {
            foo: String,
            bar: String,
        }

        let req = test::TestRequest::with_uri("/test?FOO=hello&BaR=world").to_http_request();

        let mut payload = actix_web::dev::Payload::None;
        let fut = OgcQueryExtractor::<TestQuery>::from_request(&req, &mut payload);
        let result = futures::executor::block_on(fut).unwrap();

        assert_eq!(result.foo, "hello");
        assert_eq!(result.bar, "world");
    }
}
