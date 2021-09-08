use chrono::FixedOffset;
use geoengine_datatypes::primitives::{AxisAlignedRectangle, BoundingBox2D};
use geoengine_datatypes::primitives::{Coordinate2D, SpatialResolution, TimeInterval};
use geoengine_datatypes::spatial_reference::SpatialReference;
use serde::de::Error;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::str::FromStr;

use super::wcs::request::WcsBoundingbox;
use crate::error::{self, Result};

#[derive(PartialEq, Debug, Deserialize, Serialize, Clone, Copy)]
pub struct OgcBoundingBox {
    values: [f64; 4],
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
    let split: Vec<_> = s
        .split('/')
        // use `from_str` instead of `parse_from_rfc3339` to use a relaxed form of RFC3339 that supports dates BC
        .map(chrono::DateTime::<FixedOffset>::from_str)
        .collect();

    match *split.as_slice() {
        [Ok(time)] => TimeInterval::new(time.timestamp_millis(), time.timestamp_millis())
            .map_err(D::Error::custom),
        [Ok(start), Ok(end)] => TimeInterval::new(start.timestamp_millis(), end.timestamp_millis())
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
    // TODO: properly handle axis order
    if spatial_reference == SpatialReference::epsg_4326() {
        A::from_min_max((b, a).into(), (d, c).into()).context(error::DataType)
    } else {
        A::from_min_max((a, b).into(), (c, d).into()).context(error::DataType)
    }
}

/// reorders the given tuple of coordinates, resolutions, etc. using the axis ordering for `spatial_reference` to give (x, y)
pub fn tuple_from_ogc_params(a: f64, b: f64, spatial_reference: SpatialReference) -> (f64, f64) {
    // TODO: properly handle axis order
    if spatial_reference == SpatialReference::epsg_4326() {
        (b, a)
    } else {
        (a, b)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use geoengine_datatypes::spatial_reference::SpatialReferenceAuthority;
    use serde::de::value::StringDeserializer;
    use serde::de::IntoDeserializer;

    use super::*;

    #[test]
    fn parse_time_normal() {
        assert_eq!(
            TimeInterval::new_instant(Utc.ymd(1970, 1, 2).and_hms_milli(9, 10, 11, 12)).unwrap(),
            parse_time(to_deserializer("1970-01-02T09:10:11.012+00:00")).unwrap()
        );
        assert_eq!(
            TimeInterval::new_instant(Utc.ymd(2020, 12, 31).and_hms_milli(23, 59, 59, 999))
                .unwrap(),
            parse_time(to_deserializer("2020-12-31T23:59:59.999Z")).unwrap()
        );

        assert_eq!(
            TimeInterval::new(
                Utc.ymd(2019, 1, 1).and_hms_milli(0, 0, 0, 0),
                Utc.ymd(2019, 12, 31).and_hms_milli(23, 59, 59, 999)
            )
            .unwrap(),
            parse_time(to_deserializer(
                "2019-01-01T00:00:00.000Z/2019-12-31T23:59:59.999Z"
            ))
            .unwrap()
        );

        assert_eq!(
            TimeInterval::new_instant(Utc.ymd(2019, 1, 1).and_hms_milli(0, 0, 0, 0)).unwrap(),
            parse_time(to_deserializer(
                "2019-01-01T00:00:00.000Z/2019-01-01T00:00:00.000Z"
            ))
            .unwrap()
        );

        assert_eq!(
            TimeInterval::new_instant(Utc.ymd(2014, 4, 1).and_hms_milli(12, 0, 0, 0)).unwrap(),
            parse_time(to_deserializer("2014-04-01T12:00:00.000+00:00")).unwrap()
        );
    }

    #[test]
    fn parse_time_medieval() {
        assert_eq!(
            TimeInterval::new_instant(Utc.ymd(600, 1, 2).and_hms_milli(9, 10, 11, 12)).unwrap(),
            parse_time(to_deserializer("600-01-02T09:10:11.012+00:00")).unwrap()
        );
        assert_eq!(
            TimeInterval::new_instant(Utc.ymd(600, 1, 2).and_hms_milli(9, 10, 11, 12)).unwrap(),
            parse_time(to_deserializer("600-01-02T09:10:11.012Z")).unwrap()
        );
    }

    #[test]
    fn parse_time_bc() {
        assert_eq!(
            TimeInterval::new_instant(Utc.ymd(-600, 1, 2).and_hms_milli(9, 10, 11, 12)).unwrap(),
            parse_time(to_deserializer("-600-01-02T09:10:11.012+00:00")).unwrap()
        );
        assert_eq!(
            TimeInterval::new_instant(Utc.ymd(-600, 1, 2).and_hms_milli(9, 10, 11, 12)).unwrap(),
            parse_time(to_deserializer("-0600-01-02T09:10:11.012+00:00")).unwrap()
        );
        assert_eq!(
            TimeInterval::new_instant(Utc.ymd(-600, 1, 2).and_hms_milli(9, 10, 11, 12)).unwrap(),
            parse_time(to_deserializer("-00600-01-02T09:10:11.012+00:00")).unwrap()
        );
        assert_eq!(
            TimeInterval::new_instant(Utc.ymd(-600, 1, 2).and_hms_milli(9, 10, 11, 0)).unwrap(),
            parse_time(to_deserializer("-00600-01-02T09:10:11.0Z")).unwrap()
        );
    }

    #[test]
    fn parse_time_with_offset() {
        assert_eq!(
            TimeInterval::new_instant(Utc.ymd(-600, 1, 2).and_hms_milli(8, 10, 11, 0)).unwrap(),
            parse_time(to_deserializer("-00600-01-02T09:10:11.0+01:00")).unwrap()
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
}
