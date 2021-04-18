use chrono::FixedOffset;
use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D, TimeInterval};
use serde::de::Error;
use serde::Deserialize;
use std::str::FromStr;

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
    parse_time(deserializer).map(Some)
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
        _ => Err(D::Error::custom("Invalid time")),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
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
}
