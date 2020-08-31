use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D, TimeInterval};
use serde::de::Error;

/// Parse bbox, format is: "x1,y1,x2,y2"
pub fn parse_bbox<'de, D>(deserializer: D) -> Result<BoundingBox2D, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = <&str as serde::Deserialize>::deserialize(deserializer)?;

    let split: Vec<Result<f64, std::num::ParseFloatError>> = s.split(',').map(str::parse).collect();

    if let [Ok(x1), Ok(y1), Ok(x2), Ok(y2)] = *split.as_slice() {
        BoundingBox2D::new(Coordinate2D::new(x1, y1), Coordinate2D::new(x2, y2))
            .map_err(|_| D::Error::custom("Invalid bbox"))
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
pub fn parse_time<'de, D>(deserializer: D) -> Result<Option<TimeInterval>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // TODO: support relative time intervals

    let s = <&str as serde::Deserialize>::deserialize(deserializer)?;

    let split: Vec<_> = s
        .split('/')
        .map(|s| chrono::DateTime::parse_from_rfc3339(s))
        .collect();

    match *split.as_slice() {
        [Ok(time)] => TimeInterval::new(time.timestamp(), time.timestamp())
            .map(Some)
            .map_err(|_| D::Error::custom("Invalid time")),
        [Ok(start), Ok(end)] => TimeInterval::new(start.timestamp(), end.timestamp())
            .map(Some)
            .map_err(|_| D::Error::custom("Invalid time")),
        _ => Err(D::Error::custom("Invalid time")),
    }
}
