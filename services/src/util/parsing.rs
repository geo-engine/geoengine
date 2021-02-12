use geoengine_datatypes::primitives::SpatialResolution;
use serde::de::Error;
use serde::Deserialize;
use std::str::FromStr;

/// Parse the `SpatialResolution` of a request by parsing `x,y`, e.g. `0.1,0.1`.
pub fn parse_spatial_resolution<'de, D>(deserializer: D) -> Result<SpatialResolution, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    let split: Result<Vec<f64>, <f64 as FromStr>::Err> = s.split(',').map(f64::from_str).collect();

    match split.as_ref().map(Vec::as_slice) {
        Ok(&[x, y]) => SpatialResolution::new(x, y).map_err(D::Error::custom),
        Err(error) => Err(D::Error::custom(error)),
        Ok(..) => Err(D::Error::custom("Invalid spatial resolution")),
    }
}
