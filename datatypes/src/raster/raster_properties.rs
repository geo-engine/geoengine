use crate::error::Error;
use crate::util::Result;

use num_traits::FromPrimitive;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::fmt::{Display, Formatter};

/// This scruct stores metadata about a raster tile.
/// The metadata is stored in a HashMap except for the `scale`, `offset` and `band_name` properties.
/// The `scale` and `offset` properties used to indicate the values the data is scaled with.
/// Scale and unscale are used in the way 'gdal_transle' does. See https://gdal.org/programs/gdal_translate.html for more information.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct RasterProperties {
    pub scale: Option<f64>,
    pub offset: Option<f64>,
    pub band_name: Option<String>,
    pub properties_map: HashMap<RasterPropertiesKey, RasterPropertiesEntry>,
}

impl RasterProperties {
    pub fn domains(&self) -> impl Iterator<Item = Option<&String>> {
        self.properties_map.keys().map(|m| m.domain.as_ref())
    }

    pub fn set_scale(&mut self, scale: f64) {
        self.scale = Some(scale);
    }

    pub fn set_offset(&mut self, offset: f64) {
        self.offset = Some(offset);
    }

    pub fn set_band_name(&mut self, band_name: String) {
        self.band_name = Some(band_name);
    }

    pub fn insert_property(
        &mut self,
        key: RasterPropertiesKey,
        value: RasterPropertiesEntry,
    ) -> Option<RasterPropertiesEntry> {
        self.properties_map.insert(key, value)
    }

    pub fn get_property(&self, key: &RasterPropertiesKey) -> Option<&RasterPropertiesEntry> {
        self.properties_map.get(key)
    }

    pub fn number_property<T: Copy + FromPrimitive>(&self, key: &RasterPropertiesKey) -> Result<T> {
        if key.domain.is_none() && key.key == "scale" {
            return self
                .scale
                .and_then(|s| T::from_f64(s))
                .ok_or(Error::MissingRasterProperty {
                    property: key.to_string(),
                });
        }

        if key.domain.is_none() && key.key == "offset" {
            return self
                .offset
                .and_then(|s| T::from_f64(s))
                .ok_or(Error::MissingRasterProperty {
                    property: key.to_string(),
                });
        }

        let val = f64::try_from(
            self.get_property(key)
                .ok_or(Error::MissingRasterProperty {
                    property: key.to_string(),
                })?
                .clone(),
        )?;
        T::from_f64(val).ok_or(Error::WrongMetadataType)
    }

    pub fn string_property(&self, key: &RasterPropertiesKey) -> Result<String> {
        if key.domain.is_none() && key.key == "band_name" {
            return self.band_name.clone().ok_or(Error::MissingRasterProperty {
                property: key.to_string(),
            });
        }

        let s = self
            .get_property(key)
            .ok_or(Error::MissingRasterProperty {
                property: key.to_string(),
            })?
            .to_string();
        Ok(s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash, Eq, PartialOrd, Ord)]
pub struct RasterPropertiesKey {
    pub domain: Option<String>,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum RasterPropertiesEntry {
    Number(f64),
    String(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RasterPropertiesEntryType {
    Number,
    String,
}

impl From<RasterPropertiesEntry> for String {
    fn from(prop: RasterPropertiesEntry) -> String {
        match prop {
            RasterPropertiesEntry::Number(n) => n.to_string(),
            RasterPropertiesEntry::String(s) => s,
        }
    }
}

impl TryFrom<RasterPropertiesEntry> for f64 {
    fn try_from(prop: RasterPropertiesEntry) -> Result<f64> {
        match prop {
            RasterPropertiesEntry::Number(n) => Ok(n),
            RasterPropertiesEntry::String(s) => s.parse().map_err(|_| Error::WrongMetadataType),
        }
    }

    type Error = crate::error::Error;
}

impl Display for RasterPropertiesKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.domain {
            Some(prefix) => write!(f, "{}.{}", prefix, &self.key),
            None => write!(f, "{}", &self.key),
        }
    }
}

impl Display for RasterPropertiesEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            RasterPropertiesEntry::Number(n) => write!(f, "{}", n),
            RasterPropertiesEntry::String(s) => write!(f, "{}", s),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error;
    use crate::raster::{RasterProperties, RasterPropertiesEntry, RasterPropertiesKey};

    #[test]
    fn property_numeric_not_found() {
        let props = RasterProperties::default();
        let key = RasterPropertiesKey {
            domain: Some("test".into()),
            key: "key".into(),
        };

        match props.number_property::<u8>(&key) {
            Err(Error::MissingRasterProperty { property: k }) if k == key.to_string() => (),
            _ => panic!("Expected missing property error"),
        }
    }

    #[test]
    fn property_numeric_parse_error() {
        let mut props = RasterProperties::default();
        let key = RasterPropertiesKey {
            domain: Some("test".into()),
            key: "key".into(),
        };

        props
            .properties_map
            .insert(key.clone(), RasterPropertiesEntry::String("test".into()));

        match props.number_property::<u8>(&key) {
            Err(Error::WrongMetadataType) => (),
            _ => panic!("Expected parse error"),
        }
    }

    #[test]
    fn property_numeric_ok() {
        let mut props = RasterProperties::default();
        let key = RasterPropertiesKey {
            domain: Some("test".into()),
            key: "key".into(),
        };

        props
            .properties_map
            .insert(key.clone(), RasterPropertiesEntry::Number(42.3));

        match props.number_property::<u8>(&key) {
            Ok(v) => assert_eq!(42, v),
            _ => panic!("Expected valid conversion."),
        }
    }

    #[test]
    fn property_string_not_found() {
        let props = RasterProperties::default();
        let key = RasterPropertiesKey {
            domain: Some("test".into()),
            key: "key".into(),
        };

        match props.string_property(&key) {
            Err(Error::MissingRasterProperty { property: k }) if k == key.to_string() => (),
            _ => panic!("Expected missing property error"),
        }
    }

    #[test]
    fn property_string_ok() {
        let mut props = RasterProperties::default();
        let key = RasterPropertiesKey {
            domain: Some("test".into()),
            key: "key".into(),
        };

        props
            .properties_map
            .insert(key.clone(), RasterPropertiesEntry::String("test".into()));

        match props.string_property(&key) {
            Ok(v) => assert_eq!("test", v.as_str()),
            _ => panic!("Expected valid property entry."),
        }
    }
}
