use crate::error::Error;
use crate::util::Result;

use num_traits::FromPrimitive;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::fmt::{Display, Formatter};

/// This struct stores properties of a raster tile.
/// This includes the scale and offset of the raster as well as a a description and a map of additional properties.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct RasterProperties {
    scale: Option<f64>,
    offset: Option<f64>,
    description: Option<String>,
    properties_map: HashMap<RasterPropertiesKey, RasterPropertiesEntry>,
}

impl RasterProperties {
    /// Returns an iterator over all domains of the properties.
    pub fn domains(&self) -> impl Iterator<Item = Option<&String>> {
        self.properties_map.keys().map(|m| m.domain.as_ref())
    }

    /// Sets the scale factor of the raster.
    pub fn set_scale(&mut self, scale: f64) {
        self.scale = Some(scale);
    }

    /// Sets the offset of the raster.
    pub fn set_offset(&mut self, offset: f64) {
        self.offset = Some(offset);
    }

    /// Sets the description of the raster.
    pub fn set_description(&mut self, description: String) {
        self.description = Some(description);
    }

    /// Inserts a property into the properties map.
    pub fn insert_property(
        &mut self,
        key: RasterPropertiesKey,
        value: RasterPropertiesEntry,
    ) -> Option<RasterPropertiesEntry> {
        self.properties_map.insert(key, value)
    }

    /// Returns the scale factor of the raster. Will return `1.0` if no scale factor is set.
    pub fn scale(&self) -> f64 {
        self.scale.unwrap_or(1.0)
    }

    /// Returns the offset of the raster. Will return `0.0` if no offset is set.
    pub fn offset(&self) -> f64 {
        self.offset.unwrap_or(0.0)
    }

    /// Returns the scale factor of the raster. Will return `None` if no scale factor is set.
    pub fn scale_option(&self) -> Option<f64> {
        self.scale
    }

    /// Returns the offset of the raster. Will return `None` if no offset is set.
    pub fn offset_option(&self) -> Option<f64> {
        self.offset
    }

    /// Returns the description of the raster. Will return `None` if no description is set.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns the value of the property with the given key.
    /// If the key is not found, `None` is returned.
    pub fn get_property(&self, key: &RasterPropertiesKey) -> Option<&RasterPropertiesEntry> {
        self.properties_map.get(key)
    }

    /// Returns a number value of the property with the given key and domain.
    ///
    /// # Errors
    /// IF the property is not found or the type does not match, an error is returned.
    pub fn number_property<T: Copy + FromPrimitive>(&self, key: &RasterPropertiesKey) -> Result<T> {
        let val = f64::try_from(self.get_property(key).cloned().ok_or(
            Error::MissingRasterProperty {
                property: key.to_string(),
            },
        )?)?;
        T::from_f64(val).ok_or(Error::WrongMetadataType)
    }

    /// Returns a string value of the property with the given key and domain.
    ///
    /// # Errors
    /// IF the property is not found or the type does not match, an error is returned.
    pub fn string_property(&self, key: &RasterPropertiesKey) -> Result<&str> {
        let s = self.get_property(key).ok_or(Error::MissingRasterProperty {
            property: key.to_string(),
        })?;
        match s {
            RasterPropertiesEntry::String(s) => Ok(s),
            RasterPropertiesEntry::Number(_) => Err(Error::WrongMetadataType),
        }
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
            RasterPropertiesEntry::Number(n) => write!(f, "{n}"),
            RasterPropertiesEntry::String(s) => write!(f, "{s}"),
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
            Ok(v) => assert_eq!("test", v),
            _ => panic!("Expected valid property entry."),
        }
    }
}
