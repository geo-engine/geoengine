use crate::error::Error;
use crate::util::{ByteSize, Result};

use num_traits::FromPrimitive;
use postgres_types::{FromSql, ToSql};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::fmt::{Display, Formatter};

/// This struct stores properties of a raster tile.
/// This includes the scale and offset of the raster as well as a a description and a map of additional properties.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct RasterProperties {
    scale: Option<f64>,
    offset: Option<f64>,
    description: Option<String>,
    // serialize as a list of tuples because `RasterPropertiesKey` cannot be used as a key in a JSON dict
    #[serde_as(as = "Vec<(_, _)>")]
    properties_map: HashMap<RasterPropertiesKey, RasterPropertiesEntry>,
}

impl ByteSize for RasterProperties {
    fn heap_byte_size(&self) -> usize {
        self.description.heap_byte_size() + self.properties_map.heap_byte_size()
    }
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

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    PartialEq,
    Hash,
    Eq,
    PartialOrd,
    Ord,
    FromSql,
    ToSql,
    JsonSchema,
)]
pub struct RasterPropertiesKey {
    pub domain: Option<String>,
    pub key: String,
}

impl ByteSize for RasterPropertiesKey {
    fn heap_byte_size(&self) -> usize {
        self.domain.heap_byte_size() + self.key.heap_byte_size()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "value")]
pub enum RasterPropertiesEntry {
    Number(f64),
    String(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromSql, ToSql)]
pub enum RasterPropertiesEntryType {
    Number,
    String,
}

impl ByteSize for RasterPropertiesEntry {
    fn heap_byte_size(&self) -> usize {
        match self {
            RasterPropertiesEntry::Number(_) => 0,
            RasterPropertiesEntry::String(s) => s.heap_byte_size(),
        }
    }
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

    use super::*;
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

    #[test]
    fn byte_size() {
        // 24 + 4 (domain) + 24 + 3 = 55
        assert_eq!(
            RasterPropertiesKey {
                domain: Some("test".into()),
                key: "key".into(),
            }
            .byte_size(),
            55
        );
        // 24 + 4 = 28
        assert_eq!(RasterPropertiesEntry::String("test".into()).byte_size(), 28);

        assert_eq!(
            HashMap::from_iter([(
                RasterPropertiesKey {
                    domain: Some("test".into()),
                    key: "key".into(),
                },
                RasterPropertiesEntry::String("test".into())
            )])
            .byte_size(),
            131 // 48 + 55 + 28
        );
        assert_eq!(
            HashMap::from_iter([
                (
                    RasterPropertiesKey {
                        domain: Some("test".into()),
                        key: "key".into(),
                    },
                    RasterPropertiesEntry::String("test".into())
                ),
                (
                    RasterPropertiesKey {
                        domain: Some("test".into()),
                        key: "yek".into(),
                    },
                    RasterPropertiesEntry::String("tset".into())
                )
            ])
            .byte_size(),
            131 + 83 // 48 + 2 x (55 + 28)
        );

        let mut props = RasterProperties::default();

        props.properties_map.insert(
            RasterPropertiesKey {
                domain: Some("test".into()),
                key: "key".into(),
            },
            RasterPropertiesEntry::String("test".into()),
        );

        // scale = 8 + 8 = 16
        // offset = 8 + 8 = 16
        // description = 24
        // properties_map = (HashMap) + (Key) + (Value)
        //                  HashMap = 48
        //                  Key = 24 + 4 (domain) + 24 + 3 (key) = 55
        //                  Value = 24 + 4 (value) = 28
        //                  48 + 55 + 28 = 131
        // total = 16 + 16 + 24 + 131 = 187

        assert_eq!(props.byte_size(), 187);

        props.properties_map.insert(
            RasterPropertiesKey {
                domain: Some("test".into()),
                key: "yek".into(),
            },
            RasterPropertiesEntry::String("tset".into()),
        );

        assert_eq!(props.byte_size(), 187 + 83);
    }
}
