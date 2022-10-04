use crate::error::Error;
use crate::util::Result;

use num_traits::FromPrimitive;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::fmt::{Display, Formatter};

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

    pub fn number_property<T: Copy + FromPrimitive>(&self, key: &RasterPropertiesKey) -> Result<T> {
        let val = f64::try_from(
            self.properties_map
                .get(key)
                .ok_or(Error::MissingRasterProperty {
                    property: key.to_string(),
                })?
                .clone(),
        )?;
        T::from_f64(val).ok_or(Error::WrongMetadataType)
    }

    pub fn string_property(&self, key: &RasterPropertiesKey) -> Result<String> {
        let s = self
            .properties_map
            .get(key)
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
