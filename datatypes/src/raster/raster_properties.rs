use crate::error::Error;
use crate::util::Result;

use num_traits::FromPrimitive;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

    pub fn get_number_property<T: Copy + FromPrimitive>(
        &self,
        key: &RasterPropertiesKey,
    ) -> Result<T> {
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

    pub fn get_string_property(&self, key: &RasterPropertiesKey) -> Result<String> {
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

impl Default for RasterProperties {
    fn default() -> Self {
        RasterProperties {
            band_name: None,
            scale: None,
            offset: None,
            properties_map: HashMap::new(),
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

impl ToString for RasterPropertiesKey {
    fn to_string(&self) -> String {
        match &self.domain {
            Some(prefix) => prefix.clone() + "." + &self.key,
            None => self.key.clone(),
        }
    }
}

impl ToString for RasterPropertiesEntry {
    fn to_string(&self) -> String {
        match self {
            RasterPropertiesEntry::Number(n) => n.to_string(),
            RasterPropertiesEntry::String(s) => s.clone(),
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

        match props.get_number_property::<u8>(&key) {
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

        match props.get_number_property::<u8>(&key) {
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

        match props.get_number_property::<u8>(&key) {
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

        match props.get_string_property(&key) {
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

        match props.get_string_property(&key) {
            Ok(v) => assert_eq!("test", v.as_str()),
            _ => panic!("Expected valid property entry."),
        }
    }
}
