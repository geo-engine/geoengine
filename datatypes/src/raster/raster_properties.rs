use crate::error::Error;
use crate::util::Result;

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

impl ToString for RasterPropertiesEntry {
    fn to_string(&self) -> String {
        match self {
            RasterPropertiesEntry::Number(n) => n.to_string(),
            RasterPropertiesEntry::String(s) => s.clone(),
        }
    }
}
