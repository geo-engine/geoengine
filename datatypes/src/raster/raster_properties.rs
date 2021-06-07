use crate::error::Error;
use crate::util::Result;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryInto;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RasterProperties {
    pub scale: Option<f64>,
    pub offset: Option<f64>,
    pub band_name: Option<String>,
    pub properties_map: HashMap<RasterPropertiesKey, RasterPropertiesEntry>,
}

impl RasterProperties {
    pub fn band_name(&self) -> Option<&String> {
        self.band_name.as_ref()
    }

    pub fn set_band_name(&mut self, band_name: String) {
        self.band_name = Some(band_name);
    }

    pub fn remove_band_name(&mut self) {
        self.band_name = None;
    }

    pub fn properties_domains(&self) -> impl Iterator<Item = Option<&String>> {
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
pub enum RasterPropertiesEntry {
    Number(f64),
    String(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RasterPropertiesEntryType {
    Number,
    String,
}

impl Into<String> for RasterPropertiesEntry {
    fn into(self) -> String {
        match self {
            RasterPropertiesEntry::Number(n) => n.to_string(),
            RasterPropertiesEntry::String(s) => s,
        }
    }
}

impl TryInto<f64> for RasterPropertiesEntry {
    fn try_into(self) -> Result<f64> {
        match self {
            RasterPropertiesEntry::Number(n) => Ok(n),
            RasterPropertiesEntry::String(s) => s.parse().map_err(|_| Error::WrongMetadataType),
        }
    }

    type Error = crate::error::Error;
}
