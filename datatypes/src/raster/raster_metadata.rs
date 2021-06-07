use crate::error::Error;
use crate::util::Result;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryInto;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RasterMetadata {
    pub scale: Option<f64>,
    pub offset: Option<f64>,
    pub band_name: Option<String>,
    pub metadata_map: HashMap<MetadataKey, MetadataEntry>,
}

impl RasterMetadata {
    pub fn band_name(&self) -> Option<&String> {
        self.band_name.as_ref()
    }

    pub fn set_band_name(&mut self, band_name: String) {
        self.band_name = Some(band_name);
    }

    pub fn remove_band_name(&mut self) {
        self.band_name = None;
    }

    pub fn metadata_domain(&self) -> impl Iterator<Item = &str> {
        vec![].into_iter() // hack
    }
}

impl Default for RasterMetadata {
    fn default() -> Self {
        RasterMetadata {
            band_name: None,
            scale: None,
            offset: None,
            metadata_map: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash, Eq, PartialOrd, Ord)]
pub struct MetadataKey {
    pub domain: Option<String>,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MetadataEntry {
    Number(f64),
    String(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MetadataEntryType {
    Number,
    String,
}

impl Into<String> for MetadataEntry {
    fn into(self) -> String {
        match self {
            MetadataEntry::Number(n) => n.to_string(),
            MetadataEntry::String(s) => s,
        }
    }
}

impl TryInto<f64> for MetadataEntry {
    fn try_into(self) -> Result<f64> {
        match self {
            MetadataEntry::Number(n) => Ok(n),
            MetadataEntry::String(s) => s.parse().map_err(|_| Error::WrongMetadataType),
        }
    }

    type Error = crate::error::Error;
}
