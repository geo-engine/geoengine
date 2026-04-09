use crate::error::Result;
use crate::layers::layer::Property;
use bytes::BytesMut;
use geoengine_datatypes::operations::image::Colorizer;
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::spatial_reference::SpatialReference;
use postgres_types::{FromSql, IsNull, ToSql, to_sql_checked};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq)]
pub struct NetCdfGroupMetadata {
    pub name: String,
    pub title: String,
    pub description: String,
    // TODO: would actually be nice if it were inside dataset/entity
    pub data_type: Option<RasterDataType>,
    pub data_range: Option<DataRange>,
    // TODO: would actually be nice if it were inside dataset/entity
    pub unit: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NetCdfOverviewMetadata {
    pub file_name: String,
    pub title: String,
    pub summary: String,
    pub spatial_reference: SpatialReference,
    pub colorizer: Colorizer,
    pub creator: Creator,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(from = "(f64, f64)", into = "(f64, f64)")]
pub struct DataRange {
    min: f64,
    max: f64,
}

impl DataRange {
    pub fn uninitialized() -> Self {
        Self {
            min: f64::INFINITY,
            max: -f64::INFINITY,
        }
    }

    pub fn new(min: f64, max: f64) -> Self {
        Self { min, max }
    }

    pub fn as_json(&self) -> serde_json::Result<String> {
        serde_json::to_string(&self)
    }

    pub fn min(&self) -> f64 {
        self.min
    }

    pub fn max(&self) -> f64 {
        self.max
    }

    pub fn update(&mut self, Self { min, max }: Self) {
        self.min = self.min.min(min);
        self.max = self.max.max(max);
    }

    pub fn update_min(&mut self, min: f64) {
        self.min = self.min.min(min);
    }

    pub fn update_max(&mut self, max: f64) {
        self.max = self.max.max(max);
    }
}

impl From<(f64, f64)> for DataRange {
    fn from((min, max): (f64, f64)) -> Self {
        Self { min, max }
    }
}

impl From<DataRange> for (f64, f64) {
    fn from(DataRange { min, max }: DataRange) -> Self {
        (min, max)
    }
}

impl FromSql<'_> for DataRange {
    fn from_sql(
        ty: &postgres_types::Type,
        raw: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let [min, max] = <[f64; 2] as FromSql>::from_sql(ty, raw)?;
        Ok(Self { min, max })
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        <[f64; 2] as FromSql>::accepts(ty)
    }
}

impl ToSql for DataRange {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let array = [self.min, self.max];
        <[f64; 2] as ToSql>::to_sql(&array, ty, out)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        <[f64; 2] as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

/// The cretor of an EBV
#[derive(Debug, Clone, PartialEq)]
pub struct Creator {
    pub name: Option<String>,
    pub email: Option<String>,
    pub institution: Option<String>,
}

impl Creator {
    pub fn new(name: Option<String>, email: Option<String>, institution: Option<String>) -> Self {
        Self {
            name,
            email,
            institution,
        }
    }

    pub fn empty() -> Self {
        Self {
            name: None,
            email: None,
            institution: None,
        }
    }

    pub fn as_property(&self) -> Option<Property> {
        let Some(name) = &self.name else {
            return None;
        };

        let email = self.email.as_deref().unwrap_or("unknown");
        let institution = self.institution.as_deref().unwrap_or("unknown");

        let property = Property::from((
            "author".to_string(),
            format!("{name}, {email}, {institution}"),
        ));

        Some(property)
    }
}
