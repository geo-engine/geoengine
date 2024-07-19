use std::str::FromStr;
use strum::EnumIter;
use strum_macros::EnumString;

use crate::{
    error::{Error, Result},
    layers::listing::LayerCollectionId,
};

#[derive(Debug, Clone)]
pub enum CopernicusDataspaceLayerCollectionId {
    Datasets,
    Sentinel1,
    Sentinel2(Sentinel2LayerCollectionId),
}

#[derive(Debug, Clone)]
pub enum Sentinel2LayerCollectionId {
    Products,
    Product {
        product: Sentinel2Product,
    },
    ProductZone {
        product: Sentinel2Product,
        zone: UtmZone,
    },
    ProductZoneBand {
        product: Sentinel2Product,
        zone: UtmZone,
        band: Sentinel2Band,
    },
}

#[derive(Debug, Clone, Copy, EnumString, strum::Display, EnumIter)]
pub enum Sentinel2Product {
    L1C,
    L2A,
}

#[derive(Debug, Clone, Copy, EnumString, strum::Display, EnumIter)]
pub enum Sentinel2Band {
    B01,
    B02,
    B03,
    B04,
    B05,
    B06,
    B07,
    B08,
    B8A,
    B09,
    B10,
    B11,
    B12,
}

#[derive(Debug, Clone, Copy)]
pub struct UtmZone {
    zone: u8,
    north: UtmZoneDirection,
}

#[derive(Debug, Clone, Copy)]
pub enum UtmZoneDirection {
    North,
    South,
}

impl FromStr for CopernicusDataspaceLayerCollectionId {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.splitn(3, '/');

        let root = parts.next().ok_or(Error::InvalidLayerCollectionId)?;
        let dataset = parts.next();
        let rem = parts.next();

        match (root, dataset, rem) {
            ("datasets", None, None) => Ok(Self::Datasets),
            ("datasets", Some("SENTINEL-1"), _) => Ok(Self::Sentinel1),
            ("datasets", Some("SENTINEL-2"), rem) => Ok(Self::Sentinel2(
                rem.unwrap_or("").parse::<Sentinel2LayerCollectionId>()?,
            )),
            _ => Err(Error::InvalidLayerCollectionId),
        }
    }
}

impl From<CopernicusDataspaceLayerCollectionId> for LayerCollectionId {
    fn from(id: CopernicusDataspaceLayerCollectionId) -> Self {
        match id {
            CopernicusDataspaceLayerCollectionId::Datasets => {
                LayerCollectionId("datasets".to_string())
            }
            CopernicusDataspaceLayerCollectionId::Sentinel1 => {
                LayerCollectionId("datasets/SENTINEL-1".to_string())
            }
            CopernicusDataspaceLayerCollectionId::Sentinel2(sentinel2) => sentinel2.into(),
        }
    }
}

impl FromStr for Sentinel2LayerCollectionId {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Ok(Self::Products);
        }

        let split: Vec<&str> = s.split('/').collect();

        Ok(match *split.as_slice() {
            [product] => Self::Product {
                product: Sentinel2Product::from_str(product)
                    .map_err(|_| Error::InvalidLayerCollectionId)?,
            },
            [product, zone] => Self::ProductZone {
                product: Sentinel2Product::from_str(product)
                    .map_err(|_| Error::InvalidLayerCollectionId)?,
                zone: UtmZone::from_str(zone)?,
            },
            [product, zone, band] => Self::ProductZoneBand {
                product: Sentinel2Product::from_str(product)
                    .map_err(|_| Error::InvalidLayerCollectionId)?,
                zone: UtmZone::from_str(zone)?,
                band: Sentinel2Band::from_str(band).map_err(|_| Error::InvalidLayerCollectionId)?,
            },
            _ => return Err(Error::InvalidLayerCollectionId),
        })
    }
}

impl From<Sentinel2LayerCollectionId> for LayerCollectionId {
    fn from(id: Sentinel2LayerCollectionId) -> Self {
        let s = match id {
            Sentinel2LayerCollectionId::Products => "datasets/SENTINEL-2".to_string(),
            Sentinel2LayerCollectionId::Product { product } => {
                format!("datasets/SENTINEL-2/{product}")
            }
            Sentinel2LayerCollectionId::ProductZone { product, zone } => {
                format!("datasets/SENTINEL-2/{product}/{zone}")
            }
            Sentinel2LayerCollectionId::ProductZoneBand {
                product,
                zone,
                band,
            } => format!("datasets/SENTINEL-2/{product}/{zone}/{band}"),
        };

        LayerCollectionId(s)
    }
}

impl FromStr for UtmZone {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 5 || &s[..3] != "UTM" {
            return Err(Error::InvalidLayerCollectionId);
        }

        let (zone_str, dir_char) = s[3..].split_at(s.len() - 4);
        let zone = zone_str
            .parse::<u8>()
            .map_err(|_| Error::InvalidLayerCollectionId)?;

        // TODO: check if zone is in valid range

        let north = match dir_char {
            "N" => UtmZoneDirection::North,
            "S" => UtmZoneDirection::South,
            _ => return Err(Error::InvalidLayerCollectionId),
        };

        Ok(Self { zone, north })
    }
}

impl std::fmt::Display for UtmZone {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "UTM{}{}",
            self.zone,
            match self.north {
                UtmZoneDirection::North => "N",
                UtmZoneDirection::South => "S",
            }
        )
    }
}

impl UtmZone {
    pub fn zones() -> impl Iterator<Item = Self> {
        (1..=60).flat_map(|zone| {
            vec![
                UtmZone {
                    zone,
                    north: UtmZoneDirection::North,
                },
                UtmZone {
                    zone,
                    north: UtmZoneDirection::South,
                },
            ]
        })
    }
}
