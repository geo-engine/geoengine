use geoengine_datatypes::dataset::{DataId, DataProviderId, ExternalDataId, LayerId};
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
}

#[derive(Debug, Clone)]
pub enum CopernicusDataspaceLayerId {
    // Sentinel1,
    Sentinel2(Sentinel2LayerId),
}

#[derive(Debug, Clone)]
pub struct Sentinel2LayerId {
    pub product: Sentinel2Product,
    pub zone: UtmZone,
    pub band: Sentinel2Band,
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

impl Sentinel2Product {
    pub fn product_type(&self) -> &str {
        match self {
            Self::L1C => "S2MSI1C",
            Self::L2A => "S2MSI2A",
        }
    }
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

impl FromStr for CopernicusDataspaceLayerId {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.splitn(3, '/');

        let root = parts.next().ok_or(Error::InvalidLayerCollectionId)?;
        let dataset = parts.next();
        let rem = parts.next();

        match (root, dataset, rem) {
            ("datasets", Some("SENTINEL-2"), rem) => Ok(Self::Sentinel2(
                rem.unwrap_or("").parse::<Sentinel2LayerId>()?,
            )),
            _ => Err(Error::InvalidLayerId),
        }
    }
}

impl From<CopernicusDataspaceLayerId> for LayerId {
    fn from(id: CopernicusDataspaceLayerId) -> Self {
        match id {
            CopernicusDataspaceLayerId::Sentinel2(sentinel2) => sentinel2.into(),
        }
    }
}

pub struct CopernicusDataId(pub CopernicusDataspaceLayerId, pub DataProviderId);

impl From<CopernicusDataId> for DataId {
    fn from(id: CopernicusDataId) -> Self {
        match id.0 {
            CopernicusDataspaceLayerId::Sentinel2(sentinel2) => {
                Sentinel2DataId(sentinel2, id.1).into()
            }
        }
    }
}

impl TryFrom<DataId> for CopernicusDataId {
    type Error = crate::error::Error;

    fn try_from(id: DataId) -> Result<Self> {
        let external_id = match id {
            DataId::Internal { dataset_id: _ } => return Err(Error::InvalidDataId),
            DataId::External(external_id) => external_id,
        };

        Ok(CopernicusDataId(
            CopernicusDataspaceLayerId::from_str(&external_id.layer_id.0)?,
            external_id.provider_id,
        ))
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
        };

        LayerCollectionId(s)
    }
}

impl FromStr for Sentinel2LayerId {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(Error::InvalidLayerId);
        }

        let split: Vec<&str> = s.split('/').collect();

        Ok(match *split.as_slice() {
            [product, zone, band] => Self {
                product: Sentinel2Product::from_str(product).map_err(|_| Error::InvalidLayerId)?,
                zone: UtmZone::from_str(zone)?,
                band: Sentinel2Band::from_str(band).map_err(|_| Error::InvalidLayerId)?,
            },
            _ => return Err(Error::InvalidLayerId),
        })
    }
}

impl From<Sentinel2LayerId> for LayerId {
    fn from(id: Sentinel2LayerId) -> Self {
        LayerId(format!(
            "datasets/SENTINEL-2/{}/{}/{}",
            id.product, id.zone, id.band
        ))
    }
}

pub struct Sentinel2DataId(Sentinel2LayerId, DataProviderId);

impl From<Sentinel2DataId> for DataId {
    fn from(id: Sentinel2DataId) -> Self {
        Self::External(ExternalDataId {
            provider_id: id.1,
            layer_id: LayerId(format!(
                "datasets/SENTINEL-2/{}/{}/{}",
                id.0.product, id.0.zone, id.0.band
            )),
        })
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
