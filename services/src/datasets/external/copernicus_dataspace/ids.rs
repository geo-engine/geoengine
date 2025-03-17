use geoengine_datatypes::{
    dataset::{DataId, DataProviderId, ExternalDataId, LayerId, NamedData},
    raster::RasterDataType,
};
use std::str::FromStr;
use strum::IntoEnumIterator;
use strum_macros::{EnumIter, EnumString};

use crate::{
    error::{Error, Result},
    layers::listing::LayerCollectionId,
    util::sentinel_2_utm_zones::UtmZone,
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
    pub product_band: Sentinel2ProductBand,
    pub zone: UtmZone,
}

#[derive(Debug, Clone, Copy, EnumString, strum::Display, EnumIter)]
pub enum Sentinel2Product {
    L1C,
    L2A,
}

impl Sentinel2Product {
    pub fn product_bands(self) -> Box<dyn Iterator<Item = Sentinel2ProductBand>> {
        match self {
            Self::L1C => Box::new(L1CBand::iter().map(Sentinel2ProductBand::L1C)),
            Self::L2A => Box::new(L2ABand::iter().map(Sentinel2ProductBand::L2A)),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Sentinel2ProductBand {
    L1C(L1CBand),
    L2A(L2ABand),
}

impl Sentinel2ProductBand {
    fn with_product_and_band_as_str(product: &str, band: &str) -> Result<Self> {
        match product {
            "L1C" => L1CBand::from_str(band)
                .map(Sentinel2ProductBand::L1C)
                .map_err(|_| Error::InvalidLayerId),
            "L2A" => L2ABand::from_str(band)
                .map(Sentinel2ProductBand::L2A)
                .map_err(|_| Error::InvalidLayerId),
            _ => Err(Error::InvalidLayerId),
        }
    }

    pub fn product_name(self) -> &'static str {
        match self {
            Sentinel2ProductBand::L1C(_) => "L1C",
            Sentinel2ProductBand::L2A(_) => "L2A",
        }
    }

    pub fn band_name(self) -> String {
        match self {
            Sentinel2ProductBand::L1C(band) => format!("{band}"),
            Sentinel2ProductBand::L2A(band) => format!("{band}"),
        }
    }

    pub fn resolution_meters(self) -> usize {
        match self {
            Sentinel2ProductBand::L1C(l1c) => l1c.resolution_meters(),
            Sentinel2ProductBand::L2A(l2a) => l2a.resolution_meters(),
        }
    }
}

#[derive(Debug, Clone, Copy, EnumString, strum::Display, EnumIter)]
pub enum L1CBand {
    B01,
    B02,
    B03,
    B04,
    B05,
    B06,
    B07,
    B08,
    B08A,
    B09,
    B10,
    B11,
    B12,
}
#[derive(Debug, Clone, Copy, EnumString, strum::Display, EnumIter)]
#[allow(non_camel_case_types)]
pub enum L2ABand {
    B01,
    B02,
    B03,
    B04,
    B05,
    B06,
    B07,
    B08,
    B08A,
    B09,
    // B10,
    B11,
    B12,
    // TODO: AOT, CLD, etc. but they have multiple resolutions
    AOT_20M,
    CLD_20M,
    SCL_20M,
    SNW_20M,
    WVP_20M,
    AOT_60M,
    CLD_60M,
    SCL_60M,
    SNW_60M,
    WVP_60M,
}

impl Sentinel2ProductBand {
    // TODO: move to sentinel2 to separate concerns
    pub fn product_type(&self) -> &str {
        match self {
            Self::L1C(_) => "S2MSI1C",
            Self::L2A(_) => "S2MSI2A",
        }
    }

    pub fn main_file_name(&self) -> &str {
        match self {
            Self::L1C(_) => "MTD_MSIL1C.xml",
            Self::L2A(_) => "MTD_MSIL2A.xml",
        }
    }

    pub fn driver_name(&self) -> &str {
        match self {
            Self::L1C(_) => "SENTINEL2_L1C",
            Self::L2A(_) => "SENTINEL2_L2A",
        }
    }
}

pub trait Sentinel2Band {
    fn resolution_meters(self) -> usize;

    fn channel_in_subdataset(self) -> usize;

    fn data_type(self) -> RasterDataType;
}

// exemplary gdalinfo output:
// $ gdalinfo /vsis3/eodata/eodata/Sentinel-2/MSI/L1C/2016/01/03/S2A_MSIL1C_20160103T154242_N0201_R068_T17NRA_20160103T154241.SAFE/MTD_MSIL1C.xml
// [...]
// Subdatasets:
//   SUBDATASET_1_NAME=SENTINEL2_L1C:/vsis3/eodata/eodata/Sentinel-2/MSI/L1C/2016/01/03/S2A_MSIL1C_20160103T154242_N0201_R068_T17NRA_20160103T154241.SAFE/MTD_MSIL1C.xml:10m:EPSG_32617
//   SUBDATASET_1_DESC=Bands B2, B3, B4, B8 with 10m resolution, UTM 17N
//   SUBDATASET_2_NAME=SENTINEL2_L1C:/vsis3/eodata/eodata/Sentinel-2/MSI/L1C/2016/01/03/S2A_MSIL1C_20160103T154242_N0201_R068_T17NRA_20160103T154241.SAFE/MTD_MSIL1C.xml:20m:EPSG_32617
//   SUBDATASET_2_DESC=Bands B5, B6, B7, B8A, B11, B12 with 20m resolution, UTM 17N
//   SUBDATASET_3_NAME=SENTINEL2_L1C:/vsis3/eodata/eodata/Sentinel-2/MSI/L1C/2016/01/03/S2A_MSIL1C_20160103T154242_N0201_R068_T17NRA_20160103T154241.SAFE/MTD_MSIL1C.xml:60m:EPSG_32617
//   SUBDATASET_3_DESC=Bands B1, B9, B10 with 60m resolution, UTM 17N
//   SUBDATASET_4_NAME=SENTINEL2_L1C:/vsis3/eodata/eodata/Sentinel-2/MSI/L1C/2016/01/03/S2A_MSIL1C_20160103T154242_N0201_R068_T17NRA_20160103T154241.SAFE/MTD_MSIL1C.xml:TCI:EPSG_32617
//   SUBDATASET_4_DESC=True color image, UTM 17N
impl Sentinel2Band for L1CBand {
    fn resolution_meters(self) -> usize {
        match self {
            L1CBand::B02 | L1CBand::B03 | L1CBand::B04 | L1CBand::B08 => 10,
            L1CBand::B05
            | L1CBand::B06
            | L1CBand::B07
            | L1CBand::B08A
            | L1CBand::B11
            | L1CBand::B12 => 20,
            L1CBand::B01 | L1CBand::B09 | L1CBand::B10 => 60,
        }
    }

    #[allow(clippy::match_same_arms)]
    fn channel_in_subdataset(self) -> usize {
        match self {
            L1CBand::B01 => 1,
            L1CBand::B02 => 1,
            L1CBand::B03 => 2,
            L1CBand::B04 => 3,
            L1CBand::B05 => 1,
            L1CBand::B06 => 2,
            L1CBand::B07 => 3,
            L1CBand::B08 => 4,
            L1CBand::B08A => 4,
            L1CBand::B09 => 2,
            L1CBand::B10 => 3,
            L1CBand::B11 => 5,
            L1CBand::B12 => 6,
        }
    }

    #[allow(clippy::unused_self)] // might need to be used in the future to distinguish between bands
    fn data_type(self) -> RasterDataType {
        RasterDataType::U16
    }
}

// exemplary gdalinfo output:
// $ gdalinfo /vsizip/download/S2A.zip/S2A_MSIL2A_20160103T154242_N0201_R068_T17NRA_20160103T154241.SAFE/MTD_MSIL2A.xml
// [...]
//     Subdatasets:
//     SUBDATASET_1_NAME=SENTINEL2_L2A:/vsizip/download/S2A.zip/S2A_MSIL2A_20160103T154242_N0201_R068_T17NRA_20160103T154241.SAFE/MTD_MSIL2A.xml:10m:EPSG_32617
//     SUBDATASET_1_DESC=Bands B2, B3, B4, B8 with 10m resolution, UTM 17N
//     SUBDATASET_2_NAME=SENTINEL2_L2A:/vsizip/download/S2A.zip/S2A_MSIL2A_20160103T154242_N0201_R068_T17NRA_20160103T154241.SAFE/MTD_MSIL2A.xml:20m:EPSG_32617
//     SUBDATASET_2_DESC=Bands B5, B6, B7, B8A, B11, B12, AOT, CLD, SCL, SNW, WVP with 20m resolution, UTM 17N
//     SUBDATASET_3_NAME=SENTINEL2_L2A:/vsizip/download/S2A.zip/S2A_MSIL2A_20160103T154242_N0201_R068_T17NRA_20160103T154241.SAFE/MTD_MSIL2A.xml:60m:EPSG_32617
//     SUBDATASET_3_DESC=Bands B1, B9, AOT, CLD, SCL, SNW, WVP with 60m resolution, UTM 17N
//     SUBDATASET_4_NAME=SENTINEL2_L2A:/vsizip/download/S2A.zip/S2A_MSIL2A_20160103T154242_N0201_R068_T17NRA_20160103T154241.SAFE/MTD_MSIL2A.xml:TCI:EPSG_32617
//     SUBDATASET_4_DESC=True color image, UTM 17N
impl Sentinel2Band for L2ABand {
    fn resolution_meters(self) -> usize {
        match self {
            L2ABand::B02 | L2ABand::B03 | L2ABand::B04 | L2ABand::B08 => 10,
            L2ABand::B05
            | L2ABand::B06
            | L2ABand::B07
            | L2ABand::B08A
            | L2ABand::B11
            | L2ABand::B12
            | L2ABand::AOT_20M
            | L2ABand::CLD_20M
            | L2ABand::SNW_20M
            | L2ABand::SCL_20M
            | L2ABand::WVP_20M => 20,
            L2ABand::B01
            | L2ABand::B09
            | L2ABand::AOT_60M
            | L2ABand::CLD_60M
            | L2ABand::SCL_60M
            | L2ABand::SNW_60M
            | L2ABand::WVP_60M => 60,
        }
    }

    #[allow(clippy::match_same_arms)]
    fn channel_in_subdataset(self) -> usize {
        match self {
            L2ABand::B01 => 1,
            L2ABand::B02 => 1,
            L2ABand::B03 => 2,
            L2ABand::B04 => 3,
            L2ABand::B05 => 1,
            L2ABand::B06 => 2,
            L2ABand::B07 => 3,
            L2ABand::B08 => 4,
            L2ABand::B08A => 4,
            L2ABand::B09 => 2,
            L2ABand::B11 => 5,
            L2ABand::B12 => 6,
            L2ABand::AOT_20M => 7,
            L2ABand::CLD_20M => 8,
            L2ABand::SCL_20M => 9,
            L2ABand::SNW_20M => 10,
            L2ABand::WVP_20M => 11,
            L2ABand::AOT_60M => 3,
            L2ABand::CLD_60M => 4,
            L2ABand::SCL_60M => 5,
            L2ABand::SNW_60M => 6,
            L2ABand::WVP_60M => 7,
        }
    }

    #[allow(clippy::unused_self)] // might need to be used in the future to distinguish between bands
    fn data_type(self) -> RasterDataType {
        RasterDataType::U16
    }
}

impl Sentinel2Band for Sentinel2ProductBand {
    fn resolution_meters(self) -> usize {
        match self {
            Sentinel2ProductBand::L1C(b) => b.resolution_meters(),
            Sentinel2ProductBand::L2A(b) => b.resolution_meters(),
        }
    }

    fn channel_in_subdataset(self) -> usize {
        match self {
            Sentinel2ProductBand::L1C(b) => b.channel_in_subdataset(),
            Sentinel2ProductBand::L2A(b) => b.channel_in_subdataset(),
        }
    }

    fn data_type(self) -> RasterDataType {
        match self {
            Sentinel2ProductBand::L1C(b) => b.data_type(),
            Sentinel2ProductBand::L2A(b) => b.data_type(),
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

impl TryFrom<LayerId> for CopernicusDataspaceLayerId {
    type Error = crate::error::Error;

    fn try_from(id: LayerId) -> Result<Self> {
        CopernicusDataspaceLayerId::from_str(&id.0)
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

impl From<CopernicusDataId> for NamedData {
    fn from(id: CopernicusDataId) -> Self {
        match id.0 {
            CopernicusDataspaceLayerId::Sentinel2(sentinel2) => NamedData {
                namespace: None,
                provider: Some(id.1 .0.to_string()),
                name: format!("datasets/{sentinel2}"),
            },
        }
    }
}

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
                zone: UtmZone::from_str(zone).map_err(|_| Error::InvalidLayerCollectionId)?,
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

impl std::fmt::Display for Sentinel2LayerId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "SENTINEL-2/{}/{}/{}",
            self.product_band.product_name(),
            self.zone,
            self.product_band.band_name()
        )
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
                product_band: Sentinel2ProductBand::with_product_and_band_as_str(product, band)
                    .map_err(|_| Error::InvalidLayerId)?,
                zone: UtmZone::from_str(zone).map_err(|_| Error::InvalidLayerId)?,
            },
            _ => return Err(Error::InvalidLayerId),
        })
    }
}

impl From<Sentinel2LayerId> for LayerId {
    fn from(id: Sentinel2LayerId) -> Self {
        LayerId(format!(
            "datasets/SENTINEL-2/{}/{}/{}",
            id.product_band.product_name(),
            id.zone,
            id.product_band.band_name()
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
                id.0.product_band.product_name(),
                id.0.zone,
                id.0.product_band.band_name()
            )),
        })
    }
}
