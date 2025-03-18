use geoengine_datatypes::raster::RasterDataType;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ImageProduct {
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
    Scl,
    Wvp,
    Aot,
    _Tci,
}

pub trait ImageProductpec {
    fn resolution_m(&self) -> f64;
    fn name(&self) -> &str;
    fn long_name(&self) -> &str;
    fn no_data_value(&self) -> Option<f64>;
    fn data_type(&self) -> RasterDataType;
}

impl ImageProductpec for ImageProduct {
    fn no_data_value(&self) -> Option<f64> {
        Some(0.)
    }

    fn resolution_m(&self) -> f64 {
        match self {
            ImageProduct::B02
            | ImageProduct::B03
            | ImageProduct::B04
            | ImageProduct::B08
            | ImageProduct::_Tci => 10.,
            ImageProduct::B05
            | ImageProduct::B06
            | ImageProduct::B07
            | ImageProduct::B8A
            | ImageProduct::B11
            | ImageProduct::B12
            | ImageProduct::Scl
            | ImageProduct::Wvp
            | ImageProduct::Aot => 20.,
            ImageProduct::B01 | ImageProduct::B09 | ImageProduct::B10 => 60.,
        }
    }

    fn name(&self) -> &str {
        match self {
            ImageProduct::B01 => "B01",
            ImageProduct::B02 => "B02",
            ImageProduct::B03 => "B03",
            ImageProduct::B04 => "B04",
            ImageProduct::B05 => "B05",
            ImageProduct::B06 => "B06",
            ImageProduct::B07 => "B07",
            ImageProduct::B08 => "B08",
            ImageProduct::B8A => "B8A",
            ImageProduct::B09 => "B09",
            ImageProduct::B10 => "B10",
            ImageProduct::B11 => "B11",
            ImageProduct::B12 => "B12",
            ImageProduct::Scl => "SCL",
            ImageProduct::Wvp => "WVP",
            ImageProduct::Aot => "AOT",
            ImageProduct::_Tci => "TCI",
        }
    }

    fn long_name(&self) -> &str {
        match self {
            ImageProduct::Scl => "Scene Classification",
            ImageProduct::Wvp => "Water Vapour",
            ImageProduct::Aot => "Aerosol Optical Thickness",
            ImageProduct::_Tci => "True Colour Image",
            _ => self.name(),
        }
    }

    fn data_type(&self) -> RasterDataType {
        match self {
            ImageProduct::Scl => RasterDataType::U8,
            _ => RasterDataType::U16,
        }
    }
}
