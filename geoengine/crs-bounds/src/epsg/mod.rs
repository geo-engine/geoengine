#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrsType {
    Projected,
    Geographic2d,
}

#[derive(Debug, Clone, Copy)]
pub struct EpsgBounds {
    #[cfg(feature = "metadata")]
    pub code: u16,
    #[cfg(feature = "metadata")]
    pub name: &'static str,

    pub crs_type: CrsType,
    pub wgs84_bounds: [f64; 4],
    pub native_bounds: Option<[f64; 4]>,
}

impl EpsgBounds {
    #[inline]
    pub const fn from_code_const<const CODE: u16>() -> Option<&'static Self> {
        registry::get_epsg_bounds(CODE)
    }

    pub const fn from_code(code: u16) -> Option<&'static Self> {
        registry::get_epsg_bounds(code)
    }
}

pub mod registry {
    #![allow(clippy::unreadable_literal, clippy::approx_constant)]

    include!("epsg_registry.rs");
}
