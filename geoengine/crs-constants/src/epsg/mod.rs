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
    pub unit: &'static str,
    pub meters_per_unit: f64,
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

// The registry is generated code (see `epsg_registry.rs`), so it is included
// via `include!` instead of a plain `mod` declaration: this keeps the file's
// name and content untouched by hand-written code and scopes the clippy
// allowances for the huge generated literals to just this module.
pub mod registry {
    #![allow(clippy::unreadable_literal, clippy::approx_constant)]

    include!("epsg_registry.rs");
}
