//! Zero-dependency static metadata for coordinate reference systems (CRS),
//! generated from PROJ's EPSG database.
//!
//! The `epsg` module provides [`EpsgBounds`] entries for every known EPSG
//! code: the WGS 84 (longitude/latitude) bounds, the native projected bounds
//! (if any), the unit and its meters-per-unit factor, plus optional code and
//! name fields gated by cargo features.
//!
//! This crate intentionally contains **only static data** and no logic: it can
//! be compiled and linked without PROJ, geodesy or any other dependency. The
//! actual coordinate projection ("the CRS maths", i.e. the projectors with
//! their runtime behavior and heavy dependencies) deliberately lives in
//! `geoengine-datatypes` (`spatial_reference`), which consumes this crate for
//! metadata lookups.
//!
//! The generated registry lives in `epsg/epsg_registry.rs` and is included by
//! `epsg/mod.rs` as a nested module.

mod epsg;

pub use epsg::EpsgBounds;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrsType {
    Projected,
    Geographic2d,
}
