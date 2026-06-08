mod epsg;

pub use epsg::EpsgBounds;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrsType {
    Projected,
    Geographic2d,
}
