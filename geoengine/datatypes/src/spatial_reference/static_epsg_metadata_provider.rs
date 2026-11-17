use crate::error::Error;
use crate::primitives::{AxisAlignedRectangle, BoundingBox2D, Coordinate2D};
use crate::spatial_reference::{CrsMetadataProvider, SpatialReference, SpatialReferenceAuthority};
use crs_constants::{AxisOrder, EpsgBounds};

/// A [`CrsMetadataProvider`] backed by the compile-time EPSG metadata of the
/// `crs-constants` crate (generated from PROJ's EPSG database).
///
/// Unlike a library-backed provider it needs neither PROJ nor geodesy at
/// runtime, but it only knows EPSG-metadata that is present in the generated
/// registry.
pub struct StaticEpsgMetadataProvider {
    /// The area of use in WGS 84 longitude/latitude degrees.
    wgs84_bounds: BoundingBox2D,
    /// The area of use in the CRS's native units, if it can be represented.
    projected_bounds: Option<BoundingBox2D>,
    /// How many meters correspond to one unit of the CRS's native unit.
    meters_per_unit: f64,
}

impl StaticEpsgMetadataProvider {
    /// The axis order of an EPSG code from the registry, without asking PROJ.
    /// `None` if the registry does not know the code or its axis order.
    pub fn axis_order(def: SpatialReference) -> Option<AxisOrder> {
        if !matches!(def.authority(), SpatialReferenceAuthority::Epsg) {
            return None;
        }
        // codes beyond u16 are not in the registry and must not wrap around onto one that is
        EpsgBounds::from_code(u16::try_from(def.code()).ok()?)?.axis_order
    }
}

impl CrsMetadataProvider for StaticEpsgMetadataProvider {
    fn new_known_crs(def: SpatialReference) -> super::Result<Self> {
        if !matches!(def.authority(), SpatialReferenceAuthority::Epsg) {
            return Err(Error::NoAreaOfUseDefined {
                proj_string: def.srs_string(),
            });
        }

        let code = def.code();

        // TODO: alias lookup here!  e.g. to catch 900931 -> web mercator

        let EpsgBounds {
            wgs84_bounds: [x1, y1, x2, y2],
            native_bounds,
            meters_per_unit,
            ..
        } = EpsgBounds::from_code(code as u16).ok_or_else(|| Error::NoAreaOfUseDefined {
            proj_string: def.srs_string(),
        })?;

        Ok(Self {
            wgs84_bounds: BoundingBox2D::new(
                Coordinate2D { x: *x1, y: *y1 },
                Coordinate2D { x: *x2, y: *y2 },
            )?,
            projected_bounds: native_bounds.map(|b| {
                BoundingBox2D::new(
                    Coordinate2D { x: b[0], y: b[1] },
                    Coordinate2D { x: b[2], y: b[3] },
                )
                .expect("the native bounds from the epsg registry should always be a valid BoundingBox2D")
            }),
            meters_per_unit: *meters_per_unit,
        })
    }

    fn area_of_use<A: AxisAlignedRectangle>(&self) -> super::Result<A> {
        A::from_min_max(
            self.wgs84_bounds.lower_left(),
            self.wgs84_bounds.upper_right(),
        )
    }

    fn area_of_use_projected<A: AxisAlignedRectangle>(&self) -> super::Result<A> {
        if let Some(projected_bounds) = self.projected_bounds {
            Ok(A::from_min_max(
                projected_bounds.lower_left(),
                projected_bounds.upper_right(),
            )?)
        } else {
            Err(Error::NoAreaOfUseDefined {
                proj_string: "no native bounds defined for this epsg code".to_string(),
            })
        }
    }

    fn uses_meters(&self) -> super::Result<bool> {
        #[allow(clippy::float_cmp)]
        // stored value is exact (1.0 or a conversion factor from proj.db)
        Ok(self.meters_per_unit == 1.0)
    }

    fn meters_per_unit(&self) -> super::Result<f64> {
        Ok(self.meters_per_unit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_returns_the_static_axis_order() {
        let epsg = |code| SpatialReference::new(SpatialReferenceAuthority::Epsg, code);
        assert_eq!(
            StaticEpsgMetadataProvider::axis_order(epsg(4326)),
            Some(AxisOrder::NorthEast)
        );
        assert_eq!(
            StaticEpsgMetadataProvider::axis_order(epsg(3857)),
            Some(AxisOrder::EastNorth)
        );
        // feet, left to PROJ
        assert_eq!(StaticEpsgMetadataProvider::axis_order(epsg(2263)), None);
        // 4326 + 2^16 must not wrap around onto 4326
        assert_eq!(StaticEpsgMetadataProvider::axis_order(epsg(69_862)), None);
        assert_eq!(
            StaticEpsgMetadataProvider::axis_order(SpatialReference::new(
                SpatialReferenceAuthority::Esri,
                4326
            )),
            None
        );
    }
}
