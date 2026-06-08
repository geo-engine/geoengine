use crate::error::Error;
use crate::primitives::{AxisAlignedRectangle, BoundingBox2D, Coordinate2D};
use crate::spatial_reference::{AreaOfUseProvider, SpatialReference, SpatialReferenceAuthority};
use crs_bounds::EpsgBounds;

pub struct StaticEpsgAreaProvider {
    wgs84_bounds: BoundingBox2D,
    projected_bounds: Option<BoundingBox2D>,
}

impl AreaOfUseProvider for StaticEpsgAreaProvider {
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
}
