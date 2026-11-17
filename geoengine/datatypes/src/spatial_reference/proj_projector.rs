use proj::Proj;
use proj_sys::{
    PJ_COORDINATE_SYSTEM_TYPE_PJ_CS_TYPE_ELLIPSOIDAL, PJ_TYPE_PJ_TYPE_BOUND_CRS,
    proj_context_create, proj_context_destroy, proj_create, proj_crs_get_coordinate_system,
    proj_cs_get_axis_info, proj_cs_get_type, proj_destroy, proj_ellipsoid_get_parameters,
    proj_get_ellipsoid, proj_get_source_crs, proj_get_type,
};
use snafu::ResultExt;
use std::ffi::CString;
use std::ptr::null_mut;
use tracing::instrument;

use crate::{
    error::{self, BoxedResultExt},
    operations::reproject::Reproject,
    primitives::{AxisAlignedRectangle, Coordinate2D},
    spatial_reference::{CoordinateProjection, CrsMetadataProvider, SpatialReference},
    util::Result,
};

/// A coordinate projector backed by the PROJ library (via the `proj` crate),
/// using its well-known-CRS pipeline for the given pair of spatial references.
/// This is the most feature-complete backend: it supports every CRS pair that
/// PROJ's database knows.
pub struct ProjCoordinateProjector {
    /// The CRS coordinates are projected from.
    pub from: SpatialReference,
    /// The CRS coordinates are projected to.
    pub to: SpatialReference,
    p: Proj,
}

impl CoordinateProjection for ProjCoordinateProjector {
    #[instrument]
    fn from_known_srs(from: SpatialReference, to: SpatialReference) -> Result<Self> {
        let p = Proj::new_known_crs(&from.srs_string(), &to.srs_string(), None)
            .map_err(|_| error::Error::NoCoordinateProjector { from, to })?;

        Ok(ProjCoordinateProjector { from, to, p })
    }

    #[instrument(skip(self))]
    fn project_coordinate(&self, c: Coordinate2D) -> Result<Coordinate2D> {
        self.p.convert(c).map_err(Into::into)
    }

    #[instrument(skip_all)]
    fn project_coordinates<A: AsRef<[Coordinate2D]>>(
        &self,
        coords: A,
    ) -> Result<Vec<Coordinate2D>> {
        let c_ref = coords.as_ref();

        let mut cc = Vec::from(c_ref);
        self.p.convert_array(&mut cc)?;

        Ok(cc)
    }

    fn source_srs(&self) -> SpatialReference {
        self.from
    }

    fn target_srs(&self) -> SpatialReference {
        self.to
    }
}

impl ProjCoordinateProjector {
    // TODO: this uses the project bounds method of proj, which might produce other results then our current implementation.
    fn _project_bounds<B: AxisAlignedRectangle>(&self, bounds: B) -> Result<B> {
        let start_time: std::time::Instant = std::time::Instant::now();

        let x = self.p.transform_bounds(
            bounds.lower_left().x,
            bounds.lower_left().y,
            bounds.upper_right().x,
            bounds.upper_right().y,
            21,
        )?;

        tracing::trace!(
            "CoordinateProjection::project_coordinates Proj transform_bounds took {}",
            start_time.elapsed().as_nanos()
        );

        let new_left = x[0];
        let new_right = x[2];

        // TODO: how to handle x-axis flip in lat/lon?
        let new_right = if self.to.code() == 4326 && new_right < new_left {
            new_right + 360.
        } else {
            new_right
        };

        B::from_min_max(
            Coordinate2D {
                x: new_left,
                y: x[1],
            },
            Coordinate2D {
                x: new_right,
                y: x[3],
            },
        )
    }
}

impl Clone for ProjCoordinateProjector {
    #[instrument(skip(self))]
    fn clone(&self) -> Self {
        let start_time: std::time::Instant = std::time::Instant::now();

        let p = Proj::new_known_crs(&self.from.to_string(), &self.to.to_string(), None)
                .expect("the Proj object creation should work because it already worked in the creation of the `CoordinateProjector`");

        tracing::trace!(
            "CoordinateProjection::clone Proj new_known_crs took {}",
            start_time.elapsed().as_nanos()
        );

        ProjCoordinateProjector {
            from: self.from,
            to: self.to,
            p,
        }
    }
}

impl AsRef<ProjCoordinateProjector> for ProjCoordinateProjector {
    fn as_ref(&self) -> &ProjCoordinateProjector {
        self
    }
}

pub struct ProjMetadataProvider {
    proj: Proj,
    def: SpatialReference,
}

impl CrsMetadataProvider for ProjMetadataProvider {
    fn new_known_crs(def: SpatialReference) -> Result<Self>
    where
        Self: Sized,
    {
        let proj = Proj::new(&def.proj_string()?)
            .map_err(|_| error::Error::ProjStringUnresolvable { spatial_ref: def })?;

        Ok(ProjMetadataProvider { proj, def })
    }

    fn area_of_use<A: AxisAlignedRectangle>(&self) -> Result<A> {
        let area =
            self.proj
                .area_of_use()
                .context(error::ProjInternal)?
                .0
                .ok_or(error::Error::NoAreaOfUseDefined {
                    proj_string: self.def.proj_string().expect(
                        "must resolve to a valid proj string or the struct can't be created",
                    ),
                })?;

        A::from_min_max(
            (area.west, area.south).into(),
            (area.east, area.north).into(),
        )
    }

    fn area_of_use_projected<A: AxisAlignedRectangle>(&self) -> Result<A> {
        if self.def == SpatialReference::epsg_4326() {
            return self.area_of_use();
        }
        let p = ProjCoordinateProjector::from_known_srs(SpatialReference::epsg_4326(), self.def)?;

        self.area_of_use::<A>()?.reproject(&p)
    }

    fn uses_meters(&self) -> Result<bool> {
        let (meters_per_unit, linear) = self.axis_unit()?;
        Ok(linear && float_cmp::approx_eq!(f64, meters_per_unit, 1.0, ulps = 2))
    }

    fn meters_per_unit(&self) -> Result<f64> {
        Ok(self.axis_unit()?.0)
    }
}

impl ProjMetadataProvider {
    /// How many meters one unit of the CRS's first axis is, and whether that unit is linear.
    /// Geographic CRSs measure angles, so their meters per unit are taken along the equator.
    fn axis_unit(&self) -> Result<(f64, bool)> {
        let srs = CString::new(self.def.proj_string()?).boxed_context(error::ProjInternal2)?;

        // SAFETY: every object is checked for null before it is used and destroyed exactly once;
        // PROJ accepts null for the out parameters and in `proj_destroy`
        let (factor, angular, semi_major) = unsafe {
            let ctx = proj_context_create();
            let crs = proj_create(ctx, srs.as_ptr());
            // a CRS with a transformation to WGS 84 (`+towgs84`) is a bound CRS around the actual one
            let source = if !crs.is_null() && proj_get_type(crs) == PJ_TYPE_PJ_TYPE_BOUND_CRS {
                proj_get_source_crs(ctx, crs)
            } else {
                null_mut()
            };
            let single = if source.is_null() { crs } else { source };
            let cs = if single.is_null() {
                null_mut()
            } else {
                proj_crs_get_coordinate_system(ctx, single)
            };

            // the unit's factor to SI, i.e. meters or radians
            let mut factor = 0.0;
            let has_axis = !cs.is_null()
                && proj_cs_get_axis_info(
                    ctx,
                    cs,
                    0,
                    null_mut(),
                    null_mut(),
                    null_mut(),
                    &raw mut factor,
                    null_mut(),
                    null_mut(),
                    null_mut(),
                ) == 1;
            let angular = !cs.is_null()
                && proj_cs_get_type(ctx, cs) == PJ_COORDINATE_SYSTEM_TYPE_PJ_CS_TYPE_ELLIPSOIDAL;

            let ellipsoid = if angular {
                proj_get_ellipsoid(ctx, single)
            } else {
                null_mut()
            };
            let mut semi_major = 0.0;
            let has_ellipsoid = !ellipsoid.is_null()
                && proj_ellipsoid_get_parameters(
                    ctx,
                    ellipsoid,
                    &raw mut semi_major,
                    null_mut(),
                    null_mut(),
                    null_mut(),
                ) == 1;

            for object in [ellipsoid, cs, source, crs] {
                proj_destroy(object);
            }
            proj_context_destroy(ctx);

            (
                has_axis.then_some(factor),
                angular,
                has_ellipsoid.then_some(semi_major),
            )
        };

        let unresolvable = || error::Error::ProjStringUnresolvable {
            spatial_ref: self.def,
        };
        let factor = factor.ok_or_else(unresolvable)?;
        if angular {
            // radians per unit times the equatorial radius in meters
            Ok((semi_major.ok_or_else(unresolvable)? * factor, false))
        } else {
            Ok((factor, true))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn it_reads_the_unit_of_the_crs() {
        for (srs, uses_meters, meters_per_unit) in [
            ("EPSG:3857", true, 1.0),
            ("ESRI:54009", true, 1.0),
            ("SR-ORG:81", true, 1.0),
            // meters with a false easting of 60 500 000
            ("EPSG:3389", true, 1.0),
            ("EPSG:2964", false, 0.304_800_609_601_219_2), // US survey foot
            ("EPSG:3139", false, 0.201_168),               // link
            ("EPSG:4326", false, 111_319.490_793_273_58),
            // Clarke 1880 (international foot), whose axis proj.db stores in feet
            ("EPSG:4748", false, 111_322.446_850_446_34),
            // grads
            ("EPSG:4807", false, 100_189.304_147_424_89),
        ] {
            let provider =
                ProjMetadataProvider::new_known_crs(SpatialReference::from_str(srs).unwrap())
                    .unwrap();
            assert_eq!(provider.uses_meters().unwrap(), uses_meters, "{srs}");
            float_cmp::assert_approx_eq!(
                f64,
                provider.meters_per_unit().unwrap(),
                meters_per_unit,
                epsilon = 1e-6
            );
        }
    }
}
