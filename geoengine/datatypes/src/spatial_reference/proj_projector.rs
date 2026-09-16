use proj::Proj;
use proj_sys::{
    proj_context_create, proj_context_destroy, proj_create, proj_destroy,
    proj_ellipsoid_get_parameters, proj_get_ellipsoid,
};
use snafu::ResultExt;
use std::ffi::CString;
use tracing::instrument;

use crate::{
    error::{self, BoxedResultExt},
    operations::reproject::Reproject,
    primitives::{AxisAlignedRectangle, Coordinate2D},
    spatial_reference::{CoordinateProjection, CrsMetadataProvider, SpatialReference},
    util::Result,
};

pub struct ProjCoordinateProjector {
    pub from: SpatialReference,
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
        let proj_string = self.def.proj_string()?;

        if proj_string.contains("+units=m") {
            return Ok(true);
        }

        let proj = Proj::new_known_crs("EPSG:4326", &proj_string, None).map_err(|_| {
            error::Error::InvalidProjDefinition {
                proj_definition: proj_string.clone(),
            }
        })?;

        // Using 500,000 Easting (UTM Center) and 100,000 Northing (just North of the Equator/Origin)
        let (Ok(coord0), Ok(coord1)) = (
            proj.project((500_000.0, 100_000.0), true),
            proj.project((500_001.0, 100_000.0), true),
        ) else {
            // If the projection cannot handle these coordinates, it's likely not in meters
            return Ok(false);
        };

        // If it handles meters, moving 1 meter changes the output degrees by a microscopic amount
        let delta = f64::abs(coord1.0 - coord0.0);
        Ok(delta < 0.1)
    }

    fn meters_per_unit(&self) -> Result<f64> {
        if self.uses_meters()? {
            return Ok(1.0);
        }

        let proj_string =
            CString::new(self.def.proj_string()?).boxed_context(error::ProjInternal2)?;
        let mut meters_per_degree = None;

        unsafe {
            // 1. Initialize the PROJ context and instantiate the CRS
            let ctx = proj_context_create();
            let crs = proj_create(ctx, proj_string.as_ptr());

            if crs.is_null() {
                proj_context_destroy(ctx);
                return Err(error::Error::ProjStringUnresolvable {
                    spatial_ref: self.def,
                });
            }

            // 2. Fetch the underlying ellipsoid object from the CRS
            let ellipsoid = proj_get_ellipsoid(ctx, crs);

            if ellipsoid.is_null() {
                proj_destroy(crs);
                proj_context_destroy(ctx);
                return Err(error::Error::ProjStringUnresolvable {
                    spatial_ref: self.def,
                });
            }

            let mut semi_major: f64 = 0.0;
            let mut semi_minor: f64 = 0.0;
            let mut is_semi_minor_computed: i32 = 0;
            let mut inv_flattening: f64 = 0.0;

            // 3. Extract the semi-major axis (Equatorial Radius)
            let success = proj_ellipsoid_get_parameters(
                ctx,
                ellipsoid,
                &raw mut semi_major,
                &raw mut semi_minor,
                &raw mut is_semi_minor_computed,
                &raw mut inv_flattening,
            );

            if success == 1 {
                // 4. Calculate the Equatorial Perimeter divided by 360 degrees
                // WGS84 Semi-major axis (semi_major) = 6378137.0 meters
                let equatorial_perimeter = semi_major * 2.0 * std::f64::consts::PI;
                meters_per_degree = Some(equatorial_perimeter / 360.0);
            }

            // Clean up the main context, CRS and ellipsoid structures
            proj_destroy(ellipsoid);
            proj_destroy(crs);
            proj_context_destroy(ctx);
        }

        if let Some(meters) = meters_per_degree {
            Ok(meters)
        } else {
            Err(error::Error::ProjStringUnresolvable {
                spatial_ref: self.def,
            })
        }
    }
}
