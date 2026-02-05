use proj::Proj;
use snafu::ResultExt;
use tracing::instrument;

use crate::{
    error,
    operations::reproject::{CoordinateProjection, Reproject},
    primitives::{AxisAlignedRectangle, Coordinate2D},
    spatial_reference::{AreaOfUseProvider, SpatialReference},
    util::result::Result,
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

pub struct ProjAreaOfUseProvider {
    proj: Proj,
    def: SpatialReference,
}

impl AreaOfUseProvider for ProjAreaOfUseProvider {
    fn new_known_crs(def: SpatialReference) -> Result<Self>
    where
        Self: Sized,
    {
        let proj = Proj::new(&def.proj_string()?)
            .map_err(|_| error::Error::ProjStringUnresolvable { spatial_ref: def })?;

        Ok(ProjAreaOfUseProvider { proj, def })
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
}
