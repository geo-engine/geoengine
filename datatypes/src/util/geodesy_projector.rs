use crs_definitions;
use geodesy::{
    coord::{AngularUnits, CoordinateTuple},
    ctx::Context,
};
use snafu::Snafu;
use strum::IntoStaticStr;

use crate::{
    operations::reproject::CoordinateProjection,
    primitives::Coordinate2D,
    spatial_reference::{SpatialReference, SpatialReferenceAuthority},
};

#[derive(Debug, Snafu, IntoStaticStr)]
pub enum Error {
    Geodesy { source: geodesy::Error },
    UnknownCrsCode { code: u32 },
    UnknownCrsAuthority { authority: String },
}

impl From<geodesy::Error> for Error {
    fn from(source: geodesy::Error) -> Self {
        Error::Geodesy { source }
    }
}

#[derive(Debug, Clone)]
pub struct Transformer<C: geodesy::ctx::Context = geodesy::ctx::Minimal> {
    ctx: C,
    source_crs: u16,
    source: geodesy::ctx::OpHandle,
    source_latlon: bool,
    target_crs: u16,
    target: geodesy::ctx::OpHandle,
    target_latlon: bool,
}

impl Transformer<geodesy::ctx::Minimal> {
    pub fn from_spatial_references(
        source: SpatialReference,
        target: SpatialReference,
    ) -> Result<Self, Error> {
        if !matches!(source.authority(), SpatialReferenceAuthority::Epsg) {
            return Err(Error::UnknownCrsAuthority {
                authority: source.authority().to_string(),
            });
        }

        if !matches!(target.authority(), SpatialReferenceAuthority::Epsg) {
            return Err(Error::UnknownCrsAuthority {
                authority: target.authority().to_string(),
            });
        }

        if source.code() > u32::from(u16::MAX) {
            return Err(Error::UnknownCrsCode {
                code: source.code(),
            });
        }

        if target.code() > u32::from(u16::MAX) {
            return Err(Error::UnknownCrsCode {
                code: target.code(),
            });
        }

        Transformer::from_epsg_codes(source.code() as u16, target.code() as u16)
    }

    pub fn from_epsg_codes(source_crs: u16, target_crs: u16) -> Result<Self, Error> {
        let start_time = std::time::Instant::now();

        let source_def = crs_definitions::from_code(source_crs).ok_or(Error::UnknownCrsCode {
            code: u32::from(source_crs),
        })?;
        let target_def = crs_definitions::from_code(target_crs).ok_or(Error::UnknownCrsCode {
            code: u32::from(target_crs),
        })?;
        let source_latlon = source_def.proj4.starts_with("+proj=longlat");
        let target_latlon = target_def.proj4.starts_with("+proj=longlat");

        let source_geodesy_string = geodesy::authoring::parse_proj(source_def.proj4)?;
        let target_geodesy_string = geodesy::authoring::parse_proj(target_def.proj4)?;

        tracing::trace!(
            "Geodesy source / target string: {source_geodesy_string} / {target_geodesy_string}"
        );

        let mut ctx = geodesy_ctx();

        let source = ctx.op(&source_geodesy_string)?;
        let target = ctx.op(&target_geodesy_string)?;

        let transformer = Transformer {
            ctx,
            source_crs,
            source,
            source_latlon,
            target_crs,
            target,
            target_latlon,
        };

        tracing::trace!(
            "Transformer::from_epsg took {}",
            start_time.elapsed().as_nanos()
        );

        Ok(transformer)
    }

    pub fn transform_coord(&self, coord: Coordinate2D) -> Result<Coordinate2D, Error> {
        let mut geodesy_coord = if self.source_latlon {
            [geodesy::coord::Coor2D::gis(coord.x, coord.y)]
        } else {
            [geodesy::coord::Coor2D::raw(coord.x, coord.y)]
        };
        self.ctx
            .apply(self.source, geodesy::Direction::Inv, &mut geodesy_coord)?;
        self.ctx
            .apply(self.target, geodesy::Direction::Fwd, &mut geodesy_coord)?;

        let (x, y) = if self.target_latlon {
            geodesy_coord[0].to_degrees().xy()
        } else {
            geodesy_coord[0].xy()
        };

        Ok(Coordinate2D::new(x, y))
    }
}

impl<C: geodesy::ctx::Context> Transformer<C> {
    pub fn transform_coords(&self, coords: &[Coordinate2D]) -> Result<Vec<Coordinate2D>, Error> {
        let mut geodesy_coords: Vec<geodesy::coord::Coor2D> = coords
            .iter()
            .map(|c| geodesy::coord::Coor2D::raw(c.x, c.y))
            .collect();

        if self.source_latlon {
            for c in &mut geodesy_coords {
                *c = c.to_radians();
            }
        }

        self.ctx
            .apply(self.source, geodesy::Direction::Inv, &mut geodesy_coords)?;
        self.ctx
            .apply(self.target, geodesy::Direction::Fwd, &mut geodesy_coords)?;

        if self.target_latlon {
            for c in &mut geodesy_coords {
                *c = c.to_degrees();
            }
        }

        let transformed_coords: Result<Vec<Coordinate2D>, Error> = geodesy_coords
            .iter()
            .map(|c| Ok(Coordinate2D::new(c.x(), c.y())))
            .collect();

        transformed_coords
    }

    /*
    pub async fn transform_coords_parallel(
        &self,
        coords: &[Coordinate2D],
    ) -> Result<Vec<Coordinate2D>, Error>
    where
        geodesy::coord::Coor2D: Sized,
    {
        const PAR_CHUNK_SIZE: usize = 64;

        let mut geodesy_coords: Vec<(f64, f64)> = coords.iter().map(|c| (c.x, c.y)).collect();

        if self.source_latlon {
            for c in geodesy_coords.as_mut_slice() {
                *c = c.to_radians();
            }
        }

        geodesy_coords.chunks_mut(PAR_CHUNK_SIZE).for_each(|chunk| {
            self.ctx
                .apply(self.source, geodesy::Direction::Inv, chunk)
                .unwrap();

            self.ctx
                .apply(self.target, geodesy::Direction::Fwd, chunk)
                .unwrap();
        });

        if self.target_latlon {
            for c in geodesy_coords.as_mut_slice() {
                *c = c.to_degrees();
            }
        }

        let transformed_coords: Result<Vec<Coordinate2D>, Error> = geodesy_coords
            .iter()
            .map(|c| Ok(Coordinate2D::new(c.x(), c.y())))
            .collect();

        Ok(transformed_coords?)
    }
    */
}

pub fn geodesy_ctx() -> geodesy::ctx::Minimal {
    geodesy::ctx::Minimal::new()
}

impl CoordinateProjection for Transformer<geodesy::ctx::Minimal> {
    fn from_known_srs(from: SpatialReference, to: SpatialReference) -> super::Result<Self>
    where
        Self: Sized,
    {
        let start_time: std::time::Instant = std::time::Instant::now();

        let t = Transformer::from_spatial_references(from, to)?;

        tracing::trace!(
            "CoordinateProjection::from_known_srs Geodesy creation took {}",
            start_time.elapsed().as_nanos()
        );

        Ok(t)
    }

    fn project_coordinate(&self, c: Coordinate2D) -> super::Result<Coordinate2D> {
        Ok(self.transform_coord(c)?)
    }

    fn project_coordinates<A: AsRef<[Coordinate2D]>>(
        &self,
        coords: A,
    ) -> super::Result<Vec<Coordinate2D>> {
        let start = std::time::Instant::now();

        let res = self.transform_coords(coords.as_ref())?;

        tracing::trace!(
            "CoordinateProjection::project_coordinates Geodesy convert_array took {}",
            start.elapsed().as_nanos()
        );

        Ok(res)
    }

    fn source_srs(&self) -> SpatialReference {
        SpatialReference::new(SpatialReferenceAuthority::Epsg, u32::from(self.source_crs))
    }

    fn target_srs(&self) -> SpatialReference {
        SpatialReference::new(SpatialReferenceAuthority::Epsg, u32::from(self.target_crs))
    }
}

#[cfg(test)]
mod tests {
    use float_cmp::approx_eq;

    use crate::util::well_known_data::{MARBURG_EPSG_3857, MARBURG_EPSG_4326};

    use super::*;

    #[test]
    fn transform_coordinate_4326_3857() -> Result<(), Error> {
        let from = SpatialReference::epsg_4326();
        let to = SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857);
        let transformer = Transformer::from_spatial_references(from, to)?;

        let rp = transformer.transform_coord(MARBURG_EPSG_4326)?;

        assert!(approx_eq!(f64, rp.x, MARBURG_EPSG_3857.x));
        assert!(approx_eq!(f64, rp.y, MARBURG_EPSG_3857.y));
        Ok(())
    }
}
