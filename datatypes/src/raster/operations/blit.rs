use crate::raster::{DynamicRasterDataType, GridIndex, Raster, Raster2D, TypedRaster2D};
use crate::util::Result;
use crate::{error, primitives::SpatialBounded};
use snafu::ensure;

pub trait Blit<R> {
    fn blit(&mut self, source: R) -> Result<()>;
}

impl<T: Copy> Blit<Raster2D<T>> for Raster2D<T> {
    /// Copy `source` raster pixels into this raster, fails if the rasters do not overlap
    #[allow(clippy::float_cmp)]
    fn blit(&mut self, source: Raster2D<T>) -> Result<()> {
        // TODO: same crs
        // TODO: allow approximately equal pixel sizes?
        // TODO: ensure pixels are aligned
        ensure!(
            (self.geo_transform().x_pixel_size == source.geo_transform().x_pixel_size)
                && (self.geo_transform().y_pixel_size == source.geo_transform().y_pixel_size),
            error::Blit {
                details: "Incompatible pixel size"
            }
        );

        let intersection = self
            .spatial_bounds()
            .intersection(&source.spatial_bounds())
            .ok_or(error::Error::Blit {
                details: "No overlapping region".into(),
            })?;

        let (start_y, start_x) = self
            .geo_transform()
            .coordinate_2d_to_grid_2d(&intersection.upper_left());
        let (stop_y, stop_x) = self
            .geo_transform()
            .coordinate_2d_to_grid_2d(&intersection.lower_right());

        let (start_source_y, start_source_x) = source
            .geo_transform()
            .coordinate_2d_to_grid_2d(&intersection.upper_left());

        // TODO: check if dimension of self and source fit?
        let width = stop_x - start_x;

        for y in 0..stop_y - start_y {
            let index = (start_y + y, start_x).grid_index_to_1d_index_unchecked(&self.dimension());
            let index_source = (start_source_y + y, start_source_x)
                .grid_index_to_1d_index_unchecked(&source.dimension());

            self.data_container.as_mut_slice()[index..index + width].copy_from_slice(
                &source.data_container().as_slice()[index_source..index_source + width],
            );
        }

        Ok(())
    }
}

impl Blit<TypedRaster2D> for TypedRaster2D {
    fn blit(&mut self, source: TypedRaster2D) -> Result<()> {
        ensure!(
            self.raster_data_type() == source.raster_data_type(),
            error::NonMatchingRasterTypes {
                a: self.raster_data_type(),
                b: source.raster_data_type()
            }
        );
        match self {
            crate::raster::typed_raster::TypedRasterNDim::U8(r) => {
                r.blit(source.get_u8().expect("Must not fail!"))
            }
            crate::raster::typed_raster::TypedRasterNDim::U16(r) => {
                r.blit(source.get_u16().expect("Must not fail!"))
            }
            crate::raster::typed_raster::TypedRasterNDim::U32(r) => {
                r.blit(source.get_u32().expect("Must not fail!"))
            }
            crate::raster::typed_raster::TypedRasterNDim::U64(r) => {
                r.blit(source.get_u64().expect("Must not fail!"))
            }
            crate::raster::typed_raster::TypedRasterNDim::I8(r) => {
                r.blit(source.get_i8().expect("Must not fail!"))
            }
            crate::raster::typed_raster::TypedRasterNDim::I16(r) => {
                r.blit(source.get_i16().expect("Must not fail!"))
            }
            crate::raster::typed_raster::TypedRasterNDim::I32(r) => {
                r.blit(source.get_i32().expect("Must not fail!"))
            }
            crate::raster::typed_raster::TypedRasterNDim::I64(r) => {
                r.blit(source.get_i64().expect("Must not fail!"))
            }
            crate::raster::typed_raster::TypedRasterNDim::F32(r) => {
                r.blit(source.get_f32().expect("Must not fail!"))
            }
            crate::raster::typed_raster::TypedRasterNDim::F64(r) => {
                r.blit(source.get_f64().expect("Must not fail!"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        primitives::TimeInterval,
        raster::{Blit, GeoTransform, Raster, Raster2D},
    };

    #[test]
    fn test_blit() {
        let dim = [4, 4];
        let data = vec![0; 16];
        let geo_transform = GeoTransform::new((0.0, 10.0).into(), 10.0 / 4.0, -10.0 / 4.0);
        let temporal_bounds: TimeInterval = TimeInterval::default();

        let mut r1 = Raster2D::new(dim.into(), data, None, temporal_bounds, geo_transform).unwrap();

        let data = vec![7; 16];
        let geo_transform = GeoTransform::new((5.0, 15.0).into(), 10.0 / 4.0, -10.0 / 4.0);

        let r2 = Raster2D::new(dim.into(), data, None, temporal_bounds, geo_transform).unwrap();

        r1.blit(r2).unwrap();

        assert_eq!(
            *r1.data_container(),
            vec![0, 0, 7, 7, 0, 0, 7, 7, 0, 0, 0, 0, 0, 0, 0, 0]
        );
    }
}
