use crate::raster::{
    typed_raster_tile::TypedRasterTile, DynamicRasterDataType, GridBlit, Pixel, Raster,
    RasterTile2D, TypedRasterTile2D,
};
use crate::util::Result;
use crate::{error, primitives::SpatialBounded};
use snafu::ensure;

pub trait Blit<R> {
    fn blit(&mut self, source: R) -> Result<()>;
}

impl<T: Pixel> Blit<RasterTile2D<T>> for RasterTile2D<T> {
    /// Copy `source` raster pixels into this raster, fails if the rasters do not overlap
    #[allow(clippy::float_cmp)]
    fn blit(&mut self, source: RasterTile2D<T>) -> Result<()> {
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

        let _ = self
            .spatial_bounds()
            .intersection(&source.spatial_bounds())
            .ok_or(error::Error::Blit {
                details: "No overlapping region".into(),
            })?;

        let offset = self
            .global_geo_transform
            .coordinate_2d_to_signed_grid_2d(source.spatial_bounds().upper_left());

        self.data.grid_blit_from(source.data, offset)?;
        Ok(())
    }
}

impl Blit<TypedRasterTile2D> for TypedRasterTile2D {
    fn blit(&mut self, source: TypedRasterTile2D) -> Result<()> {
        ensure!(
            self.raster_data_type() == source.raster_data_type(),
            error::NonMatchingRasterTypes {
                a: self.raster_data_type(),
                b: source.raster_data_type()
            }
        );
        match self {
            TypedRasterTile::U8(r) => r.blit(source.get_u8().expect("Must not fail!")),
            TypedRasterTile::U16(r) => r.blit(source.get_u16().expect("Must not fail!")),
            TypedRasterTile::U32(r) => r.blit(source.get_u32().expect("Must not fail!")),
            TypedRasterTile::U64(r) => r.blit(source.get_u64().expect("Must not fail!")),
            TypedRasterTile::I8(r) => r.blit(source.get_i8().expect("Must not fail!")),
            TypedRasterTile::I16(r) => r.blit(source.get_i16().expect("Must not fail!")),
            TypedRasterTile::I32(r) => r.blit(source.get_i32().expect("Must not fail!")),
            TypedRasterTile::I64(r) => r.blit(source.get_i64().expect("Must not fail!")),
            TypedRasterTile::F32(r) => r.blit(source.get_f32().expect("Must not fail!")),
            TypedRasterTile::F64(r) => r.blit(source.get_f64().expect("Must not fail!")),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        primitives::TimeInterval,
        raster::{Blit, GeoTransform, Raster2D, RasterTile2D},
    };

    #[test]
    fn test_blit_ur() {
        let dim = [4, 4];
        let data = vec![0; 16];
        let geo_transform = GeoTransform::new((0.0, 10.0).into(), 10.0 / 4.0, -10.0 / 4.0);
        let temporal_bounds: TimeInterval = TimeInterval::default();

        let r1 = Raster2D::new(dim.into(), data, None).unwrap();
        let mut t1 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r1);

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let geo_transform = GeoTransform::new((5.0, 15.0).into(), 10.0 / 4.0, -10.0 / 4.0);

        let r2 = Raster2D::new(dim.into(), data, None).unwrap();
        let t2 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r2);

        t1.blit(t2).unwrap();

        assert_eq!(
            t1.data.data_container,
            vec![0, 0, 8, 9, 0, 0, 12, 13, 0, 0, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn test_blit_ul() {
        let dim = [4, 4];
        let data = vec![0; 16];
        let geo_transform = GeoTransform::new((0.0, 10.0).into(), 10.0 / 4.0, -10.0 / 4.0);
        let temporal_bounds: TimeInterval = TimeInterval::default();

        let r1 = Raster2D::new(dim.into(), data, None).unwrap();
        let mut t1 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r1);

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let geo_transform = GeoTransform::new((-5.0, 15.0).into(), 10.0 / 4.0, -10.0 / 4.0);

        let r2 = Raster2D::new(dim.into(), data, None).unwrap();
        let t2 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r2);

        t1.blit(t2).unwrap();

        assert_eq!(
            t1.data.data_container,
            vec![10, 11, 0, 0, 14, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn test_blit_ll() {
        let dim = [4, 4];
        let data = vec![0; 16];
        let geo_transform = GeoTransform::new((0.0, 15.0).into(), 10.0 / 4.0, -10.0 / 4.0);
        let temporal_bounds: TimeInterval = TimeInterval::default();

        let r1 = Raster2D::new(dim.into(), data, None).unwrap();
        let mut t1 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r1);

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let geo_transform = GeoTransform::new((-5.0, 10.0).into(), 10.0 / 4.0, -10.0 / 4.0);

        let r2 = Raster2D::new(dim.into(), data, None).unwrap();
        let t2 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r2);

        t1.blit(t2).unwrap();

        assert_eq!(
            t1.data.data_container,
            vec![0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 6, 7, 0, 0]
        );
    }
}
