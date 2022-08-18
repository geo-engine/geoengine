use crate::error;
use crate::raster::{
    ChangeGridBounds, GeoTransformAccess, GridBlit, GridIdx2D, MaterializedRasterTile2D, Pixel,
    RasterTile2D,
};
use crate::util::Result;

use snafu::ensure;

pub trait Blit<R> {
    fn blit(&mut self, source: R) -> Result<()>;
}

impl<T: Pixel> Blit<RasterTile2D<T>> for MaterializedRasterTile2D<T> {
    /// Copy `source` raster pixels into this raster, fails if the rasters do not overlap
    #[allow(clippy::float_cmp)]
    fn blit(&mut self, source: RasterTile2D<T>) -> Result<()> {
        // TODO: same crs
        // TODO: allow approximately equal pixel sizes?
        // TODO: ensure pixels are aligned

        let into_geo_transform = self.geo_transform();
        let from_geo_transform = source.geo_transform();

        ensure!(
            (self.geo_transform().x_pixel_size() == source.geo_transform().x_pixel_size())
                && (self.geo_transform().y_pixel_size() == source.geo_transform().y_pixel_size()),
            error::Blit {
                details: "Incompatible pixel size"
            }
        );

        let offset = from_geo_transform.origin_coordinate - into_geo_transform.origin_coordinate;

        let offset_x_pixels = (offset.x / into_geo_transform.x_pixel_size()).round() as isize;
        let offset_y_pixels = (offset.y / into_geo_transform.y_pixel_size()).round() as isize;

        /*
        ensure!(
            offset_x_pixels.abs() <= self.grid_array.axis_size_x() as isize
                && offset_y_pixels.abs() <= self.grid_array.axis_size_y() as isize,
            error::Blit {
                details: "No overlapping region",
            }
        );
        */

        let origin_offset_pixels = GridIdx2D::new([offset_y_pixels, offset_x_pixels]);

        let tile_offset_pixels = source.tile_information().global_upper_left_pixel_idx()
            - self.tile_information().global_upper_left_pixel_idx();
        let global_offset_pixels = origin_offset_pixels + tile_offset_pixels;

        let shifted_source = source.grid_array.shift_by_offset(global_offset_pixels);

        self.grid_array.grid_blit_from(&shifted_source);

        Ok(())
    }
}

impl<T: Pixel> Blit<RasterTile2D<T>> for RasterTile2D<T> {
    /// Copy `source` raster pixels into this raster, fails if the rasters do not overlap
    #[allow(clippy::float_cmp)]
    fn blit(&mut self, source: RasterTile2D<T>) -> Result<()> {
        // TODO: same crs
        // TODO: allow approximately equal pixel sizes?
        // TODO: ensure pixels are aligned

        let into_geo_transform = self.geo_transform();
        let from_geo_transform = source.geo_transform();

        ensure!(
            (self.geo_transform().x_pixel_size() == source.geo_transform().x_pixel_size())
                && (self.geo_transform().y_pixel_size() == source.geo_transform().y_pixel_size()),
            error::Blit {
                details: "Incompatible pixel size"
            }
        );

        let offset = from_geo_transform.origin_coordinate - into_geo_transform.origin_coordinate;

        let offset_x_pixels = (offset.x / into_geo_transform.x_pixel_size()).round() as isize;
        let offset_y_pixels = (offset.y / into_geo_transform.y_pixel_size()).round() as isize;

        /*
        ensure!(
            offset_x_pixels.abs() <= self.grid_array.axis_size_x() as isize
                && offset_y_pixels.abs() <= self.grid_array.axis_size_y() as isize,
            error::Blit {
                details: "No overlapping region",
            }
        );
        */

        let origin_offset_pixels = GridIdx2D::new([offset_y_pixels, offset_x_pixels]);

        let tile_offset_pixels = source.tile_information().global_upper_left_pixel_idx()
            - self.tile_information().global_upper_left_pixel_idx();
        let global_offset_pixels = origin_offset_pixels + tile_offset_pixels;

        let shifted_source = source.grid_array.shift_by_offset(global_offset_pixels);

        self.grid_array.grid_blit_from(&shifted_source);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        primitives::TimeInterval,
        raster::{Blit, GeoTransform, Grid2D, RasterTile2D},
    };

    #[test]
    fn test_blit_ur_materialized() {
        let dim = [4, 4];
        let data = vec![0; 16];
        let geo_transform = GeoTransform::new((0.0, 10.0).into(), 10.0 / 4.0, -10.0 / 4.0);
        let temporal_bounds: TimeInterval = TimeInterval::default();

        let r1 = Grid2D::new(dim.into(), data).unwrap();
        let mut t1 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r1)
            .into_materialized_tile();

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let geo_transform = GeoTransform::new((5.0, 15.0).into(), 10.0 / 4.0, -10.0 / 4.0);

        let r2 = Grid2D::new(dim.into(), data).unwrap();
        let t2 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r2);

        t1.blit(t2).unwrap();

        assert_eq!(
            t1.grid_array.inner_grid.data,
            vec![0, 0, 8, 9, 0, 0, 12, 13, 0, 0, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn test_blit_ul() {
        let dim = [4, 4];
        let data = vec![0; 16];
        let geo_transform = GeoTransform::new((0.0, 10.0).into(), 10.0 / 4.0, -10.0 / 4.0);
        let temporal_bounds: TimeInterval = TimeInterval::default();

        let r1 = Grid2D::new(dim.into(), data).unwrap();
        let mut t1 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r1)
            .into_materialized_tile();

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let geo_transform = GeoTransform::new((-5.0, 15.0).into(), 10.0 / 4.0, -10.0 / 4.0);

        let r2 = Grid2D::new(dim.into(), data).unwrap();
        let t2 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r2);

        t1.blit(t2).unwrap();

        assert_eq!(
            t1.grid_array.inner_grid.data,
            vec![10, 11, 0, 0, 14, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn test_blit_ll() {
        let dim = [4, 4];
        let data = vec![0; 16];
        let geo_transform = GeoTransform::new((0.0, 15.0).into(), 10.0 / 4.0, -10.0 / 4.0);
        let temporal_bounds: TimeInterval = TimeInterval::default();

        let r1 = Grid2D::new(dim.into(), data).unwrap();
        let mut t1 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r1)
            .into_materialized_tile();

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let geo_transform = GeoTransform::new((-5.0, 10.0).into(), 10.0 / 4.0, -10.0 / 4.0);

        let r2 = Grid2D::new(dim.into(), data).unwrap();
        let t2 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r2);

        t1.blit(t2).unwrap();

        assert_eq!(
            t1.grid_array.inner_grid.data,
            vec![0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 6, 7, 0, 0]
        );
    }

    #[test]
    fn test_blit_ur() {
        let dim = [4, 4];
        let data = vec![0; 16];
        let geo_transform = GeoTransform::new((0.0, 10.0).into(), 10.0 / 4.0, -10.0 / 4.0);
        let temporal_bounds: TimeInterval = TimeInterval::default();

        let r1 = Grid2D::new(dim.into(), data).unwrap();
        let mut t1 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r1);

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let geo_transform = GeoTransform::new((5.0, 15.0).into(), 10.0 / 4.0, -10.0 / 4.0);

        let r2 = Grid2D::new(dim.into(), data).unwrap();
        let t2 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r2);

        t1.blit(t2).unwrap();

        assert!(!t1.is_empty());

        let masked_grid = match t1.grid_array {
            crate::raster::GridOrEmpty::Grid(g) => g,
            crate::raster::GridOrEmpty::Empty(_) => panic!("exppected a materialized grid"),
        };

        assert_eq!(
            masked_grid.inner_grid.data,
            vec![0, 0, 8, 9, 0, 0, 12, 13, 0, 0, 0, 0, 0, 0, 0, 0]
        );
    }
}
