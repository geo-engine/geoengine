use crate::collections::ToGeoJson;
use crate::error;
use crate::primitives::{AxisAlignedRectangle, SpatialPartitioned};
use crate::raster::{
    ChangeGridBounds, GeoTransformAccess, GridBlit, GridSize, MaterializedRasterTile2D, Pixel,
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

        ensure!(
            (self.geo_transform().x_pixel_size() == source.geo_transform().x_pixel_size())
                && (self.geo_transform().y_pixel_size() == source.geo_transform().y_pixel_size()),
            error::Blit {
                details: "Incompatible pixel size"
            }
        );

        ensure!(
            self.spatial_partition()
                .intersects(&source.spatial_partition()),
            error::Blit {
                details: "No overlapping region",
            }
        );

        let offset = self
            .tile_geo_transform()
            .coordinate_to_grid_idx_2d(source.spatial_partition().upper_left());

        crate::util::test::save_test_bytes(
            source.to_feature_collection()?.to_geo_json().as_bytes(),
            &format!(
                "source_tile_{}_{}.json",
                offset.inner()[1],
                offset.inner()[0]
            ),
        );
        // let offset_r = self
        //     .tile_geo_transform()
        //     .coordinate_to_grid_idx_2d(source.spatial_partition().upper_right());

        // if (offset.inner()[1] - offset_r.inner()[1]).abs() as usize
        //     != source
        //         .tile_information()
        //         .tile_size_in_pixels()
        //         .axis_size_x()
        // {
        //     dbg!("HENLO");
        // }

        let offset_l = self
            .tile_geo_transform()
            .coordinate_to_grid_idx_2d(source.spatial_partition().lower_right());

        // if (offset.inner()[0] - offset_l.inner()[0]).abs() as usize
        //     != source
        //         .tile_information()
        //         .tile_size_in_pixels()
        //         .axis_size_y()
        // {
        //     dbg!("HENLO2");
        // }

        // dbg!(self.tile_geo_transform());
        // dbg!(offset);

        let shifted_source = source.grid_array.shift_by_offset(offset);

        let mut s = String::new();

        // for (idx, coord) in self.grid_array.data.iter().enumerate() {
        //     if idx % 32 == 0 {
        //         s.push('\n');
        //     }
        //     match coord {
        //         v => s.push_str(&format!("{:?}", v)),
        //         a if self.grid_array.no_data_value.unwrap() == *a => s.push(' '),
        //     }
        // }

        // eprintln!("my grid b4 blit::::\n{}", s);

        self.grid_array.grid_blit_from(shifted_source);

        let mut s = String::new();

        // for (idx, coord) in self.grid_array.data.iter().enumerate() {
        //     if idx % 32 == 0 {
        //         s.push('\n');
        //     }
        //     match coord {
        //         _ => s.push('█'),
        //         a if self.grid_array.no_data_value.unwrap() == *a => s.push(' '),
        //     }
        // }

        // eprintln!("my grid after blit::::\n{}", s);

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
    fn test_blit_ur() {
        let dim = [4, 4];
        let data = vec![0; 16];
        let geo_transform = GeoTransform::new((0.0, 10.0).into(), 10.0 / 4.0, -10.0 / 4.0);
        let temporal_bounds: TimeInterval = TimeInterval::default();

        let r1 = Grid2D::new(dim.into(), data, None).unwrap();
        let mut t1 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r1)
            .into_materialized_tile();

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let geo_transform = GeoTransform::new((5.0, 15.0).into(), 10.0 / 4.0, -10.0 / 4.0);

        let r2 = Grid2D::new(dim.into(), data, None).unwrap();
        let t2 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r2);

        t1.blit(t2).unwrap();

        assert_eq!(
            t1.grid_array.data,
            vec![0, 0, 8, 9, 0, 0, 12, 13, 0, 0, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn test_blit_ul() {
        let dim = [4, 4];
        let data = vec![0; 16];
        let geo_transform = GeoTransform::new((0.0, 10.0).into(), 10.0 / 4.0, -10.0 / 4.0);
        let temporal_bounds: TimeInterval = TimeInterval::default();

        let r1 = Grid2D::new(dim.into(), data, None).unwrap();
        let mut t1 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r1)
            .into_materialized_tile();

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let geo_transform = GeoTransform::new((-5.0, 15.0).into(), 10.0 / 4.0, -10.0 / 4.0);

        let r2 = Grid2D::new(dim.into(), data, None).unwrap();
        let t2 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r2);

        t1.blit(t2).unwrap();

        assert_eq!(
            t1.grid_array.data,
            vec![10, 11, 0, 0, 14, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn test_blit_ll() {
        let dim = [4, 4];
        let data = vec![0; 16];
        let geo_transform = GeoTransform::new((0.0, 15.0).into(), 10.0 / 4.0, -10.0 / 4.0);
        let temporal_bounds: TimeInterval = TimeInterval::default();

        let r1 = Grid2D::new(dim.into(), data, None).unwrap();
        let mut t1 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r1)
            .into_materialized_tile();

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let geo_transform = GeoTransform::new((-5.0, 10.0).into(), 10.0 / 4.0, -10.0 / 4.0);

        let r2 = Grid2D::new(dim.into(), data, None).unwrap();
        let t2 = RasterTile2D::new_without_offset(temporal_bounds, geo_transform, r2);

        t1.blit(t2).unwrap();

        assert_eq!(
            t1.grid_array.data,
            vec![0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 6, 7, 0, 0]
        );
    }
}
