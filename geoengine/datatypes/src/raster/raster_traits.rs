use super::{
    BaseTile, GeoTransform, GridBoundingBox2D, GridBounds, GridContains, GridIdx2D,
    GridIndexAccess, GridShapeAccess, GridSize, Pixel,
};
use crate::{
    error,
    primitives::{Coordinate2D, SpatialBounded, TemporalBounded},
    util::Result,
};
use snafu::ensure;

pub trait Raster<D: GridSize, T: Pixel>:
    SpatialBounded + TemporalBounded + GridShapeAccess<ShapeArray = D::ShapeArray> + GeoTransformAccess
{
    type DataContainer;
    /// returns a reference to the data container used to hold the pixels / cells of the raster
    fn data_container(&self) -> &Self::DataContainer;
}

pub trait GeoTransformAccess {
    /// returns a reference to the geo transform describing the origin of the raster and the pixel size
    fn geo_transform(&self) -> GeoTransform;
}

/// This trait enables fast track access to pixel values at `Coordinate2D` locations of pixels.
pub trait CoordinatePixelAccess<P>
where
    P: Pixel,
{
    type Output;

    fn pixel_value_at_coord(&self, coordinate: Coordinate2D) -> Result<Self::Output>;

    fn pixel_value_at_coord_unchecked(&self, coordinate: Coordinate2D) -> Self::Output;
}

impl<G, P> CoordinatePixelAccess<P> for BaseTile<G>
where
    G: GridSize + Clone,
    P: Pixel,
    Self: GridIndexAccess<Option<P>, GridIdx2D>,
{
    type Output = Option<P>;

    fn pixel_value_at_coord(&self, coordinate: Coordinate2D) -> Result<Option<P>> {
        // TODO: benchmark the impact of creating the `GeoTransform`s

        // The core transform yields a core-anchored local index (negative for halo
        // pixels). Validate it against the tile's local pixel bounds, then add the
        // overlap offset to address the stored grid, whose origin is the data corner.
        let local_index = self
            .core_geo_transform()
            .coordinate_to_grid_idx_2d(coordinate);
        let bounds = self.local_total_pixel_bounds();
        let point = GridBoundingBox2D::new_unchecked(local_index, local_index);
        ensure!(
            bounds.contains(&point),
            error::GridIndexOutOfBounds {
                index: local_index.as_slice().to_vec(),
                min_index: bounds.min_index().as_slice().to_vec(),
                max_index: bounds.max_index().as_slice().to_vec(),
            }
        );

        self.get_at_grid_index(local_index + self.overlap_offset())
    }

    fn pixel_value_at_coord_unchecked(&self, coordinate: Coordinate2D) -> Option<P> {
        // Core-anchored local index (negative for halo pixels); add the overlap
        // offset to address the stored grid, whose origin is the data corner.
        let local_index = self
            .core_geo_transform()
            .coordinate_to_grid_idx_2d(coordinate);

        self.get_at_grid_index_unchecked(local_index + self.overlap_offset())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        primitives::{CacheHint, TimeInterval},
        raster::{Grid2D, RasterTile2D, TileIdx, TileInformation, TileOverlap, TileSize},
        util::test::TestDefault,
    };

    use super::*;

    #[test]
    fn coordinate_pixel_access() {
        fn validate_coordinate<C: Into<Coordinate2D> + Copy>(
            raster_tile: &RasterTile2D<i32>,
            coordinate: C,
        ) {
            let coordinate: Coordinate2D = coordinate.into();

            let grid_index = raster_tile
                .core_geo_transform()
                .coordinate_to_grid_idx_2d(coordinate)
                + raster_tile.overlap_offset();

            let value_a = raster_tile.pixel_value_at_coord(coordinate);

            let value_b = raster_tile.get_at_grid_index(grid_index);

            match (value_a, value_b) {
                (Ok(a), Ok(b)) => assert_eq!(a, b),
                (Err(e1), Err(e2)) => assert_eq!(format!("{e1:?}"), format!("{e2:?}")),
                (Err(e), _) | (_, Err(e)) => panic!("{}", e.to_string()),
            }
        }

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                overlap: TileOverlap::zero(),
                global_geo_transform: TestDefault::test_default(),
                tile_position: TileIdx::new_y_x(0, 0),
                tile_size: TileSize::new_y_x(3, 2),
            },
            0,
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
            CacheHint::default(),
        );

        validate_coordinate(&raster_tile, (0.0, 0.0));
        validate_coordinate(&raster_tile, (1.0, 0.0));

        assert_eq!(
            raster_tile.pixel_value_at_coord_unchecked((0.0, 0.0).into()),
            Some(1)
        );
        assert_eq!(
            raster_tile.pixel_value_at_coord_unchecked((1.0, 0.0).into()),
            Some(2)
        );

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                overlap: TileOverlap::zero(),
                global_geo_transform: TestDefault::test_default(),
                tile_position: TileIdx::new_y_x(1, 1),
                tile_size: TileSize::new_y_x(3, 2),
            },
            0,
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
            CacheHint::default(),
        );

        validate_coordinate(&raster_tile, (0.0, 0.0));
        validate_coordinate(&raster_tile, (2.0, -3.0));
        validate_coordinate(&raster_tile, (3.0, -3.0));

        assert_eq!(
            raster_tile.pixel_value_at_coord_unchecked((2.0, -3.0).into()),
            Some(1)
        );
        assert_eq!(
            raster_tile.pixel_value_at_coord_unchecked((3.0, -3.0).into()),
            Some(2)
        );
    }

    #[test]
    fn coordinate_pixel_access_with_overlap() {
        // Core of [2,2] with a (1,1) overlap halo -> the stored grid is [4,4].
        // Stored value at [y][x] is `y * 4 + x`. The core occupies the inner
        // 2x2 block (stored rows/cols 1..2); halo pixels live in the outer ring
        // and map to *negative* core indices.
        let grid = Grid2D::new([4, 4].into(), (0..16).collect::<Vec<i32>>())
            .unwrap()
            .into();

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                overlap: TileOverlap::new(1, 1),
                global_geo_transform: TestDefault::test_default(),
                tile_position: TileIdx::new_y_x(0, 0),
                tile_size: TileSize([2, 2].into()),
            },
            0,
            grid,
            CacheHint::default(),
        );

        // Core pixel (0,0) -> stored (1,1) = 5
        assert_eq!(
            raster_tile.pixel_value_at_coord_unchecked((0.0, 0.0).into()),
            Some(5)
        );
        // Core pixel (0,1) -> stored (1,2) = 6
        assert_eq!(
            raster_tile.pixel_value_at_coord_unchecked((1.0, 0.0).into()),
            Some(6)
        );
        // Halo pixel: coordinate (0,1) -> core (-1,0) -> stored (0,1) = 1
        assert_eq!(
            raster_tile.pixel_value_at_coord_unchecked((0.0, 1.0).into()),
            Some(1)
        );
        // Core pixel (1,1) -> stored (2,2) = 10
        assert_eq!(
            raster_tile.pixel_value_at_coord_unchecked((1.0, -1.0).into()),
            Some(10)
        );
    }

    #[test]
    fn coordinate_pixel_access_checked_respects_local_bounds() {
        // Core of [2,2] with a (1,1) overlap halo -> the stored grid is [4,4] and the
        // core-anchored local total bounds are min (-1,-1), max (2,2). The checked
        // access must succeed exactly when the coordinate's local index lies inside
        // those local bounds (which differ from the stored shape in the halo ring).
        let grid = Grid2D::new([4, 4].into(), (0..16).collect::<Vec<i32>>())
            .unwrap()
            .into();

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                overlap: TileOverlap::new(1, 1),
                global_geo_transform: TestDefault::test_default(),
                tile_position: TileIdx::new_y_x(0, 0),
                tile_size: TileSize([2, 2].into()),
            },
            0,
            grid,
            CacheHint::default(),
        );

        let transform = raster_tile.core_geo_transform();
        let local_bounds = raster_tile.local_total_pixel_bounds();
        assert_eq!(local_bounds.min_index(), GridIdx2D::new([-1, -1]));
        assert_eq!(local_bounds.max_index(), GridIdx2D::new([2, 2]));

        let mut saw_in_bounds = false;
        let mut saw_out_of_bounds = false;

        for y in -3..=4 {
            for x in -3..=4 {
                let local_index = GridIdx2D::new([y, x]);
                let coordinate = transform.grid_idx_to_pixel_upper_left_coordinate_2d(local_index);

                // The upper-left corner coordinate maps back to the same local index.
                assert_eq!(transform.coordinate_to_grid_idx_2d(coordinate), local_index);

                let expected_in_bounds = local_bounds
                    .contains(&GridBoundingBox2D::new_unchecked(local_index, local_index));

                let result = raster_tile.pixel_value_at_coord(coordinate);
                match &result {
                    Ok(_) => saw_in_bounds = true,
                    Err(_) => saw_out_of_bounds = true,
                }
                assert_eq!(
                    result.is_ok(),
                    expected_in_bounds,
                    "checked access should succeed iff the local index is inside the local bounds"
                );
            }
        }

        // Ensure the sweep actually covered both in-bounds and out-of-bounds pixels.
        assert!(
            saw_in_bounds,
            "expected at least one in-bounds pixel in the sweep"
        );
        assert!(
            saw_out_of_bounds,
            "expected at least one out-of-bounds pixel in the sweep"
        );
    }
}
