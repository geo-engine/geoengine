use super::{BaseTile, GeoTransform, GridIdx2D, GridIndexAccess, GridShapeAccess, GridSize, Pixel};
use crate::{
    primitives::{Coordinate2D, SpatialBounded, TemporalBounded},
    util::Result,
};

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

        let grid_index = self
            .tile_geo_transform()
            .coordinate_to_grid_idx_2d(coordinate);

        self.get_at_grid_index(grid_index)
    }

    fn pixel_value_at_coord_unchecked(&self, coordinate: Coordinate2D) -> Option<P> {
        let grid_index = self
            .tile_geo_transform()
            .coordinate_to_grid_idx_2d(coordinate);

        self.get_at_grid_index_unchecked(grid_index)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        primitives::TimeInterval,
        raster::{Grid2D, RasterTile2D, TileInformation},
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

            let tile_geo_transform = raster_tile.tile_information().tile_geo_transform();

            let value_a = raster_tile.pixel_value_at_coord(coordinate);

            let value_b = raster_tile
                .get_at_grid_index(tile_geo_transform.coordinate_to_grid_idx_2d(coordinate));

            match (value_a, value_b) {
                (Ok(a), Ok(b)) => assert_eq!(a, b),
                (Err(e1), Err(e2)) => assert_eq!(format!("{e1:?}"), format!("{e2:?}")),
                (Err(e), _) | (_, Err(e)) => panic!("{}", e.to_string()),
            };
        }

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
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
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [1, 1].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
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
}
