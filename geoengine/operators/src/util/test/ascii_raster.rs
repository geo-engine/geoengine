use crate::{
    engine::{
        RasterBandDescriptors, RasterResultDescriptor, SpatialGridDescriptor, TimeDescriptor,
    },
    mock::{MockRasterSource, MockRasterSourceParams},
};
use geoengine_datatypes::{
    raster::{Pixel, TileSize},
    spatial_reference::SpatialReference,
    util::test::AsciiRasterTiles,
};
use std::str::FromStr;

/// Builds operator inputs from raster tiles that are described as ASCII art.
pub trait AsciiRasterTilesExt {
    /// A result descriptor for the described raster:
    /// the data type of `T`, EPSG:4326, the time bounds of all time steps (regular if possible),
    /// the pixel bounds of all tiles as source grid, and unitless bands.
    ///
    /// Adjust the returned descriptor if a test needs something else, e.g., another spatial reference.
    ///
    /// # Panics
    ///
    /// If the text is malformed.
    fn result_descriptor<T: Pixel>(&self) -> RasterResultDescriptor;

    /// A [`MockRasterSource`] that returns the described tiles with [`Self::result_descriptor`].
    ///
    /// # Panics
    ///
    /// If the text is malformed or a value cannot be parsed as `T`.
    fn mock_raster_source<T: Pixel + FromStr>(&self) -> MockRasterSource<T>;
}

impl AsciiRasterTilesExt for AsciiRasterTiles<'_> {
    fn result_descriptor<T: Pixel>(&self) -> RasterResultDescriptor {
        let info = self.info();

        let time = match info.regular_time_step {
            Some(step) => TimeDescriptor::new_regular_with_epoch(Some(info.time_bounds), step),
            None => TimeDescriptor::new_irregular(Some(info.time_bounds)),
        };

        let bands = if info.num_bands == 1 {
            RasterBandDescriptors::new_single_band()
        } else {
            RasterBandDescriptors::new_multiple_bands(info.num_bands)
        };

        RasterResultDescriptor {
            data_type: T::TYPE,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time,
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                info.geo_transform,
                info.pixel_bounds,
                TileSize::try_from(info.tile_size).expect("parsed tiles are non-empty"),
            ),
            bands,
        }
    }

    fn mock_raster_source<T: Pixel + FromStr>(&self) -> MockRasterSource<T> {
        MockRasterSource {
            params: MockRasterSourceParams {
                data: self.tiles(),
                result_descriptor: self.result_descriptor::<T>(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::{
        primitives::{TimeInterval, TimeStep},
        raster::{GeoTransform, GridBoundingBox2D, RasterDataType, TileSize},
        util::test::TestDefault,
    };

    #[test]
    fn it_builds_a_regular_single_band_descriptor() {
        let descriptor = AsciiRasterTiles::new(
            "
            [0, 10)
            +-------+-------+
            | 1 2 3 | 4 5 6 |
            | 1 2 3 | 4 5 6 |
            +-------+-------+
            [10, 20)
            +-------+-------+
            | 1 2 3 | 4 5 6 |
            | 1 2 3 | 4 5 6 |
            +-------+-------+
            ",
        )
        .tile_origin([-1, 0])
        .result_descriptor::<i8>();

        assert_eq!(
            descriptor,
            RasterResultDescriptor {
                data_type: RasterDataType::I8,
                spatial_reference: SpatialReference::epsg_4326().into(),
                time: TimeDescriptor::new_regular_with_epoch(
                    Some(TimeInterval::new_unchecked(0, 20)),
                    TimeStep::millis(10).unwrap(),
                ),
                spatial_grid: SpatialGridDescriptor::source_from_parts(
                    GeoTransform::test_default(),
                    GridBoundingBox2D::new([-2, 0], [-1, 5]).unwrap(),
                    TileSize::new_unchecked(2, 3),
                ),
                bands: RasterBandDescriptors::new_single_band(),
            }
        );
    }

    #[test]
    fn it_builds_an_irregular_multi_band_descriptor() {
        let descriptor = AsciiRasterTiles::new(
            "
            [0, 10)
            | 1 |
            [0, 10) band 1
            | 2 |
            [20, 25)
            | 3 |
            [20, 25) band 1
            | 4 |
            ",
        )
        .result_descriptor::<f32>();

        assert_eq!(descriptor.data_type, RasterDataType::F32);
        assert_eq!(
            descriptor.time,
            TimeDescriptor::new_irregular(Some(TimeInterval::new_unchecked(0, 25)))
        );
        assert_eq!(
            descriptor.bands,
            RasterBandDescriptors::new_multiple_bands(2)
        );
    }

    #[test]
    fn it_builds_a_mock_raster_source() {
        let tiles = AsciiRasterTiles::new(
            "
            [0, 10)
            +-----+
            | 1 _ |
            +-----+
            ",
        );

        let source = tiles.mock_raster_source::<u8>();

        assert_eq!(source.params.data, tiles.tiles::<u8>());
        assert_eq!(
            source.params.result_descriptor,
            tiles.result_descriptor::<u8>()
        );
    }
}
