use crate::engine::RasterBandDescriptors;
use crate::error::Error;
use crate::util::Result;
use geoengine_datatypes::raster::{
    GridBoundingBox2D, GridIndexAccess, GridIntersection, GridOrEmpty, GridSize, Pixel,
    RasterTile2D, grid_idx_iter_2d,
};
use itertools::Either;

/// A band of a raster input, identified by its index and name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SelectedBand {
    pub index: u32,
    pub name: String,
}

/// Selects all `bands` if `band_names` is empty, otherwise the bands with the given names.
/// The result is ordered by band index and contains no duplicates.
pub(super) fn select_bands(
    bands: &RasterBandDescriptors,
    band_names: &[String],
) -> Result<Vec<SelectedBand>> {
    let mut selected = bands
        .iter()
        .enumerate()
        .map(|(index, band)| SelectedBand {
            index: index as u32,
            name: band.name.clone(),
        })
        .collect::<Vec<_>>();

    if band_names.is_empty() {
        return Ok(selected);
    }

    if let Some(unknown) = band_names
        .iter()
        .find(|name| !selected.iter().any(|band| &band.name == *name))
    {
        return Err(Error::InvalidOperatorSpec {
            reason: format!("Band '{unknown}' does not exist."),
        });
    }

    selected.retain(|band| band_names.contains(&band.name));

    Ok(selected)
}

/// Returns the part of `tile` that lies within `query_bounds`, both in global pixel indices.
fn tile_bounds_in_query<T: Pixel>(
    tile: &RasterTile2D<T>,
    query_bounds: &GridBoundingBox2D,
) -> Option<GridBoundingBox2D> {
    tile.tile_information()
        .global_pixel_bounds()
        .intersection(query_bounds)
}

/// Number of pixels of `tile` that lie within `query_bounds` (global pixel indices).
pub(super) fn pixel_count_in_query<T: Pixel>(
    tile: &RasterTile2D<T>,
    query_bounds: &GridBoundingBox2D,
) -> usize {
    tile_bounds_in_query(tile, query_bounds).map_or(0, |bounds| bounds.number_of_elements())
}

/// Iterates over the masked values of the pixels of `tile` that lie within `query_bounds` (global pixel indices).
/// Pixels of empty tiles yield `None`.
pub(super) fn masked_pixels_in_query<'t, T: Pixel>(
    tile: &'t RasterTile2D<T>,
    query_bounds: &GridBoundingBox2D,
) -> impl Iterator<Item = Option<T>> + 't {
    let tile_info = tile.tile_information();
    let bounds = tile_bounds_in_query(tile, query_bounds);

    match (&tile.grid_array, bounds) {
        (GridOrEmpty::Grid(grid), Some(bounds)) if bounds == tile_info.global_pixel_bounds() => {
            Either::Left(grid.masked_element_deref_iterator())
        }
        (grid_array, bounds) => {
            let tile_offset = tile_info.global_upper_left_pixel_idx();
            Either::Right(
                bounds
                    .into_iter()
                    .flat_map(|bounds| grid_idx_iter_2d(&bounds))
                    .map(move |global_idx| {
                        grid_array.get_at_grid_index_unchecked(global_idx - tile_offset)
                    }),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::RasterBandDescriptor;
    use geoengine_datatypes::primitives::{CacheHint, Measurement, TimeInterval};
    use geoengine_datatypes::raster::{
        EmptyGrid2D, GeoTransform, Grid2D, GridShape2D, MaskedGrid2D, TileInformation,
    };
    use geoengine_datatypes::util::test::TestDefault;

    fn tile(grid_array: GridOrEmpty<GridShape2D, u8>) -> RasterTile2D<u8> {
        RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: GeoTransform::test_default(),
                global_tile_position: [1, 1].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            grid_array,
            CacheHint::no_cache(),
        )
    }

    /// Tile at global pixel bounds `[3, 2]..=[5, 3]` with values 1..=6, where 4 is no-data.
    fn grid_tile() -> RasterTile2D<u8> {
        let data =
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).expect("grid should be valid");
        let mask = Grid2D::new([3, 2].into(), vec![true, true, true, false, true, true])
            .expect("mask should be valid");
        tile(
            MaskedGrid2D::new(data, mask)
                .expect("grid and mask should have the same shape")
                .into(),
        )
    }

    #[test]
    fn it_returns_all_pixels_of_contained_tile() {
        let tile = grid_tile();
        let query_bounds =
            GridBoundingBox2D::new([0, 0], [10, 10]).expect("bounds should be valid");

        assert_eq!(pixel_count_in_query(&tile, &query_bounds), 6);
        assert_eq!(
            masked_pixels_in_query(&tile, &query_bounds).collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(3), None, Some(5), Some(6)]
        );
    }

    #[test]
    fn it_returns_only_pixels_within_query_bounds() {
        let tile = grid_tile();
        let query_bounds = GridBoundingBox2D::new([4, 0], [10, 2]).expect("bounds should be valid");

        assert_eq!(pixel_count_in_query(&tile, &query_bounds), 2);
        assert_eq!(
            masked_pixels_in_query(&tile, &query_bounds).collect::<Vec<_>>(),
            vec![Some(3), Some(5)]
        );
    }

    #[test]
    fn it_returns_no_pixels_outside_query_bounds() {
        let tile = grid_tile();
        let query_bounds = GridBoundingBox2D::new([0, 0], [2, 1]).expect("bounds should be valid");

        assert_eq!(pixel_count_in_query(&tile, &query_bounds), 0);
        assert_eq!(masked_pixels_in_query(&tile, &query_bounds).count(), 0);
    }

    #[test]
    fn it_returns_no_data_for_empty_tile() {
        let tile = tile(EmptyGrid2D::new([3, 2].into()).into());
        let query_bounds = GridBoundingBox2D::new([3, 3], [4, 10]).expect("bounds should be valid");

        assert_eq!(pixel_count_in_query(&tile, &query_bounds), 2);
        assert_eq!(
            masked_pixels_in_query(&tile, &query_bounds).collect::<Vec<_>>(),
            vec![None, None]
        );
    }

    fn bands() -> RasterBandDescriptors {
        RasterBandDescriptors::new(
            ["red", "green", "blue"]
                .into_iter()
                .map(|name| RasterBandDescriptor::new(name.to_string(), Measurement::Unitless))
                .collect(),
        )
        .expect("band names should be unique")
    }

    #[test]
    fn it_selects_all_bands_without_names() {
        assert_eq!(
            select_bands(&bands(), &[]).expect("selecting all bands should work"),
            vec![
                SelectedBand {
                    index: 0,
                    name: "red".to_string()
                },
                SelectedBand {
                    index: 1,
                    name: "green".to_string()
                },
                SelectedBand {
                    index: 2,
                    name: "blue".to_string()
                },
            ]
        );
    }

    #[test]
    fn it_selects_bands_by_name_in_band_order() {
        assert_eq!(
            select_bands(
                &bands(),
                &["blue".to_string(), "red".to_string(), "blue".to_string()]
            )
            .expect("bands should exist"),
            vec![
                SelectedBand {
                    index: 0,
                    name: "red".to_string()
                },
                SelectedBand {
                    index: 2,
                    name: "blue".to_string()
                },
            ]
        );
    }

    #[test]
    fn it_fails_on_unknown_band_name() {
        assert!(matches!(
            select_bands(&bands(), &["alpha".to_string()]),
            Err(Error::InvalidOperatorSpec { reason }) if reason == "Band 'alpha' does not exist."
        ));
    }
}
