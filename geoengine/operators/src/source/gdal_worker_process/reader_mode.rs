use geoengine_datatypes::raster::{
    GridBoundingBox2D, GridBounds, GridContains, GridIdx2D, GridShapeAccess, GridSize,
    SpatialGridDefinition,
};

use crate::source::gdal_worker_process::process_common::{GdalReadAdvise, GdalReadWindow};

#[derive(Copy, Clone, Debug)]
pub struct ReaderState {
    pub dataset_spatial_grid: SpatialGridDefinition,
}

impl ReaderState {
    /// Returns the read advise for the tiling based bounds.
    pub fn tiling_to_dataset_read_advise(
        &self,
        actual_gdal_dataset_spatial_grid_definition: &SpatialGridDefinition,
        tile: &SpatialGridDefinition,
    ) -> Option<GdalReadAdvise> {
        // Check if the y_axis is flipped.
        let (actual_gdal_dataset_spatial_grid_definition, flip_y) =
            if actual_gdal_dataset_spatial_grid_definition
                .geo_transform()
                .y_axis_is_neg()
                == self.dataset_spatial_grid.geo_transform().y_axis_is_neg()
            {
                (*actual_gdal_dataset_spatial_grid_definition, false)
            } else {
                (
                    actual_gdal_dataset_spatial_grid_definition
                        .flip_axis_y() // first: reverse the coordinate system to match the one used by tiling
                        .shift_bounds_relative_by_pixel_offset(GridIdx2D::new_y_x(
                            // second: move the origin to the other end of the y-axis
                            actual_gdal_dataset_spatial_grid_definition
                                .grid_bounds()
                                .axis_size_y() as isize,
                            0,
                        )),
                    true,
                )
            };

        // Now we can work with a matching dataset. However, we need to reverse the read window later!

        // let's only look at data in the geo engine dataset definition! The intersection is relative to the first elements origin coordinate.
        let dataset_gdal_data_intersection =
            actual_gdal_dataset_spatial_grid_definition.intersection(&self.dataset_spatial_grid)?;

        // Now, we need the tile in the gdal dataset bounds to identify readable areas
        let tile_in_gdal_dataset_bounds = tile.with_moved_origin_exact_grid(
            actual_gdal_dataset_spatial_grid_definition
                .geo_transform()
                .origin_coordinate,
        )?; // TODO: raise error if this fails!

        // Then, calculate the intersection between the datataset and the tile. Again, the intersection is relative to the first elements orrigin coordinate.
        let tile_gdal_dataset_intersection =
            dataset_gdal_data_intersection.intersection(&tile_in_gdal_dataset_bounds)?;

        // if we need to unflip the dataset grid now is the time to do this.
        let tile_intersection_for_read_window = if flip_y {
            tile_gdal_dataset_intersection
                .flip_axis_y() // first: reverse the coordinate system to match the one used by tiling
                .shift_bounds_relative_by_pixel_offset(GridIdx2D::new_y_x(
                    // second: move the origin to the other end of the y-axis
                    actual_gdal_dataset_spatial_grid_definition
                        .grid_bounds()
                        .axis_size_y() as isize,
                    0,
                ))
        } else {
            tile_gdal_dataset_intersection
        };

        // generate the read window for GDAL

        let gdal_read_window = GdalReadWindow::new(
            tile_intersection_for_read_window.grid_bounds.min_index(),
            tile_intersection_for_read_window.grid_bounds.grid_shape(),
        );

        // if the read window has the same shape as the tiling based bounds we can fill that completely
        if tile_in_gdal_dataset_bounds == tile_gdal_dataset_intersection {
            return Some(GdalReadAdvise {
                gdal_read_widow: gdal_read_window,
                read_window_bounds: tile.grid_bounds,
                bounds_of_target: tile.grid_bounds,
                flip_y,
            });
        }

        // we need to crop the window to the intersection of the tiling based bounds and the dataset bounds
        let crop_tl =
            tile_gdal_dataset_intersection.min_index() - tile_in_gdal_dataset_bounds.min_index();
        let crop_lr =
            tile_gdal_dataset_intersection.max_index() - tile_in_gdal_dataset_bounds.max_index();

        let shifted_tl = tile.grid_bounds.min_index() + crop_tl;
        let shifted_lr = tile.grid_bounds.max_index() + crop_lr;

        // now we need to adapt the target pixel space read window to the clipped dataset intersection area
        let shifted_readable_bounds = GridBoundingBox2D::new_unchecked(shifted_tl, shifted_lr);
        debug_assert!(
            tile.grid_bounds().contains(&shifted_readable_bounds),
            "readable bounds must be contained in tile bounds"
        );

        Some(GdalReadAdvise {
            gdal_read_widow: gdal_read_window,
            read_window_bounds: shifted_readable_bounds,
            bounds_of_target: tile.grid_bounds,
            flip_y,
        })
    }
}
