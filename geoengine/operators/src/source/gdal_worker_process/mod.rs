mod db_types;
mod gdal_dataset_params;
mod grid_and_properties;
pub mod process_common;
pub mod process_impl;
mod process_pool;
pub mod reader;
pub mod reader_mode;

use geoengine_datatypes::raster::{GeoTransform, GridBoundingBox2D, SpatialGridDefinition};
use num::integer::div_floor;

pub use gdal_dataset_params::{
    FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalMetadataMapping,
    GdalRetryOptions, GdalSourceTimePlaceholder, TimeReference,
};
pub use grid_and_properties::GridAndProperties;
pub use process_impl::{OpenTelemetryConfig, WorkerConfig, WorkerLoggingConfig};
pub use process_pool::{GdalPoolDispatcher, GdalProcessPool, GdalProcessPoolError};
pub use reader::{GdalPoolReader, GdalProcessReadResult};
pub use reader_mode::{GdalReaderMode, OverviewReaderState, ReaderState};

/// Computes a reduced-resolution spatial grid for a given overview level.
///
/// Returns `None` if the overview level is 0 (original resolution).
/// The overview level multiplies the pixel size and divides the grid bounds accordingly.
pub(crate) fn overview_level_spatial_grid(
    source_spatial_grid: SpatialGridDefinition,
    overview_level: u32,
) -> Option<SpatialGridDefinition> {
    if overview_level > 0 {
        tracing::trace!("Using overview level {overview_level}");
        let geo_transform = GeoTransform::new(
            source_spatial_grid.geo_transform.origin_coordinate,
            source_spatial_grid.geo_transform.x_pixel_size() * f64::from(overview_level),
            source_spatial_grid.geo_transform.y_pixel_size() * f64::from(overview_level),
        );
        let grid_bounds = GridBoundingBox2D::new_min_max(
            div_floor(
                source_spatial_grid.grid_bounds.y_min(),
                overview_level as isize,
            ),
            div_floor(
                source_spatial_grid.grid_bounds.y_max(),
                overview_level as isize,
            ),
            div_floor(
                source_spatial_grid.grid_bounds.x_min(),
                overview_level as isize,
            ),
            div_floor(
                source_spatial_grid.grid_bounds.x_max(),
                overview_level as isize,
            ),
        )
        .expect("overview level must be a positive integer");

        Some(SpatialGridDefinition::new(geo_transform, grid_bounds))
    } else {
        tracing::trace!("Using original resolution (ov = 0)");
        None
    }
}

pub trait GdalProcessPoolAccess {
    fn get_gdal_pool(&self) -> &std::sync::Arc<GdalProcessPool>;
    fn get_gdal_worker(&self) -> GdalPoolDispatcher {
        GdalPoolDispatcher::new(self.get_gdal_pool().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::primitives::Coordinate2D;

    #[test]
    fn it_computes_spatial_grids_for_overviews() {
        let spatial_grid_definition = SpatialGridDefinition::new(
            GeoTransform::new(Coordinate2D::new(0., 0.), 0.1, -0.1),
            GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap(),
        );

        let spatial_grid_definition_2x =
            overview_level_spatial_grid(spatial_grid_definition, 2).unwrap();
        let expected = SpatialGridDefinition::new(
            GeoTransform::new(Coordinate2D::new(0., 0.), 0.2, -0.2),
            GridBoundingBox2D::new([-450, -900], [449, 899]).unwrap(),
        );
        assert_eq!(spatial_grid_definition_2x, expected);

        let spatial_grid_definition_4x =
            overview_level_spatial_grid(spatial_grid_definition, 4).unwrap();
        let expected = SpatialGridDefinition::new(
            GeoTransform::new(Coordinate2D::new(0., 0.), 0.4, -0.4),
            GridBoundingBox2D::new([-225, -450], [224, 449]).unwrap(),
        );
        assert_eq!(spatial_grid_definition_4x, expected);

        let spatial_grid_definition_8x =
            overview_level_spatial_grid(spatial_grid_definition, 8).unwrap();
        let expected = SpatialGridDefinition::new(
            GeoTransform::new(Coordinate2D::new(0., 0.), 0.8, -0.8),
            GridBoundingBox2D::new([-113, -225], [112, 224]).unwrap(),
        );
        assert_eq!(spatial_grid_definition_8x, expected);
    }
}
