use gdal::raster::GdalType;
use geoengine_datatypes::{
    primitives::{CacheTtlSeconds, TimeInterval},
    raster::{
        ChangeGridBounds, EmptyGrid, GridBoundingBox2D, GridOrEmpty, Pixel, RasterTile2D,
        TileInformation,
    },
};
use num::FromPrimitive;
use tracing::{debug, trace};

use crate::source::{
    MultiBandGdalLoadingInfo, MultiBandGdalSourceError,
    gdal_worker_process::GdalReaderMode,
    gdal_worker_process::{
        GdalDatasetParameters, GdalPoolDispatcher, GdalProcessPoolError, GdalProcessReadResult,
        GridAndProperties, process_common::GdalReadAdvise,
    },
};

pub(crate) struct GdalPoolReader(GdalPoolDispatcher);

impl From<GdalPoolDispatcher> for GdalPoolReader {
    fn from(value: GdalPoolDispatcher) -> Self {
        GdalPoolReader(value)
    }
}

impl GdalPoolReader {
    #[inline]
    fn worker_instance(&self) -> &GdalPoolDispatcher {
        &self.0
    }

    /// Loads a tile using the separate process and the provided parameters and read advise.
    /// This is the method where the source operator attaches to the process pool.
    /// The method sends a message to the process pool and waits for the response. The response is then converted to a `RasterTile2D` and returned.
    /// The method also handles the case where the file is not found and the `file_not_found_handling` is set to `NoData` by returning `None`.
    /// # Errors
    /// Returns a `GdalSourceError` if the process returns an error, or if the response cannot be converted to a `RasterTile2D`.
    pub async fn load_tile_data_process<T: Pixel + GdalType + FromPrimitive>(
        &self,
        dataset_params: GdalDatasetParameters,
        local_read_advise: GdalReadAdvise,
    ) -> Result<Option<GridAndProperties<T, GridBoundingBox2D>>, GdalProcessPoolError> {
        let shared_reader = crate::source::gdal_worker_process::reader::GdalPoolReader::from(
            self.worker_instance().clone(),
        );

        match shared_reader
            .read_tile_data::<T>(dataset_params, local_read_advise)
            .await?
        {
            GdalProcessReadResult::Grid(grid_and_properties) => Ok(Some(*grid_and_properties)),
            GdalProcessReadResult::FileNotFoundAsNoData => Ok(None),
        }
    }

    async fn load_tile_grid_props<T: Pixel + GdalType + FromPrimitive>(
        &self,
        dataset_params: GdalDatasetParameters,
        reader_mode: GdalReaderMode,
        tile_information: TileInformation,
        tile_raster: &GridOrEmpty<GridBoundingBox2D, T>,
    ) -> Result<Option<GridAndProperties<T, GridBoundingBox2D>>, GdalProcessPoolError> {
        let ds_spatial_grid = dataset_params.spatial_grid_definition();
        let tile_spatial_grid = tile_information.spatial_grid_definition();
        let Some(local_read_advise) =
            reader_mode.tiling_to_dataset_read_advise(&ds_spatial_grid, &tile_spatial_grid)
        else {
            trace!(
                "no read advise returned for tile {:?}, skipping file.",
                tile_information.global_tile_position,
            );
            return Ok(None);
        };

        if tile_raster
            .as_masked_grid()
            .is_some_and(|grid| grid.all_valid_in_bbox(&local_read_advise.read_window_bounds))
        {
            trace!(
                file = %dataset_params.file_path.display(),
                "skipping file whose read area is covered by higher-z valid pixels"
            );
            return Ok(None);
        }

        let file_tile = self
            .load_tile_data_process::<T>(dataset_params, local_read_advise)
            .await?;

        Ok(file_tile)
    }

    pub async fn load_tile_from_files_async<T: Pixel + GdalType + FromPrimitive>(
        loading_info: MultiBandGdalLoadingInfo,
        reader_mode: GdalReaderMode,
        tile_information: TileInformation,
        time: TimeInterval,
        band: u32,
        gdal_worker: GdalPoolDispatcher,
        default_cache_ttl: CacheTtlSeconds,
    ) -> Result<RasterTile2D<T>, MultiBandGdalSourceError> {
        debug!(
            "loading tile {:?} for time: {}, band: {band}",
            tile_information.global_tile_position.inner(),
            time.to_string()
        );
        let tile_files = loading_info.tile_files(time, tile_information, band);

        debug!(
            "tile_files: {}",
            tile_files
                .iter()
                .map(|tf| tf.file_path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );

        let mut tile_raster: GridOrEmpty<GridBoundingBox2D, T> =
            GridOrEmpty::from(EmptyGrid::new(tile_information.global_pixel_bounds()));

        let mut properties = None;
        let cache_hint = loading_info.cache_hint(default_cache_ttl);

        let reader = Self::from(gdal_worker);

        // Highest z-index wins at each valid pixel. Read lower-priority files
        // only where higher-priority files left gaps in the output mask.
        // TODO: Use available coverage/no-data metadata to choose between parallel
        // prefetching and lazy loading, while preserving per-pixel z-index priority.
        for dataset_params in tile_files.into_iter().rev() {
            if let Some(file_tile) = reader
                .load_tile_grid_props(dataset_params, reader_mode, tile_information, &tile_raster)
                .await?
            {
                tile_raster.grid_blit_fill_no_data(&file_tile.grid);
                properties.get_or_insert(file_tile.properties);
            }
        }

        Ok(RasterTile2D::new_with_properties(
            time,
            tile_information.global_tile_position,
            band,
            tile_information.global_geo_transform,
            tile_raster.unbounded(),
            properties.unwrap_or_default(),
            cache_hint,
        ))
    }
}
