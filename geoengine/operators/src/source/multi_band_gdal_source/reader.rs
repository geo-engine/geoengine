use gdal::raster::GdalType;
use geoengine_datatypes::{
    primitives::TimeInterval,
    raster::{
        ChangeGridBounds, EmptyGrid, GridBlit, GridBoundingBox2D, GridOrEmpty, MaskedGrid, Pixel,
        RasterProperties, RasterTile2D, TileInformation,
    },
};
use num::FromPrimitive;
use tracing::{debug, trace};

use crate::source::{
    MultiBandGdalLoadingInfo, MultiBandGdalSourceError,
    gdal_worker_process::{
        FileNotFoundHandling, GdalDatasetParameters, GdalPoolDispatcher, GdalProcessPoolError,
        GridAndProperties,
        process_common::{
            GdalReadAdvise, IpcChannelMessage, IpcChannelMessagePayload, IpcProcessError,
            IpcProcessGdalErrorKind,
        },
    },
    multi_band_gdal_source::reader_mode::GdalReaderMode,
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
    /// The method also handles the case where the file is not found and the `file_not_found_handling` is set to `NoData` by returning a tile filled with nodata values.
    /// # Errors
    /// Returns a `GdalSourceError` if the process returns an error, or if the response cannot be converted to a `RasterTile2D`.
    ///
    /// # Panics
    /// Panics if the response from the process is not in the expected format, or if the grid blitting fails (which should not happen if the bounds are correct).
    pub async fn load_tile_data_process<T: Pixel + GdalType + FromPrimitive>(
        &self,
        dataset_params: GdalDatasetParameters,
        local_read_advise: GdalReadAdvise,
    ) -> Result<Option<GridAndProperties<T, GridBoundingBox2D>>, GdalProcessPoolError> {
        let file_not_found_as_no_data =
            dataset_params.file_not_found_handling == FileNotFoundHandling::NoData;

        let message = IpcChannelMessage::new_request_tile_message(IpcChannelMessagePayload {
            dataset_params,
            read_advise: local_read_advise,
            data_type: T::TYPE,
        });

        let res: Result<_, _> = self.worker_instance().read_data(message).await;

        let res = match res {
            Ok(t) => {
                // Here we need to handle edges!
                // First, convert response to GridAndProperties
                let GridAndProperties { grid, properties } = t.into();
                // Second, flip y-axis if necessary
                let grid = if local_read_advise.flip_y {
                    match grid {
                        GridOrEmpty::Grid(MaskedGrid {
                            inner_grid,
                            validity_mask,
                        }) => GridOrEmpty::new_grid(
                            MaskedGrid::new(
                                inner_grid.reversed_y_axis_grid(),
                                validity_mask.reversed_y_axis_grid(),
                            )
                            .expect("The bounds of the input grid should be the same after reversing the y axis, so this should never fail"),
                        ),
                        GridOrEmpty::Empty(e) => GridOrEmpty::new_empty(e),
                    }
                } else {
                    grid
                };

                Ok(Some(GridAndProperties { grid, properties }))
            }
            Err(GdalProcessPoolError::IpcProcessError {
                source:
                    IpcProcessError::GdalError {
                        kind: IpcProcessGdalErrorKind::FileNotFound,
                        details: _details,
                    },
            }) if file_not_found_as_no_data => Ok(None),
            Err(other_err) => Err(other_err),
        }?;

        Ok(res)
    }

    async fn load_tile_grid_props<T: Pixel + GdalType + FromPrimitive>(
        &self,
        dataset_params: GdalDatasetParameters,
        reader_mode: GdalReaderMode,
        tile_information: TileInformation,
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

        let mut properties = RasterProperties::default();
        let cache_hint = loading_info.cache_hint();

        let reader = Self::from(gdal_worker);

        for dataset_params in tile_files {
            if let Some(file_tile) = reader
                .load_tile_grid_props(dataset_params, reader_mode, tile_information)
                .await?
            {
                tile_raster.grid_blit_from(&file_tile.grid);
                properties = file_tile.properties;
            }
        }

        Ok(RasterTile2D::new_with_properties(
            time,
            tile_information.global_tile_position,
            band,
            tile_information.global_geo_transform,
            tile_raster.unbounded(),
            properties,
            cache_hint,
        ))
    }
}
