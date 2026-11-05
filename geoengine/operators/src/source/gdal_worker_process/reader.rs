use gdal::raster::GdalType;
use geoengine_datatypes::primitives::CacheHint;
use geoengine_datatypes::raster::{GridBoundingBox2D, GridOrEmpty, MaskedGrid, Pixel};
use num::FromPrimitive;
use std::sync::Arc;
use tracing::Instrument;

use crate::source::gdal_worker_process::{
    FileNotFoundHandling, GdalDatasetParameters, GdalPoolDispatcher, GdalProcessPoolError,
    GridAndProperties,
    process_common::{
        GdalReadAdvise, GdalReadKind, GdalRequest, IpcChannelMessage, IpcChannelMessagePayload,
        IpcChannelResult, IpcProcessError, IpcProcessGdalErrorKind, RasterInfo,
    },
};

/// Result of reading a tile through the GDAL worker process.
pub enum GdalProcessReadResult<T: Pixel> {
    /// The tile data was read successfully.
    Grid(Box<GridAndProperties<T, GridBoundingBox2D>>),
    /// The file was not found and the dataset is configured to treat that as no-data.
    FileNotFoundAsNoData,
}

/// Reverses the rows of a grid (and its validity mask) if `flip_y` is set: upside-down datasets
/// return data in stored row order, the caller flips it back to the tile (north-up) order.
pub(crate) fn flip_grid_y_if_needed<T: Pixel>(
    grid: GridOrEmpty<GridBoundingBox2D, T>,
    flip_y: bool,
) -> GridOrEmpty<GridBoundingBox2D, T> {
    if !flip_y {
        return grid;
    }

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
}

/// Reader that dispatches GDAL tile requests to the [`GdalProcessPool`].
#[derive(Clone)]
pub struct GdalPoolReader(GdalPoolDispatcher);

impl From<GdalPoolDispatcher> for GdalPoolReader {
    fn from(value: GdalPoolDispatcher) -> Self {
        GdalPoolReader(value)
    }
}

impl GdalPoolReader {
    #[inline]
    fn dispatcher(&self) -> &GdalPoolDispatcher {
        &self.0
    }

    /// Reads a tile via the worker process and applies the required post-processing
    /// (Y-axis flip) that is common to all GDAL-based raster sources.
    ///
    /// This method intentionally does **not** blit the result into the final tile bounds;
    /// callers decide whether and how to composite the result.
    ///
    /// # Errors
    /// Returns a `GdalProcessPoolError` if the worker returns an error, or if the response
    /// cannot be converted to a raster tile.
    ///
    /// # Panics
    /// Panics if the Y-axis flipped grid cannot be wrapped in a `MaskedGrid`.
    pub async fn read_tile_data<T: Pixel + GdalType + FromPrimitive>(
        &self,
        dataset_params: GdalDatasetParameters,
        read_advise: GdalReadAdvise,
    ) -> Result<GdalProcessReadResult<T>, GdalProcessPoolError> {
        self.read_raster(dataset_params, read_advise, GdalReadKind::Raster)
            .await
    }

    /// Reads `read_advise` from the level `factor` times coarser than the full resolution, at
    /// native size and without resampling: `gdal_read_widow` is in the level's pixel space and
    /// has the size of `read_window_bounds`. A missing level is an error.
    pub async fn read_level_data<T: Pixel + GdalType + FromPrimitive>(
        &self,
        dataset_params: GdalDatasetParameters,
        factor: usize,
        read_advise: GdalReadAdvise,
    ) -> Result<GdalProcessReadResult<T>, GdalProcessPoolError> {
        self.read_raster(dataset_params, read_advise, GdalReadKind::Level { factor })
            .await
    }

    async fn read_raster<T: Pixel + GdalType + FromPrimitive>(
        &self,
        dataset_params: GdalDatasetParameters,
        read_advise: GdalReadAdvise,
        kind: GdalReadKind,
    ) -> Result<GdalProcessReadResult<T>, GdalProcessPoolError> {
        let file_not_found_as_no_data =
            dataset_params.file_not_found_handling == FileNotFoundHandling::NoData;

        // Compute a read_id from request content hash + timestamp.
        // Deduped concurrent reads share the same read_id (leader's timestamp).
        // Re-reads of the same tile later get a different read_id (different timestamp).
        let read_id = dataset_params.create_read_id(&read_advise);

        let span = tracing::info_span!(
            "gdal_pool_read",
            read_id = %read_id,
            dataset = %dataset_params.file_path.display(),
            band = dataset_params.rasterband_channel,
        );

        let res = self
            .dispatcher()
            .read_data(IpcChannelMessage::new_request_tile_message(
                IpcChannelMessagePayload {
                    dataset_params,
                    read_id: Some(read_id),
                    request: GdalRequest::Read {
                        read_advise,
                        data_type: T::TYPE,
                        kind,
                    },
                },
            ))
            .instrument(span)
            .await;

        let Some(t) = read_result_or_no_data(res, file_not_found_as_no_data)? else {
            return Ok(GdalProcessReadResult::FileNotFoundAsNoData);
        };

        let GridAndProperties { grid, properties } = t.into();
        let grid = flip_grid_y_if_needed(grid, read_advise.flip_y);
        Ok(GdalProcessReadResult::Grid(Box::new(GridAndProperties {
            grid,
            properties,
        })))
    }

    /// Reads the data type of the `rasterband_channel` band and the CRS of a raster in a worker,
    /// so the server never opens the file itself. A missing file is an error here, whatever
    /// `file_not_found_handling` says: there is no header to report.
    ///
    /// # Errors
    /// Returns a `GdalProcessPoolError` if the file or band cannot be opened or the data type is
    /// not supported.
    pub async fn read_raster_info(
        &self,
        dataset_params: GdalDatasetParameters,
    ) -> Result<RasterInfo, GdalProcessPoolError> {
        match self.read_header(dataset_params).await? {
            IpcChannelResult::RasterInfo(info) => Ok(info),
            IpcChannelResult::Raster(_) => Err(GdalProcessPoolError::IpcProcessError {
                source: IpcProcessError::IpcOther {
                    msg: "the worker answered a header read with a raster".to_string(),
                },
            }),
        }
    }

    /// Like [`Self::read_raster_info`], but `None` if the file is missing and its parameters say
    /// to treat that as no data.
    pub async fn read_raster_info_or_no_data(
        &self,
        dataset_params: GdalDatasetParameters,
    ) -> Result<Option<RasterInfo>, GdalProcessPoolError> {
        let file_not_found_as_no_data =
            dataset_params.file_not_found_handling == FileNotFoundHandling::NoData;
        let res = self.read_header(dataset_params).await;
        match read_result_or_no_data(res, file_not_found_as_no_data)? {
            Some(IpcChannelResult::RasterInfo(info)) => Ok(Some(info)),
            Some(IpcChannelResult::Raster(_)) => Err(GdalProcessPoolError::IpcProcessError {
                source: IpcProcessError::IpcOther {
                    msg: "the worker answered a header read with a raster".to_string(),
                },
            }),
            None => Ok(None),
        }
    }

    /// The header of `file` if [`Self::read_and_remember_raster_info`] read it and it is valid.
    pub fn remembered_raster_info(&self, file: &str) -> Option<RasterInfo> {
        self.dispatcher().remembered_raster_info(file)
    }

    /// Like [`Self::read_raster_info_or_no_data`], and remembers the header of `file`, which must
    /// identify `dataset_params`, until `cache_hint` expires.
    pub async fn read_and_remember_raster_info(
        &self,
        file: &Arc<str>,
        dataset_params: GdalDatasetParameters,
        cache_hint: CacheHint,
    ) -> Result<Option<RasterInfo>, GdalProcessPoolError> {
        let info = self.read_raster_info_or_no_data(dataset_params).await?;
        if let Some(info) = &info {
            self.dispatcher()
                .remember_raster_info(file.clone(), info.clone(), cache_hint);
        }
        Ok(info)
    }

    async fn read_header(
        &self,
        dataset_params: GdalDatasetParameters,
    ) -> Result<IpcChannelResult, GdalProcessPoolError> {
        let span = tracing::info_span!(
            "gdal_pool_read_header",
            dataset = %dataset_params.file_path.display(),
        );
        let request = IpcChannelMessage::new_request_tile_message(IpcChannelMessagePayload {
            dataset_params,
            read_id: None,
            request: GdalRequest::Header,
        });

        self.dispatcher()
            .read_header(request)
            .instrument(span)
            .await
    }
}

/// Unwraps a worker read result, turning a `FileNotFound` error into `None` when the
/// dataset is configured to treat a missing file as no-data.
fn read_result_or_no_data<T>(
    res: Result<T, GdalProcessPoolError>,
    file_not_found_as_no_data: bool,
) -> Result<Option<T>, GdalProcessPoolError> {
    match res {
        Ok(value) => Ok(Some(value)),
        Err(GdalProcessPoolError::IpcProcessError {
            source:
                IpcProcessError::GdalError {
                    kind: IpcProcessGdalErrorKind::FileNotFound,
                    details: _details,
                },
        }) if file_not_found_as_no_data => Ok(None),
        Err(other_err) => Err(other_err),
    }
}
