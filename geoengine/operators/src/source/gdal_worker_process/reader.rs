use gdal::raster::GdalType;
use geoengine_datatypes::raster::{GridBoundingBox2D, GridOrEmpty, MaskedGrid, Pixel};
use num::FromPrimitive;
use tracing::Instrument;

use crate::source::gdal_worker_process::{
    FileNotFoundHandling, GdalDatasetParameters, GdalPoolDispatcher, GdalProcessPoolError,
    GridAndProperties,
    process_common::{
        GdalReadAdvise, GdalReadKind, IpcChannelMessage, IpcChannelMessagePayload, IpcProcessError,
        IpcProcessGdalErrorKind,
    },
};

/// Result of reading a tile through the GDAL worker process.
pub enum GdalProcessReadResult<T: Pixel> {
    /// The tile data was read successfully.
    Grid(Box<GridAndProperties<T, GridBoundingBox2D>>),
    /// The file was not found and the dataset is configured to treat that as no-data.
    FileNotFoundAsNoData,
}

/// Result of reading a batch of z-slices through the GDAL worker process.
pub enum GdalProcessMdReadResult<T: Pixel> {
    /// One grid per z-slice of the requested batch, in ascending z order.
    Grids(Vec<GridAndProperties<T, GridBoundingBox2D>>),
    /// The file was not found and the dataset is configured to treat that as no-data.
    FileNotFoundAsNoData,
}

/// Reverses the rows of a grid (and its validity mask) if `flip_y` is set.
/// MD and upside-down 2D datasets return data in stored row order; the caller
/// flips it back to the tile (north-up) order.
fn flip_grid_y_if_needed<T: Pixel>(
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
                    read_advise,
                    data_type: T::TYPE,
                    read_id: Some(read_id),
                    read_kind: GdalReadKind::Raster,
                },
            ))
            .instrument(span)
            .await;

        let Some(t) = read_result_or_no_data(res, file_not_found_as_no_data)? else {
            return Ok(GdalProcessReadResult::FileNotFoundAsNoData);
        };

        // First, convert response to GridAndProperties
        let GridAndProperties { grid, properties } = t.into();
        // Second, flip y-axis if necessary
        let grid = flip_grid_y_if_needed(grid, read_advise.flip_y);
        Ok(GdalProcessReadResult::Grid(Box::new(GridAndProperties {
            grid,
            properties,
        })))
    }

    /// Reads a batch of z-slices from a multidim array via the worker process in a
    /// single request, applying the same post-processing (Y-axis flip per z-slice)
    /// and file-not-found handling as [`Self::read_tile_data`].
    ///
    /// This method intentionally does **not** blit the results into the final tile
    /// bounds; callers composite the slices.
    ///
    /// # Errors
    /// Returns a `GdalProcessPoolError` if the worker returns an error, or if a
    /// response cannot be converted to a raster grid.
    ///
    /// # Panics
    /// Panics if a Y-axis flipped grid cannot be wrapped in a `MaskedGrid`.
    pub async fn read_md_batch_data<T: Pixel + GdalType + FromPrimitive>(
        &self,
        dataset_params: GdalDatasetParameters,
        read_advise: GdalReadAdvise,
        group: Option<&str>,
        array_name: &str,
        z_range: std::ops::Range<usize>,
        leading_prefix: &[u64],
    ) -> Result<GdalProcessMdReadResult<T>, GdalProcessPoolError> {
        let file_not_found_as_no_data =
            dataset_params.file_not_found_handling == FileNotFoundHandling::NoData;

        // extend the read_id with the MD-specific request parts
        let read_id = format!(
            "{}-{array_name}-{}-{}",
            dataset_params.create_read_id(&read_advise),
            z_range.start,
            z_range.end
        );

        let span = tracing::info_span!(
            "gdal_pool_read_md",
            read_id = %read_id,
            dataset = %dataset_params.file_path.display(),
            group = ?group,
            array = array_name,
            z_start = z_range.start,
            z_end = z_range.end,
        );

        let request = IpcChannelMessage::new_request_tile_message(IpcChannelMessagePayload {
            dataset_params,
            read_advise,
            data_type: T::TYPE,
            read_id: Some(read_id),
            read_kind: GdalReadKind::MdArray {
                group: group.map(str::to_string),
                array_name: array_name.to_string(),
                z_range,
                leading_prefix: leading_prefix.to_vec(),
            },
        });

        let res = self
            .dispatcher()
            .read_md_batch_data::<T>(request)
            .instrument(span)
            .await;

        let Some(payloads) = read_result_or_no_data(res, file_not_found_as_no_data)? else {
            return Ok(GdalProcessMdReadResult::FileNotFoundAsNoData);
        };

        let grids = payloads
            .into_iter()
            .map(|p| {
                let GridAndProperties { grid, properties } = p.into();
                let grid = flip_grid_y_if_needed(grid, read_advise.flip_y);
                GridAndProperties { grid, properties }
            })
            .collect();
        Ok(GdalProcessMdReadResult::Grids(grids))
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
