use gdal::raster::GdalType;
use geoengine_datatypes::raster::{GridBoundingBox2D, GridOrEmpty, MaskedGrid, Pixel};
use num::FromPrimitive;
use tracing::Instrument;

use crate::source::gdal_worker_process::{
    FileNotFoundHandling, GdalDatasetParameters, GdalPoolDispatcher, GdalProcessPoolError,
    GridAndProperties,
    process_common::{
        GdalReadAdvise, IpcChannelMessage, IpcChannelMessagePayload, IpcProcessError,
        IpcProcessGdalErrorKind,
    },
};

/// Result of reading a tile through the GDAL worker process.
pub enum GdalProcessReadResult<T: Pixel> {
    /// The tile data was read successfully.
    Grid(GridAndProperties<T, GridBoundingBox2D>),
    /// The file was not found and the dataset is configured to treat that as no-data.
    FileNotFoundAsNoData,
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
                },
            ))
            .instrument(span)
            .await;

        match res {
            Ok(t) => {
                // First, convert response to GridAndProperties
                let GridAndProperties { grid, properties } = t.into();
                // Second, flip y-axis if necessary
                let grid = if read_advise.flip_y {
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
                Ok(GdalProcessReadResult::Grid(GridAndProperties {
                    grid,
                    properties,
                }))
            }
            Err(GdalProcessPoolError::IpcProcessError {
                source:
                    IpcProcessError::GdalError {
                        kind: IpcProcessGdalErrorKind::FileNotFound,
                        details: _details,
                    },
            }) if file_not_found_as_no_data => Ok(GdalProcessReadResult::FileNotFoundAsNoData),
            Err(other_err) => Err(other_err),
        }
    }
}
