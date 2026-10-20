use gdal::raster::GdalType;
use geoengine_datatypes::raster::{
    GridBoundingBox2D, GridOrEmpty, MaskedGrid, Pixel, RasterDataType,
};
use num::FromPrimitive;
use tracing::Instrument;

use crate::source::gdal_worker_process::{
    FileNotFoundHandling, GdalDatasetParameters, GdalPoolDispatcher, GdalProcessPoolError,
    GridAndProperties,
    process_common::{
        GdalDataGridVariant, GdalIpcMdPayload, GdalIpcPayload, GdalReadAdvise, GdalReadKind,
        GdalReadWindow, IpcChannelMessage, IpcChannelMessagePayload, IpcChannelResult,
        IpcProcessError, IpcProcessGdalErrorKind, MdArrayInfo, MdAxisMapping, RasterInfo,
    },
};

/// Result of reading a tile through the GDAL worker process.
pub enum GdalProcessReadResult<T: Pixel> {
    /// The tile data was read successfully.
    Grid(Box<GridAndProperties<T, GridBoundingBox2D>>),
    /// The file was not found and the dataset is configured to treat that as no-data.
    FileNotFoundAsNoData,
}

/// Result of reading a z-slice of a multidimensional array through the GDAL worker process.
pub enum GdalProcessMdReadResult<T: Pixel> {
    /// The grid of the requested z-slice.
    Grid(Box<GridAndProperties<T, GridBoundingBox2D>>),
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

/// Gathers the z-slice of a worker buffer into row-major `[y, x]` order. The worker reads one
/// z index, so a z axis has size 1 and adds no offset.
fn gather_md_slice<T: Copy>(
    data: &[T],
    axis_sizes: &[usize],
    axis_mapping: &[MdAxisMapping],
    size_y: usize,
    size_x: usize,
) -> Result<Vec<T>, IpcProcessError> {
    if axis_sizes.len() != axis_mapping.len() {
        return Err(IpcProcessError::IpcOther {
            msg: "MD payload dimension count does not match its axis mapping".to_string(),
        });
    }

    let axis_index = |role: fn(&MdAxisMapping) -> bool, name: &str| {
        axis_mapping
            .iter()
            .position(role)
            .ok_or_else(|| IpcProcessError::IpcOther {
                msg: format!("MD axis mapping is missing {name}"),
            })
    };
    // A variable without a z role is a single slice.
    let z_size = axis_mapping
        .iter()
        .position(|axis| axis.is_z())
        .map_or(1, |axis| axis_sizes[axis]);
    let y_axis = axis_index(|axis| *axis == MdAxisMapping::Y, "Y")?;
    let x_axis = axis_index(|axis| *axis == MdAxisMapping::X, "X")?;

    if (z_size, axis_sizes[y_axis], axis_sizes[x_axis]) != (1, size_y, size_x) {
        return Err(IpcProcessError::IpcOther {
            msg: format!(
                "MD payload shape {:?} does not match requested z/y/x shape (1, {size_y}, {size_x})",
                (z_size, axis_sizes[y_axis], axis_sizes[x_axis])
            ),
        });
    }

    let expected_len = axis_sizes
        .iter()
        .try_fold(1usize, |size, axis_size| size.checked_mul(*axis_size));
    if expected_len != Some(data.len()) {
        return Err(IpcProcessError::IpcOther {
            msg: format!(
                "MD buffer has {} values but shape {:?} requires {}",
                data.len(),
                axis_sizes,
                expected_len.unwrap_or(usize::MAX)
            ),
        });
    }

    let mut strides = vec![1usize; axis_sizes.len()];
    for axis in (0..axis_sizes.len().saturating_sub(1)).rev() {
        strides[axis] = strides[axis + 1] * axis_sizes[axis + 1];
    }

    let (y_stride, x_stride) = (strides[y_axis], strides[x_axis]);
    let mut slice = Vec::with_capacity(size_y * size_x);
    for y in 0..size_y {
        let row = y * y_stride;
        if x_stride == 1 {
            slice.extend_from_slice(&data[row..row + size_x]);
        } else {
            slice.extend((0..size_x).map(|x| data[row + x * x_stride]));
        }
    }

    Ok(slice)
}

fn split_md_slice<T: Pixel>(
    batch: GdalIpcMdPayload<T>,
    axis_mapping: &[MdAxisMapping],
    read_advise: GdalReadAdvise,
) -> Result<GdalIpcPayload<T>, IpcProcessError> {
    let size_x = read_advise.gdal_read_widow.size_x;
    let size_y = read_advise.gdal_read_widow.size_y;
    size_y
        .checked_mul(size_x)
        .ok_or_else(|| IpcProcessError::IpcOther {
            msg: "MD slice dimensions overflow addressable memory".to_string(),
        })?;

    let GdalIpcMdPayload {
        axis_sizes,
        properties,
        data_variant,
    } = batch;
    let gather = |values: &[T]| gather_md_slice(values, &axis_sizes, axis_mapping, size_y, size_x);
    let data_variant = match data_variant {
        GdalDataGridVariant::Empty => GdalDataGridVariant::Empty,
        GdalDataGridVariant::AllValid { data } => GdalDataGridVariant::AllValid {
            data: gather(&data)?,
        },
        GdalDataGridVariant::WithNoData {
            data,
            no_data_value,
        } => GdalDataGridVariant::WithNoData {
            data: gather(&data)?,
            no_data_value,
        },
        GdalDataGridVariant::WithExplicitMask {
            data,
            validity_mask,
        } => GdalDataGridVariant::WithExplicitMask {
            data: gather(&data)?,
            validity_mask: gather_md_slice(
                &validity_mask,
                &axis_sizes,
                axis_mapping,
                size_y,
                size_x,
            )?,
        },
    };

    Ok(GdalIpcPayload {
        dimensions: read_advise.read_window_bounds,
        properties,
        data_variant,
    })
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

    /// Reads the shape and declared CRS of the MD array `array_name` (in `group`) in a worker,
    /// so the server never opens the file itself. A missing file is an error here, whatever
    /// `file_not_found_handling` says: there is no header to report.
    ///
    /// # Errors
    /// Returns a `GdalProcessPoolError` if the file, group or array cannot be opened.
    pub async fn read_md_array_info(
        &self,
        dataset_params: GdalDatasetParameters,
        group: Option<&str>,
        array_name: &str,
    ) -> Result<MdArrayInfo, GdalProcessPoolError> {
        let read_kind = GdalReadKind::MdArrayInfo {
            group: group.map(str::to_string),
            array_name: array_name.to_string(),
        };
        match self.read_header(dataset_params, read_kind).await? {
            IpcChannelResult::MdArrayInfo(info) => Ok(info),
            _ => Err(unexpected_header()),
        }
    }

    /// Reads the data type of the `rasterband_channel` band and the CRS of a classic raster in
    /// a worker, so the server never opens the file itself. A missing file is an error here,
    /// whatever `file_not_found_handling` says.
    ///
    /// # Errors
    /// Returns a `GdalProcessPoolError` if the file or band cannot be opened, the data type is
    /// not supported or the file has no CRS.
    pub async fn read_raster_info(
        &self,
        dataset_params: GdalDatasetParameters,
    ) -> Result<RasterInfo, GdalProcessPoolError> {
        match self
            .read_header(dataset_params, GdalReadKind::RasterInfo)
            .await?
        {
            IpcChannelResult::RasterInfo(info) => Ok(info),
            _ => Err(unexpected_header()),
        }
    }

    async fn read_header(
        &self,
        dataset_params: GdalDatasetParameters,
        read_kind: GdalReadKind,
    ) -> Result<IpcChannelResult, GdalProcessPoolError> {
        let span = tracing::info_span!(
            "gdal_pool_read_header",
            dataset = %dataset_params.file_path.display(),
            read_kind = ?read_kind,
        );
        let empty = GridBoundingBox2D::new_unchecked([0, 0], [0, 0]);
        let request = IpcChannelMessage::new_request_tile_message(IpcChannelMessagePayload {
            dataset_params,
            read_advise: GdalReadAdvise {
                gdal_read_widow: GdalReadWindow::new([0, 0].into(), [0, 0].into()),
                read_window_bounds: empty,
                bounds_of_target: empty,
                flip_y: false,
            },
            data_type: RasterDataType::U8,
            read_id: None,
            read_kind,
        });

        self.dispatcher()
            .read_header(request)
            .instrument(span)
            .await
    }

    /// Reads one z-slice from a multidim array via the worker process in a single request, applying the same post-processing (Y-axis flip per z-slice)
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
    pub async fn read_md_slice_data<T: Pixel + GdalType + FromPrimitive>(
        &self,
        dataset_params: GdalDatasetParameters,
        read_advise: GdalReadAdvise,
        group: Option<&str>,
        array_name: &str,
        z_index: usize,
        axis_mapping: &[MdAxisMapping],
    ) -> Result<GdalProcessMdReadResult<T>, GdalProcessPoolError> {
        let file_not_found_as_no_data =
            dataset_params.file_not_found_handling == FileNotFoundHandling::NoData;

        // extend the read_id with the MD-specific request parts
        let read_id = format!(
            "{}-{array_name}-{z_index}",
            dataset_params.create_read_id(&read_advise),
        );

        let span = tracing::info_span!(
            "gdal_pool_read_md",
            read_id = %read_id,
            dataset = %dataset_params.file_path.display(),
            group = ?group,
            array = array_name,
            z_index,
        );

        let request = IpcChannelMessage::new_request_tile_message(IpcChannelMessagePayload {
            dataset_params,
            read_advise,
            data_type: T::TYPE,
            read_id: Some(read_id),
            read_kind: GdalReadKind::MdArray {
                group: group.map(str::to_string),
                array_name: array_name.to_string(),
                z_index,
                axis_mapping: axis_mapping.to_vec(),
            },
        });

        let res = self
            .dispatcher()
            .read_md_slice_data::<T>(request)
            .instrument(span)
            .await;

        let Some(payload) = read_result_or_no_data(res, file_not_found_as_no_data)? else {
            return Ok(GdalProcessMdReadResult::FileNotFoundAsNoData);
        };

        let slice = split_md_slice(payload, axis_mapping, read_advise)?;
        let GridAndProperties { grid, properties } = slice.into();
        let grid = flip_grid_y_if_needed(grid, read_advise.flip_y);
        Ok(GdalProcessMdReadResult::Grid(Box::new(GridAndProperties {
            grid,
            properties,
        })))
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

fn unexpected_header() -> GdalProcessPoolError {
    GdalProcessPoolError::IpcProcessError {
        source: IpcProcessError::IpcOther {
            msg: "the worker answered a header read with a different result kind".to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::gdal_worker_process::process_common::GdalReadWindow;
    use geoengine_datatypes::raster::{GridShape2D, RasterProperties};

    fn advise() -> GdalReadAdvise {
        GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), GridShape2D::new([2, 2])),
            read_window_bounds: GridBoundingBox2D::new_unchecked([0, 0], [1, 1]),
            bounds_of_target: GridBoundingBox2D::new_unchecked([0, 0], [1, 1]),
            flip_y: false,
        }
    }

    fn all_valid_data(payload: GdalIpcPayload<i32>) -> Vec<i32> {
        match payload.data_variant {
            GdalDataGridVariant::AllValid { data } => data,
            other => panic!("expected all-valid data, got {other:?}"),
        }
    }

    #[test]
    fn md_slice_without_z_role_is_the_whole_buffer() {
        let batch = GdalIpcMdPayload {
            axis_sizes: vec![2, 2],
            properties: RasterProperties::default(),
            data_variant: GdalDataGridVariant::AllValid {
                data: (0..4).collect(),
            },
        };
        let mapping = [MdAxisMapping::Y, MdAxisMapping::X];

        let payload = split_md_slice(batch, &mapping, advise()).unwrap();

        assert_eq!(all_valid_data(payload), vec![0, 1, 2, 3]);
    }

    #[test]
    fn md_slice_gathers_strided_x_axis() {
        let batch = GdalIpcMdPayload {
            axis_sizes: vec![2, 1, 2],
            properties: RasterProperties::default(),
            data_variant: GdalDataGridVariant::AllValid {
                data: (0..4).collect(),
            },
        };
        let mapping = [MdAxisMapping::X, MdAxisMapping::Time, MdAxisMapping::Y];

        let payload = split_md_slice(batch, &mapping, advise()).unwrap();

        assert_eq!(all_valid_data(payload), vec![0, 2, 1, 3]);
    }
}
