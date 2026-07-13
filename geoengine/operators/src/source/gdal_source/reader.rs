use gdal::raster::GdalType;
use geoengine_datatypes::raster::{
    EmptyGrid, GridBoundingBox2D, GridOrEmpty, MaskedGrid, Pixel, RasterProperties,
    grid_blit_valid_only,
};
use num::FromPrimitive;

use crate::source::gdal_in::{
    FileNotFoundHandling, GdalDatasetParameters, GdalPoolWorkerInstance, GdalProcessPoolError,
    GridAndProperties,
    process_common::{
        GdalReadAdvise, IpcChannelMessage, IpcChannelMessagePayload, IpcProcessError,
        IpcProcessGdalErrorKind,
    },
};

pub struct GdalPoolReader(GdalPoolWorkerInstance);

impl From<GdalPoolWorkerInstance> for GdalPoolReader {
    fn from(value: GdalPoolWorkerInstance) -> Self {
        GdalPoolReader(value)
    }
}

impl GdalPoolReader {
    #[inline]
    fn worker_instance(&self) -> &GdalPoolWorkerInstance {
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
        read_advise: GdalReadAdvise,
    ) -> Result<GridAndProperties<T, GridBoundingBox2D>, GdalProcessPoolError> {
        let file_not_found_as_no_data =
            dataset_params.file_not_found_handling == FileNotFoundHandling::NoData;

        let message = IpcChannelMessage::new_request_tile_message(IpcChannelMessagePayload {
            dataset_params,
            read_advise,
            data_type: T::TYPE,
            span_context: Default::default(),
            is_follower: false,
        });

        let res: Result<_, _> = self.worker_instance().read_data(message).await;

        let res = match res {
            Ok(t) => {
                // Here we need to handle edges!
                // First, convert response to GridAndProperties
                let GridAndProperties { grid, properties }: GridAndProperties<
                    T,
                    GridBoundingBox2D,
                > = t.into();
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
                // Third, blit the grid to the tile bounds if necessary
                let grid = if read_advise.direct_read() {
                    grid
                } else {
                    let mut tile_raster =
                        GridOrEmpty::from(EmptyGrid::new(read_advise.bounds_of_target));
                    grid_blit_valid_only(&mut tile_raster, &grid);
                    tile_raster
                };
                Ok(GridAndProperties { grid, properties })
            }
            Err(GdalProcessPoolError::IpcProcessError {
                source:
                    IpcProcessError::GdalError {
                        kind: IpcProcessGdalErrorKind::FileNotFound,
                        details: _details,
                    },
            }) if file_not_found_as_no_data => Ok(GridAndProperties {
                grid: GridOrEmpty::new_empty_shape(read_advise.bounds_of_target),
                properties: RasterProperties::default(),
            }),
            Err(other_err) => Err(other_err),
        }?;

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{AxisAlignedRectangle, CacheHint, SpatialPartition2D, TimeInterval},
        raster::{
            ChangeGridBounds, GeoTransform, GridShape2D, GridSize, RasterPropertiesEntry,
            RasterPropertiesEntryType, RasterPropertiesKey, RasterTile2D, TileInformation,
        },
        test_data,
    };

    use crate::source::gdal_in::{
        GdalDatasetGeoTransform, GdalMetadataMapping, GdalProcessPool, GdalProcessPoolAccess,
        process_common::GdalReadWindow,
    };

    use super::*;

    // TODO (low): name / test
    async fn load_ndvi_jan_2014_by_process(
        gdal_read_advice: GdalReadAdvise,
        tile_information: TileInformation,
        gdal_worker: GdalPoolWorkerInstance,
    ) -> Result<RasterTile2D<u8>, GdalProcessPoolError> {
        let dataset_params = GdalDatasetParameters {
            file_path: test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF").into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (-180., 90.).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            width: 3600,
            height: 1800,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: Some(0.),
            properties_mapping: Some(vec![
                GdalMetadataMapping {
                    source_key: RasterPropertiesKey {
                        domain: None,
                        key: "AREA_OR_POINT".to_string(),
                    },
                    target_type: RasterPropertiesEntryType::String,
                    target_key: RasterPropertiesKey {
                        domain: None,
                        key: "AREA_OR_POINT".to_string(),
                    },
                },
                GdalMetadataMapping {
                    source_key: RasterPropertiesKey {
                        domain: Some("IMAGE_STRUCTURE".to_string()),
                        key: "COMPRESSION".to_string(),
                    },
                    target_type: RasterPropertiesEntryType::String,
                    target_key: RasterPropertiesKey {
                        domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
                        key: "COMPRESSION".to_string(),
                    },
                },
            ]),
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        };

        GdalPoolReader::from(gdal_worker)
            .load_tile_data_process::<u8>(dataset_params, gdal_read_advice)
            .await
            .map(|r| {
                RasterTile2D::new_with_tile_info_and_properties(
                    TimeInterval::default(),
                    tile_information,
                    0,
                    r.grid.unbounded(),
                    r.properties,
                    CacheHint::default(),
                )
            })
            .map_err(Into::into)
    }

    fn tile_information_with_partition_and_shape(
        partition: SpatialPartition2D,
        shape: GridShape2D,
    ) -> TileInformation {
        let real_geotransform = GeoTransform::new(
            partition.upper_left(),
            partition.size_x() / shape.axis_size_x() as f64,
            -partition.size_y() / shape.axis_size_y() as f64,
        );

        TileInformation {
            tile_size_in_pixels: shape,
            global_tile_position: [0, 0].into(),
            global_geo_transform: real_geotransform,
        }
    }

    #[tokio::test]
    async fn test_load_tile_data_process() {
        let output_shape: GridShape2D = [8, 8].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (-179.2, 89.2).into());

        let gdal_read_advice = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), output_shape),
            read_window_bounds: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            flip_y: false,
        };

        let tile_information =
            tile_information_with_partition_and_shape(output_bounds, output_shape);

        let gpp = GdalProcessPool::new(2, 2, 2, None);
        let gw = gpp.get_gdal_worker();

        let RasterTile2D {
            global_geo_transform: _,
            grid_array: grid,
            tile_position: _,
            band: _,
            time: _,
            properties,
            cache_hint: _,
        } = load_ndvi_jan_2014_by_process(gdal_read_advice, tile_information, gw)
            .await
            .unwrap();

        assert!(!grid.is_empty());

        let grid = grid.into_materialized_masked_grid();

        assert_eq!(grid.inner_grid.data.len(), 64);
        assert_eq!(
            grid.inner_grid.data,
            &[
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255
            ]
        );

        assert_eq!(grid.validity_mask.data.len(), 64);
        assert_eq!(grid.validity_mask.data, &[true; 64]);

        assert!((properties.scale_option()).is_none());
        assert!(properties.offset_option().is_none());
        assert_eq!(
            properties.get_property(&RasterPropertiesKey {
                domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
                key: "COMPRESSION".to_string(),
            }),
            Some(&RasterPropertiesEntry::String("LZW".to_string()))
        );
        assert_eq!(
            properties.get_property(&RasterPropertiesKey {
                domain: None,
                key: "AREA_OR_POINT".to_string(),
            }),
            Some(&RasterPropertiesEntry::String("Area".to_string()))
        );
    }
}
