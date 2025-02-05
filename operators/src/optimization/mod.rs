use geoengine_datatypes::primitives::SpatialResolution;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum OptimizationError {
    TargetResolutionMustBeHigherThanSourceResolution {
        source_resolution: SpatialResolution,
        target_resolution: SpatialResolution,
    },
    OptimizationNotYetImplementedForOperator,
}

// for optimization the output resolution should be a multiple of the input resolution
pub fn resolution_compatible_for_downsampling(
    source_resolution: SpatialResolution,
    target_resolution: SpatialResolution,
) -> bool {
    target_resolution.x > source_resolution.x
        && target_resolution.y > source_resolution.y
        && target_resolution.x % source_resolution.x == 0.
        && target_resolution.y % source_resolution.y == 0.
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        dataset::{DataId, DatasetId, NamedData},
        raster::{GeoTransform, GridBoundingBox2D, RasterDataType, RasterTile2D, RenameBands},
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    use crate::{
        engine::{
            MockExecutionContext, MultipleRasterSources, RasterBandDescriptors, RasterOperator,
            RasterResultDescriptor, SingleRasterSource, SpatialGridDescriptor,
            WorkflowOperatorPath,
        },
        mock::{MockRasterSource, MockRasterSourceParams},
        processing::{Expression, ExpressionParams, RasterStacker, RasterStackerParams},
        source::{GdalSource, GdalSourceParameters},
        util::gdal::add_ndvi_dataset,
    };

    use super::*;

    #[tokio::test]
    async fn it_optimizes_gdal_source() {
        let mut exe_ctx = MockExecutionContext::test_default();

        let ndvi_id = add_ndvi_dataset(&mut exe_ctx);

        let gdal = GdalSource {
            params: GdalSourceParameters {
                data: ndvi_id.clone(),
                overview_level: None,
            },
        }
        .boxed();

        let gdal_initialized = gdal
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        assert_eq!(
            gdal_initialized
                .result_descriptor()
                .spatial_grid
                .spatial_resolution(),
            SpatialResolution::new(0.1, 0.1).unwrap()
        );

        let gdal_optimized = gdal_initialized
            .optimize(SpatialResolution::new(1., 1.).unwrap())
            .unwrap();

        let json = serde_json::to_value(&gdal_optimized).unwrap();

        assert_eq!(
            json,
            serde_json::json!({
                "params": {
                    "outputOriginReference": null,
                    "outputResolution": {
                        "type": "resolution",
                        "x": 1.0,
                        "y": 1.0
                    },
                    "samplingMethod": "nearestNeighbor"
                },
                "sources": {
                    "raster": {
                        "params": {
                            "data": "ndvi",
                            "overviewLevel": 3
                        },
                        "type": "GdalSource"
                    }
                },
                "type": "Downsampling"
            })
        );

        let gdal_optimized_initialized = gdal_optimized
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        assert_eq!(
            gdal_optimized_initialized
                .result_descriptor()
                .spatial_grid
                .spatial_resolution(),
            SpatialResolution::new(1., 1.).unwrap()
        );
    }

    #[tokio::test]
    async fn it_optimizes_expression() {
        let mut exe_ctx = MockExecutionContext::test_default();

        let ndvi_id = add_ndvi_dataset(&mut exe_ctx);

        let gdal = GdalSource {
            params: GdalSourceParameters {
                data: ndvi_id.clone(),
                overview_level: None,
            },
        }
        .boxed();

        let expression = Expression {
            params: ExpressionParams {
                expression: "A + 1".to_string(),
                output_type: RasterDataType::U8,
                output_band: None,
                map_no_data: false,
            },
            sources: SingleRasterSource { raster: gdal },
        }
        .boxed();

        let expression_initialized = expression
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        assert_eq!(
            expression_initialized
                .result_descriptor()
                .spatial_grid
                .spatial_resolution(),
            SpatialResolution::new(0.1, 0.1).unwrap()
        );

        let expression_optimized = expression_initialized
            .optimize(SpatialResolution::new(1., 1.).unwrap())
            .unwrap();

        let json = serde_json::to_value(&expression_optimized).unwrap();
        dbg!(json.to_string());

        assert_eq!(
            json,
            serde_json::json!({
                "params": {
                    "expression": "A + 1",
                    "mapNoData": false,
                    "outputBand": {
                        "measurement": {
                            "type": "unitless"
                        },
                        "name": "expression"
                    },
                    "outputType": "U8"
                },
                "sources": {
                    "raster": {
                        "params": {
                            "outputOriginReference": null,
                            "outputResolution": {
                                "type": "resolution",
                                "x": 1.0,
                                "y": 1.0
                            },
                            "samplingMethod": "nearestNeighbor"
                        },
                        "sources": {
                            "raster": {
                                "params": {
                                    "data": "ndvi",
                                    "overviewLevel": 3
                                },
                                "type": "GdalSource"
                            }
                        },
                        "type": "Downsampling"
                    }
                },
                "type": "Expression"
            })
        );

        let expression_optimized_initialized = expression_optimized
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        assert_eq!(
            expression_optimized_initialized
                .result_descriptor()
                .spatial_grid
                .spatial_resolution(),
            SpatialResolution::new(1., 1.).unwrap()
        );
    }

    fn mock_raster_source(resolution: SpatialResolution) -> Box<dyn RasterOperator> {
        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: None,
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new_with_coordinate_x_y(0.0, resolution.x, 0.0, -resolution.y),
                GridBoundingBox2D::new([-2, 0], [0, 4]).unwrap(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let data: Vec<RasterTile2D<u8>> = vec![];

        MockRasterSource {
            params: MockRasterSourceParams {
                data,
                result_descriptor,
            },
        }
        .boxed()
    }

    // #[tokio::test]
    // async fn it_removes_upsampling() {
    //     let mut exe_ctx = MockExecutionContext::test_default();

    //     let id: DataId = DatasetId::new().into();
    //     exe_ctx.add_meta_data(
    //         id.clone(),
    //         NamedData::with_system_name("data0"),
    //         Box::new(MockDatasetDataSourceLoadingInfo {
    //             points: vec![Coordinate2D::new(1., 2.); 3],
    //         }),
    //     );

    //     let mps = MockDatasetDataSource {
    //         params: MockDatasetDataSourceParams {
    //             data: NamedData::with_system_name("points"),
    //         },
    //     }
    //     .boxed();

    //     let stacker = RasterStacker {
    //         params: RasterStackerParams {
    //             rename_bands: RenameBands::Default,
    //         },
    //         sources: MultipleRasterSources {
    //             rasters: vec![
    //                 mock_raster_source(SpatialResolution::new_unchecked(0.1, 0.1)),
    //                 mock_raster_source(SpatialResolution::new_unchecked(0.1, 0.1)),
    //             ],
    //         },
    //     }
    //     .boxed();

    //     let gdal_initialized = gdal
    //         .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
    //         .await
    //         .unwrap();

    //     assert_eq!(
    //         gdal_initialized
    //             .result_descriptor()
    //             .spatial_grid
    //             .spatial_resolution(),
    //         SpatialResolution::new(0.1, 0.1).unwrap()
    //     );

    //     let gdal_optimized = gdal_initialized
    //         .optimize(SpatialResolution::new(1., 1.).unwrap())
    //         .unwrap();

    //     let json = serde_json::to_value(&gdal_optimized).unwrap();

    //     assert_eq!(
    //         json,
    //         serde_json::json!({
    //             "params": {
    //                 "outputOriginReference": null,
    //                 "outputResolution": {
    //                     "type": "resolution",
    //                     "x": 1.0,
    //                     "y": 1.0
    //                 },
    //                 "samplingMethod": "nearestNeighbor"
    //             },
    //             "sources": {
    //                 "raster": {
    //                     "params": {
    //                         "data": "ndvi",
    //                         "overviewLevel": 3
    //                     },
    //                     "type": "GdalSource"
    //                 }
    //             },
    //             "type": "Downsampling"
    //         })
    //     );

    //     let gdal_optimized_initialized = gdal_optimized
    //         .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
    //         .await
    //         .unwrap();

    //     assert_eq!(
    //         gdal_optimized_initialized
    //             .result_descriptor()
    //             .spatial_grid
    //             .spatial_resolution(),
    //         SpatialResolution::new(1., 1.).unwrap()
    //     );
    // }
}
