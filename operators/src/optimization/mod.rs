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
    use geoengine_datatypes::{raster::RasterDataType, util::test::TestDefault};

    use crate::{
        engine::{MockExecutionContext, RasterOperator, SingleRasterSource, WorkflowOperatorPath},
        processing::{Expression, ExpressionParams},
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
}
