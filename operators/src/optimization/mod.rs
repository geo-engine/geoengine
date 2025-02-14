use geoengine_datatypes::{error::ErrorSource, primitives::SpatialResolution};
use snafu::{ensure, Snafu};

use crate::engine::InitializedRasterOperator;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum OptimizationError {
    TargetResolutionMustNotBeSmallerThanSourceResolution {
        source_resolution: SpatialResolution,
        target_resolution: SpatialResolution,
    },
    TargetResolutionMustBeDivisibleBySourceResolution {
        source_resolution: SpatialResolution,
        target_resolution: SpatialResolution,
    },
    SourcesMustNotUseOverviews {
        data: String,
        oveview_level: u32,
    },
    OptimizationNotYetImplementedForOperator {
        operator: String,
    },
    ProjectionOptimizationFailed {
        source: Box<dyn ErrorSource>,
    },
}

// TODO: move `optimize` method from `RasterOperator` to `OptimizableOperator`
pub trait OptimizableOperator: InitializedRasterOperator {
    // TODO: automatically call before `optimize` or make it part of the `optimize` method
    fn ensure_resolution_is_compatible_for_optimization(
        &self,
        target_resolution: SpatialResolution,
    ) -> Result<(), OptimizationError> {
        let original_resolution = self.result_descriptor().spatial_grid.spatial_resolution();
        ensure!(
            target_resolution >= original_resolution,
            TargetResolutionMustNotBeSmallerThanSourceResolution {
                source_resolution: original_resolution,
                target_resolution,
            }
        );

        ensure!(
            target_resolution.x % original_resolution.x == 0.
                && target_resolution.y % original_resolution.y == 0.,
            TargetResolutionMustBeDivisibleBySourceResolution {
                source_resolution: original_resolution,
                target_resolution,
            }
        );

        Ok(())
    }
}

impl<T> OptimizableOperator for T where T: InitializedRasterOperator {}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        collections::VectorDataType,
        dataset::{DataId, DatasetId, NamedData},
        primitives::{BoundingBox2D, CacheTtlSeconds, VectorQueryRectangle},
        raster::{RasterDataType, RenameBands},
        spatial_reference::{SpatialReference, SpatialReferenceAuthority},
        test_data,
        util::{test::TestDefault, Identifier},
    };

    use crate::{
        engine::{
            MockExecutionContext, MultipleRasterSources, RasterOperator,
            SingleRasterOrVectorSource, SingleRasterSource, SingleVectorMultipleRasterSources,
            SingleVectorSource, StaticMetaData, VectorOperator, VectorResultDescriptor,
            WorkflowOperatorPath,
        },
        processing::{
            ColumnNames, ColumnRangeFilter, ColumnRangeFilterParams, DeriveOutRasterSpecsSource,
            Downsampling, DownsamplingMethod, DownsamplingParams, DownsamplingResolution,
            Expression, ExpressionParams, FeatureAggregationMethod, Interpolation,
            InterpolationMethod, InterpolationParams, InterpolationResolution, RasterStacker,
            RasterStackerParams, RasterTypeConversion, RasterTypeConversionParams,
            RasterVectorJoin, RasterVectorJoinParams, Rasterization, RasterizationParams,
            Reprojection, ReprojectionParams, TemporalAggregationMethod,
        },
        source::{
            GdalSource, GdalSourceParameters, OgrSource, OgrSourceDataset,
            OgrSourceDatasetTimeType, OgrSourceErrorSpec, OgrSourceParameters,
        },
        util::{
            gdal::{add_ndvi_dataset, add_ndvi_downscaled_3x_dataset},
            input::RasterOrVectorOperator,
        },
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
            .optimize(SpatialResolution::new(0.8, 0.8).unwrap())
            .unwrap();

        let json = serde_json::to_value(&gdal_optimized).unwrap();

        // TODO: add test where downsampling is injected because there are is no overview available with required level

        // assert_eq!(
        //     json,
        //     serde_json::json!({
        //         "params": {
        //             "outputOriginReference": null,
        //             "outputResolution": {
        //                 "type": "resolution",
        //                 "x":0.8,
        //                 "y": 0.8
        //             },
        //             "samplingMethod": "nearestNeighbor"
        //         },
        //         "sources": {
        //             "raster": {
        //                 "params": {
        //                     "data": "ndvi",
        //                     "overviewLevel": 3
        //                 },
        //                 "type": "GdalSource"
        //             }
        //         },
        //         "type": "Downsampling"
        //     })
        // );

        assert_eq!(
            json,
            serde_json::json!({
                    "params": {
                        "data": "ndvi",
                        "overviewLevel": 8
                    },
                    "type": "GdalSource"
                }
            )
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
            SpatialResolution::new(0.8, 0.8).unwrap()
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
            .optimize(SpatialResolution::new(0.8, 0.8).unwrap())
            .unwrap();

        let json = serde_json::to_value(&expression_optimized).unwrap();

        // assert_eq!(
        //     json,
        //     serde_json::json!({
        //         "params": {
        //             "expression": "A + 1",
        //             "mapNoData": false,
        //             "outputBand": {
        //                 "measurement": {
        //                     "type": "unitless"
        //                 },
        //                 "name": "expression"
        //             },
        //             "outputType": "U8"
        //         },
        //         "sources": {
        //             "raster": {
        //                 "params": {
        //                     "outputOriginReference": null,
        //                     "outputResolution": {
        //                         "type": "resolution",
        //                         "x": 0.8,
        //                         "y": 0.8
        //                     },
        //                     "samplingMethod": "nearestNeighbor"
        //                 },
        //                 "sources": {
        //                     "raster": {
        //                         "params": {
        //                             "data": "ndvi",
        //                             "overviewLevel": 3
        //                         },
        //                         "type": "GdalSource"
        //                     }
        //                 },
        //                 "type": "Downsampling"
        //             }
        //         },
        //         "type": "Expression"
        //     })
        // );

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
                            "data": "ndvi",
                            "overviewLevel": 8
                        },
                        "type": "GdalSource"
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
            SpatialResolution::new(0.8, 0.8).unwrap()
        );
    }

    #[tokio::test]
    async fn it_removes_upsampling() {
        todo!("need a workflow, dataset and target resolution that makes upsampling obsolete")
    }

    #[tokio::test]
    async fn it_reduces_interpolation_resolution() {
        let mut exe_ctx = MockExecutionContext::test_default();

        let ndvi_id = add_ndvi_dataset(&mut exe_ctx);
        let ndvi_downscaled_3x_id = add_ndvi_downscaled_3x_dataset(&mut exe_ctx);

        let workflow = RasterStacker {
            params: RasterStackerParams {
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![
                    GdalSource {
                        params: GdalSourceParameters {
                            data: ndvi_id.clone(),
                            overview_level: None,
                        },
                    }
                    .boxed(),
                    Interpolation {
                        params: InterpolationParams {
                            interpolation: InterpolationMethod::NearestNeighbor,
                            output_resolution: InterpolationResolution::Resolution(
                                SpatialResolution::new_unchecked(0.1, 0.1),
                            ),
                            output_origin_reference: None,
                        },
                        sources: SingleRasterSource {
                            raster: GdalSource {
                                params: GdalSourceParameters {
                                    data: ndvi_downscaled_3x_id.clone(),
                                    overview_level: None,
                                },
                            }
                            .boxed(),
                        },
                    }
                    .boxed(),
                ],
            },
        }
        .boxed();

        let workflow_initialized = workflow
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        assert_eq!(
            workflow_initialized
                .result_descriptor()
                .spatial_grid
                .spatial_resolution(),
            SpatialResolution::new(0.1, 0.1).unwrap()
        );

        let workflow_optimized = workflow_initialized
            .optimize(SpatialResolution::new(0.2, 0.2).unwrap())
            .unwrap();

        let json = serde_json::to_value(&workflow_optimized).unwrap();

        assert_eq!(
            json,
            serde_json::json!({
                "params": {
                    "renameBands": {
                        "type": "default"
                    }
                },
                "sources": {
                    "rasters": [
                        {
                            "params": {
                                "data": "ndvi",
                                "overviewLevel": 2
                            },
                            "type": "GdalSource"
                        },
                        {
                            "params": {
                                "interpolation": "nearestNeighbor",
                                "outputOriginReference": null,
                                "outputResolution": {
                                    "type": "resolution",
                                    "x": 0.2,
                                    "y": 0.2
                                }
                            },
                            "sources": {
                                "raster": {
                                    "params": {
                                        "data": "ndvi_downscaled_3x",
                                        "overviewLevel": 0
                                    },
                                    "type": "GdalSource"
                                }
                            },
                            "type": "Interpolation"
                        }
                    ]
                },
                "type": "RasterStacker"
            })
        );

        let gdal_optimized_initialized = workflow_optimized
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        assert_eq!(
            gdal_optimized_initialized
                .result_descriptor()
                .spatial_grid
                .spatial_resolution(),
            SpatialResolution::new(0.2, 0.2).unwrap()
        );
    }

    #[tokio::test]
    async fn it_replaces_upsampling_with_downsampling() {
        let mut exe_ctx = MockExecutionContext::test_default();

        let ndvi_id = add_ndvi_dataset(&mut exe_ctx);
        let ndvi_downscaled_3x_id = add_ndvi_downscaled_3x_dataset(&mut exe_ctx);

        let workflow = RasterStacker {
            params: RasterStackerParams {
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![
                    GdalSource {
                        params: GdalSourceParameters {
                            data: ndvi_id.clone(),
                            overview_level: None,
                        },
                    }
                    .boxed(),
                    Interpolation {
                        params: InterpolationParams {
                            interpolation: InterpolationMethod::NearestNeighbor,
                            output_resolution: InterpolationResolution::Resolution(
                                SpatialResolution::new_unchecked(0.1, 0.1),
                            ),
                            output_origin_reference: None,
                        },
                        sources: SingleRasterSource {
                            raster: GdalSource {
                                params: GdalSourceParameters {
                                    data: ndvi_downscaled_3x_id.clone(),
                                    overview_level: None,
                                },
                            }
                            .boxed(),
                        },
                    }
                    .boxed(),
                ],
            },
        }
        .boxed();

        let workflow_initialized = workflow
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        assert_eq!(
            workflow_initialized
                .result_descriptor()
                .spatial_grid
                .spatial_resolution(),
            SpatialResolution::new(0.1, 0.1).unwrap()
        );

        let workflow_optimized = workflow_initialized
            .optimize(SpatialResolution::new(0.8, 0.8).unwrap())
            .unwrap();

        let json = serde_json::to_value(&workflow_optimized).unwrap();

        assert_eq!(
            json,
            serde_json::json!({
                "params": {
                    "renameBands": {
                        "type": "default"
                    }
                },
                "sources": {
                    "rasters": [
                        {
                            "params": {
                                "data": "ndvi",
                                "overviewLevel": 8
                            },
                            "type": "GdalSource"
                        },
                        {
                            "params": {
                                "outputOriginReference": null,
                                "outputResolution": {
                                    "type": "resolution",
                                    "x": 0.8,
                                    "y": 0.8
                                },
                                "samplingMethod": "nearestNeighbor"
                            },
                            "sources": {
                                "raster": {
                                    "params": {
                                        "data": "ndvi_downscaled_3x",
                                        "overviewLevel": 2
                                    },
                                    "type": "GdalSource"
                                }
                            },
                            "type": "Downsampling"
                        }
                    ]
                },
                "type": "RasterStacker"
            })
        );

        let gdal_optimized_initialized = workflow_optimized
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        assert_eq!(
            gdal_optimized_initialized
                .result_descriptor()
                .spatial_grid
                .spatial_resolution(),
            SpatialResolution::new(0.8, 0.8).unwrap()
        );
    }

    #[tokio::test]
    async fn it_optimizes_downsampling() {
        let mut exe_ctx = MockExecutionContext::test_default();

        let ndvi_id = add_ndvi_dataset(&mut exe_ctx);
        let ndvi_downscaled_3x_id = add_ndvi_downscaled_3x_dataset(&mut exe_ctx);

        let workflow = RasterStacker {
            params: RasterStackerParams {
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![
                    GdalSource {
                        params: GdalSourceParameters {
                            data: ndvi_downscaled_3x_id.clone(),
                            overview_level: None,
                        },
                    }
                    .boxed(),
                    Downsampling {
                        params: DownsamplingParams {
                            sampling_method: DownsamplingMethod::NearestNeighbor,
                            output_resolution: DownsamplingResolution::Resolution(
                                SpatialResolution::new_unchecked(0.3, 0.3),
                            ),
                            output_origin_reference: None,
                        },
                        sources: SingleRasterSource {
                            raster: GdalSource {
                                params: GdalSourceParameters {
                                    data: ndvi_id.clone(),
                                    overview_level: None,
                                },
                            }
                            .boxed(),
                        },
                    }
                    .boxed(),
                ],
            },
        }
        .boxed();

        let workflow_initialized = workflow
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        assert_eq!(
            workflow_initialized
                .result_descriptor()
                .spatial_grid
                .spatial_resolution(),
            SpatialResolution::new(0.3, 0.3).unwrap()
        );

        let workflow_optimized = workflow_initialized
            .optimize(SpatialResolution::new(0.6, 0.6).unwrap())
            .unwrap();

        let json = serde_json::to_value(&workflow_optimized).unwrap();

        assert_eq!(
            json,
            serde_json::json!({
                "params": {
                    "renameBands": {
                        "type": "default"
                    }
                },
                "sources": {
                    "rasters": [
                        {
                            "params": {
                                "data": "ndvi_downscaled_3x",
                                "overviewLevel": 2
                            },
                            "type": "GdalSource"
                        },
                        {
                            "params": {
                                "outputOriginReference": null,
                                "outputResolution": {
                                    "type": "resolution",
                                    "x": 0.6,
                                    "y": 0.6
                                },
                                "samplingMethod": "nearestNeighbor"
                            },
                            "sources": {
                                "raster": {
                                    "params": {
                                        "data": "ndvi",
                                        "overviewLevel": 4
                                    },
                                    "type": "GdalSource"
                                }
                            },
                            "type": "Downsampling"
                        }
                    ]
                },
                "type": "RasterStacker"
            })
        );

        let gdal_optimized_initialized = workflow_optimized
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        assert_eq!(
            gdal_optimized_initialized
                .result_descriptor()
                .spatial_grid
                .spatial_resolution(),
            SpatialResolution::new(0.6, 0.6).unwrap()
        );
    }

    #[tokio::test]
    async fn it_optimizes_reprojection() {
        let mut exe_ctx = MockExecutionContext::test_default();

        let ndvi_id = add_ndvi_dataset(&mut exe_ctx);

        let workflow: Box<dyn RasterOperator> = Box::new(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: SpatialReference::new(
                    SpatialReferenceAuthority::Epsg,
                    3857,
                ),
                derive_out_spec: DeriveOutRasterSpecsSource::default(),
            },
            sources: SingleRasterOrVectorSource {
                source: RasterOrVectorOperator::Raster(
                    GdalSource {
                        params: GdalSourceParameters {
                            data: ndvi_id.clone(),
                            overview_level: None,
                        },
                    }
                    .boxed(),
                ),
            },
        });

        let workflow_initialized = workflow
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let expected_resolution = 14255.015508816849;

        assert_eq!(
            workflow_initialized
                .result_descriptor()
                .spatial_grid
                .spatial_resolution(),
            SpatialResolution::new(expected_resolution, expected_resolution).unwrap()
        );

        let optimize_resolution = expected_resolution * 2.0;

        let workflow_optimized = workflow_initialized
            .optimize(SpatialResolution::new(optimize_resolution, optimize_resolution).unwrap())
            .unwrap();

        let json = serde_json::to_value(&workflow_optimized).unwrap();

        // require an interpolation in addition to the reprojection
        // TODO: should we instead load the source in higher resolution and downsample?
        //       or: we could make an exception that the reprojection may not have to produce the exact resolution, but how would we choose the correct resolution for the top level operator then?
        assert_eq!(
            json,
            serde_json::json!({
                "params": {
                    "interpolation": "nearestNeighbor",
                    "outputOriginReference": null,
                    "outputResolution": {
                        "type": "resolution",
                        "x": 28510.031017633697,
                        "y": 28510.031017633697
                    }
                },
                "sources": {
                    "raster": {
                        "params": {
                            "deriveOutSpec": "projectionBounds",
                            "targetSpatialReference": "EPSG:3857"
                        },
                        "sources": {
                            "source": {
                                "params": {
                                    "data": "ndvi",
                                    "overviewLevel": 2
                                },
                                "type": "GdalSource"
                            }
                        },
                        "type": "Reprojection"
                    }
                },
                "type": "Interpolation"
            })
        );

        let gdal_optimized_initialized = workflow_optimized
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        assert_eq!(
            gdal_optimized_initialized
                .result_descriptor()
                .spatial_grid
                .spatial_resolution(),
            SpatialResolution::new(optimize_resolution, optimize_resolution).unwrap()
        );

        // check that the reprojection was necessary
        let workflow: Box<dyn RasterOperator> = Box::new(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: SpatialReference::new(
                    SpatialReferenceAuthority::Epsg,
                    3857,
                ),
                derive_out_spec: DeriveOutRasterSpecsSource::default(),
            },
            sources: SingleRasterOrVectorSource {
                source: RasterOrVectorOperator::Raster(
                    GdalSource {
                        params: GdalSourceParameters {
                            data: ndvi_id.clone(),
                            overview_level: Some(2),
                        },
                    }
                    .boxed(),
                ),
            },
        });

        let workflow_initialized = workflow
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        assert!(
            workflow_initialized
                .result_descriptor()
                .spatial_grid
                .spatial_resolution()
                > SpatialResolution::new(optimize_resolution, optimize_resolution).unwrap()
        );
    }

    // TODO: a test with raster and vector data that pushes the resoltion down through the vector operator
    // idea: - Rasterization(?)
    //       - RasterVectorJoin, outputs a vector, but the raster shall be optimized
    #[tokio::test]
    async fn it_optimizes_rasterization() {
        let id: DataId = DatasetId::new().into();
        let name = NamedData::with_system_name("ne_10m_ports");
        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.add_meta_data::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>(
            id.clone(),
            name.clone(),
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!(
                        "vector/data/ne_10m_ports/with_spatial_index/ne_10m_ports.gpkg"
                    )
                    .into(),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: None,
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                    cache_ttl: CacheTtlSeconds::default(),
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: Default::default(),
                    time: None,
                    bbox: Some(BoundingBox2D::new_unchecked(
                        [-171.75795, -54.809444].into(),
                        [179.309364, 78.226111].into(),
                    )),
                },
                phantom: Default::default(),
            }),
        );

        let source = OgrSource {
            params: OgrSourceParameters {
                data: name,
                attribute_projection: None,
                attribute_filters: None,
            },
        }
        .boxed();

        let rasterization = Rasterization {
            params: RasterizationParams {
                spatial_resolution: SpatialResolution::new_unchecked(0.1, 0.1),
                origin_coordinate: (0., 0.).into(),
            },
            sources: SingleVectorSource { vector: source },
        }
        .boxed();

        let rasterization_initialized = rasterization
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        assert_eq!(
            rasterization_initialized
                .result_descriptor()
                .spatial_grid
                .spatial_resolution(),
            SpatialResolution::new(0.1, 0.1).unwrap()
        );

        let rasterization_optimized = rasterization_initialized
            .optimize(SpatialResolution::new(0.8, 0.8).unwrap())
            .unwrap();

        let json = serde_json::to_value(&rasterization_optimized).unwrap();

        assert_eq!(
            json,
            serde_json::json!({
                "params": {
                    "originCoordinate": {
                        "x": 0.0,
                        "y": 0.0
                    },
                    "spatialResolution": {
                        "x": 0.8,
                        "y": 0.8
                    }
                },
                "sources": {
                    "vector": {
                        "params": {
                            "attributeFilters": null,
                            "attributeProjection": null,
                            "data": "ne_10m_ports"
                        },
                        "type": "OgrSource"
                    }
                },
                "type": "Rasterization"
            })
        );

        let expression_optimized_initialized = rasterization_optimized
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        assert_eq!(
            expression_optimized_initialized
                .result_descriptor()
                .spatial_grid
                .spatial_resolution(),
            SpatialResolution::new(0.8, 0.8).unwrap()
        );
    }

    #[tokio::test]
    async fn it_optimizes_raster_vector_join() {
        let mut exe_ctx = MockExecutionContext::test_default();

        let ndvi_id = add_ndvi_dataset(&mut exe_ctx);
        let ndvi_downscaled_3x_id = add_ndvi_downscaled_3x_dataset(&mut exe_ctx);

        let id: DataId = DatasetId::new().into();
        let name = NamedData::with_system_name("ne_10m_ports");

        exe_ctx.add_meta_data::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>(
            id.clone(),
            name.clone(),
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!(
                        "vector/data/ne_10m_ports/with_spatial_index/ne_10m_ports.gpkg"
                    )
                    .into(),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: None,
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                    cache_ttl: CacheTtlSeconds::default(),
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: Default::default(),
                    time: None,
                    bbox: Some(BoundingBox2D::new_unchecked(
                        [-171.75795, -54.809444].into(),
                        [179.309364, 78.226111].into(),
                    )),
                },
                phantom: Default::default(),
            }),
        );

        let ogr_source = OgrSource {
            params: OgrSourceParameters {
                data: name,
                attribute_projection: None,
                attribute_filters: None,
            },
        }
        .boxed();

        let gdal_source = GdalSource {
            params: GdalSourceParameters {
                data: ndvi_id.clone(),
                overview_level: None,
            },
        }
        .boxed();

        let gdal_source2 = GdalSource {
            params: GdalSourceParameters {
                data: ndvi_downscaled_3x_id.clone(),
                overview_level: None,
            },
        }
        .boxed();

        let raster_vector_join = RasterVectorJoin {
            params: RasterVectorJoinParams {
                names: ColumnNames::Default,
                feature_aggregation: FeatureAggregationMethod::First,
                feature_aggregation_ignore_no_data: false,
                temporal_aggregation: TemporalAggregationMethod::None,
                temporal_aggregation_ignore_no_data: false,
            },
            sources: SingleVectorMultipleRasterSources {
                vector: ogr_source,
                rasters: vec![gdal_source, gdal_source2],
            },
        }
        .boxed();

        let raster_vector_join_initialized = raster_vector_join
            .clone()
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let raster_vector_join_optimized = raster_vector_join_initialized
            .optimize(SpatialResolution::new(0.8, 0.8).unwrap())
            .unwrap();

        let json = serde_json::to_value(&raster_vector_join_optimized).unwrap();

        assert_eq!(
            json,
            serde_json::json!({
                "params": {
                    "featureAggregation": "first",
                    "featureAggregationIgnoreNoData": false,
                    "names": {
                        "type": "default"
                    },
                    "temporalAggregation": "none",
                    "temporalAggregationIgnoreNoData": false
                },
                "sources": {
                    "rasters": [
                        {
                            "params": {
                                "data": "ndvi",
                                "overviewLevel": 8
                            },
                            "type": "GdalSource"
                        },
                        {
                            "params": {
                                "data": "ndvi_downscaled_3x",
                                "overviewLevel": 2
                            },
                            "type": "GdalSource"
                        }
                    ],
                    "vector": {
                        "params": {
                            "attributeFilters": null,
                            "attributeProjection": null,
                            "data": "ne_10m_ports"
                        },
                        "type": "OgrSource"
                    }
                },
                "type": "RasterVectorJoin"
            })
        );

        assert!(raster_vector_join_optimized
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .is_ok());

        // check case where output is finer than input
        let raster_vector_join_optimized = raster_vector_join_initialized
            .optimize(SpatialResolution::new(0.05, 0.05).unwrap())
            .unwrap();

        let json = serde_json::to_value(&raster_vector_join_optimized).unwrap();

        assert_eq!(
            json,
            serde_json::json!({
                "params": {
                    "featureAggregation": "first",
                    "featureAggregationIgnoreNoData": false,
                    "names": {
                        "type": "default"
                    },
                    "temporalAggregation": "none",
                    "temporalAggregationIgnoreNoData": false
                },
                "sources": {
                    "rasters": [
                        {
                            "params": {
                                "data": "ndvi",
                                "overviewLevel": 0
                            },
                            "type": "GdalSource"
                        },
                        {
                            "params": {
                                "data": "ndvi_downscaled_3x",
                                "overviewLevel": 0
                            },
                            "type": "GdalSource"
                        }
                    ],
                    "vector": {
                        "params": {
                            "attributeFilters": null,
                            "attributeProjection": null,
                            "data": "ne_10m_ports"
                        },
                        "type": "OgrSource"
                    }
                },
                "type": "RasterVectorJoin"
            })
        );
    }

    #[tokio::test]
    async fn it_optimizes_complex_workflow() {
        let mut exe_ctx = MockExecutionContext::test_default();

        let ndvi_id = add_ndvi_dataset(&mut exe_ctx);
        let ndvi_downscaled_3x_id = add_ndvi_downscaled_3x_dataset(&mut exe_ctx);

        let id: DataId = DatasetId::new().into();
        let ports_name = NamedData::with_system_name("ne_10m_ports");

        exe_ctx.add_meta_data::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>(
            id.clone(),
            ports_name.clone(),
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!(
                        "vector/data/ne_10m_ports/with_spatial_index/ne_10m_ports.gpkg"
                    )
                    .into(),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: None,
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                    cache_ttl: CacheTtlSeconds::default(),
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: Default::default(),
                    time: None,
                    bbox: Some(BoundingBox2D::new_unchecked(
                        [-171.75795, -54.809444].into(),
                        [179.309364, 78.226111].into(),
                    )),
                },
                phantom: Default::default(),
            }),
        );

        let workflow = Expression {
            params: ExpressionParams {
                expression: "A + B".to_string(),
                output_type: RasterDataType::F64,
                output_band: None,
                map_no_data: false,
            },
            sources: SingleRasterSource {
                raster: RasterStacker {
                    params: RasterStackerParams {
                        rename_bands: RenameBands::Default,
                    },
                    sources: MultipleRasterSources {
                        rasters: vec![
                            RasterTypeConversion {
                                params: RasterTypeConversionParams {
                                    output_data_type: RasterDataType::F64,
                                },
                                sources: SingleRasterSource {
                                    raster: GdalSource {
                                        params: GdalSourceParameters {
                                            data: ndvi_downscaled_3x_id.clone(),
                                            overview_level: None,
                                        },
                                    }
                                    .boxed(),
                                },
                            }
                            .boxed(),
                            Rasterization {
                                params: RasterizationParams {
                                    spatial_resolution: SpatialResolution::new_unchecked(0.3, 0.3),
                                    origin_coordinate: (0., 0.).into(),
                                },
                                sources: SingleVectorSource {
                                    vector: ColumnRangeFilter {
                                        params: ColumnRangeFilterParams {
                                            column: "natlscale".to_string(),
                                            ranges: vec![(1..=2).into()],
                                            keep_nulls: false,
                                        },
                                        sources: SingleVectorSource {
                                            vector: RasterVectorJoin {
                                                params: RasterVectorJoinParams {
                                                    names: ColumnNames::Default,
                                                    feature_aggregation:
                                                        FeatureAggregationMethod::First,
                                                    feature_aggregation_ignore_no_data: false,
                                                    temporal_aggregation:
                                                        TemporalAggregationMethod::None,
                                                    temporal_aggregation_ignore_no_data: false,
                                                },
                                                sources: SingleVectorMultipleRasterSources {
                                                    rasters: vec![Downsampling {
                                                params: DownsamplingParams {
                                                    sampling_method:
                                                        DownsamplingMethod::NearestNeighbor,
                                                    output_resolution:
                                                        DownsamplingResolution::Resolution(
                                                            SpatialResolution::new_unchecked(
                                                                0.3, 0.3,
                                                            ),
                                                        ),
                                                    output_origin_reference: None,
                                                },
                                                sources: SingleRasterSource {
                                                    raster: GdalSource {
                                                        params: GdalSourceParameters {
                                                            data: ndvi_id.clone(),
                                                            overview_level: None,
                                                        },
                                                    }
                                                    .boxed(),
                                                },
                                            }
                                            .boxed()],
                                                    vector: OgrSource {
                                                        params: OgrSourceParameters {
                                                            data: ports_name,
                                                            attribute_projection: None,
                                                            attribute_filters: None,
                                                        },
                                                    }
                                                    .boxed(),
                                                },
                                            }
                                            .boxed(),
                                        },
                                    }
                                    .boxed(),
                                },
                            }
                            .boxed(),
                        ],
                    },
                }
                .boxed(),
            },
        }
        .boxed();

        let workflow_initialized = workflow
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        assert_eq!(
            workflow_initialized
                .result_descriptor()
                .spatial_grid
                .spatial_resolution(),
            SpatialResolution::new(0.3, 0.3).unwrap()
        );

        let workflow_optimized = workflow_initialized
            .optimize(SpatialResolution::new(0.6, 0.6).unwrap())
            .unwrap();

        let json = serde_json::to_value(&workflow_optimized).unwrap();

        assert_eq!(
            json,
            serde_json::json!({
                "params": {
                    "expression": "A + B",
                    "mapNoData": false,
                    "outputBand": {
                        "measurement": {
                            "type": "unitless"
                        },
                        "name": "expression"
                    },
                    "outputType": "F64"
                },
                "sources": {
                    "raster": {
                        "params": {
                            "renameBands": {
                                "type": "default"
                            }
                        },
                        "sources": {
                            "rasters": [
                                {
                                    "params": {
                                        "outputDataType": "F64"
                                    },
                                    "sources": {
                                        "raster": {
                                            "params": {
                                                "data": "ndvi_downscaled_3x",
                                                "overviewLevel": 2
                                            },
                                            "type": "GdalSource"
                                        }
                                    },
                                    "type": "RasterTypeConversion"
                                },
                                {
                                    "params": {
                                        "originCoordinate": {
                                            "x": 0.0,
                                            "y": 0.0
                                        },
                                        "spatialResolution": {
                                            "x": 0.6,
                                            "y": 0.6
                                        }
                                    },
                                    "sources": {
                                        "vector": {
                                            "params": {
                                                "column": "natlscale",
                                                "keepNulls": false,
                                                "ranges": [
                                                    [
                                                        1,
                                                        2
                                                    ]
                                                ]
                                            },
                                            "sources": {
                                                "vector": {
                                                    "params": {
                                                        "featureAggregation": "first",
                                                        "featureAggregationIgnoreNoData": false,
                                                        "names": {
                                                            "type": "default"
                                                        },
                                                        "temporalAggregation": "none",
                                                        "temporalAggregationIgnoreNoData": false
                                                    },
                                                    "sources": {
                                                        "rasters": [
                                                            {
                                                                "params": {
                                                                    "outputOriginReference": null,
                                                                    "outputResolution": {
                                                                        "type": "resolution",
                                                                        "x": 0.6,
                                                                        "y": 0.6
                                                                    },
                                                                    "samplingMethod": "nearestNeighbor"
                                                                },
                                                                "sources": {
                                                                    "raster": {
                                                                        "params": {
                                                                            "data": "ndvi",
                                                                            "overviewLevel": 4
                                                                        },
                                                                        "type": "GdalSource"
                                                                    }
                                                                },
                                                                "type": "Downsampling"
                                                            }
                                                        ],
                                                        "vector": {
                                                            "params": {
                                                                "attributeFilters": null,
                                                                "attributeProjection": null,
                                                                "data": "ne_10m_ports"
                                                            },
                                                            "type": "OgrSource"
                                                        }
                                                    },
                                                    "type": "RasterVectorJoin"
                                                }
                                            },
                                            "type": "ColumnRangeFilter"
                                        }
                                    },
                                    "type": "Rasterization"
                                }
                            ]
                        },
                        "type": "RasterStacker"
                    }
                },
                "type": "Expression"
            })
        );

        let gdal_optimized_initialized = workflow_optimized
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        assert_eq!(
            gdal_optimized_initialized
                .result_descriptor()
                .spatial_grid
                .spatial_resolution(),
            SpatialResolution::new(0.6, 0.6).unwrap()
        );
    }
}
