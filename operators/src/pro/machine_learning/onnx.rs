use std::sync::Arc;

use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedRasterOperator, InitializedSources, Operator,
    OperatorName, QueryContext, RasterBandDescriptors, RasterOperator, RasterQueryProcessor,
    RasterResultDescriptor, ResultDescriptor, SingleRasterSource, TypedRasterQueryProcessor,
    WorkflowOperatorPath,
};
use geoengine_datatypes::error::{BoxedResultExt, ErrorSource};
use ndarray::{arr2, Array2};

use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use geoengine_datatypes::primitives::RasterQueryRectangle;
use geoengine_datatypes::raster::{
    Grid, Grid2D, GridIdx2D, GridIndexAccess, GridOrEmpty2D, GridSize, MapElementsParallel, Pixel,
    RasterDataType, RasterTile2D,
};
use geoengine_datatypes::test_data;
use geoengine_expression::{
    DataType, ExpressionAst, ExpressionParser, LinkedExpression, Parameter,
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tract_onnx::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OnnxParams {
    // TODO: model_id
}

/// This `QueryProcessor` applies a ml model in Onnx format on all bands of its input raster series.
/// For now, the model has to be for a single pixel and multiple bands.
pub type Onnx = Operator<OnnxParams, SingleRasterSource>;

impl OperatorName for Onnx {
    const TYPE_NAME: &'static str = "Onnx";
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for Onnx {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let name = CanonicOperatorName::from(&self);

        let source = self.sources.initialize_sources(path, context).await?.raster;

        let in_descriptor = source.result_descriptor();

        // TODO: check that number of input bands fits number of model features

        // TODO: output descriptor may depend on the ml task, e.g. classification or regression
        let out_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::I64,
            spatial_reference: in_descriptor.spatial_reference,
            time: in_descriptor.time,
            bbox: in_descriptor.bbox,
            resolution: in_descriptor.resolution,
            bands: RasterBandDescriptors::new_single_band(), // TODO: output bands depend on ml task, e.g. classification or regression
        };

        Ok(Box::new(InitializedOnnx {
            name,
            result_descriptor: out_descriptor,
            source,
        }))
    }

    span_fn!(Onnx);
}

pub struct InitializedOnnx {
    name: CanonicOperatorName,
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
}

impl InitializedRasterOperator for InitializedOnnx {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        Ok(TypedRasterQueryProcessor::I64(
            // TODO: output type depends on ml task, e.g. classification or regression
            OnnxProcessor::new(
                self.source.query_processor()?.into_f32(),
                self.result_descriptor.clone(),
            )
            .boxed(),
        ))
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }
}

pub(crate) struct OnnxProcessor {
    source: Box<dyn RasterQueryProcessor<RasterType = f32>>, // as most ml algorithms work on f32 we use this as input type
    result_descriptor: RasterResultDescriptor,
}

impl OnnxProcessor {
    pub fn new(
        source: Box<dyn RasterQueryProcessor<RasterType = f32>>,
        result_descriptor: RasterResultDescriptor,
    ) -> Self {
        Self {
            source,
            result_descriptor,
        }
    }
}

#[async_trait]
impl RasterQueryProcessor for OnnxProcessor {
    type RasterType = i64; // TODO: output type depends on ml task, e.g. classification or regression

    async fn raster_query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<i64>>>> {
        let num_bands = self.source.raster_result_descriptor().bands.count() as usize;

        let mut source_query = query.clone();
        source_query.attributes = (0..num_bands)
            .into_iter()
            .map(|b| b as u32)
            .collect::<Vec<u32>>()
            .try_into()
            .unwrap();

        let session = ort::Session::builder()
            .unwrap()
            .commit_from_file(test_data!("pro/ml/onnx/test.onnx"))
            .unwrap();

        let stream = self
            .source
            .raster_query(source_query, ctx)
            .await?
            .chunks(num_bands) // chunk the tiles to get all bands for a spatial index at once
            .map(move |chunk| {
                if chunk.len() != num_bands {
                    // if there are not exactly N tiles, it should mean the last tile was an error and the chunker ended prematurely
                    if let Some(Err(e)) = chunk.into_iter().last() {
                        return Err(e);
                    }
                    // if there is no error, the source did not produce all bands, which likely means a bug in an operator
                    unreachable!("the source did not produce all bands");
                }

                let tiles = chunk.into_iter().map(Result::unwrap).collect::<Vec<_>>(); // TODO: handle errors

                let first_tile = &tiles[0];
                let time = first_tile.time;
                let tile_position = first_tile.tile_position;
                let global_geo_transform = first_tile.global_geo_transform;
                let cache_hint = first_tile.cache_hint;

                let width = tiles[0].grid_array.axis_size_x();
                let height = tiles[0].grid_array.axis_size_y();

                // TODO: collect into a ndarray directly

                let mut pixels: Vec<Vec<f32>> = vec![vec![0.0; num_bands]; width * height];

                for (tile_index, tile) in tiles.into_iter().enumerate() {
                    for y in 0..height {
                        for x in 0..width {
                            let pixel_index = y * width + x; // Calculate the linear index for the pixel
                            let pixel_value = tile
                                .get_at_grid_index(GridIdx2D::from([y as isize, x as isize]))
                                .unwrap()
                                .unwrap();
                            pixels[pixel_index][tile_index] = pixel_value;
                        }
                    }
                }

                let pixels = pixels.into_iter().flatten().collect::<Vec<f32>>();
                let rows = width * height;
                let cols = num_bands;

                let samples = Array2::from_shape_vec((rows, cols), pixels).unwrap();

                let input_name = &session.inputs[0].name;

                let outputs = session
                    .run(ort::inputs![input_name => samples].unwrap())
                    .unwrap();

                let predictions = outputs["output_label"].try_extract_tensor::<i64>().unwrap();
                let predictions = predictions.into_owned().into_raw_vec();

                Ok(RasterTile2D::new(
                    time,
                    tile_position,
                    0,
                    global_geo_transform,
                    Grid::new([width, height].into(), predictions)?.into(),
                    cache_hint,
                ))
            });

        Ok(stream.boxed())
    }

    fn raster_result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{CacheHint, SpatialPartition2D, SpatialResolution, TimeInterval},
        raster::{GridShape, RenameBands, TilesEqualIgnoringCacheHint},
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };
    use ndarray::{arr2, Array1, Array2};

    use crate::{
        engine::{MockExecutionContext, MockQueryContext, MultipleRasterSources},
        mock::{MockRasterSource, MockRasterSourceParams},
        processing::{RasterStacker, RasterStackerParams},
    };

    use super::*;

    #[test]
    fn tract() {
        // TODO: load during initialization? or at least store it for later reuse
        let model = tract_onnx::onnx()
            // load the model
            .model_for_path(test_data!("pro/ml/onnx/test_no_zipmap.onnx"))
            .unwrap()
            .into_runnable()
            .unwrap();

        let array = arr2(&[[0.1f32, 0.2], [0.2, 0.3], [0.2, 0.2], [0.3, 0.1]]);

        let tensor: Tensor = array.into();

        // run the model on the input

        // TODO: fix the input because the prediction is wrong
        let result = model.run(tvec!(tensor.into())).unwrap();
        dbg!(&result);

        let result_array_view = result[0].to_array_view::<i64>().unwrap();

        dbg!(result_array_view);
    }

    #[test]
    fn ort() {
        let session = ort::Session::builder()
            .unwrap()
            .commit_from_file(test_data!("pro/ml/onnx/test.onnx"))
            .unwrap();

        let input_name = &session.inputs[0].name;

        let new_samples = arr2(&[[0.1f32, 0.2], [0.2, 0.3], [0.2, 0.2], [0.3, 0.1]]);

        let outputs = session
            .run(ort::inputs![input_name => new_samples].unwrap())
            .unwrap();

        let predictions = outputs["output_label"].try_extract_tensor::<i64>().unwrap();

        assert_eq!(
            predictions,
            Array1::from(vec![33i64, 33, 42, 42]).into_dyn()
        );
    }

    #[test]
    fn ort_dynamic() {
        let session = ort::Session::builder()
            .unwrap()
            .commit_from_file(test_data!("pro/ml/onnx/test.onnx"))
            .unwrap();

        let input_name = &session.inputs[0].name;

        let pixels = vec![
            vec![0.1f32, 0.2],
            vec![0.2, 0.3],
            vec![0.2, 0.2],
            vec![0.3, 0.1],
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<f32>>();

        let rows = 4;
        let cols = 2;

        let new_samples = Array2::from_shape_vec((rows, cols), pixels).unwrap();

        let outputs = session
            .run(ort::inputs![input_name => new_samples].unwrap())
            .unwrap();

        let predictions = outputs["output_label"].try_extract_tensor::<i64>().unwrap();

        assert_eq!(
            predictions,
            Array1::from(vec![33i64, 33, 42, 42]).into_dyn()
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_classifies() {
        let data: Vec<RasterTile2D<f32>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.1f32, 0.1, 0.2, 0.2])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.2f32, 0.2, 0.1, 0.1])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let data2: Vec<RasterTile2D<f32>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.2f32, 0.2, 0.1, 0.1])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.1f32, 0.1, 0.2, 0.2])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::F32,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::F32,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let stacker = RasterStacker {
            params: RasterStackerParams {
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![mrs1, mrs2],
            },
        }
        .boxed();

        let onnx = Onnx {
            params: OnnxParams {},
            sources: SingleRasterSource { raster: stacker },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 1.).into(), (3., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 5),
            spatial_resolution: SpatialResolution::one(),
            attributes: [0].try_into().unwrap(),
        };

        let query_ctx = MockQueryContext::test_default();

        let op = onnx
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let qp = op.query_processor().unwrap().get_i64().unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        let expected: Vec<RasterTile2D<i64>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![33i64, 33, 42, 42])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![42i64, 42, 33, 33])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        assert!(expected.tiles_equal_ignoring_cache_hint(&result));
    }
}
