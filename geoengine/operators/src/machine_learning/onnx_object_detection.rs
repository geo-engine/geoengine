use std::collections::HashMap;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use geoengine_datatypes::collections::{MultiPolygonCollection, VectorDataType};
use geoengine_datatypes::machine_learning::MlModelName;
use geoengine_datatypes::primitives::{
    BandSelection, BoundingBox2D, CacheHint, ColumnSelection, FeatureData, FeatureDataType,
    Measurement, MultiPolygon, RasterQueryRectangle, SpatialResolution, TimeInterval,
    VectorQueryRectangle,
};
use geoengine_datatypes::raster::{GridIdx2D, GridIndexAccess, GridSize, RasterDataType};
use ndarray::Array4;
use ort::value::TensorRef;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};

use crate::engine::{
    BoxRasterQueryProcessor, CanonicOperatorName, ExecutionContext, InitializedRasterOperator,
    InitializedSources, InitializedVectorOperator, Operator, OperatorName, QueryContext,
    QueryProcessor, RasterQueryProcessor, SingleRasterSource, TypedVectorQueryProcessor,
    VectorColumnInfo, VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
    WorkflowOperatorPath,
};
use crate::error;
use crate::machine_learning::{
    MlModelLoadingInfo,
    detection_decoder::{BoxFormat, DetectionDecoder, YoloBoxesDecoder, nms},
    error::{InputResolutionMismatch, InputSizeMismatch, InputTypeMismatch, Ort},
    onnx_util::load_onnx_model_from_loading_info,
};
use crate::optimization::OptimizationError;
use crate::util::Result;

/// How a detection model's raw output tensor should be decoded into boxes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DetectionLayout {
    /// YOLO raw (pre-NMS) detect output, channel-major `[4(+1 obj), C, N]`.
    /// `objectness` selects YOLOv5/v6/v7 (`true`, objectness at channel 4)
    /// vs YOLOv8/v9/v10 (`false`, class scores start at channel 4).
    YoloBoxes { objectness: bool },
    /// Output already carries decoded boxes, scores and classes (e.g. TF object-detection API).
    PreDecoded,
}

/// Parameters of the [`OnnxObjectDetection`] operator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OnnxObjectDetectionParams {
    /// The name of the detection model.
    pub model: MlModelName,
    /// The spatial resolution (in linear units per pixel) the model was trained at.
    pub expected_resolution: f64,
    /// Relative tolerance for the resolution check. A source is accepted when
    /// `|actual - expected| / expected <= resolution_epsilon`.
    pub resolution_epsilon: f64,
    /// How to interpret the model's raw output tensor.
    pub layout: DetectionLayout,
    /// Number of object classes the model can predict.
    pub num_classes: u32,
    /// Confidence threshold applied to decoded detections.
    pub conf_threshold: f32,
    /// `IoU` threshold for non-maximum suppression.
    pub iou_threshold: f32,
    /// Optional human-readable class labels, indexed by class id.
    #[serde(default)]
    pub class_names: Vec<String>,
}

impl OnnxObjectDetectionParams {
    pub fn new(model: MlModelName, expected_resolution: f64) -> Self {
        Self {
            model,
            expected_resolution,
            resolution_epsilon: 0.01,
            layout: DetectionLayout::YoloBoxes { objectness: false },
            num_classes: 1,
            conf_threshold: 0.5,
            iou_threshold: 0.5,
            class_names: Vec::new(),
        }
    }
}

/// Applies an object-detection ONNX model to a raster and emits the detected
/// bounding boxes as `MultiPolygon` rectangle features.
pub type OnnxObjectDetection = Operator<OnnxObjectDetectionParams, SingleRasterSource>;

impl OperatorName for OnnxObjectDetection {
    const TYPE_NAME: &'static str = "OnnxObjectDetection";
}

#[typetag::serde]
#[async_trait]
impl VectorOperator for OnnxObjectDetection {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        let name = CanonicOperatorName::from(&self);
        let params = self.params;

        let source = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?
            .raster;

        let in_descriptor = source.result_descriptor();
        let model_loading_info = context.ml_model_loading_info(&params.model).await?;
        let metadata = &model_loading_info.metadata;

        // The detection model consumes a fixed-size NHWC tensor, so the source
        // tile must match the model's input pixel size exactly (no resampling).
        let source_tile_shape = in_descriptor.spatial_grid.tile_size.0;
        ensure!(
            metadata
                .input_shape
                .yx_matches_tile_shape(&source_tile_shape),
            InputSizeMismatch {
                expected_width: metadata.input_shape.x,
                expected_height: metadata.input_shape.y,
                actual_width: source_tile_shape.axis_size_x() as u32,
                actual_height: source_tile_shape.axis_size_y() as u32,
            }
        );

        // The model was trained at a specific spatial resolution; the source must
        // match it within the configured relative tolerance (no resampling).
        let actual = in_descriptor.spatial_grid.spatial_resolution();
        let expected = params.expected_resolution;
        let epsilon = params.resolution_epsilon;
        let resolution_matches = (actual.x - expected).abs() / expected <= epsilon
            && (actual.y - expected).abs() / expected <= epsilon;
        ensure!(
            resolution_matches,
            InputResolutionMismatch {
                expected,
                actual: actual.x,
                epsilon,
            }
        );

        // Detection models consume float tensors.
        ensure!(
            in_descriptor.data_type == RasterDataType::F32,
            InputTypeMismatch {
                model_input_type: RasterDataType::F32,
                source_type: in_descriptor.data_type,
            }
        );

        let mut columns = HashMap::new();
        columns.insert(
            "class".to_string(),
            VectorColumnInfo {
                data_type: FeatureDataType::Category,
                measurement: Measurement::Unitless,
            },
        );
        columns.insert(
            "score".to_string(),
            VectorColumnInfo {
                data_type: FeatureDataType::Float,
                measurement: Measurement::Unitless,
            },
        );

        let result_descriptor = VectorResultDescriptor {
            data_type: VectorDataType::MultiPolygon,
            spatial_reference: in_descriptor.spatial_reference,
            columns,
            time: None,
            bbox: None,
        };

        Ok(Box::new(InitializedOnnxObjectDetection {
            name,
            path,
            params,
            result_descriptor,
            source,
            model_loading_info,
        }))
    }

    span_fn!(OnnxObjectDetection);
}

pub struct InitializedOnnxObjectDetection {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    params: OnnxObjectDetectionParams,
    result_descriptor: VectorResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
    model_loading_info: MlModelLoadingInfo,
}

impl InitializedVectorOperator for InitializedOnnxObjectDetection {
    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        let source = self.source.query_processor()?;
        let source = source
            .get_f32()
            .expect("source raster type was checked as f32 during initialization");

        Ok(TypedVectorQueryProcessor::MultiPolygon(
            OnnxObjectDetectionProcessor::new(
                source,
                self.result_descriptor.clone(),
                self.model_loading_info.clone(),
                self.params.clone(),
            )
            .boxed(),
        ))
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        OnnxObjectDetection::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }

    fn optimize(
        &self,
        target_resolution: SpatialResolution,
    ) -> Result<Box<dyn VectorOperator>, OptimizationError> {
        Ok(OnnxObjectDetection {
            params: self.params.clone(),
            sources: SingleRasterSource {
                raster: self.source.optimize(target_resolution)?,
            },
        }
        .boxed())
    }
}

pub struct OnnxObjectDetectionProcessor {
    source: BoxRasterQueryProcessor<f32>,
    result_descriptor: VectorResultDescriptor,
    model_loading_info: MlModelLoadingInfo,
    params: OnnxObjectDetectionParams,
}

impl OnnxObjectDetectionProcessor {
    fn new(
        source: BoxRasterQueryProcessor<f32>,
        result_descriptor: VectorResultDescriptor,
        model_loading_info: MlModelLoadingInfo,
        params: OnnxObjectDetectionParams,
    ) -> Self {
        Self {
            source,
            result_descriptor,
            model_loading_info,
            params,
        }
    }
}

#[async_trait]
impl QueryProcessor for OnnxObjectDetectionProcessor {
    type Output = MultiPolygonCollection;
    type SpatialBounds = BoundingBox2D;
    type Selection = ColumnSelection;
    type ResultDescription = VectorResultDescriptor;

    #[allow(clippy::too_many_lines)]
    async fn _query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<MultiPolygonCollection>>> {
        let source_descriptor = self.source.raster_result_descriptor();
        let num_bands = self.model_loading_info.metadata.input_shape.bands as usize;
        let geo_transform = source_descriptor.spatial_grid.geo_transform();

        let raster_query = RasterQueryRectangle::from_bounds_and_geo_transform(
            &query,
            BandSelection::first_n(num_bands as u32),
            geo_transform,
        );

        let mut session = load_onnx_model_from_loading_info(&self.model_loading_info)?;
        let input_name = session.inputs()[0].name().to_string();
        let decoder = decoder_from_params(&self.params);
        let conf_threshold = self.params.conf_threshold;
        let iou_threshold = self.params.iou_threshold;
        let in_height = self.model_loading_info.metadata.input_shape.y as usize;
        let in_width = self.model_loading_info.metadata.input_shape.x as usize;

        let mut records: Vec<(MultiPolygon, u8, f64)> = Vec::new();
        let mut chunked_stream = self
            .source
            .raster_query(raster_query, ctx)
            .await?
            .chunks(num_bands);
        while let Some(chunk) = chunked_stream.next().await {
            let tiles: Vec<_> = chunk.into_iter().collect::<Result<Vec<_>>>()?;
            let Some(reference_tile) = tiles.iter().find(|tile| !tile.is_empty()) else {
                continue;
            };

            let mut packed: Vec<Vec<f32>> = vec![vec![0.0; num_bands]; in_width * in_height];
            for (band_idx, tile) in tiles.iter().enumerate() {
                if tile.is_empty() {
                    continue;
                }
                for y in 0..in_height {
                    for x in 0..in_width {
                        let pixel = tile
                            .get_at_grid_index_unchecked(GridIdx2D::from([y as isize, x as isize]))
                            .unwrap_or(0.0);
                        packed[y * in_width + x][band_idx] = pixel;
                    }
                }
            }

            let pixels = packed.into_iter().flatten().collect::<Vec<f32>>();
            let samples = Array4::from_shape_vec((1, in_height, in_width, num_bands), pixels)
                .expect("packed pixel buffer size matches the model input shape");

            let outputs = session
                .run(ort::inputs![
                    &input_name => TensorRef::from_array_view(&samples).context(Ort)?
                ])
                .context(Ort)
                .map_err(error::Error::from)?;
            let predictions = outputs[0].try_extract_tensor::<f32>().context(Ort)?;
            let (_shape, raw) = predictions.to_owned();
            let output_data = Vec::from(raw);

            let detections = decoder.decode(&output_data);
            let filtered = detections
                .into_iter()
                .filter(|det| det.score >= conf_threshold)
                .collect::<Vec<_>>();
            let kept = nms(&filtered, iou_threshold);

            let base = reference_tile
                .global_geo_transform
                .grid_idx_to_pixel_upper_left_coordinate_2d(
                    reference_tile.global_data_upper_left_pixel_idx(),
                );
            let gt = &reference_tile.global_geo_transform;
            for &i in &kept {
                let det = &filtered[i];
                let (x1, y1) = (
                    base.x + f64::from(det.x1) * gt.x_pixel_size(),
                    base.y + f64::from(det.y1) * gt.y_pixel_size(),
                );
                let (x2, y2) = (
                    base.x + f64::from(det.x2) * gt.x_pixel_size(),
                    base.y + f64::from(det.y2) * gt.y_pixel_size(),
                );
                let ring = vec![
                    (x1, y1).into(),
                    (x2, y1).into(),
                    (x2, y2).into(),
                    (x1, y2).into(),
                    (x1, y1).into(),
                ];
                let geometry = MultiPolygon::new(vec![vec![ring]])?;
                records.push((geometry, det.class_id as u8, f64::from(det.score)));
            }
        }

        let collection = build_collection(&records)?;
        Ok(futures::stream::iter([Ok(collection)]).boxed())
    }

    fn result_descriptor(&self) -> &Self::ResultDescription {
        &self.result_descriptor
    }
}

fn decoder_from_params(params: &OnnxObjectDetectionParams) -> YoloBoxesDecoder {
    let has_objectness = match &params.layout {
        DetectionLayout::YoloBoxes { objectness } => *objectness,
        DetectionLayout::PreDecoded => false,
    };
    YoloBoxesDecoder::new(
        params.num_classes as usize,
        has_objectness,
        BoxFormat::Center,
    )
}

fn build_collection(records: &[(MultiPolygon, u8, f64)]) -> Result<MultiPolygonCollection> {
    let geometries = records
        .iter()
        .map(|(geometry, _, _)| geometry.clone())
        .collect();
    let time_intervals = vec![TimeInterval::default(); records.len()];
    let classes = records
        .iter()
        .map(|(_, class_id, _)| *class_id)
        .collect::<Vec<_>>();
    let scores = records
        .iter()
        .map(|(_, _, score)| *score)
        .collect::<Vec<_>>();

    let mut data = HashMap::new();
    data.insert("class".to_string(), FeatureData::Category(classes));
    data.insert("score".to_string(), FeatureData::Float(scores));

    MultiPolygonCollection::from_data(geometries, time_intervals, data, CacheHint::default())
        .map_err(error::Error::from)
}

#[cfg(test)]
mod tests {
    use approx::assert_abs_diff_eq;
    use futures::StreamExt;

    use crate::engine::{
        MockExecutionContext, RasterBandDescriptors, RasterOperator, RasterResultDescriptor,
        SingleRasterSource, SpatialGridDescriptor, TimeDescriptor, VectorOperator,
        WorkflowOperatorPath,
    };
    use crate::machine_learning::{
        MlModelInputNoDataHandling, MlModelLoadingInfo, MlModelMetadata,
        MlModelOutputNoDataHandling,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use crate::util::Result;

    use geoengine_datatypes::collections::{
        FeatureCollectionInfos, IntoGeometryIterator, MultiPolygonCollection,
    };
    use geoengine_datatypes::machine_learning::{MlModelName, MlTensorShape3D};
    use geoengine_datatypes::primitives::TimeInterval;
    use geoengine_datatypes::primitives::{
        BoundingBox2D, CacheHint, ColumnSelection, Coordinate2D, FeatureDataRef, GeometryRef,
        MultiPolygonAccess, TimeStep, VectorQueryRectangle,
    };
    use geoengine_datatypes::raster::{
        Grid, GridBoundingBox2D, RasterDataType, RasterTile2D, TileOverlap, TileSize,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::test_data;
    use geoengine_datatypes::util::test::TestDefault;

    use super::{DetectionLayout, OnnxObjectDetection, OnnxObjectDetectionParams};

    fn assert_ring(got: &[Coordinate2D], expected: &[(f64, f64)]) {
        assert_eq!(got.len(), expected.len());
        for (a, (ex, ey)) in got.iter().zip(expected) {
            assert_abs_diff_eq!(a.x, *ex, epsilon = 1e-9);
            assert_abs_diff_eq!(a.y, *ey, epsilon = 1e-9);
        }
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn it_detects_objects() {
        // A single 4x4 single-band f32 source tile at global position [0,0].
        // Pixel values are irrelevant: the test model emits a constant output.
        let data: Vec<RasterTile2D<f32>> = vec![RasterTile2D {
            time: TimeInterval::new_unchecked(0, 5),
            tile_position: [0, 0].into(),
            band: 0,
            global_geo_transform: TestDefault::test_default(),
            grid_array: Grid::new([4, 4].into(), vec![1.0f32; 16]).unwrap().into(),
            properties: Default::default(),
            cache_hint: CacheHint::default(),
            overlap: TileOverlap::zero(),
        }];

        let source = MockRasterSource {
            params: MockRasterSourceParams {
                data,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::F32,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: TimeDescriptor::new_regular_with_epoch(
                        None,
                        TimeStep::millis(5).unwrap(),
                    ),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        TestDefault::test_default(),
                        GridBoundingBox2D::new_min_max(0, 3, 0, 3).unwrap(),
                        TileSize::new(4, 4),
                    ),
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let model_name = MlModelName {
            namespace: None,
            name: "test_detection".into(),
        };

        let op = OnnxObjectDetection {
            params: OnnxObjectDetectionParams {
                model: model_name.clone(),
                expected_resolution: 1.0,
                resolution_epsilon: 0.01,
                layout: DetectionLayout::YoloBoxes { objectness: false },
                num_classes: 2,
                conf_threshold: 0.5,
                iou_threshold: 0.5,
                class_names: vec!["a".into(), "b".into()],
            },
            sources: SingleRasterSource { raster: source },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        // Tiles must match the model's 4x4 input (no resampling).
        exe_ctx.tiling_specification.tile_size = TileSize::new(4, 4);
        exe_ctx.ml_models.insert(
            model_name,
            MlModelLoadingInfo {
                storage_path: test_data!("ml/onnx/test_detection.onnx").to_owned(),
                metadata: MlModelMetadata {
                    input_type: RasterDataType::F32,
                    output_type: RasterDataType::F32,
                    input_shape: MlTensorShape3D::new_y_x_bands(4, 4, 1),
                    output_shape: MlTensorShape3D::new_y_x_bands(6, 4, 1),
                    input_no_data_handling: MlModelInputNoDataHandling::SkipIfNoData,
                    output_no_data_handling: MlModelOutputNoDataHandling::NanIsNoData,
                },
            },
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let initialized = op
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();
        let qp = initialized
            .query_processor()
            .unwrap()
            .multi_polygon()
            .unwrap();

        // Full source extent: world (0,-4) to (4,0) for the TestDefault transform.
        let query_rect = VectorQueryRectangle::new(
            BoundingBox2D::new(Coordinate2D::new(0.0, -4.0), Coordinate2D::new(4.0, 0.0)).unwrap(),
            TimeInterval::new_unchecked(0, 5),
            ColumnSelection::all(),
        );

        let collections = qp
            .vector_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<MultiPolygonCollection>>>()
            .unwrap();

        assert_eq!(collections.len(), 1);
        let collection = &collections[0];
        assert_eq!(collection.len(), 2);

        let rings: Vec<Vec<Coordinate2D>> = collection
            .geometries()
            .map(|g| {
                let mp = g.as_geometry();
                mp.polygons()[0][0].clone()
            })
            .collect();

        // Feature 0: class 0, score 0.90 (highest confidence).
        assert_ring(
            &rings[0],
            &[
                (0.5, -0.5),
                (1.5, -0.5),
                (1.5, -1.5),
                (0.5, -1.5),
                (0.5, -0.5),
            ],
        );
        // Feature 1: class 1, score 0.85.
        assert_ring(
            &rings[1],
            &[
                (2.0, -2.0),
                (3.0, -2.0),
                (3.0, -3.0),
                (2.0, -3.0),
                (2.0, -2.0),
            ],
        );

        let classes = match collection.data("class").unwrap() {
            FeatureDataRef::Category(c) => c.as_ref().to_vec(),
            _ => panic!("expected a category column"),
        };
        assert_eq!(classes, vec![0u8, 1]);

        let scores = match collection.data("score").unwrap() {
            FeatureDataRef::Float(f) => f.as_ref().to_vec(),
            _ => panic!("expected a float column"),
        };
        // Scores round-trip through f32 in the model, so compare with a relaxed epsilon.
        assert_abs_diff_eq!(scores[0], 0.9, epsilon = 1e-6);
        assert_abs_diff_eq!(scores[1], 0.85, epsilon = 1e-6);
    }
}
