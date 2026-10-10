use std::collections::HashMap;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use geoengine_datatypes::collections::{MultiPolygonCollection, VectorDataType};
use geoengine_datatypes::machine_learning::MlModelName;
use geoengine_datatypes::primitives::{
    BandSelection, BoundingBox2D, CacheHint, ColumnSelection, FeatureData, FeatureDataType,
    Measurement, MultiPolygon, MultiPolygonAccess, RasterQueryRectangle, SpatialResolution,
    TimeInterval, VectorQueryRectangle,
};
use geoengine_datatypes::raster::{
    GridIdx2D, GridIndexAccess, GridSize, RasterDataType, TileOverlap,
};
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
        let source_tile_shape = in_descriptor.spatial_grid.tile_size.grid_shape();
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

        let source_overlap = in_descriptor.spatial_grid.tile_overlap();

        Ok(Box::new(InitializedOnnxObjectDetection {
            name,
            path,
            params,
            result_descriptor,
            source,
            model_loading_info,
            source_overlap,
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
    /// Halo the source tiles carry. The model input is core-sized, so it has to
    /// be read from the core region of each tile, not from its data corner.
    source_overlap: TileOverlap,
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
                self.source_overlap,
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
    source_overlap: TileOverlap,
}

impl OnnxObjectDetectionProcessor {
    fn new(
        source: BoxRasterQueryProcessor<f32>,
        result_descriptor: VectorResultDescriptor,
        model_loading_info: MlModelLoadingInfo,
        params: OnnxObjectDetectionParams,
        source_overlap: TileOverlap,
    ) -> Self {
        Self {
            source,
            result_descriptor,
            model_loading_info,
            params,
            source_overlap,
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

        let mut records: Vec<(MultiPolygon, u8, f64, TimeInterval)> = Vec::new();
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
            let tile_time = reference_tile.time;

            // `.chunks(num_bands)` assumes the stream groups the bands of one
            // tile consecutively. A source that interleaves differently would be
            // mis-packed, so assert the grouping before it silently corrupts data.
            debug_assert!(tiles.iter().enumerate().all(|(i, tile)| tile.tile_position
                == reference_tile.tile_position
                && tile.time == tile_time
                && tile.band == i as u32));
            let mut packed: Vec<Vec<f32>> = vec![vec![0.0; num_bands]; in_width * in_height];
            // The model input is core-sized. A tile that carries a halo stores
            // `overlap + core + overlap` pixels with the data corner at [0,0],
            // so the core window starts one halo in. Same offset as `onnx.rs`.
            let src_off_y = self.source_overlap.axis_size_y() as isize;
            let src_off_x = self.source_overlap.axis_size_x() as isize;
            for (band_idx, tile) in tiles.iter().enumerate() {
                if tile.is_empty() {
                    continue;
                }
                for y in 0..in_height {
                    for x in 0..in_width {
                        let pixel = tile
                            .get_at_grid_index_unchecked(GridIdx2D::from([
                                y as isize + src_off_y,
                                x as isize + src_off_x,
                            ]))
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

            // Tile-local transform: same pixel size, origin moved to the tile's
            // data upper-left corner. Detection coordinates are fractional pixels
            // relative to that corner, so they are scaled rather than indexed.
            // The model input is read from the core window, so the boxes are
            // anchored at the core's upper-left, not the tile's data corner.
            let tile_gt = reference_tile
                .global_geo_transform
                .shift_by_pixel_offset(reference_tile.global_core_upper_left_pixel_idx());
            let origin = tile_gt.origin_coordinate();
            for &i in &kept {
                let det = &filtered[i];
                let (x1, y1) = (
                    origin.x + f64::from(det.x1) * tile_gt.x_pixel_size(),
                    origin.y + f64::from(det.y1) * tile_gt.y_pixel_size(),
                );
                let (x2, y2) = (
                    origin.x + f64::from(det.x2) * tile_gt.x_pixel_size(),
                    origin.y + f64::from(det.y2) * tile_gt.y_pixel_size(),
                );
                let ring = vec![
                    (x1, y1).into(),
                    (x2, y1).into(),
                    (x2, y2).into(),
                    (x1, y2).into(),
                    (x1, y1).into(),
                ];
                let geometry = MultiPolygon::new(vec![vec![ring]])?;
                records.push((
                    geometry,
                    det.class_id as u8,
                    f64::from(det.score),
                    tile_time,
                ));
            }
        }

        // An object inside an overlap halo is detected once per tile, so
        // deduplicate across tiles here. Per-tile `nms` above cannot see this.
        let records = deduplicate_across_tiles(&records, iou_threshold);

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

/// Axis-aligned bounds of a detection's exterior ring, in the CRS of the data.
///
/// Kept in `f64`: CRS coordinates reach ~2e7 (Web Mercator), where `f32`
/// resolves only ~1 m and would merge or split boxes that are metres apart.
fn ring_bounds(geometry: &MultiPolygon) -> Option<(f64, f64, f64, f64)> {
    let ring = geometry.polygons().first()?.first()?;
    Some(ring.iter().fold(
        (
            f64::INFINITY,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NEG_INFINITY,
        ),
        |(x1, y1, x2, y2), c| (x1.min(c.x), y1.min(c.y), x2.max(c.x), y2.max(c.y)),
    ))
}

/// Intersection over union of two `f64` boxes.
fn bbox_iou_f64(a: (f64, f64, f64, f64), b: (f64, f64, f64, f64)) -> f64 {
    let (ax1, ay1, ax2, ay2) = a;
    let (bx1, by1, bx2, by2) = b;
    let inter_w = (ax2.min(bx2) - ax1.max(bx1)).max(0.0);
    let inter_h = (ay2.min(by2) - ay1.max(by1)).max(0.0);
    let inter = inter_w * inter_h;
    let area_a = (ax2 - ax1).max(0.0) * (ay2 - ay1).max(0.0);
    let area_b = (bx2 - bx1).max(0.0) * (by2 - by1).max(0.0);
    let union = area_a + area_b - inter;
    if union > 0.0 { inter / union } else { 0.0 }
}

/// Drop detections that repeat an already-kept detection of the same class.
///
/// This is `nms` again, but across the detections of all tiles rather than
/// within one tile: with an overlap halo the same object is detected in every
/// tile that sees it. Highest score wins, matching per-tile `nms`.
fn deduplicate_across_tiles(
    records: &[(MultiPolygon, u8, f64, TimeInterval)],
    iou_threshold: f32,
) -> Vec<(MultiPolygon, u8, f64, TimeInterval)> {
    let mut with_bounds: Vec<&(MultiPolygon, u8, f64, TimeInterval)> = records.iter().collect();
    with_bounds.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

    let mut kept: Vec<&(MultiPolygon, u8, f64, TimeInterval)> = Vec::new();
    let mut kept_bounds: Vec<(u8, (f64, f64, f64, f64))> = Vec::new();
    for record in with_bounds {
        let Some(bounds) = ring_bounds(&record.0) else {
            kept.push(record);
            continue;
        };
        let duplicate = kept_bounds.iter().any(|(class, kept_bounds)| {
            *class == record.1 && bbox_iou_f64(*kept_bounds, bounds) > f64::from(iou_threshold)
        });
        if !duplicate {
            kept_bounds.push((record.1, bounds));
            kept.push(record);
        }
    }

    kept.into_iter().cloned().collect()
}

fn build_collection(
    records: &[(MultiPolygon, u8, f64, TimeInterval)],
) -> Result<MultiPolygonCollection> {
    let geometries = records
        .iter()
        .map(|(geometry, ..)| geometry.clone())
        .collect();
    let time_intervals = records
        .iter()
        .map(|(_, _, _, time)| *time)
        .collect::<Vec<_>>();
    let classes = records
        .iter()
        .map(|(_, class_id, ..)| *class_id)
        .collect::<Vec<_>>();
    let scores = records
        .iter()
        .map(|(_, _, score, _)| *score)
        .collect::<Vec<_>>();

    let mut data = HashMap::new();
    data.insert("class".to_string(), FeatureData::Category(classes));
    data.insert("score".to_string(), FeatureData::Float(scores));

    MultiPolygonCollection::from_data(geometries, time_intervals, data, CacheHint::no_cache())
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
        Grid, GridBoundingBox2D, GridIdx2D, RasterDataType, RasterTile2D, TileIdx, TileOverlap,
        TileSize,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::test_data;
    use geoengine_datatypes::util::test::TestDefault;

    use super::{DetectionLayout, MultiPolygon, OnnxObjectDetection, OnnxObjectDetectionParams};

    fn box_record(
        x1: f64,
        y1: f64,
        x2: f64,
        y2: f64,
        class_id: u8,
        score: f64,
    ) -> (MultiPolygon, u8, f64, TimeInterval) {
        let ring = vec![
            (x1, y1).into(),
            (x2, y1).into(),
            (x2, y2).into(),
            (x1, y2).into(),
            (x1, y1).into(),
        ];
        (
            MultiPolygon::new(vec![vec![ring]]).unwrap(),
            class_id,
            score,
            TimeInterval::new_unchecked(0, 5),
        )
    }

    #[test]
    fn dedup_collapses_the_same_object_from_two_overlapping_tiles() {
        // Same object detected in two tiles that both see it through their halo.
        let records = vec![
            box_record(10.0, 10.0, 20.0, 20.0, 0, 0.9),
            box_record(10.2, 10.1, 20.2, 20.1, 0, 0.8),
        ];

        let deduped = super::deduplicate_across_tiles(&records, 0.5);

        assert_eq!(deduped.len(), 1, "duplicate across tiles must collapse");
        assert!((deduped[0].2 - 0.9).abs() < 1e-9, "highest score must win");
    }

    #[test]
    fn dedup_keeps_different_classes_and_disjoint_objects() {
        let records = vec![
            box_record(0.0, 0.0, 10.0, 10.0, 0, 0.9),
            box_record(0.0, 0.0, 10.0, 10.0, 1, 0.8),
            box_record(100.0, 100.0, 110.0, 110.0, 0, 0.7),
        ];

        let deduped = super::deduplicate_across_tiles(&records, 0.5);

        assert_eq!(
            deduped.len(),
            3,
            "distinct classes and disjoint boxes are kept"
        );
    }

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
        detect_objects(TileOverlap::zero()).await;
    }

    /// Boxes are anchored at the tile core, so a source halo must not move them.
    ///
    /// The test model emits constant output regardless of its input, so this pins
    /// the georeference of the boxes, not which window of the tile was analysed.
    /// The core-window read itself is asserted separately in `tile_read_window_is_the_core`.
    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn it_detects_objects_with_a_source_halo_at_the_same_world_coords() {
        detect_objects(TileOverlap::new(2, 2)).await;
    }

    #[test]
    fn tile_read_window_is_the_core() {
        // A halo-carrying tile stores `overlap + core + overlap` pixels with the
        // data corner at [0,0]; the model input is core-sized and must therefore
        // be read starting one halo in. This is the offset the query loop applies.
        let core = TileSize::new_y_x(4, 4);
        let overlap = TileOverlap::new(2, 2);
        let stored_y = core.axis_size_y() + 2 * overlap.axis_size_y();
        let stored_x = core.axis_size_x() + 2 * overlap.axis_size_x();

        let first_read = GridIdx2D::from([0isize, 0isize])
            + GridIdx2D::from([
                overlap.axis_size_y() as isize,
                overlap.axis_size_x() as isize,
            ]);
        let last_read = GridIdx2D::from([
            core.axis_size_y() as isize - 1,
            core.axis_size_x() as isize - 1,
        ]) + first_read;

        assert_eq!(stored_y, 8);
        assert_eq!(stored_x, 8);
        // first read is inside the stored grid, last read is exactly its last pixel
        assert!(
            last_read.inner()[0] < stored_y as isize && last_read.inner()[1] < stored_x as isize
        );
        // core spans 4 pixels from offset 2, so it ends at index 5, leaving the
        // trailing 2 halo rows of the 8x8 stored grid unread.
        assert_eq!(last_read, GridIdx2D::from([5, 5]));
        assert_eq!(first_read, GridIdx2D::from([2, 2]));
    }

    #[allow(clippy::too_many_lines)]
    async fn detect_objects(source_overlap: TileOverlap) {
        // A single 4x4-core single-band f32 source tile at global position [0,0].
        // Pixel values are irrelevant: the test model emits a constant output,
        // so the assertions pin the *window* and its georeference, not the data.
        // A tile carrying a halo stores overlap + core + overlap pixels.
        let oy = source_overlap.axis_size_y();
        let ox = source_overlap.axis_size_x();
        let stored = 4 + 2 * oy.max(ox);
        let data: Vec<RasterTile2D<f32>> = vec![RasterTile2D {
            time: TimeInterval::new_unchecked(0, 5),
            tile_position: TileIdx::new_y_x(0, 0),
            band: 0,
            global_geo_transform: TestDefault::test_default(),
            grid_array: Grid::new([stored, stored].into(), vec![1.0f32; stored * stored])
                .unwrap()
                .into(),
            properties: Default::default(),
            cache_hint: CacheHint::no_cache(),
            overlap: source_overlap,
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
                        TileSize::new_y_x(4, 4),
                    )
                    .with_tile_overlap(source_overlap),
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
        exe_ctx.tiling_specification.tile_size = TileSize::new_y_x(4, 4);
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
