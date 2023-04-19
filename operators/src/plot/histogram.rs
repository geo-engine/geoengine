use crate::engine::{
    ExecutionContext, InitializedPlotOperator, InitializedRasterOperator,
    InitializedVectorOperator, Operator, OperatorName, PlotOperator, PlotQueryProcessor,
    PlotResultDescriptor, QueryContext, SingleRasterOrVectorSource, TypedPlotQueryProcessor,
    TypedRasterQueryProcessor, TypedVectorQueryProcessor,
};
use crate::engine::{QueryProcessor, WorkflowOperatorPath};
use crate::error;
use crate::error::Error;
use crate::string_token;
use crate::util::input::RasterOrVectorOperator;
use crate::util::Result;
use async_trait::async_trait;
use float_cmp::approx_eq;
use futures::stream::BoxStream;
use futures::{StreamExt, TryFutureExt};
use geoengine_datatypes::plots::{Plot, PlotData};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BoundingBox2D, DataRef, FeatureDataRef, FeatureDataType, Geometry,
    Measurement, VectorQueryRectangle,
};
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use geoengine_datatypes::{
    collections::{FeatureCollection, FeatureCollectionInfos},
    raster::GridSize,
};
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt};
use std::convert::TryFrom;

pub const HISTOGRAM_OPERATOR_NAME: &str = "Histogram";

/// A histogram plot about either a raster or a vector input.
///
/// For vector inputs, it calculates the histogram on one of its attributes.
///
pub type Histogram = Operator<HistogramParams, SingleRasterOrVectorSource>;

impl OperatorName for Histogram {
    const TYPE_NAME: &'static str = "Histogram";
}

/// The parameter spec for `Histogram`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistogramParams {
    /// Name of the (numeric) attribute to compute the histogram on. Ignored for operation on rasters.
    pub column_name: Option<String>,
    /// The bounds (min/max) of the histogram.
    pub bounds: HistogramBounds,
    /// Specify the number of buckets or how it should be derived.
    pub buckets: HistogramBuckets,
    /// Whether to create an interactive output (`false` by default)
    #[serde(default)]
    pub interactive: bool,
}

/// Options for how to derive the histogram's number of buckets.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum HistogramBuckets {
    #[serde(rename_all = "camelCase")]
    Number { value: u8 },
    #[serde(rename_all = "camelCase")]
    SquareRootChoiceRule {
        #[serde(default = "default_max_number_of_buckets")]
        max_number_of_buckets: u8,
    },
}

fn default_max_number_of_buckets() -> u8 {
    100
}

string_token!(Data, "data");

/// Let the bounds either be computed or given.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HistogramBounds {
    Data(Data),
    Values { min: f64, max: f64 },
    // TODO: use bounds in measurement if they are available
}

#[typetag::serde]
#[async_trait]
impl PlotOperator for Histogram {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedPlotOperator>> {
        Ok(match self.sources.source {
            RasterOrVectorOperator::Raster(raster_source) => {
                ensure!(
                    self.params.column_name.is_none(),
                    error::InvalidOperatorSpec {
                        reason: "Histogram on raster input must not have `columnName` field set"
                            .to_string(),
                    }
                );

                let raster_source = raster_source
                    .initialize(path.clone_and_append(0), context)
                    .await?;

                let in_desc = raster_source.result_descriptor();
                InitializedHistogram::new(
                    PlotResultDescriptor {
                        spatial_reference: in_desc.spatial_reference,
                        time: in_desc.time,
                        // converting `SpatialPartition2D` to `BoundingBox2D` is ok here, because is makes the covered area only larger
                        bbox: in_desc
                            .bbox
                            .and_then(|p| BoundingBox2D::new(p.lower_left(), p.upper_right()).ok()),
                    },
                    self.params,
                    raster_source,
                )
                .boxed()
            }
            RasterOrVectorOperator::Vector(vector_source) => {
                let column_name =
                    self.params
                        .column_name
                        .as_ref()
                        .context(error::InvalidOperatorSpec {
                            reason: "Histogram on vector input is missing `columnName` field"
                                .to_string(),
                        })?;

                let vector_source = vector_source
                    .initialize(path.clone_and_append(0), context)
                    .await?;

                match vector_source
                    .result_descriptor()
                    .column_data_type(column_name)
                {
                    None => {
                        return Err(Error::ColumnDoesNotExist {
                            column: column_name.to_string(),
                        });
                    }
                    Some(FeatureDataType::Category | FeatureDataType::Text) => {
                        // TODO: incorporate category data
                        return Err(Error::InvalidOperatorSpec {
                            reason: format!("column `{column_name}` must be numerical"),
                        });
                    }
                    Some(
                        FeatureDataType::Int
                        | FeatureDataType::Float
                        | FeatureDataType::Bool
                        | FeatureDataType::DateTime,
                    ) => {
                        // okay
                    }
                }

                let in_desc = vector_source.result_descriptor().clone();

                InitializedHistogram::new(in_desc.into(), self.params, vector_source).boxed()
            }
        })
    }

    span_fn!(Histogram);
}

/// The initialization of `Histogram`
pub struct InitializedHistogram<Op> {
    result_descriptor: PlotResultDescriptor,
    metadata: HistogramMetadataOptions,
    source: Op,
    interactive: bool,
    column_name: Option<String>,
}

impl<Op> InitializedHistogram<Op> {
    pub fn new(
        result_descriptor: PlotResultDescriptor,
        params: HistogramParams,
        source: Op,
    ) -> Self {
        let (min, max) = if let HistogramBounds::Values { min, max } = params.bounds {
            (Some(min), Some(max))
        } else {
            (None, None)
        };

        let (number_of_buckets, max_number_of_buckets) = match params.buckets {
            HistogramBuckets::Number {
                value: number_of_buckets,
            } => (Some(number_of_buckets as usize), None),
            HistogramBuckets::SquareRootChoiceRule {
                max_number_of_buckets,
            } => (None, Some(max_number_of_buckets as usize)),
        };

        Self {
            result_descriptor,
            metadata: HistogramMetadataOptions {
                number_of_buckets,
                max_number_of_buckets,
                min,
                max,
            },
            source,
            interactive: params.interactive,
            column_name: params.column_name,
        }
    }
}

impl InitializedPlotOperator for InitializedHistogram<Box<dyn InitializedRasterOperator>> {
    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        let processor = HistogramRasterQueryProcessor {
            input: self.source.query_processor()?,
            measurement: self.source.result_descriptor().measurement.clone(),
            metadata: self.metadata,
            interactive: self.interactive,
        };

        Ok(TypedPlotQueryProcessor::JsonVega(processor.boxed()))
    }

    fn result_descriptor(&self) -> &PlotResultDescriptor {
        &self.result_descriptor
    }
}

impl InitializedPlotOperator for InitializedHistogram<Box<dyn InitializedVectorOperator>> {
    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        let processor = HistogramVectorQueryProcessor {
            input: self.source.query_processor()?,
            column_name: self.column_name.clone().unwrap_or_default(),
            measurement: self
                .source
                .result_descriptor()
                .column_measurement(self.column_name.as_deref().unwrap_or_default())
                .cloned()
                .into(),
            metadata: self.metadata,
            interactive: self.interactive,
        };

        Ok(TypedPlotQueryProcessor::JsonVega(processor.boxed()))
    }

    fn result_descriptor(&self) -> &PlotResultDescriptor {
        &self.result_descriptor
    }
}

/// A query processor that calculates the Histogram about its raster inputs.
pub struct HistogramRasterQueryProcessor {
    input: TypedRasterQueryProcessor,
    measurement: Measurement,
    metadata: HistogramMetadataOptions,
    interactive: bool,
}

/// A query processor that calculates the Histogram about its vector inputs.
pub struct HistogramVectorQueryProcessor {
    input: TypedVectorQueryProcessor,
    column_name: String,
    measurement: Measurement,
    metadata: HistogramMetadataOptions,
    interactive: bool,
}

#[async_trait]
impl PlotQueryProcessor for HistogramRasterQueryProcessor {
    type OutputFormat = PlotData;

    fn plot_type(&self) -> &'static str {
        HISTOGRAM_OPERATOR_NAME
    }

    async fn plot_query<'p>(
        &'p self,
        query: VectorQueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<Self::OutputFormat> {
        self.preprocess(query, ctx)
            .and_then(move |mut histogram_metadata| async move {
                histogram_metadata.sanitize();
                if histogram_metadata.has_invalid_parameters() {
                    // early return of empty histogram
                    return self.empty_histogram();
                }

                self.process(histogram_metadata, query, ctx).await
            })
            .await
    }
}

#[async_trait]
impl PlotQueryProcessor for HistogramVectorQueryProcessor {
    type OutputFormat = PlotData;

    fn plot_type(&self) -> &'static str {
        HISTOGRAM_OPERATOR_NAME
    }

    async fn plot_query<'p>(
        &'p self,
        query: VectorQueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<Self::OutputFormat> {
        self.preprocess(query, ctx)
            .and_then(move |mut histogram_metadata| async move {
                histogram_metadata.sanitize();
                if histogram_metadata.has_invalid_parameters() {
                    // early return of empty histogram
                    return self.empty_histogram();
                }

                self.process(histogram_metadata, query, ctx).await
            })
            .await
    }
}

impl HistogramRasterQueryProcessor {
    async fn preprocess<'p>(
        &'p self,
        query: VectorQueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<HistogramMetadata> {
        async fn process_metadata<T: Pixel>(
            mut input: BoxStream<'_, Result<RasterTile2D<T>>>,
            metadata: HistogramMetadataOptions,
        ) -> Result<HistogramMetadata> {
            let mut computed_metadata = HistogramMetadataInProgress::default();

            while let Some(tile) = input.next().await {
                match tile?.grid_array {
                    geoengine_datatypes::raster::GridOrEmpty::Grid(g) => {
                        computed_metadata.add_raster_batch(g.masked_element_deref_iterator());
                    }
                    geoengine_datatypes::raster::GridOrEmpty::Empty(_) => {} // TODO: find out if we really do nothing for empty tiles?
                }
            }

            Ok(metadata.merge_with(computed_metadata.into()))
        }

        if let Ok(metadata) = HistogramMetadata::try_from(self.metadata) {
            return Ok(metadata);
        }

        // TODO: compute only number of buckets if possible

        call_on_generic_raster_processor!(&self.input, processor => {
            process_metadata(processor.query(query.into(), ctx).await?, self.metadata).await
        })
    }

    async fn process<'p>(
        &'p self,
        metadata: HistogramMetadata,
        query: VectorQueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<<HistogramRasterQueryProcessor as PlotQueryProcessor>::OutputFormat> {
        let mut histogram = geoengine_datatypes::plots::Histogram::builder(
            metadata.number_of_buckets,
            metadata.min,
            metadata.max,
            self.measurement.clone(),
        )
        .build()
        .map_err(Error::from)?;

        call_on_generic_raster_processor!(&self.input, processor => {
            let mut query = processor.query(query.into(), ctx).await?;

            while let Some(tile) = query.next().await {


                match tile?.grid_array {
                    geoengine_datatypes::raster::GridOrEmpty::Grid(g) => histogram.add_raster_data(g.masked_element_deref_iterator()),
                    geoengine_datatypes::raster::GridOrEmpty::Empty(n) => histogram.add_nodata_batch(n.number_of_elements() as u64) // TODO: why u64?
                }
            }
        });

        let chart = histogram.to_vega_embeddable(self.interactive)?;

        Ok(chart)
    }

    fn empty_histogram(
        &self,
    ) -> Result<<HistogramRasterQueryProcessor as PlotQueryProcessor>::OutputFormat> {
        let histogram =
            geoengine_datatypes::plots::Histogram::builder(1, 0., 0., self.measurement.clone())
                .build()
                .map_err(Error::from)?;

        let chart = histogram.to_vega_embeddable(self.interactive)?;

        Ok(chart)
    }
}

impl HistogramVectorQueryProcessor {
    async fn preprocess<'p>(
        &'p self,
        query: VectorQueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<HistogramMetadata> {
        async fn process_metadata<'m, G>(
            mut input: BoxStream<'m, Result<FeatureCollection<G>>>,
            column_name: &'m str,
            metadata: HistogramMetadataOptions,
        ) -> Result<HistogramMetadata>
        where
            G: Geometry + 'static,
            FeatureCollection<G>: FeatureCollectionInfos,
        {
            let mut computed_metadata = HistogramMetadataInProgress::default();

            while let Some(collection) = input.next().await {
                let collection = collection?;

                let feature_data = collection.data(column_name).expect("check in param");
                computed_metadata.add_vector_batch(feature_data);
            }

            Ok(metadata.merge_with(computed_metadata.into()))
        }

        if let Ok(metadata) = HistogramMetadata::try_from(self.metadata) {
            return Ok(metadata);
        }

        // TODO: compute only number of buckets if possible

        call_on_generic_vector_processor!(&self.input, processor => {
            process_metadata(processor.query(query, ctx).await?, &self.column_name, self.metadata).await
        })
    }

    async fn process<'p>(
        &'p self,
        metadata: HistogramMetadata,
        query: VectorQueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<<HistogramRasterQueryProcessor as PlotQueryProcessor>::OutputFormat> {
        let mut histogram = geoengine_datatypes::plots::Histogram::builder(
            metadata.number_of_buckets,
            metadata.min,
            metadata.max,
            self.measurement.clone(),
        )
        .build()
        .map_err(Error::from)?;

        call_on_generic_vector_processor!(&self.input, processor => {
            let mut query = processor.query(query, ctx).await?;

            while let Some(collection) = query.next().await {
                let collection = collection?;

                let feature_data = collection.data(&self.column_name).expect("checked in param");

                histogram.add_feature_data(feature_data)?;
            }
        });

        let chart = histogram.to_vega_embeddable(self.interactive)?;

        Ok(chart)
    }

    fn empty_histogram(
        &self,
    ) -> Result<<HistogramRasterQueryProcessor as PlotQueryProcessor>::OutputFormat> {
        let histogram =
            geoengine_datatypes::plots::Histogram::builder(1, 0., 0., self.measurement.clone())
                .build()
                .map_err(Error::from)?;

        let chart = histogram.to_vega_embeddable(self.interactive)?;

        Ok(chart)
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
struct HistogramMetadata {
    pub number_of_buckets: usize,
    pub min: f64,
    pub max: f64,
}

impl HistogramMetadata {
    /// Fix invalid configurations if they are fixeable
    fn sanitize(&mut self) {
        // prevent the rare case that min=max and you have more than one bucket
        if approx_eq!(f64, self.min, self.max) && self.number_of_buckets > 1 {
            self.number_of_buckets = 1;
        }
    }

    fn has_invalid_parameters(&self) -> bool {
        self.number_of_buckets == 0 || self.min > self.max
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
struct HistogramMetadataOptions {
    pub number_of_buckets: Option<usize>,
    pub max_number_of_buckets: Option<usize>,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

impl TryFrom<HistogramMetadataOptions> for HistogramMetadata {
    type Error = ();

    fn try_from(options: HistogramMetadataOptions) -> Result<Self, Self::Error> {
        match (options.number_of_buckets, options.min, options.max) {
            (Some(number_of_buckets), Some(min), Some(max)) => Ok(Self {
                number_of_buckets,
                min,
                max,
            }),
            _ => Err(()),
        }
    }
}

impl HistogramMetadataOptions {
    fn merge_with(self, metadata: HistogramMetadata) -> HistogramMetadata {
        let number_of_buckets = if let Some(number_of_buckets) = self.number_of_buckets {
            number_of_buckets
        } else if let Some(max_number_of_buckets) = self.max_number_of_buckets {
            metadata.number_of_buckets.min(max_number_of_buckets)
        } else {
            metadata.number_of_buckets
        };

        HistogramMetadata {
            number_of_buckets,
            min: self.min.unwrap_or(metadata.min),
            max: self.max.unwrap_or(metadata.max),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
struct HistogramMetadataInProgress {
    pub n: usize,
    pub min: f64,
    pub max: f64,
}

impl Default for HistogramMetadataInProgress {
    fn default() -> Self {
        Self {
            n: 0,
            min: f64::MAX,
            max: f64::MIN,
        }
    }
}

impl HistogramMetadataInProgress {
    #[inline]
    fn add_raster_batch<T: Pixel, I: Iterator<Item = Option<T>>>(&mut self, values: I) {
        values.for_each(|pixel_option| {
            if let Some(p) = pixel_option {
                self.n += 1;
                self.update_minmax(p.as_());
            }
        });
    }

    #[inline]
    fn add_vector_batch(&mut self, values: FeatureDataRef) {
        fn add_data_ref<'d, D, T>(metadata: &mut HistogramMetadataInProgress, data_ref: &'d D)
        where
            D: DataRef<'d, T>,
            T: 'static,
        {
            for v in data_ref.float_options_iter().flatten() {
                metadata.n += 1;
                metadata.update_minmax(v);
            }
        }

        match values {
            FeatureDataRef::Int(values) => {
                add_data_ref(self, &values);
            }
            FeatureDataRef::Float(values) => {
                add_data_ref(self, &values);
            }
            FeatureDataRef::Bool(values) => {
                add_data_ref(self, &values);
            }
            FeatureDataRef::DateTime(values) => {
                add_data_ref(self, &values);
            }
            FeatureDataRef::Category(_) | FeatureDataRef::Text(_) => {
                // do nothing since we don't support them
                // TODO: fill with live once we support category and text types
            }
        }
    }

    #[inline]
    fn update_minmax(&mut self, value: f64) {
        self.min = f64::min(self.min, value);
        self.max = f64::max(self.max, value);
    }
}

impl From<HistogramMetadataInProgress> for HistogramMetadata {
    fn from(metadata: HistogramMetadataInProgress) -> Self {
        Self {
            number_of_buckets: f64::sqrt(metadata.n as f64) as usize,
            min: metadata.min,
            max: metadata.max,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::{
        ChunkByteSize, MockExecutionContext, MockQueryContext, RasterOperator,
        RasterResultDescriptor, StaticMetaData, VectorColumnInfo, VectorOperator,
        VectorResultDescriptor,
    };
    use crate::mock::{MockFeatureCollectionSource, MockRasterSource, MockRasterSourceParams};
    use crate::source::{
        OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType, OgrSourceErrorSpec,
    };
    use crate::test_data;
    use geoengine_datatypes::dataset::{DataId, DatasetId};
    use geoengine_datatypes::primitives::{
        BoundingBox2D, DateTime, FeatureData, NoGeometry, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::raster::{
        EmptyGrid2D, Grid2D, RasterDataType, RasterTile2D, TileInformation, TilingSpecification,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_datatypes::util::Identifier;
    use geoengine_datatypes::{
        collections::{DataCollection, VectorDataType},
        primitives::MultiPoint,
    };
    use serde_json::json;

    #[test]
    fn serialization() {
        let histogram = Histogram {
            params: HistogramParams {
                column_name: Some("foobar".to_string()),
                bounds: HistogramBounds::Values {
                    min: 5.0,
                    max: 10.0,
                },
                buckets: HistogramBuckets::Number { value: 15 },
                interactive: false,
            },
            sources: MockFeatureCollectionSource::<MultiPoint>::multiple(vec![])
                .boxed()
                .into(),
        };

        let serialized = json!({
            "type": "Histogram",
            "params": {
                "columnName": "foobar",
                "bounds": {
                    "min": 5.0,
                    "max": 10.0,
                },
                "buckets": {
                    "type": "number",
                    "value": 15,
                },
                "interactivity": false,
            },
            "sources": {
                "source": {
                    "type": "MockFeatureCollectionSourceMultiPoint",
                    "params": {
                        "collections": [],
                        "spatialReference": "EPSG:4326",
                        "measurements": {},
                    }
                }
            }
        })
        .to_string();

        let deserialized: Histogram = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.params, histogram.params);
    }

    #[test]
    fn serialization_alt() {
        let histogram = Histogram {
            params: HistogramParams {
                column_name: None,
                bounds: HistogramBounds::Data(Default::default()),
                buckets: HistogramBuckets::SquareRootChoiceRule {
                    max_number_of_buckets: 100,
                },
                interactive: false,
            },
            sources: MockFeatureCollectionSource::<MultiPoint>::multiple(vec![])
                .boxed()
                .into(),
        };

        let serialized = json!({
            "type": "Histogram",
            "params": {
                "bounds": "data",
                "buckets": {
                    "type": "squareRootChoiceRule",
                    "maxNumberOfBuckets": 100,
                },
            },
            "sources": {
                "source": {
                    "type": "MockFeatureCollectionSourceMultiPoint",
                    "params": {
                        "collections": [],
                        "spatialReference": "EPSG:4326",
                        "measurements": {},
                    }
                }
            }
        })
        .to_string();

        let deserialized: Histogram = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.params, histogram.params);
    }

    #[tokio::test]
    async fn column_name_for_raster_source() {
        let histogram = Histogram {
            params: HistogramParams {
                column_name: Some("foo".to_string()),
                bounds: HistogramBounds::Values { min: 0.0, max: 8.0 },
                buckets: HistogramBuckets::Number { value: 3 },
                interactive: false,
            },
            sources: mock_raster_source().into(),
        };

        let execution_context = MockExecutionContext::test_default();

        assert!(histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .is_err());
    }

    fn mock_raster_source() -> Box<dyn RasterOperator> {
        MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![RasterTile2D::new_with_tile_info(
                    TimeInterval::default(),
                    TileInformation {
                        global_geo_transform: TestDefault::test_default(),
                        global_tile_position: [0, 0].into(),
                        tile_size_in_pixels: [3, 2].into(),
                    },
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                        .unwrap()
                        .into(),
                )],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
            },
        }
        .boxed()
    }

    #[tokio::test]
    async fn simple_raster() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };
        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let histogram = Histogram {
            params: HistogramParams {
                column_name: None,
                bounds: HistogramBounds::Values { min: 0.0, max: 8.0 },
                buckets: HistogramBuckets::Number { value: 3 },
                interactive: false,
            },
            sources: mock_raster_source().into(),
        };

        let query_processor = histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((0., -3.).into(), (2., 0.).into()).unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            geoengine_datatypes::plots::Histogram::builder(3, 0., 8., Measurement::Unitless)
                .counts(vec![2, 3, 1])
                .build()
                .unwrap()
                .to_vega_embeddable(false)
                .unwrap()
        );
    }

    #[tokio::test]
    async fn simple_raster_without_spec() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };
        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let histogram = Histogram {
            params: HistogramParams {
                column_name: None,
                bounds: HistogramBounds::Data(Default::default()),
                buckets: HistogramBuckets::SquareRootChoiceRule {
                    max_number_of_buckets: 100,
                },
                interactive: false,
            },
            sources: mock_raster_source().into(),
        };

        let query_processor = histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((0., -3.).into(), (2., 0.).into()).unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            geoengine_datatypes::plots::Histogram::builder(2, 1., 6., Measurement::Unitless)
                .counts(vec![3, 3])
                .build()
                .unwrap()
                .to_vega_embeddable(false)
                .unwrap()
        );
    }

    #[tokio::test]
    async fn vector_data() {
        let vector_source = MockFeatureCollectionSource::multiple(vec![
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 8],
                &[("foo", FeatureData::Int(vec![1, 1, 2, 2, 3, 3, 4, 4]))],
            )
            .unwrap(),
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 4],
                &[("foo", FeatureData::Int(vec![5, 6, 7, 8]))],
            )
            .unwrap(),
        ])
        .boxed();

        let histogram = Histogram {
            params: HistogramParams {
                column_name: Some("foo".to_string()),
                bounds: HistogramBounds::Values { min: 0.0, max: 8.0 },
                buckets: HistogramBuckets::Number { value: 3 },
                interactive: true,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::test_default();

        let query_processor = histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            geoengine_datatypes::plots::Histogram::builder(3, 0., 8., Measurement::Unitless)
                .counts(vec![4, 5, 3])
                .build()
                .unwrap()
                .to_vega_embeddable(true)
                .unwrap()
        );
    }

    #[tokio::test]
    async fn vector_data_with_nulls() {
        let vector_source = MockFeatureCollectionSource::single(
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 6],
                &[(
                    "foo",
                    FeatureData::NullableFloat(vec![
                        Some(1.),
                        Some(2.),
                        None,
                        Some(4.),
                        None,
                        Some(5.),
                    ]),
                )],
            )
            .unwrap(),
        )
        .boxed();

        let histogram = Histogram {
            params: HistogramParams {
                column_name: Some("foo".to_string()),
                bounds: HistogramBounds::Data(Default::default()),
                buckets: HistogramBuckets::SquareRootChoiceRule {
                    max_number_of_buckets: 100,
                },
                interactive: false,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::test_default();

        let query_processor = histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            geoengine_datatypes::plots::Histogram::builder(2, 1., 5., Measurement::Unitless)
                .counts(vec![2, 2])
                .build()
                .unwrap()
                .to_vega_embeddable(false)
                .unwrap()
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn text_attribute() {
        let dataset_id = DatasetId::new();

        let workflow = serde_json::json!({
            "type": "Histogram",
            "params": {
                "columnName": "featurecla",
                "bounds": "data",
                "buckets": {
                    "type": "squareRootChoiceRule",
                    "maxNumberOfBuckets": 100,
                }
            },
            "sources": {
                "source": {
                    "type": "OgrSource",
                    "params": {
                        "data": {
                            "type": "internal",
                            "datasetId": dataset_id
                        },
                        "attributeProjection": null
                    },
                }
            }
        });
        let histogram: Histogram = serde_json::from_value(workflow).unwrap();

        let mut execution_context = MockExecutionContext::test_default();
        execution_context.add_meta_data::<_, _, VectorQueryRectangle>(
            DataId::Internal { dataset_id },
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/ne_10m_ports/ne_10m_ports.shp").into(),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: String::new(),
                        y: None,
                        int: vec!["natlscale".to_string()],
                        float: vec!["scalerank".to_string()],
                        text: vec![
                            "featurecla".to_string(),
                            "name".to_string(),
                            "website".to_string(),
                        ],
                        bool: vec![],
                        datetime: vec![],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        (
                            "natlscale".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Float,
                                measurement: Measurement::Unitless,
                            },
                        ),
                        (
                            "scalerank".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Int,
                                measurement: Measurement::Unitless,
                            },
                        ),
                        (
                            "featurecla".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless,
                            },
                        ),
                        (
                            "name".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless,
                            },
                        ),
                        (
                            "website".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless,
                            },
                        ),
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default(),
            }),
        );

        if let Err(Error::InvalidOperatorSpec { reason }) = histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
        {
            assert_eq!(reason, "column `featurecla` must be numerical");
        } else {
            panic!("we currently don't support text features, but this went through");
        }
    }

    #[tokio::test]
    async fn no_data_raster() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };
        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);
        let histogram = Histogram {
            params: HistogramParams {
                column_name: None,
                bounds: HistogramBounds::Data(Data::default()),
                buckets: HistogramBuckets::SquareRootChoiceRule {
                    max_number_of_buckets: 100,
                },
                interactive: false,
            },
            sources: MockRasterSource {
                params: MockRasterSourceParams {
                    data: vec![RasterTile2D::new_with_tile_info(
                        TimeInterval::default(),
                        TileInformation {
                            global_geo_transform: TestDefault::test_default(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels,
                        },
                        EmptyGrid2D::<u8>::new(tile_size_in_pixels).into(),
                    )],
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        measurement: Measurement::Unitless,
                        time: None,
                        bbox: None,
                        resolution: None,
                    },
                },
            }
            .boxed()
            .into(),
        };

        let query_processor = histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((0., -3.).into(), (2., 0.).into()).unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            geoengine_datatypes::plots::Histogram::builder(1, 0., 0., Measurement::Unitless)
                .build()
                .unwrap()
                .to_vega_embeddable(false)
                .unwrap()
        );
    }

    #[tokio::test]
    async fn empty_feature_collection() {
        let vector_source = MockFeatureCollectionSource::single(
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[] as &[TimeInterval],
                &[("foo", FeatureData::Float(vec![]))],
            )
            .unwrap(),
        )
        .boxed();

        let histogram = Histogram {
            params: HistogramParams {
                column_name: Some("foo".to_string()),
                bounds: HistogramBounds::Data(Default::default()),
                buckets: HistogramBuckets::SquareRootChoiceRule {
                    max_number_of_buckets: 100,
                },
                interactive: false,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::test_default();

        let query_processor = histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            geoengine_datatypes::plots::Histogram::builder(1, 0., 0., Measurement::Unitless)
                .build()
                .unwrap()
                .to_vega_embeddable(false)
                .unwrap()
        );
    }

    #[tokio::test]
    async fn feature_collection_with_one_feature() {
        let vector_source = MockFeatureCollectionSource::with_collections_and_measurements(
            vec![DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default()],
                &[("foo", FeatureData::Float(vec![5.0]))],
            )
            .unwrap()],
            [(
                "foo".to_string(),
                Measurement::continuous("bar".to_string(), None),
            )]
            .into_iter()
            .collect(),
        )
        .boxed();

        let histogram = Histogram {
            params: HistogramParams {
                column_name: Some("foo".to_string()),
                bounds: HistogramBounds::Data(Default::default()),
                buckets: HistogramBuckets::SquareRootChoiceRule {
                    max_number_of_buckets: 100,
                },
                interactive: false,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::test_default();

        let query_processor = histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            geoengine_datatypes::plots::Histogram::builder(
                1,
                5.,
                5.,
                Measurement::continuous("bar".to_string(), None)
            )
            .counts(vec![1])
            .build()
            .unwrap()
            .to_vega_embeddable(false)
            .unwrap()
        );
    }

    #[tokio::test]
    async fn single_value_raster_stream() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };
        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);
        let histogram = Histogram {
            params: HistogramParams {
                column_name: None,
                bounds: HistogramBounds::Data(Data::default()),
                buckets: HistogramBuckets::SquareRootChoiceRule {
                    max_number_of_buckets: 100,
                },
                interactive: false,
            },
            sources: MockRasterSource {
                params: MockRasterSourceParams {
                    data: vec![RasterTile2D::new_with_tile_info(
                        TimeInterval::default(),
                        TileInformation {
                            global_geo_transform: TestDefault::test_default(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels,
                        },
                        Grid2D::new(tile_size_in_pixels, vec![4; 6]).unwrap().into(),
                    )],
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        measurement: Measurement::Unitless,
                        time: None,
                        bbox: None,
                        resolution: None,
                    },
                },
            }
            .boxed()
            .into(),
        };

        let query_processor = histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((0., -3.).into(), (2., 0.).into()).unwrap(),
                    time_interval: TimeInterval::new_instant(DateTime::new_utc(
                        2013, 12, 1, 12, 0, 0,
                    ))
                    .unwrap(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            geoengine_datatypes::plots::Histogram::builder(1, 4., 4., Measurement::Unitless)
                .counts(vec![6])
                .build()
                .unwrap()
                .to_vega_embeddable(false)
                .unwrap()
        );
    }
}
