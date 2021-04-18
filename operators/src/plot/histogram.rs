use crate::engine::{
    ExecutionContext, InitializedOperator, InitializedOperatorImpl, InitializedPlotOperator,
    Operator, PlotOperator, PlotQueryProcessor, PlotResultDescriptor, QueryContext, QueryProcessor,
    QueryRectangle, TypedPlotQueryProcessor, TypedRasterQueryProcessor, TypedVectorQueryProcessor,
};
use crate::error;
use crate::error::Error;
use crate::string_token;
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryFutureExt};
use geoengine_datatypes::collections::{FeatureCollection, FeatureCollectionInfos};
use geoengine_datatypes::plots::{Plot, PlotData};
use geoengine_datatypes::primitives::{
    DataRef, FeatureDataRef, FeatureDataType, Geometry, Measurement,
};
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt};
use std::convert::TryFrom;

pub const HISTOGRAM_OPERATOR_NAME: &str = "Histogram";

/// A histogram plot about either a raster or a vector input.
///
/// For vector inputs, it calculates the histogram on one of its attributes.
///
pub type Histogram = Operator<HistogramParams>;

/// The parameter spec for `Histogram`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistogramParams {
    /// Name of the (numeric) attribute to compute the histogram on. Ignored for operation on rasters.
    pub column_name: Option<String>,
    /// The bounds (min/max) of the histogram.
    pub bounds: HistogramBounds,
    /// If the number of buckets is undefined, it is derived from the square-root choice rule.
    pub buckets: Option<usize>,
    /// Whether to create an interactive output (`false` by default)
    #[serde(default)]
    pub interactive: bool,
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
impl PlotOperator for Histogram {
    fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<InitializedPlotOperator>> {
        ensure!(
            self.vector_sources.len() + self.raster_sources.len() == 1,
            error::InvalidNumberOfInputs {
                expected: 1..2,
                found: self.vector_sources.len() + self.raster_sources.len()
            }
        );
        ensure!(
            self.raster_sources.is_empty() || self.params.column_name.is_none(),
            error::InvalidOperatorSpec {
                reason: "`column_name` must not be specified on raster input".to_string(),
            }
        );

        let vector_sources = self
            .vector_sources
            .into_iter()
            .map(|o| o.initialize(context))
            .collect::<Result<Vec<_>>>()?;
        if !vector_sources.is_empty() {
            let column_name =
                self.params
                    .column_name
                    .as_ref()
                    .context(error::InvalidOperatorSpec {
                        reason: "Histogram on vector input is missing `column_name` field"
                            .to_string(),
                    })?;

            let vector_result_descriptor = vector_sources[0].result_descriptor();

            match vector_result_descriptor.columns.get(column_name) {
                None => {
                    return Err(Error::ColumnDoesNotExist {
                        column: column_name.to_string(),
                    });
                }
                Some(FeatureDataType::Categorical | FeatureDataType::Text) => {
                    // TODO: incorporate categorical data
                    return Err(Error::InvalidOperatorSpec {
                        reason: format!("column `{}` must be numerical", column_name),
                    });
                }
                Some(FeatureDataType::Decimal | FeatureDataType::Number) => {
                    // okay
                }
            }
        }

        Ok(InitializedHistogram {
            result_descriptor: PlotResultDescriptor {},
            raster_sources: self
                .raster_sources
                .into_iter()
                .map(|o| o.initialize(context))
                .collect::<Result<Vec<_>>>()?,
            vector_sources,
            state: self.params,
        }
        .boxed())
    }
}

/// The initialization of `Histogram`
pub type InitializedHistogram = InitializedOperatorImpl<PlotResultDescriptor, HistogramParams>;

impl InitializedOperator<PlotResultDescriptor, TypedPlotQueryProcessor> for InitializedHistogram {
    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        let (min, max) = if let HistogramBounds::Values { min, max } = self.state.bounds {
            (Some(min), Some(max))
        } else {
            (None, None)
        };
        let metadata = HistogramMetadataOptions {
            number_of_buckets: self.state.buckets,
            min,
            max,
        };

        if self.vector_sources.is_empty() {
            let raster_source = &self.raster_sources[0];

            Ok(TypedPlotQueryProcessor::JsonVega(
                HistogramRasterQueryProcessor {
                    input: raster_source.query_processor()?,
                    measurement: raster_source.result_descriptor().measurement.clone(),
                    metadata,
                    interactive: self.state.interactive,
                }
                .boxed(),
            ))
        } else {
            let vector_source = &self.vector_sources[0];

            Ok(TypedPlotQueryProcessor::JsonVega(
                HistogramVectorQueryProcessor {
                    input: vector_source.query_processor()?,
                    column_name: self.state.column_name.clone().expect("checked in param"),
                    measurement: Measurement::Unitless, // TODO: incorporate measurement once it is there
                    metadata,
                    interactive: self.state.interactive,
                }
                .boxed(),
            ))
        }
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
        query: QueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<Self::OutputFormat> {
        self.preprocess(query, ctx)
            .and_then(move |histogram_metadata| async move {
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
        query: QueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<Self::OutputFormat> {
        self.preprocess(query, ctx)
            .and_then(move |histogram_metadata| async move {
                self.process(histogram_metadata, query, ctx).await
            })
            .await
    }
}

impl HistogramRasterQueryProcessor {
    async fn preprocess<'p>(
        &'p self,
        query: QueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<HistogramMetadata> {
        async fn process_metadata<T: Pixel>(
            mut input: BoxStream<'_, Result<RasterTile2D<T>>>,
            metadata: HistogramMetadataOptions,
        ) -> Result<HistogramMetadata> {
            let mut computed_metadata = HistogramMetadataInProgress::default();

            while let Some(tile) = input.next().await {
                let tile = tile?;

                computed_metadata
                    .add_raster_batch(&tile.grid_array.data, tile.grid_array.no_data_value);
            }

            Ok(metadata.merge_with(computed_metadata.into()))
        }

        if let Ok(metadata) = HistogramMetadata::try_from(self.metadata) {
            return Ok(metadata);
        }

        // TODO: compute only number of buckets if possible

        call_on_generic_raster_processor!(&self.input, processor => {
            process_metadata(processor.query(query, ctx)?, self.metadata).await
        })
    }

    async fn process<'p>(
        &'p self,
        metadata: HistogramMetadata,
        query: QueryRectangle,
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
            let mut query = processor.query(query, ctx)?;

            while let Some(tile) = query.next().await {
                let tile = tile?;

                histogram.add_raster_data(&tile.grid_array.data, tile.grid_array.no_data_value);
            }
        });

        let chart = histogram.to_vega_embeddable(self.interactive)?;

        Ok(chart)
    }
}

impl HistogramVectorQueryProcessor {
    async fn preprocess<'p>(
        &'p self,
        query: QueryRectangle,
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
            process_metadata(processor.query(query, ctx)?, &self.column_name, self.metadata).await
        })
    }

    async fn process<'p>(
        &'p self,
        metadata: HistogramMetadata,
        query: QueryRectangle,
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
            let mut query = processor.query(query, ctx)?;

            while let Some(collection) = query.next().await {
                let collection = collection?;

                let feature_data = collection.data(&self.column_name).expect("checked in param");

                histogram.add_feature_data(feature_data)?;
            }
        });

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

#[derive(Debug, Copy, Clone, PartialEq)]
struct HistogramMetadataOptions {
    pub number_of_buckets: Option<usize>,
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
        HistogramMetadata {
            number_of_buckets: self.number_of_buckets.unwrap_or(metadata.number_of_buckets),
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
    fn add_raster_batch<T: Pixel>(&mut self, values: &[T], no_data: Option<T>) {
        if let Some(no_data) = no_data {
            for &v in values {
                if v == no_data {
                    continue;
                }

                self.n += 1;
                self.update_minmax(v.as_());
            }
        } else {
            self.n += values.len();
            for v in values {
                self.update_minmax(v.as_());
            }
        }
    }

    #[inline]
    fn add_vector_batch(&mut self, values: FeatureDataRef) {
        fn add_data_ref<'d, D, T>(metadata: &mut HistogramMetadataInProgress, data_ref: &D)
        where
            D: DataRef<'d, T>,
            T: Pixel,
        {
            if data_ref.has_nulls() {
                for (i, v) in data_ref.as_ref().iter().enumerate() {
                    if data_ref.is_null(i) {
                        continue;
                    }

                    metadata.n += 1;
                    metadata.update_minmax(v.as_());
                }
            } else {
                let values = data_ref.as_ref();
                metadata.n += values.len();
                for v in values {
                    metadata.update_minmax(v.as_());
                }
            }
        }

        match values {
            FeatureDataRef::Decimal(values) => {
                add_data_ref(self, &values);
            }
            FeatureDataRef::Number(values) => {
                add_data_ref(self, &values);
            }
            FeatureDataRef::Categorical(_) | FeatureDataRef::Text(_) => {
                // do nothing since we don't support them
                // TODO: fill with live once we support categorical and textual types
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
        MockExecutionContext, MockQueryContext, RasterOperator, RasterResultDescriptor,
        StaticMetaData, VectorOperator, VectorResultDescriptor,
    };
    use crate::mock::{MockFeatureCollectionSource, MockRasterSource, MockRasterSourceParams};
    use crate::source::{
        OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType, OgrSourceErrorSpec,
    };
    use geoengine_datatypes::collections::{DataCollection, VectorDataType};
    use geoengine_datatypes::dataset::{DatasetId, InternalDatasetId};
    use geoengine_datatypes::primitives::{
        BoundingBox2D, FeatureData, NoGeometry, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::raster::{Grid2D, RasterDataType, RasterTile2D, TileInformation};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::Identifier;
    use num_traits::AsPrimitive;
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
                buckets: Some(15),
                interactive: false,
            },
            raster_sources: vec![],
            vector_sources: vec![],
        };

        let serialized = json!({
            "type": "Histogram",
            "params": {
                "column_name": "foobar",
                "bounds": {
                    "min": 5.0,
                    "max": 10.0,
                },
                "buckets": 15,
                "interactivity": false,
            },
            "raster_sources": [],
            "vector_sources": [],
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
                buckets: None,
                interactive: false,
            },
            raster_sources: vec![],
            vector_sources: vec![],
        };

        let serialized = json!({
            "type": "Histogram",
            "params": {
                "bounds": "data",
            },
            "raster_sources": [],
            "vector_sources": [],
        })
        .to_string();

        let deserialized: Histogram = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.params, histogram.params);
    }

    #[test]
    fn column_name_for_raster_source() {
        let histogram = Histogram {
            params: HistogramParams {
                column_name: Some("foo".to_string()),
                bounds: HistogramBounds::Values { min: 0.0, max: 8.0 },
                buckets: Some(3),
                interactive: false,
            },
            raster_sources: vec![mock_raster_source()],
            vector_sources: vec![],
        };

        let execution_context = MockExecutionContext::default();

        assert!(histogram.boxed().initialize(&execution_context).is_err());
    }

    fn mock_raster_source() -> Box<dyn RasterOperator> {
        let no_data_value = None;
        MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![RasterTile2D::new_with_tile_info(
                    TimeInterval::default(),
                    TileInformation {
                        global_geo_transform: Default::default(),
                        global_tile_position: [0, 0].into(),
                        tile_size_in_pixels: [3, 2].into(),
                    },
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], no_data_value).unwrap(),
                )],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed()
    }

    #[tokio::test]
    async fn simple_raster() {
        let histogram = Histogram {
            params: HistogramParams {
                column_name: None,
                bounds: HistogramBounds::Values { min: 0.0, max: 8.0 },
                buckets: Some(3),
                interactive: false,
            },
            raster_sources: vec![mock_raster_source()],
            vector_sources: vec![],
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = histogram
            .boxed()
            .initialize(&execution_context)
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                QueryRectangle {
                    bbox: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(0),
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
        let histogram = Histogram {
            params: HistogramParams {
                column_name: None,
                bounds: HistogramBounds::Data(Default::default()),
                buckets: None,
                interactive: false,
            },
            raster_sources: vec![mock_raster_source()],
            vector_sources: vec![],
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = histogram
            .boxed()
            .initialize(&execution_context)
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                QueryRectangle {
                    bbox: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(0),
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
                &[("foo", FeatureData::Decimal(vec![1, 1, 2, 2, 3, 3, 4, 4]))],
            )
            .unwrap(),
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 4],
                &[("foo", FeatureData::Decimal(vec![5, 6, 7, 8]))],
            )
            .unwrap(),
        ])
        .boxed();

        let histogram = Histogram {
            params: HistogramParams {
                column_name: Some("foo".to_string()),
                bounds: HistogramBounds::Values { min: 0.0, max: 8.0 },
                buckets: Some(3),
                interactive: true,
            },
            raster_sources: vec![],
            vector_sources: vec![vector_source],
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = histogram
            .boxed()
            .initialize(&execution_context)
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                QueryRectangle {
                    bbox: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(0),
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
                    FeatureData::NullableNumber(vec![
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
                buckets: None,
                interactive: false,
            },
            raster_sources: vec![],
            vector_sources: vec![vector_source],
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = histogram
            .boxed()
            .initialize(&execution_context)
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                QueryRectangle {
                    bbox: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(0),
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
    async fn textual_attribute() {
        let dataset_id = InternalDatasetId::new();

        let workflow = serde_json::json!({
            "type": "Histogram",
            "params": {
                "column_name": "featurecla",
                "bounds": "data"
            },
            "raster_sources": [],
            "vector_sources": [{
                "type": "OgrSource",
                "params": {
                    "dataset": {
                        "Internal": dataset_id
                    },
                    "attribute_projection": null
                }
            }]
        });
        let histogram: Histogram = serde_json::from_value(workflow).unwrap();

        let mut execution_context = MockExecutionContext::default();
        execution_context.add_meta_data(
            DatasetId::Internal(dataset_id),
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: "operators/test-data/vector/data/ne_10m_ports/ne_10m_ports.shp"
                        .into(),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    columns: Some(OgrSourceColumnSpec {
                        x: "".to_string(),
                        y: None,
                        numeric: vec!["natlscale".to_string()],
                        decimal: vec!["scalerank".to_string()],
                        textual: vec![
                            "featurecla".to_string(),
                            "name".to_string(),
                            "website".to_string(),
                        ],
                    }),
                    default_geometry: None,
                    force_ogr_time_filter: false,
                    on_error: OgrSourceErrorSpec::Skip,
                    provenance: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        ("natlscale".to_string(), FeatureDataType::Number),
                        ("scalerank".to_string(), FeatureDataType::Decimal),
                        ("featurecla".to_string(), FeatureDataType::Text),
                        ("name".to_string(), FeatureDataType::Text),
                        ("website".to_string(), FeatureDataType::Text),
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                },
            }),
        );

        if let Err(Error::InvalidOperatorSpec { reason }) =
            histogram.boxed().initialize(&execution_context)
        {
            assert_eq!(reason, "column `featurecla` must be numerical");
        } else {
            panic!("we currently don't support textual features, but this went through");
        }
    }
}
