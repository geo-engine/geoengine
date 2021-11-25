use async_trait::async_trait;
use futures::StreamExt;
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};

use geoengine_datatypes::collections::FeatureCollectionInfos;
use geoengine_datatypes::plots::{BoxPlotAttribute, Plot, PlotData};
use geoengine_datatypes::raster::{GridOrEmpty, GridSize, NoDataValue};

use crate::engine::{
    ExecutionContext, InitializedPlotOperator, InitializedRasterOperator,
    InitializedVectorOperator, MultipleRasterOrSingleVectorSource, Operator, PlotOperator,
    PlotQueryProcessor, PlotQueryRectangle, PlotResultDescriptor, QueryContext, QueryProcessor,
    ResultDescriptor, TypedPlotQueryProcessor, TypedRasterQueryProcessor,
    TypedVectorQueryProcessor, VectorQueryRectangle,
};
use crate::error::{self, Error};
use crate::util::input::MultiRasterOrVectorOperator;
use crate::util::statistics::PSquareQuantileEstimator;
use crate::util::Result;
use snafu::ensure;

pub const BOXPLOT_OPERATOR_NAME: &str = "BoxPlot";
const EXACT_CALC_BOUND: usize = 10_000;
const BATCH_SIZE: usize = 1_000;
const MAX_NUMBER_OF_RASTER_INPUTS: usize = 8;

/// A box plot about vector data attribute values
pub type BoxPlot = Operator<BoxPlotParams, MultipleRasterOrSingleVectorSource>;

/// The parameter spec for `BoxPlot`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BoxPlotParams {
    /// Name of the (numeric) attributes to compute the box plots on.
    #[serde(default)]
    pub column_names: Vec<String>,

    /// For rasters, we have the option to include no-data values
    #[serde(default)]
    pub include_no_data: bool,
}

#[typetag::serde]
#[async_trait]
impl PlotOperator for BoxPlot {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedPlotOperator>> {
        match self.sources.source {
            MultiRasterOrVectorOperator::Raster(raster_sources) => {
                ensure!(
                    (1..=MAX_NUMBER_OF_RASTER_INPUTS).contains(&raster_sources.len()),
                    error::InvalidNumberOfRasterInputs {
                        expected: 1..MAX_NUMBER_OF_RASTER_INPUTS,
                        found: raster_sources.len()
                    }
                );
                ensure!( self.params.column_names.is_empty() || self.params.column_names.len() == raster_sources.len(),
                    error::InvalidOperatorSpec {
                        reason: "BoxPlot on raster data must either contain a name/alias for every input ('column_names' parameter) or no names at all."
                            .to_string(),
                });

                let output_names = if self.params.column_names.is_empty() {
                    (1..=raster_sources.len())
                        .map(|i| format!("Raster-{}", i))
                        .collect::<Vec<_>>()
                } else {
                    self.params.column_names.clone()
                };

                let initialized = futures::future::join_all(
                    raster_sources.into_iter().map(|op| op.initialize(context)),
                )
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;

                if initialized.len() > 1 {
                    let srs = initialized[0].result_descriptor().spatial_reference;
                    ensure!(
                        initialized
                            .iter()
                            .all(|op| op.result_descriptor().spatial_reference == srs),
                        error::AllSourcesMustHaveSameSpatialReference
                    );
                }

                Ok(InitializedBoxPlot::new(
                    PlotResultDescriptor {
                        spatial_reference: initialized[0].result_descriptor().spatial_reference,
                    },
                    output_names,
                    self.params.include_no_data,
                    initialized,
                )
                .boxed())
            }
            MultiRasterOrVectorOperator::Vector(vector_source) => {
                ensure!( !self.params.column_names.is_empty(),
                    error::InvalidOperatorSpec {
                        reason: "BoxPlot on vector data requires the selection of at least one numeric column ('column_names' parameter)."
                            .to_string(),
                    }
                );

                let source = vector_source.initialize(context).await?;
                for cn in &self.params.column_names {
                    match source.result_descriptor().columns.get(cn.as_str()) {
                        Some(column) if !column.is_numeric() => {
                            return Err(Error::InvalidOperatorSpec {
                                reason: format!("Column '{}' is not numeric.", cn),
                            });
                        }
                        Some(_) => {
                            // OK
                        }
                        None => {
                            return Err(Error::ColumnDoesNotExist {
                                column: cn.to_string(),
                            });
                        }
                    }
                }
                Ok(InitializedBoxPlot::new(
                    PlotResultDescriptor {
                        spatial_reference: source.result_descriptor().spatial_reference(),
                    },
                    self.params.column_names.clone(),
                    self.params.include_no_data,
                    source,
                )
                .boxed())
            }
        }
    }
}

/// The initialization of `BoxPlot`
pub struct InitializedBoxPlot<Op> {
    result_descriptor: PlotResultDescriptor,
    names: Vec<String>,
    include_no_data: bool,
    source: Op,
}

impl<Op> InitializedBoxPlot<Op> {
    pub fn new(
        result_descriptor: PlotResultDescriptor,
        names: Vec<String>,
        include_no_data: bool,
        source: Op,
    ) -> Self {
        Self {
            result_descriptor,
            names,
            include_no_data,
            source,
        }
    }
}
impl InitializedPlotOperator for InitializedBoxPlot<Box<dyn InitializedVectorOperator>> {
    fn result_descriptor(&self) -> &PlotResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        let processor = BoxPlotVectorQueryProcessor {
            input: self.source.query_processor()?,
            column_names: self.names.clone(),
        };

        Ok(TypedPlotQueryProcessor::JsonVega(processor.boxed()))
    }
}

impl InitializedPlotOperator for InitializedBoxPlot<Vec<Box<dyn InitializedRasterOperator>>> {
    fn result_descriptor(&self) -> &PlotResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        let input = self
            .source
            .iter()
            .map(InitializedRasterOperator::query_processor)
            .collect::<Result<Vec<_>>>()?;

        let processor = BoxPlotRasterQueryProcessor {
            input,
            names: self.names.clone(),
            include_no_data: self.include_no_data,
        };
        Ok(TypedPlotQueryProcessor::JsonVega(processor.boxed()))
    }
}

/// A query processor that calculates the boxplots about its vector input.
pub struct BoxPlotVectorQueryProcessor {
    input: TypedVectorQueryProcessor,
    column_names: Vec<String>,
}

#[async_trait]
impl PlotQueryProcessor for BoxPlotVectorQueryProcessor {
    type OutputFormat = PlotData;

    fn plot_type(&self) -> &'static str {
        BOXPLOT_OPERATOR_NAME
    }

    async fn plot_query<'p>(
        &'p self,
        query: VectorQueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<Self::OutputFormat> {
        let mut accums: Vec<BoxPlotAccum> = self
            .column_names
            .iter()
            .map(|name| BoxPlotAccum::new(name.clone()))
            .collect();

        call_on_generic_vector_processor!(&self.input, processor => {
            let mut query = processor.query(query, ctx).await?;
            while let Some(collection) = query.next().await {
                let collection = collection?;

                for accum in &mut accums {
                    let feature_data = collection.data(&accum.name).expect("checked in param");
                    let iter = feature_data.float_options_iter().map(|o| match o {
                        Some(v) => v,
                        None => f64::NAN,
                    });
                    accum.update(iter)?;
                }
            }
        });

        let mut chart = geoengine_datatypes::plots::BoxPlot::new();
        for accum in &mut accums {
            if let Some(attrib) = accum.finish()? {
                chart.add_attribute(attrib);
            }
        }
        Ok(chart.to_vega_embeddable(false)?)
    }
}

/// A query processor that calculates the boxplots about its raster input.
pub struct BoxPlotRasterQueryProcessor {
    input: Vec<TypedRasterQueryProcessor>,
    names: Vec<String>,
    include_no_data: bool,
}

impl BoxPlotRasterQueryProcessor {
    async fn process_raster(
        name: String,
        include_no_data: bool,
        input: &TypedRasterQueryProcessor,
        query: PlotQueryRectangle,
        ctx: &dyn QueryContext,
    ) -> Result<Option<BoxPlotAttribute>> {
        call_on_generic_raster_processor!(input, processor => {


            let mut stream = processor.query(query.into(), ctx).await?;
            let mut accum = BoxPlotAccum::new(name);

            while let Some(tile) = stream.next().await {
                let tile = tile?;

                match tile.grid_array {
                    GridOrEmpty::Empty(grid) if include_no_data => {
                        let v:f64 = grid.no_data_value().expect("Empty grids always have a no-data value").as_();
                        let iter = std::iter::repeat(v).take(grid.number_of_elements());
                        accum.update(iter)?;
                    },
                    // Ignore empty grids if no_data should not be included
                    GridOrEmpty::Empty(_) => {},
                    GridOrEmpty::Grid(grid) if include_no_data => {
                        accum.update(grid.data.iter().map(|x| (*x).as_()))?;
                    },
                    GridOrEmpty::Grid(grid) => {
                        accum.update(grid.data.iter().filter(|&x| !grid.is_no_data(*x)).map(|x| (*x).as_()))?;
                    }
                }
            }
            accum.finish()
        })
    }
}

#[async_trait]
impl PlotQueryProcessor for BoxPlotRasterQueryProcessor {
    type OutputFormat = PlotData;

    fn plot_type(&self) -> &'static str {
        BOXPLOT_OPERATOR_NAME
    }

    async fn plot_query<'p>(
        &'p self,
        query: PlotQueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<Self::OutputFormat> {
        let results: Vec<_> = self
            .input
            .iter()
            .zip(self.names.iter())
            .map(|(proc, name)| {
                Self::process_raster(name.clone(), self.include_no_data, proc, query, ctx)
            })
            .collect();

        let results = futures::future::join_all(results)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>();

        let mut chart = geoengine_datatypes::plots::BoxPlot::new();
        results?
            .into_iter()
            .flatten()
            .for_each(|a| chart.add_attribute(a));
        Ok(chart.to_vega_embeddable(false)?)
    }
}

//
// AUX Structures
//

enum BoxPlotAccumKind {
    Exact(Vec<f64>),
    Estimated(PSquareQuantileEstimator<f64>),
}

impl BoxPlotAccumKind {
    fn update(&mut self, values: impl Iterator<Item = f64>) -> crate::util::Result<()> {
        match self {
            Self::Exact(ref mut x) => {
                x.extend(values.filter(|x| x.is_finite()));

                if x.len() > EXACT_CALC_BOUND {
                    let est = PSquareQuantileEstimator::new(0.5, x.as_slice())?;
                    *self = Self::Estimated(est);
                }
                Ok(())
            }
            Self::Estimated(ref mut est) => {
                for v in values {
                    est.update(v);
                }
                Ok(())
            }
        }
    }

    fn median(values: &[f64]) -> f64 {
        if values.len() % 2 == 0 {
            let i = values.len() / 2;
            (values[i] + values[i - 1]) / 2.0
        } else {
            values[values.len() / 2]
        }
    }

    fn split(values: &[f64]) -> (&[f64], &[f64]) {
        let idx = values.len() / 2;

        let s = values.split_at(idx);

        if values.len() % 2 == 0 {
            s
        } else {
            (s.0, &s.1[1..])
        }
    }

    fn create_plot(
        &mut self,
        name: String,
    ) -> Result<Option<geoengine_datatypes::plots::BoxPlotAttribute>> {
        match self {
            Self::Estimated(est) => Ok(Some(BoxPlotAttribute::new(
                name,
                est.min(),
                est.max(),
                est.quantile_estimate(),
                est.marker2(),
                est.marker4(),
                false,
            )?)),
            Self::Exact(v) => {
                match v.len() {
                    0 => Ok(None),
                    1 => {
                        let x = v[0];
                        Ok(Some(BoxPlotAttribute::new(name, x, x, x, x, x, true)?))
                    }
                    l => {
                        v.sort_unstable_by(|a, b| {
                            a.partial_cmp(b).expect("Infinite values were filtered")
                        });
                        let min = v[0];
                        let max = v[l - 1];
                        // We compute the quartiles accodring to https://en.wikipedia.org/wiki/Quartile#Method_1
                        let median = Self::median(v);
                        let (low, high) = Self::split(v);
                        let q1 = Self::median(low);
                        let q3 = Self::median(high);
                        Ok(Some(BoxPlotAttribute::new(
                            name, min, max, median, q1, q3, true,
                        )?))
                    }
                }
            }
        }
    }
}

struct BoxPlotAccum {
    name: String,
    accum: BoxPlotAccumKind,
}

impl BoxPlotAccum {
    fn new(name: String) -> BoxPlotAccum {
        BoxPlotAccum {
            name,
            accum: BoxPlotAccumKind::Exact(Vec::new()),
        }
    }

    fn update(&mut self, values: impl Iterator<Item = f64>) -> crate::util::Result<()> {
        for chunk in &itertools::Itertools::chunks(values, BATCH_SIZE) {
            self.accum.update(chunk)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<Option<geoengine_datatypes::plots::BoxPlotAttribute>> {
        self.accum.create_plot(self.name.clone())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use geoengine_datatypes::primitives::{
        BoundingBox2D, FeatureData, Measurement, NoGeometry, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::raster::{
        EmptyGrid2D, Grid2D, RasterDataType, RasterTile2D, TileInformation,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_datatypes::{collections::DataCollection, primitives::MultiPoint};

    use crate::engine::{
        ChunkByteSize, MockExecutionContext, MockQueryContext, RasterOperator,
        RasterResultDescriptor, VectorOperator,
    };
    use crate::mock::{MockFeatureCollectionSource, MockRasterSource, MockRasterSourceParams};

    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn serialization() {
        let histogram = BoxPlot {
            params: BoxPlotParams {
                column_names: vec!["foobar".to_string()],
                include_no_data: true,
            },
            sources: MockFeatureCollectionSource::<MultiPoint>::multiple(vec![])
                .boxed()
                .into(),
        };

        let serialized = json!({
            "type": "BoxPlot",
            "params": {
                "columnNames": ["foobar"],
                "includeNoData": true,
            },
            "sources": {
                "source": {
                    "type": "MockFeatureCollectionSourceMultiPoint",
                    "params": {
                        "collections": []
                    }
                }
            }
        })
        .to_string();

        let deserialized: BoxPlot = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.params, histogram.params);
    }

    #[test]
    fn serialization_alt() {
        let histogram = BoxPlot {
            params: BoxPlotParams {
                column_names: vec![],
                include_no_data: false,
            },
            sources: MockFeatureCollectionSource::<MultiPoint>::multiple(vec![])
                .boxed()
                .into(),
        };

        let serialized = json!({
            "type": "BoxPlot",
            "params": {
            },
            "sources": {
                "source": {
                    "type": "MockFeatureCollectionSourceMultiPoint",
                    "params": {
                        "collections": []
                    }
                }
            }
        })
        .to_string();

        let deserialized: BoxPlot = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.params, histogram.params);
    }

    #[tokio::test]
    async fn vector_data() {
        let vector_source = MockFeatureCollectionSource::multiple(vec![
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 8],
                &[
                    ("foo", FeatureData::Int(vec![1, 1, 2, 2, 3, 3, 4, 4])),
                    ("bar", FeatureData::Int(vec![1, 1, 2, 2, 3, 3, 4, 4])),
                ],
            )
            .unwrap(),
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 4],
                &[
                    ("foo", FeatureData::Int(vec![5, 6, 7, 8])),
                    ("bar", FeatureData::Int(vec![5, 6, 7, 8])),
                ],
            )
            .unwrap(),
        ])
        .boxed();

        let box_plot = BoxPlot {
            params: BoxPlotParams {
                column_names: vec!["foo".to_string(), "bar".to_string()],
                include_no_data: false,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = box_plot
            .boxed()
            .initialize(&execution_context)
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

        let mut expected = geoengine_datatypes::plots::BoxPlot::new();
        expected.add_attribute(
            BoxPlotAttribute::new("foo".to_string(), 1.0, 8.0, 3.5, 2.0, 5.5, true).unwrap(),
        );
        expected.add_attribute(
            BoxPlotAttribute::new("bar".to_string(), 1.0, 8.0, 3.5, 2.0, 5.5, true).unwrap(),
        );

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn vector_data_with_nulls() {
        let vector_source = MockFeatureCollectionSource::single(
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 7],
                &[(
                    "foo",
                    FeatureData::NullableFloat(vec![
                        Some(1.),
                        Some(2.),
                        None,
                        Some(4.),
                        None,
                        Some(6.),
                        Some(7.),
                    ]),
                )],
            )
            .unwrap(),
        )
        .boxed();

        let box_plot = BoxPlot {
            params: BoxPlotParams {
                column_names: vec!["foo".to_string()],
                include_no_data: false,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = box_plot
            .boxed()
            .initialize(&execution_context)
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

        let mut expected = geoengine_datatypes::plots::BoxPlot::new();
        expected.add_attribute(
            BoxPlotAttribute::new("foo".to_string(), 1.0, 7.0, 4.0, 1.5, 6.5, true).unwrap(),
        );

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn vector_data_text_column() {
        let vector_source = MockFeatureCollectionSource::single(
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 1],
                &[("foo", FeatureData::Text(vec!["test".to_string()]))],
            )
            .unwrap(),
        )
        .boxed();

        let box_plot = BoxPlot {
            params: BoxPlotParams {
                column_names: vec!["foo".to_string()],
                include_no_data: false,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::default();

        let init = box_plot.boxed().initialize(&execution_context).await;

        assert!(init.is_err());
    }

    #[tokio::test]
    async fn vector_data_missing_column() {
        let vector_source = MockFeatureCollectionSource::single(
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 1],
                &[("foo", FeatureData::Text(vec!["test".to_string()]))],
            )
            .unwrap(),
        )
        .boxed();

        let box_plot = BoxPlot {
            params: BoxPlotParams {
                column_names: vec![],
                include_no_data: false,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::default();

        let init = box_plot.boxed().initialize(&execution_context).await;

        assert!(init.is_err());
    }

    #[tokio::test]
    async fn vector_data_single_feature() {
        let vector_source =
            MockFeatureCollectionSource::multiple(vec![DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 1],
                &[("foo", FeatureData::Int(vec![1]))],
            )
            .unwrap()])
            .boxed();

        let box_plot = BoxPlot {
            params: BoxPlotParams {
                column_names: vec!["foo".to_string()],
                include_no_data: false,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = box_plot
            .boxed()
            .initialize(&execution_context)
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

        let mut expected = geoengine_datatypes::plots::BoxPlot::new();
        expected.add_attribute(
            BoxPlotAttribute::new("foo".to_string(), 1.0, 1.0, 1.0, 1.0, 1.0, true).unwrap(),
        );

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn vector_data_empty() {
        let vector_source =
            MockFeatureCollectionSource::multiple(vec![DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[] as &[TimeInterval],
                &[("foo", FeatureData::Int(vec![]))],
            )
            .unwrap()])
            .boxed();

        let box_plot = BoxPlot {
            params: BoxPlotParams {
                column_names: vec!["foo".to_string()],
                include_no_data: false,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = box_plot
            .boxed()
            .initialize(&execution_context)
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

        let expected = geoengine_datatypes::plots::BoxPlot::new();
        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn vector_data_estimator_switch() {
        let mut data = Vec::<i64>::with_capacity(2 * super::EXACT_CALC_BOUND);

        for i in 1..=data.capacity() as i64 {
            data.push(i);
        }

        let vector_source =
            MockFeatureCollectionSource::multiple(vec![DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 2 * super::EXACT_CALC_BOUND],
                &[("foo", FeatureData::Int(data))],
            )
            .unwrap()])
            .boxed();

        let box_plot = BoxPlot {
            params: BoxPlotParams {
                column_names: vec!["foo".to_string()],
                include_no_data: false,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = box_plot
            .boxed()
            .initialize(&execution_context)
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

        let mut expected = geoengine_datatypes::plots::BoxPlot::new();
        expected.add_attribute(
            BoxPlotAttribute::new(
                "foo".to_string(),
                1.0,
                2.0 * super::EXACT_CALC_BOUND as f64,
                super::EXACT_CALC_BOUND as f64,
                0.5 * super::EXACT_CALC_BOUND as f64,
                1.5 * super::EXACT_CALC_BOUND as f64,
                false,
            )
            .unwrap(),
        );

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn no_data_raster_exclude_no_data() {
        let no_data_value = Some(0);
        let box_plot = BoxPlot {
            params: BoxPlotParams {
                column_names: vec![],
                include_no_data: false,
            },
            sources: MockRasterSource {
                params: MockRasterSourceParams {
                    data: vec![RasterTile2D::new_with_tile_info(
                        TimeInterval::default(),
                        TileInformation {
                            global_geo_transform: TestDefault::test_default(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels: [3, 2].into(),
                        },
                        Grid2D::new([3, 2].into(), vec![0, 0, 0, 0, 0, 0], no_data_value)
                            .unwrap()
                            .into(),
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
            .into(),
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = box_plot
            .boxed()
            .initialize(&execution_context)
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

        let expected = geoengine_datatypes::plots::BoxPlot::new();

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn no_data_raster_include_no_data() {
        let no_data_value = Some(0);
        let box_plot = BoxPlot {
            params: BoxPlotParams {
                column_names: vec![],
                include_no_data: true,
            },
            sources: MockRasterSource {
                params: MockRasterSourceParams {
                    data: vec![RasterTile2D::new_with_tile_info(
                        TimeInterval::default(),
                        TileInformation {
                            global_geo_transform: TestDefault::test_default(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels: [3, 2].into(),
                        },
                        Grid2D::new([3, 2].into(), vec![0, 0, 0, 0, 0, 0], no_data_value)
                            .unwrap()
                            .into(),
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
            .into(),
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = box_plot
            .boxed()
            .initialize(&execution_context)
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

        let mut expected = geoengine_datatypes::plots::BoxPlot::new();
        expected.add_attribute(
            BoxPlotAttribute::new("Raster-1".to_owned(), 0.0, 0.0, 0.0, 0.0, 0.0, true).unwrap(),
        );

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn empty_tile_raster_exclude_no_data() {
        let no_data_value = Some(0_u8);
        let box_plot = BoxPlot {
            params: BoxPlotParams {
                column_names: vec![],
                include_no_data: false,
            },
            sources: MockRasterSource {
                params: MockRasterSourceParams {
                    data: vec![RasterTile2D::new_with_tile_info(
                        TimeInterval::default(),
                        TileInformation {
                            global_geo_transform: TestDefault::test_default(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels: [3, 2].into(),
                        },
                        EmptyGrid2D::new([3, 2].into(), no_data_value.unwrap()).into(),
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
            .into(),
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = box_plot
            .boxed()
            .initialize(&execution_context)
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

        let expected = geoengine_datatypes::plots::BoxPlot::new();

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn empty_tile_raster_include_no_data() {
        let no_data_value = Some(0_u8);
        let box_plot = BoxPlot {
            params: BoxPlotParams {
                column_names: vec![],
                include_no_data: true,
            },
            sources: MockRasterSource {
                params: MockRasterSourceParams {
                    data: vec![RasterTile2D::new_with_tile_info(
                        TimeInterval::default(),
                        TileInformation {
                            global_geo_transform: TestDefault::test_default(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels: [3, 2].into(),
                        },
                        EmptyGrid2D::new([3, 2].into(), no_data_value.unwrap()).into(),
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
            .into(),
        };

        let execution_context = MockExecutionContext::default();

        let query_processor = box_plot
            .boxed()
            .initialize(&execution_context)
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

        let mut expected = geoengine_datatypes::plots::BoxPlot::new();
        expected.add_attribute(
            BoxPlotAttribute::new("Raster-1".to_owned(), 0.0, 0.0, 0.0, 0.0, 0.0, true).unwrap(),
        );

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn single_value_raster_stream() {
        let execution_context = MockExecutionContext::default();

        let no_data_value = None;
        let histogram = BoxPlot {
            params: BoxPlotParams {
                column_names: vec![],
                include_no_data: false,
            },
            sources: MockRasterSource {
                params: MockRasterSourceParams {
                    data: vec![RasterTile2D::new_with_tile_info(
                        TimeInterval::default(),
                        TileInformation {
                            global_geo_transform: TestDefault::test_default(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels: [3, 2].into(),
                        },
                        Grid2D::new([3, 2].into(), vec![4; 6], no_data_value)
                            .unwrap()
                            .into(),
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
            .into(),
        };

        let query_processor = histogram
            .boxed()
            .initialize(&execution_context)
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
                    time_interval: TimeInterval::new_instant(
                        NaiveDate::from_ymd(2013, 12, 1).and_hms(12, 0, 0),
                    )
                    .unwrap(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::default(),
            )
            .await
            .unwrap();

        let mut expected = geoengine_datatypes::plots::BoxPlot::new();
        expected.add_attribute(
            BoxPlotAttribute::new("Raster-1".to_owned(), 4.0, 4.0, 4.0, 4.0, 4.0, true).unwrap(),
        );

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn raster_with_no_data_exclude_no_data() {
        let execution_context = MockExecutionContext::default();

        let no_data_value = Some(0);
        let histogram = BoxPlot {
            params: BoxPlotParams {
                column_names: vec![],
                include_no_data: false,
            },
            sources: MockRasterSource {
                params: MockRasterSourceParams {
                    data: vec![RasterTile2D::new_with_tile_info(
                        TimeInterval::default(),
                        TileInformation {
                            global_geo_transform: TestDefault::test_default(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels: [4, 2].into(),
                        },
                        Grid2D::new([4, 2].into(), vec![1, 2, 0, 4, 0, 6, 7, 0], no_data_value)
                            .unwrap()
                            .into(),
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
            .into(),
        };

        let query_processor = histogram
            .boxed()
            .initialize(&execution_context)
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
                    time_interval: TimeInterval::new_instant(
                        NaiveDate::from_ymd(2013, 12, 1).and_hms(12, 0, 0),
                    )
                    .unwrap(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::default(),
            )
            .await
            .unwrap();

        let mut expected = geoengine_datatypes::plots::BoxPlot::new();
        expected.add_attribute(
            BoxPlotAttribute::new("Raster-1".to_string(), 1.0, 7.0, 4.0, 1.5, 6.5, true).unwrap(),
        );

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn raster_with_no_data_include_no_data() {
        let execution_context = MockExecutionContext::default();

        let no_data_value = Some(0);
        let histogram = BoxPlot {
            params: BoxPlotParams {
                column_names: vec![],
                include_no_data: true,
            },
            sources: MockRasterSource {
                params: MockRasterSourceParams {
                    data: vec![RasterTile2D::new_with_tile_info(
                        TimeInterval::default(),
                        TileInformation {
                            global_geo_transform: TestDefault::test_default(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels: [4, 2].into(),
                        },
                        Grid2D::new([4, 2].into(), vec![1, 2, 0, 4, 0, 6, 7, 0], no_data_value)
                            .unwrap()
                            .into(),
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
            .into(),
        };

        let query_processor = histogram
            .boxed()
            .initialize(&execution_context)
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
                    time_interval: TimeInterval::new_instant(
                        NaiveDate::from_ymd(2013, 12, 1).and_hms(12, 0, 0),
                    )
                    .unwrap(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::default(),
            )
            .await
            .unwrap();

        let mut expected = geoengine_datatypes::plots::BoxPlot::new();
        expected.add_attribute(
            BoxPlotAttribute::new("Raster-1".to_string(), 0.0, 7.0, 1.5, 0.0, 5.0, true).unwrap(),
        );

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn multiple_rasters_with_no_data_exclude_no_data() {
        let execution_context = MockExecutionContext::default();
        let no_data_value = Some(0);

        let src = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![RasterTile2D::new_with_tile_info(
                    TimeInterval::default(),
                    TileInformation {
                        global_geo_transform: TestDefault::test_default(),
                        global_tile_position: [0, 0].into(),
                        tile_size_in_pixels: [4, 2].into(),
                    },
                    Grid2D::new([4, 2].into(), vec![1, 2, 0, 4, 0, 6, 7, 0], no_data_value)
                        .unwrap()
                        .into(),
                )],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        };

        let histogram = BoxPlot {
            params: BoxPlotParams {
                column_names: vec![],
                include_no_data: false,
            },
            sources: vec![
                src.clone().boxed(),
                src.clone().boxed(),
                src.clone().boxed(),
            ]
            .into(),
        };

        let query_processor = histogram
            .boxed()
            .initialize(&execution_context)
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
                    time_interval: TimeInterval::new_instant(
                        NaiveDate::from_ymd(2013, 12, 1).and_hms(12, 0, 0),
                    )
                    .unwrap(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::default(),
            )
            .await
            .unwrap();

        let mut expected = geoengine_datatypes::plots::BoxPlot::new();
        expected.add_attribute(
            BoxPlotAttribute::new("Raster-1".to_string(), 1.0, 7.0, 4.0, 1.5, 6.5, true).unwrap(),
        );
        expected.add_attribute(
            BoxPlotAttribute::new("Raster-2".to_string(), 1.0, 7.0, 4.0, 1.5, 6.5, true).unwrap(),
        );
        expected.add_attribute(
            BoxPlotAttribute::new("Raster-3".to_string(), 1.0, 7.0, 4.0, 1.5, 6.5, true).unwrap(),
        );

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }
}
