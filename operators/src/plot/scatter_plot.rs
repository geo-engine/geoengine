use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use geoengine_datatypes::collections::FeatureCollectionInfos;
use geoengine_datatypes::plots::{Histogram2D, HistogramDimension, Plot, PlotData};

use crate::engine::{
    ExecutionContext, InitializedPlotOperator, InitializedVectorOperator, Operator, PlotOperator,
    PlotQueryProcessor, PlotResultDescriptor, QueryContext, QueryProcessor, SingleVectorSource,
    TypedPlotQueryProcessor, TypedVectorQueryProcessor, VectorQueryRectangle,
};
use crate::error::Error;
use crate::util::Result;
use geoengine_datatypes::primitives::Coordinate2D;

pub const SCATTERPLOT_OPERATOR_NAME: &str = "ScatterPlot";

/// The maximum number of elements for a scatter plot
const SCATTER_PLOT_THRESHOLD: usize = 500;

/// The number of elements to process at once (i.e., without switching from scatter-plot to histogram)
const BATCH_SIZE: usize = 1000;

/// The maximum number of elements before we turn the collector into a histogram.
/// At this point, the bounds of the histogram are fixed (i.e., further values exceeding
/// the min/max seen so far are ignored)
const COLLECTOR_TO_HISTOGRAM_THRESHOLD: usize = BATCH_SIZE * 10;

/// A scatter plot about two attributes of a vector dataset. If the
/// dataset contains more then `SCATTER_PLOT_THRESHOLD` elements, this
/// operator creates a 2D histogram.
pub type ScatterPlot = Operator<ScatterPlotParams, SingleVectorSource>;

/// The parameter spec for `ScatterPlot`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScatterPlotParams {
    /// Name of the (numeric) attribute for the x-axis.
    pub column_x: String,
    /// Name of the (numeric) attribute for the y-axis.
    pub column_y: String,
}

#[typetag::serde]
#[async_trait]
impl PlotOperator for ScatterPlot {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedPlotOperator>> {
        let source = self.sources.vector.initialize(context).await?;
        for cn in [&self.params.column_x, &self.params.column_y] {
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
        Ok(InitializedScatterPlot::new(PlotResultDescriptor {}, self.params, source).boxed())
    }
}

/// The initialization of `Histogram`
pub struct InitializedScatterPlot<Op> {
    result_descriptor: PlotResultDescriptor,
    column_x: String,
    column_y: String,
    source: Op,
}

impl<Op> InitializedScatterPlot<Op> {
    pub fn new(
        result_descriptor: PlotResultDescriptor,
        params: ScatterPlotParams,
        source: Op,
    ) -> Self {
        Self {
            result_descriptor,
            column_x: params.column_x,
            column_y: params.column_y,
            source,
        }
    }
}
impl InitializedPlotOperator for InitializedScatterPlot<Box<dyn InitializedVectorOperator>> {
    fn result_descriptor(&self) -> &PlotResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        let processor = ScatterPlotQueryProcessor {
            input: self.source.query_processor()?,
            column_x: self.column_x.clone(),
            column_y: self.column_y.clone(),
        };

        Ok(TypedPlotQueryProcessor::JsonVega(processor.boxed()))
    }
}

/// A query processor that calculates the scatter plot about its vector input.
pub struct ScatterPlotQueryProcessor {
    input: TypedVectorQueryProcessor,
    column_x: String,
    column_y: String,
}

#[async_trait]
impl PlotQueryProcessor for ScatterPlotQueryProcessor {
    type OutputFormat = PlotData;

    fn plot_type(&self) -> &'static str {
        SCATTERPLOT_OPERATOR_NAME
    }

    async fn plot_query<'p>(
        &'p self,
        query: VectorQueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<Self::OutputFormat> {
        let mut collector =
            CollectorKind::Values(Collector::new(self.column_x.clone(), self.column_y.clone()));

        call_on_generic_vector_processor!(&self.input, processor => {
            let mut query = processor.query(query, ctx).await?;
            while let Some(collection) = query.next().await {
                let collection = collection?;

                let data_x = collection.data(&self.column_x).expect("checked in param");
                let data_y = collection.data(&self.column_y).expect("checked in param");

                let valid_points = data_x.float_options_iter().zip(data_y.float_options_iter()).filter_map(|(a,b)| match (a,b) {
                    (Some(x),Some(y)) if x.is_finite() && y.is_finite() => Some(Coordinate2D::new(x,y)),
                    _ => None
                });

                for chunk in &itertools::Itertools::chunks(valid_points, BATCH_SIZE) {
                    collector.add_batch( chunk )?;
                }
            }
        });
        Ok(collector.into_plot()?.to_vega_embeddable(false)?)
    }
}

struct Collector {
    elements: Vec<Coordinate2D>,
    column_x: String,
    column_y: String,
    bounds_x: (f64, f64),
    bounds_y: (f64, f64),
}

impl Collector {
    fn new(column_x: String, column_y: String) -> Self {
        Collector {
            column_x,
            column_y,
            elements: Vec::with_capacity(SCATTER_PLOT_THRESHOLD),
            bounds_x: (f64::INFINITY, f64::NEG_INFINITY),
            bounds_y: (f64::INFINITY, f64::NEG_INFINITY),
        }
    }

    fn element_count(&self) -> usize {
        self.elements.len()
    }

    fn add_batch(&mut self, values: impl Iterator<Item = Coordinate2D>) {
        for v in values {
            self.add(v);
        }
    }

    fn add(&mut self, value: Coordinate2D) {
        if value.x.is_finite() && value.y.is_finite() {
            self.bounds_x.0 = std::cmp::min_by(self.bounds_x.0, value.x, |a, b| {
                a.partial_cmp(b).expect("checked")
            });
            self.bounds_x.1 = std::cmp::max_by(self.bounds_x.1, value.x, |a, b| {
                a.partial_cmp(b).expect("checked")
            });
            self.bounds_y.0 = std::cmp::min_by(self.bounds_y.0, value.y, |a, b| {
                a.partial_cmp(b).expect("checked")
            });
            self.bounds_y.1 = std::cmp::max_by(self.bounds_y.1, value.y, |a, b| {
                a.partial_cmp(b).expect("checked")
            });
            self.elements.push(value);
        }
    }
}

enum CollectorKind {
    Values(Collector),
    Histogram(Histogram2D),
}

impl CollectorKind {
    fn histogram_from_collector(value: &Collector) -> Result<Histogram2D> {
        let bucket_count = std::cmp::min(100, f64::sqrt(value.element_count() as f64) as usize);

        let dim_x = HistogramDimension::new(
            value.column_x.clone(),
            value.bounds_x.0,
            value.bounds_x.1,
            bucket_count,
        )?;
        let dim_y = HistogramDimension::new(
            value.column_y.clone(),
            value.bounds_y.0,
            value.bounds_y.1,
            bucket_count,
        )?;

        let mut result = Histogram2D::new(dim_x, dim_y);
        result.update_batch(value.elements.iter().copied());
        Ok(result)
    }

    fn add_batch(&mut self, values: impl Iterator<Item = Coordinate2D>) -> Result<()> {
        match self {
            Self::Values(ref mut c) => {
                c.add_batch(values);
                if c.element_count() > COLLECTOR_TO_HISTOGRAM_THRESHOLD {
                    *self = Self::Histogram(Self::histogram_from_collector(c)?)
                }
            }
            Self::Histogram(ref mut h) => {
                h.update_batch(values);
            }
        }
        Ok(())
    }

    fn into_plot(self) -> Result<Box<dyn Plot>> {
        match self {
            Self::Histogram(h) => Ok(Box::new(h)),
            Self::Values(v) if v.element_count() <= SCATTER_PLOT_THRESHOLD => Ok(Box::new(
                geoengine_datatypes::plots::ScatterPlot::new_with_data(
                    v.column_x, v.column_y, v.elements,
                ),
            )),
            Self::Values(v) => Ok(Box::new(Self::histogram_from_collector(&v)?)),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use geoengine_datatypes::primitives::{
        BoundingBox2D, FeatureData, NoGeometry, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::{collections::DataCollection, primitives::MultiPoint};

    use crate::engine::{MockExecutionContext, MockQueryContext, VectorOperator};
    use crate::mock::MockFeatureCollectionSource;

    use super::*;

    #[test]
    fn serialization() {
        let scatter_plot = ScatterPlot {
            params: ScatterPlotParams {
                column_x: "foo".to_owned(),
                column_y: "bar".to_owned(),
            },
            sources: MockFeatureCollectionSource::<MultiPoint>::multiple(vec![])
                .boxed()
                .into(),
        };

        let serialized = json!({
            "type": "ScatterPlot",
            "params": {
                "columnX": "foo",
                "columnY": "bar",
            },
            "sources": {
                "vector": {
                    "type": "MockFeatureCollectionSourceMultiPoint",
                    "params": {
                        "collections": []
                    }
                }
            }
        })
        .to_string();

        let deserialized: ScatterPlot = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.params, scatter_plot.params);
    }

    #[tokio::test]
    async fn vector_data() {
        let vector_source = MockFeatureCollectionSource::multiple(vec![
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 4],
                &[
                    ("foo", FeatureData::Int(vec![1, 2, 3, 4])),
                    ("bar", FeatureData::Int(vec![1, 2, 3, 4])),
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

        let box_plot = ScatterPlot {
            params: ScatterPlotParams {
                column_x: "foo".to_string(),
                column_y: "bar".to_string(),
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
                &MockQueryContext::new(0),
            )
            .await
            .unwrap();

        let mut expected =
            geoengine_datatypes::plots::ScatterPlot::new("foo".to_string(), "bar".to_string());
        for i in 1..=8 {
            expected.update(Coordinate2D::new(f64::from(i), f64::from(i)));
        }
        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn vector_data_with_nulls_and_nan() {
        let vector_source =
            MockFeatureCollectionSource::multiple(vec![DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 7],
                &[
                    (
                        "foo",
                        FeatureData::NullableFloat(vec![
                            Some(1.0),
                            None,
                            Some(3.0),
                            None,
                            Some(f64::NAN),
                            Some(6.0),
                            Some(f64::NAN),
                        ]),
                    ),
                    (
                        "bar",
                        FeatureData::NullableFloat(vec![
                            Some(1.0),
                            Some(2.0),
                            None,
                            None,
                            Some(5.0),
                            Some(f64::NAN),
                            Some(f64::NAN),
                        ]),
                    ),
                ],
            )
            .unwrap()])
            .boxed();

        let box_plot = ScatterPlot {
            params: ScatterPlotParams {
                column_x: "foo".to_string(),
                column_y: "bar".to_string(),
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
                &MockQueryContext::new(0),
            )
            .await
            .unwrap();

        let mut expected =
            geoengine_datatypes::plots::ScatterPlot::new("foo".to_string(), "bar".to_string());
        expected.update(Coordinate2D::new(1.0, 1.0));
        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn vector_data_text_column_x() {
        let vector_source = MockFeatureCollectionSource::single(
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 1],
                &[
                    ("foo", FeatureData::Text(vec!["test".to_string()])),
                    ("bar", FeatureData::Int(vec![64])),
                ],
            )
            .unwrap(),
        )
        .boxed();

        let box_plot = ScatterPlot {
            params: ScatterPlotParams {
                column_x: "foo".to_string(),
                column_y: "bar".to_string(),
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::default();

        let init = box_plot.boxed().initialize(&execution_context).await;

        assert!(init.is_err());
    }

    #[tokio::test]
    async fn vector_data_text_column_y() {
        let vector_source = MockFeatureCollectionSource::single(
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 1],
                &[
                    ("foo", FeatureData::Text(vec!["test".to_string()])),
                    ("bar", FeatureData::Int(vec![64])),
                ],
            )
            .unwrap(),
        )
        .boxed();

        let box_plot = ScatterPlot {
            params: ScatterPlotParams {
                column_x: "bar".to_string(),
                column_y: "foo".to_string(),
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::default();

        let init = box_plot.boxed().initialize(&execution_context).await;

        assert!(init.is_err());
    }

    #[tokio::test]
    async fn vector_data_missing_column_x() {
        let vector_source = MockFeatureCollectionSource::single(
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 1],
                &[
                    ("foo", FeatureData::Text(vec!["test".to_string()])),
                    ("bar", FeatureData::Int(vec![64])),
                ],
            )
            .unwrap(),
        )
        .boxed();

        let box_plot = ScatterPlot {
            params: ScatterPlotParams {
                column_x: "fo".to_string(),
                column_y: "bar".to_string(),
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::default();

        let init = box_plot.boxed().initialize(&execution_context).await;

        assert!(init.is_err());
    }

    #[tokio::test]
    async fn vector_data_missing_column_y() {
        let vector_source = MockFeatureCollectionSource::single(
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 1],
                &[
                    ("foo", FeatureData::Text(vec!["test".to_string()])),
                    ("bar", FeatureData::Int(vec![64])),
                ],
            )
            .unwrap(),
        )
        .boxed();

        let box_plot = ScatterPlot {
            params: ScatterPlotParams {
                column_x: "foo".to_string(),
                column_y: "ba".to_string(),
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
                &[
                    ("foo", FeatureData::Int(vec![1])),
                    ("bar", FeatureData::Int(vec![1])),
                ],
            )
            .unwrap()])
            .boxed();

        let box_plot = ScatterPlot {
            params: ScatterPlotParams {
                column_x: "foo".to_string(),
                column_y: "bar".to_string(),
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
                &MockQueryContext::new(0),
            )
            .await
            .unwrap();

        let mut expected =
            geoengine_datatypes::plots::ScatterPlot::new("foo".to_string(), "bar".to_string());
        expected.update(Coordinate2D::new(1.0, 1.0));
        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn vector_data_empty() {
        let vector_source =
            MockFeatureCollectionSource::multiple(vec![DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[] as &[TimeInterval],
                &[
                    ("foo", FeatureData::Int(vec![])),
                    ("bar", FeatureData::Int(vec![])),
                ],
            )
            .unwrap()])
            .boxed();

        let box_plot = ScatterPlot {
            params: ScatterPlotParams {
                column_x: "foo".to_string(),
                column_y: "bar".to_string(),
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
                &MockQueryContext::new(0),
            )
            .await
            .unwrap();

        let expected =
            geoengine_datatypes::plots::ScatterPlot::new("foo".to_string(), "bar".to_string());
        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn to_histogram_at_end() {
        let mut values = vec![1; 700];
        values.push(2);

        let vector_source =
            MockFeatureCollectionSource::multiple(vec![DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 701],
                &[
                    ("foo", FeatureData::Int(values.clone())),
                    ("bar", FeatureData::Int(values.clone())),
                ],
            )
            .unwrap()])
            .boxed();

        let box_plot = ScatterPlot {
            params: ScatterPlotParams {
                column_x: "foo".to_string(),
                column_y: "bar".to_string(),
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
                &MockQueryContext::new(0),
            )
            .await
            .unwrap();

        let dim_x = HistogramDimension::new("foo".to_string(), 1.0, 2.0, 26).unwrap();
        let dim_y = HistogramDimension::new("bar".to_string(), 1.0, 2.0, 26).unwrap();

        let mut expected = geoengine_datatypes::plots::Histogram2D::new(dim_x, dim_y);
        expected.update_batch(
            values
                .into_iter()
                .map(|x| Coordinate2D::new(x as f64, x as f64)),
        );
        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[tokio::test]
    async fn to_histogram_while_iterating() {
        let mut values = vec![1; 5999];
        values.push(2);

        let vector_source = MockFeatureCollectionSource::multiple(vec![
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 6000],
                &[
                    ("foo", FeatureData::Int(values.clone())),
                    ("bar", FeatureData::Int(values.clone())),
                ],
            )
            .unwrap(),
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 6000],
                &[
                    ("foo", FeatureData::Int(values.clone())),
                    ("bar", FeatureData::Int(values.clone())),
                ],
            )
            .unwrap(),
            DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 6000],
                &[
                    ("foo", FeatureData::Int(values.clone())),
                    ("bar", FeatureData::Int(values.clone())),
                ],
            )
            .unwrap(),
        ])
        .boxed();

        let box_plot = ScatterPlot {
            params: ScatterPlotParams {
                column_x: "foo".to_string(),
                column_y: "bar".to_string(),
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
                &MockQueryContext::new(0),
            )
            .await
            .unwrap();

        let dim_x = HistogramDimension::new("foo".to_string(), 1.0, 2.0, 100).unwrap();
        let dim_y = HistogramDimension::new("bar".to_string(), 1.0, 2.0, 100).unwrap();

        let mut expected = geoengine_datatypes::plots::Histogram2D::new(dim_x, dim_y);
        expected.update_batch(
            values
                .iter()
                .map(|x| Coordinate2D::new(*x as f64, *x as f64)),
        );
        expected.update_batch(
            values
                .iter()
                .map(|x| Coordinate2D::new(*x as f64, *x as f64)),
        );
        expected.update_batch(
            values
                .iter()
                .map(|x| Coordinate2D::new(*x as f64, *x as f64)),
        );
        assert_eq!(expected.to_vega_embeddable(false).unwrap(), result);
    }

    #[test]
    fn test_collector_kind_empty() {
        let cx = "x".to_string();
        let cy = "y".to_string();

        let c = CollectorKind::Values(Collector::new(cx.clone(), cy.clone()));
        let res = c.into_plot().unwrap().to_vega_embeddable(false).unwrap();

        let expected = geoengine_datatypes::plots::ScatterPlot::new(cx, cy)
            .to_vega_embeddable(false)
            .unwrap();

        assert_eq!(expected, res);
    }

    #[test]
    fn test_collector_kind_scatter_plot() {
        let cx = "x".to_string();
        let cy = "y".to_string();

        let mut values = Vec::with_capacity(200);
        for i in 0..SCATTER_PLOT_THRESHOLD / 2 {
            values.push(Coordinate2D::new(i as f64, i as f64));
        }

        let mut c = CollectorKind::Values(Collector::new(cx.clone(), cy.clone()));

        c.add_batch(values.clone().into_iter()).unwrap();

        assert!(matches!(c, CollectorKind::Values(_)));

        let res = c.into_plot().unwrap().to_vega_embeddable(false).unwrap();

        let mut expected = geoengine_datatypes::plots::ScatterPlot::new(cx, cy);
        expected.update_batch(values.into_iter());

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), res);
    }

    #[test]
    fn test_collector_kind_histogram_end() {
        let cx = "x".to_string();
        let cy = "y".to_string();

        let element_count = SCATTER_PLOT_THRESHOLD * 2;

        let mut values = Vec::with_capacity(element_count);
        for i in 0..element_count {
            values.push(Coordinate2D::new(i as f64, i as f64));
        }

        let mut c = CollectorKind::Values(Collector::new(cx.clone(), cy.clone()));
        c.add_batch(values.clone().into_iter()).unwrap();

        assert!(matches!(c, CollectorKind::Values(_)));

        let res = c.into_plot().unwrap().to_vega_embeddable(false).unwrap();

        // expected
        let bucket_count = std::cmp::min(100, f64::sqrt(element_count as f64) as usize);
        let dimx =
            HistogramDimension::new(cx, 0.0, (element_count - 1) as f64, bucket_count).unwrap();

        let dimy =
            HistogramDimension::new(cy, 0.0, (element_count - 1) as f64, bucket_count).unwrap();

        let mut expected = geoengine_datatypes::plots::Histogram2D::new(dimx, dimy);
        expected.update_batch(values.into_iter());

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), res);
    }
    #[test]
    fn test_collector_kind_histogram_in_flight() {
        let cx = "x".to_string();
        let cy = "y".to_string();

        let element_count = COLLECTOR_TO_HISTOGRAM_THRESHOLD + 1;

        let mut values = Vec::with_capacity(element_count);
        for i in 0..element_count {
            values.push(Coordinate2D::new(i as f64, i as f64));
        }

        let mut c = CollectorKind::Values(Collector::new(cx.clone(), cy.clone()));
        c.add_batch(values.clone().into_iter()).unwrap();

        assert!(matches!(c, CollectorKind::Histogram(_)));

        let res = c.into_plot().unwrap().to_vega_embeddable(false).unwrap();

        // expected
        let bucket_count = std::cmp::min(100, f64::sqrt(element_count as f64) as usize);
        let dimx =
            HistogramDimension::new(cx, 0.0, (element_count - 1) as f64, bucket_count).unwrap();

        let dimy =
            HistogramDimension::new(cy, 0.0, (element_count - 1) as f64, bucket_count).unwrap();

        let mut expected = geoengine_datatypes::plots::Histogram2D::new(dimx, dimy);
        expected.update_batch(values.into_iter());

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), res);
    }

    #[test]
    fn test_collector_kind_histogram_out_of_range() {
        let cx = "x".to_string();
        let cy = "y".to_string();

        let element_count = COLLECTOR_TO_HISTOGRAM_THRESHOLD + 1;

        let mut values = Vec::with_capacity(element_count);
        for i in 0..element_count {
            values.push(Coordinate2D::new(i as f64, i as f64));
        }

        let mut c = CollectorKind::Values(Collector::new(cx.clone(), cy.clone()));
        c.add_batch(values.clone().into_iter()).unwrap();

        assert!(matches!(c, CollectorKind::Histogram(_)));

        // This value should be skipped
        c.add_batch(
            [Coordinate2D::new(
                element_count as f64,
                element_count as f64,
            )]
            .into_iter(),
        )
        .unwrap();

        let res = c.into_plot().unwrap().to_vega_embeddable(false).unwrap();

        // expected
        let bucket_count = std::cmp::min(100, f64::sqrt(element_count as f64) as usize);
        let dimx =
            HistogramDimension::new(cx, 0.0, (element_count - 1) as f64, bucket_count).unwrap();

        let dimy =
            HistogramDimension::new(cy, 0.0, (element_count - 1) as f64, bucket_count).unwrap();

        let mut expected = geoengine_datatypes::plots::Histogram2D::new(dimx, dimy);
        expected.update_batch(values.into_iter());

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), res);
    }

    #[test]
    fn test_collector_kind_histogram_infinite() {
        let cx = "x".to_string();
        let cy = "y".to_string();

        let element_count = COLLECTOR_TO_HISTOGRAM_THRESHOLD + 1;

        let mut values = Vec::with_capacity(element_count);
        for i in 0..element_count {
            values.push(Coordinate2D::new(i as f64, i as f64));
        }

        let mut c = CollectorKind::Values(Collector::new(cx.clone(), cy.clone()));
        c.add_batch(values.clone().into_iter()).unwrap();

        assert!(matches!(c, CollectorKind::Histogram(_)));

        // This value should be skipped
        c.add_batch([Coordinate2D::new(f64::NAN, f64::NAN)].into_iter())
            .unwrap();

        let res = c.into_plot().unwrap().to_vega_embeddable(false).unwrap();

        // expected
        let bucket_count = std::cmp::min(100, f64::sqrt(element_count as f64) as usize);
        let dimx =
            HistogramDimension::new(cx, 0.0, (element_count - 1) as f64, bucket_count).unwrap();

        let dimy =
            HistogramDimension::new(cy, 0.0, (element_count - 1) as f64, bucket_count).unwrap();

        let mut expected = geoengine_datatypes::plots::Histogram2D::new(dimx, dimy);
        expected.update_batch(values.into_iter());

        assert_eq!(expected.to_vega_embeddable(false).unwrap(), res);
    }
}
