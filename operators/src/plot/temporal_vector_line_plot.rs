use crate::engine::QueryProcessor;
use crate::engine::{
    ExecutionContext, InitializedPlotOperator, InitializedVectorOperator, Operator, PlotOperator,
    PlotQueryProcessor, PlotResultDescriptor, QueryContext, SingleVectorSource,
    TypedPlotQueryProcessor, VectorQueryProcessor, VectorQueryRectangle,
};
use crate::error;
use crate::util::Result;
use async_trait::async_trait;
use futures::StreamExt;
use geoengine_datatypes::primitives::FeatureDataType;
use geoengine_datatypes::{
    collections::FeatureCollection,
    plots::{Plot, PlotData},
};
use geoengine_datatypes::{
    collections::FeatureCollectionInfos,
    plots::{DataPoint, MultiLineChart},
};
use geoengine_datatypes::{
    primitives::{Geometry, Measurement, TimeInterval},
    util::arrow::ArrowTyped,
};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use std::collections::HashMap;
use std::{
    cmp::Ordering,
    collections::hash_map::Entry::{Occupied, Vacant},
};

pub const FEATURE_ATTRIBUTE_OVER_TIME_NAME: &str = "Feature Attribute over Time";
const MAX_FEATURES: usize = 20;

/// A plot that shows the value of an feature attribute over time.
pub type FeatureAttributeValuesOverTime =
    Operator<FeatureAttributeValuesOverTimeParams, SingleVectorSource>;

/// The parameter spec for `FeatureAttributeValuesOverTime`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureAttributeValuesOverTimeParams {
    pub id_column: String,
    pub value_column: String,
}

#[typetag::serde]
#[async_trait]
impl PlotOperator for FeatureAttributeValuesOverTime {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedPlotOperator>> {
        let source = self.sources.vector.initialize(context).await?;
        let result_descriptor = source.result_descriptor();
        let columns: &HashMap<String, FeatureDataType> = &result_descriptor.columns;

        ensure!(
            columns.contains_key(&self.params.id_column),
            error::ColumnDoesNotExist {
                column: self.params.id_column.clone()
            }
        );

        ensure!(
            columns.contains_key(&self.params.value_column),
            error::ColumnDoesNotExist {
                column: self.params.value_column.clone()
            }
        );

        let id_type = columns.get(&self.params.id_column).expect("checked");
        let value_type = columns.get(&self.params.value_column).expect("checked");

        // TODO: ensure column is really an id
        ensure!(
            id_type == &FeatureDataType::Text
                || id_type == &FeatureDataType::Int
                || id_type == &FeatureDataType::Category,
            error::InvalidFeatureDataType,
        );

        ensure!(
            value_type.is_numeric() || value_type == &FeatureDataType::Category,
            error::InvalidFeatureDataType,
        );

        Ok(InitializedFeatureAttributeValuesOverTime {
            result_descriptor: PlotResultDescriptor {
                spatial_reference: source.result_descriptor().spatial_reference,
            },
            vector_source: source,
            state: self.params,
        }
        .boxed())
    }
}

/// The initialization of `FeatureAttributeValuesOverTime`
pub struct InitializedFeatureAttributeValuesOverTime {
    result_descriptor: PlotResultDescriptor,
    vector_source: Box<dyn InitializedVectorOperator>,
    state: FeatureAttributeValuesOverTimeParams,
}

impl InitializedPlotOperator for InitializedFeatureAttributeValuesOverTime {
    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        let input_processor = self.vector_source.query_processor()?;

        let processor = call_on_generic_vector_processor!(input_processor, features => {
            FeatureAttributeValuesOverTimeQueryProcessor { params: self.state.clone(), features }.boxed()
        });

        Ok(TypedPlotQueryProcessor::JsonVega(processor))
    }

    fn result_descriptor(&self) -> &PlotResultDescriptor {
        &self.result_descriptor
    }
}

/// A query processor that calculates the `TemporalVectorLinePlot` on its input.
pub struct FeatureAttributeValuesOverTimeQueryProcessor<G>
where
    G: Geometry + ArrowTyped + Sync + Send + 'static,
{
    params: FeatureAttributeValuesOverTimeParams,
    features: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
}

#[async_trait]
impl<G> PlotQueryProcessor for FeatureAttributeValuesOverTimeQueryProcessor<G>
where
    G: Geometry + ArrowTyped + Sync + Send + 'static,
{
    type OutputFormat = PlotData;

    fn plot_type(&self) -> &'static str {
        FEATURE_ATTRIBUTE_OVER_TIME_NAME
    }

    async fn plot_query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<Self::OutputFormat> {
        let values = FeatureAttributeValues::<MAX_FEATURES>::default();

        let values = self
            .features
            .query(query, ctx)
            .await?
            .fold(Ok(values), |acc, features| async {
                match (acc, features) {
                    (Ok(mut acc), Ok(features)) => {
                        let ids = features.data(&self.params.id_column)?;
                        let values = features.data(&self.params.value_column)?;

                        for ((id, value), &time) in ids
                            .strings_iter()
                            .zip(values.float_options_iter())
                            .zip(features.time_intervals())
                        {
                            if id.is_empty() || value.is_none() {
                                continue;
                            }

                            let value = value.expect("checked above");

                            acc.add(id, (time, value));
                        }

                        Ok(acc)
                    }
                    (Err(err), _) | (_, Err(err)) => Err(err),
                }
            })
            .await?;

        let data_points = values.get_data_points();
        let measurement = Measurement::Unitless; // TODO: attach actual unit if we know it
        MultiLineChart::new(data_points, measurement)
            .to_vega_embeddable(false)
            .context(error::DataType)
    }
}

struct TemporalValue {
    pub time: TimeInterval,
    pub value: f64,
}

impl From<(TimeInterval, f64)> for TemporalValue {
    fn from(value: (TimeInterval, f64)) -> Self {
        Self {
            time: value.0,
            value: value.1,
        }
    }
}

struct FeatureAttributeValues<const LENGTH: usize> {
    values: HashMap<String, Vec<TemporalValue>>,
}

impl<const LENGTH: usize> Default for FeatureAttributeValues<LENGTH> {
    fn default() -> Self {
        Self {
            values: HashMap::with_capacity(LENGTH),
        }
    }
}

impl<const LENGTH: usize> FeatureAttributeValues<LENGTH> {
    /// Add value to the data structure. If `id` is new and there are already `LENGTH` existing
    /// `id`-entries, the value is ignored
    pub fn add<V>(&mut self, id: String, value: V)
    where
        V: Into<TemporalValue>,
    {
        let len = self.values.len();

        match self.values.entry(id) {
            Occupied(mut entry) => entry.get_mut().push(value.into()),
            Vacant(entry) => {
                if len < LENGTH {
                    entry.insert(vec![value.into()]);
                }
            }
        }
    }

    pub fn get_data_points(mut self) -> Vec<DataPoint> {
        let mut data = self
            .values
            .drain()
            .flat_map(|(id, values)| {
                values.into_iter().map(move |value| DataPoint {
                    series: id.clone(),
                    time: value.time.start(),
                    value: value.value,
                })
            })
            .collect::<Vec<_>>();

        data.sort_unstable_by(|a, b| match a.series.cmp(&b.series) {
            Ordering::Equal => a.time.cmp(&b.time),
            other => other,
        });
        data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use geoengine_datatypes::{
        collections::MultiPointCollection,
        plots::PlotMetaData,
        primitives::{BoundingBox2D, FeatureData, MultiPoint, SpatialResolution, TimeInterval},
    };

    use crate::{
        engine::{ChunkByteSize, MockExecutionContext, MockQueryContext, VectorOperator},
        mock::MockFeatureCollectionSource,
    };

    #[tokio::test]
    async fn plot() {
        let point_source = MockFeatureCollectionSource::single(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![
                    vec![(-13.95, 20.05)],
                    vec![(-14.05, 20.05)],
                    vec![(-13.95, 20.05)],
                ])
                .unwrap(),
                vec![
                    TimeInterval::new_unchecked(
                        NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
                        NaiveDate::from_ymd(2014, 2, 1).and_hms(0, 0, 0),
                    ),
                    TimeInterval::new_unchecked(
                        NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
                        NaiveDate::from_ymd(2014, 3, 1).and_hms(0, 0, 0),
                    ),
                    TimeInterval::new_unchecked(
                        NaiveDate::from_ymd(2014, 2, 1).and_hms(0, 0, 0),
                        NaiveDate::from_ymd(2014, 3, 1).and_hms(0, 0, 0),
                    ),
                ],
                [
                    (
                        "id".to_string(),
                        FeatureData::Text(vec!["S0".to_owned(), "S1".to_owned(), "S0".to_owned()]),
                    ),
                    ("value".to_string(), FeatureData::Float(vec![0., 2., 1.])),
                ]
                .iter()
                .cloned()
                .collect(),
            )
            .unwrap(),
        )
        .boxed();

        let exe_ctc = MockExecutionContext::default();

        let operator = FeatureAttributeValuesOverTime {
            params: FeatureAttributeValuesOverTimeParams {
                id_column: "id".to_owned(),
                value_column: "value".to_owned(),
            },
            sources: point_source.into(),
        };

        let operator = operator.boxed().initialize(&exe_ctc).await.unwrap();

        let query_processor = operator.query_processor().unwrap().json_vega().unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            PlotData {
                vega_string: r#"{"$schema":"https://vega.github.io/schema/vega-lite/v4.17.0.json","data":{"values":[{"x":"2014-01-01T00:00:00+00:00","y":0.0,"series":"S0"},{"x":"2014-02-01T00:00:00+00:00","y":1.0,"series":"S0"},{"x":"2014-01-01T00:00:00+00:00","y":2.0,"series":"S1"}]},"description":"Multi Line Chart","encoding":{"x":{"field":"x","title":"Time","type":"temporal"},"y":{"field":"y","title":"","type":"quantitative"},"color":{"field":"series","scale":{"scheme":"category20"}}},"mark":{"type":"line","line":true,"point":true}}"#.to_owned(),
                metadata: PlotMetaData::None,
            }
        );
    }

    #[tokio::test]
    async fn plot_with_nulls() {
        let point_source = MockFeatureCollectionSource::single(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![
                    vec![(-13.95, 20.05)],
                    vec![(-14.05, 20.05)],
                    vec![(-13.95, 20.05)],
                    vec![(-14.05, 20.05)],
                    vec![(-13.95, 20.05)],
                ])
                .unwrap(),
                vec![
                    TimeInterval::new_unchecked(
                        NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
                        NaiveDate::from_ymd(2014, 2, 1).and_hms(0, 0, 0),
                    ),
                    TimeInterval::new_unchecked(
                        NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
                        NaiveDate::from_ymd(2014, 3, 1).and_hms(0, 0, 0),
                    ),
                    TimeInterval::new_unchecked(
                        NaiveDate::from_ymd(2014, 2, 1).and_hms(0, 0, 0),
                        NaiveDate::from_ymd(2014, 3, 1).and_hms(0, 0, 0),
                    ),
                    TimeInterval::new_unchecked(
                        NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
                        NaiveDate::from_ymd(2014, 3, 1).and_hms(0, 0, 0),
                    ),
                    TimeInterval::new_unchecked(
                        NaiveDate::from_ymd(2014, 2, 1).and_hms(0, 0, 0),
                        NaiveDate::from_ymd(2014, 3, 1).and_hms(0, 0, 0),
                    ),
                ],
                [
                    (
                        "id".to_string(),
                        FeatureData::NullableText(vec![
                            Some("S0".to_owned()),
                            Some("S1".to_owned()),
                            Some("S0".to_owned()),
                            None,
                            Some("S2".to_owned()),
                        ]),
                    ),
                    (
                        "value".to_string(),
                        FeatureData::NullableFloat(vec![
                            Some(0.),
                            Some(2.),
                            Some(1.),
                            Some(3.),
                            None,
                        ]),
                    ),
                ]
                .iter()
                .cloned()
                .collect(),
            )
            .unwrap(),
        )
        .boxed();

        let exe_ctc = MockExecutionContext::default();

        let operator = FeatureAttributeValuesOverTime {
            params: FeatureAttributeValuesOverTimeParams {
                id_column: "id".to_owned(),
                value_column: "value".to_owned(),
            },
            sources: point_source.into(),
        };

        let operator = operator.boxed().initialize(&exe_ctc).await.unwrap();

        let query_processor = operator.query_processor().unwrap().json_vega().unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            PlotData {
                vega_string: r#"{"$schema":"https://vega.github.io/schema/vega-lite/v4.17.0.json","data":{"values":[{"x":"2014-01-01T00:00:00+00:00","y":0.0,"series":"S0"},{"x":"2014-02-01T00:00:00+00:00","y":1.0,"series":"S0"},{"x":"2014-01-01T00:00:00+00:00","y":2.0,"series":"S1"}]},"description":"Multi Line Chart","encoding":{"x":{"field":"x","title":"Time","type":"temporal"},"y":{"field":"y","title":"","type":"quantitative"},"color":{"field":"series","scale":{"scheme":"category20"}}},"mark":{"type":"line","line":true,"point":true}}"#.to_owned(),
                metadata: PlotMetaData::None,
            }
        );
    }

    #[tokio::test]
    async fn plot_with_duplicates() {
        let point_source = MockFeatureCollectionSource::single(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![
                    vec![(-13.95, 20.05)],
                    vec![(-14.05, 20.05)],
                    vec![(-13.95, 20.05)],
                    vec![(-13.95, 20.05)],
                ])
                .unwrap(),
                vec![
                    TimeInterval::new_unchecked(
                        NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
                        NaiveDate::from_ymd(2014, 2, 1).and_hms(0, 0, 0),
                    ),
                    TimeInterval::new_unchecked(
                        NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0),
                        NaiveDate::from_ymd(2014, 3, 1).and_hms(0, 0, 0),
                    ),
                    TimeInterval::new_unchecked(
                        NaiveDate::from_ymd(2014, 2, 1).and_hms(0, 0, 0),
                        NaiveDate::from_ymd(2014, 3, 1).and_hms(0, 0, 0),
                    ),
                    TimeInterval::new_unchecked(
                        NaiveDate::from_ymd(2014, 2, 1).and_hms(0, 0, 0),
                        NaiveDate::from_ymd(2014, 3, 1).and_hms(0, 0, 0),
                    ),
                ],
                [
                    (
                        "id".to_string(),
                        FeatureData::Text(vec![
                            "S0".to_owned(),
                            "S1".to_owned(),
                            "S0".to_owned(),
                            "S0".to_owned(),
                        ]),
                    ),
                    (
                        "value".to_string(),
                        FeatureData::Float(vec![0., 2., 1., 1.]),
                    ),
                ]
                .iter()
                .cloned()
                .collect(),
            )
            .unwrap(),
        )
        .boxed();

        let exe_ctc = MockExecutionContext::default();

        let operator = FeatureAttributeValuesOverTime {
            params: FeatureAttributeValuesOverTimeParams {
                id_column: "id".to_owned(),
                value_column: "value".to_owned(),
            },
            sources: point_source.into(),
        };

        let operator = operator.boxed().initialize(&exe_ctc).await.unwrap();

        let query_processor = operator.query_processor().unwrap().json_vega().unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            PlotData {
                vega_string: r#"{"$schema":"https://vega.github.io/schema/vega-lite/v4.17.0.json","data":{"values":[{"x":"2014-01-01T00:00:00+00:00","y":0.0,"series":"S0"},{"x":"2014-02-01T00:00:00+00:00","y":1.0,"series":"S0"},{"x":"2014-02-01T00:00:00+00:00","y":1.0,"series":"S0"},{"x":"2014-01-01T00:00:00+00:00","y":2.0,"series":"S1"}]},"description":"Multi Line Chart","encoding":{"x":{"field":"x","title":"Time","type":"temporal"},"y":{"field":"y","title":"","type":"quantitative"},"color":{"field":"series","scale":{"scheme":"category20"}}},"mark":{"type":"line","line":true,"point":true}}"#.to_owned(),
                metadata: PlotMetaData::None,
            }
        );
    }
}
