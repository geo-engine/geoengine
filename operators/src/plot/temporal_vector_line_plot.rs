use crate::error;
use crate::util::Result;
use crate::{
    engine::{
        ExecutionContext, InitializedOperator, InitializedOperatorImpl, InitializedPlotOperator,
        Operator, PlotOperator, PlotQueryProcessor, PlotResultDescriptor, QueryContext,
        QueryRectangle, TypedPlotQueryProcessor, VectorQueryProcessor,
    },
    error::Error,
};
use async_trait::async_trait;
use futures::StreamExt;
use geoengine_datatypes::primitives::{FeatureDataType, FeatureDataValue};
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
pub type FeatureAttributeValuesOverTime = Operator<FeatureAttributeValuesOverTimeParams>;

/// The parameter spec for `FeatureAttributeValuesOverTime`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureAttributeValuesOverTimeParams {
    pub id_column: String,
    pub value_column: String,
}

#[typetag::serde]
impl PlotOperator for FeatureAttributeValuesOverTime {
    fn initialize(
        mut self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<InitializedPlotOperator>> {
        ensure!(
            self.vector_sources.len() == 1,
            error::InvalidNumberOfVectorInputs {
                expected: 1..2,
                found: self.vector_sources.len()
            }
        );
        ensure!(
            self.raster_sources.is_empty(),
            error::InvalidNumberOfVectorInputs {
                expected: 0..1,
                found: self.raster_sources.len()
            }
        );

        let source = self.vector_sources.remove(0).initialize(context)?;
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

        ensure!(
            id_type == &FeatureDataType::Text || id_type == &FeatureDataType::Int,
            error::InvalidFeatureDataType,
        );

        ensure!(
            value_type != &FeatureDataType::Text,
            error::InvalidFeatureDataType,
        );

        Ok(InitializedFeatureAttributeValuesOverTime {
            result_descriptor: PlotResultDescriptor {},
            raster_sources: vec![],
            vector_sources: vec![source],
            state: self.params,
        }
        .boxed())
    }
}

/// The initialization of `FeatureAttributeValuesOverTime`
pub type InitializedFeatureAttributeValuesOverTime =
    InitializedOperatorImpl<PlotResultDescriptor, FeatureAttributeValuesOverTimeParams>;

impl InitializedOperator<PlotResultDescriptor, TypedPlotQueryProcessor>
    for InitializedFeatureAttributeValuesOverTime
{
    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        let input_processor = self.vector_sources[0].query_processor()?;

        let processor = call_on_generic_vector_processor!(input_processor, features => {
            FeatureAttributeValuesOverTimeQueryProcessor { params: self.state.clone(), features }.boxed()
        });

        Ok(TypedPlotQueryProcessor::JsonVega(processor))
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
        query: QueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<Self::OutputFormat> {
        let values = FeatureAttributeValues::<MAX_FEATURES>::default();

        let values = self
            .features
            .vector_query(query, ctx)?
            .fold(Ok(values), |acc, features| async {
                match (acc, features) {
                    (Ok(mut acc), Ok(features)) => {
                        let ids = features.data(&self.params.id_column)?;
                        let values = features.data(&self.params.value_column)?;

                        for i in 0..features.len() {
                            let id: String = match ids.get_unchecked(i) {
                                FeatureDataValue::Int(v) => v.to_string(),
                                FeatureDataValue::NullableInt(v) => v
                                    .map(|v| v.to_string())
                                    .ok_or(Error::FeatureDataValueMustNotBeNull)?,
                                FeatureDataValue::Float(v) => v.to_string(),
                                FeatureDataValue::NullableFloat(v) => v
                                    .map(|v| v.to_string())
                                    .ok_or(Error::FeatureDataValueMustNotBeNull)?,
                                FeatureDataValue::Text(v) => v.clone(),
                                FeatureDataValue::NullableText(v) => {
                                    v.ok_or(Error::FeatureDataValueMustNotBeNull)?
                                }
                                FeatureDataValue::Category(v) => v.to_string(),
                                FeatureDataValue::NullableCategory(v) => {
                                    v.map(|v| v.to_string())
                                        .ok_or(Error::FeatureDataValueMustNotBeNull)?
                                }
                            };

                            let value: f64 = match values.get_unchecked(i) {
                                FeatureDataValue::Int(v) => v as f64,
                                FeatureDataValue::NullableInt(v) => v.map_or(0.0, |v| v as f64), // TODO: NAN better default?
                                FeatureDataValue::Float(v) => v,
                                FeatureDataValue::NullableFloat(v) => v.unwrap_or(0.0),
                                FeatureDataValue::Category(v) => f64::from(v),
                                FeatureDataValue::NullableCategory(v) => v.map_or(0.0, f64::from),
                                _ => return Err(Error::InvalidFeatureDataType),
                            };

                            let time = features.time_intervals()[i];

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

#[derive(Default)]
struct FeatureAttributeValues<const LENGTH: usize> {
    values: HashMap<String, Vec<TemporalValue>>,
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

        data.sort_by(|a, b| match a.series.cmp(&b.series) {
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
        engine::{MockExecutionContext, MockQueryContext, VectorOperator},
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
            raster_sources: vec![],
            vector_sources: vec![point_source],
        };

        let operator = operator.boxed().initialize(&exe_ctc).unwrap();

        let query_processor = operator.query_processor().unwrap().json_vega().unwrap();

        let result = query_processor
            .plot_query(
                QueryRectangle {
                    bbox: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::new(0.1, 0.1).unwrap(),
                },
                &MockQueryContext::new(0),
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
}
