use super::{ExpressionParameter, ExpressionParser};
use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedSources, InitializedVectorOperator, Operator,
    OperatorName, QueryContext, SingleVectorSource, TypedVectorQueryProcessor, VectorColumnInfo,
    VectorOperator, VectorQueryProcessor, VectorResultDescriptor, WorkflowOperatorPath,
};
use crate::processing::LinkedExpression;
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::collections::{
    FeatureCollection, FeatureCollectionInfos, FeatureCollectionModifications,
};
use geoengine_datatypes::primitives::{
    FeatureData, FeatureDataRef, FeatureDataType, Geometry, Measurement, MultiLineString,
    MultiPoint, MultiPolygon, NoGeometry, VectorQueryRectangle,
};
use geoengine_datatypes::util::arrow::ArrowTyped;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::collections::HashMap;
use std::sync::Arc;

/// A vector expression creates or replaces a column in a `FeatureCollection` by evaluating an expression.
/// The expression receives the feature's columns as variables.
pub type VectorExpression = Operator<VectorExpressionParams, SingleVectorSource>;

impl OperatorName for VectorExpression {
    const TYPE_NAME: &'static str = "VectorExpression";
}

const MAX_INPUT_COLUMNS: usize = 8;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct VectorExpressionParams {
    /// The columns to use as variables in the expression.
    pub input_columns: Vec<String>,

    /// The expression to evaluate.
    pub expression: String,

    // TODO: allow more types than `Float`s
    // pub output_type: VectorDataType,
    /// The name of the new column.
    pub output_column: String,

    /// The measurement of the new column.
    #[serde(default)]
    pub output_measurement: Measurement,
}

struct InitializedVectorExpression {
    name: CanonicOperatorName,
    result_descriptor: VectorResultDescriptor,
    features: Box<dyn InitializedVectorOperator>,
    expression: Arc<LinkedExpression>,
    input_columns: Vec<String>,
    output_column: String,
}

#[typetag::serde]
#[async_trait]
impl VectorOperator for VectorExpression {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        // TODO: This is super ugly to being forced to do this on every operator. This must be refactored.
        let name = CanonicOperatorName::from(&self);

        if self.params.input_columns.len() > MAX_INPUT_COLUMNS {
            Err(VectorExpressionError::TooManyInputColumns {
                max: MAX_INPUT_COLUMNS,
                found: self.params.input_columns.len(),
            })?;
        }

        let initialized_source = self.sources.initialize_sources(path, context).await?;

        // we can reuse the result descriptor, because we only add a column later on
        let mut result_descriptor = initialized_source.vector.result_descriptor().clone();

        check_input_column_validity(&result_descriptor.columns, &self.params.input_columns)?;

        insert_new_column(
            &mut result_descriptor.columns,
            self.params.output_column.clone(),
            self.params.output_measurement,
        )?;

        let expression = Arc::new(compile_expression(
            &self.params.expression,
            &self.params.input_columns,
        )?);

        let initialized_operator = InitializedVectorExpression {
            name,
            result_descriptor,
            features: initialized_source.vector,
            expression,
            input_columns: self.params.input_columns,
            output_column: self.params.output_column,
        };

        Ok(initialized_operator.boxed())
    }

    span_fn!(VectorExpression);
}

fn check_input_column_validity(
    columns: &HashMap<String, VectorColumnInfo>,
    input_columns: &[String],
) -> Result<(), VectorExpressionError> {
    for input_column in input_columns {
        let Some(column_info) = columns.get(input_column) else {
            return Err(VectorExpressionError::InputColumnNotExisting {
                name: input_column.clone(),
            });
        };

        match column_info.data_type {
            FeatureDataType::Float | FeatureDataType::Int => {}
            _ => Err(VectorExpressionError::InputColumnNotNumeric {
                name: input_column.clone(),
            })?,
        }
    }

    Ok(())
}

fn insert_new_column(
    columns: &mut HashMap<String, VectorColumnInfo>,
    name: String,
    measurement: Measurement,
) -> Result<(), VectorExpressionError> {
    let output_column_collision = columns.insert(
        name.clone(),
        VectorColumnInfo {
            data_type: FeatureDataType::Float,
            measurement,
        },
    );

    if output_column_collision.is_some() {
        return Err(VectorExpressionError::OutputColumnCollision { name });
    }

    Ok(())
}

fn compile_expression(expression_code: &str, parameters: &[String]) -> Result<LinkedExpression> {
    let parameters = parameters
        .iter()
        .map(|p| ExpressionParameter::Number(p.into()))
        .collect::<Vec<_>>();
    let expression = ExpressionParser::new(&parameters)?.parse(
        "expression", // TODO: generate and store a unique name
        expression_code,
    )?;

    Ok(LinkedExpression::new(&expression)?)
}

impl InitializedVectorOperator for InitializedVectorExpression {
    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        let source_processor = self.features.query_processor()?;

        Ok(match source_processor {
            TypedVectorQueryProcessor::Data(source) => {
                let processor: VectorExpressionProcessor<
                    Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<NoGeometry>>>,
                    NoGeometry,
                > = VectorExpressionProcessor {
                    source,
                    result_descriptor: self.result_descriptor.clone(),
                    expression: self.expression.clone(),
                    input_columns: self.input_columns.clone(),
                    output_column: self.output_column.clone(),
                };

                TypedVectorQueryProcessor::Data(processor.boxed())
            }
            TypedVectorQueryProcessor::MultiPoint(source) => {
                let processor: VectorExpressionProcessor<
                    Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<MultiPoint>>>,
                    MultiPoint,
                > = VectorExpressionProcessor {
                    source,
                    result_descriptor: self.result_descriptor.clone(),
                    expression: self.expression.clone(),
                    input_columns: self.input_columns.clone(),
                    output_column: self.output_column.clone(),
                };

                TypedVectorQueryProcessor::MultiPoint(processor.boxed())
            }
            TypedVectorQueryProcessor::MultiLineString(source) => {
                let processor: VectorExpressionProcessor<
                    Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<MultiLineString>>>,
                    MultiLineString,
                > = VectorExpressionProcessor {
                    source,
                    result_descriptor: self.result_descriptor.clone(),
                    expression: self.expression.clone(),
                    input_columns: self.input_columns.clone(),
                    output_column: self.output_column.clone(),
                };

                TypedVectorQueryProcessor::MultiLineString(processor.boxed())
            }
            TypedVectorQueryProcessor::MultiPolygon(source) => {
                let processor: VectorExpressionProcessor<
                    Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<MultiPolygon>>>,
                    MultiPolygon,
                > = VectorExpressionProcessor {
                    source,
                    result_descriptor: self.result_descriptor.clone(),
                    expression: self.expression.clone(),
                    input_columns: self.input_columns.clone(),
                    output_column: self.output_column.clone(),
                };

                TypedVectorQueryProcessor::MultiPolygon(processor.boxed())
            }
        })
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }
}

pub struct VectorExpressionProcessor<Q, G>
where
    G: Geometry + 'static + Sized,
    Q: VectorQueryProcessor<VectorType = FeatureCollection<G>> + 'static + Sized,
    // GR: GeometryRef<GeometryType = G>,
    // G: Geometry + 'static + Sized,
    // Q: VectorQueryProcessor<VectorType = FeatureCollection<G>> + Sized + 'static,
    // for<'a> &'a FeatureCollection<G>: IntoIterator<Item = FeatureCollectionRow<'a, GR>>,
    // GR: GeometryRef<GeometryType = G>,
    // GR: Sized + 'static,
    // Self: Sized + 'static,
{
    source: Q,
    result_descriptor: VectorResultDescriptor,
    expression: Arc<LinkedExpression>,
    input_columns: Vec<String>,
    output_column: String,
}

#[async_trait]
impl<Q, G> VectorQueryProcessor for VectorExpressionProcessor<Q, G>
where
    // G: Geometry + 'static + Sized + Sync + Send,
    // Q: VectorQueryProcessor<VectorType = FeatureCollection<G>> + 'static + Sized + Sync + Send,
    // FeatureCollection<G>: Sized + 'static,
    // for<'a> &'a FeatureCollection<G>:
    //     IntoIterator<Item = FeatureCollectionRow<'a, GR>> + Sized + Sync + Send,
    // // for<'a> <FeatureCollection<G> as IntoIterator>::Item: Sized,
    // GR: GeometryRef<GeometryType = G> + Sized + 'static + Sync + Send,
    // Self: Sized + 'static + Sync + Send,
    Q: VectorQueryProcessor<VectorType = FeatureCollection<G>>,
    G: Geometry + ArrowTyped + 'static,
    // for<'c> FeatureCollection<G>: IntoGeometryIterator<'c>
    //     + GeoFeatureCollectionModifications<G, Output = FeatureCollection<G>>,
    // for<'a> &'a FeatureCollection<G>: IntoIterator<Item = FeatureCollectionRow<'a, GR>>,
    // GR: GeometryRef<GeometryType = G>,
{
    type VectorType = FeatureCollection<G>;

    async fn vector_query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::VectorType>>> {
        let stream = self.source.vector_query(query, ctx).await?;

        let stream = stream.then(move |collection| async move {
            let collection = collection?;
            let input_columns = self.input_columns.clone();
            let output_column = self.output_column.clone();
            let expression = self.expression.clone();

            crate::util::spawn_blocking(move || {
                // TODO: parallelize

                let inputs: Vec<FeatureDataRef> = input_columns
                    .into_iter()
                    .map(|input_column| {
                        collection
                            .data(&input_column)
                            .expect("was checked durin initialization")
                    })
                    .collect();

                let result: Vec<Option<f64>> = {
                    let mut float_inputs: Vec<Box<dyn Iterator<Item = Option<f64>>>> = inputs
                        .iter()
                        .map(FeatureDataRef::float_options_iter)
                        .collect::<Vec<_>>();

                    match float_inputs.as_mut_slice() {
                        [a] => {
                            let f = unsafe {
                                expression.function_nary::<fn(Option<f64>) -> Option<f64>>()
                            }
                            .context(error::CallingExpression)?;

                            Ok(a.map(*f).collect::<Vec<_>>())
                        }
                        // TODO: implement more cases
                        other => Err(VectorExpressionError::TooManyInputColumns {
                            max: MAX_INPUT_COLUMNS,
                            found: other.len(),
                        }),
                    }?
                };

                Ok(collection
                    .add_column(&output_column, FeatureData::NullableFloat(result))
                    .context(error::AddColumn {
                        name: output_column,
                    })?)
            })
            .await?
        });

        Ok(stream.boxed())
    }

    fn vector_result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)), module(error))]
pub enum VectorExpressionError {
    #[snafu(display("Input column `{name}` does not exist."))]
    InputColumnNotExisting { name: String },

    #[snafu(display("Input column `{name}` is not numeric."))]
    InputColumnNotNumeric { name: String },

    #[snafu(display("Found {found} columns, but only up to {max} are allowed."))]
    TooManyInputColumns { max: usize, found: usize },

    #[snafu(display("Output column `{name}` already exists."))]
    OutputColumnCollision { name: String },

    #[snafu(display("Cannot call expression function."))]
    CallingExpression {
        source: crate::processing::expression::ExpressionError,
    },

    #[snafu(display("Cannot add column {name}."))]
    AddColumn {
        name: String,
        source: geoengine_datatypes::error::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        engine::{ChunkByteSize, MockExecutionContext, MockQueryContext, QueryProcessor},
        mock::MockFeatureCollectionSource,
    };
    use geoengine_datatypes::{
        collections::{ChunksEqualIgnoringCacheHint, MultiPointCollection},
        primitives::{BoundingBox2D, ColumnSelection, SpatialResolution, TimeInterval},
        util::test::TestDefault,
    };

    #[test]
    fn test_json() {
        let def: Operator<VectorExpressionParams, SingleVectorSource> = VectorExpression {
            params: VectorExpressionParams {
                input_columns: vec!["foo".into(), "bar".into()],
                expression: "foo + bar".into(),
                output_column: "baz".into(),
                output_measurement: Measurement::Unitless,
            },
            sources: MockFeatureCollectionSource::<MultiPoint>::multiple(vec![])
                .boxed()
                .into(),
        };

        let json = serde_json::json!({
            "params": {
                "input_columns": ["foo", "bar"],
                "expression": "foo + bar",
                "output_column": "baz",
                "output_measurement": {
                    "type": "unitless",
                },
            },
            "sources": {
                "vector": {
                    "type": "MockFeatureCollectionSourceMultiPoint",
                    "params": {
                        "collections": [],
                        "spatialReference": "EPSG:4326",
                        "measurements": null,
                    }
                }
            }
        });

        assert_eq!(serde_json::to_value(&def).unwrap(), json.clone());
        let _operator: VectorExpression = serde_json::from_value(json).unwrap();
    }

    #[tokio::test]
    async fn test_unary_float() {
        let points = MultiPointCollection::from_slices(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1), (2.0, 3.1)])
                .unwrap()
                .as_ref(),
            &[TimeInterval::new_unchecked(0, 1); 3],
            &[(
                "foo",
                FeatureData::NullableFloat(vec![Some(1.0), None, Some(3.0)]),
            )],
        )
        .unwrap();

        let point_source = MockFeatureCollectionSource::single(points.clone()).boxed();

        let operator = VectorExpression {
            params: VectorExpressionParams {
                input_columns: vec!["foo".into()],
                expression: "2 * foo".into(),
                output_column: "bar".into(),
                output_measurement: Measurement::Unitless,
            },
            sources: point_source.into(),
        }
        .boxed()
        .initialize(
            WorkflowOperatorPath::initialize_root(),
            &MockExecutionContext::test_default(),
        )
        .await
        .unwrap();

        let query_processor = operator.query_processor().unwrap().multi_point().unwrap();

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
            attributes: ColumnSelection::all(),
        };
        let ctx = MockQueryContext::new(ChunkByteSize::MAX);

        let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

        let mut result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);
        let result = result.remove(0);

        let expected_result = MultiPointCollection::from_slices(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1), (2.0, 3.1)])
                .unwrap()
                .as_ref(),
            &[TimeInterval::new_unchecked(0, 1); 3],
            &[
                (
                    "foo",
                    FeatureData::NullableFloat(vec![Some(1.0), None, Some(3.0)]),
                ),
                (
                    "bar",
                    FeatureData::NullableFloat(vec![Some(2.0), None, Some(6.0)]),
                ),
            ],
        )
        .unwrap();

        // TODO: maybe it is nicer to have something wrapping the actual data that we care about and just adds some cache info
        assert!(
            result.chunks_equal_ignoring_cache_hint(&expected_result),
            "{result:#?} != {expected_result:#?}",
        );
    }
}
