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
use geoengine_datatypes::collections::{FeatureCollection, FeatureCollectionRow};
use geoengine_datatypes::primitives::{
    FeatureDataType, Geometry, GeometryRef, Measurement, MultiPoint, MultiPointRef,
    VectorQueryRectangle,
};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
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
            })?
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

        Ok(
            call_on_generic_vector_processor!(source_processor, source => VectorExpressionProcessor {
                source,
                result_descriptor: self.result_descriptor.clone(),
                expression: self.expression.clone(),
            }.boxed().into()),
            // match source_processor {
            //     TypedVectorQueryProcessor::Data(source) => todo!(),
            //     TypedVectorQueryProcessor::MultiPoint(source) => {
            //         let processor: VectorExpressionProcessor<
            //             Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<MultiPoint>>>,
            //             MultiPoint,
            //             // MultiPointRef,
            //         > = VectorExpressionProcessor {
            //             source,
            //             result_descriptor: self.result_descriptor.clone(),
            //             expression: self.expression.clone(),
            //         };

            //         // processor.boxed()

            //         let boxed_processor =
            //             Box::<dyn VectorQueryProcessor<VectorType = MultiPoint>>::new(processor);

            //         TypedVectorQueryProcessor::MultiPoint(boxed_processor)
            //     }
            //     TypedVectorQueryProcessor::MultiLineString(source) => todo!(),
            //     TypedVectorQueryProcessor::MultiPolygon(source) => todo!(),
            // },
        )
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
    // for<'a> &'a FeatureCollection<G>: IntoIterator<Item = FeatureCollectionRow<'a, GR>> + Sized,
    // GR: GeometryRef<GeometryType = G>,
    // GR: Sized + 'static,
    // Self: Sized + 'static,
{
    source: Q,
    result_descriptor: VectorResultDescriptor,
    expression: Arc<LinkedExpression>,
}

#[async_trait]
impl<Q, G, GR> VectorQueryProcessor for VectorExpressionProcessor<Q, G>
where
    G: Geometry + 'static + Sized,
    Q: VectorQueryProcessor<VectorType = FeatureCollection<G>> + Sized + 'static,
    for<'a> &'a FeatureCollection<G>: IntoIterator<Item = FeatureCollectionRow<'a, GR>> + Sized,
    for<'a> <FeatureCollection<G> as IntoIterator>::Item: Sized,
    GR: GeometryRef<GeometryType = G> + Sized + 'static,
    // GR: Sized + 'static,
    // Self: Sized + 'static,
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

            crate::util::spawn_blocking(move || {
                // TODO: parallelize

                // for feature in collection.into_iter() {
                //     dbg!(feature.index());
                // }

                Ok(collection)
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
}
