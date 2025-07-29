use super::{
    AsExpressionGeo, FromExpressionGeo, VectorExpressionError, canonicalize_name,
    error::vector as error, get_expression_dependencies,
};
use crate::{
    engine::{
        CanonicOperatorName, ExecutionContext, InitializedSources, InitializedVectorOperator,
        Operator, OperatorName, QueryContext, SingleVectorSource, TypedVectorQueryProcessor,
        VectorColumnInfo, VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
        WorkflowOperatorPath,
    },
    util::Result,
};
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use geoengine_datatypes::primitives::{
    FeatureData, FeatureDataRef, FeatureDataType, FloatOptionsParIter, Geometry, Measurement,
    MultiLineString, MultiPoint, MultiPolygon, VectorQueryRectangle,
};
use geoengine_datatypes::util::arrow::ArrowTyped;
use geoengine_datatypes::{
    collections::{
        FeatureCollection, FeatureCollectionInfos, FeatureCollectionModifications,
        GeoFeatureCollectionModifications, GeoVectorDataType, IntoGeometryOptionsIterator,
        VectorDataType,
    },
    primitives::NoGeometry,
};
use geoengine_expression::{
    DataType, ExpressionParser, LinkedExpression, Parameter as ExpressionParameter,
    is_allowed_variable_name,
};
use rayon::iter::{
    FromParallelIterator, IndexedParallelIterator, IntoParallelIterator, ParallelIterator,
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

/// A vector expression creates or replaces a column in a `FeatureCollection` by evaluating an expression.
/// The expression receives the feature's columns as variables.
pub type VectorExpression = Operator<VectorExpressionParams, SingleVectorSource>;

impl OperatorName for VectorExpression {
    const TYPE_NAME: &'static str = "VectorExpression";
}

const MAX_INPUT_COLUMNS: usize = 8;
const PARALLEL_MIN_BATCH_SIZE: usize = 32; // TODO: find good default
const EXPRESSION_MAIN_NAME: &str = "expression";

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorExpressionParams {
    /// The columns to use as variables in the expression.
    ///
    /// For usage in the expression, all special characters are replaced by underscores.
    /// E.g., `precipitation.cm` becomes `precipitation_cm`.
    ///
    /// If the column name starts with a number, an underscore is prepended.
    /// E.g., `1column` becomes `_1column`.
    ///
    pub input_columns: Vec<String>,

    /// The expression to evaluate.
    pub expression: String,

    /// The type and name of the new column.
    pub output_column: OutputColumn,

    /// The expression will always include the geometry column.
    /// Thus, it is necessary to specify the variable name of the geometry column.
    /// The default is `geom`.
    #[serde(default = "geometry_default_column_name")]
    pub geometry_column_name: String,

    /// The measurement of the new column.
    /// The default is [`Measurement::Unitless`].
    #[serde(default)]
    pub output_measurement: Measurement,
}

fn geometry_default_column_name() -> String {
    "geom".into()
}

/// Specify the output of the expression.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "camelCase")]
pub enum OutputColumn {
    /// The expression will override the current geometry
    Geometry(GeoVectorDataType),
    /// The expression will append a new `Float` column
    // TODO: allow more types than `Float`s
    Column(String),
}

struct InitializedVectorExpression {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    result_descriptor: VectorResultDescriptor,
    features: Box<dyn InitializedVectorOperator>,
    expression: Arc<LinkedExpression>,
    input_columns: Vec<String>,
    output_column: OutputColumn,
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

        let initialized_source = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?;

        // we can reuse the result descriptor, because we only add a column later on
        let mut result_descriptor = initialized_source.vector.result_descriptor().clone();

        check_input_column_validity(&result_descriptor.columns, &self.params.input_columns)?;
        check_output_column_validity(&self.params.output_column)?;

        let expression_geom_input_type = result_descriptor.data_type;
        let expression_output_type = match &self.params.output_column {
            OutputColumn::Geometry(vector_data_type) => {
                result_descriptor.data_type = (*vector_data_type).into();
                match vector_data_type {
                    GeoVectorDataType::MultiPoint => DataType::MultiPoint,
                    GeoVectorDataType::MultiLineString => DataType::MultiLineString,
                    GeoVectorDataType::MultiPolygon => DataType::MultiPolygon,
                }
            }
            OutputColumn::Column(output_column_name) => {
                insert_new_column(
                    &mut result_descriptor.columns,
                    output_column_name.clone(),
                    self.params.output_measurement,
                )?;
                DataType::Number
            }
        };

        let mut expression_input_names = Vec::with_capacity(self.params.input_columns.len());
        for input_column in &self.params.input_columns {
            let variable_name = canonicalize_name(input_column);

            if !is_allowed_variable_name(&variable_name) {
                return Err(VectorExpressionError::ColumnNameContainsSpecialCharacters {
                    name: variable_name,
                })?;
            }

            expression_input_names.push(variable_name);
        }

        let expression = {
            let expression_code = self.params.expression.clone();
            let geometry_column_name = self.params.geometry_column_name.clone();

            crate::util::spawn_blocking(move || {
                compile_expression(
                    &expression_code,
                    geometry_column_name,
                    expression_geom_input_type,
                    &expression_input_names,
                    expression_output_type,
                )
                .map(Arc::new)
            })
            .await
            .map_err(|source| VectorExpressionError::CompilationTask { source })??
        };

        let initialized_operator = InitializedVectorExpression {
            name,
            path,
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
        if input_column.contains(|c: char| !c.is_alphanumeric()) {
            Err(VectorExpressionError::ColumnNameContainsSpecialCharacters {
                name: input_column.clone(),
            })?;
        }

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

fn check_output_column_validity(output_column: &OutputColumn) -> Result<(), VectorExpressionError> {
    match output_column {
        OutputColumn::Geometry(_) => {}
        OutputColumn::Column(column) => {
            if column.contains(|c: char| !c.is_alphanumeric()) {
                Err(VectorExpressionError::ColumnNameContainsSpecialCharacters {
                    name: column.clone(),
                })?;
            }
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

fn compile_expression(
    expression_code: &str,
    geom_name: String,
    geom_type: VectorDataType,
    parameters: &[String],
    output_type: DataType,
) -> Result<LinkedExpression, VectorExpressionError> {
    let geom_parameter = match geom_type {
        VectorDataType::Data | VectorDataType::MultiPoint => {
            ExpressionParameter::MultiPoint(geom_name.into())
        }
        VectorDataType::MultiLineString => ExpressionParameter::MultiLineString(geom_name.into()),
        VectorDataType::MultiPolygon => ExpressionParameter::MultiPolygon(geom_name.into()),
    };
    let mut expression_parameters = Vec::with_capacity(parameters.len() + 1);
    expression_parameters.push(geom_parameter);
    expression_parameters.extend(
        parameters
            .iter()
            .map(|p| ExpressionParameter::Number(p.into())),
    );
    let expression = ExpressionParser::new(&expression_parameters, output_type)?
        .parse(EXPRESSION_MAIN_NAME, expression_code)?;

    let expression_dependencies = get_expression_dependencies().context(error::Dependencies)?;

    Ok(LinkedExpression::from_ast(
        &expression,
        expression_dependencies,
    )?)
}

impl InitializedVectorExpression {
    #[inline]
    fn column_processor<G>(
        &self,
        source: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
        output_column: String,
    ) -> TypedVectorQueryProcessor
    where
        G: Geometry + ArrowTyped + 'static,
        FeatureCollection<G>: for<'g> IntoGeometryOptionsIterator<'g> + 'static,
        for<'g> <FeatureCollection<G> as IntoGeometryOptionsIterator<'g>>::GeometryType:
            AsExpressionGeo + Send,
        for<'g> <<FeatureCollection<G> as IntoGeometryOptionsIterator<'g>>::GeometryOptionIterator as IntoParallelIterator>::Iter:
        IndexedParallelIterator + Send,
        TypedVectorQueryProcessor: From<Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>>,
    {
        VectorExpressionColumnProcessor {
            source,
            result_descriptor: self.result_descriptor.clone(),
            expression: self.expression.clone(),
            input_columns: self.input_columns.clone(),
            output_column,
        }
        .boxed()
        .into()
    }

    #[inline]
    fn geometry_processor<GIn, GOut>(
        &self,
        source: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<GIn>>>,
    ) -> TypedVectorQueryProcessor
    where
        GIn: Geometry + ArrowTyped + Send + Sync + 'static + Sized,
        GOut: Geometry
            + ArrowTyped
            + FromExpressionGeo
            + Send
            + Sync
            + 'static
            + Sized,
        FeatureCollection<GIn>: GeoFeatureCollectionModifications<GOut> + for<'g> IntoGeometryOptionsIterator<'g>,
        for<'g> <<FeatureCollection<GIn> as IntoGeometryOptionsIterator<'g>>::GeometryOptionIterator as IntoParallelIterator>::Iter:
            IndexedParallelIterator + Send,
        for<'g> <FeatureCollection<GIn> as IntoGeometryOptionsIterator<'g>>::GeometryType: AsExpressionGeo,
        TypedVectorQueryProcessor: From<Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<GOut>>>>,
    {
        VectorExpressionGeometryProcessor {
            source,
            result_descriptor: self.result_descriptor.clone(),
            expression: self.expression.clone(),
            input_columns: self.input_columns.clone(),
            _out: PhantomData::<GOut>,
        }
        .boxed()
        .into()
    }

    #[inline]
    fn dispatch_float_column_output(
        &self,
        source_processor: TypedVectorQueryProcessor,
        output_column: String,
    ) -> TypedVectorQueryProcessor {
        match source_processor {
            TypedVectorQueryProcessor::Data(source) => {
                self.column_processor::<NoGeometry>(source, output_column)
            }
            TypedVectorQueryProcessor::MultiPoint(source) => {
                self.column_processor::<MultiPoint>(source, output_column)
            }
            TypedVectorQueryProcessor::MultiLineString(source) => {
                self.column_processor::<MultiLineString>(source, output_column)
            }
            TypedVectorQueryProcessor::MultiPolygon(source) => {
                self.column_processor::<MultiPolygon>(source, output_column)
            }
        }
    }

    #[inline]
    fn dispatch_geometry_output_for_type<GOut>(
        &self,
        source_processor: TypedVectorQueryProcessor,
    ) -> TypedVectorQueryProcessor
    where
        GOut: Geometry + ArrowTyped + FromExpressionGeo + Send + Sync + 'static + Sized,
        TypedVectorQueryProcessor:
            From<Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<GOut>>>>,
    {
        match source_processor {
            TypedVectorQueryProcessor::Data(source) => {
                self.geometry_processor::<NoGeometry, GOut>(source)
            }
            TypedVectorQueryProcessor::MultiPoint(source) => {
                self.geometry_processor::<MultiPoint, GOut>(source)
            }
            TypedVectorQueryProcessor::MultiLineString(source) => {
                self.geometry_processor::<MultiLineString, GOut>(source)
            }
            TypedVectorQueryProcessor::MultiPolygon(source) => {
                self.geometry_processor::<MultiPolygon, GOut>(source)
            }
        }
    }

    #[inline]
    fn dispatch_geometry_output(
        &self,
        source_processor: TypedVectorQueryProcessor,
        vector_data_type: GeoVectorDataType,
    ) -> TypedVectorQueryProcessor {
        match vector_data_type {
            GeoVectorDataType::MultiPoint => {
                self.dispatch_geometry_output_for_type::<MultiPoint>(source_processor)
            }
            GeoVectorDataType::MultiLineString => {
                self.dispatch_geometry_output_for_type::<MultiLineString>(source_processor)
            }
            GeoVectorDataType::MultiPolygon => {
                self.dispatch_geometry_output_for_type::<MultiPolygon>(source_processor)
            }
        }
    }
}

impl InitializedVectorOperator for InitializedVectorExpression {
    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        let source_processor = self.features.query_processor()?;

        Ok(match self.output_column.clone() {
            OutputColumn::Geometry(vector_data_type) => {
                self.dispatch_geometry_output(source_processor, vector_data_type)
            }
            OutputColumn::Column(output_column) => {
                self.dispatch_float_column_output(source_processor, output_column)
            }
        })
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        VectorExpression::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }
}

/// A processor that evaluates an expression on the columns of a `FeatureCollection`.
/// The result is a new `FeatureCollection` with the evaluated column added.
pub struct VectorExpressionColumnProcessor<Q, G>
where
    G: Geometry,
    Q: VectorQueryProcessor<VectorType = FeatureCollection<G>>,
{
    source: Q,
    result_descriptor: VectorResultDescriptor,
    expression: Arc<LinkedExpression>,
    input_columns: Vec<String>,
    output_column: String,
}

/// A processor that evaluates an expression on the columns of a `FeatureCollection`.
/// The result is a new `FeatureCollection` with a replaced geometry column.
pub struct VectorExpressionGeometryProcessor<Q, GIn, GOut>
where
    GIn: Geometry,
    GOut: Geometry,
    Q: VectorQueryProcessor<VectorType = FeatureCollection<GIn>>,
{
    source: Q,
    result_descriptor: VectorResultDescriptor,
    expression: Arc<LinkedExpression>,
    input_columns: Vec<String>,
    _out: PhantomData<GOut>,
}

type ExpressionGeometryType<'g, G> = <<FeatureCollection<G> as IntoGeometryOptionsIterator<'g>>::GeometryType as AsExpressionGeo>::ExpressionGeometryType;

#[async_trait]
impl<Q, G> VectorQueryProcessor for VectorExpressionColumnProcessor<Q, G>
where
    Q: VectorQueryProcessor<VectorType = FeatureCollection<G>>,
    G: Geometry + ArrowTyped + 'static,
    FeatureCollection<G>: for<'g> IntoGeometryOptionsIterator<'g> + 'static,
    for<'g> <FeatureCollection<G> as IntoGeometryOptionsIterator<'g>>::GeometryType:
        AsExpressionGeo + Send,
    for<'g> <<FeatureCollection<G> as IntoGeometryOptionsIterator<'g>>::GeometryOptionIterator as IntoParallelIterator>::Iter:
        IndexedParallelIterator + Send,
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

            crate::util::spawn_blocking_with_thread_pool(ctx.thread_pool().clone(), move || {
                let result: Vec<Option<f64>> = call_expression_function(
                    &expression,
                    &collection,
                    &input_columns,
                    std::convert::identity,
                )?;

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

#[async_trait]
impl<Q, GIn, GOut> VectorQueryProcessor
    for VectorExpressionGeometryProcessor<Q, GIn, GOut>
where
    Q: VectorQueryProcessor<VectorType = FeatureCollection<GIn>> + 'static + Sized,
    GIn: Geometry + ArrowTyped + Send + Sync + 'static + Sized,
    GOut: Geometry
        + ArrowTyped
        + FromExpressionGeo
        + Send
        + Sync
        + 'static
        + Sized,
    FeatureCollection<GIn>: GeoFeatureCollectionModifications<GOut> + for<'g> IntoGeometryOptionsIterator<'g>,
    for<'g> <<FeatureCollection<GIn> as IntoGeometryOptionsIterator<'g>>::GeometryOptionIterator as IntoParallelIterator>::Iter:
        IndexedParallelIterator + Send,
    for<'g> <FeatureCollection<GIn> as IntoGeometryOptionsIterator<'g>>::GeometryType: AsExpressionGeo,
    Vec<Option<GOut>>: FromParallelIterator<Option<GOut>>,
{
    type VectorType = FeatureCollection<GOut>;

    async fn vector_query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::VectorType>>> {
        let stream = self.source.vector_query(query, ctx).await?;

        let stream = stream.then(move |collection| async move {
            let collection = collection?;
            let input_columns = self.input_columns.clone();
            let expression = self.expression.clone();

            crate::util::spawn_blocking_with_thread_pool(ctx.thread_pool().clone(), move || {
                let (geometry_options, row_filter): (Vec<Option<GOut>>, Vec<bool>) = call_expression_function(
                    &expression,
                    &collection,
                    &input_columns,
                    |geom_option| {
                        let geom_option = geom_option.and_then(<GOut as FromExpressionGeo>::from_expression_geo);

                        let row_filter = geom_option.is_some();

                        (geom_option, row_filter)
                    },
                )?;

                // remove all `None`s and output only the geometries
                let geometries = geometry_options.into_par_iter().with_min_len(PARALLEL_MIN_BATCH_SIZE).filter_map(std::convert::identity).collect::<Vec<_>>();

                Ok(collection
                    .filter(row_filter) // we have to filter out the rows with empty geometries
                    .context(error::FilterEmptyGeometries)?
                    .replace_geometries(geometries)
                    .context(error::ReplaceGeometries)?)
            })
            .await?
        });

        Ok(stream.boxed())
    }

    fn vector_result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }
}

fn call_expression_function<GIn, ExprOut, MapOut, Out>(
    expression: &Arc<LinkedExpression>,
    collection: &FeatureCollection<GIn>,
    input_columns: &[String],
    map_fn: fn(Option<ExprOut>) -> MapOut,
) -> Result<Out, VectorExpressionError>
where
    GIn: Geometry + ArrowTyped + 'static,
    for<'i> FeatureCollection<GIn>: IntoGeometryOptionsIterator<'i>,
    for<'g> <<FeatureCollection<GIn> as IntoGeometryOptionsIterator<'g>>::GeometryOptionIterator as IntoParallelIterator>::Iter:
        IndexedParallelIterator + Send,
    for<'g> <FeatureCollection<GIn> as IntoGeometryOptionsIterator<'g>>::GeometryType: AsExpressionGeo,
    ExprOut: Send,
    MapOut: Send,
    Out: FromParallelIterator<MapOut> + Send,
{
    let data_columns: Vec<FeatureDataRef> = input_columns
        .iter()
        .map(|input_column| {
            collection
                .data(input_column)
                .expect("was checked durin initialization")
        })
        .collect();

    let float_inputs: Vec<FloatOptionsParIter> = data_columns
        .iter()
        .map(FeatureDataRef::float_options_par_iter)
        .collect::<Vec<_>>();

    let geom_input = collection
        .geometry_options()
        .into_par_iter()
        .map(|geometry_option| {
            if let Some(geometry) = geometry_option.as_ref() {
                geometry.as_expression_geo()
            } else {
                None
            }
        });

    macro_rules! impl_expression_subcall {
        ($n:literal, $($i:ident),*) => {
            {
                let [ $($i),* ] = <[_; $n]>::try_from(float_inputs).expect("it matches the match condition");
                let f = unsafe {
                    expression.function_nary::<fn(
                        Option<ExpressionGeometryType<'_, GIn>>,
                        $( impl_expression_subcall!(@float_option $i), )*
                    ) -> Option<ExprOut>>()
                }
                .map_err(VectorExpressionError::from)?;

                (geom_input, $($i),*)
                    .into_par_iter()
                    .with_min_len(PARALLEL_MIN_BATCH_SIZE)
                    .map(|(geom, $($i),*)| map_fn(f(geom, $($i),*)))
                    .collect()
            }
        };
        // Create one float option for each float input
        (@float_option $i:ident) => {
            Option<f64>
        };
    }

    Ok(match float_inputs.len() {
        0 => {
            let f = unsafe {
                expression.function_nary::<fn(
                    Option<ExpressionGeometryType<'_, GIn>>,
                ) -> Option<ExprOut>>()
            }
            .map_err(VectorExpressionError::from)?;

            geom_input
                .with_min_len(PARALLEL_MIN_BATCH_SIZE)
                .map(|geom| map_fn(f(geom)))
                .collect()
        }
        1 => impl_expression_subcall!(1, i1),
        2 => impl_expression_subcall!(2, i1, i2),
        3 => impl_expression_subcall!(3, i1, i2, i3),
        4 => impl_expression_subcall!(4, i1, i2, i3, i4),
        5 => impl_expression_subcall!(5, i1, i2, i3, i4, i5),
        6 => impl_expression_subcall!(6, i1, i2, i3, i4, i5, i6),
        7 => impl_expression_subcall!(7, i1, i2, i3, i4, i5, i6, i7),
        8 => impl_expression_subcall!(8, i1, i2, i3, i4, i5, i6, i7, i8),
        other => Err(VectorExpressionError::TooManyInputColumns {
            max: MAX_INPUT_COLUMNS,
            found: other,
        })?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        engine::{ChunkByteSize, MockExecutionContext, QueryProcessor},
        mock::MockFeatureCollectionSource,
    };
    use geoengine_datatypes::{
        collections::{
            ChunksEqualIgnoringCacheHint, IntoGeometryIterator, MultiPointCollection,
            MultiPolygonCollection,
        },
        primitives::{BoundingBox2D, ColumnSelection, MultiPoint, MultiPolygon, TimeInterval},
        util::test::TestDefault,
    };

    #[test]
    fn it_deserializes_the_operator() {
        let def: Operator<VectorExpressionParams, SingleVectorSource> = VectorExpression {
            params: VectorExpressionParams {
                input_columns: vec!["foo".into(), "bar".into()],
                expression: "foo + bar".into(),
                output_column: OutputColumn::Column("baz".into()),
                output_measurement: Measurement::Unitless,
                geometry_column_name: "geom".to_string(),
            },
            sources: MockFeatureCollectionSource::<MultiPoint>::multiple(vec![])
                .boxed()
                .into(),
        };

        let json = serde_json::json!({
            "params": {
                "inputColumns": ["foo", "bar"],
                "expression": "foo + bar",
                "outputColumn": {
                    "type": "column",
                    "value": "baz",
                },
                "outputMeasurement": {
                    "type": "unitless",
                },
                "geometryColumnName": "geom",
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
    async fn it_computes_unary_float_expressions() {
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

        let exe_ctx = MockExecutionContext::test_default();

        let operator = VectorExpression {
            params: VectorExpressionParams {
                input_columns: vec!["foo".into()],
                expression: "2 * foo".into(),
                output_column: OutputColumn::Column("bar".into()),
                output_measurement: Measurement::Unitless,
                geometry_column_name: "geom".to_string(),
            },
            sources: point_source.into(),
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap();

        let query_processor = operator.query_processor().unwrap().multi_point().unwrap();

        let query_rectangle = VectorQueryRectangle::new(
            BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
            TimeInterval::default(),
            ColumnSelection::all(),
        );
        let ctx = exe_ctx.mock_query_context(ChunkByteSize::MAX);

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

    #[tokio::test]
    async fn it_computes_binary_float_expressions() {
        let points = MultiPointCollection::from_slices(
            MultiPoint::many(vec![
                (0.0, 0.1),
                (1.0, 1.1),
                (2.0, 2.1),
                (3.0, 3.1),
                (4.0, 4.1),
            ])
            .unwrap()
            .as_ref(),
            &[TimeInterval::new_unchecked(0, 1); 5],
            &[
                (
                    "foo",
                    FeatureData::NullableFloat(vec![Some(1.0), None, Some(3.0), None, Some(5.0)]),
                ),
                (
                    "bar",
                    FeatureData::NullableInt(vec![Some(10), None, None, Some(40), Some(50)]),
                ),
            ],
        )
        .unwrap();

        let point_source = MockFeatureCollectionSource::single(points.clone()).boxed();
        let exe_ctx = MockExecutionContext::test_default();

        let operator = VectorExpression {
            params: VectorExpressionParams {
                input_columns: vec!["foo".into(), "bar".into()],
                expression: "foo + bar".into(),
                output_column: OutputColumn::Column("baz".into()),
                output_measurement: Measurement::Unitless,
                geometry_column_name: "geom".to_string(),
            },
            sources: point_source.into(),
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap();

        let query_processor = operator.query_processor().unwrap().multi_point().unwrap();

        let query_rectangle = VectorQueryRectangle::new(
            BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
            TimeInterval::default(),
            ColumnSelection::all(),
        );
        let ctx = exe_ctx.mock_query_context(ChunkByteSize::MAX);

        let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

        let mut result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);
        let result = result.remove(0);

        let expected_result = MultiPointCollection::from_slices(
            MultiPoint::many(vec![
                (0.0, 0.1),
                (1.0, 1.1),
                (2.0, 2.1),
                (3.0, 3.1),
                (4.0, 4.1),
            ])
            .unwrap()
            .as_ref(),
            &[TimeInterval::new_unchecked(0, 1); 5],
            &[
                (
                    "foo",
                    FeatureData::NullableFloat(vec![Some(1.0), None, Some(3.0), None, Some(5.0)]),
                ),
                (
                    "bar",
                    FeatureData::NullableInt(vec![Some(10), None, None, Some(40), Some(50)]),
                ),
                (
                    "baz",
                    FeatureData::NullableFloat(vec![Some(11.0), None, None, None, Some(55.0)]),
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

    #[tokio::test]
    async fn it_computes_the_area_from_a_geom() {
        let polygons = MockFeatureCollectionSource::single(
            MultiPolygonCollection::from_slices(
                &[
                    MultiPolygon::new(vec![vec![vec![
                        (0.5, -0.5).into(),
                        (4., -1.).into(),
                        (0.5, -2.5).into(),
                        (0.5, -0.5).into(),
                    ]]])
                    .unwrap(),
                    MultiPolygon::new(vec![vec![vec![
                        (1.0, -1.0).into(),
                        (8., -2.).into(),
                        (1.0, -5.0).into(),
                        (1.0, -1.0).into(),
                    ]]])
                    .unwrap(),
                ],
                &[TimeInterval::new_unchecked(0, 1); 2],
                &[("foo", FeatureData::NullableFloat(vec![Some(1.0), None]))],
            )
            .unwrap(),
        )
        .boxed();

        let result = compute_result::<MultiPolygonCollection>(
            VectorExpression {
                params: VectorExpressionParams {
                    input_columns: vec![],
                    expression: "area(geom)".into(),
                    output_column: OutputColumn::Column("area".into()),
                    output_measurement: Measurement::Unitless,
                    geometry_column_name: "geom".to_string(),
                },
                sources: polygons.into(),
            },
            VectorQueryRectangle::new(
                BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
                TimeInterval::default(),
                ColumnSelection::all(),
            ),
        )
        .await;

        let expected_result = MultiPolygonCollection::from_slices(
            &[
                MultiPolygon::new(vec![vec![vec![
                    (0.5, -0.5).into(),
                    (4., -1.).into(),
                    (0.5, -2.5).into(),
                    (0.5, -0.5).into(),
                ]]])
                .unwrap(),
                MultiPolygon::new(vec![vec![vec![
                    (1.0, -1.0).into(),
                    (8., -2.).into(),
                    (1.0, -5.0).into(),
                    (1.0, -1.0).into(),
                ]]])
                .unwrap(),
            ],
            &[TimeInterval::new_unchecked(0, 1); 2],
            &[
                ("foo", FeatureData::NullableFloat(vec![Some(1.0), None])),
                (
                    "area",
                    FeatureData::NullableFloat(vec![Some(3.5), Some(14.0)]),
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

    #[tokio::test]
    async fn it_computes_the_centroid_of_a_geom() {
        let polygons = MockFeatureCollectionSource::single(
            MultiPolygonCollection::from_slices(
                &[
                    MultiPolygon::new(vec![vec![vec![
                        (0., 0.).into(),
                        (5., 0.).into(),
                        (5., 6.).into(),
                        (0., 6.).into(),
                        (0., 0.).into(),
                    ]]])
                    .unwrap(),
                    MultiPolygon::new(vec![vec![vec![
                        (0.5, -0.5).into(),
                        (4., -1.).into(),
                        (0.5, -2.5).into(),
                        (0.5, -0.5).into(),
                    ]]])
                    .unwrap(),
                ],
                &[TimeInterval::new_unchecked(0, 1); 2],
                &[("foo", FeatureData::NullableFloat(vec![Some(1.0), None]))],
            )
            .unwrap(),
        )
        .boxed();

        let result = compute_result::<MultiPointCollection>(
            VectorExpression {
                params: VectorExpressionParams {
                    input_columns: vec![],
                    expression: "centroid(geom)".into(),
                    output_column: OutputColumn::Geometry(GeoVectorDataType::MultiPoint),
                    output_measurement: Measurement::Unitless,
                    geometry_column_name: "geom".to_string(),
                },
                sources: polygons.into(),
            },
            VectorQueryRectangle::new(
                BoundingBox2D::new((0., 0.).into(), (10., 10.).into()).unwrap(),
                TimeInterval::default(),
                ColumnSelection::all(),
            ),
        )
        .await;

        let expected_result = MultiPointCollection::from_slices(
            MultiPoint::many(vec![
                (2.5, 3.0),
                (1.666_666_666_666_666_7, -1.333_333_333_333_333_5),
            ])
            .unwrap()
            .as_ref(),
            &[TimeInterval::new_unchecked(0, 1); 2],
            &[("foo", FeatureData::NullableFloat(vec![Some(1.0), None]))],
        )
        .unwrap();

        // TODO: maybe it is nicer to have something wrapping the actual data that we care about and just adds some cache info
        assert!(
            result.chunks_equal_ignoring_cache_hint(&expected_result),
            "{result_geometries:#?} != {expected_result_geometries:#?}",
            result_geometries = result.geometries().collect::<Vec<_>>(),
            expected_result_geometries = expected_result.geometries().collect::<Vec<_>>(),
        );
    }

    #[tokio::test]
    async fn it_computes_eight_larger_inputs() {
        const NUMBER_OF_ROWS: usize = 100;
        let points = MultiPointCollection::from_slices(
            MultiPoint::many((0..NUMBER_OF_ROWS).map(|i| (i as f64, i as f64)).collect())
                .unwrap()
                .as_ref(),
            &[TimeInterval::new_unchecked(0, 1); NUMBER_OF_ROWS],
            &[
                (
                    "f1",
                    FeatureData::Float((0..NUMBER_OF_ROWS).map(|i| i as f64).collect()),
                ),
                (
                    "f2",
                    FeatureData::Float((0..NUMBER_OF_ROWS).map(|i| i as f64).collect()),
                ),
                (
                    "f3",
                    FeatureData::Float((0..NUMBER_OF_ROWS).map(|i| i as f64).collect()),
                ),
                (
                    "f4",
                    FeatureData::Float((0..NUMBER_OF_ROWS).map(|i| i as f64).collect()),
                ),
                (
                    "f5",
                    FeatureData::Float((0..NUMBER_OF_ROWS).map(|i| i as f64).collect()),
                ),
                (
                    "f6",
                    FeatureData::Float((0..NUMBER_OF_ROWS).map(|i| i as f64).collect()),
                ),
                (
                    "f7",
                    FeatureData::Float((0..NUMBER_OF_ROWS).map(|i| i as f64).collect()),
                ),
                (
                    "f8",
                    FeatureData::Float((0..NUMBER_OF_ROWS).map(|i| i as f64).collect()),
                ),
            ],
        )
        .unwrap();

        let result = compute_result::<MultiPointCollection>(
            VectorExpression {
                params: VectorExpressionParams {
                    input_columns: vec![
                        "f1".into(),
                        "f2".into(),
                        "f3".into(),
                        "f4".into(),
                        "f5".into(),
                        "f6".into(),
                        "f7".into(),
                        "f8".into(),
                    ],
                    expression: "f1 + f2 + f3 + f4 + f5 + f6 + f7 + f8".into(),
                    output_column: OutputColumn::Column("new".into()),
                    output_measurement: Measurement::Unitless,
                    geometry_column_name: "geom".to_string(),
                },
                sources: MockFeatureCollectionSource::single(points.clone())
                    .boxed()
                    .into(),
            },
            VectorQueryRectangle::new(
                BoundingBox2D::new(
                    (0., 0.).into(),
                    (NUMBER_OF_ROWS as f64, NUMBER_OF_ROWS as f64).into(),
                )
                .unwrap(),
                TimeInterval::default(),
                ColumnSelection::all(),
            ),
        )
        .await;

        let expected_result = points
            .add_column(
                "new",
                FeatureData::Float((0..NUMBER_OF_ROWS).map(|i| 8.0 * i as f64).collect()),
            )
            .unwrap();

        // TODO: maybe it is nicer to have something wrapping the actual data that we care about and just adds some cache info
        assert!(
            result.chunks_equal_ignoring_cache_hint(&expected_result),
            "{result_geometries:#?} != {expected_result_geometries:#?}",
            result_geometries = result.geometries().collect::<Vec<_>>(),
            expected_result_geometries = expected_result.geometries().collect::<Vec<_>>(),
        );
    }

    async fn compute_result<C>(operator: VectorExpression, query_rectangle: VectorQueryRectangle) -> C
    where
        C: 'static,
        Box<dyn VectorQueryProcessor<VectorType = C>>: TryFrom<TypedVectorQueryProcessor>,
        <Box<dyn VectorQueryProcessor<VectorType = C>> as TryFrom<TypedVectorQueryProcessor>>::Error: std::fmt::Debug,
    {
        let operator = operator
            .boxed()
            .initialize(
                WorkflowOperatorPath::initialize_root(),
                &MockExecutionContext::test_default(),
            )
            .await
            .unwrap();

        let query_processor: Box<dyn VectorQueryProcessor<VectorType = C>> =
            operator.query_processor().unwrap().try_into().unwrap();

        let ecx = MockExecutionContext::test_default();
        let ctx = ecx.mock_query_context(ChunkByteSize::MAX);

        let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

        let mut result = query.map(Result::unwrap).collect::<Vec<C>>().await;

        assert_eq!(result.len(), 1);
        result.remove(0)
    }
}
