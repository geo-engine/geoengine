use crate::{
    error::{Error, Result},
    workflows::workflow::Workflow,
};
use futures::{stream::select_all, StreamExt, TryFutureExt, TryStreamExt};
use geoengine_datatypes::{
    primitives::VectorQueryRectangle,
    raster::{BaseTile, ConvertDataTypeParallel, GridOrEmpty, GridShape},
};
use geoengine_operators::{
    call_on_generic_raster_processor,
    engine::{
        ExecutionContext, QueryContext, QueryProcessor, RasterOperator, TypedRasterQueryProcessor,
        WorkflowOperatorPath,
    },
    util::spawn_blocking_with_thread_pool,
};

use crate::machine_learning::ml_error::MachineLearningError;

use super::{Aggregatable, MachineLearningFeature};

/// This method initializes the raster operators and produces a vector of typed raster query
/// processors.
pub async fn get_query_processors(
    operators: Vec<Box<dyn RasterOperator>>,
    exe_ctx: &dyn ExecutionContext,
) -> Result<Vec<TypedRasterQueryProcessor>> {
    let wop = WorkflowOperatorPath::initialize_root();
    let initialized =
        futures::future::join_all(operators.into_iter().enumerate().map(|(i, op)| {
            let init = op.initialize(wop.clone_and_append(i as u8), exe_ctx);
            init
        }))
        .await
        .into_iter()
        .collect::<Vec<_>>();

    let query_processors = initialized
        .into_iter()
        .map(|init_raster_op| {
            let query_processor = init_raster_op?.query_processor()?;
            Ok(query_processor)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(query_processors)
}

/// These workflows represent the input data for the machine learning model.
/// This could either be feature data, or label data.
pub fn get_operators_from_workflows(
    workflows: &[Workflow],
) -> Result<Vec<Box<dyn RasterOperator>>> {
    let initialized_raster_operators = workflows
        .iter()
        .map(|workflow| {
            let op = &workflow.operator;
            let raster_op = op.clone().get_raster()?;
            Ok(raster_op.clone())
        })
        .collect::<Result<Vec<_>, Error>>()?;

    Ok(initialized_raster_operators)
}

//TODO: add a way to abort the query execution when the tasks is aborted
/// Build ML Features from the raw data and assign feature names.
pub async fn accumulate_raster_data<A>(
    feature_names: &[Option<String>],
    processors: Vec<TypedRasterQueryProcessor>,
    query: VectorQueryRectangle,
    ctx: &dyn QueryContext,
    aggregator_vec: &mut [A],
) -> Result<Vec<MachineLearningFeature>>
where
    A: Aggregatable<Data = f32>,
{
    let mut queries = Vec::with_capacity(processors.len());
    let q = query.into();
    for (i, raster_processor) in processors.iter().enumerate() {
        queries.push(
            call_on_generic_raster_processor!(raster_processor, processor => {
                processor.query(q, ctx).await?
                         .and_then(
                            move |tile| spawn_blocking_with_thread_pool(
                                ctx.thread_pool().clone(),
                                move || (i, tile.convert_data_type_parallel()) ).map_err(Into::into)
                        ).boxed()
            }),
        );
    }

    // select_all is used to simultaneously query all inputs
    // the tiles are then processed in whatever order they become ready
    while let Some(ready_elem) = select_all(&mut queries).next().await {
        type Tile = BaseTile<GridOrEmpty<GridShape<[usize; 2]>, f32>>;
        let (i, raster_tile): (usize, Tile) = ready_elem?;

        match raster_tile.grid_array {
            GridOrEmpty::Grid(g) => {
                let mut tile_data = g.masked_element_deref_iterator();

                let agg: &mut A = aggregator_vec
                            .get_mut(i).expect("Aggregator should exist, because one was created for each input, and the index is computed via enumeration");

                agg.aggregate(&mut tile_data);
            }
            GridOrEmpty::Empty(_) => {}
        };
    }

    let result: Result<Vec<MachineLearningFeature>> = aggregator_vec
        .iter_mut()
        .enumerate()
        .map(|(i, agg)| {
            let name = feature_names
                .get(i)
                .ok_or(MachineLearningError::CouldNotGetMlFeatureName { index: i })?;
            let collected_data = agg.finish();
            let ml_feature: MachineLearningFeature =
                MachineLearningFeature::new(name.clone(), collected_data);

            Ok(ml_feature)
        })
        .collect::<Result<Vec<MachineLearningFeature>>>();

    result
}
