use super::Result;
use crate::engine::{ExecutionContext, QueryContext, RasterOperator, WorkflowOperatorPath};
use futures::StreamExt;
use geoengine_datatypes::{
    primitives::RasterQueryRectangle, raster::RasterTile2D,
    util::test::assert_eq_two_list_of_tiles_u8,
};

pub async fn raster_operator_to_list_of_tiles_u8<E: ExecutionContext, Q: QueryContext>(
    exe_ctx: &E,
    query_ctx: &Q,
    operator: Box<dyn RasterOperator>,
    query_rectangle: RasterQueryRectangle,
) -> Result<Vec<RasterTile2D<u8>>> {
    let initialized_operator = operator
        .initialize(WorkflowOperatorPath::initialize_root(), exe_ctx)
        .await?;
    let query_processor = initialized_operator.query_processor()?.get_u8().ok_or(
        crate::error::Error::MustNotHappen {
            message: "Operator does not produce u8 while this function requires it".to_owned(),
        },
    )?;

    let res = query_processor
        .raster_query(query_rectangle, query_ctx)
        .await?
        .collect::<Vec<_>>()
        .await;

    let res = res.into_iter().collect::<Result<Vec<_>, _>>()?;

    Ok(res)
}

/// Compares the output of a raster operators and a list of tiles and panics with a message if they are not equal
///
/// # Panics
///
/// If there are tiles that are not equal
pub async fn assert_eq_raster_operator_res_and_list_of_tiles_u8<
    E: ExecutionContext,
    Q: QueryContext,
>(
    exe_ctx: &E,
    query_ctx: &Q,
    operator: Box<dyn RasterOperator>,
    query_rectangle: RasterQueryRectangle,
    compare_cache_hint: bool,
    list_of_tiles: &[RasterTile2D<u8>],
) {
    let res_a = raster_operator_to_list_of_tiles_u8(exe_ctx, query_ctx, operator, query_rectangle)
        .await
        .expect("raster operator to list failed!");

    assert_eq_two_list_of_tiles_u8(&res_a, list_of_tiles, compare_cache_hint);
}

/// Compares the output of two raster operators and panics with a message if they are not equal
///
/// # Panics
///
/// If there are tiles that are not equal
pub async fn assert_eq_two_raster_operator_res_u8<E: ExecutionContext, Q: QueryContext>(
    exe_ctx: &E,
    query_ctx: &Q,
    operator_a: Box<dyn RasterOperator>,
    operator_b: Box<dyn RasterOperator>,
    query_rectangle: RasterQueryRectangle,
    compare_cache_hint: bool,
) {
    let res_a = raster_operator_to_list_of_tiles_u8(
        exe_ctx,
        query_ctx,
        operator_a,
        query_rectangle.clone(),
    )
    .await
    .expect("raster operator to list failed for operator_a!");

    assert_eq_raster_operator_res_and_list_of_tiles_u8(
        exe_ctx,
        query_ctx,
        operator_b,
        query_rectangle,
        compare_cache_hint,
        &res_a,
    )
    .await;
}
