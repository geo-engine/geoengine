use std::path::Path;

use super::Result;
use crate::{
    engine::{ExecutionContext, QueryContext, RasterOperator, WorkflowOperatorPath},
    util::gdal::gdal_open_dataset,
};
use futures::StreamExt;
use gdal::raster::GdalType;
use geoengine_datatypes::{
    primitives::{CacheHint, RasterQueryRectangle, TimeInterval},
    raster::{
        GeoTransform, Grid, GridOrEmpty, GridShape2D, MapElements, MaskedGrid, Pixel, RasterTile2D,
        TilingSpatialGridDefinition,
    },
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

/// Reads a single raster tile from a single file and returns it as a `RasterTile2D<u8>`.
/// This assumes that the file actually contains exactly one geoengine tile according to the spatial grid definition.
pub fn raster_tile_from_file<T: Pixel + GdalType>(
    file_path: &Path,
    tiling_spatial_grid: TilingSpatialGridDefinition,
    time: TimeInterval,
    band: u32,
) -> Result<RasterTile2D<T>> {
    let ds = gdal_open_dataset(file_path)?;

    let gf: GeoTransform = ds.geo_transform()?.into();

    let tiling_strategy = tiling_spatial_grid.generate_data_tiling_strategy();

    let tiling_geo_transform = tiling_spatial_grid.tiling_geo_transform();
    let tile_origin_pixel_idx =
        tiling_geo_transform.coordinate_to_grid_idx_2d(gf.origin_coordinate);
    let tile_position = tiling_strategy.pixel_idx_to_tile_idx(tile_origin_pixel_idx);

    let rasterband = ds.rasterband(1)?;

    let out_shape: GridShape2D = GridShape2D::new([ds.raster_size().0, ds.raster_size().1]);

    let buffer = rasterband.read_as::<T>(
        (0, 0),
        ds.raster_size(),
        ds.raster_size(), // or: tile_size_in_pixels
        None,
    )?;

    let (_, buffer_data) = buffer.into_shape_and_vec();
    let data_grid = Grid::new(out_shape.clone(), buffer_data)?;

    let mask_band = rasterband.open_mask_band()?;
    let mask_buffer = mask_band.read_as::<u8>(
        (0, 0),
        ds.raster_size(),
        ds.raster_size(), // or: tile_size_in_pixels
        None,
    )?;
    let (_, mask_buffer_data) = mask_buffer.into_shape_and_vec();
    let mask_grid = Grid::new(out_shape, mask_buffer_data)?.map_elements(|p: u8| p > 0);

    let masked_grid = MaskedGrid::new(data_grid, mask_grid)?;

    Ok(RasterTile2D::<T>::new(
        time,
        tile_position,
        band,
        tiling_geo_transform,
        GridOrEmpty::from(masked_grid),
        CacheHint::default(),
    ))
}
