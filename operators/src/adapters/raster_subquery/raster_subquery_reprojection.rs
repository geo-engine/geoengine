use crate::engine::RasterQueryRectangle;
use crate::error;
use crate::util::Result;
use futures::{FutureExt, TryFuture};
use geoengine_datatypes::operations::reproject::Reproject;
use geoengine_datatypes::primitives::{SpatialPartition2D, SpatialPartitioned};
use geoengine_datatypes::{
    error::Error::{GridIndexOutOfBounds, InvalidGridIndex},
    operations::reproject::{CoordinateProjection, CoordinateProjector},
    primitives::{SpatialResolution, TimeInterval},
    raster::{grid_idx_iter_2d, BoundedGrid, EmptyGrid, MaterializedRasterTile2D},
    spatial_reference::SpatialReference,
};
use geoengine_datatypes::{
    primitives::{Coordinate2D, TimeInstance},
    raster::{
        CoordinatePixelAccess, GridIdx2D, GridIndexAccessMut, Pixel, RasterTile2D, TileInformation,
    },
};

use log::debug;

use super::{FoldTileAccu, FoldTileAccuMut, SubQueryTileAggregator};

#[derive(Debug)]
pub struct TileReprojectionSubQuery<T, F> {
    pub in_srs: SpatialReference,
    pub out_srs: SpatialReference,
    pub no_data_and_fill_value: T,
    pub fold_fn: F,
    pub in_spatial_res: SpatialResolution,
    pub valid_bounds_in: Option<SpatialPartition2D>,
    pub valid_bounds_out: Option<SpatialPartition2D>,
}

impl<T, FoldM, FoldF> SubQueryTileAggregator<T> for TileReprojectionSubQuery<T, FoldM>
where
    T: Pixel,
    FoldM: Send + Clone + Fn(TileWithProjectionCoordinates<T>, RasterTile2D<T>) -> FoldF,
    FoldF: Send + TryFuture<Ok = TileWithProjectionCoordinates<T>, Error = error::Error>,
{
    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

    type TileAccu = TileWithProjectionCoordinates<T>;

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
    ) -> Result<Self::TileAccu> {
        let output_raster =
            EmptyGrid::new(tile_info.tile_size_in_pixels, self.no_data_and_fill_value);

        // get all pixel idxs and coordinates.
        let idxs: Vec<GridIdx2D> = grid_idx_iter_2d(&output_raster.bounding_box()).collect();
        let idx_coords: Vec<(GridIdx2D, Coordinate2D)> = idxs
            .iter()
            .map(|&i| {
                (
                    i,
                    tile_info
                        .tile_geo_transform()
                        .grid_idx_to_upper_left_coordinate_2d(i),
                )
            })
            .collect();

        // if there is a valid output spatial partition, we need to reproject the coordinates.
        let idx_coords = if let Some(valid_out_area) = self.valid_bounds_out {
            // check if the tile to fill is contained by the valid bounds of the input projection
            if valid_out_area.contains(&tile_info.spatial_partition()) {
                debug!("reproject whole tile");
                // use all pixel coordinates
                idx_coords
            } else if valid_out_area.intersects(&tile_info.spatial_partition()) {
                debug!("reproject part of tile");
                // filter the coordinates to only contain valid ones
                idx_coords
                    .into_iter()
                    .filter(|(_, c)| valid_out_area.contains_coordinate(c))
                    .collect()
            } else {
                debug!("reproject empty tile");
                // fastpath to skip filter
                vec![]
            }
        } else {
            debug!("reproject tile outside valid bounds");
            // TODO: fail here?
            vec![]
        };

        // unzip to use batch projection
        let (idxs, coords): (Vec<_>, Vec<_>) = idx_coords.into_iter().unzip();
        debug!("reproject {} pixels", idxs.len());

        // project
        let proj = CoordinateProjector::from_known_srs(self.out_srs, self.in_srs)?; // TODO: check if we can store it in self. Maybe not because send/sync.
        let projected_coords = proj.project_coordinates(&coords)?; //TODO: we can filter the projected coordinates however they are vaild if the projection was ok.

        // zip idx and projected coordinates
        let coords: Vec<(GridIdx2D, Coordinate2D)> =
            idxs.into_iter().zip(projected_coords.into_iter()).collect();

        Ok(TileWithProjectionCoordinates {
            accu_tile: RasterTile2D::new_with_tile_info(
                query_rect.time_interval,
                tile_info,
                output_raster.into(),
            ),
            coords,
        })
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>> {
        // this is the spatial partition we are interested in
        let valid_spatial_bounds = self
            .valid_bounds_out
            .and_then(|vo| vo.intersection(&tile_info.spatial_partition()))
            .and_then(|vo| vo.intersection(&query_rect.spatial_partition()));
        if let Some(bounds) = valid_spatial_bounds {
            let proj = CoordinateProjector::from_known_srs(self.out_srs, self.in_srs)?;
            let projected_bounds = bounds.reproject(&proj)?;
            Ok(Some(RasterQueryRectangle {
                spatial_bounds: projected_bounds,
                time_interval: TimeInterval::new_instant(start_time)?,
                spatial_resolution: self.in_spatial_res,
            }))
        } else {
            // output query rectangle is not valid in source projection => produce empty tile
            Ok(None)
        }
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

#[allow(dead_code)]
pub fn fold_by_coordinate_lookup_future<T>(
    accu: TileWithProjectionCoordinates<T>,
    tile: RasterTile2D<T>,
) -> impl TryFuture<Ok = TileWithProjectionCoordinates<T>, Error = error::Error>
where
    T: Pixel,
{
    tokio::task::spawn_blocking(|| fold_by_coordinate_lookup_impl(accu, tile)).then(
        |x| async move {
            match x {
                Ok(r) => r,
                Err(e) => Err(e.into()),
            }
        },
    )
}

#[allow(dead_code)]
#[allow(clippy::type_complexity)]
#[allow(clippy::needless_pass_by_value)]
pub fn fold_by_coordinate_lookup_impl<T>(
    accu: TileWithProjectionCoordinates<T>,
    tile: RasterTile2D<T>,
) -> Result<TileWithProjectionCoordinates<T>>
where
    T: Pixel,
{
    let mut accu = accu;
    let t_union = accu.accu_tile.time.union(&tile.time)?;

    accu.tile_mut().time = t_union;

    if tile.grid_array.is_empty() {
        return Ok(accu);
    }

    let TileWithProjectionCoordinates { accu_tile, coords } = accu;

    let mut materialized_accu_tile = accu_tile.into_materialized_tile(); //in a fold chain the real materialization should only happen once. All other calls will be simple conversions.

    match insert_projected_pixels(&mut materialized_accu_tile, &tile, coords.iter()) {
        Ok(_) => Ok(TileWithProjectionCoordinates {
            accu_tile: materialized_accu_tile.into(),
            coords,
        }),
        Err(error) => Err(error),
    }
}

/// This method takes two tiles and a map from `GridIdx2D` to `Coordinate2D`. Then for all `GridIdx2D` we set the values from the corresponding coordinate in the source tile.
pub fn insert_projected_pixels<'a, T: Pixel, I: Iterator<Item = &'a (GridIdx2D, Coordinate2D)>>(
    target: &mut MaterializedRasterTile2D<T>,
    source: &RasterTile2D<T>,
    local_target_idx_source_coordinate_map: I,
) -> Result<()> {
    // TODO: it would be better to run the pixel wise stuff in insert_projected_pixels in parallel...
    for (idx, coord) in local_target_idx_source_coordinate_map {
        match source.pixel_value_at_coord(*coord) {
            Ok(px_value) => target.set_at_grid_index(*idx, px_value)?,
            Err(e) => match e {
                // Ignore errors where a coordinate is not inside a source tile. This is by design.
                GridIndexOutOfBounds {
                    index: _,
                    min_index: _,
                    max_index: _,
                }
                | InvalidGridIndex {
                    grid_index: _,
                    description: _,
                } => {}
                _ => return Err(error::Error::DataType { source: e }),
            },
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
pub struct TileWithProjectionCoordinates<T> {
    accu_tile: RasterTile2D<T>,
    coords: Vec<(GridIdx2D, Coordinate2D)>,
}

impl<T: Pixel> FoldTileAccu for TileWithProjectionCoordinates<T> {
    type RasterType = T;

    fn into_tile(self) -> RasterTile2D<Self::RasterType> {
        self.accu_tile
    }
}

impl<T: Pixel> FoldTileAccuMut for TileWithProjectionCoordinates<T> {
    fn tile_mut(&mut self) -> &mut RasterTile2D<Self::RasterType> {
        &mut self.accu_tile
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use geoengine_datatypes::{
        primitives::Measurement,
        raster::{Grid, GridShape, RasterDataType},
        util::test::TestDefault,
    };
    use num_traits::AsPrimitive;

    use crate::{
        adapters::RasterSubQueryAdapter,
        engine::{MockExecutionContext, MockQueryContext, RasterOperator, RasterResultDescriptor},
        mock::{MockRasterSource, MockRasterSourceParams},
    };

    use super::*;

    #[tokio::test]
    async fn identity_projection() {
        let projection = SpatialReference::epsg_4326();
        let no_data_value = Some(0);

        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 1.).into(), (3., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::one(),
        };

        let query_ctx = MockQueryContext::default();
        let tiling_strat = exe_ctx.tiling_specification;

        let op = mrs1.initialize(&exe_ctx).await.unwrap();

        let raster_res_desc: &RasterResultDescriptor = op.result_descriptor();

        let no_data_v = raster_res_desc
            .no_data_value
            .map(AsPrimitive::as_)
            .expect("must be 0 since we specified it above");

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let valid_bounds = projection.area_of_use_projected().unwrap();

        let state_gen = TileReprojectionSubQuery {
            in_srs: projection,
            out_srs: projection,
            no_data_and_fill_value: no_data_v,
            fold_fn: fold_by_coordinate_lookup_future,
            in_spatial_res: query_rect.spatial_resolution,
            valid_bounds_in: Some(valid_bounds),
            valid_bounds_out: Some(valid_bounds),
        };
        let a = RasterSubQueryAdapter::new(&qp, query_rect, tiling_strat, &query_ctx, state_gen);
        let res = a
            .map(Result::unwrap)
            .map(Option::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;
        assert_eq!(data, res);
    }
}
