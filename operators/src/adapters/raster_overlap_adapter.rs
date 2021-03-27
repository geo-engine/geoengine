use crate::engine::{QueryContext, QueryRectangle, RasterQueryProcessor};
use crate::error;
use crate::util::Result;
use futures::{ready, stream::BoxStream, StreamExt};
use futures::{stream::Fold, Stream};
use futures::{stream::FusedStream, Future};
use geoengine_datatypes::error::Error::{GridIndexOutOfBounds, InvalidGridIndex};
use geoengine_datatypes::{
    primitives::{Coordinate2D, TimeInstance},
    raster::{
        Blit, CoordinatePixelAccess, GridIdx2D, GridIndexAccessMut, Pixel, RasterTile2D,
        TileInformation, TilingStrategy,
    },
};
use pin_project::pin_project;
use std::task::Poll;

use std::pin::Pin;

type RasterStream<'a, T> = BoxStream<'a, Result<RasterTile2D<T>>>;
type AccuType<T, X> = Result<(RasterTile2D<T>, X)>;

pub type RasterFold<'a, T, FoldFut, FoldF, X> =
    Fold<RasterStream<'a, T>, FoldFut, AccuType<T, X>, FoldF>;

#[pin_project(project = RasterOverlapAdapterProjection)]
pub struct RasterOverlapAdapter<'a, T, Q, QC, FoldF, FoldFut, XF, X, QF>
where
    T: Pixel,
    Q: RasterQueryProcessor<RasterType = T>,
    QC: QueryContext,
{
    query_rect: QueryRectangle,
    time_start: Option<TimeInstance>,
    time_end: Option<TimeInstance>,
    // TODO: calculate at start when tiling info is available before querying first tile
    spatial_tiles: Vec<TileInformation>,
    current_spatial_tile: usize,
    source: &'a Q,
    query_ctx: &'a QC,
    #[pin]
    running_future: Option<RasterFold<'a, T, FoldFut, FoldF, X>>,
    ended: bool,
    fold_fn: FoldF,
    accu_creator: XF,
    tile_query_rewrite: QF,
}

impl<'a, T, Q, QC, FoldF, FoldFut, XF, X, QF>
    RasterOverlapAdapter<'a, T, Q, QC, FoldF, FoldFut, XF, X, QF>
where
    T: Pixel,
    Q: RasterQueryProcessor<RasterType = T>,
    QC: QueryContext,
    FoldFut: Future<Output = Result<(RasterTile2D<T>, X)>>,
    FoldF: Clone + Fn(Result<(RasterTile2D<T>, X)>, Result<RasterTile2D<T>>) -> FoldFut,
{
    pub fn new(
        source: &'a Q,
        query_rect: QueryRectangle,
        tiling_strat: TilingStrategy,
        query_ctx: &'a QC,
        fold_fn: FoldF,
        accu_creator: XF,
        tile_query_rewrite: QF,
    ) -> Self {
        let tx: Vec<TileInformation> = tiling_strat
            .tile_information_iterator(query_rect.bbox)
            .collect();
        dbg!(&tx);

        Self {
            source,
            query_rect,
            spatial_tiles: tx, // TODO: no unwrap, actually if there are empty intersections the tiling strategy has a bug.
            current_spatial_tile: 0,
            time_start: Some(query_rect.time_interval.start()),
            time_end: None,
            running_future: None,
            query_ctx,
            ended: false,
            fold_fn,
            accu_creator,
            tile_query_rewrite,
        }
    }
}

impl<T, Q, QC, FoldF, FoldFut, XF, X, QF> FusedStream
    for RasterOverlapAdapter<'_, T, Q, QC, FoldF, FoldFut, XF, X, QF>
where
    T: Pixel,
    Q: RasterQueryProcessor<RasterType = T>,
    QC: QueryContext,
    XF: Clone + Fn(TileInformation, QueryRectangle) -> Result<(RasterTile2D<T>, X)>,
    X: Clone + Send,

    FoldFut: Future<Output = Result<(RasterTile2D<T>, X)>>,
    FoldF: Clone + Fn(Result<(RasterTile2D<T>, X)>, Result<RasterTile2D<T>>) -> FoldFut,

    QF: Clone + Fn(&TileInformation, &QueryRectangle, TimeInstance) -> Result<QueryRectangle>,
{
    fn is_terminated(&self) -> bool {
        self.ended
    }
}

impl<'a, T, Q, QC, FoldF, FoldFut, XF, X, QF> Stream
    for RasterOverlapAdapter<'a, T, Q, QC, FoldF, FoldFut, XF, X, QF>
where
    T: Pixel,
    Q: RasterQueryProcessor<RasterType = T>,
    QC: QueryContext,
    XF: Clone + Fn(TileInformation, QueryRectangle) -> Result<(RasterTile2D<T>, X)>,
    X: Clone + Send,

    FoldFut: Future<Output = Result<(RasterTile2D<T>, X)>>,
    FoldF: Clone + Fn(Result<(RasterTile2D<T>, X)>, Result<RasterTile2D<T>>) -> FoldFut,

    QF: Clone + Fn(&TileInformation, &QueryRectangle, TimeInstance) -> Result<QueryRectangle>,
{
    type Item = Result<RasterTile2D<T>>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.is_terminated() {
            return Poll::Ready(None);
        }

        let mut this = self.project();

        if this.running_future.as_ref().is_none() {
            if *this.current_spatial_tile >= this.spatial_tiles.len() {
                *this.current_spatial_tile = 0;
                *this.time_start = *this.time_end;
                *this.time_end = None;
            }

            if let Some(t_start) = *this.time_start {
                if t_start >= this.query_rect.time_interval.end() {
                    *this.ended = true;
                } else {
                    // TODO: maybe this whole block should also be a future!
                    let current_spatial_tile_info = this.spatial_tiles[*this.current_spatial_tile];

                    let fold_tile_spec = TileInformation {
                        tile_size_in_pixels: current_spatial_tile_info.tile_size_in_pixels,
                        global_tile_position: current_spatial_tile_info.global_tile_position,
                        global_geo_transform: current_spatial_tile_info.global_geo_transform,
                    };

                    let tqr = (this.tile_query_rewrite)(
                        &current_spatial_tile_info,
                        &this.query_rect,
                        t_start,
                    )?;

                    let qs = this.source.raster_query(tqr, *this.query_ctx)?;

                    let accu = (this.accu_creator)(fold_tile_spec, tqr);

                    let ttf = qs.fold(accu, this.fold_fn.clone());

                    this.running_future.set(Some(ttf));
                }
            }
        }

        let rv = match this.running_future.as_mut().as_pin_mut() {
            Some(fut) => ready!(fut.poll(cx)),
            None => return Poll::Ready(None),
        };

        let r = match rv {
            Ok(tile) => tile,
            Err(err) => return Poll::Ready(Some(Err(err))),
        };

        // set the running future to None --> will create a new one in the next call
        this.running_future.set(None);

        // update the end_time from the produced tile (should/must not change within a tile run?)
        let t_end = r.0.time.end();
        let _old_t_end = this.time_end.replace(t_end);

        //TODO:  if end = start then +1

        *this.current_spatial_tile += 1;

        Poll::Ready(Some(Ok(r.0)))
    }
}

#[allow(dead_code)]
pub fn fold_by_blit<T, X>(
    accu: AccuType<T, X>,
    tile: Result<RasterTile2D<T>>,
) -> futures::future::Ready<AccuType<T, X>>
where
    T: Pixel,
    X: Clone,
{
    let result: Result<(RasterTile2D<T>, X)> = match (accu, tile) {
        (Ok((mut raster2d, unused)), Ok(t)) => {
            let t_union = raster2d.time.union(&t.time).unwrap();
            match raster2d.blit(t) {
                Ok(_) => {
                    raster2d.time = t_union;
                    Ok((raster2d, unused))
                }
                Err(_error) => {
                    // Err(error.into())
                    // dbg!("Skipping non-overlapping area tiles in blit method. This schould be a bug!!!", error);
                    Ok((raster2d, unused))
                }
            }
        }
        (Err(error), _) | (_, Err(error)) => Err(error),
    };

    match result {
        Ok(updated_raster2d) => futures::future::ok(updated_raster2d),
        Err(error) => futures::future::err(error),
    }
}

#[allow(dead_code)]
#[allow(clippy::type_complexity)]
pub fn fold_by_coordinate_lookup<T>(
    accu: AccuType<T, Vec<(GridIdx2D, Coordinate2D)>>,
    tile: Result<RasterTile2D<T>>,
) -> futures::future::Ready<AccuType<T, Vec<(GridIdx2D, Coordinate2D)>>>
where
    T: Pixel,
{
    let result: AccuType<T, Vec<(GridIdx2D, Coordinate2D)>> = match (accu, tile) {
        (Ok((mut raster2d, lookup)), Ok(t)) => {
            let t_union = raster2d.time.union(&t.time).unwrap();

            match insert_projected_pixels(&mut raster2d, &t, lookup.iter()) {
                Ok(_) => {
                    raster2d.time = t_union;
                    Ok((raster2d, lookup))
                }
                Err(error) => Err(error),
            }
        }
        (Err(error), _) | (_, Err(error)) => Err(error),
    };

    match result {
        Ok(updated_raster2d) => futures::future::ok(updated_raster2d),
        Err(error) => futures::future::err(error),
    }
}

pub fn insert_projected_pixels<'a, T: Pixel, I: Iterator<Item = &'a (GridIdx2D, Coordinate2D)>>(
    target: &mut RasterTile2D<T>,
    source: &RasterTile2D<T>,
    local_target_idx_source_coordinate_map: I,
) -> Result<()> {
    for (idx, coord) in local_target_idx_source_coordinate_map {
        match source.pixel_value_at_coord(*coord) {
            Ok(px_value) => target.set_at_grid_index(*idx, px_value)?,
            Err(e) => match e {
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

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        operations::reproject::{
            project_coordinates_fail_tolarant, CoordinateProjection, CoordinateProjector, Reproject,
        },
        primitives::{BoundingBox2D, Measurement, SpatialBounded, SpatialResolution, TimeInterval},
        raster::{grid_idx_iter_2d, BoundedGrid, Grid, Grid2D, GridShape, RasterDataType},
        spatial_reference::SpatialReference,
    };

    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use crate::engine::{RasterOperator, RasterResultDescriptor};
    use crate::mock::{MockRasterSource, MockRasterSourceParams};

    #[tokio::test]
    async fn identity() {
        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4], Some(0)).unwrap(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10], Some(0)).unwrap(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], Some(0)).unwrap(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22], Some(0)).unwrap(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                },
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = QueryRectangle {
            bbox: BoundingBox2D::new_unchecked((0., 0.).into(), (3., 1.).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::one(),
        };

        let query_ctx = MockQueryContext {
            chunk_byte_size: 1024 * 1024,
        };
        let tiling_strat = exe_ctx.tiling_specification.strategy(
            query_rect.spatial_resolution.x,
            -query_rect.spatial_resolution.y,
        );

        let op = mrs1.initialize(&exe_ctx).unwrap();

        // would be nice to get from result descriptor
        let no_data_v = Some(0_u8); // op.result_descriptor().no_data_value;

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let accu_creator = |tile_spec: TileInformation, query_rect: QueryRectangle| {
            let output_raster = Grid2D::new_filled(tile_spec.tile_size_in_pixels, 0, no_data_v);
            Ok((
                RasterTile2D::new_with_tile_info(
                    query_rect.time_interval,
                    tile_spec,
                    output_raster,
                ),
                (),
            ))
        };

        let tqr = |tile: &TileInformation, qr: &QueryRectangle, time: TimeInstance| {
            Ok(QueryRectangle {
                bbox: tile.spatial_bounds(),
                spatial_resolution: qr.spatial_resolution,
                time_interval: TimeInterval::new_instant(time),
            })
        };

        let a = RasterOverlapAdapter::new(
            &qp,
            query_rect,
            tiling_strat,
            &query_ctx,
            fold_by_blit,
            accu_creator,
            tqr,
        );
        let res = a
            .map(Result::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;
        assert_eq!(data, res)
    }

    #[tokio::test]
    async fn identity_projection() {
        let projection = SpatialReference::new(
            geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
            4326,
        );

        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4], Some(0)).unwrap(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10], Some(0)).unwrap(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], Some(0)).unwrap(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22], Some(0)).unwrap(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                },
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = QueryRectangle {
            bbox: BoundingBox2D::new_unchecked((0., 0.).into(), (3., 1.).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::one(),
        };

        let query_ctx = MockQueryContext {
            chunk_byte_size: 1024 * 1024,
        };
        let tiling_strat = exe_ctx.tiling_specification.strategy(
            query_rect.spatial_resolution.x,
            -query_rect.spatial_resolution.y,
        );

        let op = mrs1.initialize(&exe_ctx).unwrap();

        // would be nice to get from result descriptor
        let no_data_v = Some(0_u8); // op.result_descriptor().no_data_value;

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let accu_creator = |tile_spec: TileInformation, query_rect: QueryRectangle| {
            let output_raster = Grid2D::new_filled(tile_spec.tile_size_in_pixels, 0, no_data_v);
            let idxs: Vec<GridIdx2D> = grid_idx_iter_2d(&output_raster.bounding_box()).collect();
            let coords: Vec<Coordinate2D> = idxs
                .iter()
                .map(|&i| tile_spec.tile_geo_transform().grid_idx_to_coordinate_2d(i))
                .collect();

            let proj = CoordinateProjector::from_known_srs(projection, projection)?;
            let projected_coords = project_coordinates_fail_tolarant(&coords, &proj);

            let accu_companion: Vec<(GridIdx2D, Coordinate2D)> = idxs
                .into_iter()
                .zip(projected_coords.into_iter())
                .filter_map(|(i, c)| c.map(|c| (i, c)))
                .collect();

            Ok((
                RasterTile2D::new_with_tile_info(
                    query_rect.time_interval,
                    tile_spec,
                    output_raster,
                ),
                accu_companion,
            ))
        };

        let tqr = |tile: &TileInformation, qr: &QueryRectangle, time: TimeInstance| {
            let proj = CoordinateProjector::from_known_srs(projection, projection)?;

            Ok(QueryRectangle {
                bbox: tile.spatial_bounds().reproject(&proj)?,
                spatial_resolution: qr.spatial_resolution,
                time_interval: TimeInterval::new_instant(time),
            })
        };

        let a = RasterOverlapAdapter::new(
            &qp,
            query_rect,
            tiling_strat,
            &query_ctx,
            fold_by_coordinate_lookup,
            accu_creator,
            tqr,
        );
        let res = a
            .map(Result::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;
        assert_eq!(data, res)
    }
}
