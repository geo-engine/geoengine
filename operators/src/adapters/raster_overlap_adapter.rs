use crate::engine::{QueryContext, QueryRectangle, RasterQueryProcessor};
use crate::util::Result;
use futures::{ready, stream::BoxStream, StreamExt};
use futures::{stream::Fold, Stream};
use futures::{stream::FusedStream, Future};
use geoengine_datatypes::{
    primitives::{SpatialBounded, TimeInstance, TimeInterval},
    raster::{Blit, Grid2D, Pixel, RasterTile2D, TileInformation, TilingStrategy},
};
use pin_project::pin_project;
use std::task::Poll;

use std::pin::Pin;

type RasterStream<'a, T> = BoxStream<'a, Result<RasterTile2D<T>>>;

#[pin_project(project = RasterOverlapAdapterProjection)]
pub struct RasterOverlapAdapter<'a, T, Q, QC, FoldF, FoldFut>
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
    running_future: Option<RasterFold<'a, T, FoldFut, FoldF>>,
    ended: bool,
    fold_fn: FoldF,
    no_data_value: Option<T>,
}

impl<'a, T, Q, QC, FoldF, FoldFut> RasterOverlapAdapter<'a, T, Q, QC, FoldF, FoldFut>
where
    T: Pixel,
    Q: RasterQueryProcessor<RasterType = T>,
    QC: QueryContext,
    FoldFut: Future<Output = Result<RasterTile2D<T>>>,
    FoldF: Clone + Fn(Result<RasterTile2D<T>>, Result<RasterTile2D<T>>) -> FoldFut,
{
    pub fn new(
        source: &'a Q,
        query_rect: QueryRectangle,
        tiling_strat: TilingStrategy,
        query_ctx: &'a QC,
        fold_fn: FoldF,
        no_data_value: Option<T>,
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
            no_data_value,
        }
    }
    /*
        fn meh(&mut self) {
            let qs = self
                .source
                .raster_query(self.query_rect, self.query_ctx)
                .unwrap();
            self.query_stream = Some(qs);
        }
    */
}

impl<T, Q, QC, FoldF, FoldFut> FusedStream for RasterOverlapAdapter<'_, T, Q, QC, FoldF, FoldFut>
where
    T: Pixel,
    Q: RasterQueryProcessor<RasterType = T>,
    QC: QueryContext,

    FoldFut: Future<Output = Result<RasterTile2D<T>>>,
    FoldF: Clone + Fn(Result<RasterTile2D<T>>, Result<RasterTile2D<T>>) -> FoldFut,
{
    fn is_terminated(&self) -> bool {
        self.ended
    }
}

impl<'a, T, Q, QC, FoldF, FoldFut> Stream for RasterOverlapAdapter<'a, T, Q, QC, FoldF, FoldFut>
where
    T: Pixel,
    Q: RasterQueryProcessor<RasterType = T>,
    QC: QueryContext,

    FoldFut: Future<Output = Result<RasterTile2D<T>>>,
    FoldF: Clone + Fn(Result<RasterTile2D<T>>, Result<RasterTile2D<T>>) -> FoldFut,
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
                    let current_spatial_tile_info = this.spatial_tiles[*this.current_spatial_tile];

                    let tile_query_rect = QueryRectangle {
                        bbox: current_spatial_tile_info.spatial_bounds(),
                        spatial_resolution: this.query_rect.spatial_resolution,
                        time_interval: TimeInterval::new_instant(t_start),
                    };

                    let fold_tile_spec = TileInformation {
                        tile_size_in_pixels: current_spatial_tile_info.tile_size_in_pixels,
                        global_tile_position: current_spatial_tile_info.global_tile_position,
                        global_geo_transform: current_spatial_tile_info.global_geo_transform,
                    };

                    // TODO: rewrite query here? YEAS!
                    let qs = this.source.raster_query(tile_query_rect, *this.query_ctx)?;
                    let ttf = tiles_to_the_future(
                        qs,
                        fold_tile_spec,
                        *this.no_data_value,
                        tile_query_rect,
                        this.fold_fn.clone(),
                    );
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
        let t_end = r.time.end();
        let _old_t_end = this.time_end.replace(t_end);

        *this.current_spatial_tile += 1;

        Poll::Ready(Some(Ok(r)))
    }
}

#[allow(dead_code)]
pub fn fold_by_blit<T>(
    accu: Result<RasterTile2D<T>>,
    tile: Result<RasterTile2D<T>>,
) -> futures::future::Ready<Result<RasterTile2D<T>>>
where
    T: Pixel,
{
    let result: Result<RasterTile2D<T>> = match (accu, tile) {
        (Ok(mut raster2d), Ok(t)) => {
            let t_union = raster2d.time.union(&t.time).unwrap();
            match raster2d.blit(t) {
                Ok(_) => {
                    raster2d.time = t_union;
                    Ok(raster2d)
                }
                Err(_error) => {
                    // Err(error.into())
                    // dbg!("Skipping non-overlapping area tiles in blit method. This schould be a bug!!!", error);
                    Ok(raster2d)
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

pub type RasterFold<'a, T, FoldFut, FoldF> =
    Fold<RasterStream<'a, T>, FoldFut, Result<RasterTile2D<T>>, FoldF>;

#[allow(clippy::needless_lifetimes)]
fn tiles_to_the_future<'a, T, FoldFut, FoldF>(
    source_stream: RasterStream<'a, T>,
    tile_spec: TileInformation,
    no_data_value: Option<T>,
    query_rect: QueryRectangle,
    fold_f: FoldF,
) -> RasterFold<'a, T, FoldFut, FoldF>
where
    T: Pixel,
    FoldFut: Future<Output = Result<RasterTile2D<T>>>,
    FoldF: FnMut(Result<RasterTile2D<T>>, Result<RasterTile2D<T>>) -> FoldFut,
{
    let output_raster = Grid2D::new_filled(tile_spec.tile_size_in_pixels, T::zero(), no_data_value);
    let output_tile = Ok(RasterTile2D::new_with_tile_info(
        query_rect.time_interval,
        tile_spec,
        output_raster,
    ));

    source_stream.fold(output_tile, fold_f)
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{BoundingBox2D, Measurement, SpatialResolution, TimeInterval},
        raster::{Grid, GridShape, RasterDataType},
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
        let no_data_value = Some(0); // op.result_descriptor().no_data_value;

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let a = RasterOverlapAdapter::new(
            &qp,
            query_rect,
            tiling_strat,
            &query_ctx,
            fold_by_blit,
            no_data_value,
        );
        let res = a
            .map(Result::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;
        assert_eq!(data, res)
    }
}
