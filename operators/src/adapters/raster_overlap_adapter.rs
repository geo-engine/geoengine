use crate::engine::{QueryContext, QueryRectangle, RasterQueryProcessor};
use crate::util::Result;
use futures::{ready, stream::BoxStream, StreamExt};
use futures::{stream::Fold, Stream};
use futures::{stream::FusedStream, Future};
use geoengine_datatypes::{
    primitives::TimeInstance,
    raster::{Blit, GeoTransform, Grid2D, Pixel, RasterTile2D, TileInformation, TilingStrategy},
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
    fn create_stream_future(&mut self, query_rect: QueryRectangle) -> Result<()> {
        let qs = self.source.raster_query(self.query_rect, self.query_ctx)?;

        let query_geo_transform = GeoTransform::new(
            query_rect.bbox.upper_left(),
            query_rect.spatial_resolution.x,
            -query_rect.spatial_resolution.y, // TODO: negative, s.t. geo transform fits...
        );

        let dim = [
            (query_rect.bbox.size_y() / query_rect.spatial_resolution.y) as usize,
            (query_rect.bbox.size_x() / query_rect.spatial_resolution.x) as usize,
        ];

        let output_raster = Grid2D::new_filled(dim.into(), T::zero(), None);
        let output_tile = Ok(RasterTile2D::new_without_offset(
            query_rect.time_interval,
            query_geo_transform,
            output_raster,
        ));

        let ttf: RasterFold<'a, T, FoldFut, FoldF> = qs.fold(output_tile, self.fold_fn.clone());

        self.running_future.replace(ttf);
        Ok(())
    }

    fn clear_future(&mut self) {
        self.running_future.take();
    }

    fn fwd(&mut self) {}
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
                dbg!(*this.current_spatial_tile, *this.time_start, *this.time_end);
                *this.current_spatial_tile = 0;
                *this.time_start = *this.time_end;
                *this.time_end = None;
                dbg!(*this.current_spatial_tile, *this.time_start, *this.time_end);
            }

            if let Some(t_start) = *this.time_start {
                if t_start >= this.query_rect.time_interval.end() {
                    *this.ended = true;
                    dbg!(*this.current_spatial_tile, this.ended);
                } else {
                    // TODO: rewrite query here?
                    let qs = this
                        .source
                        .raster_query(*this.query_rect, *this.query_ctx)?;
                    let ttf = tiles_to_the_future(qs, *this.query_rect, this.fold_fn.clone());
                    this.running_future.set(Some(ttf));
                }
            }
        }

        let rv = match this.running_future.as_mut().as_pin_mut() {
            Some(fut) => ready!(fut.poll(cx)),
            None => return Poll::Ready(None), // meybe this is not needed
        };

        let r = match rv {
            Ok(tile) => tile,
            Err(err) => return Poll::Ready(Some(Err(err))),
        };

        // TODO: error on error!

        // set the running future to None --> will create a new one in the next call
        this.running_future.set(None);

        // update the end_time from the produced tile (should/must not change within a tile run?)
        let t_end = r.time.end();
        let old_t_end = this.time_end.replace(t_end);
        dbg!(*this.current_spatial_tile, old_t_end, t_end);

        *this.current_spatial_tile += 1;

        Poll::Ready(Some(Ok(r)))
    }
}

pub fn fold_by_blit<T>(
    accu: Result<RasterTile2D<T>>,
    tile: Result<RasterTile2D<T>>,
) -> futures::future::Ready<Result<RasterTile2D<T>>>
where
    T: Pixel,
{
    let result: Result<RasterTile2D<T>> = match (accu, tile) {
        (Ok(mut raster2d), Ok(tile)) => match raster2d.blit(tile) {
            Ok(_) => Ok(raster2d),
            Err(error) => Err(error.into()),
        },
        (Err(error), _) => Err(error),
        (_, Err(error)) => Err(error.into()),
    };

    match result {
        Ok(updated_raster2d) => futures::future::ok(updated_raster2d),
        Err(error) => futures::future::err(error),
    }
}

pub type RasterFold<'a, T, FoldFut, FoldF> =
    Fold<RasterStream<'a, T>, FoldFut, Result<RasterTile2D<T>>, FoldF>;

fn tiles_to_the_future<'a, T, FoldFut, FoldF>(
    source_stream: RasterStream<'a, T>,
    query_rect: QueryRectangle,
    fold_f: FoldF,
) -> RasterFold<'a, T, FoldFut, FoldF>
where
    T: Pixel,
    FoldFut: Future<Output = Result<RasterTile2D<T>>>,
    FoldF: FnMut(Result<RasterTile2D<T>>, Result<RasterTile2D<T>>) -> FoldFut,
{
    let query_geo_transform = GeoTransform::new(
        query_rect.bbox.upper_left(),
        query_rect.spatial_resolution.x,
        -query_rect.spatial_resolution.y, // TODO: negative, s.t. geo transform fits...
    );

    let dim = [
        (query_rect.bbox.size_y() / query_rect.spatial_resolution.y) as usize,
        (query_rect.bbox.size_x() / query_rect.spatial_resolution.x) as usize,
    ];

    let output_raster = Grid2D::new_filled(dim.into(), T::zero(), None);
    let output_tile = Ok(RasterTile2D::new_without_offset(
        query_rect.time_interval,
        query_geo_transform,
        output_raster,
    ));

    let output_future = source_stream.fold(output_tile, fold_f);

    output_future
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{BoundingBox2D, Measurement, SpatialResolution, TimeInterval},
        raster::{Grid, RasterDataType},
        spatial_reference::SpatialReference,
    };

    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use crate::engine::{RasterOperator, RasterResultDescriptor};
    use crate::mock::{MockRasterSource, MockRasterSourceParams};

    #[tokio::test]
    async fn test_one() {
        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 5),
                        tile_position: [0, 0].into(),
                        global_geo_transform: Default::default(),
                        grid_array: Grid::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], Some(0))
                            .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 5),
                        tile_position: [0, 1].into(),
                        global_geo_transform: Default::default(),
                        grid_array: Grid::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12], Some(0))
                            .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(5, 10),
                        tile_position: [0, 0].into(),
                        global_geo_transform: Default::default(),
                        grid_array: Grid::new([3, 2].into(), vec![13, 14, 15, 16, 17, 18], Some(0))
                            .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(5, 10),
                        tile_position: [0, 1].into(),
                        global_geo_transform: Default::default(),
                        grid_array: Grid::new([3, 2].into(), vec![19, 20, 21, 22, 23, 24], Some(0))
                            .unwrap(),
                    },
                ],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                },
            },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::default();
        let query_rect = QueryRectangle {
            bbox: BoundingBox2D::new_unchecked((0., 0.).into(), (3., 3.).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::one(),
        };
        dbg!(&query_rect);

        let query_ctx = MockQueryContext {
            chunk_byte_size: 1024 * 1024,
        };
        let tiling_strat = exe_ctx.tiling_specification.strategy(
            query_rect.spatial_resolution.x,
            -query_rect.spatial_resolution.y,
        );
        dbg!(&tiling_strat);

        let qp = mrs1
            .initialize(&exe_ctx)
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let fbb = fold_by_blit;

        let a = RasterOverlapAdapter::new(&qp, query_rect, tiling_strat, &query_ctx, fbb);
        let res = a
            .map(Result::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;
        dbg!(res);
    }
}
