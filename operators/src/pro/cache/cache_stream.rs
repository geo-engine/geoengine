use super::error::CacheError;
use super::shared_cache::{CacheBackendElementExt, CacheElement};
use crate::util::Result;
use futures::stream::FusedStream;
use futures::{Future, Stream};
use pin_project::pin_project;
use std::{pin::Pin, sync::Arc};

type DecompressorFutureType<X> = tokio::task::JoinHandle<std::result::Result<X, CacheError>>;

/// Our own cache element stream that "owns" the data (more precisely a reference to the data)

#[pin_project(project = CacheStreamProjection)]
pub struct CacheStream<I, O, Q> {
    inner: CacheStreamInner<I, Q>,
    #[pin]
    state: Option<DecompressorFutureType<O>>,
}

pub struct CacheStreamInner<I, Q> {
    data: Arc<Vec<I>>,
    query: Q,
    idx: usize,
}

impl<I, Q> CacheStreamInner<I, Q>
where
    I: CacheBackendElementExt<Query = Q>,
{
    // TODO: we could use a iter + filter adapter here to return refs however this would require a lot of lifetime annotations
    fn next_idx(&mut self) -> Option<usize> {
        for i in self.idx..self.data.len() {
            let element_ref = &self.data[i];
            if element_ref.intersects_query(&self.query) {
                self.idx = i + 1;
                return Some(i);
            }
        }
        None
    }

    fn data_arc(&self) -> Arc<Vec<I>> {
        self.data.clone()
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn remaining(&self) -> usize {
        self.len() - self.idx
    }

    fn terminated(&self) -> bool {
        self.idx >= self.len()
    }

    fn new(data: Arc<Vec<I>>, query: Q) -> Self {
        Self {
            data,
            query,
            idx: 0,
        }
    }
}

impl<I, O, Q> CacheStream<I, O, Q>
where
    O: CacheElement<Query = Q, StoredCacheElement = I> + 'static,
    I: CacheBackendElementExt<Query = Q> + 'static,
{
    pub fn new(data: Arc<Vec<I>>, query: Q) -> Self {
        Self {
            inner: CacheStreamInner::new(data, query),
            state: None,
        }
    }

    pub fn element_count(&self) -> usize {
        self.inner.len()
    }

    fn terminated(&self) -> bool {
        self.state.is_none() && self.inner.terminated()
    }

    fn check_decompress_future_res(
        future_res: Result<Result<O, CacheError>, tokio::task::JoinError>,
    ) -> Result<O, CacheError> {
        match future_res {
            Ok(Ok(res)) => Ok(res),
            Ok(Err(err)) => Err(err),
            Err(source) => Err(CacheError::CouldNotRunDecompressionTask { source }),
        }
    }

    fn create_decompression_future(data: Arc<Vec<I>>, idx: usize) -> DecompressorFutureType<O> {
        crate::util::spawn_blocking(move || O::from_stored_element_ref(&data[idx]))
    }
}

impl<I, O, Q> Stream for CacheStream<I, O, Q>
where
    O: CacheElement<StoredCacheElement = I, Query = Q> + 'static,
    I: CacheBackendElementExt<Query = Q> + 'static,
{
    type Item = Result<O, CacheError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.terminated() {
            return std::task::Poll::Ready(None);
        }

        let CacheStreamProjection { inner, mut state } = self.as_mut().project();

        if state.is_none() {
            if let Some(next) = inner.next_idx() {
                let future_data = inner.data_arc();
                let future = Self::create_decompression_future(future_data, next);
                state.set(Some(future));
            }
        }

        if let Some(pin_state) = state.as_mut().as_pin_mut() {
            let res = futures::ready!(pin_state.poll(cx));
            state.set(None);
            let element = Self::check_decompress_future_res(res);
            return std::task::Poll::Ready(Some(element));
        }

        std::task::Poll::Ready(None)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.terminated() {
            return (0, Some(0));
        }
        // There must be a cache hit to produce this stream. So there must be at least one element inside the query.
        (1, Some(self.inner.remaining()))
    }
}

impl<I, O, Q> FusedStream for CacheStream<I, O, Q>
where
    O: CacheElement<Query = Q, StoredCacheElement = I> + 'static,
    I: CacheBackendElementExt<Query = Q> + 'static,
{
    fn is_terminated(&self) -> bool {
        self.terminated()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use geoengine_datatypes::{
        collections::MultiPointCollection,
        primitives::{
            BoundingBox2D, CacheHint, FeatureData, MultiPoint, RasterQueryRectangle,
            SpatialPartition2D, SpatialResolution, TimeInterval, VectorQueryRectangle,
        },
        raster::{GeoTransform, Grid2D, GridIdx2D, RasterTile2D},
    };

    use crate::pro::cache::{
        cache_chunks::CompressedFeatureCollection,
        cache_stream::CacheStreamInner,
        cache_tiles::{CompressedRasterTile2D, CompressedRasterTileExt},
    };

    fn create_test_raster_data() -> Vec<CompressedRasterTile2D<u8>> {
        let mut data = Vec::new();
        for i in 0..10 {
            let tile = RasterTile2D::<u8>::new(
                TimeInterval::new_unchecked(0, 10),
                GridIdx2D::new([i, i]),
                GeoTransform::new([0., 0.].into(), 0.5, -0.5),
                geoengine_datatypes::raster::GridOrEmpty::from(
                    Grid2D::new([2, 2].into(), vec![i as u8; 4]).unwrap(),
                ),
                CacheHint::default(),
            );
            let compressed_tile = CompressedRasterTile2D::compress_tile(tile);
            data.push(compressed_tile);
        }
        data
    }

    fn create_test_vecor_data() -> Vec<CompressedFeatureCollection<MultiPoint>> {
        let mut data = Vec::new();

        for x in 0..9 {
            let mut points = Vec::new();
            let mut strngs = Vec::new();
            for i in x..x + 2 {
                let p = MultiPoint::new(vec![(f64::from(i), f64::from(i)).into()]).unwrap();
                points.push(p);
                strngs.push(format!("test {i}"));
            }

            let collection = MultiPointCollection::from_data(
                points,
                vec![TimeInterval::default(); 2],
                HashMap::<String, FeatureData>::from([(
                    "strings".to_owned(),
                    FeatureData::Text(strngs),
                )]),
                CacheHint::default(),
            )
            .unwrap();

            let compressed_collection =
                CompressedFeatureCollection::from_collection(collection).unwrap();
            data.push(compressed_collection);
        }
        data
    }

    #[test]
    fn test_cache_stream_inner_raster() {
        let data = Arc::new(create_test_raster_data());
        let query = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((2., -2.).into(), (8., -8.).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::zero_point_five(),
        };

        let mut res = Vec::new();
        let mut inner = CacheStreamInner::new(data, query);

        // next_idx should return the idx of tiles that hit the query
        while let Some(n) = inner.next_idx() {
            res.push(n);
        }
        assert_eq!(res.len(), 6);
        assert!(res.iter().all(|&n| (2..=8).contains(&n)));
    }

    #[test]
    fn test_cache_stream_inner_vector() {
        let data = Arc::new(create_test_vecor_data());
        let query = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new_unchecked((2.1, 2.1).into(), (7.9, 7.9).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::zero_point_five(),
        };

        let mut res = Vec::new();
        let mut inner = CacheStreamInner::new(data, query);

        // next_idx should return the idx of tiles that hit the query
        while let Some(n) = inner.next_idx() {
            res.push(n);
        }
        assert_eq!(res.len(), 6);
        assert!(res.iter().all(|&n| (2..=8).contains(&n)));
    }
}
