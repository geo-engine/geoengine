mod feature_collection_merger;
mod raster_overlap_adapter;
mod raster_time;
mod raster_time_substream;

pub use feature_collection_merger::FeatureCollectionChunkMerger;
pub use raster_overlap_adapter::{
    fold_by_coordinate_lookup_future, RasterOverlapAdapter, SubQueryTileAggregator,
    TileReprojectionSubQuery,
};
pub use raster_time::RasterTimeAdapter;

use self::raster_time_substream::RasterTimeMultiFold;
use crate::util::Result;
use futures::{stream::Fuse, Future, Stream, StreamExt};
use geoengine_datatypes::{
    collections::FeatureCollection,
    primitives::Geometry,
    raster::{Pixel, RasterTile2D},
    util::arrow::ArrowTyped,
};

pub trait RasterStreamExt<P>: Stream<Item = Result<RasterTile2D<P>>>
where
    P: Pixel,
{
    fn time_multi_fold<A, AInit, F, Fut>(
        self,
        accumulator_initializer: AInit,
        f: F,
    ) -> RasterTimeMultiFold<Self, P, A, AInit, F, Fut>
    where
        Self: Sized,
        AInit: FnMut() -> A,
        F: FnMut(A, Self::Item) -> Fut,
        Fut: Future<Output = A>,
    {
        RasterTimeMultiFold::new(self, accumulator_initializer, f)
    }
}

impl<T: ?Sized, P: Pixel> RasterStreamExt<P> for T where T: Stream<Item = Result<RasterTile2D<P>>> {}

pub trait FeatureCollectionStreamExt<CollectionType>:
    Stream<Item = Result<FeatureCollection<CollectionType>>>
where
    CollectionType: Geometry + ArrowTyped + 'static,
{
    fn merge_chunks(
        self,
        chunk_size_bytes: usize,
    ) -> FeatureCollectionChunkMerger<Fuse<Self>, CollectionType>
    where
        Self: Sized,
    {
        FeatureCollectionChunkMerger::new(self.fuse(), chunk_size_bytes)
    }
}

impl<T: ?Sized, CollectionType: Geometry + ArrowTyped + 'static>
    FeatureCollectionStreamExt<CollectionType> for T
where
    T: Stream<Item = Result<FeatureCollection<CollectionType>>>,
{
}
