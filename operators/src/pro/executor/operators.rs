use crate::engine::{QueryContext, RasterQueryProcessor, VectorQueryProcessor};
use crate::util::Result;
use futures::stream::BoxStream;
use futures::task::{Context, Poll};
use futures::{Future, Stream};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BoundingBox2D, QueryRectangle, RasterQueryRectangle, SpatialPartition2D,
    VectorQueryRectangle,
};
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use ouroboros::self_referencing;
use std::pin::Pin;

/// Turns a [`QueryProcessor`][crate::engine::QueryProcessor] into a [Stream] of results.
pub trait OneshotQueryProcessor<C: QueryContext, Output> {
    type BBox: AxisAlignedRectangle;
    type Output: Stream + 'static;

    fn into_stream(
        self,
        qr: QueryRectangle<Self::BBox>,
        ctx: C,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Output>>>>;
}

/// Helper struct to tie together a [result stream][Stream] with the bounding [`VectorQueryProcessor`].
#[self_referencing]
struct FeatureStreamWrapper<V, C>
where
    V: 'static,
    C: 'static,
{
    proc: Box<dyn VectorQueryProcessor<VectorType = V>>,
    ctx: C,
    #[borrows(proc, ctx)]
    #[covariant]
    stream: BoxStream<'this, Result<V>>,
}

/// A helper to generate a static [`Stream`] for a given [`VectorQueryProcessor`].
pub struct FeatureStreamBoxer<V, C>
where
    V: 'static,
    C: 'static,
{
    h: FeatureStreamWrapper<V, C>,
}

impl<V, C> FeatureStreamBoxer<V, C>
where
    V: 'static,
    C: QueryContext + 'static,
{
    /// Consumes a [`VectorQueryProcessor`], a [`VectorQueryRectangle`] and a [`QueryContext`] and
    /// returns a `'static` [`Stream`] that can be passed to an [`Executor`][crate::pro::executor::Executor].
    pub async fn new(
        proc: Box<dyn VectorQueryProcessor<VectorType = V>>,
        qr: VectorQueryRectangle,
        ctx: C,
    ) -> Result<Self> {
        let h = FeatureStreamWrapperAsyncTryBuilder {
            proc,
            ctx,
            stream_builder: |proc, ctx| Box::pin(async move { proc.vector_query(qr, ctx).await }),
        }
        .try_build()
        .await?;
        Ok(Self { h })
    }
}

impl<V, C> Stream for FeatureStreamBoxer<V, C>
where
    V: 'static,
    C: QueryContext + 'static,
{
    type Item = Result<V>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .h
            .with_stream_mut(|s| Pin::new(s).poll_next(cx))
    }
}

impl<C, V> OneshotQueryProcessor<C, V> for Box<dyn VectorQueryProcessor<VectorType = V>>
where
    V: 'static,
    C: QueryContext + 'static,
{
    type BBox = BoundingBox2D;
    type Output = FeatureStreamBoxer<V, C>;

    fn into_stream(
        self,
        qr: QueryRectangle<Self::BBox>,
        ctx: C,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Output>>>> {
        Box::pin(FeatureStreamBoxer::new(self, qr, ctx))
    }
}

/// Helper struct to tie together a [result stream][Stream] with the bounding [`RasterQueryProcessor`].
#[self_referencing]
struct RasterStreamWrapper<RasterType, C>
where
    RasterType: 'static,
    C: 'static,
{
    proc: Box<dyn RasterQueryProcessor<RasterType = RasterType>>,
    ctx: C,
    #[borrows(proc, ctx)]
    #[covariant]
    stream: BoxStream<'this, Result<RasterTile2D<RasterType>>>,
}

pub struct RasterStreamBoxer<RasterType, C>
where
    RasterType: 'static,
    C: 'static,
{
    h: RasterStreamWrapper<RasterType, C>,
}

impl<RasterType, C> RasterStreamBoxer<RasterType, C>
where
    RasterType: Pixel + 'static,
    C: QueryContext + 'static,
{
    /// Consumes a [`RasterQueryProcessor`], a [`RasterQueryRectangle`] and a [`QueryContext`] and
    /// returns a `'static` [`Stream`] that can be passed to an [`Executor`][crate::pro::executor::Executor].
    pub async fn new(
        proc: Box<dyn RasterQueryProcessor<RasterType = RasterType>>,
        qr: RasterQueryRectangle,
        ctx: C,
    ) -> Result<Self> {
        let h = RasterStreamWrapperAsyncTryBuilder {
            proc,
            ctx,
            stream_builder: |proc, ctx| Box::pin(async move { proc.raster_query(qr, ctx).await }),
        }
        .try_build()
        .await?;
        Ok(Self { h })
    }
}

impl<RasterType, C> Stream for RasterStreamBoxer<RasterType, C>
where
    RasterType: Pixel + 'static,
    C: QueryContext + 'static,
{
    type Item = Result<RasterTile2D<RasterType>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .h
            .with_stream_mut(|s| Pin::new(s).poll_next(cx))
    }
}

impl<C, RasterType> OneshotQueryProcessor<C, RasterType>
    for Box<dyn RasterQueryProcessor<RasterType = RasterType>>
where
    RasterType: Pixel + 'static,
    C: QueryContext + 'static,
{
    type BBox = SpatialPartition2D;
    type Output = RasterStreamBoxer<RasterType, C>;

    fn into_stream(
        self,
        qr: QueryRectangle<Self::BBox>,
        ctx: C,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Output>>>> {
        Box::pin(RasterStreamBoxer::new(self, qr, ctx))
    }
}
