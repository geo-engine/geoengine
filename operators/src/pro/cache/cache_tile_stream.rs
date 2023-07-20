use std::{pin::Pin, sync::Arc};

use crate::util::Result;
use futures::Stream;
use geoengine_datatypes::{
    primitives::{RasterQueryRectangle, SpatialPartitioned},
    raster::{Pixel, RasterTile2D},
};
use pin_project::pin_project;

/// Our own tile stream that "owns" the data (more precisely a reference to the data)
#[pin_project(project = CacheTileStreamProjection)]
pub struct CacheTileStream<T> {
    data: Arc<Vec<RasterTile2D<T>>>,
    query: RasterQueryRectangle,
    idx: usize,
}

impl<T> CacheTileStream<T> {
    pub fn new(data: Arc<Vec<RasterTile2D<T>>>, query: RasterQueryRectangle) -> Self {
        Self {
            data,
            query,
            idx: 0,
        }
    }

    pub fn count_matching_elements(&self) -> usize
    where
        T: Pixel,
    {
        self.data
            .iter()
            .filter(|t| {
                let tile_bbox = t.tile_information().spatial_partition();

                (tile_bbox == self.query.spatial_bounds
                    || tile_bbox.intersects(&self.query.spatial_bounds))
                    && (t.time == self.query.time_interval
                        || t.time.intersects(&self.query.time_interval))
            })
            .count()
    }

    pub fn element_count(&self) -> usize {
        self.data.len()
    }
}

impl<T: Pixel> Stream for CacheTileStream<T> {
    type Item = Result<RasterTile2D<T>>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let CacheTileStreamProjection { data, query, idx } = self.as_mut().project();

        // return the next tile that is contained in the query, skip all tiles that are not contained
        for i in *idx..data.len() {
            let tile = &data[i];
            let tile_bbox = tile.tile_information().spatial_partition();

            if (tile_bbox == query.spatial_bounds || tile_bbox.intersects(&query.spatial_bounds))
                && (tile.time == query.time_interval || tile.time.intersects(&query.time_interval))
            {
                *idx = i + 1;
                return std::task::Poll::Ready(Some(Ok(tile.clone())));
            }
        }

        std::task::Poll::Ready(None)
    }
}

pub enum TypedCacheTileStream {
    U8(CacheTileStream<u8>),
    U16(CacheTileStream<u16>),
    U32(CacheTileStream<u32>),
    U64(CacheTileStream<u64>),
    I8(CacheTileStream<i8>),
    I16(CacheTileStream<i16>),
    I32(CacheTileStream<i32>),
    I64(CacheTileStream<i64>),
    F32(CacheTileStream<f32>),
    F64(CacheTileStream<f64>),
}
