use std::{pin::Pin, sync::Arc};

use super::{error::CacheError, tile_cache::LandingZoneQueryTiles};
use crate::util::Result;
use futures::Stream;
use geoengine_datatypes::{
    collections::{FeatureCollection, FeatureCollectionInfos, GeometryCollection},
    primitives::{Geometry, MultiPoint, MultiPolygon, NoGeometry, VectorQueryRectangle},
};
use pin_project::pin_project;

/// Our own tile stream that "owns" the data (more precisely a reference to the data)
#[pin_project(project = CacheChunkStreamProjection)]
pub struct CacheChunkStream<G> {
    data: Arc<Vec<FeatureCollection<G>>>,
    query: VectorQueryRectangle,
    idx: usize,
}

impl<G> CacheChunkStream<G>
where
    G: Geometry,
    FeatureCollection<G>: GeometryCollection + FeatureCollectionInfos,
{
    pub fn new(data: Arc<Vec<FeatureCollection<G>>>, query: VectorQueryRectangle) -> Self {
        Self {
            data,
            query,
            idx: 0,
        }
    }

    pub fn count_matching_elements(&self) -> usize
    where
        G: Geometry,
    {
        self.data
            .iter()
            .filter(|t| {
                let feature_collection_bbox = if let Some(bbox) = t.bbox() {
                    bbox
                } else {
                    return false;
                };

                let time_bounds = if let Some(ti) = t.time_bounds() {
                    ti
                } else {
                    return false;
                };

                (feature_collection_bbox == self.query.spatial_bounds
                    || feature_collection_bbox.intersects_bbox(&self.query.spatial_bounds))
                    && (time_bounds == self.query.time_interval
                        || time_bounds.intersects(&self.query.time_interval))
            })
            .count()
    }

    pub fn element_count(&self) -> usize {
        self.data.len()
    }
}

impl<G: Geometry> Stream for CacheChunkStream<G> {
    type Item = Result<FeatureCollection<G>>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let CacheChunkStreamProjection { data, query, idx } = self.as_mut().project();

        // return the next tile that is contained in the query, skip all tiles that are not contained
        for i in *idx..data.len() {
            let chunk = &data[i];
            let chunk_bbox = if let Some(bbox) = chunk.bbox() {
                bbox
            } else {
                continue;
            };

            let time_bounds = if let Some(ti) = chunk.time_bounds() {
                ti
            } else {
                return false;
            };

            if (chunk_bbox == query.spatial_bounds
                || chunk_bbox.intersects_bbox(&query.spatial_bounds))
                && (time_bounds == query.time_interval
                    || time_bounds.intersects(&query.time_interval))
            {
                *idx = i + 1;
                return std::task::Poll::Ready(Some(Ok(tile.clone())));
            }
        }

        std::task::Poll::Ready(None)
    }
}

pub enum TypedChunkStream {
    Data(CacheChunkStream<NoGeometry>),
    MultiPoint(CacheChunkStream<MultiPoint>),
    MultiLineString(CacheChunkStream<MultiLineString>),
    MultiPolygon(CacheChunkStream<MultiPolygon>),
}

/// A helper trait that allows converting between enums variants and generic structs
pub trait Cachable: Sized {
    fn stream(b: TypedCacheTileStream) -> Option<CacheTileStream<Self>>;

    fn insert_tile(
        tiles: &mut LandingZoneQueryTiles,
        tile: RasterTile2D<Self>,
    ) -> Result<(), CacheError>;

    fn create_active_query_tiles() -> LandingZoneQueryTiles;
}

macro_rules! impl_tile_streamer {
    ($t:ty, $variant:ident) => {
        impl Cachable for $t {
            fn stream(t: TypedCacheTileStream) -> Option<CacheTileStream<$t>> {
                if let TypedCacheTileStream::$variant(s) = t {
                    return Some(s);
                }
                None
            }

            fn insert_tile(
                tiles: &mut LandingZoneQueryTiles,
                tile: RasterTile2D<Self>,
            ) -> Result<(), CacheError> {
                if let LandingZoneQueryTiles::$variant(ref mut tiles) = tiles {
                    tiles.push(tile);
                    return Ok(());
                }
                Err(super::error::CacheError::InvalidRasterDataTypeForInsertion.into())
            }

            fn create_active_query_tiles() -> LandingZoneQueryTiles {
                LandingZoneQueryTiles::$variant(Vec::new())
            }
        }
    };
}
impl_tile_streamer!(i8, I8);
impl_tile_streamer!(u8, U8);
impl_tile_streamer!(i16, I16);
impl_tile_streamer!(u16, U16);
impl_tile_streamer!(i32, I32);
impl_tile_streamer!(u32, U32);
impl_tile_streamer!(i64, I64);
impl_tile_streamer!(u64, U64);
impl_tile_streamer!(f32, F32);
impl_tile_streamer!(f64, F64);
