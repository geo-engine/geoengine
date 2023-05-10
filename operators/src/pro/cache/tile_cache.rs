use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    pin::Pin,
    sync::Arc,
};

use futures::{
    stream::{self, BoxStream},
    Stream, StreamExt,
};
use geoengine_datatypes::{
    primitives::RasterQueryRectangle,
    raster::{Pixel, RasterTile2D},
};
use pin_project::pin_project;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::engine::{CanonicOperatorName, InitializedRasterOperator};
use crate::util::Result;
use geoengine_datatypes::raster::TypedRasterTile2D;

/// The tile cache caches all tiles of a query and is able to answer queries that are fully contained in the cache.
#[derive(Default)]
pub struct TileCache {
    // TODO: more fine granular locking?
    // for each operator graph, we have a cache, that can efficiently be accessed
    operator_caches: RwLock<HashMap<CanonicOperatorName, OperatorTileCache>>,
}

/// Holds all the cached results for an operator graph (workflow)
#[derive(Default)]
pub struct OperatorTileCache {
    // for a given operator and query we need to look through all entries to find one that matches
    // TODO: use a multi-dimensional index to speed up the lookup
    entries: Vec<CachedQueryResult>,

    // running queries insert their tiles as they are produced. The entry will be created once the query is done.
    // The query is identified by a Uuid instead of the query rectangle to avoid confusions with other queries
    active_queries: HashMap<QueryId, ActiveQueryResult>,
}

pub type QueryId = Uuid;

impl OperatorTileCache {
    pub fn find_match(&self, query: &RasterQueryRectangle) -> Option<&CachedQueryResult> {
        self.entries.iter().find(|r| r.matches(query))
    }
}

/// Holds all the tiles for a given query and is able to answer queries that are fully contained
pub struct CachedQueryResult {
    query: RasterQueryRectangle,
    tiles: CachedTiles,
}

impl CachedQueryResult {
    /// Return true if the query can be answered in full by this cache entry
    /// For this, the bbox and time has to be fully contained, and the spatial resolution has to match
    pub fn matches(&self, query: &RasterQueryRectangle) -> bool {
        // TODO: implement
        todo!()
    }

    /// Produces a tile stream from the cache
    pub fn tile_stream(&self, query: &RasterQueryRectangle) -> TypedCacheTileStream {
        self.tiles.tile_stream(query)
    }
}

pub enum CachedTiles {
    U8(Arc<Vec<RasterTile2D<u8>>>),
    U16(Arc<Vec<RasterTile2D<u16>>>),
    U32(Arc<Vec<RasterTile2D<u32>>>),
    U64(Arc<Vec<RasterTile2D<u64>>>),
    I8(Arc<Vec<RasterTile2D<i8>>>),
    I16(Arc<Vec<RasterTile2D<i16>>>),
    I32(Arc<Vec<RasterTile2D<i32>>>),
    I64(Arc<Vec<RasterTile2D<i64>>>),
    F32(Arc<Vec<RasterTile2D<f32>>>),
    F64(Arc<Vec<RasterTile2D<f64>>>),
}

struct ActiveQueryResult {
    query: RasterQueryRectangle,
    tiles: ActiveQueryTiles,
}

pub enum ActiveQueryTiles {
    U8(Vec<RasterTile2D<u8>>),
    U16(Vec<RasterTile2D<u16>>),
    U32(Vec<RasterTile2D<u32>>),
    U64(Vec<RasterTile2D<u64>>),
    I8(Vec<RasterTile2D<i8>>),
    I16(Vec<RasterTile2D<i16>>),
    I32(Vec<RasterTile2D<i32>>),
    I64(Vec<RasterTile2D<i64>>),
    F32(Vec<RasterTile2D<f32>>),
    F64(Vec<RasterTile2D<f64>>),
}

impl From<ActiveQueryTiles> for CachedTiles {
    fn from(value: ActiveQueryTiles) -> Self {
        match value {
            ActiveQueryTiles::U8(t) => CachedTiles::U8(Arc::new(t)),
            ActiveQueryTiles::U16(t) => CachedTiles::U16(Arc::new(t)),
            ActiveQueryTiles::U32(t) => CachedTiles::U32(Arc::new(t)),
            ActiveQueryTiles::U64(t) => CachedTiles::U64(Arc::new(t)),
            ActiveQueryTiles::I8(t) => CachedTiles::I8(Arc::new(t)),
            ActiveQueryTiles::I16(t) => CachedTiles::I16(Arc::new(t)),
            ActiveQueryTiles::I32(t) => CachedTiles::I32(Arc::new(t)),
            ActiveQueryTiles::I64(t) => CachedTiles::I64(Arc::new(t)),
            ActiveQueryTiles::F32(t) => CachedTiles::F32(Arc::new(t)),
            ActiveQueryTiles::F64(t) => CachedTiles::F64(Arc::new(t)),
        }
    }
}

impl From<ActiveQueryResult> for CachedQueryResult {
    fn from(value: ActiveQueryResult) -> Self {
        Self {
            query: value.query,
            tiles: value.tiles.into(),
        }
    }
}

impl CachedTiles {
    pub fn tile_stream(&self, query: &RasterQueryRectangle) -> TypedCacheTileStream {
        match self {
            CachedTiles::U8(v) => TypedCacheTileStream::U8(CacheTileStream::new(v.clone(), *query)),
            CachedTiles::U16(v) => {
                TypedCacheTileStream::U16(CacheTileStream::new(v.clone(), *query))
            }
            CachedTiles::U32(v) => {
                TypedCacheTileStream::U32(CacheTileStream::new(v.clone(), *query))
            }
            CachedTiles::U64(v) => {
                TypedCacheTileStream::U64(CacheTileStream::new(v.clone(), *query))
            }
            CachedTiles::I8(v) => TypedCacheTileStream::I8(CacheTileStream::new(v.clone(), *query)),
            CachedTiles::I16(v) => {
                TypedCacheTileStream::I16(CacheTileStream::new(v.clone(), *query))
            }
            CachedTiles::I32(v) => {
                TypedCacheTileStream::I32(CacheTileStream::new(v.clone(), *query))
            }
            CachedTiles::I64(v) => {
                TypedCacheTileStream::I64(CacheTileStream::new(v.clone(), *query))
            }
            CachedTiles::F32(v) => {
                TypedCacheTileStream::F32(CacheTileStream::new(v.clone(), *query))
            }
            CachedTiles::F64(v) => {
                TypedCacheTileStream::F64(CacheTileStream::new(v.clone(), *query))
            }
        }
    }
}

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
}

impl<T: Pixel> Stream for CacheTileStream<T> {
    type Item = Result<RasterTile2D<T>>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let CacheTileStreamProjection { data, query, idx } = self.as_mut().project();

        // return the next tile that is contained in the query
        for i in *idx..data.len() {
            let tile = &data[i];

            if true
            /* TODO: check query contains tile */
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

/// A helper trait that allows getting the concrete stream from a typed stream
pub trait Cachable: Sized {
    fn stream(b: TypedCacheTileStream) -> Option<CacheTileStream<Self>>;

    fn insert_tile(tiles: &mut ActiveQueryTiles, tile: RasterTile2D<Self>) -> Result<()>;

    fn create_active_query_tiles() -> ActiveQueryTiles;
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

            fn insert_tile(tiles: &mut ActiveQueryTiles, tile: RasterTile2D<Self>) -> Result<()> {
                if let ActiveQueryTiles::$variant(ref mut tiles) = tiles {
                    tiles.push(tile);
                    return Ok(());
                }
                Err(crate::error::Error::QueryProcessor)
            }

            fn create_active_query_tiles() -> ActiveQueryTiles {
                ActiveQueryTiles::$variant(Vec::new())
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

impl TileCache {
    /// Query the cache and on hit create a stream of tiles
    pub async fn query_cache<T>(
        &self,
        key: CanonicOperatorName,
        query: &RasterQueryRectangle,
    ) -> Option<CacheTileStream<T>>
    where
        T: Pixel + Cachable,
    {
        let caches = self.operator_caches.read().await;
        let cache = caches.get(&key)?;

        let entry = cache.find_match(query)?;
        let typed_stream = entry.tile_stream(query);
        T::stream(typed_stream)
    }

    /// When inserting a new query, we first register the query and then insert the tiles as they are produced
    /// This is to avoid confusing different queries on the same operator and query rectangle
    pub async fn insert_query<T: Pixel + Cachable>(
        &self,
        key: CanonicOperatorName,
        query: &RasterQueryRectangle,
    ) -> QueryId {
        let mut caches = self.operator_caches.write().await;
        let cache = caches.entry(key).or_default();

        let query_id = Uuid::new_v4();
        cache.active_queries.insert(
            query_id,
            ActiveQueryResult {
                query: *query,
                tiles: T::create_active_query_tiles(),
            },
        );

        query_id
    }

    /// Insert a tile for a given query. The query has to be inserted first.
    pub async fn insert_tile<T>(
        &self,
        key: CanonicOperatorName,
        query_id: QueryId,
        tile: RasterTile2D<T>,
    ) -> Result<()>
    where
        T: Pixel + Cachable,
    {
        let mut caches = self.operator_caches.write().await;
        let cache = caches.entry(key).or_default();

        let entry = cache
            .active_queries
            .get_mut(&query_id)
            .ok_or(crate::error::Error::QueryProcessor)?; // TODO: better error

        T::insert_tile(&mut entry.tiles, tile)?;

        Ok(())
    }

    /// Abort the query and remove the tiles from the cache
    pub async fn abort_query(&self, key: CanonicOperatorName, query_id: QueryId) {
        let mut caches = self.operator_caches.write().await;
        let cache = caches.entry(key).or_default();
        cache.active_queries.remove(&query_id);
    }

    /// Finish the query and make the tiles available in the cache
    pub async fn finish_query(&self, key: CanonicOperatorName, query_id: QueryId) -> Result<()> {
        let mut caches = self.operator_caches.write().await;
        let cache = caches.entry(key).or_default();
        let active_query = cache
            .active_queries
            .remove(&query_id)
            .ok_or(crate::error::Error::QueryProcessor)?; // TODO: better error

        // TODO: maybe check if this cache result is already in the cache or could displace another one
        cache.entries.push(active_query.into());

        Ok(())
    }
}
