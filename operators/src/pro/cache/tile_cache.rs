use std::{collections::HashMap, pin::Pin, sync::Arc};

use futures::Stream;
use geoengine_datatypes::{
    identifier,
    primitives::{RasterQueryRectangle, SpatialPartitioned},
    raster::{GridContains, Pixel, RasterTile2D},
    util::{test::TestDefault, Identifier},
};
use lru::LruCache;
use pin_project::pin_project;
use snafu::ensure;
use tokio::sync::RwLock;

use crate::engine::CanonicOperatorName;
use crate::util::Result;

use super::error::CacheError;

/// The tile cache caches all tiles of a query and is able to answer queries that are fully contained in the cache.
/// New tiles are inserted into the cache on-the-fly as they are produced by query processors.
/// The tiles are first inserted into a landing zone, until the query in completely finished and only then moved to the cache.
/// Both the landing zone and the cache have a maximum size.
/// If the landing zone is full, the caching of the current query will be aborted.
/// If the cache is full, the least recently used entries will be evicted if necessary to make room for the new entry.
#[derive(Debug)]
pub struct TileCacheBackend {
    // TODO: more fine granular locking?
    // for each operator graph, we have a cache, that can efficiently be accessed
    operator_caches: HashMap<CanonicOperatorName, OperatorTileCache>,

    cache_byte_size_total: usize,
    cache_byte_size_used: usize,

    landing_zone_byte_size_total: usize,
    landing_zone_byte_size_used: usize,

    // we only use the LruCache for determining the least recently used elements and evict as many entries as needed to fit the new one
    lru: LruCache<CacheEntryId, CanonicOperatorName>,
}

#[derive(Debug)]
pub struct TileCache {
    backend: RwLock<TileCacheBackend>,
}

impl TileCache {
    pub fn new(cache_size_in_mb: usize, landing_zone_ratio: f64) -> Result<Self> {
        if landing_zone_ratio <= 0.0 {
            return Err(crate::error::Error::QueryingProcessorFailed {
                source: Box::new(CacheError::LandingZoneRatioMustBeLargerThanZero),
            });
        }

        Ok(Self {
            backend: RwLock::new(TileCacheBackend {
                operator_caches: Default::default(),
                lru: LruCache::unbounded(), // we need no cap because we evict manually
                cache_byte_size_total: (cache_size_in_mb as f64
                    * (1.0 - landing_zone_ratio)
                    * 1024.0
                    * 1024.0) as usize,
                cache_byte_size_used: 0,
                landing_zone_byte_size_total: (cache_size_in_mb as f64
                    * landing_zone_ratio
                    * 1024.0
                    * 1024.0) as usize,
                landing_zone_byte_size_used: 0,
            }),
        })
    }
}

impl TestDefault for TileCache {
    fn test_default() -> Self {
        Self {
            backend: RwLock::new(TileCacheBackend {
                operator_caches: Default::default(),
                lru: LruCache::unbounded(), // we need no cap because we evict manually
                cache_byte_size_total: usize::MAX,
                cache_byte_size_used: 0,
                landing_zone_byte_size_total: usize::MAX,
                landing_zone_byte_size_used: 0,
            }),
        }
    }
}

/// Holds all the cached results for an operator graph (workflow)
#[derive(Debug, Default)]
pub struct OperatorTileCache {
    // for a given operator and query we need to look through all entries to find one that matches
    // TODO: use a multi-dimensional index to speed up the lookup
    entries: HashMap<CacheEntryId, CacheEntry>,

    // running queries insert their tiles as they are produced. The entry will be created once the query is done.
    // The query is identified by a Uuid instead of the query rectangle to avoid confusions with other queries
    landing_zone: HashMap<QueryId, LandingZoneEntry>,
}

identifier!(QueryId);

impl OperatorTileCache {
    pub fn find_match(&self, query: &RasterQueryRectangle) -> Option<(&CacheEntryId, &CacheEntry)> {
        self.entries.iter().find(|(_id, r)| r.matches(query))
    }
}

/// Holds all the tiles for a given query and is able to answer queries that are fully contained
#[derive(Debug)]
pub struct CacheEntry {
    query: RasterQueryRectangle,
    tiles: CachedTiles,
}

identifier!(CacheEntryId);

impl CacheEntry {
    /// Return true if the query can be answered in full by this cache entry
    /// For this, the bbox and time has to be fully contained, and the spatial resolution has to match
    pub fn matches(&self, query: &RasterQueryRectangle) -> bool {
        (self.query.spatial_bounds == query.spatial_bounds
            || self.query.spatial_bounds.geo_transform == query.spatial_bounds.geo_transform
                && self
                    .query
                    .spatial_bounds
                    .grid_bounds
                    .contains(&query.spatial_bounds.grid_bounds))
            && self.query.time_interval.contains(&query.time_interval)
    }

    /// Produces a tile stream from the cache
    pub fn tile_stream(&self, query: &RasterQueryRectangle) -> TypedCacheTileStream {
        self.tiles.tile_stream(query)
    }

    fn byte_size(&self) -> usize {
        self.tiles.byte_size() + std::mem::size_of::<RasterQueryRectangle>()
    }
}

#[derive(Debug)]
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

impl CachedTiles {
    fn byte_size(&self) -> usize {
        std::mem::size_of::<CachedTiles>()
            + match self {
                CachedTiles::U8(v) => v.len() * std::mem::size_of::<RasterTile2D<u8>>(),
                CachedTiles::U16(v) => v.len() * std::mem::size_of::<RasterTile2D<u16>>(),
                CachedTiles::U32(v) => v.len() * std::mem::size_of::<RasterTile2D<u32>>(),
                CachedTiles::U64(v) => v.len() * std::mem::size_of::<RasterTile2D<u64>>(),
                CachedTiles::I8(v) => v.len() * std::mem::size_of::<RasterTile2D<i8>>(),
                CachedTiles::I16(v) => v.len() * std::mem::size_of::<RasterTile2D<i16>>(),
                CachedTiles::I32(v) => v.len() * std::mem::size_of::<RasterTile2D<i32>>(),
                CachedTiles::I64(v) => v.len() * std::mem::size_of::<RasterTile2D<i64>>(),
                CachedTiles::F32(v) => v.len() * std::mem::size_of::<RasterTile2D<f32>>(),
                CachedTiles::F64(v) => v.len() * std::mem::size_of::<RasterTile2D<f64>>(),
            }
    }
}

#[derive(Debug)]
struct LandingZoneEntry {
    query: RasterQueryRectangle,
    tiles: LandingZoneQueryTiles,
}

impl LandingZoneEntry {
    fn byte_size(&self) -> usize {
        self.tiles.byte_size() + std::mem::size_of::<RasterQueryRectangle>()
    }
}

#[derive(Debug)]
pub enum LandingZoneQueryTiles {
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

impl LandingZoneQueryTiles {
    fn byte_size(&self) -> usize {
        // TODO: include size of the Vec itself
        match self {
            LandingZoneQueryTiles::U8(v) => v.len() * std::mem::size_of::<RasterTile2D<u8>>(),
            LandingZoneQueryTiles::U16(v) => v.len() * std::mem::size_of::<RasterTile2D<u16>>(),
            LandingZoneQueryTiles::U32(v) => v.len() * std::mem::size_of::<RasterTile2D<u32>>(),
            LandingZoneQueryTiles::U64(v) => v.len() * std::mem::size_of::<RasterTile2D<u64>>(),
            LandingZoneQueryTiles::I8(v) => v.len() * std::mem::size_of::<RasterTile2D<i8>>(),
            LandingZoneQueryTiles::I16(v) => v.len() * std::mem::size_of::<RasterTile2D<i16>>(),
            LandingZoneQueryTiles::I32(v) => v.len() * std::mem::size_of::<RasterTile2D<i32>>(),
            LandingZoneQueryTiles::I64(v) => v.len() * std::mem::size_of::<RasterTile2D<i64>>(),
            LandingZoneQueryTiles::F32(v) => v.len() * std::mem::size_of::<RasterTile2D<f32>>(),
            LandingZoneQueryTiles::F64(v) => v.len() * std::mem::size_of::<RasterTile2D<f64>>(),
        }
    }
}

impl From<LandingZoneQueryTiles> for CachedTiles {
    fn from(value: LandingZoneQueryTiles) -> Self {
        match value {
            LandingZoneQueryTiles::U8(t) => CachedTiles::U8(Arc::new(t)),
            LandingZoneQueryTiles::U16(t) => CachedTiles::U16(Arc::new(t)),
            LandingZoneQueryTiles::U32(t) => CachedTiles::U32(Arc::new(t)),
            LandingZoneQueryTiles::U64(t) => CachedTiles::U64(Arc::new(t)),
            LandingZoneQueryTiles::I8(t) => CachedTiles::I8(Arc::new(t)),
            LandingZoneQueryTiles::I16(t) => CachedTiles::I16(Arc::new(t)),
            LandingZoneQueryTiles::I32(t) => CachedTiles::I32(Arc::new(t)),
            LandingZoneQueryTiles::I64(t) => CachedTiles::I64(Arc::new(t)),
            LandingZoneQueryTiles::F32(t) => CachedTiles::F32(Arc::new(t)),
            LandingZoneQueryTiles::F64(t) => CachedTiles::F64(Arc::new(t)),
        }
    }
}

impl From<LandingZoneEntry> for CacheEntry {
    fn from(value: LandingZoneEntry) -> Self {
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

        // return the next tile that is contained in the query, skip all tiles that are not contained
        for i in *idx..data.len() {
            let tile = &data[i];
            let tile_bbox = tile.tile_information().spatial_partition();

            if tile_bbox.intersects(&query.spatial_bounds.spatial_partition()) // TODO: use the Grid Bounds directly
                && tile.time.intersects(&query.time_interval)
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
        let mut backend = self.backend.write().await;

        let (entry_id, typed_stream) = {
            let cache = backend.operator_caches.get(&key)?;

            let (id, entry) = cache.find_match(query)?;

            let typed_stream = entry.tile_stream(query);

            (*id, typed_stream)
        };

        // set as most recently used
        backend.lru.promote(&entry_id);

        T::stream(typed_stream)
    }

    /// When inserting a new query, we first register the query and then insert the tiles as they are produced
    /// This is to avoid confusing different queries on the same operator and query rectangle
    pub async fn insert_query<T: Pixel + Cachable>(
        &self,
        key: CanonicOperatorName,
        query: &RasterQueryRectangle,
    ) -> Result<QueryId, CacheError> {
        let mut backend = self.backend.write().await;

        let mut entry_size =
            std::mem::size_of::<QueryId>() + std::mem::size_of::<RasterQueryRectangle>(); // TODO: incorporate overhead for empty tiles vec(?)

        if !backend.operator_caches.contains_key(&key) {
            entry_size += key.byte_size();
        }

        ensure!(
            backend.landing_zone_byte_size_used + entry_size
                <= backend.landing_zone_byte_size_total,
            crate::pro::cache::error::NotEnoughSpaceInLandingZone
        );

        let cache = backend.operator_caches.entry(key).or_default();

        let query_id = QueryId::new();
        cache.landing_zone.insert(
            query_id,
            LandingZoneEntry {
                query: *query,
                tiles: T::create_active_query_tiles(),
            },
        );

        Ok(query_id)
    }

    /// Insert a tile for a given query. The query has to be inserted first.
    /// The tile is inserted into the landing zone and only moved to the cache when the query is finished.
    /// If the landing zone is full, the caching of the query is aborted.
    pub async fn insert_tile<T>(
        &self,
        key: CanonicOperatorName,
        query_id: QueryId,
        tile: RasterTile2D<T>,
    ) -> Result<(), CacheError>
    where
        T: Pixel + Cachable,
    {
        let mut backend = self.backend.write().await;

        // check if landing zone has enough space, otherwise abort query
        if backend.landing_zone_byte_size_used + std::mem::size_of::<RasterTile2D<T>>()
            > backend.landing_zone_byte_size_total
        {
            let cache = backend.operator_caches.entry(key).or_default();

            let entry = cache.landing_zone.remove(&query_id);

            if let Some(entry) = entry {
                backend.landing_zone_byte_size_used -= entry.byte_size();
            }

            return Err(super::error::CacheError::NotEnoughSpaceInLandingZone);
        }

        let cache = backend.operator_caches.entry(key).or_default();

        let entry = cache
            .landing_zone
            .get_mut(&query_id)
            .ok_or(super::error::CacheError::QueryNotFoundInLandingZone)?;

        T::insert_tile(&mut entry.tiles, tile)?;

        backend.landing_zone_byte_size_used += entry.byte_size();

        Ok(())
    }

    /// Abort the query and remove the tiles from the cache
    pub async fn abort_query(&self, key: CanonicOperatorName, query_id: QueryId) {
        let mut backend = self.backend.write().await;

        let cache = backend.operator_caches.entry(key).or_default();
        let entry = cache.landing_zone.remove(&query_id);

        // update landing zone
        if let Some(entry) = entry {
            backend.landing_zone_byte_size_used -= entry.byte_size();
        }
    }

    /// Finish the query and make the tiles available in the cache
    pub async fn finish_query(
        &self,
        key: CanonicOperatorName,
        query_id: QueryId,
    ) -> Result<(), CacheError> {
        // TODO: maybe check if this cache result is already in the cache or could displace another one

        let mut backend = self.backend.write().await;

        // this should always work, because the query was inserted at some point and then the cache entry was created
        let cache = backend
            .operator_caches
            .get_mut(&key)
            .ok_or(super::error::CacheError::QueryNotFoundInLandingZone)?;

        let active_query = cache
            .landing_zone
            .remove(&query_id)
            .ok_or(super::error::CacheError::QueryNotFoundInLandingZone)?;

        let loading_zone_entry_size = active_query.byte_size(); // The loading zone entry might have a different size than the cache entry

        // move entry from landing zone into cache
        let entry: CacheEntry = active_query.into();
        let entry_id = CacheEntryId::new();

        // calculate size of cache entry. This might be different from the size of the landing zone entry.
        let cache_entry_size = entry.byte_size();
        cache.entries.insert(entry_id, entry);

        backend.lru.push(entry_id, key);
        // reduce the size of the landing zone
        backend.landing_zone_byte_size_used -= loading_zone_entry_size;
        // TODO: include the size of the entry in the lru and the operator_caches as well(?)
        // increase the size of the cache
        backend.cache_byte_size_used += cache_entry_size;

        // cache bound can be temporarily exceeded as the entry is moved form the landing zone into the cache
        // but the total of cache + landing zone is still below the bound
        // We now evict elements from the cache until bound is satisfied again
        while backend.cache_byte_size_used > backend.cache_byte_size_total {
            // this should always work, because otherwise it would mean the cache is not empty but the lru is.
            // the landing zone is smaller than the cache size and the entry must fit into the landing zone.
            if let Some(entry) = backend
                .lru
                .pop_lru()
                .and_then(|(id, op)| backend.operator_caches.get_mut(&op).map(|c| (id, c)))
                .and_then(|(id, evict_cache)| evict_cache.entries.remove(&id))
            {
                backend.cache_byte_size_used -= entry.byte_size();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{DateTime, SpatialPartition2D, SpatialResolution, TimeInterval},
        raster::{Grid, RasterProperties},
    };
    use serde_json::json;

    use super::*;

    async fn process_query(tile_cache: &mut TileCache, op_name: CanonicOperatorName) {
        let query_id = tile_cache
            .insert_query::<u8>(op_name.clone(), &query_rect())
            .await
            .unwrap();

        tile_cache
            .insert_tile(op_name.clone(), query_id, create_tile())
            .await
            .unwrap();

        tile_cache
            .finish_query(op_name.clone(), query_id)
            .await
            .unwrap();
    }

    fn create_tile() -> RasterTile2D<u8> {
        RasterTile2D::<u8> {
            time: TimeInterval::new_unchecked(1, 1),
            tile_position: [-1, 0].into(),
            global_geo_transform: TestDefault::test_default(),
            grid_array: Grid::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
            properties: RasterProperties::default(),
        }
    }

    fn query_rect() -> RasterQueryRectangle {
        RasterQueryRectangle::with_partition_and_resolution(
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into()),
            SpatialResolution::one(),
            TimeInterval::new_instant(DateTime::new_utc(2014, 3, 1, 0, 0, 0)).unwrap(),
        )
    }

    fn op(idx: usize) -> CanonicOperatorName {
        CanonicOperatorName::new_unchecked(&json!({
            "type": "GdalSource",
            "params": {
                "data": idx
            }
        }))
    }

    #[tokio::test]
    async fn it_evicts_lru() {
        // Create cache entry and landing zone entry to geht the size of both
        let landing_zone_entry = LandingZoneEntry {
            query: query_rect(),
            tiles: LandingZoneQueryTiles::U8(vec![create_tile()]),
        };
        let size_of_landing_zone_entry = landing_zone_entry.byte_size();
        let cache_entry: CacheEntry = landing_zone_entry.into();
        let size_of_cache_entry = cache_entry.byte_size();
        // Select the max of both sizes
        // This is done because the landing zone should not be smaller then the cache
        let m_size = size_of_cache_entry.max(size_of_landing_zone_entry);

        // set limits s.t. three tiles fit
        let mut tile_cache = TileCache {
            backend: RwLock::new(TileCacheBackend {
                operator_caches: Default::default(),
                lru: LruCache::unbounded(),
                cache_byte_size_total: m_size * 3,
                cache_byte_size_used: 0,
                landing_zone_byte_size_total: m_size * 3,
                landing_zone_byte_size_used: 0,
            }),
        };
        // process three different queries
        process_query(&mut tile_cache, op(1)).await;
        process_query(&mut tile_cache, op(2)).await;
        process_query(&mut tile_cache, op(3)).await;

        // query the first one s.t. it is the most recently used
        tile_cache
            .query_cache::<u8>(op(1), &query_rect())
            .await
            .unwrap();
        // process a fourth query
        process_query(&mut tile_cache, op(4)).await;

        // assure the seconds query is evicted because it is the least recently used
        assert!(tile_cache
            .query_cache::<u8>(op(2), &query_rect())
            .await
            .is_none());

        // assure that the other queries are still in the cache
        for i in [1, 3, 4] {
            assert!(tile_cache
                .query_cache::<u8>(op(i), &query_rect())
                .await
                .is_some());
        }
    }
}
