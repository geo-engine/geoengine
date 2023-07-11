use std::{collections::HashMap, pin::Pin, sync::Arc};

use futures::Stream;
use geoengine_datatypes::{
    identifier,
    primitives::{RasterQueryRectangle, SpatialPartitioned},
    raster::{Pixel, RasterTile2D},
    util::{test::TestDefault, ByteSize, Identifier},
};
use log::{debug, trace};
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

    cache_size: TileCacheBackendSize,
    landing_zone_size: TileCacheBackendSize,

    // we only use the LruCache for determining the least recently used elements and evict as many entries as needed to fit the new one
    lru: LruCache<CacheEntryId, CanonicOperatorName>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TileCacheBackendSize {
    byte_size_total: usize,
    byte_size_used: usize,
}

impl TileCacheBackendSize {
    pub fn new(byte_size_total: usize) -> Self {
        Self {
            byte_size_total,
            byte_size_used: 0,
        }
    }

    #[inline]
    pub fn total_byte_size(&self) -> usize {
        self.byte_size_total
    }

    #[inline]
    pub fn byte_size_used(&self) -> usize {
        self.byte_size_used
    }

    #[inline]
    pub fn byte_size_free(&self) -> usize {
        if self.byte_size_used > self.byte_size_total {
            0
        } else {
            self.byte_size_total - self.byte_size_used
        }
    }

    #[inline]
    pub fn size_used_fraction(&self) -> f64 {
        self.byte_size_used as f64 / self.byte_size_total as f64
    }

    #[inline]
    pub fn can_fit_bytes(&self, bytes: usize) -> bool {
        self.byte_size_free() >= bytes
    }

    #[inline]
    pub fn can_fit_element_bytes<T: ByteSize>(&self, element: &T) -> bool {
        self.can_fit_bytes(element.byte_size())
    }

    #[inline]
    pub fn is_overfull(&self) -> bool {
        self.byte_size_used > self.byte_size_total
    }

    #[inline]
    fn add_bytes(&mut self, bytes: usize) {
        debug_assert!(
            self.can_fit_bytes(bytes),
            "adding too many bytes {} for free capacity {}",
            bytes,
            self.byte_size_free()
        );
        self.byte_size_used += bytes;
    }

    #[inline]
    #[cfg(test)] // used for tests
    fn add_element_bytes<T: ByteSize>(&mut self, element: &T) {
        self.add_bytes(element.byte_size());
    }

    #[inline]
    fn remove_bytes(&mut self, bytes: usize) {
        debug_assert!(
            self.byte_size_used >= bytes,
            "removing more bytes {} than used {}",
            bytes,
            self.byte_size_used
        );
        self.byte_size_used -= bytes;
    }

    #[inline]
    fn remove_element_bytes<T: ByteSize>(&mut self, element: &T) {
        self.remove_bytes(element.byte_size());
    }

    #[inline]
    pub fn try_add_bytes(&mut self, bytes: usize) -> Result<(), CacheError> {
        ensure!(
            self.can_fit_bytes(bytes),
            crate::pro::cache::error::NotEnoughSpaceInCache
        );
        self.add_bytes(bytes);
        Ok(())
    }

    #[inline]
    pub fn try_add_element_bytes<T: ByteSize>(&mut self, element: &T) -> Result<(), CacheError> {
        let bytes = element.byte_size();
        self.try_add_bytes(bytes)
    }

    #[inline]
    pub fn try_remove_bytes(&mut self, bytes: usize) -> Result<(), CacheError> {
        ensure!(
            self.byte_size_used >= bytes,
            crate::pro::cache::error::NegativeSizeOfCache
        );
        self.remove_bytes(bytes);
        Ok(())
    }

    #[inline]
    pub fn try_remove_element_bytes<T: ByteSize>(&mut self, element: &T) -> Result<(), CacheError> {
        let bytes = element.byte_size();
        self.try_remove_bytes(bytes)
    }

    #[inline]
    fn add_bytes_allow_overflow(&mut self, bytes: usize) {
        self.byte_size_used += bytes;
        if self.byte_size_used > self.byte_size_total {
            trace!(
                "overflowing cache size by {} bytes, total size: {}, added bytes: {}",
                self.byte_size_used - self.byte_size_total,
                self.byte_size_total,
                bytes
            );
        }
    }
}

impl TileCacheBackend {
    pub fn query_cache<T>(
        &mut self,
        key: &CanonicOperatorName,
        query: &RasterQueryRectangle,
    ) -> Option<CacheTileStream<T>>
    where
        T: Pixel + Cachable,
    {
        let cache = &mut self.operator_caches.get_mut(key)?.entries;

        let mut expired_ids = Vec::new();
        let x = cache.iter().find(|&(id, r)| {
            if r.is_expired() {
                expired_ids.push(*id);
                return false;
            }
            r.matches(query)
        });

        let res = if let Some((id, entry)) = x {
            // set as most recently used
            self.lru.promote(id);
            T::stream(entry.tiles.tile_stream(query))
        } else {
            None
        };

        // remove expired entries
        self.remove_querys_from_cache_and_lru(key, expired_ids.as_slice());

        res
    }

    pub fn insert_query<T: Pixel + Cachable>(
        &mut self,
        key: &CanonicOperatorName,
        query: &RasterQueryRectangle,
    ) -> Result<QueryId, CacheError> {
        // if there is no cache for this operator graph, we create one. However, it is not clear yet, if we will actually cache anything. The underling query might be aborted.
        if !self.operator_caches.contains_key(key) {
            self.operator_caches.insert(key.clone(), Default::default());
            // self.cache_size.try_add_element_bytes(&key)?; // TODO: can we delete this at a later point?
        };

        let query_id = QueryId::new();

        let landing_zone_entry = LandingZoneEntry {
            query: *query,
            tiles: T::create_active_query_tiles(),
        };

        let landing_zone = &mut self
            .operator_caches
            .get_mut(key)
            .expect("There must be an entry since we just added one")
            .landing_zone;

        // This gets or creates the cache entry for the operator graph. If it already exists, we don't need to add the size of the key again (see if case above).
        self.landing_zone_size.try_add_element_bytes(&query_id)?;
        self.landing_zone_size
            .try_add_element_bytes(&landing_zone_entry)?;
        let old_entry = landing_zone.insert(query_id, landing_zone_entry);

        debug_assert!(
            old_entry.is_none(),
            "There must not be an entry for the same query id in the landing zone! This is a bug."
        );

        Ok(query_id)
    }

    fn remove_query_from_landing_zone(&mut self, key: &CanonicOperatorName, query_id: &QueryId) {
        let caches_are_empty = if let Some(operator_caches) = self.operator_caches.get_mut(key) {
            let landing_zone = &mut operator_caches.landing_zone;
            if let Some(entry) = landing_zone.remove(query_id) {
                self.landing_zone_size.remove_element_bytes(query_id);
                self.landing_zone_size.remove_element_bytes(&entry);
            }
            landing_zone.is_empty() && operator_caches.entries.is_empty()
        } else {
            debug!("Tried to remove query ({}) from landing zone, but there was no entry for the operator ({:?})!", query_id, key);
            false
        };
        if caches_are_empty {
            // there are no more entries for this operator graph, so we can remove the whole cache
            self.operator_caches.remove(key);
        }
    }

    /// Insert a tile for a given query. The query has to be inserted first.
    /// The tile is inserted into the landing zone and only moved to the cache when the query is finished.
    /// If the landing zone is full, the caching of the query is aborted.
    pub fn insert_tile<T>(
        &mut self,
        key: &CanonicOperatorName,
        query_id: &QueryId,
        tile: RasterTile2D<T>,
    ) -> Result<(), CacheError>
    where
        T: Pixel + Cachable,
    {
        // if the tile is already expired we can stop caching the whole query
        if tile.cache_hint.is_expired() {
            // TODO: cache hint as trait
            self.remove_query_from_landing_zone(key, query_id);
            debug!(
                "Tile expired before insertion {}. Is empty: {}",
                tile.cache_hint.expires().datetime(),
                tile.is_empty()
            );
            return Err(super::error::CacheError::TileExpiredBeforeInsertion);
        }

        let tile_size_bytes = tile.byte_size();

        // check if landing zone has enough space, otherwise abort caching the query
        if !self.landing_zone_size.can_fit_bytes(tile_size_bytes) {
            debug!(
                "Not enough space in landing zone. Removing query {}. Landing zone size: {}. Landing zone used: {}. Landing zone used percentage: {}. Tile size: {}",
                query_id, self.landing_zone_size.total_byte_size(), self.landing_zone_size.byte_size_used(), self.landing_zone_size.size_used_fraction(), tile_size_bytes
            );
            self.remove_query_from_landing_zone(key, query_id);
            return Err(super::error::CacheError::NotEnoughSpaceInLandingZone);
        }

        let landing_zone = &mut self
            .operator_caches
            .get_mut(key)
            .ok_or(super::error::CacheError::QueryNotFoundInLandingZone)?
            .landing_zone;

        let entry = landing_zone
            .get_mut(query_id)
            .ok_or(super::error::CacheError::QueryNotFoundInLandingZone)?;

        // Since we always produce whole tiles, we can extend the spatial extent of the cache entry by the spatial extent of the tile. This will result in more cache hits for queries that cover the same tiles but are not inside the area of the original query.
        entry.query.spatial_bounds.extend(&tile.spatial_partition());
        // The temporal extent of the query might produce tiles that have a temporal extent that is larger then the original query. We can extend the temporal extent of the query by the temporal extent of the tiles. Again this will result in more cache hits for queries that cover the same tiles but are not inside the temporal extent of the original query.
        entry.query.time_interval = entry
            .query
            .time_interval
            .union(&tile.time)
            .expect("time of tile must overlap with query");

        T::insert_tile(&mut entry.tiles, tile)?;
        self.landing_zone_size.try_add_bytes(tile_size_bytes)?;

        trace!(
            "Inserted tile for query {} into landing zone. Landing zone size: {}. Landing zone size used: {}. Landing zone used percentage: {}",
            query_id, self.landing_zone_size.total_byte_size(), self.landing_zone_size.byte_size_used(), self.landing_zone_size.size_used_fraction()
        );

        Ok(())
    }

    /// Finish the query and make the tiles available in the cache
    ///
    /// # Panics
    ///
    /// Panics if there is aleady a cache entry for the query. This should never happen.
    ///
    pub fn finish_inserting_query(
        &mut self,
        key: &CanonicOperatorName,
        query_id: &QueryId,
    ) -> Result<(), CacheError> {
        // TODO: maybe check if this cache result is already in the cache or could displace another one

        // this should always work, because the query was inserted at some point and then the cache entry was created
        let landing_zone = &mut self
            .operator_caches
            .get_mut(key)
            .ok_or(super::error::CacheError::QueryNotFoundInLandingZone)?
            .landing_zone;

        let active_query = landing_zone
            .remove(query_id)
            .ok_or(super::error::CacheError::QueryNotFoundInLandingZone)?;

        // remove the size of the query from the landing zone
        self.landing_zone_size.remove_element_bytes(query_id);
        self.landing_zone_size.remove_element_bytes(&active_query);

        // debug output
        debug!(
            "Finished query {}. Landing zone size: {}. Landing zone size used: {}, Landing zone used percentage: {}.",
            query_id, self.landing_zone_size.total_byte_size(), self.landing_zone_size.byte_size_used(), self.landing_zone_size.size_used_fraction()
        );

        debug_assert!(
            active_query.tiles.len() > 0,
            "There must be at least one tile in the active query. CanonicOperatorName: {}, Query: {}",
            key,
            query_id
        );

        // move entry from landing zone into cache
        let cache_entry: CacheEntry = active_query.into();
        let cache_entry_id = CacheEntryId::new();

        // calculate size of cache entry. This might be different from the size of the landing zone entry.
        let cache_entry_size_bytes = cache_entry.byte_size() + cache_entry_id.byte_size();

        let cache = &mut self
            .operator_caches
            .get_mut(key)
            .expect("There must be cache elements entry if there was a loading zone")
            .entries;

        let old_cache_entry = cache.insert(cache_entry_id, cache_entry);
        let old_lru_entry = self.lru.push(cache_entry_id, key.clone());
        assert!(old_lru_entry.is_none()); // this should always work, because we just inserted a new CacheEntryId
        assert!(old_cache_entry.is_none()); // this should always work, because we just inserted a new CacheEntryId

        // cache bound can be temporarily exceeded as the entry is moved form the landing zone into the cache
        // but the total of cache + landing zone is still below the bound
        self.cache_size
            .add_bytes_allow_overflow(cache_entry_size_bytes);

        debug!(
            "Finished query {}. Cache size: {}. Cache size used: {}, Cache used percentage: {}.",
            query_id,
            self.cache_size.total_byte_size(),
            self.cache_size.byte_size_used(),
            self.cache_size.size_used_fraction()
        );

        // We now evict elements from the cache until bound is satisfied again
        self.evict_elements_until_cache_size_is_satisfied();

        Ok(())
    }

    fn evict_elements_until_cache_size_is_satisfied(&mut self) {
        while self.cache_size.is_overfull() {
            // this should always work, because otherwise it would mean the cache is not empty but the lru is.
            // the landing zone is smaller than the cache size and the entry must fit into the landing zone.
            if let Some((pop_entry_id, pop_entry_key)) = self.lru.pop_lru() {
                let cache = &mut self
                    .operator_caches
                    .get_mut(&pop_entry_key)
                    .expect("There must be cache elements entry if there was an LRU entry");
                let cache_entries = &mut cache.entries;
                {
                    let old_cache_entry = cache_entries
                        .remove(&pop_entry_id)
                        .expect("LRU entry must be in cache");
                    self.cache_size.remove_element_bytes(&pop_entry_id);
                    self.cache_size.remove_element_bytes(&old_cache_entry);
                }

                debug!(
                    "Evicted query {}. Cache size: {}. Cache size used: {}, Cache used percentage: {}.",
                    pop_entry_id,
                    self.cache_size.total_byte_size(),
                    self.cache_size.byte_size_used(),
                    self.cache_size.size_used_fraction()
                );

                // if there are no more cache entries and the landing zone is empty, remove the cache for this operator graph
                if cache_entries.is_empty() && cache.landing_zone.is_empty() {
                    self.operator_caches.remove(&pop_entry_key);
                }
            } else {
                panic!("Cache is overfull but LRU is empty. This must not happen.");
            }
        }
    }

    fn remove_querys_from_cache_and_lru(
        &mut self,
        key: &CanonicOperatorName,
        cache_entry_ids: &[CacheEntryId],
    ) {
        if let Some(cache) = self.operator_caches.get_mut(key) {
            let cache_entries = &mut cache.entries;
            for cache_entry_id in cache_entry_ids {
                let _old_lru_entry = self.lru.pop_entry(cache_entry_id);
                if let Some(old_cache_entry) = cache_entries.remove(cache_entry_id) {
                    self.cache_size.remove_element_bytes(cache_entry_id);
                    self.cache_size.remove_element_bytes(&old_cache_entry);
                }
            }
            // if there are no more cache entries and no more landing zone entries, remove the cache for this operator graph
            if cache_entries.is_empty() && cache.landing_zone.is_empty() {
                self.operator_caches.remove(key);
            }
        }
    }
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

        if landing_zone_ratio >= 1.0 {
            return Err(crate::error::Error::QueryingProcessorFailed {
                source: Box::new(CacheError::LandingZoneRatioMustBeSmallerThanOne),
            });
        }

        let cache_size_bytes =
            (cache_size_in_mb as f64 * (1.0 - landing_zone_ratio) * 1024.0 * 1024.0) as usize;

        let landing_zone_size_bytes =
            (cache_size_in_mb as f64 * landing_zone_ratio * 1024.0 * 1024.0) as usize;

        Ok(Self {
            backend: RwLock::new(TileCacheBackend {
                operator_caches: Default::default(),
                lru: LruCache::unbounded(), // we need no cap because we evict manually
                cache_size: TileCacheBackendSize::new(cache_size_bytes),
                landing_zone_size: TileCacheBackendSize::new(landing_zone_size_bytes),
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
                cache_size: TileCacheBackendSize::new(usize::MAX),
                landing_zone_size: TileCacheBackendSize::new(usize::MAX),
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

impl ByteSize for QueryId {}

/// Holds all the tiles for a given query and is able to answer queries that are fully contained
#[derive(Debug)]
pub struct CacheEntry {
    query: RasterQueryRectangle,
    tiles: CachedTiles,
}

identifier!(CacheEntryId);

impl ByteSize for CacheEntryId {}

impl CacheEntry {
    /// Return true if the query can be answered in full by this cache entry
    /// For this, the bbox and time has to be fully contained, and the spatial resolution has to match
    pub fn matches(&self, query: &RasterQueryRectangle) -> bool {
        self.query.spatial_bounds.contains(&query.spatial_bounds)
            && self.query.time_interval.contains(&query.time_interval)
            && self.query.spatial_resolution == query.spatial_resolution
    }

    /// Produces a tile stream from the cache
    pub fn tile_stream(&self, query: &RasterQueryRectangle) -> TypedCacheTileStream {
        self.tiles.tile_stream(query)
    }

    fn is_expired(&self) -> bool {
        self.tiles.is_expired()
    }
}

impl ByteSize for CacheEntry {
    fn heap_byte_size(&self) -> usize {
        self.tiles.heap_byte_size()
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

impl ByteSize for CachedTiles {
    fn heap_byte_size(&self) -> usize {
        // we need to use `byte_size` instead of `heap_byte_size` here, because `Arc` stores its data on the heap
        match self {
            CachedTiles::U8(tiles) => tiles.byte_size(),
            CachedTiles::U16(tiles) => tiles.byte_size(),
            CachedTiles::U32(tiles) => tiles.byte_size(),
            CachedTiles::U64(tiles) => tiles.byte_size(),
            CachedTiles::I8(tiles) => tiles.byte_size(),
            CachedTiles::I16(tiles) => tiles.byte_size(),
            CachedTiles::I32(tiles) => tiles.byte_size(),
            CachedTiles::I64(tiles) => tiles.byte_size(),
            CachedTiles::F32(tiles) => tiles.byte_size(),
            CachedTiles::F64(tiles) => tiles.byte_size(),
        }
    }
}

impl CachedTiles {
    fn is_expired(&self) -> bool {
        match self {
            CachedTiles::U8(v) => v.iter().any(|t| t.cache_hint.is_expired()),
            CachedTiles::U16(v) => v.iter().any(|t| t.cache_hint.is_expired()),
            CachedTiles::U32(v) => v.iter().any(|t| t.cache_hint.is_expired()),
            CachedTiles::U64(v) => v.iter().any(|t| t.cache_hint.is_expired()),
            CachedTiles::I8(v) => v.iter().any(|t| t.cache_hint.is_expired()),
            CachedTiles::I16(v) => v.iter().any(|t| t.cache_hint.is_expired()),
            CachedTiles::I32(v) => v.iter().any(|t| t.cache_hint.is_expired()),
            CachedTiles::I64(v) => v.iter().any(|t| t.cache_hint.is_expired()),
            CachedTiles::F32(v) => v.iter().any(|t| t.cache_hint.is_expired()),
            CachedTiles::F64(v) => v.iter().any(|t| t.cache_hint.is_expired()),
        }
    }
}

#[derive(Debug)]
struct LandingZoneEntry {
    query: RasterQueryRectangle,
    tiles: LandingZoneQueryTiles,
}

impl ByteSize for LandingZoneEntry {
    fn heap_byte_size(&self) -> usize {
        self.tiles.heap_byte_size()
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
    pub fn len(&self) -> usize {
        match self {
            LandingZoneQueryTiles::U8(v) => v.len(),
            LandingZoneQueryTiles::U16(v) => v.len(),
            LandingZoneQueryTiles::U32(v) => v.len(),
            LandingZoneQueryTiles::U64(v) => v.len(),
            LandingZoneQueryTiles::I8(v) => v.len(),
            LandingZoneQueryTiles::I16(v) => v.len(),
            LandingZoneQueryTiles::I32(v) => v.len(),
            LandingZoneQueryTiles::I64(v) => v.len(),
            LandingZoneQueryTiles::F32(v) => v.len(),
            LandingZoneQueryTiles::F64(v) => v.len(),
        }
    }
}

impl ByteSize for LandingZoneQueryTiles {
    fn heap_byte_size(&self) -> usize {
        // we need to use `byte_size` instead of `heap_byte_size` here, because `Vec` stores its data on the heap
        match self {
            LandingZoneQueryTiles::U8(v) => v.byte_size(),
            LandingZoneQueryTiles::U16(v) => v.byte_size(),
            LandingZoneQueryTiles::U32(v) => v.byte_size(),
            LandingZoneQueryTiles::U64(v) => v.byte_size(),
            LandingZoneQueryTiles::I8(v) => v.byte_size(),
            LandingZoneQueryTiles::I16(v) => v.byte_size(),
            LandingZoneQueryTiles::I32(v) => v.byte_size(),
            LandingZoneQueryTiles::I64(v) => v.byte_size(),
            LandingZoneQueryTiles::F32(v) => v.byte_size(),
            LandingZoneQueryTiles::F64(v) => v.byte_size(),
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
        key: &CanonicOperatorName,
        query: &RasterQueryRectangle,
    ) -> Option<CacheTileStream<T>>
    where
        T: Pixel + Cachable,
    {
        let mut backend = self.backend.write().await;
        backend.query_cache::<T>(key, query)
    }

    /// When inserting a new query, we first register the query and then insert the tiles as they are produced
    /// This is to avoid confusing different queries on the same operator and query rectangle
    pub async fn insert_query<T: Pixel + Cachable>(
        &self,
        key: &CanonicOperatorName,
        query: &RasterQueryRectangle,
    ) -> Result<QueryId, CacheError> {
        let mut backend = self.backend.write().await;
        backend.insert_query::<T>(key, query)
    }

    /// Insert a tile for a given query. The query has to be inserted first.
    /// The tile is inserted into the landing zone and only moved to the cache when the query is finished.
    /// If the landing zone is full, the caching of the query is aborted.
    pub async fn insert_tile<T>(
        &self,
        key: &CanonicOperatorName,
        query_id: &QueryId,
        tile: RasterTile2D<T>,
    ) -> Result<(), CacheError>
    where
        T: Pixel + Cachable,
    {
        let mut backend = self.backend.write().await;
        backend.insert_tile::<T>(key, query_id, tile)
    }

    /// Abort the query and remove the tiles from the cache
    pub async fn abort_query(&self, key: &CanonicOperatorName, query_id: &QueryId) {
        let mut backend = self.backend.write().await;
        backend.remove_query_from_landing_zone(key, query_id);
    }

    /// Finish the query and make the tiles available in the cache
    pub async fn finish_query(
        &self,
        key: &CanonicOperatorName,
        query_id: &QueryId,
    ) -> Result<(), CacheError> {
        // TODO: maybe check if this cache result is already in the cache or could displace another one

        let mut backend = self.backend.write().await;
        backend.finish_inserting_query(key, query_id)
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{CacheHint, DateTime, SpatialPartition2D, SpatialResolution, TimeInterval},
        raster::{Grid, RasterProperties},
    };
    use serde_json::json;

    use super::*;

    async fn process_query(tile_cache: &mut TileCache, op_name: CanonicOperatorName) {
        let query_id = tile_cache
            .insert_query::<u8>(&op_name, &query_rect())
            .await
            .unwrap();

        tile_cache
            .insert_tile(&op_name, &query_id, create_tile())
            .await
            .unwrap();

        tile_cache.finish_query(&op_name, &query_id).await.unwrap();
    }

    fn create_tile() -> RasterTile2D<u8> {
        RasterTile2D::<u8> {
            time: TimeInterval::new_instant(DateTime::new_utc(2014, 3, 1, 0, 0, 0)).unwrap(),
            tile_position: [-1, 0].into(),
            global_geo_transform: TestDefault::test_default(),
            grid_array: Grid::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
            properties: RasterProperties::default(),
            cache_hint: CacheHint::max_duration(),
        }
    }

    fn query_rect() -> RasterQueryRectangle {
        RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked(
                (-180., 90.).into(),
                (180., -90.).into(),
            ),
            time_interval: TimeInterval::new_instant(DateTime::new_utc(2014, 3, 1, 0, 0, 0))
                .unwrap(),
            spatial_resolution: SpatialResolution::one(),
        }
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
        let query_id = QueryId::new();
        let size_of_landing_zone_entry = landing_zone_entry.byte_size() + query_id.byte_size();
        let cache_entry: CacheEntry = landing_zone_entry.into();
        let cache_entry_id = CacheEntryId::new();
        let size_of_cache_entry = cache_entry.byte_size() + cache_entry_id.byte_size();

        // Select the max of both sizes
        // This is done because the landing zone should not be smaller then the cache
        let m_size = size_of_cache_entry.max(size_of_landing_zone_entry);

        // set limits s.t. three tiles fit
        let mut tile_cache = TileCache {
            backend: RwLock::new(TileCacheBackend {
                operator_caches: Default::default(),
                lru: LruCache::unbounded(),
                cache_size: TileCacheBackendSize::new(m_size * 3),
                landing_zone_size: TileCacheBackendSize::new(m_size * 3),
            }),
        };
        // process three different queries
        process_query(&mut tile_cache, op(1)).await;
        process_query(&mut tile_cache, op(2)).await;
        process_query(&mut tile_cache, op(3)).await;

        // query the first one s.t. it is the most recently used
        tile_cache
            .query_cache::<u8>(&op(1), &query_rect())
            .await
            .unwrap();
        // process a fourth query
        process_query(&mut tile_cache, op(4)).await;

        // assure the seconds query is evicted because it is the least recently used
        assert!(tile_cache
            .query_cache::<u8>(&op(2), &query_rect())
            .await
            .is_none());

        // assure that the other queries are still in the cache
        for i in [1, 3, 4] {
            assert!(tile_cache
                .query_cache::<u8>(&op(i), &query_rect())
                .await
                .is_some());
        }

        assert_eq!(
            tile_cache.backend.read().await.cache_size.byte_size_used(),
            3 * size_of_cache_entry
        );
    }

    #[test]
    fn cache_byte_size() {
        assert_eq!(create_tile().byte_size(), 284);
        assert_eq!(
            CachedTiles::U8(Arc::new(vec![create_tile()])).byte_size(),
            /* enum + arc */ 16 + /* vec */ 24  + /* tile */ 284
        );
        assert_eq!(
            CachedTiles::U8(Arc::new(vec![create_tile(), create_tile()])).byte_size(),
            /* enum + arc */ 16 + /* vec */ 24  + /* tile */ 2 * 284
        );
    }

    #[tokio::test]
    async fn it_checks_ttl() {
        let mut tile_cache = TileCache {
            backend: RwLock::new(TileCacheBackend {
                operator_caches: Default::default(),
                lru: LruCache::unbounded(),
                cache_size: TileCacheBackendSize::new(usize::MAX),
                landing_zone_size: TileCacheBackendSize::new(usize::MAX),
            }),
        };

        process_query(&mut tile_cache, op(1)).await;

        // access works because no ttl is set
        tile_cache
            .query_cache::<u8>(&op(1), &query_rect())
            .await
            .unwrap();

        // manually expire entry
        {
            let mut backend = tile_cache.backend.write().await;
            let cache = backend.operator_caches.iter_mut().next().unwrap();

            let tiles = &mut cache.1.entries.iter_mut().next().unwrap().1.tiles;
            match tiles {
                CachedTiles::U8(tiles) => {
                    let mut expired_tiles = (**tiles).clone();
                    expired_tiles[0].cache_hint = CacheHint::with_created_and_expires(
                        DateTime::new_utc(0, 1, 1, 0, 0, 0),
                        DateTime::new_utc(0, 1, 1, 0, 0, 1).into(),
                    );
                    *tiles = Arc::new(expired_tiles);
                }
                _ => panic!("wrong tile type"),
            }
        }

        // access fails because ttl is expired
        assert!(tile_cache
            .query_cache::<u8>(&op(1), &query_rect())
            .await
            .is_none());
    }

    #[tokio::test]
    async fn tile_cache_init_size() {
        let tile_cache = TileCache::new(100, 0.1).unwrap();

        let backend = tile_cache.backend.read().await;

        let cache_size = 90 * 1024 * 1024;
        let landing_zone_size = 10 * 1024 * 1024;

        assert_eq!(backend.cache_size.total_byte_size(), cache_size);
        assert_eq!(
            backend.landing_zone_size.total_byte_size(),
            landing_zone_size
        );
    }

    #[test]
    fn tile_cache_backend_size_init() {
        let TileCacheBackendSize {
            byte_size_total,
            byte_size_used,
        } = TileCacheBackendSize::new(123);
        assert_eq!(byte_size_total, 123);
        assert_eq!(byte_size_used, 0);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn tile_cache_backend_size_add_remove() {
        let mut size = TileCacheBackendSize::new(100);
        size.add_bytes(10);
        assert_eq!(size.byte_size_total, 100);
        assert_eq!(size.byte_size_used, 10);
        assert_eq!(size.total_byte_size(), 100);
        assert_eq!(size.byte_size_used(), 10);
        assert_eq!(size.byte_size_free(), 100 - 10);
        assert_eq!(size.size_used_fraction(), 0.1);

        size.add_bytes(20);
        assert_eq!(size.byte_size_total, 100);
        assert_eq!(size.byte_size_used, 30);
        assert_eq!(size.total_byte_size(), 100);
        assert_eq!(size.byte_size_used(), 30);
        assert_eq!(size.byte_size_free(), 100 - 30);
        assert_eq!(size.size_used_fraction(), 0.3);

        size.remove_bytes(10);
        assert_eq!(size.byte_size_total, 100);
        assert_eq!(size.byte_size_used, 20);
        assert_eq!(size.total_byte_size(), 100);
        assert_eq!(size.byte_size_used(), 20);
        assert_eq!(size.byte_size_free(), 100 - 20);
        assert_eq!(size.size_used_fraction(), 0.2);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn tile_cache_backend_size_add_remove_overflow() {
        let mut size = TileCacheBackendSize::new(100);
        size.add_bytes_allow_overflow(110);
        assert_eq!(size.byte_size_total, 100);
        assert_eq!(size.byte_size_used, 110);
        assert_eq!(size.total_byte_size(), 100);
        assert_eq!(size.byte_size_used(), 110);
        assert_eq!(size.byte_size_free(), 0);
        assert!(size.is_overfull());

        size.remove_bytes(110);
        assert_eq!(size.byte_size_total, 100);
        assert_eq!(size.byte_size_used, 0);
        assert_eq!(size.total_byte_size(), 100);
        assert_eq!(size.byte_size_used(), 0);
        assert_eq!(size.byte_size_free(), 100);
        assert_eq!(size.size_used_fraction(), 0.0);
        assert!(!size.is_overfull());
    }

    #[test]
    fn tile_cache_backend_element_size() {
        let tile = create_tile();
        let tile_size = tile.byte_size();
        let mut size = TileCacheBackendSize::new(100_000);
        size.add_element_bytes(&tile);
        assert!(size.byte_size_used() == tile_size);
        assert!(size.byte_size_free() == 100_000 - tile_size);
        size.remove_element_bytes(&tile);
        assert!(size.byte_size_used() == 0);
    }

    #[test]
    fn tile_cache_backend_can_fit() {
        let tile_size = 1001;
        let mut size = TileCacheBackendSize::new(2000);
        assert!(size.can_fit_bytes(tile_size));
        size.add_bytes(tile_size);
        assert!(!size.can_fit_bytes(tile_size));
        size.remove_bytes(tile_size);
        assert!(size.can_fit_bytes(tile_size));
    }
}
