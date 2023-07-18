use std::{collections::HashMap, hash::Hash, sync::Arc};

use futures::Stream;
use geoengine_datatypes::{
    collections::{
        DataCollection, FeatureCollection, FeatureCollectionInfos, MultiLineStringCollection,
        MultiPointCollection, MultiPolygonCollection,
    },
    identifier,
    primitives::{RasterQueryRectangle, VectorQueryRectangle},
    raster::{Pixel, RasterTile2D},
    util::{test::TestDefault, ByteSize, Identifier},
};
use log::debug;
use lru::LruCache;
use tokio::sync::RwLock;

use crate::engine::CanonicOperatorName;
use crate::util::Result;

use super::{
    cache_tile_stream::{Cachable, CacheTileStream, TypedCacheTileStream},
    error::CacheError,
    util::CacheSize,
};

/// This is a wrapper arund a `HashMap`.
#[derive(Debug, Clone)]
pub struct OperatorCache<O>(HashMap<CanonicOperatorName, O>);

impl<O> OperatorCache<O> {
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn get_mut(&mut self, key: &CanonicOperatorName) -> Option<&mut O> {
        self.0.get_mut(key)
    }

    fn insert(&mut self, key: CanonicOperatorName, value: O) -> Option<O> {
        self.0.insert(key, value)
    }

    fn remove(&mut self, key: &CanonicOperatorName) -> Option<O> {
        self.0.remove(key)
    }

    fn contains_key(&self, key: &CanonicOperatorName) -> bool {
        self.0.contains_key(key)
    }
}

impl<O: Hash> ByteSize for OperatorCache<O>
where
    O: ByteSize,
{
    fn heap_byte_size(&self) -> usize {
        self.0.heap_byte_size()
    }
}

impl<O> Default for OperatorCache<O> {
    fn default() -> Self {
        Self::new()
    }
}

/// The tile cache caches all tiles of a query and is able to answer queries that are fully contained in the cache.
/// New tiles are inserted into the cache on-the-fly as they are produced by query processors.
/// The tiles are first inserted into a landing zone, until the query in completely finished and only then moved to the cache.
/// Both the landing zone and the cache have a maximum size.
/// If the landing zone is full, the caching of the current query will be aborted.
/// If the cache is full, the least recently used entries will be evicted if necessary to make room for the new entry.
#[derive(Debug)]
pub struct CacheBackend {
    // TODO: more fine granular locking?
    // for each operator graph, we have a cache, that can efficiently be accessed
    raster_caches: OperatorCache<RasterOperatorCacheEntry>,
    vector_caches: OperatorCache<VectorOperatorCacheEntry>,

    cache_size: CacheSize,
    landing_zone_size: CacheSize,

    // we only use the LruCache for determining the least recently used elements and evict as many entries as needed to fit the new one
    lru: LruCache<CacheEntryId, TypedCanonicOperatorName>,
}

impl CacheBackend {
    pub fn evict_until_can_fit_bytes(&mut self, bytes: usize) {
        while !self.cache_size.can_fit_bytes(bytes) {
            if let Some((pop_id, pop_key)) = self.lru.pop_lru() {
                match pop_key {
                    TypedCanonicOperatorName::Raster(raster_pop_key) => {
                        let op_cache = self
                            .raster_caches
                            .get_mut(&raster_pop_key)
                            .expect("LRU entry must exist in the cache!");
                        let query_element = op_cache
                            .remove_cache_entry(&pop_id)
                            .expect("LRU entry must exist in the cache!");
                        self.cache_size.remove_element_bytes(&query_element);
                    }
                    TypedCanonicOperatorName::Vector(vector_pop_key) => {
                        let op_cache = self
                            .vector_caches
                            .get_mut(&vector_pop_key)
                            .expect("LRU entry must exist in the cache!");
                        let query_element = op_cache
                            .remove_cache_entry(&pop_id)
                            .expect("LRU entry must exist in the cache!");
                        self.cache_size.remove_element_bytes(&query_element);
                    }
                };
                self.cache_size.remove_element_bytes(&pop_id);

                debug!(
                    "Evicted query {}. Cache size: {}. Cache size used: {}, Cache used percentage: {}.",
                    pop_id,
                    self.cache_size.total_byte_size(),
                    self.cache_size.byte_size_used(),
                    self.cache_size.size_used_fraction()
                );
            }
        }
    }
}

pub trait CacheEvictUntilFit {
    fn evict_entries_until_can_fit_bytes(&mut self, bytes: usize);
}

impl CacheEvictUntilFit for CacheBackend {
    fn evict_entries_until_can_fit_bytes(&mut self, bytes: usize) {
        self.evict_until_can_fit_bytes(bytes);
    }
}

pub trait CacheView<C, L>: CacheEvictUntilFit {
    fn operator_caches_mut(&mut self) -> &mut OperatorCache<OperatorCacheEntry<C, L>>;

    fn create_operator_cache_if_needed(&mut self, key: CanonicOperatorName) {
        if !self.operator_caches_mut().contains_key(&key) {
            // TODO: maybe add the size of the OperatorCacheEntry to the cache size?
            self.operator_caches_mut()
                .insert(key, OperatorCacheEntry::new());
        }
    }

    fn remove_operator_cache(
        &mut self,
        key: &CanonicOperatorName,
    ) -> Option<OperatorCacheEntry<C, L>> {
        // TODO: maybe remove the size of the OperatorCacheEntry to the cache size?
        self.operator_caches_mut().remove(key)
    }
}

#[allow(clippy::type_complexity)]
pub struct OperatorCacheEntryView<'a, C: CacheElement> {
    operator_cache: &'a mut OperatorCacheEntry<
        CacheQueryEntry<C::Query, C::CacheContainer>,
        CacheQueryEntry<C::Query, C::LandingZoneContainer>,
    >,
    cache_size: &'a mut CacheSize,
    landing_zone_size: &'a mut CacheSize,
    lru: &'a mut LruCache<CacheEntryId, TypedCanonicOperatorName>,
}

impl<'a, C> OperatorCacheEntryView<'a, C>
where
    C: CacheElement + ByteSize,
    C::Query: Clone + CacheQueryMatch,
    CacheQueryEntry<C::Query, C::LandingZoneContainer>: ByteSize,
    CacheQueryEntry<C::Query, C::CacheContainer>: ByteSize,
{
    fn is_empty(&self) -> bool {
        self.operator_cache.is_empty()
    }

    fn remove_query_from_landing_zone(
        &mut self,
        query_id: &QueryId,
    ) -> Option<CacheQueryEntry<C::Query, C::LandingZoneContainer>> {
        if let Some(entry) = self.operator_cache.remove_landing_zone_entry(query_id) {
            self.landing_zone_size.remove_element_bytes(query_id);
            self.landing_zone_size.remove_element_bytes(&entry);
            Some(entry)
        } else {
            None
        }
    }

    fn remove_query_from_cache_and_lru(
        &mut self,
        cache_entry_id: &CacheEntryId,
    ) -> Option<CacheQueryEntry<C::Query, C::CacheContainer>> {
        if let Some(entry) = self.operator_cache.remove_cache_entry(cache_entry_id) {
            let old_lru_entry = self.lru.pop_entry(cache_entry_id);
            debug_assert!(old_lru_entry.is_some(), "CacheEntryId not found in LRU");
            self.cache_size.remove_element_bytes(cache_entry_id);
            self.cache_size.remove_element_bytes(&entry);
            Some(entry)
        } else {
            None
        }
    }

    fn discard_querys_from_cache_and_lru(&mut self, cache_entry_ids: &[CacheEntryId]) {
        for cache_entry_id in cache_entry_ids {
            let old_entry = self.remove_query_from_cache_and_lru(cache_entry_id);
            debug_assert!(
                old_entry.is_some(),
                "CacheEntryId not found in OperatorCacheEntry"
            );
        }
    }

    fn add_query_element_to_landing_zone(
        &mut self,
        query_id: &QueryId,
        landing_zone_element: C,
    ) -> Result<(), CacheError> {
        let element_bytes_size = landing_zone_element.byte_size();

        // TODO: add this check again
        /*
        ensure!(
            self.landing_zone_size.can_fit_bytes(element_bytes_size),
            CacheError::NotEnoughSpaceInLandingZone
        );
        */

        self.operator_cache
            .landing_zone_entry_mut(query_id)
            .ok_or(CacheError::QueryNotFoundInLandingZone)?
            .insert_element(landing_zone_element)?;

        // we add the bytes size of the element to the landing zone size after we have inserted it.
        self.landing_zone_size.try_add_bytes(element_bytes_size)?;

        Ok(())
    }

    fn insert_query_into_landing_zone(&mut self, query: &C::Query) -> Result<QueryId, CacheError> {
        let landing_zone_entry = CacheQueryEntry::create_empty::<C>(query.clone());
        let query_id = QueryId::new();

        self.landing_zone_size.try_add_element_bytes(&query_id)?;
        self.landing_zone_size
            .try_add_element_bytes(&landing_zone_entry)?;

        self.operator_cache
            .insert_landing_zone_entry(query_id, landing_zone_entry)?;

        // TODO: maybe add some kind of rollback or retry logic?

        Ok(query_id)
    }

    fn insert_cache_entry(
        &mut self,
        cache_entry: CacheQueryEntry<C::Query, C::CacheContainer>,
    ) -> Result<CacheEntryId, CacheError> {
        let cache_entry_id = CacheEntryId::new();
        self.cache_size.try_add_element_bytes(&cache_entry)?;
        self.cache_size.try_add_element_bytes(&cache_entry_id)?;
        self.operator_cache
            .insert_cache_entry(cache_entry_id, cache_entry)?;

        Ok(cache_entry_id)
    }

    fn find_matching_cache_entry_and_collect_expired_entries(
        &mut self,
        query: &C::Query,
    ) -> CacheQueryResult<C::Query, C::CacheContainer> {
        let mut expired_cache_entry_ids = vec![];

        let x = self.operator_cache.iter().find(|&(id, entry)| {
            if entry.elements.is_expired() {
                expired_cache_entry_ids.push(*id);
            }
            entry.query.is_match(query)
        });

        CacheQueryResult {
            cache_hit: x.map(|(id, entry)| (*id, entry)),
            expired_cache_entry_ids,
        }
    }
}

struct CacheQueryResult<'a, Query, CE> {
    cache_hit: Option<(CacheEntryId, &'a CacheQueryEntry<Query, CE>)>,
    expired_cache_entry_ids: Vec<CacheEntryId>,
}

pub trait CacheAbc<C: CacheElement>:
    CacheView<
    CacheQueryEntry<C::Query, C::CacheContainer>,
    CacheQueryEntry<C::Query, C::LandingZoneContainer>,
>
where
    C::Query: Clone + CacheQueryMatch,
    CacheQueryEntry<C::Query, C::LandingZoneContainer>: ByteSize,
    CacheQueryEntry<C::Query, C::CacheContainer>: ByteSize,
    CacheQueryEntry<C::Query, C::CacheContainer>:
        From<CacheQueryEntry<C::Query, C::LandingZoneContainer>>,
{
    fn operator_cache_view_mut(
        &mut self,
        key: &CanonicOperatorName,
    ) -> Option<OperatorCacheEntryView<C>>;

    fn query_and_promote(
        &mut self,
        key: &CanonicOperatorName,
        query: &C::Query,
    ) -> Result<Option<C::ResultStream>, CacheError> {
        let mut cache = self
            .operator_cache_view_mut(key)
            .ok_or(CacheError::OperatorCacheEntryNotFound)?;

        let CacheQueryResult {
            cache_hit,
            expired_cache_entry_ids,
        } = cache.find_matching_cache_entry_and_collect_expired_entries(query);

        let res = if let Some((cache_entry_id, cache_entry)) = cache_hit {
            let stream = cache_entry.elements.result_stream(query);

            // promote the cache entry in the LRU
            cache.lru.promote(&cache_entry_id);
            Some(stream)
        } else {
            None
        };

        // discard expired cache entries
        cache.discard_querys_from_cache_and_lru(&expired_cache_entry_ids);

        Ok(res.flatten())
    }

    fn insert_query_into_landing_zone(
        &mut self,
        key: &CanonicOperatorName,
        query: &C::Query,
    ) -> Result<QueryId, CacheError> {
        self.create_operator_cache_if_needed(key.clone());
        self.operator_cache_view_mut(key)
            .expect("OperatorCache was Just created ")
            .insert_query_into_landing_zone(query)
    }

    fn insert_query_element_into_landing_zone(
        &mut self,
        key: &CanonicOperatorName,
        query_id: &QueryId,
        landing_zone_element: C,
    ) -> Result<(), CacheError> {
        let mut cache = self
            .operator_cache_view_mut(key)
            .ok_or(CacheError::QueryNotFoundInLandingZone)?;
        let res = cache.add_query_element_to_landing_zone(query_id, landing_zone_element);

        // if we cant add the element to the landing zone, we remove the query from the landing zone
        if res.is_err() {
            let _old_entry = cache.remove_query_from_landing_zone(query_id);

            // if the operator cache is empty, we remove it from the cache
            if cache.is_empty() {
                self.remove_operator_cache(key);
            }
        }

        res
    }

    fn discard_query_from_landing_zone(&mut self, key: &CanonicOperatorName, query_id: &QueryId) {
        if let Some(mut cache) = self.operator_cache_view_mut(key) {
            cache.remove_query_from_landing_zone(query_id);
            if cache.is_empty() {
                self.remove_operator_cache(key);
            }
        }
    }

    fn discard_querys_from_cache_and_lru(
        &mut self,
        key: &CanonicOperatorName,
        cache_entry_ids: &[CacheEntryId],
    ) {
        if let Some(mut cache) = self.operator_cache_view_mut(key) {
            cache.discard_querys_from_cache_and_lru(cache_entry_ids);
            if cache.is_empty() {
                self.remove_operator_cache(key);
            }
        }
    }

    fn move_query_from_landing_to_cache(
        &mut self,
        key: &CanonicOperatorName,
        query_id: &QueryId,
    ) -> Result<CacheEntryId, CacheError> {
        let mut operator_cache = self
            .operator_cache_view_mut(key)
            .ok_or(CacheError::OperatorCacheEntryNotFound)?;
        let landing_zone_entry = operator_cache
            .remove_query_from_landing_zone(query_id)
            .ok_or(CacheError::QueryNotFoundInLandingZone)?;
        let cache_entry = landing_zone_entry.into();
        let cache_entry_id = operator_cache.insert_cache_entry(cache_entry)?;
        // We could also first try to evict until the cache can hold the entry.
        // However, then we would need to lookup the cache entry twice.
        // To avoid that, we just evict after we moved the entry from the landing zone to the cache.
        // This is also not a problem since the total cache size is the cache size + the landing zone size.
        self.evict_entries_until_can_fit_bytes(0);
        Ok(cache_entry_id)
    }
}

impl<T> CacheAbc<RasterTile2D<T>> for CacheBackend
where
    T: Pixel + Cachable,
{
    fn operator_cache_view_mut(
        &mut self,
        key: &CanonicOperatorName,
    ) -> Option<OperatorCacheEntryView<RasterTile2D<T>>> {
        self.raster_caches
            .get_mut(key)
            .map(|op| OperatorCacheEntryView {
                operator_cache: op,
                cache_size: &mut self.cache_size,
                landing_zone_size: &mut self.landing_zone_size,
                lru: &mut self.lru,
            })
    }
}
/*
impl CacheAbc<VectorQueryRectangle, LandingZoneQueryFeatures, CachedFeatures> for CacheBackend {
    fn operator_cache_view_mut(
        &mut self,
        key: &CanonicOperatorName,
    ) -> Option<
        OperatorCacheEntryView<VectorQueryRectangle, LandingZoneQueryFeatures, CachedFeatures>,
    > {
        self.vector_caches
            .get_mut(key)
            .map(|op| OperatorCacheEntryView {
                operator_cache: op,
                cache_size: &mut self.cache_size,
                landing_zone_size: &mut self.landing_zone_size,
                lru: &mut self.lru,
            })
    }
}
*/

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TypedCanonicOperatorName {
    Raster(CanonicOperatorName),
    Vector(CanonicOperatorName),
}

impl TypedCanonicOperatorName {
    pub fn as_raster(&self) -> Option<&CanonicOperatorName> {
        match self {
            Self::Raster(name) => Some(name),
            Self::Vector(_) => None,
        }
    }

    pub fn as_vector(&self) -> Option<&CanonicOperatorName> {
        match self {
            Self::Raster(_) => None,
            Self::Vector(name) => Some(name),
        }
    }
}

trait TypedCanonicOperatorNameCreator {
    // TOOO: find better name
    fn typed_canonical_operator_name(con: CanonicOperatorName) -> TypedCanonicOperatorName;
}

impl TypedCanonicOperatorNameCreator for CachedTiles {
    fn typed_canonical_operator_name(con: CanonicOperatorName) -> TypedCanonicOperatorName {
        TypedCanonicOperatorName::Raster(con)
    }
}

impl TypedCanonicOperatorNameCreator for CachedFeatures {
    fn typed_canonical_operator_name(con: CanonicOperatorName) -> TypedCanonicOperatorName {
        TypedCanonicOperatorName::Vector(con)
    }
}

impl CacheView<RasterCacheQueryEntry, RasterLandingQueryEntry> for CacheBackend {
    fn operator_caches_mut(&mut self) -> &mut OperatorCache<RasterOperatorCacheEntry> {
        &mut self.raster_caches
    }
}

impl CacheView<VectorCacheQueryEntry, VectorLandingQueryEntry> for CacheBackend {
    fn operator_caches_mut(&mut self) -> &mut OperatorCache<VectorOperatorCacheEntry> {
        &mut self.vector_caches
    }
}

impl CacheBackend {
    /*
    pub fn insert_query<T: Pixel + Cachable>(
        &mut self,
        key: &CanonicOperatorName,
        query: &RasterQueryRectangle,
    ) -> Result<QueryId, CacheError> {
        // if there is no cache for this operator graph, we create one. However, it is not clear yet, if we will actually cache anything. The underling query might be aborted.
        if !self.raster_caches.contains_key(key) {
            self.raster_caches
                .insert(key.clone(), RasterOperatorCacheEntry::new());
            // self.cache_size.try_add_element_bytes(&key)?; // TODO: can we delete this at a later point?
        };

        let query_id = QueryId::new();

        let landing_zone_entry = RasterLandingQueryEntry {
            query: *query,
            elements: T::create_active_query_tiles(),
        };

        let landing_zone = &mut self
            .raster_caches
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
            .raster_caches
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

        T::insert_tile(&mut entry.elements, tile)?;
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
            .raster_caches
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
            !active_query.elements.is_empty(),
            "There must be at least one tile in the active query. CanonicOperatorName: {key}, Query: {query_id}",
        );

        // move entry from landing zone into cache
        let cache_entry: RasterCacheQueryEntry = active_query.into();
        let cache_entry_id = CacheEntryId::new();

        // calculate size of cache entry. This might be different from the size of the landing zone entry.
        let cache_entry_size_bytes = cache_entry.byte_size() + cache_entry_id.byte_size();

        let cache = &mut self
            .raster_caches
            .get_mut(key)
            .expect("There must be cache elements entry if there was a loading zone")
            .entries;

        let old_cache_entry = cache.insert(cache_entry_id, cache_entry);
        let old_lru_entry = self.lru.push(
            cache_entry_id,
            TypedCanonicOperatorName::Raster(key.clone()),
        );
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
                let cache = self
                    .raster_caches
                    .get_mut(&pop_entry_key.as_raster().unwrap())
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
                    self.raster_caches
                        .remove(&pop_entry_key.as_raster().unwrap());
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
        if let Some(cache) = self.raster_caches.get_mut(key) {
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
                self.raster_caches.remove(key);
            }
        }
    }
     */
}

pub trait CacheElement: ByteSize
where
    Self: Sized,
{
    type Query: CacheQueryMatch + Clone;
    type LandingZoneContainer: LandingZoneElementsContainer<Self>;
    type CacheContainer: CacheElementsContainer<Self::Query, Self, ResultStream = Self::ResultStream>
        + From<Self::LandingZoneContainer>;
    type ResultStream;

    fn move_into_landing_zone(
        self,
        landing_zone: &mut Self::LandingZoneContainer,
    ) -> Result<(), CacheError> {
        landing_zone.insert_element(self)
    }
}

impl<T> CacheElement for RasterTile2D<T>
where
    T: Cachable + Pixel,
{
    type Query = RasterQueryRectangle;
    type LandingZoneContainer = LandingZoneQueryTiles;
    type CacheContainer = CachedTiles;
    type ResultStream = CacheTileStream<T>;
}

#[derive(Debug)]
pub struct SharedCache {
    backend: RwLock<CacheBackend>,
}

impl SharedCache {
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

        // TODO: landing zone < 50% of cache size

        let cache_size_bytes =
            (cache_size_in_mb as f64 * (1.0 - landing_zone_ratio) * 1024.0 * 1024.0) as usize;

        let landing_zone_size_bytes =
            (cache_size_in_mb as f64 * landing_zone_ratio * 1024.0 * 1024.0) as usize;

        Ok(Self {
            backend: RwLock::new(CacheBackend {
                vector_caches: Default::default(),
                raster_caches: Default::default(),
                lru: LruCache::unbounded(), // we need no cap because we evict manually
                cache_size: CacheSize::new(cache_size_bytes),
                landing_zone_size: CacheSize::new(landing_zone_size_bytes),
            }),
        })
    }
}

impl TestDefault for SharedCache {
    fn test_default() -> Self {
        Self {
            backend: RwLock::new(CacheBackend {
                vector_caches: Default::default(),
                raster_caches: Default::default(),
                lru: LruCache::unbounded(), // we need no cap because we evict manually
                cache_size: CacheSize::new(usize::MAX),
                landing_zone_size: CacheSize::new(usize::MAX),
            }),
        }
    }
}

/// Holds all the cached results for an operator graph (workflow)
#[derive(Debug, Default)]
pub struct OperatorCacheEntry<C, L> {
    // for a given operator and query we need to look through all entries to find one that matches
    // TODO: use a multi-dimensional index to speed up the lookup
    entries: HashMap<CacheEntryId, C>,

    // running queries insert their tiles as they are produced. The entry will be created once the query is done.
    // The query is identified by a Uuid instead of the query rectangle to avoid confusions with other queries
    landing_zone: HashMap<QueryId, L>,
}

impl<C, L> OperatorCacheEntry<C, L> {
    pub fn new() -> Self {
        Self {
            entries: Default::default(),
            landing_zone: Default::default(),
        }
    }

    fn insert_landing_zone_entry(
        &mut self,
        query_id: QueryId,
        landing_zone_entry: L,
    ) -> Result<(), CacheError> {
        let old_entry = self.landing_zone.insert(query_id, landing_zone_entry);

        if old_entry.is_some() {
            Err(CacheError::QueryIdAlreadyInLandingZone)
        } else {
            Ok(())
        }
    }

    fn remove_landing_zone_entry(&mut self, query_id: &QueryId) -> Option<L> {
        self.landing_zone.remove(query_id)
    }

    fn landing_zone_entry_mut(&mut self, query_id: &QueryId) -> Option<&mut L> {
        self.landing_zone.get_mut(query_id)
    }

    fn insert_cache_entry(
        &mut self,
        cache_entry_id: CacheEntryId,
        cache_entry: C,
    ) -> Result<(), CacheError> {
        let old_entry = self.entries.insert(cache_entry_id, cache_entry);

        if old_entry.is_some() {
            Err(CacheError::CacheEntryIdAlreadyInCache)
        } else {
            Ok(())
        }
    }

    fn remove_cache_entry(&mut self, cache_entry_id: &CacheEntryId) -> Option<C> {
        self.entries.remove(cache_entry_id)
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.landing_zone.is_empty()
    }

    fn iter(&self) -> impl Iterator<Item = (&CacheEntryId, &C)> {
        self.entries.iter()
    }
}

identifier!(QueryId);

impl ByteSize for QueryId {}

identifier!(CacheEntryId);

impl ByteSize for CacheEntryId {}

/// Holds all the tiles for a given query and is able to answer queries that are fully contained
#[derive(Debug, Hash)]
pub struct CacheQueryEntry<Query, Elements> {
    query: Query,
    elements: Elements,
}
type RasterOperatorCacheEntry = OperatorCacheEntry<RasterCacheQueryEntry, RasterLandingQueryEntry>;
type RasterCacheQueryEntry = CacheQueryEntry<RasterQueryRectangle, CachedTiles>;
type RasterLandingQueryEntry = CacheQueryEntry<RasterQueryRectangle, LandingZoneQueryTiles>;

type VectorOperatorCacheEntry = OperatorCacheEntry<VectorCacheQueryEntry, VectorLandingQueryEntry>;
type VectorCacheQueryEntry = CacheQueryEntry<VectorQueryRectangle, CachedFeatures>;
type VectorLandingQueryEntry = CacheQueryEntry<VectorQueryRectangle, LandingZoneQueryFeatures>;

impl<Query, Elements> CacheQueryEntry<Query, Elements> {
    pub fn create_empty<E>(query: Query) -> Self
    where
        Elements: LandingZoneElementsContainer<E>,
    {
        Self {
            query,
            elements: Elements::create_empty(),
        }
    }

    pub fn query(&self) -> &Query {
        &self.query
    }

    pub fn elements_mut(&mut self) -> &mut Elements {
        &mut self.elements
    }

    pub fn insert_element<E>(&mut self, element: E) -> Result<(), CacheError>
    where
        Elements: LandingZoneElementsContainer<E>,
    {
        self.elements.insert_element(element)
    }
}

impl<Query, Elements> ByteSize for CacheQueryEntry<Query, Elements>
where
    Elements: ByteSize,
{
    fn heap_byte_size(&self) -> usize {
        self.elements.heap_byte_size()
    }
}

pub trait CacheQueryMatch<RHS = Self> {
    fn is_match(&self, query: &RHS) -> bool;
}

impl CacheQueryMatch for RasterQueryRectangle {
    fn is_match(&self, query: &RasterQueryRectangle) -> bool {
        self.spatial_bounds.contains(&query.spatial_bounds)
            && self.time_interval.contains(&query.time_interval)
            && self.spatial_resolution == query.spatial_resolution
    }
}

impl CacheQueryMatch for VectorQueryRectangle {
    // TODO: check if that is what we need
    fn is_match(&self, query: &VectorQueryRectangle) -> bool {
        self.spatial_bounds.contains_bbox(&query.spatial_bounds)
            && self.time_interval.contains(&query.time_interval)
            && self.spatial_resolution == query.spatial_resolution
    }
}

pub trait LandingZoneElementsContainer<E> {
    fn insert_element(&mut self, element: E) -> Result<(), CacheError>;
    fn create_empty() -> Self;
}

impl<T> LandingZoneElementsContainer<RasterTile2D<T>> for LandingZoneQueryTiles
where
    T: Cachable + Pixel,
{
    fn insert_element(&mut self, element: RasterTile2D<T>) -> Result<(), CacheError> {
        T::insert_tile(self, element)
    }

    fn create_empty() -> Self {
        T::create_active_query_tiles()
    }
}

impl<G> LandingZoneElementsContainer<FeatureCollection<G>> for LandingZoneQueryFeatures {
    fn insert_element(&mut self, element: FeatureCollection<G>) -> Result<(), CacheError> {
        unimplemented!()
    }

    fn create_empty() -> Self {
        unimplemented!()
    }
}

pub trait CacheElementsContainer<Query, E> {
    type ResultStream: Stream<Item = Result<E>>;
    fn is_expired(&self) -> bool;
    fn result_stream(&self, query: &Query) -> Option<Self::ResultStream>;
}

impl<T> CacheElementsContainer<RasterQueryRectangle, RasterTile2D<T>> for CachedTiles
where
    T: Cachable + Pixel,
{
    type ResultStream = CacheTileStream<T>;
    fn is_expired(&self) -> bool {
        self.is_expired()
    }

    fn result_stream(&self, query: &RasterQueryRectangle) -> Option<Self::ResultStream> {
        T::stream(self.tile_stream(query))
    }
}

impl<G> CacheElementsContainer<VectorQueryRectangle, FeatureCollection<G>> for CachedFeatures {
    type ResultStream = CacheFeatureStream<G>;

    fn is_expired(&self) -> bool {
        unimplemented!()
    }

    fn result_stream(&self, query: &VectorQueryRectangle) -> () {
        unimplemented!()
    }
}

impl CacheQueryEntry<RasterQueryRectangle, CachedTiles> {
    /// Return true if the query can be answered in full by this cache entry
    /// For this, the bbox and time has to be fully contained, and the spatial resolution has to match
    pub fn matches(&self, query: &RasterQueryRectangle) -> bool {
        self.query.spatial_bounds.contains(&query.spatial_bounds)
            && self.query.time_interval.contains(&query.time_interval)
            && self.query.spatial_resolution == query.spatial_resolution
    }

    /// Produces a tile stream from the cache
    pub fn tile_stream(&self, query: &RasterQueryRectangle) -> TypedCacheTileStream {
        self.elements.tile_stream(query)
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

impl From<RasterLandingQueryEntry> for RasterCacheQueryEntry {
    fn from(value: RasterLandingQueryEntry) -> Self {
        Self {
            query: value.query,
            elements: value.elements.into(),
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

#[derive(Debug)]
pub enum CachedFeatures {
    Data(Arc<Vec<DataCollection>>),
    MultiPoint(Arc<Vec<MultiPointCollection>>),
    MultilineString(Arc<Vec<MultiLineStringCollection>>),
    MultiPolygon(Arc<Vec<MultiPolygonCollection>>),
}

impl CachedFeatures {
    pub fn len(&self) -> usize {
        match self {
            CachedFeatures::Data(v) => v.len(),
            CachedFeatures::MultiPoint(v) => v.len(),
            CachedFeatures::MultilineString(v) => v.len(),
            CachedFeatures::MultiPolygon(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl ByteSize for CachedFeatures {
    fn heap_byte_size(&self) -> usize {
        // we need to use `byte_size` instead of `heap_byte_size` here, because `Vec` stores its data on the heap
        match self {
            CachedFeatures::Data(v) => v.iter().map(FeatureCollectionInfos::byte_size).sum(),
            CachedFeatures::MultiPoint(v) => v.iter().map(FeatureCollectionInfos::byte_size).sum(),
            CachedFeatures::MultilineString(v) => {
                v.iter().map(FeatureCollectionInfos::byte_size).sum()
            }
            CachedFeatures::MultiPolygon(v) => {
                v.iter().map(FeatureCollectionInfos::byte_size).sum()
            }
        }
    }
}

#[derive(Debug)]
pub enum LandingZoneQueryFeatures {
    Data(Vec<DataCollection>),
    MultiPoint(Vec<MultiPointCollection>),
    MultilineString(Vec<MultiLineStringCollection>),
    MultiPolygon(Vec<MultiPolygonCollection>),
}

impl LandingZoneQueryFeatures {
    pub fn len(&self) -> usize {
        match self {
            LandingZoneQueryFeatures::Data(v) => v.len(),
            LandingZoneQueryFeatures::MultiPoint(v) => v.len(),
            LandingZoneQueryFeatures::MultilineString(v) => v.len(),
            LandingZoneQueryFeatures::MultiPolygon(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl ByteSize for LandingZoneQueryFeatures {
    fn heap_byte_size(&self) -> usize {
        // we need to use `byte_size` instead of `heap_byte_size` here, because `Vec` stores its data on the heap
        match self {
            LandingZoneQueryFeatures::Data(v) => {
                v.iter().map(FeatureCollectionInfos::byte_size).sum()
            }
            LandingZoneQueryFeatures::MultiPoint(v) => {
                v.iter().map(FeatureCollectionInfos::byte_size).sum()
            }
            LandingZoneQueryFeatures::MultilineString(v) => {
                v.iter().map(FeatureCollectionInfos::byte_size).sum()
            }
            LandingZoneQueryFeatures::MultiPolygon(v) => {
                v.iter().map(FeatureCollectionInfos::byte_size).sum()
            }
        }
    }
}

impl From<LandingZoneQueryFeatures> for CachedFeatures {
    fn from(value: LandingZoneQueryFeatures) -> Self {
        match value {
            LandingZoneQueryFeatures::Data(v) => CachedFeatures::Data(Arc::new(v)),
            LandingZoneQueryFeatures::MultiPoint(v) => CachedFeatures::MultiPoint(Arc::new(v)),
            LandingZoneQueryFeatures::MultilineString(v) => {
                CachedFeatures::MultilineString(Arc::new(v))
            }
            LandingZoneQueryFeatures::MultiPolygon(v) => CachedFeatures::MultiPolygon(Arc::new(v)),
        }
    }
}

impl From<VectorLandingQueryEntry> for VectorCacheQueryEntry {
    fn from(value: VectorLandingQueryEntry) -> Self {
        Self {
            query: value.query,
            elements: value.elements.into(),
        }
    }
}

impl SharedCache {
    pub async fn query_cache<C: CacheElement>(
        &self,
        key: &CanonicOperatorName,
        query: &C::Query,
    ) -> Result<Option<C::ResultStream>, CacheError>
    where
        CacheBackend: CacheAbc<C>,
        CacheQueryEntry<C::Query, C::LandingZoneContainer>: ByteSize,
        CacheQueryEntry<C::Query, C::CacheContainer>:
            ByteSize + From<CacheQueryEntry<C::Query, C::LandingZoneContainer>>,
    {
        let mut backend = self.backend.write().await;
        backend.query_and_promote(key, query)
    }

    pub async fn insert_query<C: CacheElement>(
        &self,
        key: &CanonicOperatorName,
        query: &C::Query,
    ) -> Result<QueryId, CacheError>
    where
        CacheBackend: CacheAbc<C>,
        CacheQueryEntry<C::Query, C::LandingZoneContainer>: ByteSize,
        CacheQueryEntry<C::Query, C::CacheContainer>:
            ByteSize + From<CacheQueryEntry<C::Query, C::LandingZoneContainer>>,
    {
        let mut backend = self.backend.write().await;
        backend.insert_query_into_landing_zone(key, query)
    }

    pub async fn insert_query_element<C: CacheElement>(
        &self,
        key: &CanonicOperatorName,
        query_id: &QueryId,
        landing_zone_element: C,
    ) -> Result<(), CacheError>
    where
        CacheBackend: CacheAbc<C>,
        CacheQueryEntry<C::Query, C::LandingZoneContainer>: ByteSize,
        CacheQueryEntry<C::Query, C::CacheContainer>:
            ByteSize + From<CacheQueryEntry<C::Query, C::LandingZoneContainer>>,
    {
        let mut backend = self.backend.write().await;
        backend.insert_query_element_into_landing_zone(key, query_id, landing_zone_element)
    }

    pub async fn abort_query<C: CacheElement>(&self, key: &CanonicOperatorName, query_id: &QueryId)
    where
        CacheBackend: CacheAbc<C>,
        CacheQueryEntry<C::Query, C::LandingZoneContainer>: ByteSize,
        CacheQueryEntry<C::Query, C::CacheContainer>:
            ByteSize + From<CacheQueryEntry<C::Query, C::LandingZoneContainer>>,
    {
        let mut backend = self.backend.write().await;
        backend.discard_query_from_landing_zone(key, query_id);
    }

    pub async fn finish_query<C: CacheElement>(
        &self,
        key: &CanonicOperatorName,
        query_id: &QueryId,
    ) -> Result<CacheEntryId, CacheError>
    where
        CacheBackend: CacheAbc<C>,
        CacheQueryEntry<C::Query, C::LandingZoneContainer>: ByteSize,
        CacheQueryEntry<C::Query, C::CacheContainer>:
            ByteSize + From<CacheQueryEntry<C::Query, C::LandingZoneContainer>>,
    {
        let mut backend = self.backend.write().await;
        backend.move_query_from_landing_to_cache(key, query_id)
    }
}

/*
#[async_trait]
pub trait AsyncSharedCache {
    async fn query_cache<C>(
        &self,
        key: &CanonicOperatorName,
        query: &C::Query,
    ) -> Result<Option<C::ResultStream>, CacheError>
    where
        C: CacheElement;

    async fn insert_query<C>(
        &self,
        key: &CanonicOperatorName,
        query: &C::Query,
    ) -> Result<QueryId, CacheError>
    where
        C: CacheElement;

    async fn insert_query_element<C>(
        &self,
        key: &CanonicOperatorName,
        query_id: &QueryId,
        landing_zone_element: C,
    ) -> Result<(), CacheError>
    where
        C: CacheElement;

    async fn abort_query<C>(&self, key: &CanonicOperatorName, query_id: &QueryId)
    where
        C: CacheElement;

    async fn finish_query<C>(
        &self,
        key: &CanonicOperatorName,
        query_id: &QueryId,
    ) -> Result<CacheEntryId, CacheError>
    where
        C: CacheElement;
}

#[async_trait]
impl AsyncSharedCache for SharedCache {
    /// Query the cache and on hit create a stream of tiles
    async fn query_cache(
        &self,
        key: &CanonicOperatorName,
        query: &RasterQueryRectangle,
    ) -> Result<Option<CacheTileStream<T>>, CacheError> {
        let mut backend = self.backend.write().await;
        backend.query_and_promote(key, query)
    }

    /// When inserting a new query, we first register the query and then insert the tiles as they are produced
    /// This is to avoid confusing different queries on the same operator and query rectangle
    async fn insert_query(
        &self,
        key: &CanonicOperatorName,
        query: &RasterQueryRectangle,
    ) -> Result<QueryId, CacheError> {
        let mut backend = self.backend.write().await;
        backend.insert_query_into_landing_zone(key, query)
    }

    /// Insert a tile for a given query. The query has to be inserted first.
    /// The tile is inserted into the landing zone and only moved to the cache when the query is finished.
    /// If the landing zone is full, the caching of the query is aborted.
    async fn insert_query_element(
        &self,
        key: &CanonicOperatorName,
        query_id: &QueryId,
        landing_zone_element: RasterTile2D<T>,
    ) -> Result<(), CacheError> {
        let mut backend = self.backend.write().await;
        backend.insert_query_element_into_landing_zone(key, query_id, landing_zone_element)
    }

    /// Abort the query and remove the tiles from the cache
    async fn abort_query(&self, key: &CanonicOperatorName, query_id: &QueryId) {
        let mut backend = self.backend.write().await;
        backend.discard_query_from_landing_zone(key, query_id);
    }

    /// Finish the query and make the tiles available in the cache
    async fn finish_query(
        &self,
        key: &CanonicOperatorName,
        query_id: &QueryId,
    ) -> Result<CacheEntryId, CacheError> {
        // TODO: maybe check if this cache result is already in the cache or could displace another one

        let mut backend = self.backend.write().await;
        backend.move_query_from_landing_to_cache(key, query_id)
    }
}

*/
#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{CacheHint, DateTime, SpatialPartition2D, SpatialResolution, TimeInterval},
        raster::{Grid, RasterProperties},
    };
    use serde_json::json;

    use super::*;

    async fn process_query(tile_cache: &mut SharedCache, op_name: CanonicOperatorName) {
        let query_id = tile_cache
            .insert_query::<RasterTile2D<u8>>(&op_name, &query_rect())
            .await
            .unwrap();

        tile_cache
            .insert_query_element::<RasterTile2D<u8>>(&op_name, &query_id, create_tile())
            .await
            .unwrap();

        tile_cache
            .finish_query::<RasterTile2D<u8>>(&op_name, &query_id)
            .await
            .unwrap();
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
        let landing_zone_entry = RasterLandingQueryEntry {
            query: query_rect(),
            elements: LandingZoneQueryTiles::U8(vec![create_tile()]),
        };
        let query_id = QueryId::new();
        let size_of_landing_zone_entry = landing_zone_entry.byte_size() + query_id.byte_size();
        let cache_entry: RasterCacheQueryEntry = landing_zone_entry.into();
        let cache_entry_id = CacheEntryId::new();
        let size_of_cache_entry = cache_entry.byte_size() + cache_entry_id.byte_size();

        // Select the max of both sizes
        // This is done because the landing zone should not be smaller then the cache
        let m_size = size_of_cache_entry.max(size_of_landing_zone_entry);

        // set limits s.t. three tiles fit
        let mut tile_cache = SharedCache {
            backend: RwLock::new(CacheBackend {
                raster_caches: Default::default(),
                vector_caches: Default::default(),
                lru: LruCache::unbounded(),
                cache_size: CacheSize::new(m_size * 3),
                landing_zone_size: CacheSize::new(m_size * 3),
            }),
        };

        // process three different queries
        process_query(&mut tile_cache, op(1)).await;
        process_query(&mut tile_cache, op(2)).await;
        process_query(&mut tile_cache, op(3)).await;

        // query the first one s.t. it is the most recently used
        tile_cache
            .query_cache::<RasterTile2D<u8>>(&op(1), &query_rect())
            .await
            .unwrap();
        // process a fourth query
        process_query(&mut tile_cache, op(4)).await;

        // assure the seconds query is evicted because it is the least recently used
        assert!(tile_cache
            .query_cache::<RasterTile2D<u8>>(&op(2), &query_rect())
            .await
            .unwrap()
            .is_none());

        // assure that the other queries are still in the cache
        for i in [1, 3, 4] {
            assert!(tile_cache
                .query_cache::<RasterTile2D<u8>>(&op(i), &query_rect())
                .await
                .unwrap()
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
        let mut tile_cache = SharedCache {
            backend: RwLock::new(CacheBackend {
                raster_caches: Default::default(),
                vector_caches: Default::default(),
                lru: LruCache::unbounded(),
                cache_size: CacheSize::new(usize::MAX),
                landing_zone_size: CacheSize::new(usize::MAX),
            }),
        };

        process_query(&mut tile_cache, op(1)).await;

        // access works because no ttl is set
        tile_cache
            .query_cache::<RasterTile2D<u8>>(&op(1), &query_rect())
            .await
            .unwrap()
            .unwrap();

        // manually expire entry
        {
            let mut backend = tile_cache.backend.write().await;
            let cache = backend.raster_caches.0.iter_mut().next().unwrap();

            let tiles = &mut cache.1.entries.iter_mut().next().unwrap().1.elements;
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
            .query_cache::<RasterTile2D<u8>>(&op(1), &query_rect())
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn tile_cache_init_size() {
        let tile_cache = SharedCache::new(100, 0.1).unwrap();

        let backend = tile_cache.backend.read().await;

        let cache_size = 90 * 1024 * 1024;
        let landing_zone_size = 10 * 1024 * 1024;

        assert_eq!(backend.cache_size.total_byte_size(), cache_size);
        assert_eq!(
            backend.landing_zone_size.total_byte_size(),
            landing_zone_size
        );
    }
}
