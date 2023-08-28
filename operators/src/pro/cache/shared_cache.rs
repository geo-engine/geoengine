use super::{
    cache_chunks::{CacheElementHitCheck, CachedFeatures, LandingZoneQueryFeatures},
    cache_tiles::{CachedTiles, CompressedRasterTile2D, LandingZoneQueryTiles},
    error::CacheError,
    util::CacheSize,
};
use crate::engine::CanonicOperatorName;
use crate::util::Result;
use async_trait::async_trait;
use futures::Stream;
use geoengine_datatypes::{
    collections::FeatureCollection,
    identifier,
    primitives::{CacheHint, Geometry, RasterQueryRectangle, VectorQueryRectangle},
    raster::Pixel,
    util::{arrow::ArrowTyped, test::TestDefault, ByteSize, Identifier},
};
use log::{debug, log_enabled};
use lru::LruCache;
use std::{collections::HashMap, hash::Hash, sync::Arc};
use tokio::sync::RwLock;

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
    raster_caches: HashMap<CanonicOperatorName, RasterOperatorCacheEntry>,
    vector_caches: HashMap<CanonicOperatorName, VectorOperatorCacheEntry>,

    cache_size: CacheSize,
    landing_zone_size: CacheSize,

    // we only use the LruCache for determining the least recently used elements and evict as many entries as needed to fit the new one
    lru: LruCache<CacheEntryId, TypedCanonicOperatorName>,
}

impl CacheBackend {
    /// This method removes entries from the cache until it can fit the given amount of bytes.
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

pub trait CacheEvictUntilFit {
    fn evict_entries_until_can_fit_bytes(&mut self, bytes: usize);
}

impl CacheEvictUntilFit for CacheBackend {
    fn evict_entries_until_can_fit_bytes(&mut self, bytes: usize) {
        self.evict_until_can_fit_bytes(bytes);
    }
}

pub trait CacheView<C, L>: CacheEvictUntilFit {
    fn operator_caches_mut(
        &mut self,
    ) -> &mut HashMap<CanonicOperatorName, OperatorCacheEntry<C, L>>;

    fn create_operator_cache_if_needed(&mut self, key: CanonicOperatorName) {
        // TODO: add size of the OperatorCacheEntry to the cache size?
        self.operator_caches_mut()
            .entry(key)
            .or_insert_with(|| OperatorCacheEntry::new());
    }

    fn remove_operator_cache(
        &mut self,
        key: &CanonicOperatorName,
    ) -> Option<OperatorCacheEntry<C, L>> {
        // TODO: remove the size of the OperatorCacheEntry to the cache size?
        self.operator_caches_mut().remove(key)
    }
}

#[allow(clippy::type_complexity)]
pub struct OperatorCacheEntryView<'a, C: CacheBackendElementExt> {
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
    C: CacheBackendElementExt + ByteSize,
    C::Query: Clone + CacheQueryMatch,
    CacheQueryEntry<C::Query, C::LandingZoneContainer>: ByteSize,
    CacheQueryEntry<C::Query, C::CacheContainer>: ByteSize,
{
    fn is_empty(&self) -> bool {
        self.operator_cache.is_empty()
    }

    /// This method removes a query from the landing zone.
    ///
    /// If the query is not in the landing zone, this method returns None.
    ///
    fn remove_query_from_landing_zone(
        &mut self,
        query_id: &QueryId,
    ) -> Option<CacheQueryEntry<C::Query, C::LandingZoneContainer>> {
        if let Some(entry) = self.operator_cache.remove_landing_zone_entry(query_id) {
            self.landing_zone_size.remove_element_bytes(query_id);
            self.landing_zone_size.remove_element_bytes(&entry);

            // debug output
            log::debug!(
                "Removed query {}. Landing zone size: {}. Landing zone size used: {}, Landing zone used percentage: {}.",
                query_id, self.landing_zone_size.total_byte_size(), self.landing_zone_size.byte_size_used(), self.landing_zone_size.size_used_fraction()
            );

            Some(entry)
        } else {
            None
        }
    }

    /// This method removes a query from the cache and the LRU.
    /// It will remove a queries cache entry from the cache and the LRU.
    ///
    /// If the query is not in the cache, this method returns None.
    ///
    fn remove_query_from_cache_and_lru(
        &mut self,
        cache_entry_id: &CacheEntryId,
    ) -> Option<CacheQueryEntry<C::Query, C::CacheContainer>> {
        if let Some(entry) = self.operator_cache.remove_cache_entry(cache_entry_id) {
            let old_lru_entry = self.lru.pop_entry(cache_entry_id);
            debug_assert!(old_lru_entry.is_some(), "CacheEntryId not found in LRU");
            self.cache_size.remove_element_bytes(cache_entry_id);
            self.cache_size.remove_element_bytes(&entry);

            log::debug!(
                "Removed cache entry {}. Cache size: {}. Cache size used: {}, Cache used percentage: {}.",
                cache_entry_id, self.cache_size.total_byte_size(), self.cache_size.byte_size_used(), self.cache_size.size_used_fraction()
            );

            Some(entry)
        } else {
            None
        }
    }

    /// This method removes a list of queries from the cache and the LRU.
    fn discard_queries_from_cache_and_lru(&mut self, cache_entry_ids: &[CacheEntryId]) {
        for cache_entry_id in cache_entry_ids {
            let old_entry = self.remove_query_from_cache_and_lru(cache_entry_id);
            debug_assert!(
                old_entry.is_some(),
                "CacheEntryId not found in OperatorCacheEntry"
            );
        }
    }

    /// This method adds a query element to the landing zone.
    /// It will add the element to the landing zone entry of the query.
    ///
    /// # Errors
    ///
    /// This method returns an error if the query is not in the landing zone.
    /// This method returns an error if the element is already expired.
    /// This method returns an error if the landing zone is full or the new element would cause the landing zone to overflow.
    ///
    fn add_query_element_to_landing_zone(
        &mut self,
        query_id: &QueryId,
        landing_zone_element: C,
    ) -> Result<(), CacheError> {
        let landing_zone_entry = self
            .operator_cache
            .landing_zone_entry_mut(query_id)
            .ok_or(CacheError::QueryNotFoundInLandingZone)?;

        if landing_zone_element.cache_hint().is_expired() {
            log::trace!("Element is already expired");
            return Err(CacheError::TileExpiredBeforeInsertion);
        };

        let element_bytes_size = landing_zone_element.byte_size();

        if !self.landing_zone_size.can_fit_bytes(element_bytes_size) {
            return Err(CacheError::NotEnoughSpaceInLandingZone);
        }

        // new entries might update the query bounds stored for this entry
        landing_zone_element.update_stored_query(&mut landing_zone_entry.query)?;

        // actually insert the element into the landing zone
        landing_zone_entry.insert_element(landing_zone_element)?;

        // we add the bytes size of the element to the landing zone size after we have inserted it.
        self.landing_zone_size
            .try_add_bytes(element_bytes_size)
            .expect(
            "The Landing Zone must have enough space for the element since we checked it before",
        );

        log::trace!(
            "Inserted tile for query {} into landing zone. Landing zone size: {}. Landing zone size used: {}. Landing zone used percentage: {}",
            query_id, self.landing_zone_size.total_byte_size(), self.landing_zone_size.byte_size_used(), self.landing_zone_size.size_used_fraction()
        );

        Ok(())
    }

    /// This method inserts a query into the landing zone.
    /// It will cause the operator cache to create a new landing zone entry.
    /// Therefore, the size of the query and the size of the landing zone entry will be added to the landing zone size.
    ///
    /// # Errors
    ///
    /// This method returns an error if the query is already in the landing zone.
    /// This method returns an error if the landing zone is full or the new query would cause the landing zone to overflow.
    ///
    fn insert_query_into_landing_zone(&mut self, query: &C::Query) -> Result<QueryId, CacheError> {
        let landing_zone_entry = CacheQueryEntry::create_empty::<C>(query.clone());
        let query_id = QueryId::new();

        let query_id_bytes_size = query_id.byte_size();
        let landing_zone_entry_bytes_size = landing_zone_entry.byte_size();

        self.landing_zone_size.try_add_bytes(query_id_bytes_size)?;

        // if this fails, we have to remove the query id size again
        if let Err(e) = self
            .landing_zone_size
            .try_add_bytes(landing_zone_entry_bytes_size)
        {
            self.landing_zone_size.remove_bytes(query_id_bytes_size);
            return Err(e);
        }

        // if this fails, we have to remove the query id size and the landing zone entry size again
        if let Err(e) = self
            .operator_cache
            .insert_landing_zone_entry(query_id, landing_zone_entry)
        {
            self.landing_zone_size.remove_bytes(query_id_bytes_size);
            self.landing_zone_size
                .remove_bytes(landing_zone_entry_bytes_size);
            return Err(e);
        }

        // debug output
        log::trace!(
            "Added query {} to landing zone. Landing zone size: {}. Landing zone size used: {}, Landing zone used percentage: {}.",
            query_id, self.landing_zone_size.total_byte_size(), self.landing_zone_size.byte_size_used(), self.landing_zone_size.size_used_fraction()
        );

        Ok(query_id)
    }

    /// This method inserts a cache entry into the cache and the LRU.
    /// It allows the element cache to overflow the cache size.
    /// This is done because the total cache size is the cache size + the landing zone size.
    /// This method is used when moving an element from the landing zone to the cache.
    ///
    /// # Errors
    ///
    /// This method returns an error if the cache entry is already in the cache.
    ///
    fn insert_cache_entry_allow_overflow(
        &mut self,
        cache_entry: CacheQueryEntry<C::Query, C::CacheContainer>,
        key: &CanonicOperatorName,
    ) -> Result<CacheEntryId, CacheError> {
        let cache_entry_id = CacheEntryId::new();
        let bytes = cache_entry.byte_size() + cache_entry_id.byte_size();
        // When inserting data from the landing zone into the cache, we allow the cache to overflow.
        // This is done because the total cache size is the cache size + the landing zone size.
        self.cache_size.add_bytes_allow_overflow(bytes);
        self.operator_cache
            .insert_cache_entry(cache_entry_id, cache_entry)?;
        // we have to wrap the key in a TypedCanonicOperatorName to be able to insert it into the LRU
        self.lru.push(
            cache_entry_id,
            C::typed_canonical_operator_name(key.clone()),
        );

        // debug output
        log::trace!(
            "Added cache entry {}. Cache size: {}. Cache size used: {}, Cache used percentage: {}.",
            cache_entry_id,
            self.cache_size.total_byte_size(),
            self.cache_size.byte_size_used(),
            self.cache_size.size_used_fraction()
        );

        Ok(cache_entry_id)
    }

    /// This method finds a cache entry in the cache that matches the query.
    /// It will also collect all expired cache entries.
    /// The cache entry is returned together with the expired ids.
    fn find_matching_cache_entry_and_collect_expired_entries(
        &mut self,
        query: &C::Query,
    ) -> CacheQueryResult<C::Query, C::CacheContainer> {
        let mut expired_cache_entry_ids = vec![];

        let x = self.operator_cache.iter().find(|&(id, entry)| {
            if entry.elements.is_expired() {
                expired_cache_entry_ids.push(*id);
                return false;
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

pub trait Cache<C: CacheBackendElementExt>:
    CacheView<
    CacheQueryEntry<C::Query, C::CacheContainer>,
    CacheQueryEntry<C::Query, C::LandingZoneContainer>,
>
where
    C::Query: Clone + CacheQueryMatch,
{
    /// This method returns a mutable reference to the cache entry of an operator.
    /// If there is no cache entry for the operator, this method returns None.
    fn operator_cache_view_mut(
        &mut self,
        key: &CanonicOperatorName,
    ) -> Option<OperatorCacheEntryView<C>>;

    /// This method queries the cache for a given query.
    /// If the query matches an entry in the cache, the cache entry is returned and it is promoted in the LRU.
    /// If the query does not match an entry in the cache, None is returned. Also if a cache entry is found but it is expired, None is returned.
    ///
    /// # Errors
    /// This method returns an error if the cache entry is not found.
    ///
    fn query_and_promote(
        &mut self,
        key: &CanonicOperatorName,
        query: &C::Query,
    ) -> Result<Option<Arc<Vec<C>>>, CacheError> {
        let mut cache = self
            .operator_cache_view_mut(key)
            .ok_or(CacheError::OperatorCacheEntryNotFound)?;

        let CacheQueryResult {
            cache_hit,
            expired_cache_entry_ids,
        } = cache.find_matching_cache_entry_and_collect_expired_entries(query);

        let res = if let Some((cache_entry_id, cache_entry)) = cache_hit {
            let potential_result_elements = cache_entry.elements.results_arc();

            // promote the cache entry in the LRU
            cache.lru.promote(&cache_entry_id);
            Some(potential_result_elements)
        } else {
            None
        };

        // discard expired cache entries
        cache.discard_queries_from_cache_and_lru(&expired_cache_entry_ids);

        Ok(res.flatten())
    }

    /// This method inserts a query into the cache.
    ///
    /// # Errors
    /// This method returns an error if the query is already in the cache.
    ///
    fn insert_query_into_landing_zone(
        &mut self,
        key: &CanonicOperatorName,
        query: &C::Query,
    ) -> Result<QueryId, CacheError> {
        self.create_operator_cache_if_needed(key.clone());
        self.operator_cache_view_mut(key)
            .expect("This method must not fail since the OperatorCache was created one line above.")
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

    /// This method discards a query from the landing zone.
    /// If the query is not in the landing zone, this method does nothing.
    fn discard_query_from_landing_zone(&mut self, key: &CanonicOperatorName, query_id: &QueryId) {
        if let Some(mut cache) = self.operator_cache_view_mut(key) {
            cache.remove_query_from_landing_zone(query_id);
            if cache.is_empty() {
                self.remove_operator_cache(key);
            }
        }
    }

    /// This method discards a query from the cache and the LRU.
    /// If the query is not in the cache, this method does nothing.
    fn discard_querys_from_cache_and_lru(
        &mut self,
        key: &CanonicOperatorName,
        cache_entry_ids: &[CacheEntryId],
    ) {
        if let Some(mut cache) = self.operator_cache_view_mut(key) {
            cache.discard_queries_from_cache_and_lru(cache_entry_ids);
            if cache.is_empty() {
                self.remove_operator_cache(key);
            }
        }
    }

    /// This method moves a query from the landing zone to the cache.
    /// It will remove the query from the landing zone and insert it into the cache.
    /// If the cache is full, the least recently used entries will be evicted if necessary to make room for the new entry.
    /// This method returns the cache entry id of the inserted cache entry.
    ///
    /// # Errors
    /// This method returns an error if the query is not in the landing zone.
    /// This method returns an error if the cache entry is already in the cache.
    /// This method returns an error if the cache is full and the least recently used entries cannot be evicted to make room for the new entry.
    ///
    fn move_query_from_landing_zone_to_cache(
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
        let cache_entry: CacheQueryEntry<
            <C as CacheBackendElement>::Query,
            <C as CacheBackendElementExt>::CacheContainer,
        > = C::landing_zone_to_cache_entry(landing_zone_entry);
        // when moving an element from the landing zone to the cache, we allow the cache size to overflow.
        // This is done because the total cache size is the cache size + the landing zone size.
        let cache_entry_id = operator_cache.insert_cache_entry_allow_overflow(cache_entry, key)?;
        // We could also first try to evict until the cache can hold the entry.
        // However, then we would need to lookup the cache entry twice.
        // To avoid that, we just evict after we moved the entry from the landing zone to the cache.
        // This is also not a problem since the total cache size is the cache size + the landing zone size.
        self.evict_entries_until_can_fit_bytes(0);

        Ok(cache_entry_id)
    }
}

impl<T> Cache<CompressedRasterTile2D<T>> for CacheBackend
where
    T: Pixel,
    CompressedRasterTile2D<T>: CacheBackendElementExt<
        Query = RasterQueryRectangle,
        LandingZoneContainer = LandingZoneQueryTiles,
        CacheContainer = CachedTiles,
    >,
{
    fn operator_cache_view_mut(
        &mut self,
        key: &CanonicOperatorName,
    ) -> Option<OperatorCacheEntryView<CompressedRasterTile2D<T>>> {
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

impl<T> Cache<FeatureCollection<T>> for CacheBackend
where
    T: Geometry + ArrowTyped,
    FeatureCollection<T>: CacheElementHitCheck
        + CacheBackendElementExt<
            Query = VectorQueryRectangle,
            LandingZoneContainer = LandingZoneQueryFeatures,
            CacheContainer = CachedFeatures,
        >,
{
    fn operator_cache_view_mut(
        &mut self,
        key: &CanonicOperatorName,
    ) -> Option<OperatorCacheEntryView<FeatureCollection<T>>> {
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

impl CacheView<RasterCacheQueryEntry, RasterLandingQueryEntry> for CacheBackend {
    fn operator_caches_mut(
        &mut self,
    ) -> &mut HashMap<CanonicOperatorName, RasterOperatorCacheEntry> {
        &mut self.raster_caches
    }
}

impl CacheView<VectorCacheQueryEntry, VectorLandingQueryEntry> for CacheBackend {
    fn operator_caches_mut(
        &mut self,
    ) -> &mut HashMap<CanonicOperatorName, VectorOperatorCacheEntry> {
        &mut self.vector_caches
    }
}

pub trait CacheBackendElement: ByteSize + Send + ByteSize
where
    Self: Sized,
{
    type Query: CacheQueryMatch + Clone + Send + Sync;

    fn update_stored_query(&self, query: &mut Self::Query) -> Result<(), CacheError>;

    fn cache_hint(&self) -> CacheHint;

    fn typed_canonical_operator_name(key: CanonicOperatorName) -> TypedCanonicOperatorName;
}

pub trait CacheBackendElementExt: CacheBackendElement {
    type LandingZoneContainer: LandingZoneElementsContainer<Self> + ByteSize;
    type CacheContainer: CacheElementsContainer<Self::Query, Self>
        + ByteSize
        + From<Self::LandingZoneContainer>;

    fn move_element_into_landing_zone(
        self,
        landing_zone: &mut Self::LandingZoneContainer,
    ) -> Result<(), super::error::CacheError>;

    fn create_empty_landing_zone() -> Self::LandingZoneContainer;

    fn results_arc(cache_elements_container: &Self::CacheContainer) -> Option<Arc<Vec<Self>>>;

    fn landing_zone_to_cache_entry(
        landing_zone_entry: CacheQueryEntry<Self::Query, Self::LandingZoneContainer>,
    ) -> CacheQueryEntry<Self::Query, Self::CacheContainer>;
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

        if landing_zone_ratio >= 0.5 {
            return Err(crate::error::Error::QueryingProcessorFailed {
                source: Box::new(CacheError::LandingZoneRatioMustBeSmallerThenHalfCacheSize),
            });
        }

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
pub struct OperatorCacheEntry<CacheEntriesContainer, LandingZoneEntriesContainer> {
    // for a given operator and query we need to look through all entries to find one that matches
    // TODO: use a multi-dimensional index to speed up the lookup
    entries: HashMap<CacheEntryId, CacheEntriesContainer>,

    // running queries insert their tiles as they are produced. The entry will be created once the query is done.
    // The query is identified by a Uuid instead of the query rectangle to avoid confusions with other queries
    landing_zone: HashMap<QueryId, LandingZoneEntriesContainer>,
}

impl<CacheEntriesContainer, LandingZoneEntriesContainer>
    OperatorCacheEntry<CacheEntriesContainer, LandingZoneEntriesContainer>
{
    pub fn new() -> Self {
        Self {
            entries: Default::default(),
            landing_zone: Default::default(),
        }
    }

    fn insert_landing_zone_entry(
        &mut self,
        query_id: QueryId,
        landing_zone_entry: LandingZoneEntriesContainer,
    ) -> Result<(), CacheError> {
        let old_entry = self.landing_zone.insert(query_id, landing_zone_entry);

        if old_entry.is_some() {
            Err(CacheError::QueryIdAlreadyInLandingZone)
        } else {
            Ok(())
        }
    }

    fn remove_landing_zone_entry(
        &mut self,
        query_id: &QueryId,
    ) -> Option<LandingZoneEntriesContainer> {
        self.landing_zone.remove(query_id)
    }

    fn landing_zone_entry_mut(
        &mut self,
        query_id: &QueryId,
    ) -> Option<&mut LandingZoneEntriesContainer> {
        self.landing_zone.get_mut(query_id)
    }

    fn insert_cache_entry(
        &mut self,
        cache_entry_id: CacheEntryId,
        cache_entry: CacheEntriesContainer,
    ) -> Result<(), CacheError> {
        let old_entry = self.entries.insert(cache_entry_id, cache_entry);

        if old_entry.is_some() {
            Err(CacheError::CacheEntryIdAlreadyInCache)
        } else {
            Ok(())
        }
    }

    fn remove_cache_entry(
        &mut self,
        cache_entry_id: &CacheEntryId,
    ) -> Option<CacheEntriesContainer> {
        self.entries.remove(cache_entry_id)
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.landing_zone.is_empty()
    }

    fn iter(&self) -> impl Iterator<Item = (&CacheEntryId, &CacheEntriesContainer)> {
        self.entries.iter()
    }
}

identifier!(QueryId);

impl ByteSize for QueryId {}

identifier!(CacheEntryId);

impl ByteSize for CacheEntryId {}

/// Holds all the elements for a given query and is able to answer queries that are fully contained
#[derive(Debug, Hash)]
pub struct CacheQueryEntry<Query, Elements> {
    query: Query,
    elements: Elements,
}
type RasterOperatorCacheEntry = OperatorCacheEntry<RasterCacheQueryEntry, RasterLandingQueryEntry>;
pub type RasterCacheQueryEntry = CacheQueryEntry<RasterQueryRectangle, CachedTiles>;
pub type RasterLandingQueryEntry = CacheQueryEntry<RasterQueryRectangle, LandingZoneQueryTiles>;

type VectorOperatorCacheEntry = OperatorCacheEntry<VectorCacheQueryEntry, VectorLandingQueryEntry>;
pub type VectorCacheQueryEntry = CacheQueryEntry<VectorQueryRectangle, CachedFeatures>;
pub type VectorLandingQueryEntry = CacheQueryEntry<VectorQueryRectangle, LandingZoneQueryFeatures>;

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

pub trait CacheElementsContainerInfos<Query> {
    fn is_expired(&self) -> bool;
}

pub trait CacheElementsContainer<Query, E>: CacheElementsContainerInfos<Query> {
    fn results_arc(&self) -> Option<Arc<Vec<E>>>;
}

impl CacheQueryEntry<RasterQueryRectangle, CachedTiles> {
    /// Return true if the query can be answered in full by this cache entry
    /// For this, the bbox and time has to be fully contained, and the spatial resolution has to match
    pub fn matches(&self, query: &RasterQueryRectangle) -> bool {
        self.query.spatial_bounds.contains(&query.spatial_bounds)
            && self.query.time_interval.contains(&query.time_interval)
            && self.query.spatial_resolution == query.spatial_resolution
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

impl From<VectorLandingQueryEntry> for VectorCacheQueryEntry {
    fn from(value: VectorLandingQueryEntry) -> Self {
        Self {
            query: value.query,
            elements: value.elements.into(),
        }
    }
}

pub trait CacheElement: Sized {
    type StoredCacheElement: CacheBackendElementExt<Query = Self::Query>;
    type Query: CacheQueryMatch;
    type ResultStream: Stream<Item = Result<Self, CacheError>>;

    fn into_stored_element(self) -> Self::StoredCacheElement;
    fn from_stored_element_ref(stored: &Self::StoredCacheElement) -> Result<Self, CacheError>;

    fn result_stream(
        stored_data: Arc<Vec<Self::StoredCacheElement>>,
        query: Self::Query,
    ) -> Self::ResultStream;
}

#[async_trait]
pub trait AsyncCache<C: CacheElement> {
    async fn query_cache(
        &self,
        key: &CanonicOperatorName,
        query: &C::Query,
    ) -> Result<Option<C::ResultStream>, CacheError>;

    async fn insert_query(
        &self,
        key: &CanonicOperatorName,
        query: &C::Query,
    ) -> Result<QueryId, CacheError>;

    async fn insert_query_element(
        &self,
        key: &CanonicOperatorName,
        query_id: &QueryId,
        landing_zone_element: C,
    ) -> Result<(), CacheError>;

    async fn abort_query(&self, key: &CanonicOperatorName, query_id: &QueryId);

    async fn finish_query(
        &self,
        key: &CanonicOperatorName,
        query_id: &QueryId,
    ) -> Result<CacheEntryId, CacheError>;
}

#[async_trait]
impl<C> AsyncCache<C> for SharedCache
where
    C: CacheElement + Send + Sync + 'static + ByteSize,
    CacheBackend: Cache<C::StoredCacheElement>,
    C::Query: Clone + CacheQueryMatch + Send + Sync,
{
    /// Query the cache and on hit create a stream of cache elements
    async fn query_cache(
        &self,
        key: &CanonicOperatorName,
        query: &C::Query,
    ) -> Result<Option<C::ResultStream>, CacheError> {
        let mut backend = self.backend.write().await;
        let res_data = backend.query_and_promote(key, query)?;
        Ok(res_data.map(|res_data| C::result_stream(res_data, query.clone())))
    }

    /// When inserting a new query, we first register the query and then insert the elements as they are produced
    /// This is to avoid confusing different queries on the same operator and query rectangle
    async fn insert_query(
        &self,
        key: &CanonicOperatorName,
        query: &C::Query,
    ) -> Result<QueryId, CacheError> {
        let mut backend = self.backend.write().await;
        backend.insert_query_into_landing_zone(key, query)
    }

    /// Insert a cachable element for a given query. The query has to be inserted first.
    /// The element is inserted into the landing zone and only moved to the cache when the query is finished.
    /// If the landing zone is full or the element size would cause the landing zone size to overflow, the caching of the query is aborted.
    async fn insert_query_element(
        &self,
        key: &CanonicOperatorName,
        query_id: &QueryId,
        landing_zone_element: C,
    ) -> Result<(), CacheError> {
        const LOG_LEVEL_THRESHOLD: log::Level = log::Level::Trace;
        let element_size = if log_enabled!(LOG_LEVEL_THRESHOLD) {
            landing_zone_element.byte_size()
        } else {
            0
        };

        let storeable_element =
            crate::util::spawn_blocking(|| landing_zone_element.into_stored_element())
                .await
                .map_err(|_| CacheError::BlockingElementConversion)?;

        if log_enabled!(LOG_LEVEL_THRESHOLD) {
            let storeable_element_size = storeable_element.byte_size();
            tracing::trace!(
                "Inserting element into landing zone for query {:?} on operator {}. Element size: {} bytes, storable element size: {} bytes, ratio: {}",
                query_id,
                key,
                element_size,
                storeable_element_size,
                storeable_element_size as f64 / element_size as f64
            );
        }

        let mut backend = self.backend.write().await;
        backend.insert_query_element_into_landing_zone(key, query_id, storeable_element)
    }

    /// Abort the query and remove already inserted elements from the caches landing zone
    async fn abort_query(&self, key: &CanonicOperatorName, query_id: &QueryId) {
        let mut backend = self.backend.write().await;
        backend.discard_query_from_landing_zone(key, query_id);
    }

    /// Finish the query and make the inserted elements available in the cache
    async fn finish_query(
        &self,
        key: &CanonicOperatorName,
        query_id: &QueryId,
    ) -> Result<CacheEntryId, CacheError> {
        let mut backend = self.backend.write().await;
        backend.move_query_from_landing_zone_to_cache(key, query_id)
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{CacheHint, DateTime, SpatialPartition2D, SpatialResolution, TimeInterval},
        raster::{Grid, RasterProperties, RasterTile2D},
    };
    use serde_json::json;
    use std::sync::Arc;

    use crate::pro::cache::cache_tiles::{CompressedGridOrEmpty, CompressedMaskedGrid};

    use super::*;

    async fn process_query_async(tile_cache: &mut SharedCache, op_name: CanonicOperatorName) {
        let query_id = <SharedCache as AsyncCache<RasterTile2D<u8>>>::insert_query(
            tile_cache,
            &op_name,
            &query_rect(),
        )
        .await
        .unwrap();

        tile_cache
            .insert_query_element(&op_name, &query_id, create_tile())
            .await
            .unwrap();

        <SharedCache as AsyncCache<RasterTile2D<u8>>>::finish_query(
            tile_cache, &op_name, &query_id,
        )
        .await
        .unwrap();
    }

    fn process_query(tile_cache: &mut CacheBackend, op_name: &CanonicOperatorName) {
        let query_id =
            <CacheBackend as Cache<CompressedRasterTile2D<u8>>>::insert_query_into_landing_zone(
                tile_cache,
                op_name,
                &query_rect(),
            )
            .unwrap();

        tile_cache
            .insert_query_element_into_landing_zone(op_name, &query_id, create_compressed_tile())
            .unwrap();

        <CacheBackend as Cache<CompressedRasterTile2D<u8>>>::move_query_from_landing_zone_to_cache(
            tile_cache, op_name, &query_id,
        )
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

    fn create_compressed_tile() -> CompressedRasterTile2D<u8> {
        CompressedRasterTile2D::<u8> {
            time: TimeInterval::new_instant(DateTime::new_utc(2014, 3, 1, 0, 0, 0)).unwrap(),
            tile_position: [-1, 0].into(),
            global_geo_transform: TestDefault::test_default(),
            grid_array: CompressedGridOrEmpty::Compressed(CompressedMaskedGrid::new(
                [3, 2].into(),
                vec![1, 2, 3, 4, 5, 6],
                vec![1; 6],
            )),
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
            elements: LandingZoneQueryTiles::U8(vec![create_compressed_tile()]),
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

        let mut cache_backend = CacheBackend {
            raster_caches: Default::default(),
            vector_caches: Default::default(),
            lru: LruCache::unbounded(),
            cache_size: CacheSize::new(m_size * 3),
            landing_zone_size: CacheSize::new(m_size * 3),
        };

        // process three different queries
        process_query(&mut cache_backend, &op(1));
        process_query(&mut cache_backend, &op(2));
        process_query(&mut cache_backend, &op(3));

        // query the first one s.t. it is the most recently used
        <CacheBackend as Cache<CompressedRasterTile2D<u8>>>::query_and_promote(
            &mut cache_backend,
            &op(1),
            &query_rect(),
        )
        .unwrap();

        // process a fourth query
        process_query(&mut cache_backend, &op(4));

        // assure the seconds query is evicted because it is the least recently used
        assert!(
            <CacheBackend as Cache<CompressedRasterTile2D<u8>>>::query_and_promote(
                &mut cache_backend,
                &op(2),
                &query_rect()
            )
            .unwrap()
            .is_none()
        );

        // assure that the other queries are still in the cache
        for i in [1, 3, 4] {
            assert!(
                <CacheBackend as Cache<CompressedRasterTile2D<u8>>>::query_and_promote(
                    &mut cache_backend,
                    &op(i),
                    &query_rect()
                )
                .unwrap()
                .is_some()
            );
        }

        assert_eq!(
            cache_backend.cache_size.byte_size_used(),
            3 * size_of_cache_entry
        );
    }

    #[test]
    fn cache_byte_size() {
        assert_eq!(create_compressed_tile().byte_size(), 268);
        assert_eq!(
            CachedTiles::U8(Arc::new(vec![create_compressed_tile()])).byte_size(),
            /* enum + arc */ 16 + /* vec */ 24  + /* tile */ 268
        );
        assert_eq!(
            CachedTiles::U8(Arc::new(vec![
                create_compressed_tile(),
                create_compressed_tile()
            ]))
            .byte_size(),
            /* enum + arc */ 16 + /* vec */ 24  + /* tile */ 2 * 268
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

        process_query_async(&mut tile_cache, op(1)).await;

        // access works because no ttl is set
        <SharedCache as AsyncCache<RasterTile2D<u8>>>::query_cache(
            &tile_cache,
            &op(1),
            &query_rect(),
        )
        .await
        .unwrap()
        .unwrap();

        // manually expire entry
        {
            let mut backend = tile_cache.backend.write().await;
            let cache = backend.raster_caches.iter_mut().next().unwrap();

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
        assert!(<SharedCache as AsyncCache<RasterTile2D<u8>>>::query_cache(
            &tile_cache,
            &op(1),
            &query_rect()
        )
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
