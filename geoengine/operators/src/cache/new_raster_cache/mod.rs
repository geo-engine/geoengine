use crate::cache::cache_tiles::CompressedRasterTile2D;
use crate::cache::cache_tiles::CompressedRasterTileExt;
use crate::cache::error::CacheError;
use crate::engine::{
    BoxRasterQueryProcessor, CanonicOperatorName, InitializedRasterOperator, QueryContext,
    QueryProcessor, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
    TypedRasterQueryProcessor, WorkflowOperatorPath,
};
use crate::optimization::OptimizationError;
use crate::util::Result;
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use futures::stream;
use futures::stream::BoxStream;
use geoengine_datatypes::primitives::{
    BandSelection, CacheHint, QueryRectangle, RasterQueryRectangle, SpatialResolution, TimeInterval,
};
use geoengine_datatypes::raster::{
    GridBoundingBox2D, GridIdx2D, Pixel, RasterTile2D, TileInformation,
};
use geoengine_datatypes::util::ByteSize;
use geoengine_datatypes::util::test::TestDefault;
use std::collections::HashMap;
use std::collections::HashSet;
use std::iter;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::RwLock;

pub struct NewRasterCache<Store> {
    store: Store,
}

pub enum NewRasterCacheEnum {
    InMemoryCompressedRasterTile2DFifo(
        NewRasterCache<InMemoryCacheStore<TypedCompressedRasterTile2D, FifoEvictionStrategy>>,
    ),
    InMemoryCompressedRasterTile2DLru(
        NewRasterCache<InMemoryCacheStore<TypedCompressedRasterTile2D, LruEvictionStrategy>>,
    ),
}

impl TestDefault for NewRasterCacheEnum {
    fn test_default() -> Self {
        Self::InMemoryCompressedRasterTile2DFifo(NewRasterCache {
            store: InMemoryCacheStore {
                cache: RwLock::new(HashMap::new()),
                eviction_strategy: RwLock::new(FifoEvictionStrategy::new(8_589_934_592)),
                total_size: Arc::new(AtomicUsize::new(0)),
            },
        })
    }
}

impl NewRasterCacheEnum {
    pub fn new_fifo(capacity_bytes: usize) -> Self {
        Self::InMemoryCompressedRasterTile2DFifo(NewRasterCache {
            store: InMemoryCacheStore {
                cache: RwLock::new(HashMap::new()),
                eviction_strategy: RwLock::new(FifoEvictionStrategy::new(capacity_bytes)),
                total_size: Arc::new(AtomicUsize::new(0)),
            },
        })
    }

    pub fn new_lru(capacity_bytes: usize) -> Self {
        Self::InMemoryCompressedRasterTile2DLru(NewRasterCache {
            store: InMemoryCacheStore {
                cache: RwLock::new(HashMap::new()),
                eviction_strategy: RwLock::new(LruEvictionStrategy::new(capacity_bytes)),
                total_size: Arc::new(AtomicUsize::new(0)),
            },
        })
    }

    async fn get(
        &self,
        key: &CacheKey,
    ) -> Result<Option<Arc<SizeTrackedEntry<TypedCompressedRasterTile2D>>>> {
        match self {
            NewRasterCacheEnum::InMemoryCompressedRasterTile2DFifo(cache) => {
                cache.store.get(key).await
            }
            NewRasterCacheEnum::InMemoryCompressedRasterTile2DLru(cache) => {
                cache.store.get(key).await
            }
        }
    }

    async fn insert(&self, key: CacheKey, tile: TypedRasterTile2D) -> Result<()> {
        match self {
            NewRasterCacheEnum::InMemoryCompressedRasterTile2DFifo(cache) => {
                cache.store.insert(key, tile).await
            }
            NewRasterCacheEnum::InMemoryCompressedRasterTile2DLru(cache) => {
                cache.store.insert(key, tile).await
            }
        }
    }

    pub fn byte_size(&self) -> usize {
        match self {
            NewRasterCacheEnum::InMemoryCompressedRasterTile2DFifo(cache) => {
                cache.store.total_size.load(Ordering::SeqCst)
            }
            NewRasterCacheEnum::InMemoryCompressedRasterTile2DLru(cache) => {
                cache.store.total_size.load(Ordering::SeqCst)
            }
        }
    }
}

#[async_trait]
trait CacheStore: Send + Sync + 'static {
    type SF: Send + Sync + 'static;
    type ES: EvictionStrategy;

    async fn get(&self, key: &CacheKey) -> Result<Option<Arc<Self::SF>>>;

    async fn insert(&self, key: CacheKey, tile: TypedRasterTile2D) -> Result<()>;
}

trait EvictionStrategy: Send + Sync + 'static {
    fn record_access(&mut self, key: &CacheKey, size: usize, cache_hint: CacheHint);
    fn record_hit(&mut self, key: &CacheKey);
    fn record_removal(&mut self, key: &CacheKey);

    fn record_hit_needs_exclusive_access(&self) -> bool {
        true
    }

    fn capacity(&self) -> usize;

    fn plan_eviction<F>(
        &self,
        current_size: usize,
        required_space: usize,
        is_pinned: F,
    ) -> Result<EvictionPlan>
    where
        F: FnMut(&CacheKey) -> bool;
}

struct EvictionPlan {
    keys_to_remove: Vec<CacheKey>,
    freed_bytes: usize,
}

pub struct FifoEvictionStrategy {
    queue: Vec<EvictionStrategyItem>,
    capacity: usize,
}

struct EvictionStrategyItem {
    key: CacheKey,
    size: usize,
    cache_hint: CacheHint,
}

impl FifoEvictionStrategy {
    fn new(capacity: usize) -> Self {
        FifoEvictionStrategy {
            queue: Vec::new(),
            capacity,
        }
    }
}

impl EvictionStrategy for FifoEvictionStrategy {
    fn record_access(&mut self, key: &CacheKey, size: usize, cache_hint: CacheHint) {
        self.queue.push(EvictionStrategyItem {
            key: key.clone(),
            size,
            cache_hint,
        });
    }

    fn record_hit(&mut self, _key: &CacheKey) {}

    fn record_hit_needs_exclusive_access(&self) -> bool {
        false
    }

    fn record_removal(&mut self, key: &CacheKey) {
        self.queue.remove(
            self.queue
                .iter()
                .position(|item| item.key.eq(key))
                .expect("Key must exist in eviction strategy"),
        );
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn plan_eviction<F>(
        &self,
        current_size: usize,
        required_space: usize,
        mut is_pinned: F,
    ) -> Result<EvictionPlan>
    where
        F: FnMut(&CacheKey) -> bool,
    {
        let needed = (current_size + required_space).saturating_sub(self.capacity);

        if needed == 0 {
            return Ok(EvictionPlan {
                keys_to_remove: vec![],
                freed_bytes: 0,
            });
        }

        let mut plan = EvictionPlan {
            keys_to_remove: vec![],
            freed_bytes: 0,
        };
        let mut expired_keys = HashSet::new();

        for item in &self.queue {
            if item.cache_hint.is_expired() {
                plan.keys_to_remove.push(item.key.clone());
                expired_keys.insert(item.key.clone());

                if !is_pinned(&item.key) {
                    plan.freed_bytes += item.size;
                }
            }
        }

        if plan.freed_bytes >= needed {
            return Ok(plan);
        }

        for item in self
            .queue
            .iter()
            .filter(|item| !expired_keys.contains(&item.key) && !is_pinned(&item.key))
        {
            plan.keys_to_remove.push(item.key.clone());
            plan.freed_bytes += item.size;

            if plan.freed_bytes >= needed {
                return Ok(plan);
            }
        }

        Ok(plan)
    }
}

pub struct LruEvictionStrategy {
    slab: Vec<Option<LruNode>>,
    free: Vec<usize>,
    index: HashMap<CacheKey, usize>,
    head: Option<usize>, // most recently used
    tail: Option<usize>, // least recently used
    capacity: usize,
}

struct LruNode {
    key: CacheKey,
    size: usize,
    cache_hint: CacheHint,
    prev: Option<usize>,
    next: Option<usize>,
}

impl LruEvictionStrategy {
    fn new(capacity: usize) -> Self {
        LruEvictionStrategy {
            slab: Vec::new(),
            free: Vec::new(),
            index: HashMap::new(),
            head: None,
            tail: None,
            capacity,
        }
    }

    fn node(&self, idx: usize) -> &LruNode {
        self.slab[idx].as_ref().expect("slot must be occupied")
    }

    fn node_mut(&mut self, idx: usize) -> &mut LruNode {
        self.slab[idx].as_mut().expect("slot must be occupied")
    }

    fn detach(&mut self, idx: usize) {
        let (prev, next) = {
            let node = self.node(idx);
            (node.prev, node.next)
        };

        match prev {
            Some(p) => self.node_mut(p).next = next,
            None => self.head = next,
        }
        match next {
            Some(n) => self.node_mut(n).prev = prev,
            None => self.tail = prev,
        }
    }

    fn attach_front(&mut self, idx: usize) {
        let old_head = self.head;

        {
            let node = self.node_mut(idx);
            node.prev = None;
            node.next = old_head;
        }

        match old_head {
            Some(h) => self.node_mut(h).prev = Some(idx),
            None => self.tail = Some(idx),
        }

        self.head = Some(idx);
    }
}

impl EvictionStrategy for LruEvictionStrategy {
    fn record_access(&mut self, key: &CacheKey, size: usize, cache_hint: CacheHint) {
        let idx = match self.free.pop() {
            Some(idx) => idx,
            None => {
                self.slab.push(None);
                self.slab.len() - 1
            }
        };

        self.slab[idx] = Some(LruNode {
            key: key.clone(),
            size,
            cache_hint,
            prev: None,
            next: None,
        });
        self.index.insert(key.clone(), idx);
        self.attach_front(idx);
    }

    fn record_hit(&mut self, key: &CacheKey) {
        if let Some(&idx) = self.index.get(key) {
            self.detach(idx);
            self.attach_front(idx);
        }
    }

    fn record_removal(&mut self, key: &CacheKey) {
        let idx = self
            .index
            .remove(key)
            .expect("Key must exist in eviction strategy");

        self.detach(idx);
        self.slab[idx] = None;
        self.free.push(idx);
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn plan_eviction<F>(
        &self,
        current_size: usize,
        required_space: usize,
        mut is_pinned: F,
    ) -> Result<EvictionPlan>
    where
        F: FnMut(&CacheKey) -> bool,
    {
        let needed = (current_size + required_space).saturating_sub(self.capacity);

        if needed == 0 {
            return Ok(EvictionPlan {
                keys_to_remove: vec![],
                freed_bytes: 0,
            });
        }

        let mut plan = EvictionPlan {
            keys_to_remove: vec![],
            freed_bytes: 0,
        };
        let mut expired_keys = HashSet::new();

        let mut cursor = self.head;
        while let Some(idx) = cursor {
            let node = self.node(idx);

            if node.cache_hint.is_expired() {
                plan.keys_to_remove.push(node.key.clone());
                expired_keys.insert(node.key.clone());

                if !is_pinned(&node.key) {
                    plan.freed_bytes += node.size;
                }
            }

            cursor = node.next;
        }

        if plan.freed_bytes >= needed {
            return Ok(plan);
        }

        let mut cursor = self.tail;
        while let Some(idx) = cursor {
            let node = self.node(idx);

            if !expired_keys.contains(&node.key) && !is_pinned(&node.key) {
                plan.keys_to_remove.push(node.key.clone());
                plan.freed_bytes += node.size;

                if plan.freed_bytes >= needed {
                    return Ok(plan);
                }
            }

            cursor = node.prev;
        }

        Ok(plan)
    }
}

#[cfg(test)]
mod eviction_strategy_tests {
    use super::{CacheKey, EvictionStrategy, FifoEvictionStrategy, LruEvictionStrategy, TileIndex};
    use geoengine_datatypes::primitives::{CacheExpiration, CacheHint, TimeInterval};

    fn expired_hint() -> CacheHint {
        CacheHint::with_created_and_expires(
            geoengine_datatypes::primitives::DateTime::new_utc(2000, 1, 1, 0, 0, 0),
            CacheExpiration::from(geoengine_datatypes::primitives::DateTime::new_utc(
                2000, 1, 1, 0, 0, 0,
            )),
        )
    }

    fn key(name: &str) -> CacheKey {
        (
            crate::engine::CanonicOperatorName::new(&name).expect("must serialize"),
            0u32,
            TimeInterval::default(),
            TileIndex::from([0, 0]),
        )
    }

    #[test]
    fn fifo_evicts_expired_pinned_key_without_counting_its_bytes_as_freed() {
        let mut strategy = FifoEvictionStrategy::new(100);
        let key_a = key("a");
        let key_b = key("b");

        strategy.record_access(&key_a, 40, expired_hint());
        strategy.record_access(&key_b, 60, CacheHint::max_duration());

        let plan = strategy.plan_eviction(100, 50, |k| k == &key_a).unwrap();

        assert_eq!(plan.keys_to_remove, vec![key_a, key_b]);
        assert_eq!(plan.freed_bytes, 60);
    }

    #[test]
    fn fifo_expired_unpinned_key_alone_can_satisfy_required_space() {
        let mut strategy = FifoEvictionStrategy::new(100);
        let key_a = key("a");
        let key_b = key("b");

        strategy.record_access(&key_a, 40, expired_hint());
        strategy.record_access(&key_b, 60, CacheHint::max_duration());

        let plan = strategy.plan_eviction(100, 30, |_| false).unwrap();

        assert_eq!(plan.keys_to_remove, vec![key_a]);
        assert_eq!(plan.freed_bytes, 40);
    }

    #[test]
    fn lru_evicts_expired_pinned_key_without_counting_its_bytes_as_freed() {
        let mut strategy = LruEvictionStrategy::new(100);
        let key_a = key("a");
        let key_b = key("b");
        let key_c = key("c");

        strategy.record_access(&key_a, 40, expired_hint());
        strategy.record_access(&key_b, 30, CacheHint::max_duration());
        strategy.record_access(&key_c, 30, CacheHint::max_duration());
        strategy.record_hit(&key_c);

        let plan = strategy.plan_eviction(100, 20, |k| k == &key_a).unwrap();

        assert_eq!(plan.keys_to_remove, vec![key_a, key_b]);
        assert_eq!(plan.freed_bytes, 30);
    }

    #[test]
    fn fifo_record_hit_does_not_need_exclusive_access() {
        assert!(!FifoEvictionStrategy::new(100).record_hit_needs_exclusive_access());
    }

    #[test]
    fn lru_record_hit_needs_exclusive_access() {
        assert!(LruEvictionStrategy::new(100).record_hit_needs_exclusive_access());
    }

    #[test]
    fn fifo_correct_eviction_target_with_partial_cache() {
        let mut strategy = FifoEvictionStrategy::new(100);
        let key_a = key("a");
        let key_b = key("b");

        strategy.record_access(&key_a, 60, CacheHint::max_duration());
        strategy.record_access(&key_b, 20, CacheHint::max_duration());

        let plan = strategy.plan_eviction(60, 70, |_| false).unwrap();

        assert_eq!(plan.keys_to_remove, vec![key_a]);
        assert_eq!(plan.freed_bytes, 60);
    }

    #[test]
    fn lru_correct_eviction_target_with_partial_cache() {
        let mut strategy = LruEvictionStrategy::new(100);
        let key_a = key("a");
        let key_b = key("b");
        let key_c = key("c");

        strategy.record_access(&key_a, 60, CacheHint::max_duration());
        strategy.record_access(&key_b, 20, CacheHint::max_duration());
        strategy.record_access(&key_c, 10, CacheHint::max_duration());
        strategy.record_hit(&key_c);

        let plan = strategy.plan_eviction(60, 70, |_| false).unwrap();

        assert_eq!(plan.keys_to_remove, vec![key_a]);
        assert_eq!(plan.freed_bytes, 60);
    }
}

struct SizeTrackedEntry<SF> {
    value: SF,
    size: usize,
    total_size: Arc<AtomicUsize>,
}

impl<SF> SizeTrackedEntry<SF> {
    fn new(value: SF, size: usize, total_size: Arc<AtomicUsize>) -> Self {
        total_size.fetch_add(size, Ordering::SeqCst);
        Self {
            value,
            size,
            total_size,
        }
    }
}

impl<SF> std::ops::Deref for SizeTrackedEntry<SF> {
    type Target = SF;

    fn deref(&self) -> &SF {
        &self.value
    }
}

impl<SF> Drop for SizeTrackedEntry<SF> {
    fn drop(&mut self) {
        self.total_size.fetch_sub(self.size, Ordering::SeqCst);
    }
}

pub struct InMemoryCacheStore<SF, ES> {
    cache: RwLock<HashMap<CacheKey, Arc<SizeTrackedEntry<SF>>>>,
    eviction_strategy: RwLock<ES>,
    total_size: Arc<AtomicUsize>,
}

type CacheKey = (CanonicOperatorName, Band, TimeInterval, TileIndex);
type Band = u32;
type TileIndex = GridIdx2D;

trait StorageFormat: Send + Sync + Sized + 'static {
    fn store(tile: TypedRasterTile2D) -> Result<Self>;

    fn load(&self) -> Result<TypedRasterTile2D>;

    fn byte_size(&self) -> Result<usize>;

    fn is_expired(&self) -> bool;
}

#[derive(Clone)]
enum TypedRasterTile2D {
    I8(RasterTile2D<i8>),
    I16(RasterTile2D<i16>),
    I32(RasterTile2D<i32>),
    I64(RasterTile2D<i64>),
    U8(RasterTile2D<u8>),
    U16(RasterTile2D<u16>),
    U32(RasterTile2D<u32>),
    U64(RasterTile2D<u64>),
    F32(RasterTile2D<f32>),
    F64(RasterTile2D<f64>),
}

impl TypedRasterTile2D {
    fn cache_hint(&self) -> CacheHint {
        match self {
            TypedRasterTile2D::I8(tile) => tile.cache_hint,
            TypedRasterTile2D::I16(tile) => tile.cache_hint,
            TypedRasterTile2D::I32(tile) => tile.cache_hint,
            TypedRasterTile2D::I64(tile) => tile.cache_hint,
            TypedRasterTile2D::U8(tile) => tile.cache_hint,
            TypedRasterTile2D::U16(tile) => tile.cache_hint,
            TypedRasterTile2D::U32(tile) => tile.cache_hint,
            TypedRasterTile2D::U64(tile) => tile.cache_hint,
            TypedRasterTile2D::F32(tile) => tile.cache_hint,
            TypedRasterTile2D::F64(tile) => tile.cache_hint,
        }
    }
}

pub enum TypedCompressedRasterTile2D {
    I8(CompressedRasterTile2D<i8>),
    I16(CompressedRasterTile2D<i16>),
    I32(CompressedRasterTile2D<i32>),
    I64(CompressedRasterTile2D<i64>),
    U8(CompressedRasterTile2D<u8>),
    U16(CompressedRasterTile2D<u16>),
    U32(CompressedRasterTile2D<u32>),
    U64(CompressedRasterTile2D<u64>),
    F32(CompressedRasterTile2D<f32>),
    F64(CompressedRasterTile2D<f64>),
}

impl StorageFormat for TypedCompressedRasterTile2D {
    fn store(tile: TypedRasterTile2D) -> Result<Self> {
        match tile {
            TypedRasterTile2D::I8(tile) => Ok(TypedCompressedRasterTile2D::I8(
                CompressedRasterTile2D::compress_tile(tile),
            )),
            TypedRasterTile2D::I16(tile) => Ok(TypedCompressedRasterTile2D::I16(
                CompressedRasterTile2D::compress_tile(tile),
            )),
            TypedRasterTile2D::I32(tile) => Ok(TypedCompressedRasterTile2D::I32(
                CompressedRasterTile2D::compress_tile(tile),
            )),
            TypedRasterTile2D::I64(tile) => Ok(TypedCompressedRasterTile2D::I64(
                CompressedRasterTile2D::compress_tile(tile),
            )),
            TypedRasterTile2D::U8(tile) => Ok(TypedCompressedRasterTile2D::U8(
                CompressedRasterTile2D::compress_tile(tile),
            )),
            TypedRasterTile2D::U16(tile) => Ok(TypedCompressedRasterTile2D::U16(
                CompressedRasterTile2D::compress_tile(tile),
            )),
            TypedRasterTile2D::U32(tile) => Ok(TypedCompressedRasterTile2D::U32(
                CompressedRasterTile2D::compress_tile(tile),
            )),
            TypedRasterTile2D::U64(tile) => Ok(TypedCompressedRasterTile2D::U64(
                CompressedRasterTile2D::compress_tile(tile),
            )),
            TypedRasterTile2D::F32(tile) => Ok(TypedCompressedRasterTile2D::F32(
                CompressedRasterTile2D::compress_tile(tile),
            )),
            TypedRasterTile2D::F64(tile) => Ok(TypedCompressedRasterTile2D::F64(
                CompressedRasterTile2D::compress_tile(tile),
            )),
        }
    }

    fn load(&self) -> Result<TypedRasterTile2D> {
        match self {
            TypedCompressedRasterTile2D::I8(compressed) => Ok(TypedRasterTile2D::I8(
                CompressedRasterTile2D::decompress_tile(compressed)?,
            )),
            TypedCompressedRasterTile2D::I16(compressed) => Ok(TypedRasterTile2D::I16(
                CompressedRasterTile2D::decompress_tile(compressed)?,
            )),
            TypedCompressedRasterTile2D::I32(compressed) => Ok(TypedRasterTile2D::I32(
                CompressedRasterTile2D::decompress_tile(compressed)?,
            )),
            TypedCompressedRasterTile2D::I64(compressed) => Ok(TypedRasterTile2D::I64(
                CompressedRasterTile2D::decompress_tile(compressed)?,
            )),
            TypedCompressedRasterTile2D::U8(compressed) => Ok(TypedRasterTile2D::U8(
                CompressedRasterTile2D::decompress_tile(compressed)?,
            )),
            TypedCompressedRasterTile2D::U16(compressed) => Ok(TypedRasterTile2D::U16(
                CompressedRasterTile2D::decompress_tile(compressed)?,
            )),
            TypedCompressedRasterTile2D::U32(compressed) => Ok(TypedRasterTile2D::U32(
                CompressedRasterTile2D::decompress_tile(compressed)?,
            )),
            TypedCompressedRasterTile2D::U64(compressed) => Ok(TypedRasterTile2D::U64(
                CompressedRasterTile2D::decompress_tile(compressed)?,
            )),
            TypedCompressedRasterTile2D::F32(compressed) => Ok(TypedRasterTile2D::F32(
                CompressedRasterTile2D::decompress_tile(compressed)?,
            )),
            TypedCompressedRasterTile2D::F64(compressed) => Ok(TypedRasterTile2D::F64(
                CompressedRasterTile2D::decompress_tile(compressed)?,
            )),
        }
    }

    fn byte_size(&self) -> Result<usize> {
        match self {
            TypedCompressedRasterTile2D::I8(compressed) => Ok(compressed.byte_size()),
            TypedCompressedRasterTile2D::I16(compressed) => Ok(compressed.byte_size()),
            TypedCompressedRasterTile2D::I32(compressed) => Ok(compressed.byte_size()),
            TypedCompressedRasterTile2D::I64(compressed) => Ok(compressed.byte_size()),
            TypedCompressedRasterTile2D::U8(compressed) => Ok(compressed.byte_size()),
            TypedCompressedRasterTile2D::U16(compressed) => Ok(compressed.byte_size()),
            TypedCompressedRasterTile2D::U32(compressed) => Ok(compressed.byte_size()),
            TypedCompressedRasterTile2D::U64(compressed) => Ok(compressed.byte_size()),
            TypedCompressedRasterTile2D::F32(compressed) => Ok(compressed.byte_size()),
            TypedCompressedRasterTile2D::F64(compressed) => Ok(compressed.byte_size()),
        }
    }

    fn is_expired(&self) -> bool {
        match self {
            TypedCompressedRasterTile2D::I8(compressed) => compressed.cache_hint.is_expired(),
            TypedCompressedRasterTile2D::I16(compressed) => compressed.cache_hint.is_expired(),
            TypedCompressedRasterTile2D::I32(compressed) => compressed.cache_hint.is_expired(),
            TypedCompressedRasterTile2D::I64(compressed) => compressed.cache_hint.is_expired(),
            TypedCompressedRasterTile2D::U8(compressed) => compressed.cache_hint.is_expired(),
            TypedCompressedRasterTile2D::U16(compressed) => compressed.cache_hint.is_expired(),
            TypedCompressedRasterTile2D::U32(compressed) => compressed.cache_hint.is_expired(),
            TypedCompressedRasterTile2D::U64(compressed) => compressed.cache_hint.is_expired(),
            TypedCompressedRasterTile2D::F32(compressed) => compressed.cache_hint.is_expired(),
            TypedCompressedRasterTile2D::F64(compressed) => compressed.cache_hint.is_expired(),
        }
    }
}

#[async_trait]
impl<SF, ES> CacheStore for InMemoryCacheStore<SF, ES>
where
    SF: StorageFormat,
    ES: EvictionStrategy,
{
    type SF = SizeTrackedEntry<SF>;
    type ES = ES;

    async fn get(&self, key: &CacheKey) -> Result<Option<Arc<Self::SF>>> {
        let hit = {
            let cache = self.cache.read().await;
            cache.get(key).map(Arc::clone)
        };

        if let Some(entry) = &hit {
            if entry.value.is_expired() {
                let mut cache = self.cache.write().await;
                let mut eviction_strategy = self.eviction_strategy.write().await;

                if cache.contains_key(key) {
                    cache.remove(key);
                    eviction_strategy.record_removal(key);
                }

                return Ok(None);
            }

            let needs_exclusive_access = self
                .eviction_strategy
                .read()
                .await
                .record_hit_needs_exclusive_access();

            if needs_exclusive_access {
                self.eviction_strategy.write().await.record_hit(key);
            }
        }

        Ok(hit)
    }

    async fn insert(&self, key: CacheKey, tile: TypedRasterTile2D) -> Result<()> {
        let cache_hint = tile.cache_hint();
        let value = SF::store(tile)?;
        let required_space = value.byte_size()?;

        let mut cache = self.cache.write().await;
        let mut eviction_strategy = self.eviction_strategy.write().await;

        let mut current_size = self.total_size.load(Ordering::SeqCst);
        let existing_size = cache.get(&key).map(|e| e.size);

        if let Some(size) = existing_size {
            current_size = current_size.saturating_sub(size);
        }

        let eviction_plan =
            eviction_strategy.plan_eviction(current_size, required_space, |key| {
                cache.get(key).is_none_or(|sf| Arc::strong_count(sf) > 1)
            })?;

        let needed = (current_size + required_space).saturating_sub(eviction_strategy.capacity());
        if eviction_plan.freed_bytes < needed {
            return Err(CacheError::NotEnoughSpaceInCache.into());
        }

        if existing_size.is_some() {
            cache.remove(&key);
            eviction_strategy.record_removal(&key);
        }

        for evict_key in eviction_plan.keys_to_remove {
            cache.remove(&evict_key);
            eviction_strategy.record_removal(&evict_key);
        }

        eviction_strategy.record_access(&key, required_space, cache_hint);
        cache.insert(
            key,
            Arc::new(SizeTrackedEntry::new(
                value,
                required_space,
                self.total_size.clone(),
            )),
        );
        Ok(())
    }
}

pub struct RasterCacheOperator<Source>
where
    Source: InitializedRasterOperator,
{
    source: Source,
}

impl<Source> RasterCacheOperator<Source>
where
    Source: InitializedRasterOperator,
{
    pub fn wrap_operator(source: Source) -> RasterCacheOperator<Source> {
        Self { source }
    }
}

struct WorkItem {
    band: Band,
    tile_info: TileInformation,
}

impl<Source> InitializedRasterOperator for RasterCacheOperator<Source>
where
    Source: InitializedRasterOperator,
{
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        self.source.result_descriptor()
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source_qp = self.source.query_processor()?;

        Ok(match source_qp {
            TypedRasterQueryProcessor::U8(s) => TypedRasterQueryProcessor::U8(
                RasterCacheQueryProcessor::boxed_new(s, self.canonic_name()),
            ),
            TypedRasterQueryProcessor::U16(s) => TypedRasterQueryProcessor::U16(
                RasterCacheQueryProcessor::boxed_new(s, self.canonic_name()),
            ),
            TypedRasterQueryProcessor::U32(s) => TypedRasterQueryProcessor::U32(
                RasterCacheQueryProcessor::boxed_new(s, self.canonic_name()),
            ),
            TypedRasterQueryProcessor::U64(s) => TypedRasterQueryProcessor::U64(
                RasterCacheQueryProcessor::boxed_new(s, self.canonic_name()),
            ),
            TypedRasterQueryProcessor::I8(s) => TypedRasterQueryProcessor::I8(
                RasterCacheQueryProcessor::boxed_new(s, self.canonic_name()),
            ),
            TypedRasterQueryProcessor::I16(s) => TypedRasterQueryProcessor::I16(
                RasterCacheQueryProcessor::boxed_new(s, self.canonic_name()),
            ),
            TypedRasterQueryProcessor::I32(s) => TypedRasterQueryProcessor::I32(
                RasterCacheQueryProcessor::boxed_new(s, self.canonic_name()),
            ),
            TypedRasterQueryProcessor::I64(s) => TypedRasterQueryProcessor::I64(
                RasterCacheQueryProcessor::boxed_new(s, self.canonic_name()),
            ),
            TypedRasterQueryProcessor::F32(s) => TypedRasterQueryProcessor::F32(
                RasterCacheQueryProcessor::boxed_new(s, self.canonic_name()),
            ),
            TypedRasterQueryProcessor::F64(s) => TypedRasterQueryProcessor::F64(
                RasterCacheQueryProcessor::boxed_new(s, self.canonic_name()),
            ),
        })
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.source.canonic_name()
    }

    fn name(&self) -> &'static str {
        self.source.name()
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.source.path()
    }

    fn optimize(
        &self,
        resolution: SpatialResolution,
    ) -> Result<Box<dyn RasterOperator>, OptimizationError> {
        self.source.optimize(resolution)
    }
}

struct RasterCacheQueryProcessor<T>
where
    T: Pixel + SupportedRasterDataType,
{
    source: BoxRasterQueryProcessor<T>,
    canonic_operator_name: CanonicOperatorName,
}
impl<T> RasterCacheQueryProcessor<T>
where
    T: Pixel + SupportedRasterDataType,
{
    fn boxed_new(
        source: BoxRasterQueryProcessor<T>,
        canonic_operator_name: CanonicOperatorName,
    ) -> BoxRasterQueryProcessor<T> {
        Box::new(Self {
            source,
            canonic_operator_name,
        })
    }

    fn result_stream<'a>(
        &'a self,
        time_intervals: Pin<Box<dyn Stream<Item = Result<TimeInterval>> + Send + 'a>>,
        work: Vec<WorkItem>,
        ctx: &'a dyn QueryContext,
    ) -> Pin<Box<dyn Stream<Item = Result<RasterTile2D<T>>> + Send + 'a>> {
        let work = Arc::new(work);

        Box::pin(time_intervals.flat_map(
            move |time_interval| -> Pin<
                Box<dyn Stream<Item=Result<RasterTile2D<T>>> + Send + 'a>,
            >   {
                let work = work.clone();

                let time_interval = match time_interval {
                    Ok(time_interval) => time_interval,
                    Err(err) => {
                        let message = err.to_string();

                        return Box::pin(stream::iter(
                            iter::once(0).chain(1..work.len()).map(move |_| {
                                Err(CacheError::TimeQueryFailed { message: message.clone() }.into())
                            })
                        ));
                    }
                };

                Box::pin(stream::unfold((work, 0), move |(work, idx)| async move {
                    if idx >= work.len() {
                        return None;
                    }

                    let job = &work[idx];

                    let key = (
                        self.canonic_operator_name.clone(),
                        job.band,
                        time_interval,
                        job.tile_info.global_upper_left_pixel_idx(),
                    );

                    let cache_store = ctx
                        .new_raster_cache()
                        .expect("Cache should have been created for this operator");

                    match cache_store.get(&key).await {
                        Err(err) => Some((Err(err), (work, idx + 1))),
                        Ok(Some(stored_tile)) => {
                            tracing::debug!("Cache HIT for tile {:?}", key);
                            let tile = stored_tile.load();

                            let tile: Result<RasterTile2D<T>> = match tile {
                                Ok(tile) => T::map_enum_to_tile(tile),
                                Err(err) => Err(err)
                            };

                            Some((
                                tile,
                                (work, idx + 1),
                            ))
                        }
                        Ok(None) => {
                            tracing::debug!("Cache MISS for tile {:?}", key);

                            let source_query = self.source.query(
                                RasterQueryRectangle::new(
                                    job.tile_info.global_pixel_bounds(),
                                    time_interval,
                                    BandSelection::new_single(job.band)
                                ),
                                ctx,
                            ).await;

                            let mut stream = match source_query {
                                Ok(stream) => stream,
                                Err(err) => return Some((Err(err), (work, idx + 1))),
                            };

                            match stream.next().await {
                                Some(Ok(tile)) => {
                                    let tile_to_cache = T::map_tile_to_enum(tile.clone());
                                    let cache_clone = cache_store.clone();
                                    let key_for_cache = key.clone();
                                    tokio::spawn(async move {
                                        if let Err(err) =
                                            cache_clone.insert(key_for_cache, tile_to_cache).await
                                        {
                                            tracing::warn!(
                                                "Failed to populate raster cache in the background: {err}"
                                            );
                                        }
                                    });
                                    Some((Ok(tile), (work, idx + 1)))
                                }
                                Some(Err(err)) => Some((Err(err), (work, idx + 1))),
                                None => Some((
                                    Err(CacheError::SourceProducedNoTile.into()),
                                    (work, idx + 1),
                                )),
                            }
                        }
                    }
                }))
            },
        ))
    }
}
#[async_trait]
impl<T> QueryProcessor for RasterCacheQueryProcessor<T>
where
    T: Pixel + SupportedRasterDataType,
{
    type Output = RasterTile2D<T>;
    type SpatialBounds = GridBoundingBox2D;
    type Selection = BandSelection;
    type ResultDescription = RasterResultDescriptor;

    async fn _query<'a>(
        &'a self,
        query: QueryRectangle<Self::SpatialBounds, Self::Selection>,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let tiling_spec = ctx.tiling_specification();
        let result_descriptor = self.result_descriptor();

        let time_intervals = self.time_query(query.time_interval(), ctx).await?;
        let bands = query.attributes().clone();

        let tile_info_iterator = result_descriptor
            .tiling_grid_definition(tiling_spec)
            .generate_data_tiling_strategy()
            .tile_information_iterator_from_pixel_bounds(query.spatial_bounds());

        let work: Vec<WorkItem> = tile_info_iterator
            .flat_map(|tile_info| {
                bands
                    .as_vec()
                    .into_iter()
                    .map(move |band| WorkItem { band, tile_info })
            })
            .collect();

        let res = self.result_stream(time_intervals, work, ctx);

        Ok(res)
    }

    fn result_descriptor(&self) -> &Self::ResultDescription {
        self.source.result_descriptor()
    }
}
#[async_trait]
impl<T> RasterQueryProcessor for RasterCacheQueryProcessor<T>
where
    T: Pixel + SupportedRasterDataType,
{
    type RasterType = T;

    async fn _time_query<'a>(
        &'a self,
        query: TimeInterval,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<TimeInterval>>> {
        self.source.time_query(query, ctx).await
    }
}

trait SupportedRasterDataType {
    fn map_enum_to_tile(tile: TypedRasterTile2D) -> Result<RasterTile2D<Self>>
    where
        Self: Sized;

    fn map_tile_to_enum(tile: RasterTile2D<Self>) -> TypedRasterTile2D
    where
        Self: Sized;
}

macro_rules! supported_raster_data_type_impl {
    ( $($ty:ty, $variant:ident),* ) => {
        $(
            impl SupportedRasterDataType for $ty {
                fn map_enum_to_tile(tile: TypedRasterTile2D) -> Result<RasterTile2D<Self>> {
                    match tile {
                        TypedRasterTile2D::$variant(tile) => Ok(tile),
                        _ => Err(CacheError::InvalidTypeForRetrieval.into()),
                    }
                }

                fn map_tile_to_enum(tile: RasterTile2D<Self>) -> TypedRasterTile2D {
                    return TypedRasterTile2D::$variant(tile);
                }
            }
        )*
    };
}

supported_raster_data_type_impl!(
    i8, I8, i16, I16, i32, I32, i64, I64, u8, U8, u16, U16, u32, U32, u64, U64, f32, F32, f64, F64
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        ChunkByteSize, MockExecutionContext, RasterBandDescriptors, SpatialGridDescriptor,
        TimeDescriptor,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use geoengine_datatypes::primitives::{Coordinate2D, TimeInstance};
    use geoengine_datatypes::raster::{
        BoundedGrid, GeoTransform, Grid2D, GridIdx2D, GridOrEmpty2D, GridShape2D, MaskedGrid2D,
        RasterDataType, TilingSpecification,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use std::str::FromStr;
    use tokio::time::sleep;

    fn make_cache_key(band: u32, idx: isize) -> CacheKey {
        (
            CanonicOperatorName::new(&"test_op").unwrap(),
            band,
            TimeInterval::new_unchecked(
                TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
                TimeInstance::from_str("2014-01-02T00:00:00.000Z").unwrap(),
            ),
            GridIdx2D::new([idx, 0]),
        )
    }

    #[test]
    fn test_cache_enum_variants() {
        let _fifo = NewRasterCacheEnum::new_fifo(1_000_000);
        let _lru = NewRasterCacheEnum::new_lru(1_000_000);
    }

    #[tokio::test]
    async fn test_cache_miss_returns_none() {
        let cache = NewRasterCacheEnum::new_lru(1_000_000);
        let key = make_cache_key(0, 0);
        let retrieved = cache.get(&key).await.unwrap();
        assert!(retrieved.is_none(), "Non-existent key should return None");
    }

    #[tokio::test]
    async fn test_concurrent_cache_operations() {
        let cache = Arc::new(NewRasterCacheEnum::new_lru(10_000_000));
        let mut handles = Vec::new();

        for i in 0..10 {
            let cache_clone = cache.clone();
            let handle = tokio::spawn(async move {
                let key = make_cache_key(0, i as isize);
                let result = cache_clone.get(&key).await.unwrap();
                assert!(result.is_none(), "Concurrent access should work safely");
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }

    fn make_tile(fill_value: u8, cache_hint: CacheHint) -> TypedRasterTile2D {
        let grid: GridOrEmpty2D<u8> = Grid2D::new_filled([2, 2].into(), fill_value).into();
        TypedRasterTile2D::U8(RasterTile2D::new(
            TimeInterval::default(),
            GridIdx2D::new([0, 0]),
            0,
            GeoTransform::test_default(),
            grid,
            cache_hint,
        ))
    }

    async fn tile_byte_size(tile: &TypedRasterTile2D) -> usize {
        let probe = NewRasterCacheEnum::new_fifo(usize::MAX / 2);
        probe
            .insert(make_cache_key(0, 0), tile.clone())
            .await
            .unwrap();
        probe.byte_size()
    }

    async fn assert_insert_and_get_roundtrip(make_cache: impl Fn(usize) -> NewRasterCacheEnum) {
        let tile = make_tile(42, CacheHint::max_duration());
        let size = tile_byte_size(&tile).await;
        let cache = make_cache(size);
        let key = make_cache_key(0, 0);

        cache.insert(key.clone(), tile.clone()).await.unwrap();

        let entry = cache
            .get(&key)
            .await
            .unwrap()
            .expect("tile should be cached");
        let loaded = entry.load().unwrap();

        match (loaded, tile) {
            (TypedRasterTile2D::U8(loaded), TypedRasterTile2D::U8(original)) => {
                assert_eq!(loaded, original);
            }
            _ => panic!("unexpected tile type"),
        }
        assert_eq!(cache.byte_size(), size);
    }

    #[tokio::test]
    async fn test_insert_and_get_roundtrip_fifo() {
        assert_insert_and_get_roundtrip(NewRasterCacheEnum::new_fifo).await;
    }

    #[tokio::test]
    async fn test_insert_and_get_roundtrip_lru() {
        assert_insert_and_get_roundtrip(NewRasterCacheEnum::new_lru).await;
    }

    #[tokio::test]
    async fn test_fifo_evicts_oldest_insertion_ignoring_recent_access() {
        let tile_a = make_tile(1, CacheHint::max_duration());
        let tile_b = make_tile(2, CacheHint::max_duration());
        let tile_c = make_tile(3, CacheHint::max_duration());

        let size_a = tile_byte_size(&tile_a).await;
        let size_b = tile_byte_size(&tile_b).await;

        let key_a = make_cache_key(0, 0);
        let key_b = make_cache_key(0, 1);
        let key_c = make_cache_key(0, 2);

        let cache = NewRasterCacheEnum::new_fifo(size_a + size_b);

        cache.insert(key_a.clone(), tile_a).await.unwrap();
        cache.insert(key_b.clone(), tile_b).await.unwrap();

        assert!(cache.get(&key_a).await.unwrap().is_some());

        cache.insert(key_c.clone(), tile_c).await.unwrap();

        assert!(
            cache.get(&key_a).await.unwrap().is_none(),
            "FIFO must evict the oldest-inserted entry even though it was just accessed"
        );
        assert!(cache.get(&key_c).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_lru_evicts_least_recently_used_not_oldest() {
        let tile_a = make_tile(1, CacheHint::max_duration());
        let tile_b = make_tile(2, CacheHint::max_duration());
        let tile_c = make_tile(3, CacheHint::max_duration());

        let size_a = tile_byte_size(&tile_a).await;
        let size_b = tile_byte_size(&tile_b).await;

        let key_a = make_cache_key(0, 0);
        let key_b = make_cache_key(0, 1);
        let key_c = make_cache_key(0, 2);

        let cache = NewRasterCacheEnum::new_lru(size_a + size_b);

        cache.insert(key_a.clone(), tile_a).await.unwrap();
        cache.insert(key_b.clone(), tile_b).await.unwrap();

        assert!(cache.get(&key_a).await.unwrap().is_some());

        cache.insert(key_c.clone(), tile_c).await.unwrap();

        assert!(
            cache.get(&key_b).await.unwrap().is_none(),
            "LRU must evict the least recently used entry, not the oldest-inserted one"
        );
        assert!(cache.get(&key_a).await.unwrap().is_some());
        assert!(cache.get(&key_c).await.unwrap().is_some());
    }

    async fn assert_pinned_entry_blocks_eviction_until_dropped(
        make_cache: impl Fn(usize) -> NewRasterCacheEnum,
    ) {
        let tile_a = make_tile(1, CacheHint::max_duration());
        let tile_b = make_tile(2, CacheHint::max_duration());

        let size_a = tile_byte_size(&tile_a).await;
        let size_b = tile_byte_size(&tile_b).await;

        let key_a = make_cache_key(0, 0);
        let key_b = make_cache_key(0, 1);

        let cache = make_cache(size_a.max(size_b));

        cache.insert(key_a.clone(), tile_a).await.unwrap();

        let pinned = cache
            .get(&key_a)
            .await
            .unwrap()
            .expect("A should be cached");
        assert!(
            Arc::strong_count(&pinned) > 1,
            "the cache and the held reference should both own an Arc to the entry"
        );

        assert!(
            cache.insert(key_b.clone(), tile_b.clone()).await.is_err(),
            "insert must fail while the only evictable entry is pinned"
        );
        assert!(
            cache.get(&key_a).await.unwrap().is_some(),
            "a failed insert must not disturb the still-pinned entry"
        );

        drop(pinned);

        cache.insert(key_b.clone(), tile_b).await.unwrap();

        assert!(
            cache.get(&key_a).await.unwrap().is_none(),
            "A should now have been evicted to make room for B"
        );
        assert!(cache.get(&key_b).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_pinned_entry_blocks_eviction_until_dropped_fifo() {
        assert_pinned_entry_blocks_eviction_until_dropped(NewRasterCacheEnum::new_fifo).await;
    }

    #[tokio::test]
    async fn test_pinned_entry_blocks_eviction_until_dropped_lru() {
        assert_pinned_entry_blocks_eviction_until_dropped(NewRasterCacheEnum::new_lru).await;
    }

    async fn assert_repeated_insert_of_same_key_does_not_corrupt_eviction_state(
        make_cache: impl Fn(usize) -> NewRasterCacheEnum,
    ) {
        let tile_a = make_tile(1, CacheHint::max_duration());
        let tile_b = make_tile(2, CacheHint::max_duration());

        let size_a = tile_byte_size(&tile_a).await;
        let size_b = tile_byte_size(&tile_b).await;

        let key_a = make_cache_key(0, 0);
        let key_b = make_cache_key(0, 1);

        let cache = make_cache(size_a.max(size_b));

        cache.insert(key_a.clone(), tile_a.clone()).await.unwrap();
        cache.insert(key_a.clone(), tile_a.clone()).await.unwrap();

        assert_eq!(
            cache.byte_size(),
            size_a,
            "re-inserting the same key must not double-count its bytes"
        );

        cache.insert(key_b.clone(), tile_b).await.unwrap();

        assert!(cache.get(&key_a).await.unwrap().is_none());
        assert!(cache.get(&key_b).await.unwrap().is_some());
        assert_eq!(cache.byte_size(), size_b);
    }

    #[tokio::test]
    async fn test_repeated_insert_of_same_key_does_not_corrupt_eviction_state_fifo() {
        assert_repeated_insert_of_same_key_does_not_corrupt_eviction_state(
            NewRasterCacheEnum::new_fifo,
        )
        .await;
    }

    #[tokio::test]
    async fn test_repeated_insert_of_same_key_does_not_corrupt_eviction_state_lru() {
        assert_repeated_insert_of_same_key_does_not_corrupt_eviction_state(
            NewRasterCacheEnum::new_lru,
        )
        .await;
    }

    #[tokio::test]
    async fn test_expired_entry_lazy_evicted_on_get() {
        let tile_a = make_tile(1, CacheHint::seconds(0));
        let tile_b = make_tile(2, CacheHint::max_duration());

        let size_a = tile_byte_size(&tile_a).await;
        let size_b = tile_byte_size(&tile_b).await;

        let key_a = make_cache_key(0, 0);
        let key_b = make_cache_key(0, 1);

        let cache = NewRasterCacheEnum::new_lru(size_a + size_b);

        cache.insert(key_a.clone(), tile_a).await.unwrap();
        assert_eq!(
            cache.byte_size(),
            size_a,
            "tile_a should be cached initially"
        );

        cache.insert(key_b.clone(), tile_b).await.unwrap();
        assert_eq!(
            cache.byte_size(),
            size_a + size_b,
            "both tiles should be cached"
        );

        let result = cache.get(&key_a).await.unwrap();
        assert!(result.is_none(), "expired entries are never served");

        assert_eq!(
            cache.byte_size(),
            size_b,
            "expired entry bytes should be freed"
        );

        assert!(cache.get(&key_a).await.unwrap().is_none());
        assert_eq!(cache.byte_size(), size_b);
    }

    #[tokio::test]
    async fn test_failed_insert_with_existing_key_does_not_corrupt_eviction_state() {
        let tile_a = make_tile(1, CacheHint::max_duration());
        let tile_b = make_tile(2, CacheHint::max_duration());
        let tile_b2 = make_tile(2, CacheHint::max_duration());

        let size_a = tile_byte_size(&tile_a).await;
        let size_b = tile_byte_size(&tile_b).await;

        let key_a = make_cache_key(0, 0);
        let key_b = make_cache_key(0, 1);

        let cache = NewRasterCacheEnum::new_lru(size_a.max(size_b));

        cache.insert(key_a.clone(), tile_a).await.unwrap();

        let pinned = cache.get(&key_a).await.unwrap().unwrap();
        assert!(Arc::strong_count(&pinned) > 1);

        let cache_size_before = cache.byte_size();
        let result = cache.insert(key_b.clone(), tile_b).await;
        assert!(
            result.is_err(),
            "insert should fail when only pinned entries can be evicted"
        );

        assert!(
            cache.get(&key_a).await.unwrap().is_some(),
            "failed insert must not evict the pinned entry"
        );

        assert_eq!(
            cache.byte_size(),
            cache_size_before,
            "cache size should be unchanged after failed insert"
        );

        drop(pinned);

        cache.insert(key_b.clone(), tile_b2).await.unwrap();
        assert!(cache.get(&key_b).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_partial_cache_eviction_deficit_calculation() {
        let tile_a = make_tile(1, CacheHint::max_duration());
        let tile_b = make_tile(2, CacheHint::max_duration());
        let tile_c = make_tile(3, CacheHint::max_duration());

        let size_a = tile_byte_size(&tile_a).await;
        let size_b = tile_byte_size(&tile_b).await;
        let size_c = tile_byte_size(&tile_c).await;

        let key_a = make_cache_key(0, 0);
        let key_b = make_cache_key(0, 1);
        let key_c = make_cache_key(0, 2);

        let capacity = size_a + size_b + size_c;
        let cache = NewRasterCacheEnum::new_lru(capacity);

        cache.insert(key_a.clone(), tile_a).await.unwrap();
        assert_eq!(cache.byte_size(), size_a);

        cache.insert(key_b.clone(), tile_b).await.unwrap();
        assert_eq!(cache.byte_size(), size_a + size_b);

        cache.insert(key_c.clone(), tile_c).await.unwrap();
        assert_eq!(cache.byte_size(), size_a + size_b + size_c);

        assert!(
            cache.get(&key_a).await.unwrap().is_some(),
            "A should still be in cache after C was added (all fit)"
        );
        assert!(
            cache.get(&key_b).await.unwrap().is_some(),
            "B should still be in cache after C was added (all fit)"
        );
        assert!(
            cache.get(&key_c).await.unwrap().is_some(),
            "C should be in cache"
        );
    }

    fn mock_source_with_fill_value(
        fill_value: u8,
    ) -> (
        Box<dyn RasterOperator>,
        RasterTile2D<u8>,
        GridBoundingBox2D,
        TilingSpecification,
    ) {
        let tile_size_in_pixels = GridShape2D::new_2d(2, 2);
        let tiling_specification = TilingSpecification::new(tile_size_in_pixels);
        let geo_transform = GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.);

        let raster: MaskedGrid2D<u8> = Grid2D::new_filled(tile_size_in_pixels, fill_value).into();

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: geo_transform,
                global_tile_position: GridIdx2D::new([0, 0]),
                tile_size_in_pixels,
            },
            0,
            raster.into(),
            CacheHint::max_duration(),
        );

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_irregular(Some(TimeInterval::default())),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                geo_transform,
                tile_size_in_pixels.bounding_box(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let source = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile.clone()],
                result_descriptor,
            },
        }
        .boxed();

        (
            source,
            raster_tile,
            tile_size_in_pixels.bounding_box(),
            tiling_specification,
        )
    }

    #[tokio::test]
    async fn test_raster_cache_operator_populates_cache_with_correct_content_on_miss() {
        let (source, expected_tile, query_bounds, tiling_specification) =
            mock_source_with_fill_value(7);

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);
        let initialized_source = source
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();
        let cached_op = RasterCacheOperator::wrap_operator(initialized_source);
        let cache_key = (
            cached_op.canonic_name(),
            0u32,
            TimeInterval::default(),
            GridIdx2D::new([0, 0]),
        );

        let query_ctx = exe_ctx.mock_query_context_with_query_extensions(
            ChunkByteSize::test_default(),
            None,
            None,
            None,
        );

        let processor = cached_op.query_processor().unwrap().get_u8().unwrap();
        let query_rectangle = RasterQueryRectangle::new(
            query_bounds,
            TimeInterval::default(),
            BandSelection::new_single(0),
        );

        let tiles = processor
            .query(query_rectangle, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<crate::util::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(tiles, vec![expected_tile.clone()]);

        // Wait a bit for tile insert
        sleep(tokio::time::Duration::from_secs(1)).await;

        // Verify the tile is actually in the cache after the miss
        let tile_cache = query_ctx
            .new_raster_cache()
            .expect("cache should be available in query context");
        let cached_compressed = tile_cache
            .get(&cache_key)
            .await
            .expect("cache lookup should succeed")
            .expect("tile should be cached after query");

        let cached_tile_typed = cached_compressed.value.load()
            .expect("decompression should succeed");
        match cached_tile_typed {
            TypedRasterTile2D::U8(cached_u8_tile) => {
                assert_eq!(cached_u8_tile, expected_tile, "cached tile must match source tile");
            }
            _ => panic!("expected U8 tile type in cache"),
        }
    }

    #[tokio::test]
    async fn test_raster_cache_operator_hit_is_served_from_cache_and_bypasses_source() {
        let (source, source_tile, query_bounds, tiling_specification) =
            mock_source_with_fill_value(7);

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);
        let initialized_source = source
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();
        let cached_op = RasterCacheOperator::wrap_operator(initialized_source);
        let key = (
            cached_op.canonic_name(),
            0u32,
            TimeInterval::default(),
            GridIdx2D::new([0, 0]),
        );

        let poisoned_tile = make_tile(99, CacheHint::max_duration());

        let query_ctx = exe_ctx.mock_query_context_with_query_extensions(
            ChunkByteSize::test_default(),
            None,
            None,
            None,
        );

        let tile_cache = query_ctx
            .new_raster_cache()
            .expect("cache should be available in query context");
        tile_cache.insert(key, poisoned_tile.clone()).await.unwrap();

        let processor = cached_op.query_processor().unwrap().get_u8().unwrap();
        let query_rectangle = RasterQueryRectangle::new(
            query_bounds,
            TimeInterval::default(),
            BandSelection::new_single(0),
        );

        let tiles = processor
            .query(query_rectangle, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<crate::util::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(tiles.len(), 1);
        assert_ne!(
            tiles[0], source_tile,
            "a cache hit must not fall back to recomputing from the (correct) source"
        );
        match poisoned_tile {
            TypedRasterTile2D::U8(poisoned) => {
                assert_eq!(
                    tiles[0], poisoned,
                    "a cache hit must serve exactly the cached entry"
                );
            }
            _ => panic!("unexpected tile type"),
        }
    }
}
