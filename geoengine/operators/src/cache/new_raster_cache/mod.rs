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
    BandSelection, QueryRectangle, RasterQueryRectangle, SpatialResolution, TimeInterval,
};
use geoengine_datatypes::raster::{
    GridBoundingBox2D, GridIdx2D, Pixel, RasterTile2D, TileInformation,
};
use geoengine_datatypes::util::ByteSize;
use geoengine_datatypes::util::test::TestDefault;
use itertools::FoldWhile::{Continue, Done};
use itertools::Itertools;
use std::any::Any;
use std::collections::HashMap;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::iter;
use std::marker::Sync;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct NewRasterCache<Store: CacheStore> {
    store: Store,
}

pub enum NewRasterCacheEnum {
    InMemoryCompressedRasterTile2DFifo(
        NewRasterCache<InMemoryCacheStore<TypedCompressedRasterTile2D, FifoEvictionStrategy>>,
    ),
}

impl TestDefault for NewRasterCacheEnum {
    fn test_default() -> Self {
        Self::InMemoryCompressedRasterTile2DFifo(NewRasterCache {
            store: InMemoryCacheStore {
                cache: RwLock::new(HashMap::new()),
                eviction_strategy: RwLock::new(FifoEvictionStrategy::new(8_589_934_592)),
            },
        })
    }
}

impl NewRasterCacheEnum {
    async fn get(&self, key: &CacheKey) -> Result<Arc<impl StorageFormat>> {
        match self {
            NewRasterCacheEnum::InMemoryCompressedRasterTile2DFifo(cache) => {
                cache.store.get(key).await
            }
        }
    }

    async fn insert(&self, key: CacheKey, tile: TypedRasterTile2D) -> Result<()> {
        match self {
            NewRasterCacheEnum::InMemoryCompressedRasterTile2DFifo(cache) => {
                cache.store.insert(key, tile).await
            }
        }
    }
}

#[async_trait]
trait CacheStore: Send + Sync + 'static {
    type SF: StorageFormat;
    type ES: EvictionStrategy;

    async fn get(&self, key: &CacheKey) -> Result<Arc<Self::SF>>;

    async fn insert(&self, key: CacheKey, tile: TypedRasterTile2D) -> Result<()>;
}

trait EvictionStrategy: Send + Sync + 'static {
    fn record_access(&mut self, key: &CacheKey, weight: f64, size: usize);
    fn record_hit(&mut self, key: &CacheKey);
    fn record_removal(&mut self, key: &CacheKey);

    fn plan_eviction<F>(
        &self,
        required_space: usize,
        max_weight: f64,
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
    size: usize,
    capacity: usize,
}

struct EvictionStrategyItem {
    key: CacheKey,
    weight: f64,
    size: usize,
}

impl FifoEvictionStrategy {
    fn new(capacity: usize) -> Self {
        FifoEvictionStrategy {
            queue: Vec::new(),
            size: 0,
            capacity,
        }
    }
}

impl EvictionStrategy for FifoEvictionStrategy {
    fn record_access(&mut self, key: &CacheKey, weight: f64, size: usize) {
        self.queue.push(EvictionStrategyItem {
            key: key.clone(),
            weight,
            size,
        });

        self.size += size;
    }

    fn record_hit(&mut self, key: &CacheKey) {
        // Nothing to do here
    }

    fn record_removal(&mut self, key: &CacheKey) {
        let item = self.queue.remove(
            self.queue
                .iter()
                .position(|item| item.key.eq(key))
                .expect("Key must exist in eviction strategy"),
        );

        self.size -= item.size;
    }

    fn plan_eviction<F>(
        &self,
        required_space: usize,
        _max_weight: f64,
        mut is_pinned: F,
    ) -> Result<EvictionPlan>
    where
        F: FnMut(&CacheKey) -> bool,
    {
        if self.size + required_space <= self.capacity {
            return Ok(EvictionPlan {
                keys_to_remove: vec![],
                freed_bytes: required_space,
            });
        }

        Ok(self
            .queue
            .iter()
            .filter(|item| !is_pinned(&item.key))
            .fold_while(
                EvictionPlan {
                    keys_to_remove: vec![],
                    freed_bytes: 0,
                },
                |mut plan, item| {
                    plan.keys_to_remove.push(item.key.clone());
                    plan.freed_bytes += item.size;

                    if plan.freed_bytes >= required_space {
                        Done(plan)
                    } else {
                        Continue(plan)
                    }
                },
            )
            .into_inner())
    }
}

pub struct MockOnDiskCacheStore<SF: StorageFormat, ES: EvictionStrategy> {
    cache: RwLock<HashMap<CacheKey, Arc<SF>>>,
    eviction_strategy: ES,
}

pub struct MockOnDiskStorageFormat {
    tile: TypedRasterTile2D,
}

impl StorageFormat for MockOnDiskStorageFormat {
    fn store(tile: TypedRasterTile2D) -> Result<Self> {
        let mut file = File::create("foo.txt")?;
        file.write_all(&[67 as u8])?;
        Ok(Self { tile })
    }

    fn load(&self) -> Result<TypedRasterTile2D> {
        let mut file = File::open("foo.txt")?;
        let mut buf = vec![];
        file.read_to_end(&mut buf)?;
        Ok(self.tile.clone())
    }
    fn byte_size(&self) -> Result<usize> {
        let mut file = File::open("foo.txt")?;
        let mut buf = vec![];
        file.read_to_end(&mut buf)
            .map_err(|_| crate::error::Error::Cache {
                source: CacheError::Unspecified,
            })
    }
}

#[async_trait]
impl<SF: StorageFormat, ES: EvictionStrategy> CacheStore for MockOnDiskCacheStore<SF, ES> {
    type SF = SF;
    type ES = ES;

    async fn get(&self, key: &CacheKey) -> Result<Arc<Self::SF>> {
        self.cache
            .read()
            .await
            .get(key)
            .map(Arc::clone)
            .ok_or(crate::error::Error::Cache {
                source: CacheError::Unspecified,
            })
    }

    async fn insert(&self, key: CacheKey, tile: TypedRasterTile2D) -> Result<()> {
        self.cache
            .write()
            .await
            .insert(key, Arc::new(SF::store(tile)?));
        Ok(())
    }
}

pub struct InMemoryCacheStore<SF: StorageFormat, ES: EvictionStrategy> {
    cache: RwLock<HashMap<CacheKey, Arc<SF>>>,
    eviction_strategy: RwLock<ES>,
}

type CacheKey = (CanonicOperatorName, Band, TimeInterval, TileIndex);
type Band = u32;
type TileIndex = GridIdx2D;

trait StorageFormat: Send + Sync + Sized + 'static {
    fn store(tile: TypedRasterTile2D) -> Result<Self>;

    fn load(&self) -> Result<TypedRasterTile2D>;

    fn byte_size(&self) -> Result<usize>;
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
                CompressedRasterTile2D::decompress_tile(compressed)
                    .map_err(|err| CacheError::Unspecified)?,
            )),
            TypedCompressedRasterTile2D::I16(compressed) => Ok(TypedRasterTile2D::I16(
                CompressedRasterTile2D::decompress_tile(compressed)
                    .map_err(|err| CacheError::Unspecified)?,
            )),
            TypedCompressedRasterTile2D::I32(compressed) => Ok(TypedRasterTile2D::I32(
                CompressedRasterTile2D::decompress_tile(compressed)
                    .map_err(|err| CacheError::Unspecified)?,
            )),
            TypedCompressedRasterTile2D::I64(compressed) => Ok(TypedRasterTile2D::I64(
                CompressedRasterTile2D::decompress_tile(compressed)
                    .map_err(|err| CacheError::Unspecified)?,
            )),
            TypedCompressedRasterTile2D::U8(compressed) => Ok(TypedRasterTile2D::U8(
                CompressedRasterTile2D::decompress_tile(compressed)
                    .map_err(|err| CacheError::Unspecified)?,
            )),
            TypedCompressedRasterTile2D::U16(compressed) => Ok(TypedRasterTile2D::U16(
                CompressedRasterTile2D::decompress_tile(compressed)
                    .map_err(|err| CacheError::Unspecified)?,
            )),
            TypedCompressedRasterTile2D::U32(compressed) => Ok(TypedRasterTile2D::U32(
                CompressedRasterTile2D::decompress_tile(compressed)
                    .map_err(|err| CacheError::Unspecified)?,
            )),
            TypedCompressedRasterTile2D::U64(compressed) => Ok(TypedRasterTile2D::U64(
                CompressedRasterTile2D::decompress_tile(compressed)
                    .map_err(|err| CacheError::Unspecified)?,
            )),
            TypedCompressedRasterTile2D::F32(compressed) => Ok(TypedRasterTile2D::F32(
                CompressedRasterTile2D::decompress_tile(compressed)
                    .map_err(|err| CacheError::Unspecified)?,
            )),
            TypedCompressedRasterTile2D::F64(compressed) => Ok(TypedRasterTile2D::F64(
                CompressedRasterTile2D::decompress_tile(compressed)
                    .map_err(|err| CacheError::Unspecified)?,
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
}

#[async_trait]
impl<SF, ES> CacheStore for InMemoryCacheStore<SF, ES>
where
    SF: StorageFormat,
    ES: EvictionStrategy,
{
    type SF = SF;
    type ES = ES;

    async fn get(&self, key: &CacheKey) -> Result<Arc<Self::SF>> {
        self.cache
            .read()
            .await
            .get(key)
            .map(Arc::clone)
            .ok_or(crate::error::Error::Cache {
                source: CacheError::Unspecified,
            })
    }

    async fn insert(&self, key: CacheKey, tile: TypedRasterTile2D) -> Result<()> {
        let stored_tile = Arc::new(SF::store(tile)?);
        let required_space = stored_tile.byte_size()?;

        let mut cache = self.cache.write().await;
        let mut eviction_strategy = self.eviction_strategy.write().await;
        let eviction_plan = eviction_strategy.plan_eviction(required_space, f64::MAX, |key| {
            cache.get(key).map_or(true, |sf| Arc::strong_count(sf) > 1)
        })?;

        if eviction_plan.freed_bytes < required_space {
            return Err(crate::error::Error::Cache {
                source: CacheError::Unspecified,
            });
        }

        for key in eviction_plan.keys_to_remove {
            cache.remove(&key);
            eviction_strategy.record_removal(&key);
        }

        cache.insert(key, stored_tile);
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
        let source_qp = self
            .source
            .query_processor()
            .map_err(|err| CacheError::Unspecified)?;

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

                if let Err(_) = time_interval {
                    return Box::pin(stream::iter(
                        iter::once(0).chain(1..work.len()).map(|_| Err(crate::error::Error::Cache { source: CacheError::Unspecified }))
                    ));
                }
                let time_interval = time_interval.unwrap();
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

                    /*let mut s = DefaultHasher::new();
                    key.0.hash(&mut s);
                    key.1.hash(&mut s);
                    key.2.hash(&mut s);
                    key.3.hash(&mut s);
                    let hash_key = s.finish();


                    println!("Cache query for key {hash_key:?}!");*/

                    if let Ok(stored_tile) = cache_store.get(&key).await
                    {
                        //println!("Cache hit for key {hash_key:?}!");
                        let tile = stored_tile.load();

                        let tile: Result<RasterTile2D<T>> = match tile {
                            Ok(tile) => T::map_enum_to_tile(tile),
                            Err(err) => Err(err)
                        };

                        Some((
                            tile,
                            (work, idx + 1),
                        ))
                    } else {
                        //println!("Cache miss for key {hash_key:?}!");

                        let source_query = self.source.query(
                            RasterQueryRectangle::new(job.tile_info.global_pixel_bounds(), time_interval, BandSelection::new_single(job.band)),
                            ctx,
                        ).await;


                        if let Ok(mut stream) = source_query {
                            if let Some(Ok(tile)) = stream.next().await {
                                // TODO tokio::spawn
                                let _ = cache_store.insert(key, T::map_tile_to_enum(tile.clone())).await;

                                return Some((Ok(tile), (work, idx + 1)));
                            }
                        }

                        Some((
                            Err(crate::error::Error::Cache { source: CacheError::Unspecified }),
                            (work, idx + 1),
                        ))
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
        self.source._time_query(query, ctx).await
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
                        _ => Err(crate::error::Error::Cache {
                            source: CacheError::Unspecified,
                        }),
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

mod test {
    use crate::cache::new_raster_cache::{NewRasterCacheEnum, RasterCacheOperator};
    use crate::engine::{
        BoxRasterQueryProcessor, ChunkByteSize, InitializedRasterOperator, MockExecutionContext,
        MockQueryContext, RasterOperator, SingleRasterSource, WorkflowOperatorPath,
    };
    use crate::processing::{
        AggregateFunctionParams, NeighborhoodAggregate, NeighborhoodAggregateParams,
        NeighborhoodParams,
    };
    use crate::source::{GdalSource, GdalSourceParameters};
    use crate::util::gdal::add_ndvi_dataset;
    use futures::StreamExt;
    use geoengine_datatypes::primitives::{
        BandSelection, RasterQueryRectangle, TimeInstance, TimeInterval,
    };
    use geoengine_datatypes::raster::GridBoundingBox2D;
    use geoengine_datatypes::util::test::TestDefault;
    use std::hint::black_box;
    use std::str::FromStr;
    use std::sync::Arc;

    #[tokio::test]
    async fn test() {
        let (proc, exe) = setup(true).await;

        query(proc, &exe).await;
    }

    async fn setup(cache: bool) -> (Arc<BoxRasterQueryProcessor<u8>>, MockQueryContext) {
        let mut exe_ctx = MockExecutionContext::test_default();

        let ndvi_id = add_ndvi_dataset(&mut exe_ctx);

        let operator = NeighborhoodAggregate {
            params: NeighborhoodAggregateParams {
                neighborhood: NeighborhoodParams::WeightsMatrix {
                    weights: vec![vec![1., 2., 3.], vec![4., 5., 6.], vec![7., 8., 9.]],
                },
                aggregate_function: AggregateFunctionParams::Sum,
            },
            sources: SingleRasterSource {
                raster: GdalSource {
                    params: GdalSourceParameters::new(ndvi_id),
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap();

        if cache {
            let cached_op = RasterCacheOperator::wrap_operator(operator);

            let tile_cache = Arc::new(NewRasterCacheEnum::test_default());

            let query_ctx = exe_ctx.mock_query_context_with_query_extensions(
                ChunkByteSize::test_default(),
                None,
                Some(tile_cache),
                None,
                None,
            );

            let processor = cached_op.query_processor().unwrap().get_u8().unwrap();

            let processor = Arc::new(processor);

            query(processor.clone(), &query_ctx).await;

            return (processor, query_ctx);
        }

        let processor = operator.query_processor().unwrap().get_u8().unwrap();

        let query_ctx = exe_ctx.mock_query_context_with_query_extensions(
            ChunkByteSize::test_default(),
            None,
            None,
            None,
            None,
        );
        (Arc::new(processor), query_ctx)
    }

    async fn query(processor: Arc<BoxRasterQueryProcessor<u8>>, query_ctx: &MockQueryContext) {
        let stream = processor
            .query(
                RasterQueryRectangle::new(
                    GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap(),
                    //TimeInterval::default(),
                    TimeInterval::new_unchecked(
                        TimeInstance::from_str("2014-01-01T00:00:00.000Z")
                            .expect("it should only be used in tests"),
                        TimeInstance::from_str("2015-01-01T00:00:00.000Z")
                            .expect("it should only be used in tests"),
                    ),
                    BandSelection::first(),
                ),
                query_ctx,
            )
            .await
            .unwrap();

        let tiles = stream.collect::<Vec<_>>().await;

        let tiles = tiles
            .into_iter()
            .collect::<crate::util::Result<Vec<_>>>()
            .unwrap();

        black_box(tiles);
    }
}
