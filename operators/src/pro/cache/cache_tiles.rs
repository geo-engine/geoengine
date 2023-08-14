use super::error::CacheError;
use super::shared_cache::{
    CacheBackendElement, CacheBackendElementExt, CacheElement, CacheElementsContainer,
    CacheElementsContainerInfos, LandingZoneElementsContainer, RasterCacheQueryEntry,
    RasterLandingQueryEntry,
};
use crate::util::Result;
use futures::stream::FusedStream;
use futures::{Future, Stream};
use geoengine_datatypes::primitives::SpatialPartitioned;
use geoengine_datatypes::raster::{
    BaseTile, EmptyGrid, Grid, GridOrEmpty, GridShape2D, GridSize, GridSpaceToLinearSpace,
    MaskedGrid, RasterTile,
};
use geoengine_datatypes::{
    primitives::RasterQueryRectangle,
    raster::{Pixel, RasterTile2D},
    util::ByteSize,
};
use pin_project::pin_project;
use std::marker::PhantomData;
use std::{pin::Pin, sync::Arc};

#[derive(Debug)]
pub enum CachedTiles {
    U8(Arc<Vec<CompressedRasterTile2D<u8>>>),
    U16(Arc<Vec<CompressedRasterTile2D<u16>>>),
    U32(Arc<Vec<CompressedRasterTile2D<u32>>>),
    U64(Arc<Vec<CompressedRasterTile2D<u64>>>),
    I8(Arc<Vec<CompressedRasterTile2D<i8>>>),
    I16(Arc<Vec<CompressedRasterTile2D<i16>>>),
    I32(Arc<Vec<CompressedRasterTile2D<i32>>>),
    I64(Arc<Vec<CompressedRasterTile2D<i64>>>),
    F32(Arc<Vec<CompressedRasterTile2D<f32>>>),
    F64(Arc<Vec<CompressedRasterTile2D<f64>>>),
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
    U8(Vec<CompressedRasterTile2D<u8>>),
    U16(Vec<CompressedRasterTile2D<u16>>),
    U32(Vec<CompressedRasterTile2D<u32>>),
    U64(Vec<CompressedRasterTile2D<u64>>),
    I8(Vec<CompressedRasterTile2D<i8>>),
    I16(Vec<CompressedRasterTile2D<i16>>),
    I32(Vec<CompressedRasterTile2D<i32>>),
    I64(Vec<CompressedRasterTile2D<i64>>),
    F32(Vec<CompressedRasterTile2D<f32>>),
    F64(Vec<CompressedRasterTile2D<f64>>),
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

impl CacheElementsContainerInfos<RasterQueryRectangle> for CachedTiles {
    fn is_expired(&self) -> bool {
        self.is_expired()
    }
}

impl<T> CacheElementsContainer<RasterQueryRectangle, CompressedRasterTile2D<T>> for CachedTiles
where
    T: Pixel,
    CompressedRasterTile2D<T>: CacheBackendElementExt<CacheContainer = CachedTiles>,
{
    fn results_arc(&self) -> Option<Arc<Vec<CompressedRasterTile2D<T>>>> {
        CompressedRasterTile2D::<T>::results_arc(self)
    }
}

impl<T> LandingZoneElementsContainer<CompressedRasterTile2D<T>> for LandingZoneQueryTiles
where
    T: Pixel,
    CompressedRasterTile2D<T>: CacheBackendElementExt<LandingZoneContainer = LandingZoneQueryTiles>,
{
    fn insert_element(
        &mut self,
        element: CompressedRasterTile2D<T>,
    ) -> Result<(), super::error::CacheError> {
        CompressedRasterTile2D::<T>::move_element_into_landing_zone(element, self)
    }

    fn create_empty() -> Self {
        CompressedRasterTile2D::<T>::create_empty_landing_zone()
    }
}

impl<T> CacheBackendElement for CompressedRasterTile2D<T>
where
    T: Pixel,
{
    type Query = RasterQueryRectangle;

    fn cache_hint(&self) -> geoengine_datatypes::primitives::CacheHint {
        self.cache_hint
    }

    fn typed_canonical_operator_name(
        key: crate::engine::CanonicOperatorName,
    ) -> super::shared_cache::TypedCanonicOperatorName {
        super::shared_cache::TypedCanonicOperatorName::Raster(key)
    }

    fn update_stored_query(&self, query: &mut Self::Query) -> Result<(), CacheError> {
        query.spatial_bounds.extend(&self.spatial_partition());
        query.time_interval = query
            .time_interval
            .union(&self.time)
            .map_err(|_| CacheError::ElementAndQueryDoNotIntersect)?;
        Ok(())
    }
}

macro_rules! impl_cache_element_subtype {
    ($t:ty, $variant:ident) => {
        impl CacheBackendElementExt for CompressedRasterTile2D<$t> {
            type LandingZoneContainer = LandingZoneQueryTiles;
            type CacheContainer = CachedTiles;

            fn move_element_into_landing_zone(
                self,
                landing_zone: &mut LandingZoneQueryTiles,
            ) -> Result<(), super::error::CacheError> {
                match landing_zone {
                    LandingZoneQueryTiles::$variant(v) => {
                        v.push(self);
                        Ok(())
                    }
                    _ => Err(super::error::CacheError::InvalidTypeForInsertion),
                }
            }

            fn create_empty_landing_zone() -> LandingZoneQueryTiles {
                LandingZoneQueryTiles::$variant(Vec::new())
            }

            fn results_arc(cache_elements_container: &CachedTiles) -> Option<Arc<Vec<Self>>> {
                if let CachedTiles::$variant(v) = cache_elements_container {
                    Some(v.clone())
                } else {
                    None
                }
            }

            fn landing_zone_to_cache_entry(
                landing_zone_entry: RasterLandingQueryEntry,
            ) -> RasterCacheQueryEntry {
                landing_zone_entry.into()
            }
        }
    };
}
impl_cache_element_subtype!(i8, I8);
impl_cache_element_subtype!(u8, U8);
impl_cache_element_subtype!(i16, I16);
impl_cache_element_subtype!(u16, U16);
impl_cache_element_subtype!(i32, I32);
impl_cache_element_subtype!(u32, U32);
impl_cache_element_subtype!(i64, I64);
impl_cache_element_subtype!(u64, U64);
impl_cache_element_subtype!(f32, F32);
impl_cache_element_subtype!(f64, F64);

type DecompressorFutureType<T> =
    tokio::task::JoinHandle<std::result::Result<RasterTile2D<T>, CacheError>>;

/// Our own tile stream that "owns" the data (more precisely a reference to the data)

#[pin_project(project = CacheTileStreamProjection)]
pub struct CacheTileStream<T> {
    inner: CacheTileStreamInner<T>,
    #[pin]
    state: Option<DecompressorFutureType<T>>,
}

pub struct CacheTileStreamInner<T> {
    data: Arc<Vec<CompressedRasterTile2D<T>>>,
    query: RasterQueryRectangle,
    idx: usize,
}

impl<T> CacheTileStreamInner<T>
where
    T: Pixel,
{
    // TODO: we could use a iter + filter adapter here to return refs however this would require a lot of lifetime annotations
    fn next_idx(&mut self) -> Option<usize> {
        let query_st_bounds = self.query.spatial_bounds;
        let query_time_bounds = self.query.time_interval;
        for i in self.idx..self.data.len() {
            let tile_ref = &self.data[i];
            let tile_bbox = tile_ref.spatial_partition();
            if tile_bbox.intersects(&query_st_bounds)
                && tile_ref.time.intersects(&query_time_bounds)
            {
                self.idx = i + 1;
                return Some(i);
            }
        }
        None
    }

    fn data_arc(&self) -> Arc<Vec<CompressedRasterTile2D<T>>> {
        self.data.clone()
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn remaining(&self) -> usize {
        self.len() - self.idx
    }

    fn terminated(&self) -> bool {
        self.idx >= self.len()
    }

    fn new(data: Arc<Vec<CompressedRasterTile2D<T>>>, query: RasterQueryRectangle) -> Self {
        Self {
            data,
            query,
            idx: 0,
        }
    }
}

impl<T> CacheTileStream<T>
where
    T: Pixel,
{
    pub fn new(data: Arc<Vec<CompressedRasterTile2D<T>>>, query: RasterQueryRectangle) -> Self {
        Self {
            inner: CacheTileStreamInner::new(data, query),
            state: None,
        }
    }

    pub fn element_count(&self) -> usize {
        self.inner.len()
    }

    fn terminated(&self) -> bool {
        self.state.is_none() && self.inner.terminated()
    }

    fn check_decompress_future_res(
        tile_future_res: Result<Result<RasterTile2D<T>, CacheError>, tokio::task::JoinError>,
    ) -> Result<RasterTile2D<T>, CacheError> {
        match tile_future_res {
            Ok(Ok(tile)) => Ok(tile),
            Ok(Err(err)) => Err(err),
            Err(source) => Err(CacheError::CouldNotRunDecompressionTask { source }),
        }
    }

    fn create_decompression_future(
        data: Arc<Vec<CompressedRasterTile2D<T>>>,
        idx: usize,
    ) -> DecompressorFutureType<T>
    where
        T: Pixel,
        CompressedRasterTile2D<T>: CacheBackendElementExt<Query = RasterQueryRectangle>,
    {
        crate::util::spawn_blocking(move || RasterTile2D::<T>::from_stored_element_ref(&data[idx]))
    }
}

impl<T: Pixel> Stream for CacheTileStream<T>
where
    RasterTile2D<T>: CacheElement<StoredCacheElement = CompressedRasterTile2D<T>>,
    CompressedRasterTile2D<T>: CacheBackendElementExt<Query = RasterQueryRectangle>,
{
    type Item = Result<RasterTile2D<T>, CacheError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.terminated() {
            return std::task::Poll::Ready(None);
        }

        let CacheTileStreamProjection { inner, mut state } = self.as_mut().project();

        if state.is_none() {
            if let Some(next) = inner.next_idx() {
                let future_data = inner.data_arc();
                let future = Self::create_decompression_future(future_data, next);
                state.set(Some(future));
            }
        }

        if let Some(pin_state) = state.as_mut().as_pin_mut() {
            let res = futures::ready!(pin_state.poll(cx));
            state.set(None);
            let tile = Self::check_decompress_future_res(res);
            return std::task::Poll::Ready(Some(tile));
        }

        std::task::Poll::Ready(None)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.terminated() {
            return (0, Some(0));
        }
        // There must be a cache hit to produce this stream. So there must be at least one tile inside the query.
        (1, Some(self.inner.remaining()))
    }
}

impl<T> FusedStream for CacheTileStream<T>
where
    T: Pixel,
    CompressedRasterTile2D<T>: CacheBackendElementExt<Query = RasterQueryRectangle>,
{
    fn is_terminated(&self) -> bool {
        self.terminated()
    }
}

#[derive(Clone, Debug)]
pub struct CompressedMaskedGrid<D, T, C> {
    shape: D,
    type_marker: PhantomData<T>,
    data: Vec<u8>,
    mask: Vec<u8>,
    compression_marker: PhantomData<C>,
}

#[derive(Clone, Debug)]
pub enum CompressedGridOrEmpty<D, T, F> {
    Empty(EmptyGrid<D, T>),
    Compressed(CompressedMaskedGrid<D, T, F>),
}

pub type CompressedRasterTile<D, T> = BaseTile<CompressedGridOrEmpty<D, T, Lz4FlexCompression>>;
pub type CompressedRasterTile2D<T> = CompressedRasterTile<GridShape2D, T>;

impl<D, T, C: TileCompression> CompressedMaskedGrid<D, T, C> {
    #[cfg(test)]
    pub(crate) fn new(shape: D, data: Vec<u8>, mask: Vec<u8>) -> Self {
        Self {
            shape,
            type_marker: PhantomData,
            data,
            mask,
            compression_marker: PhantomData,
        }
    }

    pub fn compressed_data_len(&self) -> usize {
        self.data.len()
    }

    pub fn compressed_mask_len(&self) -> usize {
        self.mask.len()
    }

    pub fn compressed_len(&self) -> usize {
        self.compressed_data_len() + self.compressed_mask_len()
    }

    pub fn compressed_data_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn compressed_mask_slice(&self) -> &[u8] {
        &self.mask
    }

    pub fn shape(&self) -> &D {
        &self.shape
    }

    pub fn compress_masked_grid(grid: &MaskedGrid<D, T>) -> Self
    where
        D: Clone + GridSize + PartialEq,
        T: Copy,
    {
        let grid_data_compressed = C::compress(&grid.inner_grid.data);
        let grid_mask_compressed = C::compress(&grid.validity_mask.data);

        Self {
            shape: grid.shape().clone(),
            type_marker: PhantomData,
            data: grid_data_compressed,
            mask: grid_mask_compressed,
            compression_marker: PhantomData,
        }
    }

    pub fn decompress_masked_grid(&self) -> Result<MaskedGrid<D, T>, CacheError>
    where
        D: Clone + GridSize + PartialEq,
        T: Copy,
    {
        let elements = self.shape().number_of_elements();

        let grid_data = C::decompress(&self.data, elements)?;
        let grid_mask = C::decompress(&self.mask, elements)?;

        let masked_grid = MaskedGrid {
            inner_grid: Grid {
                shape: self.shape.clone(),
                data: grid_data,
            },
            validity_mask: Grid {
                shape: self.shape.clone(),
                data: grid_mask,
            },
        };

        Ok(masked_grid)
    }
}

impl<D, T, C: TileCompression> CompressedGridOrEmpty<D, T, C> {
    pub fn compressed_data_len(&self) -> usize {
        match self {
            Self::Empty(_empty_grid) => 0,
            Self::Compressed(compressed_grid) => compressed_grid.compressed_data_len(),
        }
    }

    pub fn compressed_mask_len(&self) -> usize {
        match self {
            Self::Empty(_empty_grid) => 0,
            Self::Compressed(compressed_grid) => compressed_grid.compressed_mask_len(),
        }
    }

    pub fn compressed_len(&self) -> usize {
        match self {
            Self::Empty(_empty_grid) => 0,
            Self::Compressed(compressed_grid) => compressed_grid.compressed_len(),
        }
    }

    pub fn shape(&self) -> &D {
        match self {
            Self::Empty(empty_grid) => &empty_grid.shape,
            Self::Compressed(compressed_grid) => compressed_grid.shape(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Empty(_) => true,
            Self::Compressed(_) => false,
        }
    }

    pub fn compress_grid(grid: &GridOrEmpty<D, T>) -> Self
    where
        D: Clone + GridSize + PartialEq,
        T: Copy,
    {
        match grid {
            GridOrEmpty::Empty(empty_grid) => Self::Empty(empty_grid.clone()),
            GridOrEmpty::Grid(grid) => {
                let compressed_grid = CompressedMaskedGrid::compress_masked_grid(grid);
                Self::Compressed(compressed_grid)
            }
        }
    }

    pub fn decompress_grid(&self) -> Result<GridOrEmpty<D, T>, CacheError>
    where
        D: Clone + GridSize + PartialEq,
        T: Copy,
    {
        match self {
            Self::Empty(empty_grid) => Ok(GridOrEmpty::Empty(empty_grid.clone())),
            Self::Compressed(compressed_grid) => {
                let decompressed_grid = compressed_grid.decompress_masked_grid()?;
                Ok(GridOrEmpty::Grid(decompressed_grid))
            }
        }
    }
}

impl<D, T, C> GridSize for CompressedGridOrEmpty<D, T, C>
where
    D: GridSize + GridSpaceToLinearSpace + PartialEq + Clone,
    T: Clone + Default,
    C: TileCompression,
{
    type ShapeArray = D::ShapeArray;

    const NDIM: usize = D::NDIM;

    fn axis_size(&self) -> Self::ShapeArray {
        self.shape().axis_size()
    }

    fn number_of_elements(&self) -> usize {
        self.shape().number_of_elements()
    }
}

impl<D, T, C> ByteSize for CompressedGridOrEmpty<D, T, C> {
    fn heap_byte_size(&self) -> usize {
        match self {
            Self::Empty(empty_grid) => empty_grid.heap_byte_size(),
            Self::Compressed(compressed_grid) => compressed_grid.heap_byte_size(),
        }
    }
}

impl<D, T, C> ByteSize for CompressedMaskedGrid<D, T, C> {
    fn heap_byte_size(&self) -> usize {
        self.data.heap_byte_size() + self.mask.heap_byte_size()
    }
}

pub trait CompressedRasterTileExt<D, T>
where
    Self: Sized,
{
    fn compress_tile(tile: RasterTile<D, T>) -> Self;

    fn decompress_tile(&self) -> Result<RasterTile<D, T>, CacheError>;

    fn compressed_data_len(&self) -> usize;
}

impl<T, C> CompressedRasterTileExt<GridShape2D, T>
    for BaseTile<CompressedGridOrEmpty<GridShape2D, T, C>>
where
    T: Copy,
    C: TileCompression,
{
    fn compress_tile(tile: RasterTile<GridShape2D, T>) -> Self {
        let compressed_grid = CompressedGridOrEmpty::compress_grid(&tile.grid_array);
        Self {
            grid_array: compressed_grid,
            time: tile.time,
            cache_hint: tile.cache_hint,
            global_geo_transform: tile.global_geo_transform,
            properties: tile.properties,
            tile_position: tile.tile_position,
        }
    }

    fn decompress_tile(&self) -> Result<RasterTile<GridShape2D, T>, CacheError> {
        let grid_array = match &self.grid_array {
            CompressedGridOrEmpty::Empty(empty_grid) => GridOrEmpty::Empty(*empty_grid),
            CompressedGridOrEmpty::Compressed(compressed_grid) => {
                let decompressed_grid = compressed_grid.decompress_masked_grid()?;
                GridOrEmpty::Grid(decompressed_grid)
            }
        };

        Ok(RasterTile {
            grid_array,
            time: self.time,
            cache_hint: self.cache_hint,
            global_geo_transform: self.global_geo_transform,
            properties: self.properties.clone(),
            tile_position: self.tile_position,
        })
    }

    fn compressed_data_len(&self) -> usize {
        self.grid_array.compressed_data_len()
    }
}

impl<T> CacheElement for RasterTile2D<T>
where
    T: Pixel,
    CompressedRasterTile2D<T>: CompressedRasterTileExt<GridShape2D, T>
        + CacheBackendElement<Query = RasterQueryRectangle>
        + CacheBackendElementExt,
{
    type StoredCacheElement = CompressedRasterTile2D<T>;
    type Query = RasterQueryRectangle;
    type ResultStream = CacheTileStream<T>;

    fn into_stored_element(self) -> Self::StoredCacheElement {
        CompressedRasterTile2D::compress_tile(self)
    }

    fn from_stored_element_ref(stored: &Self::StoredCacheElement) -> Result<Self, CacheError> {
        stored.decompress_tile()
    }

    fn result_stream(
        stored_data: Arc<Vec<Self::StoredCacheElement>>,
        query: Self::Query,
    ) -> Self::ResultStream {
        CacheTileStream::new(stored_data, query)
    }
}

pub trait TileCompression {
    fn compress<T>(data: &[T]) -> Vec<u8>
    where
        T: Copy;

    fn decompress<T>(data: &[u8], elements: usize) -> Result<Vec<T>, CacheError>
    where
        T: Copy;
}

#[derive(Debug, Copy, Clone)]
pub struct Lz4FlexCompression;

impl Lz4FlexCompression {
    fn cast_data_slice_to_u8_slice<T>(data: &[T]) -> &[u8]
    where
        T: Copy,
    {
        unsafe {
            std::slice::from_raw_parts(data.as_ptr().cast::<u8>(), std::mem::size_of_val(data))
        }
    }

    fn cast_u8_slice_to_data_slice<T>(data: &[u8]) -> &[T]
    where
        T: Copy,
    {
        unsafe {
            std::slice::from_raw_parts(
                data.as_ptr().cast::<T>(),
                data.len() / std::mem::size_of::<T>(),
            )
        }
    }
}

impl TileCompression for Lz4FlexCompression {
    fn compress<T>(data: &[T]) -> Vec<u8>
    where
        T: Copy,
    {
        let data_as_u8_slice = Self::cast_data_slice_to_u8_slice(data);
        lz4_flex::compress(data_as_u8_slice)
    }

    fn decompress<T>(data: &[u8], elements: usize) -> Result<Vec<T>, CacheError>
    where
        T: Copy,
    {
        let stored_bytes = elements * std::mem::size_of::<T>();
        let decompressed_data = lz4_flex::decompress(data, stored_bytes)
            .map_err(|source| CacheError::CouldNotDecompressElement { source })?;
        let decompressed_data_as_t_slice =
            Self::cast_u8_slice_to_data_slice(decompressed_data.as_slice());
        Ok(decompressed_data_as_t_slice.to_vec())
    }
}
