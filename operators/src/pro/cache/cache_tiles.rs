use super::error::CacheError;
use super::shared_cache::{
    CacheElement, CacheElementSubType, CacheElementsContainer, CacheElementsContainerInfos,
    LandingZoneElementsContainer,
};
use crate::util::Result;
use futures::Stream;
use geoengine_datatypes::primitives::SpatialPartitioned;
use geoengine_datatypes::{
    primitives::RasterQueryRectangle,
    raster::{Pixel, RasterTile2D},
    util::ByteSize,
};
use pin_project::pin_project;
use std::{pin::Pin, sync::Arc};

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

impl CacheElementsContainerInfos<RasterQueryRectangle> for CachedTiles {
    fn is_expired(&self) -> bool {
        self.is_expired()
    }
}

impl<T> CacheElementsContainer<RasterQueryRectangle, RasterTile2D<T>> for CachedTiles
where
    T: CacheElementSubType<CacheElementType = RasterTile2D<T>> + Pixel,
{
    type ResultStream = CacheTileStream<T>;

    fn result_stream(&self, query: &RasterQueryRectangle) -> Option<CacheTileStream<T>> {
        T::result_stream(self, query)
    }
}

impl<T> LandingZoneElementsContainer<RasterTile2D<T>> for LandingZoneQueryTiles
where
    T: CacheElementSubType<CacheElementType = RasterTile2D<T>> + Pixel,
{
    fn insert_element(&mut self, element: RasterTile2D<T>) -> Result<(), super::error::CacheError> {
        T::insert_element_into_landing_zone(self, element)
    }

    fn create_empty() -> Self {
        T::create_empty_landing_zone()
    }
}

impl<T> CacheElement for RasterTile2D<T>
where
    T: Pixel + CacheElementSubType<CacheElementType = Self>,
{
    type Query = RasterQueryRectangle;
    type LandingZoneContainer = LandingZoneQueryTiles;
    type CacheContainer = CachedTiles;
    type ResultStream = CacheTileStream<T>;
    type CacheElementSubType = T;

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

macro_rules! impl_cache_element_subtype_magic {
    ($t:ty, $variant:ident) => {
        impl CacheElementSubType for $t {
            type CacheElementType = RasterTile2D<$t>;

            fn insert_element_into_landing_zone(
                landing_zone: &mut LandingZoneQueryTiles,
                element: Self::CacheElementType,
            ) -> Result<(), super::error::CacheError> {
                match landing_zone {
                    LandingZoneQueryTiles::$variant(v) => {
                        v.push(element);
                        Ok(())
                    }
                    _ => Err(super::error::CacheError::InvalidTypeForInsertion),
                }
            }

            fn create_empty_landing_zone() -> LandingZoneQueryTiles {
                LandingZoneQueryTiles::$variant(Vec::new())
            }

            fn result_stream(
                cache_elements_container: &CachedTiles,
                query: &RasterQueryRectangle,
            ) -> Option<CacheTileStream<$t>> {
                if let TypedCacheTileStream::$variant(v) =
                    cache_elements_container.tile_stream(query)
                {
                    Some(v)
                } else {
                    None
                }
            }
        }
    };
}
impl_cache_element_subtype_magic!(i8, I8);
impl_cache_element_subtype_magic!(u8, U8);
impl_cache_element_subtype_magic!(i16, I16);
impl_cache_element_subtype_magic!(u16, U16);
impl_cache_element_subtype_magic!(i32, I32);
impl_cache_element_subtype_magic!(u32, U32);
impl_cache_element_subtype_magic!(i64, I64);
impl_cache_element_subtype_magic!(u64, U64);
impl_cache_element_subtype_magic!(f32, F32);
impl_cache_element_subtype_magic!(f64, F64);

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
            let tile_bbox = tile.spatial_partition();

            if tile_bbox.intersects(&query.spatial_bounds)
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
