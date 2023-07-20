use std::sync::Arc;

use geoengine_datatypes::{
    collections::{
        FeatureCollection, FeatureCollectionInfos, GeometryCollection, MultiLineStringCollection,
        MultiPointCollection, MultiPolygonCollection,
    },
    primitives::{Geometry, MultiLineString, MultiPoint, MultiPolygon, VectorQueryRectangle},
    util::{arrow::ArrowTyped, ByteSize},
};

use super::tile_cache::{
    CacheElement, CacheElementsContainer, CacheElementsContainerInfos, LandingZoneElementsContainer,
};
use super::{
    cache_chunk_stream::{CacheChunkStream, TypedCacheChunkStream},
    tile_cache::CachableSubType,
};

#[derive(Debug)]
pub enum CachedFeatures {
    // NoGeometry(Arc<Vec<DataCollection>>),
    MultiPoint(Arc<Vec<MultiPointCollection>>),
    MultiLineString(Arc<Vec<MultiLineStringCollection>>),
    MultiPolygon(Arc<Vec<MultiPolygonCollection>>),
}

impl CachedFeatures {
    pub fn len(&self) -> usize {
        match self {
            //CachedFeatures::NoGeometry(v) => v.len(),
            CachedFeatures::MultiPoint(v) => v.len(),
            CachedFeatures::MultiLineString(v) => v.len(),
            CachedFeatures::MultiPolygon(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_expired(&self) -> bool {
        match self {
            //CachedFeatures::NoGeometry(v) => v.iter().any(|c| c.cache_hint.is_expired()),
            CachedFeatures::MultiPoint(v) => v.iter().any(|c| c.cache_hint.is_expired()),
            CachedFeatures::MultiLineString(v) => v.iter().any(|c| c.cache_hint.is_expired()),
            CachedFeatures::MultiPolygon(v) => v.iter().any(|c| c.cache_hint.is_expired()),
        }
    }

    pub fn chunk_stream(&self, query: &VectorQueryRectangle) -> TypedCacheChunkStream {
        match self {
            //CachedFeatures::NoGeometry(v) => {
            //    TypedCacheChunkStream::Data(CacheChunkStream::new(Arc::clone(v), *query))
            //}
            CachedFeatures::MultiPoint(v) => {
                TypedCacheChunkStream::MultiPoint(CacheChunkStream::new(Arc::clone(v), *query))
            }
            CachedFeatures::MultiLineString(v) => {
                TypedCacheChunkStream::MultiLineString(CacheChunkStream::new(Arc::clone(v), *query))
            }
            CachedFeatures::MultiPolygon(v) => {
                TypedCacheChunkStream::MultiPolygon(CacheChunkStream::new(Arc::clone(v), *query))
            }
        }
    }
}

impl ByteSize for CachedFeatures {
    fn heap_byte_size(&self) -> usize {
        // we need to use `byte_size` instead of `heap_byte_size` here, because `Vec` stores its data on the heap
        match self {
            // CachedFeatures::NoGeometry(v) => v.iter().map(FeatureCollectionInfos::byte_size).sum(),
            CachedFeatures::MultiPoint(v) => v.iter().map(FeatureCollectionInfos::byte_size).sum(),
            CachedFeatures::MultiLineString(v) => {
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
    // NoGeometry(Vec<DataCollection>),
    MultiPoint(Vec<MultiPointCollection>),
    MultiLineString(Vec<MultiLineStringCollection>),
    MultiPolygon(Vec<MultiPolygonCollection>),
}

impl LandingZoneQueryFeatures {
    pub fn len(&self) -> usize {
        match self {
            //LandingZoneQueryFeatures::NoGeometry(v) => v.len(),
            LandingZoneQueryFeatures::MultiPoint(v) => v.len(),
            LandingZoneQueryFeatures::MultiLineString(v) => v.len(),
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
            // LandingZoneQueryFeatures::NoGeometry(v) => {
            //     v.iter().map(FeatureCollectionInfos::byte_size).sum()
            // }
            LandingZoneQueryFeatures::MultiPoint(v) => {
                v.iter().map(FeatureCollectionInfos::byte_size).sum()
            }
            LandingZoneQueryFeatures::MultiLineString(v) => {
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
            // LandingZoneQueryFeatures::NoGeometry(v) => CachedFeatures::Data(Arc::new(v)),
            LandingZoneQueryFeatures::MultiPoint(v) => CachedFeatures::MultiPoint(Arc::new(v)),
            LandingZoneQueryFeatures::MultiLineString(v) => {
                CachedFeatures::MultiLineString(Arc::new(v))
            }
            LandingZoneQueryFeatures::MultiPolygon(v) => CachedFeatures::MultiPolygon(Arc::new(v)),
        }
    }
}

impl CacheElementsContainerInfos<VectorQueryRectangle> for CachedFeatures {
    fn is_expired(&self) -> bool {
        self.is_expired()
    }
}

impl<G> CacheElementsContainer<VectorQueryRectangle, FeatureCollection<G>> for CachedFeatures
where
    G: CachableSubType<CacheElementType = FeatureCollection<G>> + Geometry + ArrowTyped,
    FeatureCollection<G>: GeometryCollection,
{
    type ResultStream = CacheChunkStream<G>;

    fn result_stream(&self, query: &VectorQueryRectangle) -> Option<CacheChunkStream<G>> {
        G::result_stream(self, query)
    }
}

impl<G> LandingZoneElementsContainer<FeatureCollection<G>> for LandingZoneQueryFeatures
where
    G: CachableSubType<CacheElementType = FeatureCollection<G>> + Geometry + ArrowTyped,
    FeatureCollection<G>: GeometryCollection,
{
    fn insert_element(
        &mut self,
        element: FeatureCollection<G>,
    ) -> Result<(), super::error::CacheError> {
        G::insert_element_into_landing_zone(self, element)
    }

    fn create_empty() -> Self {
        G::create_empty_landing_zone()
    }
}

impl<G> CacheElement for FeatureCollection<G>
where
    G: Geometry + ArrowTyped + CachableSubType<CacheElementType = Self> + ArrowTyped + Sized,
    FeatureCollection<G>: GeometryCollection,
{
    type Query = VectorQueryRectangle;
    type LandingZoneContainer = LandingZoneQueryFeatures;
    type CacheContainer = CachedFeatures;
    type ResultStream = CacheChunkStream<G>;

    fn cache_hint(&self) -> geoengine_datatypes::primitives::CacheHint {
        self.cache_hint
    }

    fn typed_canonical_operator_name(
        key: crate::engine::CanonicOperatorName,
    ) -> super::tile_cache::TypedCanonicOperatorName {
        super::tile_cache::TypedCanonicOperatorName::Vector(key)
    }
}

macro_rules! impl_cache_element_subtype_magic {
    ($g:ty, $variant:ident) => {
        impl CachableSubType for $g {
            type CacheElementType = FeatureCollection<$g>;

            fn insert_element_into_landing_zone(
                landing_zone: &mut LandingZoneQueryFeatures,
                element: Self::CacheElementType,
            ) -> Result<(), super::error::CacheError> {
                match landing_zone {
                    LandingZoneQueryFeatures::$variant(v) => {
                        v.push(element);
                        Ok(())
                    }
                    _ => Err(super::error::CacheError::InvalidTypeForInsertion),
                }
            }

            fn create_empty_landing_zone() -> LandingZoneQueryFeatures {
                LandingZoneQueryFeatures::$variant(Vec::new())
            }

            fn result_stream(
                cache_elements_container: &CachedFeatures,
                query: &VectorQueryRectangle,
            ) -> Option<CacheChunkStream<$g>> {
                if let TypedCacheChunkStream::$variant(v) =
                    cache_elements_container.chunk_stream(query)
                {
                    Some(v)
                } else {
                    None
                }
            }
        }
    };
}
impl_cache_element_subtype_magic!(MultiPoint, MultiPoint);
impl_cache_element_subtype_magic!(MultiLineString, MultiLineString);
impl_cache_element_subtype_magic!(MultiPolygon, MultiPolygon);
