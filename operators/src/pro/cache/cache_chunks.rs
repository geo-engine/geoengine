use super::error::CacheError;
use super::shared_cache::CacheElementSubType;
use super::shared_cache::{
    CacheElement, CacheElementsContainer, CacheElementsContainerInfos, LandingZoneElementsContainer,
};
use crate::util::Result;
use futures::stream::FusedStream;
use futures::Stream;
use geoengine_datatypes::{
    collections::{
        DataCollection, FeatureCollection, FeatureCollectionInfos, FeatureCollectionModifications,
        GeometryCollection, IntoGeometryIterator, MultiLineStringCollection, MultiPointCollection,
        MultiPolygonCollection,
    },
    primitives::{
        Geometry, MultiLineString, MultiPoint, MultiPolygon, NoGeometry, VectorQueryRectangle,
    },
    util::{arrow::ArrowTyped, ByteSize},
};
use pin_project::pin_project;
use std::{pin::Pin, sync::Arc};

#[derive(Debug)]
pub enum CachedFeatures {
    NoGeometry(Arc<Vec<DataCollection>>),
    MultiPoint(Arc<Vec<MultiPointCollection>>),
    MultiLineString(Arc<Vec<MultiLineStringCollection>>),
    MultiPolygon(Arc<Vec<MultiPolygonCollection>>),
}

impl CachedFeatures {
    pub fn len(&self) -> usize {
        match self {
            CachedFeatures::NoGeometry(v) => v.len(),
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
            CachedFeatures::NoGeometry(v) => v.iter().any(|c| c.cache_hint.is_expired()),
            CachedFeatures::MultiPoint(v) => v.iter().any(|c| c.cache_hint.is_expired()),
            CachedFeatures::MultiLineString(v) => v.iter().any(|c| c.cache_hint.is_expired()),
            CachedFeatures::MultiPolygon(v) => v.iter().any(|c| c.cache_hint.is_expired()),
        }
    }

    pub fn chunk_stream(&self, query: &VectorQueryRectangle) -> TypedCacheChunkStream {
        match self {
            CachedFeatures::NoGeometry(v) => {
                TypedCacheChunkStream::NoGeometry(CacheChunkStream::new(Arc::clone(v), *query))
            }
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
            CachedFeatures::NoGeometry(v) => v.iter().map(FeatureCollectionInfos::byte_size).sum(),
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
    NoGeometry(Vec<DataCollection>),
    MultiPoint(Vec<MultiPointCollection>),
    MultiLineString(Vec<MultiLineStringCollection>),
    MultiPolygon(Vec<MultiPolygonCollection>),
}

impl LandingZoneQueryFeatures {
    pub fn len(&self) -> usize {
        match self {
            LandingZoneQueryFeatures::NoGeometry(v) => v.len(),
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
            LandingZoneQueryFeatures::NoGeometry(v) => {
                v.iter().map(FeatureCollectionInfos::byte_size).sum()
            }
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
            LandingZoneQueryFeatures::NoGeometry(v) => CachedFeatures::NoGeometry(Arc::new(v)),
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
    G: CacheElementSubType<CacheElementType = FeatureCollection<G>> + Geometry + ArrowTyped,
    FeatureCollection<G>: CacheElementHitCheck,
{
    type ResultStream = CacheChunkStream<G>;

    fn result_stream(&self, query: &VectorQueryRectangle) -> Option<CacheChunkStream<G>> {
        G::result_stream(self, query)
    }
}

impl<G> LandingZoneElementsContainer<FeatureCollection<G>> for LandingZoneQueryFeatures
where
    G: CacheElementSubType<CacheElementType = FeatureCollection<G>> + Geometry + ArrowTyped,
    FeatureCollection<G>: CacheElementHitCheck,
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
    G: Geometry + ArrowTyped + CacheElementSubType<CacheElementType = Self> + ArrowTyped + Sized,
    FeatureCollection<G>: CacheElementHitCheck,
{
    type Query = VectorQueryRectangle;
    type LandingZoneContainer = LandingZoneQueryFeatures;
    type CacheContainer = CachedFeatures;
    type ResultStream = CacheChunkStream<G>;
    type CacheElementSubType = G;

    fn cache_hint(&self) -> geoengine_datatypes::primitives::CacheHint {
        self.cache_hint
    }

    fn typed_canonical_operator_name(
        key: crate::engine::CanonicOperatorName,
    ) -> super::shared_cache::TypedCanonicOperatorName {
        super::shared_cache::TypedCanonicOperatorName::Vector(key)
    }

    fn update_stored_query(&self, _query: &mut Self::Query) -> Result<(), CacheError> {
        Ok(())
    }
}

macro_rules! impl_cache_element_subtype {
    ($g:ty, $variant:ident) => {
        impl CacheElementSubType for $g {
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
impl_cache_element_subtype!(NoGeometry, NoGeometry);
impl_cache_element_subtype!(MultiPoint, MultiPoint);
impl_cache_element_subtype!(MultiLineString, MultiLineString);
impl_cache_element_subtype!(MultiPolygon, MultiPolygon);

/// Our own tile stream that "owns" the data (more precisely a reference to the data)
#[pin_project(project = CacheChunkStreamProjection)]
pub struct CacheChunkStream<G> {
    data: Arc<Vec<FeatureCollection<G>>>,
    query: VectorQueryRectangle,
    idx: usize,
}

impl<G> CacheChunkStream<G>
where
    G: Geometry,
    FeatureCollection<G>: FeatureCollectionInfos,
{
    pub fn new(data: Arc<Vec<FeatureCollection<G>>>, query: VectorQueryRectangle) -> Self {
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

impl<G: Geometry> Stream for CacheChunkStream<G>
where
    FeatureCollection<G>: CacheElementHitCheck,
    G: ArrowTyped,
{
    type Item = Result<FeatureCollection<G>>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let CacheChunkStreamProjection { data, query, idx } = self.as_mut().project();

        // return the next tile that is contained in the query, skip all tiles that are not contained
        for i in *idx..data.len() {
            let chunk = &data[i];
            if chunk.cache_element_hit(query) {
                // TODO: we really should cache the elements bbox somewhere
                let Ok(chunk) = chunk.filter_cache_element_entries(query) else {
                    // This should not happen, since we already checked that the element is contained in the query
                    log::error!("Could not filter cache element entries");
                    continue;
                };

                // if the chunk is empty, we can skip it
                if chunk.is_empty() {
                    log::trace!("Skipping empty chunk after filtering for query rectangle");
                    continue;
                }

                // set the index to the next element
                *idx = i + 1;
                return std::task::Poll::Ready(Some(Ok(chunk)));
            }
        }

        std::task::Poll::Ready(None)
    }
}

impl<G> FusedStream for CacheChunkStream<G>
where
    G: Geometry + ArrowTyped,
    FeatureCollection<G>: CacheElementHitCheck,
{
    fn is_terminated(&self) -> bool {
        self.idx >= self.data.len()
    }
}

pub enum TypedCacheChunkStream {
    NoGeometry(CacheChunkStream<NoGeometry>),
    MultiPoint(CacheChunkStream<MultiPoint>),
    MultiLineString(CacheChunkStream<MultiLineString>),
    MultiPolygon(CacheChunkStream<MultiPolygon>),
}

pub trait CacheElementHitCheck {
    fn cache_element_hit(&self, query_rect: &VectorQueryRectangle) -> bool;

    fn filter_cache_element_entries(
        &self,
        query_rect: &VectorQueryRectangle,
    ) -> Result<Self, CacheError>
    where
        Self: Sized;
}

impl CacheElementHitCheck for FeatureCollection<NoGeometry> {
    fn cache_element_hit(&self, query_rect: &VectorQueryRectangle) -> bool {
        let Some(time_bounds) = self.time_bounds() else {
            return false;
        };

        time_bounds == query_rect.time_interval || time_bounds.intersects(&query_rect.time_interval)
    }
    fn filter_cache_element_entries(
        &self,
        query_rect: &VectorQueryRectangle,
    ) -> Result<Self, CacheError> {
        let time_filter_bools = self
            .time_intervals()
            .iter()
            .map(|t| t.intersects(&query_rect.time_interval))
            .collect::<Vec<bool>>();
        self.filter(time_filter_bools)
            .map_err(|_err| CacheError::CouldNotFilterResults)
    }
}

macro_rules! impl_cache_result_check {
    ($t:ty) => {
        impl<'a> CacheElementHitCheck for FeatureCollection<$t>
        where
            FeatureCollection<$t>: GeometryCollection,
        {
            fn cache_element_hit(&self, query_rect: &VectorQueryRectangle) -> bool {
                let Some(bbox) = self.bbox() else {return false;};

                let Some(time_bounds) = self.time_bounds() else {return false;};

                (bbox == query_rect.spatial_bounds
                    || bbox.intersects_bbox(&query_rect.spatial_bounds))
                    && (time_bounds == query_rect.time_interval
                        || time_bounds.intersects(&query_rect.time_interval))
            }

            fn filter_cache_element_entries(
                &self,
                query_rect: &VectorQueryRectangle,
            ) -> Result<Self, CacheError> {
                let geoms_filter_bools = self.geometries().map(|g| {
                    g.bbox()
                        .map(|bbox| bbox.intersects_bbox(&query_rect.spatial_bounds))
                        .unwrap_or(false)
                });

                let time_filter_bools = self
                    .time_intervals()
                    .iter()
                    .map(|t| t.intersects(&query_rect.time_interval));

                let filter_bools = geoms_filter_bools
                    .zip(time_filter_bools)
                    .map(|(g, t)| g && t)
                    .collect::<Vec<bool>>();

                self.filter(filter_bools)
                    .map_err(|_err| CacheError::CouldNotFilterResults)
            }
        }
    };
}

impl_cache_result_check!(MultiPoint);
impl_cache_result_check!(MultiLineString);
impl_cache_result_check!(MultiPolygon);
