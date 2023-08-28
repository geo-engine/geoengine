use super::error::CacheError;
use super::shared_cache::{
    CacheBackendElement, CacheBackendElementExt, CacheElement, CacheElementsContainer,
    CacheElementsContainerInfos, LandingZoneElementsContainer, VectorCacheQueryEntry,
    VectorLandingQueryEntry,
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
    G: Geometry + ArrowTyped,
    FeatureCollection<G>: CacheBackendElementExt<CacheContainer = Self> + CacheElementHitCheck,
{
    fn results_arc(&self) -> Option<Arc<Vec<FeatureCollection<G>>>> {
        FeatureCollection::<G>::results_arc(self)
    }
}

impl<G> LandingZoneElementsContainer<FeatureCollection<G>> for LandingZoneQueryFeatures
where
    G: Geometry + ArrowTyped,
    FeatureCollection<G>:
        CacheBackendElementExt<LandingZoneContainer = Self> + CacheElementHitCheck,
{
    fn insert_element(
        &mut self,
        element: FeatureCollection<G>,
    ) -> Result<(), super::error::CacheError> {
        FeatureCollection::<G>::move_element_into_landing_zone(element, self)
    }

    fn create_empty() -> Self {
        FeatureCollection::<G>::create_empty_landing_zone()
    }
}

impl<G> CacheBackendElement for FeatureCollection<G>
where
    G: Geometry + ArrowTyped + ArrowTyped + Sized,
    FeatureCollection<G>: CacheElementHitCheck,
{
    type Query = VectorQueryRectangle;

    fn cache_hint(&self) -> geoengine_datatypes::primitives::CacheHint {
        self.cache_hint
    }

    fn typed_canonical_operator_name(
        key: crate::engine::CanonicOperatorName,
    ) -> super::shared_cache::TypedCanonicOperatorName {
        super::shared_cache::TypedCanonicOperatorName::Vector(key)
    }

    fn update_stored_query(&self, _query: &mut Self::Query) -> Result<(), CacheError> {
        // In this case, the elements of the cache are vector data chunks.
        // Unlike raster data, chunks have no guaranteed extent (spatial or temporal) other than the limits of the query itself.
        // If a vector element has a larger extent than the query, then the bbox computed for the collection is larger than the query bbox.
        // However, there may be a point that is outside the query bbox but inside the collection bbox. As it is not in the query bbox, it must not be returned as the result of the query.
        // So the query is not updated.
        Ok(())
    }
}

macro_rules! impl_cache_element_subtype {
    ($g:ty, $variant:ident) => {
        impl CacheBackendElementExt for FeatureCollection<$g> {
            type LandingZoneContainer = LandingZoneQueryFeatures;
            type CacheContainer = CachedFeatures;

            fn move_element_into_landing_zone(
                self,
                landing_zone: &mut LandingZoneQueryFeatures,
            ) -> Result<(), super::error::CacheError> {
                match landing_zone {
                    LandingZoneQueryFeatures::$variant(v) => {
                        v.push(self);
                        Ok(())
                    }
                    _ => Err(super::error::CacheError::InvalidTypeForInsertion),
                }
            }

            fn create_empty_landing_zone() -> LandingZoneQueryFeatures {
                LandingZoneQueryFeatures::$variant(Vec::new())
            }

            fn results_arc(cache_elements_container: &CachedFeatures) -> Option<Arc<Vec<Self>>> {
                if let CachedFeatures::$variant(v) = cache_elements_container {
                    Some(v.clone())
                } else {
                    None
                }
            }

            fn landing_zone_to_cache_entry(
                landing_zone_entry: VectorLandingQueryEntry,
            ) -> VectorCacheQueryEntry {
                landing_zone_entry.into()
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
    type Item = Result<FeatureCollection<G>, CacheError>;

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

impl<G> CacheElement for FeatureCollection<G>
where
    G: Geometry + ArrowTyped,
    FeatureCollection<G>: ByteSize
        + CacheElementHitCheck
        + CacheBackendElementExt<
            Query = VectorQueryRectangle,
            LandingZoneContainer = LandingZoneQueryFeatures,
            CacheContainer = CachedFeatures,
        >,
{
    type StoredCacheElement = FeatureCollection<G>;
    type Query = VectorQueryRectangle;
    type ResultStream = CacheChunkStream<G>;

    fn into_stored_element(self) -> Self::StoredCacheElement {
        self
    }

    fn from_stored_element_ref(stored: &Self::StoredCacheElement) -> Result<Self, CacheError> {
        Ok(stored.clone())
    }

    fn result_stream(
        stored_data: Arc<Vec<Self::StoredCacheElement>>,
        query: Self::Query,
    ) -> Self::ResultStream {
        CacheChunkStream::new(stored_data, query)
    }
}
