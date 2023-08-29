use super::cache_stream::CacheStream;
use super::error::CacheError;
use super::shared_cache::{
    CacheBackendElement, CacheBackendElementExt, CacheElement, CacheElementsContainer,
    CacheElementsContainerInfos, LandingZoneElementsContainer, VectorCacheQueryEntry,
    VectorLandingQueryEntry,
};
use crate::util::Result;
use arrow::array::StructArray;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::{FileWriter, IpcWriteOptions};
use arrow::record_batch::RecordBatch;
use geoengine_datatypes::collections::FeatureCollectionInternals;
use geoengine_datatypes::primitives::VectorQueryRectangle;
use geoengine_datatypes::{
    collections::{
        FeatureCollection, FeatureCollectionInfos, FeatureCollectionModifications,
        GeometryCollection, IntoGeometryIterator,
    },
    primitives::{Geometry, MultiLineString, MultiPoint, MultiPolygon, NoGeometry},
    util::{arrow::ArrowTyped, ByteSize},
};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

#[derive(Debug)]
pub enum CachedFeatures {
    NoGeometry(Arc<Vec<CompressedDataCollection>>),
    MultiPoint(Arc<Vec<CompressedMultiPointCollection>>),
    MultiLineString(Arc<Vec<CompressedMultiLineStringCollection>>),
    MultiPolygon(Arc<Vec<CompressedMultiPolygonCollection>>),
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
            CachedFeatures::NoGeometry(v) => v.iter().map(ByteSize::byte_size).sum(),
            CachedFeatures::MultiPoint(v) => v.iter().map(ByteSize::byte_size).sum(),
            CachedFeatures::MultiLineString(v) => v.iter().map(ByteSize::byte_size).sum(),
            CachedFeatures::MultiPolygon(v) => v.iter().map(ByteSize::byte_size).sum(),
        }
    }
}

#[derive(Debug)]
pub enum LandingZoneQueryFeatures {
    NoGeometry(Vec<CompressedDataCollection>),
    MultiPoint(Vec<CompressedMultiPointCollection>),
    MultiLineString(Vec<CompressedMultiLineStringCollection>),
    MultiPolygon(Vec<CompressedMultiPolygonCollection>),
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
            LandingZoneQueryFeatures::NoGeometry(v) => v.iter().map(ByteSize::byte_size).sum(),
            LandingZoneQueryFeatures::MultiPoint(v) => v.iter().map(ByteSize::byte_size).sum(),
            LandingZoneQueryFeatures::MultiLineString(v) => v.iter().map(ByteSize::byte_size).sum(),
            LandingZoneQueryFeatures::MultiPolygon(v) => v.iter().map(ByteSize::byte_size).sum(),
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

impl<G> CacheElementsContainer<VectorQueryRectangle, CompressedFeatureCollection<G>>
    for CachedFeatures
where
    G: Geometry + ArrowTyped,
    CompressedFeatureCollection<G>: CacheBackendElementExt<CacheContainer = Self>,
{
    fn results_arc(&self) -> Option<Arc<Vec<CompressedFeatureCollection<G>>>> {
        CompressedFeatureCollection::<G>::results_arc(self)
    }
}

impl<G> LandingZoneElementsContainer<CompressedFeatureCollection<G>> for LandingZoneQueryFeatures
where
    G: Geometry + ArrowTyped,
    CompressedFeatureCollection<G>: CacheBackendElementExt<LandingZoneContainer = Self>,
{
    fn insert_element(
        &mut self,
        element: CompressedFeatureCollection<G>,
    ) -> Result<(), super::error::CacheError> {
        CompressedFeatureCollection::<G>::move_element_into_landing_zone(element, self)
    }

    fn create_empty() -> Self {
        CompressedFeatureCollection::<G>::create_empty_landing_zone()
    }
}

impl<G> CacheBackendElement for CompressedFeatureCollection<G>
where
    G: Geometry + ArrowTyped + ArrowTyped + Sized,
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

    fn intersects_query(&self, query: &Self::Query) -> bool {
        // If the chunk has no time bounds it must be empty so we can skip the temporal check and return true.
        let temporal_hit = self
            .time_interval
            .map_or(true, |tb| tb.intersects(&query.time_interval));

        // If the chunk has no spatial bounds it is either an empty collection or a no geometry collection.
        let spatial_hit = self
            .spatial_bounds
            .map_or(true, |sb| sb.intersects_bbox(&query.spatial_bounds));

        temporal_hit && spatial_hit
    }
}

macro_rules! impl_cache_element_subtype {
    ($g:ty, $variant:ident) => {
        impl CacheBackendElementExt for CompressedFeatureCollection<$g> {
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

pub trait CacheElementSpatialBounds {
    fn filter_cache_element_entries(
        &self,
        query_rect: &VectorQueryRectangle,
    ) -> Result<Self, CacheError>
    where
        Self: Sized;

    fn cache_element_spatial_bounds(
        &self,
    ) -> Option<geoengine_datatypes::primitives::BoundingBox2D>
    where
        Self: Sized;
}

impl CacheElementSpatialBounds for FeatureCollection<NoGeometry> {
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

    fn cache_element_spatial_bounds(
        &self,
    ) -> Option<geoengine_datatypes::primitives::BoundingBox2D> {
        None
    }
}

macro_rules! impl_cache_result_check {
    ($t:ty) => {
        impl<'a> CacheElementSpatialBounds for FeatureCollection<$t>
        where
            FeatureCollection<$t>: GeometryCollection,
        {
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

            fn cache_element_spatial_bounds(
                &self,
            ) -> Option<geoengine_datatypes::primitives::BoundingBox2D> {
                self.bbox()
            }
        }
    };
}

impl_cache_result_check!(MultiPoint);
impl_cache_result_check!(MultiLineString);
impl_cache_result_check!(MultiPolygon);

impl<G> CacheElement for FeatureCollection<G>
where
    G: Geometry + ArrowTyped + 'static,
    CompressedFeatureCollection<G>: CacheBackendElementExt<
        Query = VectorQueryRectangle,
        LandingZoneContainer = LandingZoneQueryFeatures,
        CacheContainer = CachedFeatures,
    >,
    FeatureCollection<G>: ByteSize + CacheElementSpatialBounds,
{
    type StoredCacheElement = CompressedFeatureCollection<G>;
    type Query = VectorQueryRectangle;
    type ResultStream =
        CacheStream<CompressedFeatureCollection<G>, FeatureCollection<G>, VectorQueryRectangle>;

    fn into_stored_element(self) -> Self::StoredCacheElement {
        CompressedFeatureCollection::<G>::from_collection(self)
            .expect("Compressing the feature collection should not fail")
    }

    fn from_stored_element_ref(stored: &Self::StoredCacheElement) -> Result<Self, CacheError> {
        stored.to_collection()
    }

    fn result_stream(
        stored_data: Arc<Vec<Self::StoredCacheElement>>,
        query: Self::Query,
    ) -> Self::ResultStream {
        CacheStream::new(stored_data, query)
    }
}

pub type CompressedFeatureCollection<G> = CompressedFeatureCollectionImpl<G, Lz4FlexCompression>;
pub type CompressedDataCollection = CompressedFeatureCollection<NoGeometry>;
pub type CompressedMultiPointCollection = CompressedFeatureCollection<MultiPoint>;
pub type CompressedMultiLineStringCollection = CompressedFeatureCollection<MultiLineString>;
pub type CompressedMultiPolygonCollection = CompressedFeatureCollection<MultiPolygon>;

#[derive(Debug)]
pub struct CompressedFeatureCollectionImpl<G, C> {
    data: Vec<u8>,
    spatial_bounds: Option<geoengine_datatypes::primitives::BoundingBox2D>,
    time_interval: Option<geoengine_datatypes::primitives::TimeInterval>,
    types: HashMap<String, geoengine_datatypes::primitives::FeatureDataType>,
    cache_hint: geoengine_datatypes::primitives::CacheHint,
    collection_type: std::marker::PhantomData<G>,
    compression_type: std::marker::PhantomData<C>,
}

impl<G, C> ByteSize for CompressedFeatureCollectionImpl<G, C> {
    fn heap_byte_size(&self) -> usize {
        self.data.heap_byte_size()
    }
}

impl<G, C> CompressedFeatureCollectionImpl<G, C>
where
    FeatureCollection<G>: FeatureCollectionInfos + CacheElementSpatialBounds,
    C: ChunkCompression,
{
    pub fn from_collection(collection: FeatureCollection<G>) -> Result<Self, CacheError> {
        let spatial_bounds = collection.cache_element_spatial_bounds();
        let time_interval = collection.time_bounds();

        let FeatureCollectionInternals {
            table,
            types,
            collection_type,
            cache_hint,
        } = collection.into();

        let data = C::compress_array(table)?;

        let compressed: CompressedFeatureCollectionImpl<G, C> = CompressedFeatureCollectionImpl {
            data,
            spatial_bounds,
            time_interval,
            types,
            cache_hint,
            collection_type,
            compression_type: Default::default(),
        };

        Ok(compressed)
    }

    pub fn to_collection(&self) -> Result<FeatureCollection<G>, CacheError> {
        let table = C::decompress_array(&self.data)?;

        let collection_internals = FeatureCollectionInternals {
            table,
            types: self.types.clone(),
            collection_type: self.collection_type,
            cache_hint: self.cache_hint,
        };

        let collection = FeatureCollection::from(collection_internals);

        Ok(collection)
    }
}

pub trait ChunkCompression {
    fn compress_array(collection: StructArray) -> Result<Vec<u8>, CacheError>
    where
        Self: Sized;

    fn decompress_array(compressed: &[u8]) -> Result<StructArray, CacheError>
    where
        Self: Sized;

    fn array_to_bytes(table: StructArray) -> Result<Vec<u8>, CacheError> {
        let record_batch = RecordBatch::from(&table);

        let mut file_writer = FileWriter::try_new_with_options(
            Vec::new(),
            record_batch.schema().as_ref(),
            IpcWriteOptions::default(), // TODO: Add ".try_with_compression(COMPRESSION)?," once arrow provides a way to ensure that the decompressed data has the same size as the original data.
        )
        .map_err(|source| CacheError::CouldNotWriteElementToBytes { source })?;
        file_writer
            .write(&record_batch)
            .map_err(|source| CacheError::CouldNotWriteElementToBytes { source })?;
        file_writer
            .finish()
            .map_err(|source| CacheError::CouldNotWriteElementToBytes { source })?;

        file_writer
            .into_inner()
            .map_err(|source| CacheError::CouldNotWriteElementToBytes { source })
    }

    fn bytes_to_array(bytes: &[u8]) -> Result<StructArray, CacheError> {
        let mut reader = FileReader::try_new(Cursor::new(bytes), None)
            .map_err(|source| CacheError::CouldNotReadElementFromBytes { source })?;
        let record_batch = reader
            .next()
            .expect("We only call next once so there must be a record batch")
            .map_err(|source| CacheError::CouldNotReadElementFromBytes { source })?;

        Ok(StructArray::from(record_batch))
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Lz4FlexCompression;

impl ChunkCompression for Lz4FlexCompression {
    fn compress_array(collection: StructArray) -> Result<Vec<u8>, CacheError>
    where
        Self: Sized,
    {
        let bytes = Self::array_to_bytes(collection)?;
        let compressed = lz4_flex::compress_prepend_size(&bytes);
        Ok(compressed)
    }

    fn decompress_array(compressed: &[u8]) -> Result<StructArray, CacheError>
    where
        Self: Sized,
    {
        let bytes = lz4_flex::decompress_size_prepended(compressed)
            .map_err(|source| CacheError::CouldNotDecompressElement { source })?;
        let array = Self::bytes_to_array(&bytes)?;
        Ok(array)
    }
}

#[cfg(test)]
mod tests {
    use crate::pro::cache::{
        cache_chunks::{CachedFeatures, LandingZoneQueryFeatures},
        shared_cache::{
            CacheBackendElement, CacheBackendElementExt, CacheQueryMatch, VectorCacheQueryEntry,
            VectorLandingQueryEntry,
        },
    };

    use super::CompressedFeatureCollection;
    use geoengine_datatypes::{
        collections::MultiPointCollection,
        primitives::{
            BoundingBox2D, CacheHint, FeatureData, MultiPoint, SpatialResolution, TimeInterval,
            VectorQueryRectangle,
        },
    };
    use std::{collections::HashMap, sync::Arc};

    fn create_test_collection() -> Vec<CompressedFeatureCollection<MultiPoint>> {
        let mut data = Vec::new();

        for x in 0..9 {
            let mut points = Vec::new();
            let mut strngs = Vec::new();
            for i in x..x + 2 {
                let p = MultiPoint::new(vec![(f64::from(i), f64::from(i)).into()]).unwrap();
                points.push(p);
                strngs.push(format!("test {i}"));
            }

            let collection = MultiPointCollection::from_data(
                points,
                vec![TimeInterval::default(); 2],
                HashMap::<String, FeatureData>::from([(
                    "strings".to_owned(),
                    FeatureData::Text(strngs),
                )]),
                CacheHint::default(),
            )
            .unwrap();

            let compressed_collection =
                CompressedFeatureCollection::from_collection(collection).unwrap();
            data.push(compressed_collection);
        }
        data
    }

    #[test]
    fn create_empty_landing_zone() {
        let landing_zone = CompressedFeatureCollection::<MultiPoint>::create_empty_landing_zone();
        assert!(landing_zone.is_empty());
        if let LandingZoneQueryFeatures::MultiPoint(v) = landing_zone {
            assert!(v.is_empty());
        } else {
            panic!("wrong type");
        }
    }

    #[test]
    fn move_element_to_landing_zone() {
        let mut landing_zone =
            CompressedFeatureCollection::<MultiPoint>::create_empty_landing_zone();
        let col = create_test_collection();
        for c in col {
            c.move_element_into_landing_zone(&mut landing_zone).unwrap();
        }
        if let LandingZoneQueryFeatures::MultiPoint(v) = landing_zone {
            assert_eq!(v.len(), 9);
        } else {
            panic!("wrong type");
        }
    }

    #[test]
    fn landing_zone_to_cache_entry() {
        let cols = create_test_collection();
        let query = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new_unchecked((0., 0.).into(), (1., 1.).into()),
            time_interval: Default::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let mut lq =
            VectorLandingQueryEntry::create_empty::<CompressedFeatureCollection<MultiPoint>>(query);
        for c in cols {
            c.move_element_into_landing_zone(lq.elements_mut()).unwrap();
        }
        let mut cache_entry =
            CompressedFeatureCollection::<MultiPoint>::landing_zone_to_cache_entry(lq);
        assert_eq!(cache_entry.query(), &query);
        assert!(cache_entry.elements_mut().is_expired());
    }

    #[test]
    fn cache_element_hit() {
        let cols = create_test_collection();

        // elemtes are all fully contained
        let query = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new_unchecked((0., 0.).into(), (12., 12.).into()),
            time_interval: Default::default(),
            spatial_resolution: SpatialResolution::one(),
        };

        for c in &cols {
            assert!(c.intersects_query(&query));
        }

        // first element is not contained
        let query = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new_unchecked((2., 2.).into(), (10., 10.).into()),
            time_interval: Default::default(),
            spatial_resolution: SpatialResolution::one(),
        };
        assert!(!cols[0].intersects_query(&query));
        for c in &cols[1..] {
            assert!(c.intersects_query(&query));
        }

        // all elements are not contained
        let query = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new_unchecked((13., 13.).into(), (26., 26.).into()),
            time_interval: Default::default(),
            spatial_resolution: SpatialResolution::one(),
        };
        for col in &cols {
            assert!(!col.intersects_query(&query));
        }
    }

    #[test]
    fn cache_entry_matches() {
        let cols = create_test_collection();

        let cache_entry_bounds = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new_unchecked((1., 1.).into(), (11., 11.).into()),
            time_interval: Default::default(),
            spatial_resolution: SpatialResolution::one(),
        };

        let cache_query_entry = VectorCacheQueryEntry {
            query: cache_entry_bounds,
            elements: CachedFeatures::MultiPoint(Arc::new(cols)),
        };

        // query is equal
        let query = cache_entry_bounds;
        assert!(cache_query_entry.query().is_match(&query));

        // query is fully contained
        let query2 = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new_unchecked((2., 2.).into(), (10., 10.).into()),
            time_interval: Default::default(),
            spatial_resolution: SpatialResolution::one(),
        };
        assert!(cache_query_entry.query().is_match(&query2));

        // query is exceeds cached bounds
        let query3 = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new_unchecked((0., 0.).into(), (8., 8.).into()),
            time_interval: Default::default(),
            spatial_resolution: SpatialResolution::one(),
        };
        assert!(!cache_query_entry.query().is_match(&query3));
    }
}
