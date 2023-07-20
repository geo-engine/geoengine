use std::{pin::Pin, sync::Arc};

use crate::util::Result;
use futures::Stream;
use geoengine_datatypes::{
    collections::{FeatureCollection, FeatureCollectionInfos, GeometryCollection},
    primitives::{
        Geometry, MultiLineString, MultiPoint, MultiPolygon, NoGeometry, VectorQueryRectangle,
    },
};
use pin_project::pin_project;

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
    FeatureCollection<G>: GeometryCollection + FeatureCollectionInfos,
{
    pub fn new(data: Arc<Vec<FeatureCollection<G>>>, query: VectorQueryRectangle) -> Self {
        Self {
            data,
            query,
            idx: 0,
        }
    }

    pub fn count_matching_elements(&self) -> usize
    where
        G: Geometry,
    {
        self.data
            .iter()
            .filter(|t| {
                let Some(feature_collection_bbox) = t.bbox() else {
                    return false;
                };

                let Some(time_bounds) = t.time_bounds() else {
                    return false;
                };

                (feature_collection_bbox == self.query.spatial_bounds
                    || feature_collection_bbox.intersects_bbox(&self.query.spatial_bounds))
                    && (time_bounds == self.query.time_interval
                        || time_bounds.intersects(&self.query.time_interval))
            })
            .count()
    }

    pub fn element_count(&self) -> usize {
        self.data.len()
    }
}

impl<G: Geometry> Stream for CacheChunkStream<G>
where
    FeatureCollection<G>: GeometryCollection + FeatureCollectionInfos,
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
            let Some(chunk_bbox) = chunk.bbox() else {
                continue;
            };

            let Some(time_bounds) = chunk.time_bounds() else {
                continue;
            };

            if (chunk_bbox == query.spatial_bounds
                || chunk_bbox.intersects_bbox(&query.spatial_bounds))
                && (time_bounds == query.time_interval
                    || time_bounds.intersects(&query.time_interval))
            {
                *idx = i + 1;
                return std::task::Poll::Ready(Some(Ok(chunk.clone())));
            }
        }

        std::task::Poll::Ready(None)
    }
}

pub enum TypedCacheChunkStream {
    NoGeometry(CacheChunkStream<NoGeometry>),
    MultiPoint(CacheChunkStream<MultiPoint>),
    MultiLineString(CacheChunkStream<MultiLineString>),
    MultiPolygon(CacheChunkStream<MultiPolygon>),
}
