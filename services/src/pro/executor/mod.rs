use crate::handlers::plots::WrappedPlotOutput;
use crate::workflows::workflow::WorkflowId;
use float_cmp::approx_eq;
use geo::prelude::Intersects;
use geoengine_datatypes::collections::{
    FeatureCollection, FeatureCollectionInfos, FeatureCollectionModifications,
};
use geoengine_datatypes::primitives::{
    BoundingBox2D, Geometry, MultiLineString, MultiPoint, MultiPolygon, NoGeometry,
    SpatialPartition2D, SpatialPartitioned, TemporalBounded, TimeInterval,
};
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use geoengine_datatypes::util::arrow::ArrowTyped;
use geoengine_operators::pro::executor::ExecutorTaskDescription;
use std::marker::PhantomData;

/// A [description][ExecutorTaskDescription] of an [`Executor`][geoengine_operators::pro::executor::Executor] task
/// for plot data.
#[derive(Clone, Debug)]
pub struct PlotDescription {
    pub id: WorkflowId,
    pub spatial_bounds: BoundingBox2D,
    pub temporal_bounds: TimeInterval,
}

impl PlotDescription {
    pub fn new(
        id: WorkflowId,
        spatial_bounds: BoundingBox2D,
        temporal_bounds: TimeInterval,
    ) -> PlotDescription {
        PlotDescription {
            id,
            spatial_bounds,
            temporal_bounds,
        }
    }
}

impl ExecutorTaskDescription for PlotDescription {
    type KeyType = WorkflowId;
    type ResultType = crate::error::Result<WrappedPlotOutput>;

    fn primary_key(&self) -> &Self::KeyType {
        &self.id
    }

    fn can_join(&self, other: &Self) -> bool {
        self.temporal_bounds == other.temporal_bounds
            && approx_eq!(BoundingBox2D, self.spatial_bounds, other.spatial_bounds)
    }

    fn slice_result(&self, result: &Self::ResultType) -> Option<Self::ResultType> {
        Some(match result {
            Ok(wpo) => Ok(wpo.clone()),
            Err(_e) => Err(crate::error::Error::NotYetImplemented),
        })
    }
}

/// A trait for spatio-temporal filterable [feature collections][FeatureCollection].
pub trait STFilterable<G> {
    /// Filters out features that do not intersect the given spatio-temporal bounds
    /// and returns the resulting [`FeatureCollection`].
    fn apply_filter(
        self,
        t: &TimeInterval,
        s: &BoundingBox2D,
    ) -> geoengine_datatypes::util::Result<FeatureCollection<G>>;
}

impl<'a, G> STFilterable<G> for &'a FeatureCollection<G>
where
    G: Geometry + ArrowTyped,
    Self: IntoIterator,
    <Self as IntoIterator>::Item: Intersects<BoundingBox2D> + Intersects<TimeInterval>,
{
    fn apply_filter(
        self,
        t: &TimeInterval,
        s: &BoundingBox2D,
    ) -> geoengine_datatypes::util::Result<FeatureCollection<G>> {
        let mask = self
            .into_iter()
            .map(|x| x.intersects(t) && x.intersects(s))
            .collect::<Vec<_>>();
        self.filter(mask)
    }
}

pub type DataDescription = FeatureCollectionTaskDescription<NoGeometry>;
pub type MultiPointDescription = FeatureCollectionTaskDescription<MultiPoint>;
pub type MultiLinestringDescription = FeatureCollectionTaskDescription<MultiLineString>;
pub type MultiPolygonDescription = FeatureCollectionTaskDescription<MultiPolygon>;

/// A [description][ExecutorTaskDescription] of an [`Executor`][geoengine_operators::pro::executor::Executor] task
/// for feature data.
#[derive(Clone, Debug)]
pub struct FeatureCollectionTaskDescription<G> {
    pub id: WorkflowId,
    pub spatial_bounds: BoundingBox2D,
    pub temporal_bounds: TimeInterval,
    _p: PhantomData<G>,
}

impl<G> FeatureCollectionTaskDescription<G> {
    pub fn new(
        id: WorkflowId,
        spatial_bounds: BoundingBox2D,
        temporal_bounds: TimeInterval,
    ) -> Self {
        Self {
            id,
            spatial_bounds,
            temporal_bounds,
            _p: Default::default(),
        }
    }
}

impl<G> ExecutorTaskDescription for FeatureCollectionTaskDescription<G>
where
    G: Geometry + ArrowTyped + 'static,
    for<'a> &'a FeatureCollection<G>: STFilterable<G>,
{
    type KeyType = WorkflowId;
    type ResultType = geoengine_operators::util::Result<FeatureCollection<G>>;

    fn primary_key(&self) -> &Self::KeyType {
        &self.id
    }

    fn can_join(&self, other: &Self) -> bool {
        other.temporal_bounds.contains(&self.temporal_bounds) && !G::IS_GEOMETRY
            || other.spatial_bounds.contains_bbox(&self.spatial_bounds)
    }

    fn slice_result(&self, result: &Self::ResultType) -> Option<Self::ResultType> {
        match result {
            Ok(collection) => {
                let filtered = collection
                    .apply_filter(&self.temporal_bounds, &self.spatial_bounds)
                    .map_err(geoengine_operators::error::Error::from);

                match filtered {
                    Ok(f) if f.is_empty() => None,
                    Ok(f) => Some(Ok(f)),
                    Err(e) => Some(Err(e)),
                }
            }
            Err(_e) => Some(Err(geoengine_operators::error::Error::NotYetImplemented)),
        }
    }
}

/// A [description][ExecutorTaskDescription] of an [`geoengine_operators::pro::executor::Executor`] task
/// for raster data.
#[derive(Clone, Debug)]
pub struct RasterTaskDescription<P> {
    pub id: WorkflowId,
    pub spatial_bounds: SpatialPartition2D,
    pub temporal_bounds: TimeInterval,
    _p: PhantomData<P>,
}

impl<P> RasterTaskDescription<P> {
    pub fn new(
        id: WorkflowId,
        spatial_bounds: SpatialPartition2D,
        temporal_bounds: TimeInterval,
    ) -> Self {
        Self {
            id,
            spatial_bounds,
            temporal_bounds,
            _p: Default::default(),
        }
    }
}

impl<P> ExecutorTaskDescription for RasterTaskDescription<P>
where
    P: Pixel,
{
    type KeyType = WorkflowId;
    type ResultType = geoengine_operators::util::Result<RasterTile2D<P>>;

    fn primary_key(&self) -> &Self::KeyType {
        &self.id
    }

    fn can_join(&self, other: &Self) -> bool {
        other.temporal_bounds.contains(&self.temporal_bounds)
            && other.spatial_bounds.contains(&self.spatial_bounds)
    }

    fn slice_result(&self, result: &Self::ResultType) -> Option<Self::ResultType> {
        match result {
            Ok(r) => {
                if r.temporal_bounds().intersects(&self.temporal_bounds)
                    && r.spatial_partition().intersects(&self.spatial_bounds)
                {
                    Some(Ok(r.clone()))
                } else {
                    None
                }
            }
            Err(_e) => Some(Err(geoengine_operators::error::Error::NotYetImplemented)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pro::executor::MultiPointDescription;
    use crate::util::Identifier;
    use crate::workflows::workflow::WorkflowId;
    use geoengine_datatypes::collections::{FeatureCollectionInfos, MultiPointCollection};
    use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D, MultiPoint, TimeInterval};
    use geoengine_operators::pro::executor::ExecutorTaskDescription;
    use std::collections::HashMap;

    #[test]
    fn test() {
        let id = WorkflowId::new();

        let pd1 = MultiPointDescription::new(
            id,
            BoundingBox2D::new(Coordinate2D::new(0.0, 0.0), Coordinate2D::new(10.0, 10.0)).unwrap(),
            TimeInterval::new(0, 10).unwrap(),
        );

        let pd2 = MultiPointDescription::new(
            id,
            BoundingBox2D::new(Coordinate2D::new(4.0, 4.0), Coordinate2D::new(6.0, 6.0)).unwrap(),
            TimeInterval::new(4, 6).unwrap(),
        );

        let collection = MultiPointCollection::from_data(
            vec![
                MultiPoint::new(vec![Coordinate2D::new(1.0, 1.0)]).unwrap(),
                MultiPoint::new(vec![Coordinate2D::new(5.0, 5.0)]).unwrap(),
                MultiPoint::new(vec![Coordinate2D::new(5.0, 5.0)]).unwrap(),
            ],
            vec![
                TimeInterval::new(0, 10).unwrap(),
                TimeInterval::new(2, 3).unwrap(),
                TimeInterval::new(3, 8).unwrap(),
            ],
            HashMap::new(),
        )
        .unwrap();

        assert!(pd2.can_join(&pd1));
        assert!(!pd1.can_join(&pd2));

        let sliced = pd2.slice_result(&Ok(collection)).unwrap().unwrap();

        assert_eq!(1, sliced.len());
    }
}
