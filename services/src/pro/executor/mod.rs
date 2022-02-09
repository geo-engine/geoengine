use crate::handlers::plots::WrappedPlotOutput;
use crate::workflows::workflow::WorkflowId;
use float_cmp::approx_eq;
use geoengine_datatypes::collections::{
    FeatureCollection, FeatureCollectionInfos, FeatureCollectionModifications, IntoGeometryIterator,
};
use geoengine_datatypes::primitives::{
    BoundingBox2D, Geometry, MultiLineString, MultiPoint, MultiPolygon, SpatialBounded,
    TimeInterval,
};
use geoengine_datatypes::util::arrow::ArrowTyped;
use geoengine_operators::pro::executor::ExecutorTaskDescription;
use std::marker::PhantomData;

#[derive(Clone, Debug)]
pub struct PlotDescription {
    pub id: WorkflowId,
    pub spatial_bounds: BoundingBox2D,
    pub temporal_bounds: TimeInterval,
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

pub type MultiPointDescription = VectorDescription<MultiPoint>;
pub type MultiLinestringDescription = VectorDescription<MultiLineString>;
pub type MultiPolygonDescription = VectorDescription<MultiPolygon>;

#[derive(Debug)]
pub struct VectorDescription<G> {
    pub id: WorkflowId,
    pub spatial_bounds: BoundingBox2D,
    pub temporal_bounds: TimeInterval,
    _p: PhantomData<G>,
}

impl<G> VectorDescription<G> {
    pub fn new(
        id: WorkflowId,
        spatial_bounds: BoundingBox2D,
        temporal_bounds: TimeInterval,
    ) -> VectorDescription<G> {
        VectorDescription {
            id,
            spatial_bounds,
            temporal_bounds,
            _p: Default::default(),
        }
    }
}

impl<G> Clone for VectorDescription<G> {
    fn clone(&self) -> Self {
        VectorDescription {
            id: self.id,
            spatial_bounds: self.spatial_bounds,
            temporal_bounds: self.temporal_bounds,
            _p: Default::default(),
        }
    }
}

impl<G> ExecutorTaskDescription for VectorDescription<G>
where
    G: Geometry + ArrowTyped + 'static,
    for<'c> FeatureCollection<G>: IntoGeometryIterator<'c>,
    for<'c> <FeatureCollection<G> as IntoGeometryIterator<'c>>::GeometryType: SpatialBounded,
{
    type KeyType = WorkflowId;
    type ResultType = geoengine_operators::util::Result<FeatureCollection<G>>;

    fn primary_key(&self) -> &Self::KeyType {
        &self.id
    }

    fn can_join(&self, other: &Self) -> bool {
        other.temporal_bounds.contains(&self.temporal_bounds)
            && other.spatial_bounds.contains_bbox(&self.spatial_bounds)
    }

    fn slice_result(&self, result: &Self::ResultType) -> Option<Self::ResultType> {
        match result {
            Ok(c) => {
                let mask = c
                    .time_intervals()
                    .iter()
                    .zip(c.geometries())
                    .map(|(t, g)| {
                        self.temporal_bounds.intersects(t)
                            && g.spatial_bounds().intersects_bbox(&self.spatial_bounds)
                    })
                    .collect::<Vec<_>>();

                Some(c.filter(mask).map_err(|e| e.into()))
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
