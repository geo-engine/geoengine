use crate::handlers::plots::WrappedPlotOutput;
use crate::pro::executor::scheduler::MergableTaskDescription;
use crate::workflows::workflow::WorkflowId;
use float_cmp::approx_eq;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use futures_util::FutureExt;
use geo::prelude::Intersects;
use geoengine_datatypes::collections::{
    FeatureCollection, FeatureCollectionInfos, FeatureCollectionModifications,
};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BoundingBox2D, Coordinate2D, Geometry, MultiLineString, MultiPoint,
    MultiPolygon, NoGeometry, RasterQueryRectangle, SpatialPartition2D, SpatialPartitioned,
    TemporalBounded, TimeInstance, TimeInterval, VectorQueryRectangle,
};
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use geoengine_datatypes::util::arrow::ArrowTyped;
use geoengine_operators::engine::{QueryContext, RasterQueryProcessor};
use geoengine_operators::pro::executor::operators::OneshotQueryProcessor;
use geoengine_operators::pro::executor::ExecutorTaskDescription;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;

pub mod scheduler;

fn merge_time_interval(l: &TimeInterval, r: &TimeInterval) -> TimeInterval {
    TimeInterval::new_unchecked(
        TimeInstance::min(l.start(), r.start()),
        TimeInstance::max(l.end(), r.end()),
    )
}

fn merge_rect<T: AxisAlignedRectangle>(l: &T, r: &T) -> (Coordinate2D, Coordinate2D) {
    let ll = Coordinate2D::new(
        f64_min(l.lower_left().x, r.lower_left().x),
        f64_min(l.lower_left().y, r.lower_left().y),
    );

    let ur = Coordinate2D::new(
        f64_max(l.upper_right().x, r.upper_right().x),
        f64_max(l.upper_right().y, r.upper_right().y),
    );

    (ll, ur)
}

fn f64_min(l: f64, r: f64) -> f64 {
    if l < r {
        l
    } else {
        r
    }
}

fn f64_max(l: f64, r: f64) -> f64 {
    if l > r {
        l
    } else {
        r
    }
}

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

#[async_trait::async_trait]
impl ExecutorTaskDescription for PlotDescription {
    type KeyType = WorkflowId;
    type ResultType = crate::error::Result<WrappedPlotOutput>;

    fn primary_key(&self) -> &Self::KeyType {
        &self.id
    }

    fn is_contained_in(&self, other: &Self) -> bool {
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
    pub query_rectangle: VectorQueryRectangle,
    _p: PhantomData<G>,
}

impl<G> FeatureCollectionTaskDescription<G> {
    pub fn new(id: WorkflowId, query_rectangle: VectorQueryRectangle) -> Self {
        Self {
            id,
            query_rectangle,
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

    fn is_contained_in(&self, other: &Self) -> bool {
        other
            .query_rectangle
            .time_interval
            .contains(&self.query_rectangle.time_interval)
            && (!G::IS_GEOMETRY
                || other
                    .query_rectangle
                    .spatial_bounds
                    .contains_bbox(&self.query_rectangle.spatial_bounds))
    }

    fn slice_result(&self, result: &Self::ResultType) -> Option<Self::ResultType> {
        match result {
            Ok(collection) => {
                let filtered = collection
                    .apply_filter(
                        &self.query_rectangle.time_interval,
                        &self.query_rectangle.spatial_bounds,
                    )
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

#[async_trait::async_trait]
impl<G> MergableTaskDescription for FeatureCollectionTaskDescription<G>
where
    G: Geometry + ArrowTyped + 'static,
    for<'a> &'a FeatureCollection<G>: STFilterable<G>,
{
    fn merge(&self, other: &Self) -> Self {
        let merged_t = merge_time_interval(
            &self.query_rectangle.time_interval,
            &other.query_rectangle.time_interval,
        );
        let (ll, ur) = merge_rect(
            &self.query_rectangle.spatial_bounds,
            &other.query_rectangle.spatial_bounds,
        );

        Self {
            id: self.id,
            query_rectangle: VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new_unchecked(ll, ur),
                time_interval: merged_t,
                spatial_resolution: self.query_rectangle.spatial_resolution,
            },
            _p: Default::default(),
        }
    }

    fn merge_dead_space(&self, other: &Self) -> f64 {
        let t_sum = (self.query_rectangle.time_interval.duration_ms()
            + 1
            + other.query_rectangle.time_interval.duration_ms()
            + 1) as f64;
        let t_merged = (merge_time_interval(
            &self.query_rectangle.time_interval,
            &other.query_rectangle.time_interval,
        )
        .duration_ms()
            + 1) as f64;

        let (ll, ur) = merge_rect(
            &self.query_rectangle.spatial_bounds,
            &other.query_rectangle.spatial_bounds,
        );
        let merged_rect = BoundingBox2D::new_unchecked(ll, ur);

        let x_merged = merged_rect.size_x();
        let y_merged = merged_rect.size_y();

        let x_sum = self.query_rectangle.spatial_bounds.size_x()
            + other.query_rectangle.spatial_bounds.size_x();
        let y_sum = self.query_rectangle.spatial_bounds.size_y()
            + other.query_rectangle.spatial_bounds.size_y();

        let vol_merged = t_merged * x_merged * y_merged;
        let vol_sum = t_sum * x_sum * y_sum;

        f64_max(0.0, vol_merged / vol_sum - 1.0)
    }

    fn execute(
        &self,
    ) -> BoxFuture<
        'static,
        geoengine_operators::pro::executor::error::Result<BoxStream<'static, Self::ResultType>>,
    > {
        todo!()
    }
}

/// A [description][ExecutorTaskDescription] of an [`geoengine_operators::pro::executor::Executor`] task
/// for raster data.
#[derive(Clone)]
pub struct RasterTaskDescription<P>
where
    P: Pixel,
{
    pub id: WorkflowId,
    pub query_rectangle: RasterQueryRectangle,
    processor: Arc<dyn RasterQueryProcessor<RasterType = P>>,
    context: Arc<dyn QueryContext>,
    _p: PhantomData<P>,
}

impl<P> Debug for RasterTaskDescription<P>
where
    P: Pixel,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RasterTaskDescription[ id={:?}, query_rectangle={:?}]",
            &self.id, &self.query_rectangle
        )
    }
}

impl<P> RasterTaskDescription<P>
where
    P: Pixel,
{
    pub fn new(
        id: WorkflowId,
        query_rectangle: RasterQueryRectangle,
        processor: Arc<dyn RasterQueryProcessor<RasterType = P>>,
        context: Arc<dyn QueryContext>,
    ) -> Self {
        Self {
            id,
            query_rectangle,
            processor: processor,
            context,
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

    fn is_contained_in(&self, other: &Self) -> bool {
        other
            .query_rectangle
            .time_interval
            .contains(&self.query_rectangle.time_interval)
            && other
                .query_rectangle
                .spatial_bounds
                .contains(&self.query_rectangle.spatial_bounds)
            && other.query_rectangle.spatial_resolution == self.query_rectangle.spatial_resolution
    }

    fn slice_result(&self, result: &Self::ResultType) -> Option<Self::ResultType> {
        match result {
            Ok(r) => {
                if r.temporal_bounds()
                    .intersects(&self.query_rectangle.time_interval)
                    && r.spatial_partition()
                        .intersects(&self.query_rectangle.spatial_bounds)
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

#[async_trait::async_trait]
impl<P> MergableTaskDescription for RasterTaskDescription<P>
where
    P: Pixel,
{
    fn merge(&self, other: &Self) -> Self {
        let merged_t = merge_time_interval(
            &self.query_rectangle.time_interval,
            &other.query_rectangle.time_interval,
        );
        let (ll, ur) = merge_rect(
            &self.query_rectangle.spatial_bounds,
            &other.query_rectangle.spatial_bounds,
        );

        Self {
            id: self.id,
            query_rectangle: RasterQueryRectangle {
                spatial_bounds: SpatialPartition2D::new_unchecked(
                    Coordinate2D::new(ll.x, ur.y),
                    Coordinate2D::new(ur.x, ll.y),
                ),
                time_interval: merged_t,
                spatial_resolution: self.query_rectangle.spatial_resolution,
            },
            processor: self.processor.clone(),
            context: self.context.clone(),
            _p: Default::default(),
        }
    }

    fn merge_dead_space(&self, other: &Self) -> f64 {
        let t_sum = (self.query_rectangle.time_interval.duration_ms()
            + 1
            + other.query_rectangle.time_interval.duration_ms()
            + 1) as f64;
        let t_merged = (merge_time_interval(
            &self.query_rectangle.time_interval,
            &other.query_rectangle.time_interval,
        )
        .duration_ms()
            + 1) as f64;

        let (ll, ur) = merge_rect(
            &self.query_rectangle.spatial_bounds,
            &other.query_rectangle.spatial_bounds,
        );
        let merged_rect = BoundingBox2D::new_unchecked(ll, ur);

        let x_merged = merged_rect.size_x();
        let y_merged = merged_rect.size_y();

        let x_sum = self.query_rectangle.spatial_bounds.size_x()
            + other.query_rectangle.spatial_bounds.size_x();
        let y_sum = self.query_rectangle.spatial_bounds.size_y()
            + other.query_rectangle.spatial_bounds.size_y();

        let vol_merged = t_merged * x_merged * y_merged;
        let vol_sum = t_sum * x_sum * y_sum;

        f64_max(0.0, vol_merged / vol_sum - 1.0)
    }

    fn execute(
        &self,
    ) -> BoxFuture<
        'static,
        geoengine_operators::pro::executor::error::Result<BoxStream<'static, Self::ResultType>>,
    > {
        let proc = self.processor.clone();
        let qr = self.query_rectangle;
        let ctx = self.context.clone();

        let stream_future = proc.into_stream(qr, ctx).map(|result| match result {
            Ok(rb) => Ok(Box::pin(rb) as BoxStream<'static, Self::ResultType>),
            Err(e) => Err(geoengine_operators::pro::executor::error::ExecutorError::from(e)),
        });
        Box::pin(stream_future)
    }
}

#[cfg(test)]
mod tests {
    use crate::handlers::plots::WrappedPlotOutput;
    use crate::pro::executor::{
        DataDescription, MultiPointDescription, PlotDescription, RasterTaskDescription,
    };
    use crate::util::Identifier;
    use crate::workflows::workflow::WorkflowId;
    use geoengine_datatypes::collections::{
        DataCollection, FeatureCollectionInfos, MultiPointCollection,
    };
    use geoengine_datatypes::plots::PlotOutputFormat;
    use geoengine_datatypes::primitives::{
        BoundingBox2D, Coordinate2D, MultiPoint, NoGeometry, QueryRectangle, SpatialPartition2D,
        SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::raster::{
        EmptyGrid2D, GeoTransform, GridIdx2D, GridOrEmpty, RasterTile2D,
    };
    use geoengine_operators::pro::executor::ExecutorTaskDescription;
    use std::collections::HashMap;

    #[test]
    fn test_plot() {
        let id = WorkflowId::new();

        let pd1 = PlotDescription::new(
            id,
            BoundingBox2D::new((0.0, 0.0).into(), (10.0, 10.0).into()).unwrap(),
            TimeInterval::new(0, 10).unwrap(),
        );

        let pd2 = PlotDescription::new(
            id,
            BoundingBox2D::new(Coordinate2D::new(4.0, 4.0), Coordinate2D::new(6.0, 6.0)).unwrap(),
            TimeInterval::new(4, 6).unwrap(),
        );

        let pd3 = PlotDescription::new(
            id,
            BoundingBox2D::new(Coordinate2D::new(4.0, 4.0), Coordinate2D::new(6.0, 6.0)).unwrap(),
            TimeInterval::new(0, 10).unwrap(),
        );

        let pd4 = PlotDescription::new(
            id,
            BoundingBox2D::new((0.0, 0.0).into(), (10.0, 10.0).into()).unwrap(),
            TimeInterval::new(4, 6).unwrap(),
        );

        let result = Ok(WrappedPlotOutput {
            output_format: PlotOutputFormat::JsonPlain,
            plot_type: "test",
            data: Default::default(),
        });

        assert!(pd1.is_contained_in(&pd1));
        assert!(!pd1.is_contained_in(&pd2));
        assert!(!pd1.is_contained_in(&pd3));
        assert!(!pd1.is_contained_in(&pd4));
        assert!(!pd2.is_contained_in(&pd1));
        assert!(!pd2.is_contained_in(&pd1));
        assert!(!pd2.is_contained_in(&pd3));
        assert!(!pd2.is_contained_in(&pd4));
        assert!(!pd3.is_contained_in(&pd1));
        assert!(!pd3.is_contained_in(&pd2));
        assert!(!pd3.is_contained_in(&pd4));
        assert!(!pd4.is_contained_in(&pd1));
        assert!(!pd4.is_contained_in(&pd2));
        assert!(!pd4.is_contained_in(&pd3));

        assert_eq!(
            result.as_ref().unwrap(),
            pd1.slice_result(&result).unwrap().as_ref().unwrap()
        );
    }

    #[test]
    fn test_data() {
        let id = WorkflowId::new();

        let pd1 = DataDescription::new(
            id,
            BoundingBox2D::new((0.0, 0.0).into(), (10.0, 10.0).into()).unwrap(),
            TimeInterval::new(0, 10).unwrap(),
        );

        let pd2 = DataDescription::new(
            id,
            BoundingBox2D::new(Coordinate2D::new(4.0, 4.0), Coordinate2D::new(6.0, 6.0)).unwrap(),
            TimeInterval::new(4, 6).unwrap(),
        );

        let pd3 = DataDescription::new(
            id,
            BoundingBox2D::new(Coordinate2D::new(4.0, 4.0), Coordinate2D::new(6.0, 6.0)).unwrap(),
            TimeInterval::new(0, 10).unwrap(),
        );

        let collection = DataCollection::from_data(
            vec![NoGeometry, NoGeometry, NoGeometry],
            vec![
                TimeInterval::new(0, 10).unwrap(),
                TimeInterval::new(2, 3).unwrap(),
                TimeInterval::new(3, 8).unwrap(),
            ],
            HashMap::new(),
        )
        .unwrap();

        assert!(pd2.is_contained_in(&pd1));
        assert!(pd3.is_contained_in(&pd1));
        assert!(pd2.is_contained_in(&pd3));
        assert!(!pd1.is_contained_in(&pd2));
        assert!(!pd3.is_contained_in(&pd2));

        let sliced = pd2.slice_result(&Ok(collection.clone())).unwrap().unwrap();

        assert_eq!(2, sliced.len());

        let sliced = pd3.slice_result(&Ok(collection)).unwrap().unwrap();

        assert_eq!(3, sliced.len());
    }

    #[test]
    fn test_vector() {
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

        let pd3 = MultiPointDescription::new(
            id,
            BoundingBox2D::new(Coordinate2D::new(4.0, 4.0), Coordinate2D::new(6.0, 6.0)).unwrap(),
            TimeInterval::new(0, 10).unwrap(),
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

        assert!(pd2.is_contained_in(&pd1));
        assert!(pd3.is_contained_in(&pd1));
        assert!(pd2.is_contained_in(&pd3));
        assert!(!pd1.is_contained_in(&pd2));
        assert!(!pd1.is_contained_in(&pd3));
        assert!(!pd3.is_contained_in(&pd2));

        let sliced = pd2.slice_result(&Ok(collection.clone())).unwrap().unwrap();

        assert_eq!(1, sliced.len());

        let sliced = pd3.slice_result(&Ok(collection)).unwrap().unwrap();

        assert_eq!(2, sliced.len());
    }

    #[test]
    fn test_raster() {
        let id = WorkflowId::new();

        let pd1 = RasterTaskDescription::<u8>::new(
            id,
            QueryRectangle {
                spatial_bounds: SpatialPartition2D::new_unchecked(
                    Coordinate2D::new(0.0, 10.0),
                    Coordinate2D::new(10.0, 0.0),
                ),
                time_interval: TimeInterval::new_unchecked(0, 10),
                spatial_resolution: SpatialResolution::one(),
            },
        );

        let pd2 = RasterTaskDescription::<u8>::new(
            id,
            SpatialPartition2D::new(Coordinate2D::new(0.0, 10.0), Coordinate2D::new(10.0, 0.0))
                .unwrap(),
            TimeInterval::new(4, 6).unwrap(),
        );

        let pd3 = RasterTaskDescription::<u8>::new(
            id,
            SpatialPartition2D::new(Coordinate2D::new(4.0, 6.0), Coordinate2D::new(6.0, 4.0))
                .unwrap(),
            TimeInterval::new(0, 10).unwrap(),
        );

        assert!(pd2.is_contained_in(&pd1));
        assert!(pd3.is_contained_in(&pd1));
        assert!(!pd2.is_contained_in(&pd3));
        assert!(!pd1.is_contained_in(&pd2));
        assert!(!pd1.is_contained_in(&pd3));
        assert!(!pd3.is_contained_in(&pd2));

        {
            let tile = RasterTile2D::<u8>::new(
                TimeInterval::new(0, 10).unwrap(),
                GridIdx2D::new([0, 0]),
                GeoTransform::new((0., 10.).into(), 1.0, -1.0),
                GridOrEmpty::Empty(EmptyGrid2D::new([10, 10].into(), 0_u8)),
            );
            let res = Ok(tile);

            assert!(pd2.slice_result(&res).is_some());
            assert!(pd3.slice_result(&res).is_some());
        }

        {
            let tile = RasterTile2D::<u8>::new(
                TimeInterval::new(0, 3).unwrap(),
                GridIdx2D::new([0, 0]),
                GeoTransform::new((0., 10.).into(), 1.0, -1.0),
                GridOrEmpty::Empty(EmptyGrid2D::new([10, 10].into(), 0_u8)),
            );
            let res = Ok(tile);

            assert!(pd2.slice_result(&res).is_none());
            assert!(pd3.slice_result(&res).is_some());
        }

        {
            let tile = RasterTile2D::<u8>::new(
                TimeInterval::new(0, 10).unwrap(),
                GridIdx2D::new([0, 0]),
                GeoTransform::new((8., 2.).into(), 1.0, -1.0),
                GridOrEmpty::Empty(EmptyGrid2D::new([10, 10].into(), 0_u8)),
            );
            let res = Ok(tile);

            assert!(pd2.slice_result(&res).is_some());
            assert!(pd3.slice_result(&res).is_none());
        }
    }
}
