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
    MultiPolygon, NoGeometry, QueryRectangle, RasterQueryRectangle, SpatialPartition2D,
    SpatialPartitioned, TemporalBounded, TimeInstance, TimeInterval, VectorQueryRectangle,
};
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use geoengine_datatypes::util::arrow::ArrowTyped;
use geoengine_operators::engine::{QueryContext, RasterQueryProcessor, VectorQueryProcessor};
use geoengine_operators::pro::executor::operators::OneshotQueryProcessor;
use geoengine_operators::pro::executor::ExecutorTaskDescription;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub mod scheduler;

/// Merges two time intervals  regardless if they actually intersect.
fn merge_time_interval(l: &TimeInterval, r: &TimeInterval) -> TimeInterval {
    TimeInterval::new_unchecked(
        TimeInstance::min(l.start(), r.start()),
        TimeInstance::max(l.end(), r.end()),
    )
}

/// Merges to rectangles regardless if they actually intersect and returns
/// the lower left and upper right coordinate of the resulting rectangle.
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

/// Calculates the dead space that occurs when merging two query rectangles. A value of 0 indicates
/// that the merge is perfect in the sense that no superfluid calculations occur.
fn merge_dead_space<Rect: AxisAlignedRectangle>(
    l: &QueryRectangle<Rect>,
    r: &QueryRectangle<Rect>,
) -> f64 {
    let t_sum = (l.time_interval.duration_ms() + 1 + r.time_interval.duration_ms() + 1) as f64;
    let t_merged =
        (merge_time_interval(&l.time_interval, &r.time_interval).duration_ms() + 1) as f64;

    let (ll, ur) = merge_rect(&l.spatial_bounds, &r.spatial_bounds);
    let merged_rect = BoundingBox2D::new_unchecked(ll, ur);

    let x_merged = merged_rect.size_x();
    let y_merged = merged_rect.size_y();

    let x_sum = l.spatial_bounds.size_x() + r.spatial_bounds.size_x();
    let y_sum = l.spatial_bounds.size_y() + r.spatial_bounds.size_y();

    let vol_merged = t_merged * x_merged * y_merged;
    let vol_sum = t_sum * x_sum * y_sum;

    f64_max(0.0, vol_merged / vol_sum - 1.0)
}

/// Computes the minimum of two f64 values.
/// This method has no proper handling for `f64::NAN` values.
fn f64_min(l: f64, r: f64) -> f64 {
    if l < r {
        l
    } else {
        r
    }
}

/// Computes the maximum of two f64 values.
/// This method has no proper handling for `f64::NAN` values.
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
#[derive(Clone)]
pub struct FeatureCollectionTaskDescription<G> {
    pub id: WorkflowId,
    pub query_rectangle: VectorQueryRectangle,
    processor: Arc<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
    context: Arc<dyn QueryContext>,
}

impl<G> FeatureCollectionTaskDescription<G> {
    pub fn new<C: QueryContext + Send + 'static>(
        id: WorkflowId,
        query_rectangle: VectorQueryRectangle,
        processor: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
        context: C,
    ) -> Self {
        Self::new_arced(id, query_rectangle, processor.into(), Arc::new(context))
    }

    pub fn new_arced(
        id: WorkflowId,
        query_rectangle: VectorQueryRectangle,
        processor: Arc<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
        context: Arc<dyn QueryContext>,
    ) -> Self {
        Self {
            id,
            query_rectangle,
            processor,
            context,
        }
    }
}

impl<G> Debug for FeatureCollectionTaskDescription<G>
where
    G: Geometry + ArrowTyped + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FeatureCollectionTaskDescription{{id={:?}, query_rectangle={:?}}}",
            &self.id, &self.query_rectangle
        )
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
            processor: self.processor.clone(),
            context: self.context.clone(),
        }
    }

    fn merge_dead_space(&self, other: &Self) -> f64 {
        merge_dead_space(&self.query_rectangle, &other.query_rectangle)
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
}

impl<P> Debug for RasterTaskDescription<P>
where
    P: Pixel,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RasterTaskDescription{{id={:?}, query_rectangle={:?}}}",
            &self.id, &self.query_rectangle
        )
    }
}

impl<P> RasterTaskDescription<P>
where
    P: Pixel,
{
    pub fn new<C: QueryContext + Send + 'static>(
        id: WorkflowId,
        query_rectangle: RasterQueryRectangle,
        processor: Box<dyn RasterQueryProcessor<RasterType = P>>,
        context: C,
    ) -> Self {
        Self::new_arced(id, query_rectangle, processor.into(), Arc::new(context))
    }

    pub fn new_arced(
        id: WorkflowId,
        query_rectangle: RasterQueryRectangle,
        processor: Arc<dyn RasterQueryProcessor<RasterType = P>>,
        context: Arc<dyn QueryContext>,
    ) -> Self {
        Self {
            id,
            query_rectangle,
            processor,
            context,
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
        }
    }

    fn merge_dead_space(&self, other: &Self) -> f64 {
        merge_dead_space(&self.query_rectangle, &other.query_rectangle)
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
        BoundingBox2D, Coordinate2D, Measurement, MultiPoint, NoGeometry, QueryRectangle,
        SpatialPartition2D, SpatialResolution, TimeInterval, VectorQueryRectangle,
    };
    use geoengine_datatypes::raster::{
        EmptyGrid2D, GeoTransform, GridIdx2D, GridOrEmpty, RasterDataType, RasterTile2D,
    };
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::{
        MockExecutionContext, MockQueryContext, RasterOperator, RasterQueryProcessor,
        RasterResultDescriptor, TypedRasterQueryProcessor, TypedVectorQueryProcessor,
        VectorOperator, VectorQueryProcessor,
    };
    use geoengine_operators::mock::{
        MockFeatureCollectionSource, MockRasterSource, MockRasterSourceParams,
    };
    use geoengine_operators::pro::executor::ExecutorTaskDescription;
    use std::collections::HashMap;
    use std::sync::Arc;

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

    #[tokio::test]
    async fn test_data() {
        let id = WorkflowId::new();

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

        let source = MockFeatureCollectionSource::single(collection.clone()).boxed();

        let source = source
            .initialize(&MockExecutionContext::test_default())
            .await
            .unwrap();

        let proc: Arc<dyn VectorQueryProcessor<VectorType = DataCollection>> =
            if let Ok(TypedVectorQueryProcessor::Data(p)) = source.query_processor() {
                p
            } else {
                panic!()
            }
            .into();

        let ctx = Arc::new(MockQueryContext::test_default());

        let pd1 = DataDescription::new_arced(
            id,
            VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new_unchecked(
                    (0.0, 0.0).into(),
                    (10.0, 10.0).into(),
                ),
                time_interval: TimeInterval::new_unchecked(0, 10),
                spatial_resolution: SpatialResolution::one(),
            },
            proc.clone(),
            ctx.clone(),
        );

        let pd2 = DataDescription::new_arced(
            id,
            VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    Coordinate2D::new(4.0, 4.0),
                    Coordinate2D::new(6.0, 6.0),
                )
                .unwrap(),
                time_interval: TimeInterval::new(4, 6).unwrap(),
                spatial_resolution: SpatialResolution::one(),
            },
            proc.clone(),
            ctx.clone(),
        );

        let pd3 = DataDescription::new_arced(
            id,
            VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    Coordinate2D::new(4.0, 4.0),
                    Coordinate2D::new(6.0, 6.0),
                )
                .unwrap(),
                time_interval: TimeInterval::new(0, 10).unwrap(),
                spatial_resolution: SpatialResolution::one(),
            },
            proc,
            ctx,
        );

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

    #[tokio::test]
    async fn test_vector() {
        let id = WorkflowId::new();

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

        let source = MockFeatureCollectionSource::single(collection.clone()).boxed();

        let source = source
            .initialize(&MockExecutionContext::test_default())
            .await
            .unwrap();

        let proc: Arc<dyn VectorQueryProcessor<VectorType = MultiPointCollection>> =
            if let Ok(TypedVectorQueryProcessor::MultiPoint(p)) = source.query_processor() {
                p
            } else {
                panic!()
            }
            .into();

        let ctx = Arc::new(MockQueryContext::test_default());

        let pd1 = MultiPointDescription::new_arced(
            id,
            VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    Coordinate2D::new(0.0, 0.0),
                    Coordinate2D::new(10.0, 10.0),
                )
                .unwrap(),
                time_interval: TimeInterval::new(0, 10).unwrap(),
                spatial_resolution: SpatialResolution::one(),
            },
            proc.clone(),
            ctx.clone(),
        );

        let pd2 = MultiPointDescription::new_arced(
            id,
            VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    Coordinate2D::new(4.0, 4.0),
                    Coordinate2D::new(6.0, 6.0),
                )
                .unwrap(),
                time_interval: TimeInterval::new(4, 6).unwrap(),
                spatial_resolution: SpatialResolution::one(),
            },
            proc.clone(),
            ctx.clone(),
        );

        let pd3 = MultiPointDescription::new_arced(
            id,
            VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    Coordinate2D::new(4.0, 4.0),
                    Coordinate2D::new(6.0, 6.0),
                )
                .unwrap(),
                time_interval: TimeInterval::new(0, 10).unwrap(),
                spatial_resolution: SpatialResolution::one(),
            },
            proc,
            ctx,
        );

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

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_raster() {
        let id = WorkflowId::new();

        let tile = RasterTile2D::<u8>::new(
            TimeInterval::new(0, 10).unwrap(),
            GridIdx2D::new([0, 0]),
            GeoTransform::new((0., 10.).into(), 1.0, -1.0),
            GridOrEmpty::Empty(EmptyGrid2D::new([10, 10].into(), 0_u8)),
        );

        let source = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![tile.clone()],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReferenceOption::Unreferenced,
                    measurement: Measurement::Unitless,
                    no_data_value: None,
                },
            },
        }
        .boxed();

        let source = source
            .initialize(&MockExecutionContext::test_default())
            .await
            .unwrap();

        let proc: Arc<dyn RasterQueryProcessor<RasterType = u8>> =
            if let Ok(TypedRasterQueryProcessor::U8(p)) = source.query_processor() {
                p
            } else {
                panic!()
            }
            .into();

        let ctx = Arc::new(MockQueryContext::test_default());

        let pd1 = RasterTaskDescription::<u8>::new_arced(
            id,
            QueryRectangle {
                spatial_bounds: SpatialPartition2D::new_unchecked(
                    Coordinate2D::new(0.0, 10.0),
                    Coordinate2D::new(10.0, 0.0),
                ),
                time_interval: TimeInterval::new_unchecked(0, 10),
                spatial_resolution: SpatialResolution::one(),
            },
            proc.clone(),
            ctx.clone(),
        );

        let pd2 = RasterTaskDescription::<u8>::new_arced(
            id,
            QueryRectangle {
                spatial_bounds: SpatialPartition2D::new(
                    Coordinate2D::new(0.0, 10.0),
                    Coordinate2D::new(10.0, 0.0),
                )
                .unwrap(),
                time_interval: TimeInterval::new(4, 6).unwrap(),
                spatial_resolution: SpatialResolution::one(),
            },
            proc.clone(),
            ctx.clone(),
        );

        let pd3 = RasterTaskDescription::<u8>::new_arced(
            id,
            QueryRectangle {
                spatial_bounds: SpatialPartition2D::new(
                    Coordinate2D::new(4.0, 6.0),
                    Coordinate2D::new(6.0, 4.0),
                )
                .unwrap(),
                time_interval: TimeInterval::new(0, 10).unwrap(),
                spatial_resolution: SpatialResolution::one(),
            },
            proc,
            ctx,
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
