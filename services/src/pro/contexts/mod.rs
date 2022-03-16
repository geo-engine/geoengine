mod in_memory;

#[cfg(feature = "postgres")]
mod postgres;

pub use in_memory::ProInMemoryContext;
#[cfg(feature = "postgres")]
pub use postgres::PostgresContext;
use std::sync::Arc;
use std::time::Duration;

use crate::contexts::{Context, Db};
use crate::pro::executor::scheduler::TaskScheduler;
use crate::pro::executor::{
    DataDescription, FeatureCollectionTaskDescription, MultiLinestringDescription,
    MultiPointDescription, MultiPolygonDescription, PlotDescription, RasterTaskDescription,
    STFilterable,
};
use crate::pro::users::{UserDb, UserSession};
use crate::util::config::get_config_element;
use async_trait::async_trait;
use geoengine_datatypes::collections::FeatureCollection;
use geoengine_datatypes::primitives::{
    Geometry, MultiLineString, MultiPoint, MultiPolygon, NoGeometry,
};
use geoengine_datatypes::raster::Pixel;
use geoengine_datatypes::util::arrow::ArrowTyped;
use geoengine_operators::pro::executor::Executor;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

/// A pro contexts that extends the default context.
// TODO: avoid locking the individual DBs here IF they are already thread safe (e.g. guaranteed by postgres)
#[async_trait]
pub trait ProContext: Context<Session = UserSession> {
    type UserDB: UserDb;

    fn user_db(&self) -> Db<Self::UserDB>;
    async fn user_db_ref(&self) -> RwLockReadGuard<Self::UserDB>;
    async fn user_db_ref_mut(&self) -> RwLockWriteGuard<Self::UserDB>;
    fn task_manager(&self) -> TaskManager;
}

/// The `TaskManager` provides access to [Executors][Executor] for all available
/// result types.
/// It uses an [Arc] internally, so cloning is cheap and there is no
/// need to wrap it by yourself.  
#[derive(Clone)]
pub struct TaskManager {
    executors: Arc<Executors>,
}

/// Holds all executors provided by the [`TaskManager`].
struct Executors {
    plot_executor: Executor<PlotDescription>,
    vector_executors: FeatureExecutors,
    raster_executors: RasterExecutors,
}

/// Trait for retrieving an [`Executor`] instance for feature data.
pub trait GetFeatureExecutor<G>
where
    G: Geometry + ArrowTyped + 'static,
    for<'a> &'a FeatureCollection<G>: STFilterable<G>,
{
    /// Retrieves [Executor]
    fn get_feature_executor(&self) -> &Executor<FeatureCollectionTaskDescription<G>>;
}

/// Summarizes all [`Executors`][Executor] for feature data.
struct FeatureExecutors {
    data: Executor<DataDescription>,
    point: Executor<MultiPointDescription>,
    line: Executor<MultiLinestringDescription>,
    polygon: Executor<MultiPolygonDescription>,
}

impl FeatureExecutors {
    /// Creates a new instance with each [`Executor`] having the given `queue_size`.
    fn new(queue_size: usize) -> Self {
        Self {
            data: Executor::new(queue_size),
            point: Executor::new(queue_size),
            line: Executor::new(queue_size),
            polygon: Executor::new(queue_size),
        }
    }
}

/// Trait for retrieving an [`Executor`] instance for raster data.
pub trait GetRasterExecutor<P: Pixel> {
    /// Retrieves [`Executor`]
    fn get_raster_executor(&self) -> &Executor<RasterTaskDescription<P>>;
}

/// Trait for retrieving an [`TaskScheduler`] instance for raster data.
pub trait GetRasterScheduler<P: Pixel> {
    /// Retrieves [`TaskScheduler`]
    fn get_raster_scheduler(&self) -> &TaskScheduler<RasterTaskDescription<P>>;
}

/// Summarizes all [`Executors`][Executor] for feature data.
struct RasterExecutors {
    // ru8: Executor<RasterTaskDescription<u8>>,
    // ru16: Executor<RasterTaskDescription<u16>>,
    // ru32: Executor<RasterTaskDescription<u32>>,
    // ru64: Executor<RasterTaskDescription<u64>>,
    // ri8: Executor<RasterTaskDescription<i8>>,
    // ri16: Executor<RasterTaskDescription<i16>>,
    // ri32: Executor<RasterTaskDescription<i32>>,
    // ri64: Executor<RasterTaskDescription<i64>>,
    // rf32: Executor<RasterTaskDescription<f32>>,
    // rf64: Executor<RasterTaskDescription<f64>>,
    ru8: TaskScheduler<RasterTaskDescription<u8>>,
    ru16: TaskScheduler<RasterTaskDescription<u16>>,
    ru32: TaskScheduler<RasterTaskDescription<u32>>,
    ru64: TaskScheduler<RasterTaskDescription<u64>>,
    ri8: TaskScheduler<RasterTaskDescription<i8>>,
    ri16: TaskScheduler<RasterTaskDescription<i16>>,
    ri32: TaskScheduler<RasterTaskDescription<i32>>,
    ri64: TaskScheduler<RasterTaskDescription<i64>>,
    rf32: TaskScheduler<RasterTaskDescription<f32>>,
    rf64: TaskScheduler<RasterTaskDescription<f64>>,
}

impl RasterExecutors {
    /// Creates a new instance with each [`Executor`] having the given `queue_size`.
    fn new(queue_size: usize, merge_dead_space_threshold: f64, timeout: Duration) -> Self {
        Self {
            // ru8: Executor::new(queue_size),
            // ru16: Executor::new(queue_size),
            // ru32: Executor::new(queue_size),
            // ru64: Executor::new(queue_size),
            // ri8: Executor::new(queue_size),
            // ri16: Executor::new(queue_size),
            // ri32: Executor::new(queue_size),
            // ri64: Executor::new(queue_size),
            // rf32: Executor::new(queue_size),
            // rf64: Executor::new(queue_size),
            ru8: TaskScheduler::new(queue_size, merge_dead_space_threshold, timeout),
            ru16: TaskScheduler::new(queue_size, merge_dead_space_threshold, timeout),
            ru32: TaskScheduler::new(queue_size, merge_dead_space_threshold, timeout),
            ru64: TaskScheduler::new(queue_size, merge_dead_space_threshold, timeout),
            ri8: TaskScheduler::new(queue_size, merge_dead_space_threshold, timeout),
            ri16: TaskScheduler::new(queue_size, merge_dead_space_threshold, timeout),
            ri32: TaskScheduler::new(queue_size, merge_dead_space_threshold, timeout),
            ri64: TaskScheduler::new(queue_size, merge_dead_space_threshold, timeout),
            rf32: TaskScheduler::new(queue_size, merge_dead_space_threshold, timeout),
            rf64: TaskScheduler::new(queue_size, merge_dead_space_threshold, timeout),
        }
    }
}

impl TaskManager {
    pub fn plot_executor(&self) -> &Executor<PlotDescription> {
        &self.executors.plot_executor
    }
}

impl GetFeatureExecutor<NoGeometry> for TaskManager {
    fn get_feature_executor(&self) -> &Executor<FeatureCollectionTaskDescription<NoGeometry>> {
        &self.executors.vector_executors.data
    }
}

impl GetFeatureExecutor<MultiPoint> for TaskManager {
    fn get_feature_executor(&self) -> &Executor<FeatureCollectionTaskDescription<MultiPoint>> {
        &self.executors.vector_executors.point
    }
}

impl GetFeatureExecutor<MultiLineString> for TaskManager {
    fn get_feature_executor(&self) -> &Executor<FeatureCollectionTaskDescription<MultiLineString>> {
        &self.executors.vector_executors.line
    }
}

impl GetFeatureExecutor<MultiPolygon> for TaskManager {
    fn get_feature_executor(&self) -> &Executor<FeatureCollectionTaskDescription<MultiPolygon>> {
        &self.executors.vector_executors.polygon
    }
}

impl GetRasterExecutor<u8> for TaskManager {
    fn get_raster_executor(&self) -> &Executor<RasterTaskDescription<u8>> {
        &self.executors.raster_executors.ru8.executor()
    }
}

impl GetRasterExecutor<u16> for TaskManager {
    fn get_raster_executor(&self) -> &Executor<RasterTaskDescription<u16>> {
        &self.executors.raster_executors.ru16.executor()
    }
}

impl GetRasterExecutor<u32> for TaskManager {
    fn get_raster_executor(&self) -> &Executor<RasterTaskDescription<u32>> {
        &self.executors.raster_executors.ru32.executor()
    }
}

impl GetRasterExecutor<u64> for TaskManager {
    fn get_raster_executor(&self) -> &Executor<RasterTaskDescription<u64>> {
        &self.executors.raster_executors.ru64.executor()
    }
}

impl GetRasterExecutor<i8> for TaskManager {
    fn get_raster_executor(&self) -> &Executor<RasterTaskDescription<i8>> {
        &self.executors.raster_executors.ri8.executor()
    }
}

impl GetRasterExecutor<i16> for TaskManager {
    fn get_raster_executor(&self) -> &Executor<RasterTaskDescription<i16>> {
        &self.executors.raster_executors.ri16.executor()
    }
}

impl GetRasterExecutor<i32> for TaskManager {
    fn get_raster_executor(&self) -> &Executor<RasterTaskDescription<i32>> {
        &self.executors.raster_executors.ri32.executor()
    }
}

impl GetRasterExecutor<i64> for TaskManager {
    fn get_raster_executor(&self) -> &Executor<RasterTaskDescription<i64>> {
        &self.executors.raster_executors.ri64.executor()
    }
}

impl GetRasterExecutor<f32> for TaskManager {
    fn get_raster_executor(&self) -> &Executor<RasterTaskDescription<f32>> {
        &self.executors.raster_executors.rf32.executor()
    }
}

impl GetRasterExecutor<f64> for TaskManager {
    fn get_raster_executor(&self) -> &Executor<RasterTaskDescription<f64>> {
        &self.executors.raster_executors.rf64.executor()
    }
}

impl GetRasterScheduler<u8> for TaskManager {
    fn get_raster_scheduler(&self) -> &TaskScheduler<RasterTaskDescription<u8>> {
        &self.executors.raster_executors.ru8
    }
}

impl GetRasterScheduler<u16> for TaskManager {
    fn get_raster_scheduler(&self) -> &TaskScheduler<RasterTaskDescription<u16>> {
        &self.executors.raster_executors.ru16
    }
}

impl GetRasterScheduler<u32> for TaskManager {
    fn get_raster_scheduler(&self) -> &TaskScheduler<RasterTaskDescription<u32>> {
        &self.executors.raster_executors.ru32
    }
}

impl GetRasterScheduler<u64> for TaskManager {
    fn get_raster_scheduler(&self) -> &TaskScheduler<RasterTaskDescription<u64>> {
        &self.executors.raster_executors.ru64
    }
}

impl GetRasterScheduler<i8> for TaskManager {
    fn get_raster_scheduler(&self) -> &TaskScheduler<RasterTaskDescription<i8>> {
        &self.executors.raster_executors.ri8
    }
}

impl GetRasterScheduler<i16> for TaskManager {
    fn get_raster_scheduler(&self) -> &TaskScheduler<RasterTaskDescription<i16>> {
        &self.executors.raster_executors.ri16
    }
}

impl GetRasterScheduler<i32> for TaskManager {
    fn get_raster_scheduler(&self) -> &TaskScheduler<RasterTaskDescription<i32>> {
        &self.executors.raster_executors.ri32
    }
}

impl GetRasterScheduler<i64> for TaskManager {
    fn get_raster_scheduler(&self) -> &TaskScheduler<RasterTaskDescription<i64>> {
        &self.executors.raster_executors.ri64
    }
}

impl GetRasterScheduler<f32> for TaskManager {
    fn get_raster_scheduler(&self) -> &TaskScheduler<RasterTaskDescription<f32>> {
        &self.executors.raster_executors.rf32
    }
}

impl GetRasterScheduler<f64> for TaskManager {
    fn get_raster_scheduler(&self) -> &TaskScheduler<RasterTaskDescription<f64>> {
        &self.executors.raster_executors.rf64
    }
}

impl Default for TaskManager {
    fn default() -> Self {
        let queue_size =
            get_config_element::<crate::util::config::Executor>().map_or(5, |it| it.queue_size);
        let timeout = Duration::from_millis(
            get_config_element::<crate::util::config::Executor>()
                .map_or(100, |it| it.scheduler_timeout_ms),
        );
        let threshold = get_config_element::<crate::util::config::Executor>()
            .map_or(0.01, |it| it.scheduler_merge_threshold);

        TaskManager {
            executors: Arc::new(Executors {
                plot_executor: Executor::new(queue_size),
                vector_executors: FeatureExecutors::new(queue_size),
                raster_executors: RasterExecutors::new(queue_size, threshold, timeout),
            }),
        }
    }
}
