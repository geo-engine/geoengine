mod in_memory;

#[cfg(feature = "postgres")]
mod postgres;

pub use in_memory::ProInMemoryContext;
#[cfg(feature = "postgres")]
pub use postgres::PostgresContext;
use std::sync::Arc;

use crate::contexts::{Context, Db};
use crate::pro::executor::PlotDescription;
use crate::pro::users::{UserDb, UserSession};
use crate::util::config::get_config_element;
use async_trait::async_trait;
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

#[derive(Clone)]
pub struct TaskManager {
    plot_executor: Arc<Executor<PlotDescription>>,
}

impl TaskManager {
    pub fn plot_executor(&self) -> &Executor<PlotDescription> {
        self.plot_executor.as_ref()
    }
}

impl Default for TaskManager {
    fn default() -> Self {
        let queue_size =
            get_config_element::<crate::util::config::Executor>().map_or(5, |it| it.queue_size);

        TaskManager {
            plot_executor: Arc::new(Executor::new(queue_size)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pro::contexts::ExecutorKey;
    use crate::workflows::workflow::WorkflowId;
    use geoengine_datatypes::primitives::{
        BoundingBox2D, Coordinate2D, QueryRectangle, SpatialResolution, TimeInterval,
    };
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use uuid::Uuid;

    #[test]
    fn test_executor_key_bb_hash_ok() {
        let wf_id = Uuid::default();

        let k1 = ExecutorKey {
            workflow_id: WorkflowId(wf_id),
            query_rectangle: QueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    Coordinate2D::new(0.0, 0.0),
                    Coordinate2D::new(10.0, 10.0),
                )
                .unwrap(),
                time_interval: TimeInterval::new(1, 10).unwrap(),
                spatial_resolution: SpatialResolution::one(),
            },
        };

        let k2 = ExecutorKey {
            workflow_id: WorkflowId(wf_id),
            query_rectangle: QueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    Coordinate2D::new(0.0, 0.0),
                    Coordinate2D::new(10.0, 10.0),
                )
                .unwrap(),
                time_interval: TimeInterval::new(1, 10).unwrap(),
                spatial_resolution: SpatialResolution::one(),
            },
        };

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();

        k1.hash(&mut h1);
        k2.hash(&mut h2);

        assert_eq!(h1.finish(), h2.finish());
    }

    #[test]
    fn test_executor_key_bb_hash_id() {
        let wf_id = Uuid::default();
        let wf_id2 = Uuid::new_v4();

        let k1 = ExecutorKey {
            workflow_id: WorkflowId(wf_id),
            query_rectangle: QueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    Coordinate2D::new(0.0, 0.0),
                    Coordinate2D::new(10.0, 10.0),
                )
                .unwrap(),
                time_interval: TimeInterval::new(1, 10).unwrap(),
                spatial_resolution: SpatialResolution::one(),
            },
        };

        let k2 = ExecutorKey {
            workflow_id: WorkflowId(wf_id2),
            query_rectangle: QueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    Coordinate2D::new(0.0, 0.0),
                    Coordinate2D::new(10.0, 10.0),
                )
                .unwrap(),
                time_interval: TimeInterval::new(1, 10).unwrap(),
                spatial_resolution: SpatialResolution::one(),
            },
        };

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();

        k1.hash(&mut h1);
        k2.hash(&mut h2);

        assert_ne!(h1.finish(), h2.finish());
    }

    #[test]
    fn test_executor_key_bb_hash_bounds() {
        let wf_id = Uuid::default();

        let k1 = ExecutorKey {
            workflow_id: WorkflowId(wf_id),
            query_rectangle: QueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    Coordinate2D::new(0.1, 0.0),
                    Coordinate2D::new(10.0, 10.0),
                )
                .unwrap(),
                time_interval: TimeInterval::new(1, 10).unwrap(),
                spatial_resolution: SpatialResolution::one(),
            },
        };

        let k2 = ExecutorKey {
            workflow_id: WorkflowId(wf_id),
            query_rectangle: QueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    Coordinate2D::new(0.0, 0.0),
                    Coordinate2D::new(10.0, 10.0),
                )
                .unwrap(),
                time_interval: TimeInterval::new(1, 10).unwrap(),
                spatial_resolution: SpatialResolution::one(),
            },
        };

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();

        k1.hash(&mut h1);
        k2.hash(&mut h2);

        assert_ne!(h1.finish(), h2.finish());
    }

    #[test]
    fn test_executor_key_bb_hash_interval() {
        let wf_id = Uuid::default();

        let k1 = ExecutorKey {
            workflow_id: WorkflowId(wf_id),
            query_rectangle: QueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    Coordinate2D::new(0.0, 0.0),
                    Coordinate2D::new(10.0, 10.0),
                )
                .unwrap(),
                time_interval: TimeInterval::new(1, 11).unwrap(),
                spatial_resolution: SpatialResolution::one(),
            },
        };

        let k2 = ExecutorKey {
            workflow_id: WorkflowId(wf_id),
            query_rectangle: QueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    Coordinate2D::new(0.0, 0.0),
                    Coordinate2D::new(10.0, 10.0),
                )
                .unwrap(),
                time_interval: TimeInterval::new(1, 10).unwrap(),
                spatial_resolution: SpatialResolution::one(),
            },
        };

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();

        k1.hash(&mut h1);
        k2.hash(&mut h2);

        assert_ne!(h1.finish(), h2.finish());
    }

    #[test]
    fn test_executor_key_bb_hash_res() {
        let wf_id = Uuid::default();

        let k1 = ExecutorKey {
            workflow_id: WorkflowId(wf_id),
            query_rectangle: QueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    Coordinate2D::new(0.0, 0.0),
                    Coordinate2D::new(10.0, 10.0),
                )
                .unwrap(),
                time_interval: TimeInterval::new(1, 10).unwrap(),
                spatial_resolution: SpatialResolution::one(),
            },
        };

        let k2 = ExecutorKey {
            workflow_id: WorkflowId(wf_id),
            query_rectangle: QueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    Coordinate2D::new(0.0, 0.0),
                    Coordinate2D::new(10.0, 10.0),
                )
                .unwrap(),
                time_interval: TimeInterval::new(1, 10).unwrap(),
                spatial_resolution: SpatialResolution::zero_point_one(),
            },
        };

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();

        k1.hash(&mut h1);
        k2.hash(&mut h2);

        assert_ne!(h1.finish(), h2.finish());
    }
}
