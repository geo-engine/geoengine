use futures_util::future::BoxFuture;
use futures_util::stream::{BoxStream, FuturesUnordered};
use futures_util::StreamExt;
use geoengine_operators::pro::executor::error::Result;
use geoengine_operators::pro::executor::{Executor, ExecutorTaskDescription, StreamReceiver};
use log::{debug, error, warn};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::vec;
use tokio::sync::oneshot::{channel, Sender};
use tokio::task::JoinHandle;

/// An extension of the `ExecutorTaskDescription` that allows
/// to merge tasks and schedule the resulting query
#[async_trait::async_trait]
pub trait MergableTaskDescription: ExecutorTaskDescription {
    /// Merges two descriptions
    #[must_use]
    fn merge(&self, other: &Self) -> Self;

    /// Calculates the dead space that would result when merging the
    /// two given descriptions.
    fn merge_dead_space(&self, other: &Self) -> f64;

    /// Executes the query
    fn execute(
        &self,
    ) -> BoxFuture<
        'static,
        geoengine_operators::pro::executor::error::Result<BoxStream<'static, Self::ResultType>>,
    >;
}

type TaskMap<Desc> = HashMap<<Desc as ExecutorTaskDescription>::KeyType, Vec<ScheduledTask<Desc>>>;

type SharedTaskMap<Desc> = Arc<Mutex<TaskMap<Desc>>>;

type ShutdownFlag = Arc<AtomicBool>;

/// An enum representing either a single task,
/// or a set of merged tasks
enum ScheduledTask<Desc: MergableTaskDescription> {
    Single(TaskEntry<Desc>),
    Merged(MergedTasks<Desc>),
}

impl<Desc: MergableTaskDescription> ScheduledTask<Desc> {
    /// Calculates the resulting dead space when merging this task with the given one.
    fn merge_dead_space(&self, other: &Self) -> f64 {
        match (self, other) {
            (Self::Merged(l), Self::Merged(r)) => l.description.merge_dead_space(&r.description),
            (Self::Merged(l), Self::Single(r)) => l.description.merge_dead_space(&r.description),
            (Self::Single(l), Self::Merged(r)) => l.description.merge_dead_space(&r.description),
            (Self::Single(l), Self::Single(r)) => l.description.merge_dead_space(&r.description),
        }
    }

    /// Schedules this task to the executor.
    async fn schedule(self, executor: &Executor<Desc>) -> Result<()> {
        match self {
            Self::Single(t) => {
                debug!("Scheduling single task: {:?}", &t.description);
                let stream_future = t.description.execute();
                if t.response
                    .send(
                        executor
                            .submit_stream_future(t.description, stream_future)
                            .await,
                    )
                    .is_err()
                {
                    warn!("Stream consumer dropped unexpectedly");
                }
            }
            Self::Merged(parent_task) => {
                debug!("Scheduling merged task: {:?}", &parent_task.description);
                let parent_stream_future = parent_task.description.execute();
                // We need to keep this in order to keep the stream alive
                let _pres = executor
                    .submit_stream_future(parent_task.description.clone(), parent_stream_future)
                    .await?;
                for task in parent_task.covered_tasks {
                    debug!(
                        "  Appending task {:?} to {:?}",
                        &task.description, &parent_task.description
                    );
                    let task_future = task.description.execute();
                    if task
                        .response
                        .send(
                            executor
                                .submit_stream_future(task.description, task_future)
                                .await,
                        )
                        .is_err()
                    {
                        warn!("Stream consumer dropped unexpectedly");
                    }
                }
            }
        }
        Ok(())
    }

    /// Merges this task with the given one.
    fn merge(self, other: Self) -> Self {
        let (desc, covered_tasks) = match (self, other) {
            (Self::Merged(l), Self::Merged(mut r)) => {
                let mut covered_tasks = l.covered_tasks;
                covered_tasks.append(&mut r.covered_tasks);
                let desc = l.description.merge(&r.description);
                (desc, covered_tasks)
            }
            (Self::Merged(l), Self::Single(r)) => {
                let desc = l.description.merge(&r.description);
                let mut covered_tasks = l.covered_tasks;
                covered_tasks.push(r);
                (desc, covered_tasks)
            }
            (Self::Single(l), Self::Merged(r)) => {
                let desc = r.description.merge(&l.description);
                let mut covered_tasks = r.covered_tasks;
                covered_tasks.push(l);
                (desc, covered_tasks)
            }
            (Self::Single(l), Self::Single(r)) => (l.description.merge(&r.description), vec![l, r]),
        };
        Self::Merged(MergedTasks {
            description: desc,
            covered_tasks,
        })
    }
}

impl<Desc: MergableTaskDescription> From<TaskEntry<Desc>> for ScheduledTask<Desc> {
    fn from(v: TaskEntry<Desc>) -> Self {
        Self::Single(v)
    }
}

/// Represents a set of merged tasks.
/// The `description` covers the bounds of all merged tasks,
/// `covered_tasks` contains all original tasks that were merged into this one.
struct MergedTasks<Desc: MergableTaskDescription> {
    description: Desc,
    covered_tasks: Vec<TaskEntry<Desc>>,
}

/// A task send to the scheduler, `response` is used
/// to return the result to the requesting task.
struct TaskEntry<Desc: MergableTaskDescription> {
    description: Desc,
    response: Sender<Result<StreamReceiver<Desc>>>,
}

/// The looper ticks with a fixed interval and collects tasks.
/// Every tick, all collected tasks are merged as far as possible
/// and scheduled to the underlying executor.
///
/// Tasks are merged, if the occurring dead space is below `merge_dead_space_threshold`.
struct MergeLooper<Desc>
where
    Desc: MergableTaskDescription,
{
    timeout: Duration,
    executor: Arc<Executor<Desc>>,
    tasks: SharedTaskMap<Desc>,
    merge_dead_space_threshold: f64,
    shutdown: ShutdownFlag,
}

impl<Desc> MergeLooper<Desc>
where
    Desc: MergableTaskDescription,
{
    pub async fn main_loop(&mut self) {
        log::info!("Starting merger loop.");
        loop {
            tokio::time::sleep(self.timeout).await;

            // Check shutown
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            {
                let new_tasks = {
                    let mut tasks = geoengine_operators::util::safe_lock_mutex(&self.tasks);
                    tasks.drain().collect::<TaskMap<Desc>>()
                };

                if !new_tasks.is_empty() {
                    let executor = self.executor.clone();
                    let threshold = self.merge_dead_space_threshold;

                    tokio::spawn(async move {
                        debug!("Scheduling tasks.");
                        Self::schedule(executor, new_tasks, threshold).await;
                        debug!("Finished scheduling tasks.");
                    });
                }
            }
        }
        log::info!("Finished merger loop.");
    }

    /// Schedules the given set of tasks. Merging of tasks is performed, if
    /// the occurring dead space is below the given threshold.
    async fn schedule(executor: Arc<Executor<Desc>>, tasks: TaskMap<Desc>, threshold: f64) {
        let merged_tasks = tokio::task::spawn_blocking(move || Self::merge_tasks(tasks, threshold))
            .await
            .expect("Task merging must complete.");

        let futures = merged_tasks
            .into_values()
            .flat_map(std::iter::IntoIterator::into_iter)
            .map(|task| task.schedule(executor.as_ref()))
            .collect::<FuturesUnordered<_>>();

        for res in futures.collect::<Vec<_>>().await {
            if let Err(e) = res {
                error!("Failed to schedule tasks: {:?}", e);
            }
        }
    }

    /// Merges the tasks with the given threshold
    fn merge_tasks(tasks: TaskMap<Desc>, threshold: f64) -> TaskMap<Desc> {
        tasks
            .into_iter()
            .map(|(k, v)| {
                debug!("Merging {} tasks for key {:?}", v.len(), &k);
                let merged = Self::handle_homogeneous_tasks(v, threshold);
                debug!(
                    "Finished merging tasks for key {:?}. Resulted in {} tasks.",
                    &k,
                    merged.len()
                );
                (k, merged)
            })
            .collect::<HashMap<_, _>>()
    }

    /// Merges a set of homogeneous tasks. It performs `single_merge_pass` until
    /// the resulting task list does not change anymore.
    fn handle_homogeneous_tasks(
        mut tasks: Vec<ScheduledTask<Desc>>,
        threshold: f64,
    ) -> Vec<ScheduledTask<Desc>> {
        // Merge the tasks
        loop {
            let old_size = tasks.len();
            tasks = Self::single_merge_pass(tasks, threshold);
            if tasks.len() == old_size {
                break;
            }
        }
        tasks
    }

    /// A single merge pass is a O(n^2) operation on the given task list.
    /// It picks the first task from the list and tries to merge it
    /// with all other tasks.
    /// All tasks that could not be merged remain in the list and are processed
    /// in a subsequent loop. This is repeated until the list unmerged tasks is empty.
    fn single_merge_pass(
        mut tasks: Vec<ScheduledTask<Desc>>,
        threshold: f64,
    ) -> Vec<ScheduledTask<Desc>> {
        let mut merged = Vec::with_capacity(tasks.len());

        while let Some(mut current) = tasks.pop() {
            let mut tmp = Vec::with_capacity(tasks.len());
            for t in tasks {
                if current.merge_dead_space(&t) <= threshold {
                    current = current.merge(t);
                } else {
                    tmp.push(t);
                }
            }
            merged.push(current);
            tasks = tmp;
        }
        merged
    }
}

/// The scheduler ticks with a fixed interval and collects tasks.
/// Every tick, all collected tasks are merged as far as possible
/// and scheduled to the underlying executor.
///
/// Tasks are merged, if the occurring dead space is below `merge_dead_space_threshold`.
pub struct TaskScheduler<Desc>
where
    Desc: MergableTaskDescription,
{
    executor: Arc<Executor<Desc>>,
    tasks: SharedTaskMap<Desc>,
    timeout: Duration,
    _looper_handle: JoinHandle<()>,
    shutdown: ShutdownFlag,
}

impl<Desc> Drop for TaskScheduler<Desc>
where
    Desc: MergableTaskDescription,
{
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

impl<Desc> TaskScheduler<Desc>
where
    Desc: MergableTaskDescription,
{
    /// Creates a new scheduler with the given dead space and timeout.
    pub fn new(
        executor_buffer_size: usize,
        merge_dead_space_threshold: f64,
        timeout: Duration,
    ) -> TaskScheduler<Desc> {
        let tasks = Arc::new(Mutex::new(HashMap::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let executor = Arc::new(Executor::new(executor_buffer_size));

        let mut looper = MergeLooper {
            timeout,
            tasks: tasks.clone(),
            executor: executor.clone(),
            merge_dead_space_threshold,
            shutdown: shutdown.clone(),
        };

        let looper_handle = tokio::spawn(async move { looper.main_loop().await });

        Self {
            executor,
            tasks,
            timeout,
            _looper_handle: looper_handle,
            shutdown,
        }
    }

    /// Returns the backing executor
    pub fn executor(&self) -> &Executor<Desc> {
        self.executor.as_ref()
    }

    /// Directly submits a task to the executor, bypassing scheduling
    pub async fn fastpath(&self, key: Desc) -> Result<StreamReceiver<Desc>> {
        let stream = key.execute().await?;
        self.executor.submit_stream(key, stream).await
    }

    /// Schedules the given task.
    pub async fn schedule_stream(&self, key: Desc) -> Result<StreamReceiver<Desc>> {
        if self.timeout.is_zero() {
            return self.fastpath(key).await;
        }

        let (tx, rx) = channel();

        let task_entry = TaskEntry {
            description: key.clone(),
            response: tx,
        };

        {
            let mut tasks = geoengine_operators::util::safe_lock_mutex(&self.tasks);
            match tasks.entry(key.primary_key().clone()) {
                Entry::Vacant(ve) => {
                    ve.insert(vec![task_entry.into()]);
                }
                Entry::Occupied(mut oe) => {
                    oe.get_mut().push(task_entry.into());
                }
            }
        }
        rx.await?
    }
}
