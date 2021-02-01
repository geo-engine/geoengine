use std::marker::PhantomData;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::{iter, mem};

use crossbeam::atomic::AtomicCell;
use crossbeam::deque::{Injector, Steal, Stealer, Worker};
use crossbeam::sync::{Parker, Unparker, WaitGroup};
use futures::task::{Context, Poll, Waker};
use futures::Future;
use std::pin::Pin;

/// A chunk of work with some metadata
struct Task {
    _group_id: TaskGroupId,
    task_fn: Box<dyn TaskFn>,
}

type TaskGroupId = usize;

pub trait TaskFn: FnOnce() + Send {}
pub trait StaticTaskFn: TaskFn + 'static {}

impl<T> TaskFn for T where T: FnOnce() + Send {}
impl<T> StaticTaskFn for T where T: TaskFn + 'static {}

impl Task {
    /// Create a new task to be executed at some point
    fn new<F>(group_id: TaskGroupId, f: F) -> Self
    where
        F: StaticTaskFn,
    {
        Self {
            _group_id: group_id,
            task_fn: Box::new(f),
        }
    }

    /// Executes the task
    /// TODO: use `FnTraits` once stable
    fn call_once(self) {
        (self.task_fn)()
    }
}

/// A worker thread pool for compute-heavy tasks
///
/// TODO: increase threads to a certain maximum number if current threads don't produce results fast enough?
/// TODO: schedule tasks based on group context?
///
#[derive(Debug)]
pub struct ThreadPool {
    target_thread_count: usize,
    global_queue: Arc<Injector<Task>>,
    stealers: Vec<Stealer<Task>>,
    threads: Vec<JoinHandle<()>>,
    next_group_id: AtomicCell<usize>,
    parked_threads: Arc<Injector<Unparker>>,
}

impl ThreadPool {
    pub fn new(number_of_threads: usize) -> Self {
        assert!(
            number_of_threads > 0,
            "There must be at least one thread for the thread pool"
        );

        let worker_deques: Vec<Worker<Task>> =
            (0..number_of_threads).map(|_| Worker::new_fifo()).collect();

        let mut thread_pool = Self {
            target_thread_count: number_of_threads,
            global_queue: Arc::new(Injector::new()),
            stealers: worker_deques.iter().map(Worker::stealer).collect(),
            threads: Vec::with_capacity(number_of_threads),
            next_group_id: AtomicCell::new(0),
            parked_threads: Arc::new(Injector::new()),
        };

        for worker_deque in worker_deques {
            let global_queue = thread_pool.global_queue.clone();
            let stealers = thread_pool.stealers.clone();
            let parked_threads = thread_pool.parked_threads.clone();

            thread_pool.threads.push(std::thread::spawn(move || {
                Self::await_work(&worker_deque, &global_queue, &stealers, &parked_threads);
            }))
        }

        thread_pool
    }

    fn await_work(
        local: &Worker<Task>,
        global: &Injector<Task>,
        stealers: &[Stealer<Task>],
        parked_threads: &Injector<Unparker>,
    ) {
        let parker = Parker::new();
        let unparker = parker.unparker();

        loop {
            // Pop a task from the local queue, if not empty.
            let task = local.pop().or_else(|| {
                // Otherwise, we need to look for a task elsewhere.
                iter::repeat_with(|| {
                    // Try stealing a batch of tasks from the global queue.
                    global
                        .steal_batch_and_pop(local)
                        // Or try stealing a task from one of the other threads.
                        .or_else(|| stealers.iter().map(Stealer::steal).collect())
                })
                // Loop while no task was stolen and any steal operation needs to be retried.
                .find(|s| !s.is_retry())
                // Extract the stolen task, if there is one.
                .and_then(Steal::success)
            });

            if let Some(task) = task {
                // TODO: recover panics
                task.call_once();
            } else {
                parked_threads.push(unparker.clone());
                parker.park();
            }
        }
    }

    pub fn create_context(&self) -> ThreadPoolContext {
        ThreadPoolContext::new(self, self.next_group_id.fetch_add(1))
    }

    fn compute(&self, task: Task) {
        self.global_queue.push(task);

        // un-park a thread since there is new work
        if let Steal::Success(unparker) = self.parked_threads.steal() {
            unparker.unpark();
        }
    }
}

impl Default for ThreadPool {
    fn default() -> Self {
        Self::new(num_cpus::get())
    }
}

/// A computation context for a group that spawns tasks in a `ThreadPool`
#[derive(Copy, Clone, Debug)]
pub struct ThreadPoolContext<'pool> {
    thread_pool: &'pool ThreadPool,
    task_group_id: TaskGroupId,
}

impl<'pool> ThreadPoolContext<'pool> {
    /// Create a new `ThreadPoolContext`
    fn new(thread_pool: &'pool ThreadPool, task_group_id: TaskGroupId) -> Self {
        Self {
            thread_pool,
            task_group_id,
        }
    }

    /// What is the degree of parallelism that the `ThreadPool` aims for?
    /// This is helpful to determine how to split the work into tasks.
    pub fn degree_of_parallelism(&self) -> usize {
        self.thread_pool.target_thread_count
    }

    /// Compute a task in the `ThreadPool`
    pub fn compute<F>(&self, task: F)
    where
        F: StaticTaskFn,
    {
        self.thread_pool
            .compute(Task::new(self.task_group_id, task));
    }

    /// Execute a bunch of tasks in a scope that blocks until all tasks are finished.
    /// Provides a lifetime for that scope.
    /// TODO: provide an async version so that async workflows can do something in the meantime?
    /// TODO: handle panics: if a thread panics, this function will block forever
    pub fn scope<'scope, S>(&'pool self, scope_fn: S)
    where
        S: FnOnce(&Scope<'pool, 'scope>) + 'scope,
    {
        let scope = Scope::<'pool, 'scope> {
            thread_pool_context: &self,
            wait_group: WaitGroup::new(),
            _scope_marker: PhantomData,
        };

        scope_fn(&scope);

        scope.wait_group.wait();
    }
}

/// A scope in which you can execute tasks and it blocks until all tasks are finished
#[derive(Debug)]
pub struct Scope<'pool, 'scope> {
    thread_pool_context: &'pool ThreadPoolContext<'pool>,
    wait_group: WaitGroup,
    // needs to be invariant to `'scope`, cf. https://github.com/crossbeam-rs/crossbeam/pull/226/files#r232721183
    _scope_marker: PhantomData<&'scope mut &'scope ()>,
}

impl<'pool, 'scope> Scope<'pool, 'scope> {
    /// Compute a task in the `ThreadPool`
    pub fn compute<F>(&self, task: F)
    where
        F: TaskFn + 'scope,
    {
        let wait_group = self.wait_group.clone();

        // Allocate the `task` on the heap and erase the `'scope` bound.
        let task: Box<dyn TaskFn + 'scope> = Box::new(task);
        let task: Box<dyn StaticTaskFn> = unsafe { mem::transmute(task) };

        self.thread_pool_context.compute(move || {
            task();

            // decrement `WaitGroup` counter
            drop(wait_group);
        });
    }

    /// Compute a task in the `ThreadPool` and return a `Future` of a result
    pub fn compute_result<F, R>(&self, task: F) -> TaskResult<R>
    where
        F: FnOnce() -> R + Send + 'scope,
        R: Clone + Send + 'static,
    {
        let future = TaskResult::default();

        let future_ref = future.clone();
        self.compute(move || {
            future_ref.set(task());
        });

        future
    }
}

/// A future that provides the task result
pub struct TaskResult<R> {
    option: Arc<AtomicCell<TaskResultOption<R>>>,
}

// we can't derive `Clone` since it requires `R` to be `Clone` as well
impl<R> Clone for TaskResult<R> {
    fn clone(&self) -> Self {
        Self {
            option: self.option.clone(),
        }
    }
}

/// The state of the `TaskResult` future
#[derive(Debug)]
enum TaskResultOption<R> {
    None,
    Result(R),
    Waiting(Waker),
}

impl<R> Default for TaskResultOption<R> {
    fn default() -> Self {
        TaskResultOption::None
    }
}

impl<R> TaskResult<R> {
    fn set(&self, result: R) {
        match self.option.swap(TaskResultOption::Result(result)) {
            TaskResultOption::None => {} // do nothing
            TaskResultOption::Result(_) => {
                unreachable!("There must not be a second computation of the result")
            }
            TaskResultOption::Waiting(waker) => waker.wake(),
        };
    }
}

impl<R> Default for TaskResult<R> {
    fn default() -> Self {
        Self {
            option: Default::default(),
        }
    }
}

impl<R> Future for TaskResult<R> {
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self
            .option
            .swap(TaskResultOption::Waiting(cx.waker().clone()))
        {
            TaskResultOption::None | TaskResultOption::Waiting(_) => Poll::Pending,
            TaskResultOption::Result(r) => Poll::Ready(r),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};

    use futures::future;

    use super::*;
    use crossbeam::utils::Backoff;

    #[test]
    #[allow(clippy::blacklisted_name)]
    fn one_task() {
        let thread_pool = ThreadPool::new(1);

        let foo = Arc::new(AtomicI32::new(0));

        let bar = foo.clone();
        thread_pool.compute(Task::new(0, move || {
            bar.fetch_add(42, Ordering::SeqCst);
        }));

        let backoff = Backoff::new();
        while foo.load(Ordering::SeqCst) != 42 {
            backoff.snooze();
        }
    }

    #[test]
    #[allow(clippy::blacklisted_name)]
    fn two_task_one_thread() {
        let thread_pool = ThreadPool::new(2);

        let foo = Arc::new(AtomicI32::new(0));

        let bar = foo.clone();
        thread_pool.compute(Task::new(0, move || {
            bar.fetch_add(20, Ordering::SeqCst);
        }));

        let baz = foo.clone();
        thread_pool.compute(Task::new(0, move || {
            baz.fetch_add(22, Ordering::SeqCst);
        }));

        let backoff = Backoff::new();
        while foo.load(Ordering::SeqCst) != 42 {
            backoff.snooze();
        }
    }

    #[test]
    #[allow(clippy::blacklisted_name)]
    fn two_task_two_threads() {
        let thread_pool = ThreadPool::new(2);

        let foo = Arc::new(AtomicI32::new(0));

        let bar = foo.clone();
        thread_pool.compute(Task::new(0, move || {
            bar.fetch_add(20, Ordering::SeqCst);
        }));

        let baz = foo.clone();
        thread_pool.compute(Task::new(0, move || {
            baz.fetch_add(22, Ordering::SeqCst);
        }));

        let backoff = Backoff::new();
        while foo.load(Ordering::SeqCst) != 42 {
            backoff.snooze();
        }
    }

    #[test]
    fn lots_of_tasks() {
        let thread_pool = ThreadPool::new(2);

        let number_of_tasks = 1_000_000;
        let tasks_completed = Arc::new(AtomicI32::new(0));

        for _ in 0..number_of_tasks {
            let tasks_completed = tasks_completed.clone();
            thread_pool.compute(Task::new(0, move || {
                tasks_completed.fetch_add(1, Ordering::SeqCst);
            }));
        }

        let backoff = Backoff::new();
        while tasks_completed.load(Ordering::SeqCst) != number_of_tasks {
            backoff.snooze();
        }
    }

    #[test]
    fn context() {
        let thread_pool = ThreadPool::new(2);
        let context = thread_pool.create_context();

        let result = Arc::new(AtomicI32::new(0));

        let result_clone = result.clone();
        context.compute(move || {
            result_clone.fetch_add(42, Ordering::SeqCst);
        });

        let backoff = Backoff::new();
        while result.load(Ordering::SeqCst) != 42 {
            backoff.snooze();
        }
    }

    #[test]
    fn scoped() {
        const NUMBER_OF_TASKS: usize = 42;

        let thread_pool = ThreadPool::new(2);
        let context = thread_pool.create_context();

        let result = AtomicUsize::new(0);

        context.scope(|scope| {
            for _ in 0..NUMBER_OF_TASKS {
                scope.compute(|| {
                    result.fetch_add(1, Ordering::SeqCst);
                });
            }
        });

        assert_eq!(result.load(Ordering::SeqCst), NUMBER_OF_TASKS);
    }

    #[test]
    fn scoped_vec() {
        const NUMBER_OF_TASKS: usize = 42;

        let thread_pool = ThreadPool::new(2);
        let context = thread_pool.create_context();

        let mut result = vec![0; NUMBER_OF_TASKS];

        context.scope(|scope| {
            for (chunk, i) in result.chunks_exact_mut(1).zip(0..NUMBER_OF_TASKS) {
                scope.compute(move || chunk[0] = i);
            }
        });

        assert_eq!((0..NUMBER_OF_TASKS).collect::<Vec<_>>(), result);
    }

    #[test]
    fn compute_results() {
        const NUMBER_OF_TASKS: usize = 42;

        let thread_pool = ThreadPool::new(2);
        let context = thread_pool.create_context();

        let mut futures = Vec::with_capacity(NUMBER_OF_TASKS);

        context.scope(|scope| {
            for i in 0..NUMBER_OF_TASKS {
                futures.push(scope.compute_result(move || i));
            }
        });

        let result = futures::executor::block_on(future::join_all(futures));

        assert_eq!(result, (0..NUMBER_OF_TASKS).collect::<Vec<_>>());
    }

    #[test]
    fn parking() {
        let thread_pool = ThreadPool::new(1);
        let context = thread_pool.create_context();

        // wait for the thread to be parked
        let backoff = Backoff::new();
        while thread_pool.parked_threads.len() == 0 {
            backoff.snooze();
        }

        let mut unparked = false;
        context.scope(|scope| scope.compute(|| unparked = true));

        assert!(unparked)
    }
}
