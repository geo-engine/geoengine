use std::marker::PhantomData;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::{iter, mem};

use crossbeam::atomic::AtomicCell;
use crossbeam::deque::{Injector, Steal, Stealer, Worker};
use crossbeam::sync::WaitGroup;
use crossbeam::utils::Backoff;

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
    degree_of_parallelism: usize,
    global_queue: Arc<Injector<Task>>,
    stealers: Vec<Stealer<Task>>,
    threads: Vec<JoinHandle<()>>,
    next_group_id: AtomicCell<usize>,
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
            degree_of_parallelism: number_of_threads,
            global_queue: Arc::new(Injector::new()),
            stealers: worker_deques.iter().map(Worker::stealer).collect(),
            threads: Vec::with_capacity(number_of_threads),
            next_group_id: AtomicCell::new(0),
        };

        for worker_deque in worker_deques {
            let global_queue = thread_pool.global_queue.clone();
            let stealers = thread_pool.stealers.clone();

            thread_pool.threads.push(std::thread::spawn(move || {
                Self::await_work(&worker_deque, &global_queue, &stealers);
            }))
        }

        thread_pool
    }

    fn await_work(local: &Worker<Task>, global: &Injector<Task>, stealers: &[Stealer<Task>]) {
        let backoff = Backoff::new();

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
                backoff.reset();
            } else {
                // TODO: sleep instead of busy waiting?
                backoff.snooze();
            }
        }
    }

    pub fn create_context(&self) -> ThreadPoolContext {
        ThreadPoolContext::new(self, self.next_group_id.fetch_add(1))
    }

    fn compute(&self, task: Task) {
        self.global_queue.push(task);
    }

    // TODO: scope for threads
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

    /// What is the degree of paralellism that the `ThreadPool` provides?
    /// This is helpful to determine how to split the work into tasks.
    pub fn degree_of_parallelism(&self) -> usize {
        self.thread_pool.degree_of_parallelism
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
        S: FnOnce(&Scope) + 'scope,
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
    _scope_marker: PhantomData<&'scope ()>,
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
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI32, Ordering};

    use failure::_core::sync::atomic::AtomicUsize;

    use super::*;

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
            for i in 0..NUMBER_OF_TASKS {
                let r_slice: &mut [usize] = &mut result[i..=i];
                scope.compute(move || {
                    r_slice[0] = i;
                });
            }
        });

        assert_eq!((0..NUMBER_OF_TASKS).collect::<Vec<_>>(), result);
    }
}
