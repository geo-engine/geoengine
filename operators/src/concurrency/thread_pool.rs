use crossbeam::atomic::AtomicCell;
use crossbeam::deque::{Injector, Steal, Stealer, Worker};
use crossbeam::utils::Backoff;
use std::iter;
use std::sync::Arc;
use std::thread::JoinHandle;

type Task = Box<dyn FnOnce() + Send>;

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
}

impl ThreadPool {
    pub fn new(number_of_threads: usize) -> Self {
        let worker_deques: Vec<Worker<Task>> =
            (0..number_of_threads).map(|_| Worker::new_fifo()).collect();

        let mut thread_pool = Self {
            degree_of_parallelism: number_of_threads,
            global_queue: Arc::new(Injector::new()),
            stealers: worker_deques.iter().map(Worker::stealer).collect(),
            threads: Vec::with_capacity(number_of_threads),
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
                task();
                backoff.reset();
            } else {
                backoff.snooze();
            }
        }
    }

    pub fn create_context(&self) -> ThreadPoolContext {
        ThreadPoolContext::new(self)
    }

    pub fn compute<F>(&self, _task_group_id: TaskGroupId, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.global_queue.push(Box::new(task));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskGroupId(usize);

static NEXT_GROUP_ID: AtomicCell<usize> = AtomicCell::new(0);

impl TaskGroupId {
    pub fn new() -> Self {
        TaskGroupId(NEXT_GROUP_ID.fetch_add(1))
    }
}

/// A computation context for a group that spawns tasks in a `ThreadPool`
#[derive(Copy, Clone)]
pub struct ThreadPoolContext<'pool> {
    task_group_id: TaskGroupId,
    thread_pool: &'pool ThreadPool,
}

impl<'pool> ThreadPoolContext<'pool> {
    fn new(thread_pool: &'pool ThreadPool) -> Self {
        Self {
            task_group_id: TaskGroupId::new(),
            thread_pool,
        }
    }

    pub fn compute<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.thread_pool.compute(self.task_group_id, task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI32, Ordering};

    #[test]
    #[allow(clippy::blacklisted_name)]
    fn one_task() {
        let thread_pool = ThreadPool::new(1);

        let foo = Arc::new(AtomicI32::new(0));

        let bar = foo.clone();
        thread_pool.compute(TaskGroupId::new(), move || {
            bar.fetch_add(42, Ordering::SeqCst);
        });

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
        thread_pool.compute(TaskGroupId::new(), move || {
            bar.fetch_add(20, Ordering::SeqCst);
        });

        let baz = foo.clone();
        thread_pool.compute(TaskGroupId::new(), move || {
            baz.fetch_add(22, Ordering::SeqCst);
        });

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
        thread_pool.compute(TaskGroupId::new(), move || {
            bar.fetch_add(20, Ordering::SeqCst);
        });

        let baz = foo.clone();
        thread_pool.compute(TaskGroupId::new(), move || {
            baz.fetch_add(22, Ordering::SeqCst);
        });

        let backoff = Backoff::new();
        while foo.load(Ordering::SeqCst) != 42 {
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
}
