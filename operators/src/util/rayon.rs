use std::sync::Arc;

use rayon::{ThreadPool, ThreadPoolBuilder};

/// Tries to create a global thread pool that does not spawn any threads.
/// This prevents accidentally using it.
///
/// Hopefully, rayon either provides a real method for achieving this in the future
/// or does not fix this behavior.
///
/// Panics if building the global thread pool does not fail.
///
fn rayon_destroy_global_thread_pool() {
    assert!(rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .spawn_handler(|_thread| {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Do not spawn rayon global pool on purpose",
            ))
        })
        .build_global()
        .is_err());
}

/// Create a rayon thread pool with the given number of threads.
/// Use `num_threads = 0` for auto number of threads.
pub fn create_rayon_thread_pool(num_threads: usize) -> Arc<ThreadPool> {
    rayon_destroy_global_thread_pool();

    let thread_pool = ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .expect("Thread Pool must be initializable");

    Arc::new(thread_pool)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(
        expected = "The global thread pool has not been initialized.: ThreadPoolBuildError { kind: GlobalPoolAlreadyInitialized }"
    )]
    fn global_rayon_fail() {
        create_rayon_thread_pool(0);

        rayon::current_num_threads();
    }
}
