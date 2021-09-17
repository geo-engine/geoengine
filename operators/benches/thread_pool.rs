#![feature(bench_black_box)]
use std::{hint::black_box, sync::Arc, time::Instant};

use geoengine_operators::concurrency::{ThreadPool, ThreadPoolContextCreator};

fn work(num_threads: usize) {
    let thread_pool = Arc::new(ThreadPool::new(num_threads));

    let context = thread_pool.create_context();

    context.scope(|scope| {
        for _ in 0..num_threads {
            scope.compute(move || {
                for i in 0..100_000_000 / num_threads {
                    black_box(i + 1);
                }
            });
        }
    })
}

fn main() {
    println!("num_threads,time");
    for num_threads in [1, 2, 4] {
        let start = Instant::now();
        work(num_threads);
        println!("{},{:?}", num_threads, start.elapsed());
    }
}
