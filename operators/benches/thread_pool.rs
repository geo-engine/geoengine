#![feature(bench_black_box)]
use std::{hint::black_box, time::Instant};

use geoengine_operators::util::create_rayon_thread_pool;

fn work(num_threads: usize) {
    let thread_pool = create_rayon_thread_pool(num_threads);

    thread_pool.scope(|scope| {
        for _ in 0..num_threads {
            scope.spawn(move |_| {
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
