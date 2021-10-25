#![feature(bench_black_box)]
use std::{hint::black_box, time::Instant};

fn work(num_threads: usize) {
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .unwrap();

    thread_pool.install(|| {
        rayon::scope(|scope| {
            for _ in 0..num_threads {
                scope.spawn(move |_| {
                    for i in 0..100_000_000 / num_threads {
                        black_box(i + 1);
                    }
                });
            }
        })
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
