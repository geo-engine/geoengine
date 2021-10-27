#![feature(test)]

use std::time::Instant;

extern crate test;

mod util;

fn benchmark<const NUM_THREADS: usize>() {
    rayon::ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .build_global()
        .unwrap();

    let start = Instant::now();
    work(NUM_THREADS);
    print!("{},{:?}", NUM_THREADS, start.elapsed());
}

fn work(num_threads: usize) {
    rayon::scope(|scope| {
        scope.spawn(move |_| {
            for i in 0..100_000_000 / num_threads {
                test::black_box(i + 1);
            }
        });
    })
}

fn header() {
    print!("num_threads,time");
}

forked_run! {
    header
    benchmark::<1>
    benchmark::<2>
    benchmark::<4>
}
