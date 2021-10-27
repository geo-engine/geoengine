#![feature(test)]

use std::time::Instant;

extern crate test;

mod util;

fn benchmark(num_threads: usize) {
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
        .unwrap();

    let start = Instant::now();
    work(num_threads);
    println!("{},{:?}", num_threads, start.elapsed());
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

rusty_fork_bench! {

    #[bench]
    fn rayon_1(bencher: &mut Bencher) {
        benchmark(1);
    }

    #[bench]
    fn rayon_2(bencher: &mut Bencher) {
        benchmark(2);
    }

    #[bench]
    fn rayon_4(bencher: &mut Bencher) {
        benchmark(4);
    }

    #[bench]
    fn rayon_8(bencher: &mut Bencher) {
        benchmark(8);
    }

}
