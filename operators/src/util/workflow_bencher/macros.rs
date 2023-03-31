/// Macro which expands to a benchmark harness.
///
/// Currently, using Criterion.rs requires disabling the benchmark harness
/// generated automatically by rustc. This can be done like so:
///
/// ```toml
/// [[bench]]
/// name = "my_bench"
/// harness = false
/// ```
///
/// In this case, `my_bench` must be a rust file inside the 'benches' directory,
/// like so:
///
/// `benches/my_bench.rs`
///
/// Since we've disabled the default benchmark harness, we need to add our own:
///
/// ```ignore
/// #[macro_use]
/// extern crate criterion;
/// use criterion::Criterion;
/// fn bench_method1(c: &mut Criterion) {
/// }
///
/// fn bench_method2(c: &mut Criterion) {
/// }
///
/// criterion_group!(benches, bench_method1, bench_method2);
/// criterion_main!(benches);
/// ```
///
/// The `criterion_main` macro expands to a `main` function which runs all of the
/// benchmarks in the given groups.
///
#[macro_export]
macro_rules! workflow_bencher_main {
    ($( $target:path ),+ $(,)*) => {
        fn main() {
            let mut bench_collector = $crate::WorkflowBenchmarkCollector::default();
            $(
                $target(&mut bench_collector);
            )+
        }
    };
}
