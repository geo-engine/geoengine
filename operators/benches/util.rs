/// Clone of `rusty_fork_test!` that supports benches.
///
/// Unfortunately, we cannot separate the setup-phase from the actual measured work.
///
#[macro_export]
macro_rules! rusty_fork_bench {
    (#![rusty_fork(timeout_ms = $timeout:expr)]
     $(
         $(#[$meta:meta])*
         fn $test_name:ident() $body:block
    )*) => { $(
        $(#[$meta])*
        fn $test_name(bencher: &mut test::Bencher) {
            // Eagerly convert everything to function pointers so that all
            // tests use the same instantiation of `fork`.
            fn body_fn() $body
            let body: fn () = body_fn;

            fn supervise_fn(child: &mut rusty_fork::ChildWrapper,
                            _file: &mut ::std::fs::File) {
                                rusty_fork::fork_test::supervise_child(child, $timeout)
            }
            let supervise:
            fn (&mut rusty_fork::ChildWrapper, &mut ::std::fs::File) =
            supervise_fn;

            // measure time once and output the same time for iter to converge faster
            // long-running processes are otherwise unfeasible to measure
            // as they won't finish running iter over and over again
            bencher.iter(|| {
                rusty_fork::fork(
                    rusty_fork::rusty_fork_test_name!($test_name),
                    rusty_fork::rusty_fork_id!(),
                    rusty_fork::fork_test::no_configure_child,
                supervise, body).expect("forking test failed");
            });
        }
    )* };

    ($(
         $(#[$meta:meta])*
         fn $test_name:ident(bencher: &mut Bencher) $body:block
    )*) => {
        rusty_fork_bench! {
            #![rusty_fork(timeout_ms = 0)]

            $($(#[$meta])* fn $test_name() $body)*
        }
    };
}

/// Run a set of functions in a forked process one after each other.
#[macro_export]
macro_rules! forked_run {
    ($($fn_name:expr)*) => {
        fn main() {
            let fn_name = std::env::args().last().unwrap();

            $(
                if fn_name == crate::util::fn_path(&$fn_name) {
                    $fn_name();
                    return;
                }
            )*

            $(
                println!("{}", crate::util::as_process($fn_name));
            )*
        }
    };
}

/// Call `f` as an own process and capture its output as result.
#[allow(dead_code)]
pub fn as_process<F>(f: F) -> String
where
    F: std::ops::FnOnce() -> (),
{
    rusty_fork::fork(
        fn_path(&f),
        rusty_fork::rusty_fork_id!(),
        capturing_output,
        wait_for_child_output,
        f,
    )
    .unwrap()
}

/// Get path of function, e.g. `path::to::f`
pub fn fn_path<F>(_f: &F) -> &'static str {
    std::any::type_name::<F>()
}

fn capturing_output(cmd: &mut std::process::Command) {
    // Only actually capture stdout since we can't use
    // wait_with_output() since it for some reason consumes the `Child`.
    cmd.stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit());
}

fn wait_for_child_output(
    child: &mut rusty_fork::ChildWrapper,
    _file: &mut std::fs::File,
) -> String {
    use std::io::Read;
    let mut output = String::new();
    child
        .inner_mut()
        .stdout
        .as_mut()
        .unwrap()
        .read_to_string(&mut output)
        .unwrap();
    assert!(child.wait().unwrap().success());
    output
}
