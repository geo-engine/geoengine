// TODO: switch to `std::hint::black_box` when stable
pub fn black_box<T>(dummy: T) -> T {
    unsafe {
        let rv = std::ptr::read_volatile(&dummy);
        std::mem::forget(dummy);
        rv
    }
}
