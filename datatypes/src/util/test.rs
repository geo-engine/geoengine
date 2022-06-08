use crate::raster::{EmptyGrid, Grid, GridOrEmpty, GridSize, MaskedGrid};
use std::panic;

pub trait TestDefault {
    /// Generate a default value used for testing. Use this instead of the `Default` trait
    /// if the default value only makes sense in tests and not in production code.
    fn test_default() -> Self;
}

pub fn catch_unwind_silent<F: FnOnce() -> R + panic::UnwindSafe, R>(
    f: F,
) -> std::thread::Result<R> {
    let prev_hook = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));
    let result = panic::catch_unwind(f);
    panic::set_hook(prev_hook);
    result
}

pub fn grid_or_empty_grid_eq<D, T>(g1: &GridOrEmpty<D, T>, g2: &GridOrEmpty<D, T>) -> bool
where
    D: PartialEq + GridSize + Clone,
    T: PartialEq + Copy,
{
    match (g1, g2) {
        (GridOrEmpty::Grid(g1), GridOrEmpty::Grid(g2)) => masked_grid_eq(g1, g2),
        (GridOrEmpty::Empty(g1), GridOrEmpty::Empty(g2)) => empty_grid_eq(g1, g2),
        _ => false,
    }
}

pub fn masked_grid_eq<D, T>(g1: &MaskedGrid<D, T>, g2: &MaskedGrid<D, T>) -> bool
where
    D: PartialEq + GridSize + Clone,
    T: PartialEq + Copy,
{
    grid_eq(g1.as_ref(), g2.as_ref())
        && match (g1.mask_ref(), g2.mask_ref()) {
            (None, None) => true,
            (None, Some(_)) => false,
            (Some(_), None) => false,
            (Some(m1), Some(m2)) => grid_eq(m1, m2),
        }
}

pub fn grid_eq<D, T>(g1: &Grid<D, T>, g2: &Grid<D, T>) -> bool
where
    D: PartialEq,
    T: PartialEq + Copy,
{
    if g1.data.len() != g2.data.len() || g1.shape.ne(&g2.shape) {
        return false;
    }

    for (l, r) in g1.data.iter().zip(g2.data.iter()) {
        if l != r {
            return false;
        }
    }
    true
}

pub fn empty_grid_eq<D, T>(g1: &EmptyGrid<D, T>, g2: &EmptyGrid<D, T>) -> bool
where
    D: PartialEq,
    T: PartialEq + Copy,
{
    g1.shape.eq(&g2.shape)
}

/// Save bytes into a file to be used for testing.
///
/// # Panics
///
/// This function panics if the file cannot be created or written.
///
pub fn save_test_bytes(bytes: &[u8], filename: &str) {
    use std::io::Write;

    std::fs::File::create(filename)
        .unwrap()
        .write_all(bytes)
        .unwrap();
}

#[cfg(test)]
mod tests {
    use crate::{
        raster::{EmptyGrid, Grid2D, GridShape2D, MaskedGrid2D, EmptyGrid2D},
        util::test::{empty_grid_eq, grid_eq, masked_grid_eq},
    };

    #[test]
    fn test_empty_grid_eq_ok() {
        let d1: GridShape2D = [3, 2].into();
        let d2: GridShape2D = [3, 2].into();

        let r1: EmptyGrid2D<u8> = EmptyGrid::new(d1);
        let r2: EmptyGrid2D<u8> = EmptyGrid::new(d2);

        assert!(empty_grid_eq(&r1, &r2));
    }

    #[test]
    fn test_empty_grid_eq_integral_fail_dim() {
        let d1: GridShape2D = [3, 2].into();
        let d2: GridShape2D = [3, 1].into();

        let r1: EmptyGrid2D<u8> = EmptyGrid::new(d1);
        let r2: EmptyGrid2D<u8> = EmptyGrid::new(d2);

        assert!(!empty_grid_eq(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_integral_ok() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let r1 = Grid2D::new(d1, vec![1, 2, 3, 42]).unwrap();
        let r2 = Grid2D::new(d2, vec![1, 2, 3, 42]).unwrap();

        assert!(grid_eq(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_with_integral_fail_dim() {
        let d1: GridShape2D = [4, 1].into();
        let d2: GridShape2D = [2, 2].into();
        let r1 = Grid2D::new(d1, vec![1, 2, 3, 42]).unwrap();
        let r2 = Grid2D::new(d2, vec![1, 2, 3, 42]).unwrap();

        assert!(!grid_eq(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_integral_fail_data() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let r1 = Grid2D::new(d1, vec![2, 2, 3, 42]).unwrap();
        let r2 = Grid2D::new(d2, vec![1, 2, 3, 42]).unwrap();

        assert!(!grid_eq(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_float_some_ok() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let r1 = Grid2D::new(d1, vec![1_f32, 2_f32, 3_f32, 42_f32]).unwrap();
        let r2 = Grid2D::new(d2, vec![1_f32, 2_f32, 3_f32, 42_f32]).unwrap();

        assert!(grid_eq(&r1, &r2));
    }

    #[test]
    fn test_masked_grid_eq_no_mask_ok() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let r1 = MaskedGrid2D::new(Grid2D::new(d1, vec![1, 2, 3, 42]).unwrap(), None).unwrap();
        let r2 = MaskedGrid2D::new(Grid2D::new(d2, vec![1, 2, 3, 42]).unwrap(), None).unwrap();

        assert!(masked_grid_eq(&r1, &r2));
    }

    #[test]
    fn test_masked_grid_eq_mask_ok() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let m1 = Grid2D::new(d1, vec![true, true, true, false]).unwrap();
        let m2 = Grid2D::new(d2, vec![true, true, true, false]).unwrap();

        let r1 = MaskedGrid2D::new(Grid2D::new(d1, vec![1, 2, 3, 42]).unwrap(), Some(m1)).unwrap();
        let r2 = MaskedGrid2D::new(Grid2D::new(d2, vec![1, 2, 3, 42]).unwrap(), Some(m2)).unwrap();

        assert!(masked_grid_eq(&r1, &r2));
    }

    #[test]
    fn test_masked_grid_eq_no_mask_fail_data() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let r1 = MaskedGrid2D::new(Grid2D::new(d1, vec![1, 2, 3, 42]).unwrap(), None).unwrap();
        let r2 = MaskedGrid2D::new(Grid2D::new(d2, vec![42, 3, 2, 1]).unwrap(), None).unwrap();

        assert!(!masked_grid_eq(&r1, &r2));
    }

    #[test]
    fn test_masked_grid_eq_no_mask_fail_dim() {
        let d1: GridShape2D = [4, 1].into();
        let d2: GridShape2D = [2, 2].into();

        let r1 = MaskedGrid2D::new(Grid2D::new(d1, vec![1, 2, 3, 42]).unwrap(), None).unwrap();
        let r2 = MaskedGrid2D::new(Grid2D::new(d2, vec![1, 2, 3, 42]).unwrap(), None).unwrap();

        assert!(!masked_grid_eq(&r1, &r2));
    }

    #[test]
    fn test_masked_grid_eq_mask_fail_some_none() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let m1 = Grid2D::new(d1, vec![true, true, true, false]).unwrap();

        let r1 = MaskedGrid2D::new(Grid2D::new(d1, vec![1, 2, 3, 42]).unwrap(), Some(m1)).unwrap();
        let r2 = MaskedGrid2D::new(Grid2D::new(d2, vec![1, 2, 3, 42]).unwrap(), None).unwrap();

        assert!(!masked_grid_eq(&r1, &r2));
    }

    #[test]
    fn test_masked_grid_eq_mask_fail_none_some() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let m2 = Grid2D::new(d1, vec![true, true, true, false]).unwrap();

        let r1 = MaskedGrid2D::new(Grid2D::new(d1, vec![1, 2, 3, 42]).unwrap(), None).unwrap();
        let r2 = MaskedGrid2D::new(Grid2D::new(d2, vec![1, 2, 3, 42]).unwrap(), Some(m2)).unwrap();

        assert!(!masked_grid_eq(&r1, &r2));
    }

    #[test]
    fn test_masked_grid_eq_fail_mask() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let m1 = Grid2D::new(d1, vec![true, true, true, true]).unwrap();
        let m2 = Grid2D::new(d2, vec![true, true, true, false]).unwrap();

        let r1 = MaskedGrid2D::new(Grid2D::new(d1, vec![1, 2, 3, 42]).unwrap(), Some(m1)).unwrap();
        let r2 = MaskedGrid2D::new(Grid2D::new(d2, vec![1, 2, 3, 42]).unwrap(), Some(m2)).unwrap();

        assert!(!masked_grid_eq(&r1, &r2));
    }
}
