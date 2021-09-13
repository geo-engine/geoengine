use crate::raster::{EmptyGrid, Grid, GridOrEmpty, NoDataValue};
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

pub fn eq_with_no_data<D, T>(g1: &GridOrEmpty<D, T>, g2: &GridOrEmpty<D, T>) -> bool
where
    D: PartialEq,
    T: PartialEq + Copy,
{
    match (g1, g2) {
        (GridOrEmpty::Grid(g1), GridOrEmpty::Grid(g2)) => grid_eq_with_no_data(g1, g2),
        (GridOrEmpty::Empty(g1), GridOrEmpty::Empty(g2)) => empty_grid_eq_with_no_data(g1, g2),
        _ => false,
    }
}

pub fn grid_eq_with_no_data<D, T>(g1: &Grid<D, T>, g2: &Grid<D, T>) -> bool
where
    D: PartialEq,
    T: PartialEq + Copy,
{
    if g1.data.len() != g2.data.len() || g1.shape.ne(&g2.shape) {
        return false;
    }

    if !match (g1.no_data_value, g2.no_data_value) {
        (None, None) => true,
        (Some(_), None) => false,
        (_, Some(y)) => g1.is_no_data(y),
    } {
        return false;
    }

    for (l, r) in g1.data.iter().zip(g2.data.iter()) {
        if g1.is_no_data(*l) && g1.is_no_data(*r) {
            continue;
        }
        if l != r {
            return false;
        }
    }
    true
}

pub fn empty_grid_eq_with_no_data<D, T>(g1: &EmptyGrid<D, T>, g2: &EmptyGrid<D, T>) -> bool
where
    D: PartialEq,
    T: PartialEq + Copy,
{
    g1.shape.eq(&g2.shape) && g1.is_no_data(g2.no_data_value)
}

#[cfg(test)]
mod tests {
    use crate::raster::{EmptyGrid, Grid2D, GridShape2D};
    use crate::util::test::{empty_grid_eq_with_no_data, grid_eq_with_no_data};

    #[test]
    fn test_empty_grid_eq_with_no_data_integral_ok() {
        let d1: GridShape2D = [3, 2].into();
        let d2: GridShape2D = [3, 2].into();

        let ndv1 = 42;
        let ndv2 = 42;

        let r1 = EmptyGrid::new(d1, ndv1);
        let r2 = EmptyGrid::new(d2, ndv2);

        assert!(empty_grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_empty_grid_eq_with_no_data_integral_fail_dim() {
        let d1: GridShape2D = [3, 2].into();
        let d2: GridShape2D = [3, 1].into();

        let ndv1 = 42;
        let ndv2 = 42;

        let r1 = EmptyGrid::new(d1, ndv1);
        let r2 = EmptyGrid::new(d2, ndv2);

        assert!(!empty_grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_empty_grid_eq_with_no_data_integral_fail_ndv() {
        let d1: GridShape2D = [3, 2].into();
        let d2: GridShape2D = [3, 2].into();

        let ndv1 = 42;
        let ndv2 = 0;

        let r1 = EmptyGrid::new(d1, ndv1);
        let r2 = EmptyGrid::new(d2, ndv2);

        assert!(!empty_grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_empty_grid_eq_with_no_data_float_no_nan_ok() {
        let d1: GridShape2D = [3, 2].into();
        let d2: GridShape2D = [3, 2].into();

        let ndv1 = 42;
        let ndv2 = 42;

        let r1 = EmptyGrid::new(d1, ndv1);
        let r2 = EmptyGrid::new(d2, ndv2);

        assert!(empty_grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_empty_grid_eq_with_no_data_float_no_nan_fail_dim() {
        let d1: GridShape2D = [3, 2].into();
        let d2: GridShape2D = [3, 1].into();

        let ndv1 = 42_f32;
        let ndv2 = 42_f32;

        let r1 = EmptyGrid::new(d1, ndv1);
        let r2 = EmptyGrid::new(d2, ndv2);

        assert!(!empty_grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_empty_grid_eq_with_no_data_float_no_nan_fail_ndv() {
        let d1: GridShape2D = [3, 2].into();
        let d2: GridShape2D = [3, 2].into();

        let ndv1 = 42_f32;
        let ndv2 = 0_f32;

        let r1 = EmptyGrid::new(d1, ndv1);
        let r2 = EmptyGrid::new(d2, ndv2);

        assert!(!empty_grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_empty_grid_eq_with_no_data_float_nan_ok() {
        let d1: GridShape2D = [3, 2].into();
        let d2: GridShape2D = [3, 2].into();

        let ndv1 = f32::NAN;
        let ndv2 = f32::NAN;

        let r1 = EmptyGrid::new(d1, ndv1);
        let r2 = EmptyGrid::new(d2, ndv2);

        assert!(empty_grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_empty_grid_eq_with_no_data_float_nan_fail_dim() {
        let d1: GridShape2D = [3, 2].into();
        let d2: GridShape2D = [3, 1].into();

        let ndv1 = f32::NAN;
        let ndv2 = f32::NAN;

        let r1 = EmptyGrid::new(d1, ndv1);
        let r2 = EmptyGrid::new(d2, ndv2);

        assert!(!empty_grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_empty_grid_eq_with_no_data_float_nan_fail_ndv_1() {
        let d1: GridShape2D = [3, 2].into();
        let d2: GridShape2D = [3, 2].into();

        let ndv1 = f32::NAN;
        let ndv2 = 0_f32;

        let r1 = EmptyGrid::new(d1, ndv1);
        let r2 = EmptyGrid::new(d2, ndv2);

        assert!(!empty_grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_empty_grid_eq_with_no_data_float_nan_fail_ndv_2() {
        let d1: GridShape2D = [3, 2].into();
        let d2: GridShape2D = [3, 2].into();

        let ndv1 = 42_f32;
        let ndv2 = f32::NAN;

        let r1 = EmptyGrid::new(d1, ndv1);
        let r2 = EmptyGrid::new(d2, ndv2);

        assert!(!empty_grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_with_no_data_integral_ok() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let ndv1 = 42;
        let ndv2 = 42;

        let r1 = Grid2D::new(d1, vec![1, 2, 3, 42], Some(ndv1)).unwrap();
        let r2 = Grid2D::new(d2, vec![1, 2, 3, 42], Some(ndv2)).unwrap();

        assert!(grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_with_no_data_integral_fail_dim() {
        let d1: GridShape2D = [4, 1].into();
        let d2: GridShape2D = [2, 2].into();

        let ndv1 = 42;
        let ndv2 = 42;

        let r1 = Grid2D::new(d1, vec![1, 2, 3, 42], Some(ndv1)).unwrap();
        let r2 = Grid2D::new(d2, vec![1, 2, 3, 42], Some(ndv2)).unwrap();

        assert!(!grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_with_no_data_integral_fail_ndv() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let ndv1 = 42;
        let ndv2 = 1;

        let r1 = Grid2D::new(d1, vec![1, 2, 3, 42], Some(ndv1)).unwrap();
        let r2 = Grid2D::new(d2, vec![1, 2, 3, 42], Some(ndv2)).unwrap();

        assert!(!grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_with_no_data_integral_fail_data() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let ndv1 = 42;
        let ndv2 = 42;

        let r1 = Grid2D::new(d1, vec![2, 2, 3, 42], Some(ndv1)).unwrap();
        let r2 = Grid2D::new(d2, vec![1, 2, 3, 42], Some(ndv2)).unwrap();

        assert!(!grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_with_no_data_integral_fail_data_ndv() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let ndv1 = 42;
        let ndv2 = 42;

        let r1 = Grid2D::new(d1, vec![42, 2, 3, 42], Some(ndv1)).unwrap();
        let r2 = Grid2D::new(d2, vec![1, 2, 3, 42], Some(ndv2)).unwrap();

        assert!(!grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_with_no_data_float_none_ok() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let r1 = Grid2D::new(d1, vec![1_f32, 2_f32, 3_f32, 42_f32], None).unwrap();
        let r2 = Grid2D::new(d2, vec![1_f32, 2_f32, 3_f32, 42_f32], None).unwrap();

        assert!(grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_with_no_data_float_some_ok() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let ndv1 = 42_f32;
        let ndv2 = 42_f32;

        let r1 = Grid2D::new(d1, vec![1_f32, 2_f32, 3_f32, 42_f32], Some(ndv1)).unwrap();
        let r2 = Grid2D::new(d2, vec![1_f32, 2_f32, 3_f32, 42_f32], Some(ndv2)).unwrap();

        assert!(grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_with_no_data_float_nan_ok() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let ndv1 = f32::NAN;
        let ndv2 = f32::NAN;

        let r1 = Grid2D::new(d1, vec![1_f32, 2_f32, 3_f32, ndv1], Some(ndv1)).unwrap();
        let r2 = Grid2D::new(d2, vec![1_f32, 2_f32, 3_f32, ndv1], Some(ndv2)).unwrap();

        assert!(grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_with_no_data_float_fail_dim() {
        let d1: GridShape2D = [4, 1].into();
        let d2: GridShape2D = [2, 2].into();

        let r1 = Grid2D::new(d1, vec![1_f32, 2_f32, 3_f32, 42_f32], None).unwrap();
        let r2 = Grid2D::new(d2, vec![1_f32, 2_f32, 3_f32, 42_f32], None).unwrap();

        assert!(!grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_with_no_data_float_fail_ndv() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let ndv1 = 42_f32;
        let ndv2 = 1_f32;

        let r1 = Grid2D::new(d1, vec![1_f32, 2_f32, 3_f32, 42_f32], Some(ndv1)).unwrap();
        let r2 = Grid2D::new(d2, vec![1_f32, 2_f32, 3_f32, 42_f32], Some(ndv2)).unwrap();

        assert!(!grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_with_no_data_float_fail_ndv_nan1() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let ndv1 = f32::NAN;
        let ndv2 = 42_f32;

        let r1 = Grid2D::new(d1, vec![1_f32, 2_f32, 3_f32, 42_f32], Some(ndv1)).unwrap();
        let r2 = Grid2D::new(d2, vec![1_f32, 2_f32, 3_f32, 42_f32], Some(ndv2)).unwrap();

        assert!(!grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_with_no_data_float_fail_ndv_nan2() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let ndv1 = 42_f32;
        let ndv2 = f32::NAN;

        let r1 = Grid2D::new(d1, vec![1_f32, 2_f32, 3_f32, 42_f32], Some(ndv1)).unwrap();
        let r2 = Grid2D::new(d2, vec![1_f32, 2_f32, 3_f32, 42_f32], Some(ndv2)).unwrap();

        assert!(!grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_with_no_data_float_fail_data() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let ndv1 = 42_f32;
        let ndv2 = 42_f32;

        let r1 = Grid2D::new(d1, vec![2_f32, 2_f32, 3_f32, 42_f32], Some(ndv1)).unwrap();
        let r2 = Grid2D::new(d2, vec![1_f32, 2_f32, 3_f32, 42_f32], Some(ndv2)).unwrap();

        assert!(!grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_with_no_data_float_fail_data_nan1() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let ndv1 = f32::NAN;
        let ndv2 = f32::NAN;

        let r1 = Grid2D::new(d1, vec![2_f32, 2_f32, 3_f32, 42_f32], Some(ndv1)).unwrap();
        let r2 = Grid2D::new(d2, vec![1_f32, 2_f32, 3_f32, f32::NAN], Some(ndv2)).unwrap();

        assert!(!grid_eq_with_no_data(&r1, &r2));
    }

    #[test]
    fn test_grid_eq_with_no_data_float_fail_data_nan2() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let ndv1 = f32::NAN;
        let ndv2 = f32::NAN;

        let r1 = Grid2D::new(d1, vec![2_f32, 2_f32, 3_f32, f32::NAN], Some(ndv1)).unwrap();
        let r2 = Grid2D::new(d2, vec![1_f32, 2_f32, 3_f32, 42_f32], Some(ndv2)).unwrap();

        assert!(!grid_eq_with_no_data(&r1, &r2));
    }
}
