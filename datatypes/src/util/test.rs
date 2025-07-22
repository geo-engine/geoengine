use float_cmp::approx_eq;

use crate::raster::{
    EmptyGrid, GeoTransform, Grid, GridIndexAccess, GridOrEmpty, GridSize, MaskedGrid, Pixel,
    RasterTile2D, grid_idx_iter_2d,
};
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
    grid_eq(g1.as_ref(), g2.as_ref()) && grid_eq(g1.mask_ref(), g2.mask_ref())
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
        .expect("it should be possible to create this file for testing")
        .write_all(bytes)
        .expect("it should be possible to write this file for testing");
}

/// Method that compares two lists of tiles and panics with a message why there is a difference.
///
/// # Panics
/// If there is a difference between two tiles or the length of the lists
pub fn assert_eq_two_list_of_tiles_u8(
    list_a: &[RasterTile2D<u8>],
    list_b: &[RasterTile2D<u8>],
    compare_cache_hint: bool,
) {
    assert_eq_two_list_of_tiles::<u8>(list_a, list_b, compare_cache_hint);
}

pub fn assert_eq_two_list_of_tiles<P: Pixel>(
    list_a: &[RasterTile2D<P>],
    list_b: &[RasterTile2D<P>],
    compare_cache_hint: bool,
) {
    assert_eq!(
        list_a.len(),
        list_b.len(),
        "len() of input_a: {}, len of input_b: {}",
        list_a.len(),
        list_b.len()
    );

    list_a
        .iter()
        .zip(list_b)
        .enumerate()
        .for_each(|(i, (a, b))| {
            assert_eq!(
                a.time, b.time,
                "time of tile {} input_a: {}, input_b: {}",
                i, a.time, b.time
            );
            assert_eq!(
                a.band, b.band,
                "band of tile {} input_a: {}, input_b: {}",
                i, a.band, b.band
            );
            assert_eq!(
                a.tile_position, b.tile_position,
                "tile position of tile {} input_a: {:?}, input_b: {:?}",
                i, a.tile_position, b.tile_position
            );

            let spatial_grid_a = a.global_pixel_spatial_grid_definition();
            let spatial_grid_b = b.global_pixel_spatial_grid_definition();
            assert_eq!(
                spatial_grid_a.grid_bounds(),
                spatial_grid_b.grid_bounds(),
                "grid bounds of tile {} input_a: {:?}, input_b {:?}",
                i,
                spatial_grid_a.grid_bounds(),
                spatial_grid_b.grid_bounds()
            );
            assert!(
                approx_eq!(
                    GeoTransform,
                    spatial_grid_a.geo_transform(),
                    spatial_grid_b.geo_transform()
                ),
                "geo transform of tile {} input_a: {:?}, input_b: {:?}",
                i,
                spatial_grid_a.geo_transform(),
                spatial_grid_b.geo_transform()
            );
            assert_eq!(
                a.grid_array.is_empty(),
                b.grid_array.is_empty(),
                "grid shape of tile {} input_a is_empty: {:?}, input_b is_empty: {:?}",
                i,
                a.grid_array.is_empty(),
                b.grid_array.is_empty(),
            );
            if !a.grid_array.is_empty() {
                let mat_a = a.grid_array.clone().into_materialized_masked_grid();
                let mat_b = b.grid_array.clone().into_materialized_masked_grid();

                for (pi, idx) in grid_idx_iter_2d(&mat_a).enumerate() {
                    let a_v = mat_a
                        .get_at_grid_index(idx)
                        .expect("tile a must contain idx inside tile bounds");
                    let b_v = mat_b
                        .get_at_grid_index(idx)
                        .expect("tile b must contain idx inside tile bounds");
                    assert_eq!(
                        a_v, b_v,
                        "tile {i} pixel {pi} at {idx:?} input_a: {a_v:?}, input_b: {b_v:?}",
                    );
                }
            }
            if compare_cache_hint {
                assert_eq!(
                    a.cache_hint, b.cache_hint,
                    "cache hint of tile {} input_a: {:?}, input_b: {:?}",
                    i, a.cache_hint, b.cache_hint
                );
            }
        });
}

#[cfg(test)]
mod tests {
    use crate::{
        raster::{EmptyGrid, EmptyGrid2D, Grid2D, GridOrEmpty, GridShape2D, MaskedGrid2D},
        util::test::{empty_grid_eq, grid_eq, masked_grid_eq},
    };

    use super::grid_or_empty_grid_eq;

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

        let r1 = MaskedGrid2D::new_with_data(Grid2D::new(d1, vec![1, 2, 3, 42]).unwrap());
        let r2 = MaskedGrid2D::new_with_data(Grid2D::new(d2, vec![1, 2, 3, 42]).unwrap());

        assert!(masked_grid_eq(&r1, &r2));
    }

    #[test]
    fn test_masked_grid_eq_mask_ok() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let m1 = Grid2D::new(d1, vec![true, true, true, false]).unwrap();
        let m2 = Grid2D::new(d2, vec![true, true, true, false]).unwrap();

        let r1 = MaskedGrid2D::new(Grid2D::new(d1, vec![1, 2, 3, 42]).unwrap(), m1).unwrap();
        let r2 = MaskedGrid2D::new(Grid2D::new(d2, vec![1, 2, 3, 42]).unwrap(), m2).unwrap();

        assert!(masked_grid_eq(&r1, &r2));
    }

    #[test]
    fn test_masked_grid_eq_no_mask_fail_data() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let r1 = MaskedGrid2D::new_with_data(Grid2D::new(d1, vec![1, 2, 3, 42]).unwrap());
        let r2 = MaskedGrid2D::new_with_data(Grid2D::new(d2, vec![42, 3, 2, 1]).unwrap());

        assert!(!masked_grid_eq(&r1, &r2));
    }

    #[test]
    fn test_masked_grid_eq_no_mask_fail_dim() {
        let d1: GridShape2D = [4, 1].into();
        let d2: GridShape2D = [2, 2].into();

        let r1 = MaskedGrid2D::new_with_data(Grid2D::new(d1, vec![1, 2, 3, 42]).unwrap());
        let r2 = MaskedGrid2D::new_with_data(Grid2D::new(d2, vec![1, 2, 3, 42]).unwrap());

        assert!(!masked_grid_eq(&r1, &r2));
    }

    #[test]
    fn test_masked_grid_eq_mask_fail_some_none() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let m1 = Grid2D::new(d1, vec![true, true, true, false]).unwrap();

        let r1 = MaskedGrid2D::new(Grid2D::new(d1, vec![1, 2, 3, 42]).unwrap(), m1).unwrap();
        let r2 = MaskedGrid2D::new_with_data(Grid2D::new(d2, vec![1, 2, 3, 42]).unwrap());

        assert!(!masked_grid_eq(&r1, &r2));
    }

    #[test]
    fn test_masked_grid_eq_mask_fail_none_some() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let m2 = Grid2D::new(d1, vec![true, true, true, false]).unwrap();

        let r1 = MaskedGrid2D::new_with_data(Grid2D::new(d1, vec![1, 2, 3, 42]).unwrap());
        let r2 = MaskedGrid2D::new(Grid2D::new(d2, vec![1, 2, 3, 42]).unwrap(), m2).unwrap();

        assert!(!masked_grid_eq(&r1, &r2));
    }

    #[test]
    fn test_masked_grid_eq_fail_mask() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let m1 = Grid2D::new(d1, vec![true, true, true, true]).unwrap();
        let m2 = Grid2D::new(d2, vec![true, true, true, false]).unwrap();

        let r1 = MaskedGrid2D::new(Grid2D::new(d1, vec![1, 2, 3, 42]).unwrap(), m1).unwrap();
        let r2 = MaskedGrid2D::new(Grid2D::new(d2, vec![1, 2, 3, 42]).unwrap(), m2).unwrap();

        assert!(!masked_grid_eq(&r1, &r2));
    }

    #[test]
    fn test_grid_or_empty_grid_eq_empty() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let e1 = EmptyGrid2D::<u8>::new(d1);
        let e2 = EmptyGrid2D::<u8>::new(d2);

        let goe1 = GridOrEmpty::from(e1);
        let goe2 = GridOrEmpty::from(e2);

        assert!(grid_or_empty_grid_eq(&goe1, &goe2));
    }

    #[test]
    fn test_grid_or_empty_grid_eq_grid_empty() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let e1 = MaskedGrid2D::from(EmptyGrid2D::<u8>::new(d1));
        let e2 = MaskedGrid2D::from(EmptyGrid2D::<u8>::new(d2));

        let goe1 = GridOrEmpty::from(e1);
        let goe2 = GridOrEmpty::from(e2);

        assert!(grid_or_empty_grid_eq(&goe1, &goe2));
    }

    #[test]
    fn test_grid_or_empty_grid_eq_grid_values() {
        let d1: GridShape2D = [2, 2].into();
        let d2: GridShape2D = [2, 2].into();

        let m1 = Grid2D::new(d1, vec![true, true, true, false]).unwrap();
        let m2 = Grid2D::new(d2, vec![true, true, true, false]).unwrap();

        let r1 = MaskedGrid2D::new(Grid2D::new(d1, vec![1, 2, 3, 42]).unwrap(), m1).unwrap();
        let r2 = MaskedGrid2D::new(Grid2D::new(d2, vec![1, 2, 3, 42]).unwrap(), m2).unwrap();

        let goe1 = GridOrEmpty::from(r1);
        let goe2 = GridOrEmpty::from(r2);

        assert!(grid_or_empty_grid_eq(&goe1, &goe2));
    }
}
