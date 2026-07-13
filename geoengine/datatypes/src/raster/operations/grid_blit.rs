use crate::raster::{
    BoundedGrid, Grid, Grid1D, Grid3D, GridBoundingBox, GridBounds, GridIdx, GridIndexAccessMut,
    GridIntersection, GridOrEmpty, GridSize, GridSpaceToLinearSpace, empty_grid::EmptyGrid,
    masked_grid::MaskedGrid,
};

pub trait GridBlit<O, T>
where
    O: GridSize + BoundedGrid,
    T: Copy + Sized,
{
    fn grid_blit_from(&mut self, other: &O);
}

impl<D, T> GridBlit<Grid<D, T>, T> for Grid1D<T>
where
    D: GridSize<ShapeArray = [usize; 1]>
        + GridBounds<IndexArray = [isize; 1]>
        + GridSpaceToLinearSpace<IndexArray = [isize; 1]>,
    T: Copy + Sized,
{
    fn grid_blit_from(&mut self, other: &Grid<D, T>) {
        let other_offset_dim = other.bounding_box();
        let offset_dim = self.bounding_box();
        let intersection: Option<GridBoundingBox<[isize; 1]>> =
            offset_dim.intersection(&other_offset_dim);
        if let Some(intersection_offset_dim) = intersection {
            let overlap_start = intersection_offset_dim.min_index();
            let [overlap_size] = intersection_offset_dim.axis_size();

            let self_start_x = offset_dim.linear_space_index_unchecked(overlap_start);
            let other_start_x = other_offset_dim.linear_space_index_unchecked(overlap_start);

            self.data.as_mut_slice()[self_start_x..self_start_x + overlap_size]
                .copy_from_slice(&other.data[other_start_x..other_start_x + overlap_size]);
        }
    }
}

impl<D, D2, T> GridBlit<Grid<D, T>, T> for Grid<D2, T>
where
    D: GridSize<ShapeArray = [usize; 2]>
        + GridBounds<IndexArray = [isize; 2]>
        + GridSpaceToLinearSpace<IndexArray = [isize; 2]>,
    D2: GridSize<ShapeArray = [usize; 2]>
        + GridBounds<IndexArray = [isize; 2]>
        + GridSpaceToLinearSpace<IndexArray = [isize; 2]>,
    T: Copy + Sized,
{
    fn grid_blit_from(&mut self, other: &Grid<D, T>) {
        let other_offset_dim = other.bounding_box();
        let offset_dim = self.bounding_box();
        let intersection: Option<GridBoundingBox<[isize; 2]>> =
            offset_dim.intersection(&other_offset_dim);
        if let Some(intersection_offset_dim) = intersection {
            let GridIdx([overlap_y_start, overlap_x_start]) = intersection_offset_dim.min_index();
            let [overlap_y_size, overlap_x_size] = intersection_offset_dim.axis_size();

            for y in overlap_y_start..overlap_y_start + overlap_y_size as isize {
                let other_start_x =
                    other_offset_dim.linear_space_index_unchecked([y, overlap_x_start]);

                let self_start_x = offset_dim.linear_space_index_unchecked([y, overlap_x_start]);

                self.data.as_mut_slice()[self_start_x..self_start_x + overlap_x_size]
                    .copy_from_slice(&other.data[other_start_x..other_start_x + overlap_x_size]);
            }
        }
    }
}

impl<D, T> GridBlit<Grid<D, T>, T> for Grid3D<T>
where
    D: GridSize<ShapeArray = [usize; 3]>
        + GridBounds<IndexArray = [isize; 3]>
        + GridSpaceToLinearSpace<IndexArray = [isize; 3]>,
    T: Copy + Sized,
{
    fn grid_blit_from(&mut self, other: &Grid<D, T>) {
        let other_offset_dim = other.bounding_box();
        let offset_dim = self.bounding_box();
        let intersection: Option<GridBoundingBox<[isize; 3]>> =
            offset_dim.intersection(&other_offset_dim);

        if let Some(intersection_offset_dim) = intersection {
            let GridIdx([overlap_z_start, overlap_y_start, overlap_x_start]) =
                intersection_offset_dim.min_index();
            let [overlap_z_size, overlap_y_size, overlap_x_size] =
                intersection_offset_dim.axis_size();

            for z in overlap_z_start..overlap_z_start + overlap_z_size as isize {
                for y in overlap_y_start..overlap_y_start + overlap_y_size as isize {
                    let self_start_x =
                        offset_dim.linear_space_index_unchecked([z, y, overlap_x_start]);
                    let other_start_x =
                        other_offset_dim.linear_space_index_unchecked([z, y, overlap_x_start]);

                    self.data.as_mut_slice()[self_start_x..self_start_x + overlap_x_size]
                        .copy_from_slice(
                            &other.data[other_start_x..other_start_x + overlap_x_size],
                        );
                }
            }
        }
    }
}

impl<D1, D2, T, I> GridBlit<MaskedGrid<D1, T>, T> for MaskedGrid<D2, T>
where
    D1: GridSpaceToLinearSpace<IndexArray = I> + GridBounds + PartialEq + Clone,
    D2: GridSpaceToLinearSpace<IndexArray = I> + GridBounds,
    T: Copy,
    Grid<D2, T>: GridBlit<Grid<D1, T>, T>,
    Grid<D2, bool>: GridBlit<Grid<D1, bool>, bool>,
{
    fn grid_blit_from(&mut self, other: &MaskedGrid<D1, T>) {
        // easy part: blit the data
        self.inner_grid.grid_blit_from(other.as_ref());
        self.validity_mask.grid_blit_from(other.mask_ref());
    }
}

impl<D1, D2, T, I> GridBlit<EmptyGrid<D1, T>, T> for MaskedGrid<D2, T>
where
    D1: GridSpaceToLinearSpace<IndexArray = I> + GridBounds,
    D2: GridSpaceToLinearSpace<IndexArray = I> + GridBounds + PartialEq + Clone,
    T: Copy,
    Grid<D2, bool>: GridBlit<EmptyGrid<D1, T>, T>,
    Grid<D2, bool>: GridBlit<Grid<D1, bool>, bool>,
{
    fn grid_blit_from(&mut self, other: &EmptyGrid<D1, T>) {
        self.mask_mut().grid_blit_from(other);
    }
}

impl<D1, D2, T, I> GridBlit<Grid<D1, T>, T> for MaskedGrid<D2, T>
where
    D1: GridSpaceToLinearSpace<IndexArray = I> + GridBounds + Clone,
    D2: GridSpaceToLinearSpace<IndexArray = I> + GridBounds + PartialEq + Clone,
    T: Copy,
    Grid<D2, T>: GridBlit<Grid<D1, T>, T>,
    Grid<D2, bool>: GridBlit<Grid<D1, bool>, bool>,
{
    fn grid_blit_from(&mut self, other: &Grid<D1, T>) {
        let temp_mask = Grid::new_filled(other.shape.clone(), true);
        self.mask_mut().grid_blit_from(&temp_mask);
        self.as_mut().grid_blit_from(other);
    }
}

impl<D, D2, T> GridBlit<EmptyGrid<D, T>, T> for Grid<D2, bool>
where
    D: GridSize<ShapeArray = [usize; 2]>
        + GridBounds<IndexArray = [isize; 2]>
        + GridSpaceToLinearSpace<IndexArray = [isize; 2]>,
    D2: GridSize<ShapeArray = [usize; 2]>
        + GridBounds<IndexArray = [isize; 2]>
        + GridSpaceToLinearSpace<IndexArray = [isize; 2]>,
    T: Copy + Sized,
{
    fn grid_blit_from(&mut self, other: &EmptyGrid<D, T>) {
        let other_offset_dim = other.bounding_box();
        let offset_dim = self.bounding_box();
        let intersection: Option<GridBoundingBox<[isize; 2]>> =
            offset_dim.intersection(&other_offset_dim);
        if let Some(intersection_offset_dim) = intersection {
            let GridIdx([overlap_y_start, overlap_x_start]) = intersection_offset_dim.min_index();
            let [overlap_y_size, overlap_x_size] = intersection_offset_dim.axis_size();

            for y in overlap_y_start..overlap_y_start + overlap_y_size as isize {
                for x in overlap_x_start..overlap_x_start + overlap_x_size as isize {
                    self.set_at_grid_index_unchecked([y, x], false);
                }
            }
        }
    }
}

impl<D1, D2, T, I> GridBlit<GridOrEmpty<D1, T>, T> for MaskedGrid<D2, T>
where
    D1: GridSpaceToLinearSpace<IndexArray = I> + GridBounds<IndexArray = I> + PartialEq + Clone,
    D2: GridSpaceToLinearSpace<IndexArray = I> + GridBounds<IndexArray = I> + PartialEq + Clone,
    T: Copy + Default,
    I: AsRef<[isize]> + Into<GridIdx<I>>,
    Self: GridBlit<MaskedGrid<D1, T>, T> + GridBlit<EmptyGrid<D1, T>, T>,
{
    fn grid_blit_from(&mut self, other: &GridOrEmpty<D1, T>) {
        match other {
            GridOrEmpty::Grid(g) => self.grid_blit_from(g),
            GridOrEmpty::Empty(n) => self.grid_blit_from(n),
        }
    }
}

impl<D1, D2, T, I> GridBlit<GridOrEmpty<D1, T>, T> for GridOrEmpty<D2, T>
where
    D1: GridSpaceToLinearSpace<IndexArray = I> + GridBounds<IndexArray = I> + PartialEq + Clone,
    D2: GridSpaceToLinearSpace<IndexArray = I> + GridBounds<IndexArray = I> + PartialEq + Clone,
    T: Copy + Default,
    I: AsRef<[isize]> + Into<GridIdx<I>>,
    MaskedGrid<D2, T>: GridBlit<MaskedGrid<D1, T>, T> + GridBlit<EmptyGrid<D1, T>, T>,
{
    fn grid_blit_from(&mut self, other: &GridOrEmpty<D1, T>) {
        if self.is_empty() && other.is_empty() {
            return;
        }
        self.materialize();
        match self {
            GridOrEmpty::Grid(g) => g.grid_blit_from(other),
            GridOrEmpty::Empty(_) => unreachable!(),
        }
    }
}

impl<D, T> GridBlit<EmptyGrid<D, T>, T> for Grid3D<bool>
where
    D: GridSize<ShapeArray = [usize; 3]>
        + GridBounds<IndexArray = [isize; 3]>
        + GridSpaceToLinearSpace<IndexArray = [isize; 3]>,
    T: Copy + Sized,
{
    fn grid_blit_from(&mut self, other: &EmptyGrid<D, T>) {
        let other_offset_dim = other.bounding_box();
        let offset_dim = self.bounding_box();
        let intersection: Option<GridBoundingBox<[isize; 3]>> =
            offset_dim.intersection(&other_offset_dim);

        if let Some(intersection_offset_dim) = intersection {
            let GridIdx([overlap_z_start, overlap_y_start, overlap_x_start]) =
                intersection_offset_dim.min_index();
            let [overlap_z_size, overlap_y_size, overlap_x_size] =
                intersection_offset_dim.axis_size();

            for z in overlap_z_start..overlap_z_start + overlap_z_size as isize {
                for y in overlap_y_start..overlap_y_start + overlap_y_size as isize {
                    for x in overlap_x_start..overlap_x_start + overlap_x_size as isize {
                        self.set_at_grid_index_unchecked([z, y, x], false);
                    }
                }
            }
        }
    }
}

/// Like `grid_blit_from`, but only copies pixels from `source` that are marked as valid.
///
/// Pixels where the source has no-data (validity mask is `false`) are left untouched
/// in the destination. If the source is entirely empty (`EmptyGrid`), nothing is copied.
///
/// This is useful when combining tiles from multiple overlapping sources where
/// valid data should always take precedence over no-data values.
pub fn grid_blit_valid_only<D1, D2, T>(
    dest: &mut GridOrEmpty<D2, T>,
    source: &GridOrEmpty<D1, T>,
) where
    D1: GridSize<ShapeArray = [usize; 2]>
        + GridBounds<IndexArray = [isize; 2]>
        + GridSpaceToLinearSpace<IndexArray = [isize; 2]>
        + PartialEq
        + Clone,
    D2: GridSize<ShapeArray = [usize; 2]>
        + GridBounds<IndexArray = [isize; 2]>
        + GridSpaceToLinearSpace<IndexArray = [isize; 2]>
        + PartialEq
        + Clone,
    T: Copy + Default,
{
    if dest.is_empty() && source.is_empty() {
        return;
    }
    dest.materialize();
    let GridOrEmpty::Grid(dest_grid) = dest else {
        unreachable!()
    };

    match source {
        GridOrEmpty::Grid(source_grid) => {
            let src_bbox = source_grid.inner_grid.bounding_box();
            let dest_bbox = dest_grid.inner_grid.bounding_box();

            if let Some(intersection) = dest_bbox.intersection(&src_bbox) {
                let GridIdx([y_start, x_start]) = intersection.min_index();
                let [y_size, x_size] = intersection.axis_size();

                for y in y_start..y_start + y_size as isize {
                    for x in x_start..x_start + x_size as isize {
                        let src_lin = src_bbox.linear_space_index_unchecked([y, x]);
                        if source_grid.validity_mask.data[src_lin] {
                            let dest_lin = dest_bbox.linear_space_index_unchecked([y, x]);
                            dest_grid.inner_grid.data[dest_lin] =
                                source_grid.inner_grid.data[src_lin];
                            dest_grid.validity_mask.data[dest_lin] = true;
                        }
                    }
                }
            }
        }
        GridOrEmpty::Empty(_) => {
            // Source is empty (all no-data), nothing to copy
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::raster::{
        EmptyGrid2D, EmptyGrid3D, Grid, Grid2D, Grid3D, GridBlit, GridBoundingBox, GridIdx,
        GridOrEmpty, MaskedGrid, grid_blit_valid_only,
        masked_grid::{MaskedGrid2D, MaskedGrid3D},
    };

    #[test]
    fn grid_blit_from_2d_0_0() {
        let dim = [4, 4];
        let data = vec![0; 16];

        let mut r1 = Grid2D::new(dim.into(), data).unwrap();

        let data = vec![7; 16];

        let r2 = Grid2D::new(dim.into(), data).unwrap();

        r1.grid_blit_from(&r2);

        assert_eq!(r1.data, vec![7; 16]);
    }

    #[test]
    fn grid_blit_from_2d_2_2() {
        let data = vec![0; 16];

        let mut r1 = Grid2D::new([4, 4].into(), data).unwrap();

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

        let shifted_idx = GridIdx([2, 2]);
        let shifted_dim = GridBoundingBox::new(shifted_idx, shifted_idx + [3, 3]).unwrap();
        let r2 = Grid::new(shifted_dim, data).unwrap();

        r1.grid_blit_from(&r2);

        assert_eq!(
            r1.data,
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 4, 5]
        );
    }

    #[test]
    fn grid_blit_from_2d_n2_n2() {
        let data = vec![0; 16];

        let mut r1 = Grid2D::new([4, 4].into(), data).unwrap();

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

        let shifted_idx = GridIdx([-2, -2]);
        let shifted_dim = GridBoundingBox::new(shifted_idx, shifted_idx + [3, 3]).unwrap();
        let r2 = Grid::new(shifted_dim, data).unwrap();

        r1.grid_blit_from(&r2);

        assert_eq!(
            r1.data,
            vec![10, 11, 0, 0, 14, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn grid_blit_from_2d_no_data() {
        let dim = [4, 4];
        let data = vec![7; 16];

        let mut r1 = MaskedGrid2D::new_with_data(Grid2D::new(dim.into(), data).unwrap());

        let r2 = EmptyGrid2D::new(dim.into());

        r1.grid_blit_from(&r2);

        assert_eq!(r1.inner_grid.data, vec![7; 16]);
        assert_eq!(r1.validity_mask.data, vec![false; 16]);
    }

    #[test]
    fn grid_blit_from_3d_0_0() {
        let dim = [4, 4, 4];
        let data = vec![0; 64];

        let mut r1 = MaskedGrid3D::new_with_data(Grid3D::new(dim.into(), data).unwrap());

        let data = vec![7; 64];

        let r2 = Grid3D::new(dim.into(), data).unwrap();

        r1.grid_blit_from(&r2);

        assert_eq!(r1.inner_grid.data, vec![7; 64]);
        assert_eq!(r1.validity_mask.data, vec![true; 64]);
    }

    #[test]
    fn grid_blit_from_3d_2_2() {
        let data = vec![0; 64];

        let mut r1 = MaskedGrid3D::new_with_data(Grid3D::new([4, 4, 4].into(), data).unwrap());

        let data: Vec<i32> = (0..64).collect();

        let shifted_idx = GridIdx([2, 2, 2]);
        let shifted_dim = GridBoundingBox::new(shifted_idx, shifted_idx + [3, 3, 3]).unwrap();
        let r2 = Grid::new(shifted_dim, data).unwrap();

        r1.grid_blit_from(&r2);

        assert_eq!(
            r1.inner_grid.data,
            vec![
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 4, 5, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 16, 17, 0, 0, 20, 21
            ]
        );

        assert_eq!(r1.validity_mask.data, vec![true; 64]);
    }

    #[test]
    fn grid_blit_from_3d_n2_n2() {
        let data = vec![0; 64];

        let mut r1 = Grid3D::new([4, 4, 4].into(), data).unwrap();

        let data: Vec<i32> = (0..64).collect();

        let shifted_idx = GridIdx([-2, -2, -2]);
        let shifted_dim = GridBoundingBox::new(shifted_idx, shifted_idx + [3, 3, 3]).unwrap();
        let r2 = Grid::new(shifted_dim, data).unwrap();

        r1.grid_blit_from(&r2);

        assert_eq!(
            r1.data,
            vec![
                42, 43, 0, 0, 46, 47, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 58, 59, 0, 0, 62, 63, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );
    }

    #[test]
    fn grid_blit_from_3d_no_data() {
        let dim = [4, 4, 4];
        let data = vec![0; 64];

        let mut r1 = MaskedGrid3D::from(Grid3D::new(dim.into(), data).unwrap());

        let r2 = EmptyGrid3D::new(dim.into());

        r1.grid_blit_from(&r2);

        assert_eq!(r1.inner_grid.data, vec![0; 64]);
        assert_eq!(r1.validity_mask.data, vec![false; 64]);
    }

    #[test]
    fn grid_blit_valid_only_overwrites_valid_pixels() {
        // Destination: 4x4 grid, all pixels = 100, all valid
        let bbox = GridBoundingBox::new(GridIdx([0, 0]), GridIdx([3, 3])).unwrap();
        let dest_data = Grid::new(bbox, vec![100; 16]).unwrap();
        let dest_mask = Grid::new_filled(bbox, true);
        let dest_masked = MaskedGrid::new(dest_data, dest_mask).unwrap();
        let mut dest = GridOrEmpty::new_grid(dest_masked);

        // Source: same shape, all pixels = 200, but only first 8 pixels are valid
        let bbox = GridBoundingBox::new(GridIdx([0, 0]), GridIdx([3, 3])).unwrap();
        let src_data = Grid::new(bbox, vec![200; 16]).unwrap();
        let src_mask = Grid::new(bbox, vec![
            true, true, true, true, true, true, true, true, false, false, false, false, false,
            false, false, false,
        ]).unwrap();
        let src_masked = MaskedGrid::new(src_data, src_mask).unwrap();
        let src = GridOrEmpty::new_grid(src_masked);

        grid_blit_valid_only(&mut dest, &src);

        let dest_masked = dest.as_masked_grid().expect("should be a grid");

        // Valid source pixels (first 8) should overwrite dest → should be 200
        let expected_data = vec![
            200, 200, 200, 200, 200, 200, 200, 200, 100, 100, 100, 100, 100, 100, 100, 100,
        ];
        assert_eq!(dest_masked.inner_grid.data, expected_data);
        // All pixels in dest should now be valid
        assert_eq!(dest_masked.validity_mask.data, vec![true; 16]);
    }

    #[test]
    fn grid_blit_valid_only_empty_source_does_nothing() {
        // Destination: 4x4 grid, all pixels = 100, all valid
        let bbox = GridBoundingBox::new(GridIdx([0, 0]), GridIdx([3, 3])).unwrap();
        let dest_data = Grid::new(bbox, vec![100; 16]).unwrap();
        let dest_mask = Grid::new_filled(bbox, true);
        let dest_masked = MaskedGrid::new(dest_data, dest_mask).unwrap();
        let mut dest = GridOrEmpty::new_grid(dest_masked);

        // Source is EmptyGrid (all no-data)
        let bbox = GridBoundingBox::new(GridIdx([0, 0]), GridIdx([3, 3])).unwrap();
        let src = GridOrEmpty::new_empty_shape(bbox);

        grid_blit_valid_only(&mut dest, &src);

        let dest_masked = dest.as_masked_grid().expect("should be a grid");

        // Dest should be completely unchanged
        assert_eq!(dest_masked.inner_grid.data, vec![100; 16]);
        assert_eq!(dest_masked.validity_mask.data, vec![true; 16]);
    }

    #[test]
    fn grid_blit_valid_only_entirely_no_data_source_does_nothing() {
        // Destination: 4x4 grid, all pixels = 100, all valid
        let bbox = GridBoundingBox::new(GridIdx([0, 0]), GridIdx([3, 3])).unwrap();
        let dest_data = Grid::new(bbox, vec![100; 16]).unwrap();
        let dest_mask = Grid::new_filled(bbox, true);
        let dest_masked = MaskedGrid::new(dest_data, dest_mask).unwrap();
        let mut dest = GridOrEmpty::new_grid(dest_masked);

        // Source: all pixels = 200, but ALL are no-data
        let bbox = GridBoundingBox::new(GridIdx([0, 0]), GridIdx([3, 3])).unwrap();
        let src_data = Grid::new(bbox, vec![200; 16]).unwrap();
        let src_mask = Grid::new(bbox, vec![false; 16]).unwrap();
        let src_masked = MaskedGrid::new(src_data, src_mask).unwrap();
        let src = GridOrEmpty::new_grid(src_masked);

        grid_blit_valid_only(&mut dest, &src);

        let dest_masked = dest.as_masked_grid().expect("should be a grid");

        // Dest should be completely unchanged (no valid source pixels to copy)
        assert_eq!(dest_masked.inner_grid.data, vec![100; 16]);
        assert_eq!(dest_masked.validity_mask.data, vec![true; 16]);
    }

    #[test]
    fn grid_blit_valid_only_all_valid_source_works_like_regular_blit() {
        // Destination: 4x4 grid, all pixels = 100, all valid
        let bbox = GridBoundingBox::new(GridIdx([0, 0]), GridIdx([3, 3])).unwrap();
        let dest_data = Grid::new(bbox, vec![100; 16]).unwrap();
        let dest_mask = Grid::new_filled(bbox, true);
        let dest_masked = MaskedGrid::new(dest_data, dest_mask).unwrap();
        let mut dest = GridOrEmpty::new_grid(dest_masked);

        // Source: all pixels = 200, all valid
        let bbox = GridBoundingBox::new(GridIdx([0, 0]), GridIdx([3, 3])).unwrap();
        let src_data = Grid::new(bbox, vec![200; 16]).unwrap();
        let src_mask = Grid::new_filled(bbox, true);
        let src_masked = MaskedGrid::new(src_data, src_mask).unwrap();
        let src = GridOrEmpty::new_grid(src_masked);

        grid_blit_valid_only(&mut dest, &src);

        let dest_masked = dest.as_masked_grid().expect("should be a grid");

        // All pixels should be overwritten (like regular blit)
        assert_eq!(dest_masked.inner_grid.data, vec![200; 16]);
        assert_eq!(dest_masked.validity_mask.data, vec![true; 16]);
    }

    #[test]
    fn grid_blit_valid_only_partial_overlap() {
        // Destination: 4x4 grid at origin [0,0]–[3,3], all pixels = 100, all valid
        let dest_bbox = GridBoundingBox::new(GridIdx([0, 0]), GridIdx([3, 3])).unwrap();
        let dest_data = Grid::new(dest_bbox, vec![100; 16]).unwrap();
        let dest_mask = Grid::new_filled(dest_bbox, true);
        let dest_masked = MaskedGrid::new(dest_data, dest_mask).unwrap();
        let mut dest = GridOrEmpty::new_grid(dest_masked);

        // Source: 2x2 grid shifted to [2,2]–[3,3], pixels = 200, all valid
        let src_bbox = GridBoundingBox::new(GridIdx([2, 2]), GridIdx([3, 3])).unwrap();
        let src_data = Grid::new(src_bbox, vec![200; 4]).unwrap();
        let src_mask = Grid::new_filled(src_bbox, true);
        let src_masked = MaskedGrid::new(src_data, src_mask).unwrap();
        let src = GridOrEmpty::new_grid(src_masked);

        grid_blit_valid_only(&mut dest, &src);

        let dest_masked = dest.as_masked_grid().expect("should be a grid");

        // Only the overlapping 2x2 area at [2,2]–[3,3] should be overwritten
        // Linear layout of 4x4: rows [0,1,2,3], each row has 4 columns
        // Overlap is at grid positions (2,2), (2,3), (3,2), (3,3)
        // Linear indices: row 2: idx 8,9 → cols 2,3 = 200; row 3: idx 12,13 → cols 2,3 = 200
        let expected = vec![
            100, 100, 100, 100, // row 0
            100, 100, 100, 100, // row 1
            100, 100, 200, 200, // row 2, cols 2,3 overwritten
            100, 100, 200, 200, // row 3, cols 2,3 overwritten
        ];
        assert_eq!(dest_masked.inner_grid.data, expected);
        assert_eq!(dest_masked.validity_mask.data, vec![true; 16]);
    }

    #[test]
    fn grid_blit_valid_only_preserves_no_data_pixels_in_partial_overlap() {
        // Destination: 4x4 grid at origin [0,0]–[3,3]
        // Pixels = 100, but some are no-data (mask = false at positions (0,0) and (1,1))
        let dest_bbox = GridBoundingBox::new(GridIdx([0, 0]), GridIdx([3, 3])).unwrap();
        let dest_data = Grid::new(dest_bbox, vec![100; 16]).unwrap();
        let mut dest_mask_data = vec![true; 16];
        // Position (0,0) → linear index 0; Position (1,1) → linear index 5
        dest_mask_data[0] = false;
        dest_mask_data[5] = false;
        let dest_mask = Grid::new(dest_bbox, dest_mask_data).unwrap();
        let dest_masked = MaskedGrid::new(dest_data, dest_mask).unwrap();
        let mut dest = GridOrEmpty::new_grid(dest_masked);

        // Source: 2x2 all valid, pixels = 200, positioned at [0,0]–[1,1]
        let src_bbox = GridBoundingBox::new(GridIdx([0, 0]), GridIdx([1, 1])).unwrap();
        let src_data = Grid::new(src_bbox, vec![200; 4]).unwrap();
        let src_mask = Grid::new_filled(src_bbox, true);
        let src_masked = MaskedGrid::new(src_data, src_mask).unwrap();
        let src = GridOrEmpty::new_grid(src_masked);

        grid_blit_valid_only(&mut dest, &src);

        let dest_masked = dest.as_masked_grid().expect("should be a grid");

        // Top-left 2x2 should be overwritten to 200 and marked valid
        // Rest should remain 100
        // Previously no-data pixels (0,0) and (1,1) are now overwritten by valid source → marked valid
        let expected_data = vec![
            200, 200, 100, 100, // row 0: both overwritten by source
            200, 200, 100, 100, // row 1: both overwritten by source
            100, 100, 100, 100, // row 2: unchanged
            100, 100, 100, 100, // row 3: unchanged
        ];
        assert_eq!(dest_masked.inner_grid.data, expected_data);
        // All pixels should now be valid
        assert_eq!(dest_masked.validity_mask.data, vec![true; 16]);
    }

    #[test]
    fn grid_blit_valid_only_source_with_no_data_in_overlap_does_not_overwrite() {
        // Destination: 2x2 grid at origin [0,0]–[1,1], all pixels = 100, all valid
        let bbox = GridBoundingBox::new(GridIdx([0, 0]), GridIdx([1, 1])).unwrap();
        let dest_data = Grid::new(bbox, vec![100; 4]).unwrap();
        let dest_mask = Grid::new_filled(bbox, true);
        let dest_masked = MaskedGrid::new(dest_data, dest_mask).unwrap();
        let mut dest = GridOrEmpty::new_grid(dest_masked);

        // Source: same shape and bounds, all pixels = 200, but ALL are no-data
        let bbox = GridBoundingBox::new(GridIdx([0, 0]), GridIdx([1, 1])).unwrap();
        let src_data = Grid::new(bbox, vec![200; 4]).unwrap();
        let src_mask = Grid::new(bbox, vec![false; 4]).unwrap();
        let src_masked = MaskedGrid::new(src_data, src_mask).unwrap();
        let src = GridOrEmpty::new_grid(src_masked);

        grid_blit_valid_only(&mut dest, &src);

        let dest_masked = dest.as_masked_grid().expect("should be a grid");

        // Dest should be completely unchanged (all source pixels are no-data)
        assert_eq!(dest_masked.inner_grid.data, vec![100; 4]);
        assert_eq!(dest_masked.validity_mask.data, vec![true; 4]);
    }

    #[test]
    fn grid_blit_valid_only_mixed_validity_source() {
        // Destination: 3x3 grid, all pixels = 100, all valid
        let bbox = GridBoundingBox::new(GridIdx([0, 0]), GridIdx([2, 2])).unwrap();
        let dest_data = Grid::new(bbox, vec![100; 9]).unwrap();
        let dest_mask = Grid::new_filled(bbox, true);
        let dest_masked = MaskedGrid::new(dest_data, dest_mask).unwrap();
        let mut dest = GridOrEmpty::new_grid(dest_masked);

        // Source: 3x3 grid, pixels = 200, checkerboard validity pattern
        let bbox = GridBoundingBox::new(GridIdx([0, 0]), GridIdx([2, 2])).unwrap();
        let src_data = Grid::new(bbox, vec![200; 9]).unwrap();
        let src_mask = Grid::new(bbox, vec![
            true, false, true, false, true, false, true, false, true,
        ]).unwrap();
        let src_masked = MaskedGrid::new(src_data, src_mask).unwrap();
        let src = GridOrEmpty::new_grid(src_masked);

        grid_blit_valid_only(&mut dest, &src);

        let dest_masked = dest.as_masked_grid().expect("should be a grid");

        // Only source-valid positions should be overwritten
        // Positions in 3x3: (y,x) → linear index
        // Valid: (0,0)=0, (0,2)=2, (1,1)=4, (2,0)=6, (2,2)=8
        let expected_data = vec![
            200, 100, 200, // row 0: (0,0)=200, (0,1)=100, (0,2)=200
            100, 200, 100, // row 1: (1,0)=100, (1,1)=200, (1,2)=100
            200, 100, 200, // row 2: (2,0)=200, (2,1)=100, (2,2)=200
        ];
        assert_eq!(dest_masked.inner_grid.data, expected_data);
        // All positions should be valid now
        assert_eq!(dest_masked.validity_mask.data, vec![true; 9]);
    }

    #[test]
    fn grid_blit_valid_only_both_empty() {
        // Both dest and source are EmptyGrid
        let bbox = GridBoundingBox::new(GridIdx([0, 0]), GridIdx([3, 3])).unwrap();
        let mut dest: GridOrEmpty<GridBoundingBox<[isize; 2]>, i32> =
            GridOrEmpty::new_empty_shape(bbox);
        let src: GridOrEmpty<GridBoundingBox<[isize; 2]>, i32> =
            GridOrEmpty::new_empty_shape(bbox);

        grid_blit_valid_only(&mut dest, &src);

        // Should still be empty
        assert!(dest.is_empty());
    }

    #[test]
    fn grid_blit_valid_only_dest_empty_source_grid() {
        // Destination is EmptyGrid, source has valid data
        let dest_bbox = GridBoundingBox::new(GridIdx([0, 0]), GridIdx([3, 3])).unwrap();
        let mut dest: GridOrEmpty<GridBoundingBox<[isize; 2]>, i32> =
            GridOrEmpty::new_empty_shape(dest_bbox);

        // Source: 2x2 grid at [1,1]–[2,2], pixels = 200, all valid
        let src_bbox = GridBoundingBox::new(GridIdx([1, 1]), GridIdx([2, 2])).unwrap();
        let src_data = Grid::new(src_bbox, vec![200; 4]).unwrap();
        let src_mask = Grid::new_filled(src_bbox, true);
        let src_masked = MaskedGrid::new(src_data, src_mask).unwrap();
        let src = GridOrEmpty::new_grid(src_masked);

        grid_blit_valid_only(&mut dest, &src);

        let dest_masked = dest.as_masked_grid().expect("should be a grid");

        // Only the overlapping area should have data
        // 4x4 grid, source is at [1,1]–[2,2]
        // Linear layout: rows 0..3, each row has 4 columns
        // Source overwrites row 1 cols 1,2 and row 2 cols 1,2
        let mut expected = vec![0; 16]; // Default value for empty grid
        expected[5] = 200; // row 1, col 1 = idx 5
        expected[6] = 200; // row 1, col 2 = idx 6
        expected[9] = 200; // row 2, col 1 = idx 9
        expected[10] = 200; // row 2, col 2 = idx 10

        let mut expected_mask = vec![false; 16];
        expected_mask[5] = true;
        expected_mask[6] = true;
        expected_mask[9] = true;
        expected_mask[10] = true;

        assert_eq!(dest_masked.inner_grid.data, expected);
        assert_eq!(dest_masked.validity_mask.data, expected_mask);
    }
}
