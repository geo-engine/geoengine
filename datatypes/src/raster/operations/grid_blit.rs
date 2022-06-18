use crate::raster::{
    empty_grid::EmptyGrid, masked_grid::MaskedGrid, BoundedGrid, Grid, Grid1D, Grid2D, Grid3D,
    GridBoundingBox, GridBounds, GridIdx, GridIndexAccessMut, GridIntersection, GridOrEmpty,
    GridSize, GridSpaceToLinearSpace,
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

impl<D, T> GridBlit<Grid<D, T>, T> for Grid2D<T>
where
    D: GridSize<ShapeArray = [usize; 2]>
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

impl<D1, D2, T, A, I> GridBlit<MaskedGrid<D1, T>, T> for MaskedGrid<D2, T>
where
    D1: GridSize<ShapeArray = A>
        + GridBounds<IndexArray = I>
        + GridSpaceToLinearSpace<IndexArray = I>
        + Clone
        + PartialEq,
    D2: GridSize<ShapeArray = A>
        + GridBounds<IndexArray = I>
        + GridSpaceToLinearSpace<IndexArray = I>
        + Clone
        + PartialEq
        + PartialEq,
    T: Copy + Sized + Default,
    Grid<D2, T>: GridBlit<Grid<D1, T>, T>,
    Grid<D2, bool>: GridBlit<Grid<D1, bool>, bool>,
{
    fn grid_blit_from(&mut self, other: &MaskedGrid<D1, T>) {
        // easy part: blit the data
        self.inner_grid.grid_blit_from(other.as_ref());
        self.validity_mask.grid_blit_from(other.mask_ref())
    }
}

impl<D1, D2, T, A, I> GridBlit<EmptyGrid<D1, T>, T> for MaskedGrid<D2, T>
where
    D1: GridSize<ShapeArray = A>
        + GridBounds<IndexArray = I>
        + GridSpaceToLinearSpace<IndexArray = I>
        + Clone
        + PartialEq,
    D2: GridSize<ShapeArray = A>
        + GridBounds<IndexArray = I>
        + GridSpaceToLinearSpace<IndexArray = I>
        + Clone
        + PartialEq
        + PartialEq,
    T: Copy + Sized + Default,
    Grid<D2, bool>: GridBlit<EmptyGrid<D1, T>, T>,
    Grid<D2, bool>: GridBlit<Grid<D1, bool>, bool>,
{
    fn grid_blit_from(&mut self, other: &EmptyGrid<D1, T>) {
        self.mask_mut().grid_blit_from(other)
    }
}

impl<D1, D2, T, A, I> GridBlit<Grid<D1, T>, T> for MaskedGrid<D2, T>
where
    D1: GridSize<ShapeArray = A>
        + GridBounds<IndexArray = I>
        + GridSpaceToLinearSpace<IndexArray = I>
        + Clone
        + PartialEq,
    D2: GridSize<ShapeArray = A>
        + GridBounds<IndexArray = I>
        + GridSpaceToLinearSpace<IndexArray = I>
        + Clone
        + PartialEq
        + PartialEq,
    T: Copy + Sized + Default,
    Grid<D2, T>: GridBlit<Grid<D1, T>, T>,
    Grid<D2, bool>: GridBlit<Grid<D1, bool>, bool>,
{
    fn grid_blit_from(&mut self, other: &Grid<D1, T>) {
        let temp_mask = Grid::new_filled(other.shape.clone(), true);
        self.mask_mut().grid_blit_from(&temp_mask);
        self.as_mut().grid_blit_from(other);
    }
}

impl<D, T> GridBlit<EmptyGrid<D, T>, T> for Grid2D<bool>
where
    D: GridSize<ShapeArray = [usize; 2]>
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

impl<D1, D2, T, A, I> GridBlit<GridOrEmpty<D1, T>, T> for MaskedGrid<D2, T>
where
    D1: GridSize<ShapeArray = A>
        + GridBounds<IndexArray = I>
        + GridSpaceToLinearSpace<IndexArray = I>
        + Clone
        + PartialEq,
    D2: GridSize<ShapeArray = A>
        + GridBounds<IndexArray = I>
        + GridSpaceToLinearSpace<IndexArray = I>
        + Clone
        + PartialEq,
    I: Clone + AsRef<[isize]> + Into<GridIdx<I>>,
    T: Copy + Sized + Default,
    Self: GridBlit<MaskedGrid<D1, T>, T> + GridBlit<EmptyGrid<D1, T>, T>,
{
    fn grid_blit_from(&mut self, other: &GridOrEmpty<D1, T>) {
        match other {
            GridOrEmpty::Grid(g) => self.grid_blit_from(g),
            GridOrEmpty::Empty(n) => self.grid_blit_from(n),
        }
    }
}

impl<D1, D2, T, A, I> GridBlit<GridOrEmpty<D1, T>, T> for GridOrEmpty<D2, T>
where
    D1: GridSize<ShapeArray = A>
        + GridBounds<IndexArray = I>
        + GridSpaceToLinearSpace<IndexArray = I>
        + Clone
        + PartialEq,
    D2: GridSize<ShapeArray = A>
        + GridBounds<IndexArray = I>
        + GridSpaceToLinearSpace<IndexArray = I>
        + Clone
        + PartialEq,
    I: Clone + AsRef<[isize]> + Into<GridIdx<I>>,
    T: Copy + Sized + Default,
    MaskedGrid<D2,T>: GridBlit<MaskedGrid<D1, T>, T> + GridBlit<EmptyGrid<D1, T>, T>,
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

#[cfg(test)]
mod tests {
    use crate::raster::{
        masked_grid::{MaskedGrid2D, MaskedGrid3D},
        EmptyGrid2D, EmptyGrid3D, Grid, Grid2D, Grid3D, GridBlit, GridBoundingBox, GridIdx,
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
}
