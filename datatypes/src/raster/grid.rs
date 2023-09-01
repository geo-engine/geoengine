use super::{
    grid_traits::{ChangeGridBounds, GridShapeAccess},
    GridBoundingBox, GridBounds, GridContains, GridIdx, GridIdx2D, GridIndexAccess,
    GridIndexAccessMut, GridSize, GridSpaceToLinearSpace,
};
use crate::util::Result;
use crate::{error, util::ByteSize};
use num::Integer;
use num_traits::Zero;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::ops::Add;

/// An `GridShape` describes the shape of an n-dimensional array by storing the size of each axis.
#[derive(PartialEq, Eq, Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GridShape<A>
where
    A: AsRef<[usize]>,
{
    pub shape_array: A,
}

impl<A> GridShape<A>
where
    A: AsRef<[usize]>,
{
    /// create a new `GridShape`
    pub fn new(shape: A) -> Self {
        Self { shape_array: shape }
    }

    /// return a ref to the inner data
    pub fn inner_ref(&self) -> &A {
        &self.shape_array
    }

    /// return the inner data
    pub fn into_inner(self) -> A {
        self.shape_array
    }
}

pub type GridShape1D = GridShape<[usize; 1]>;
pub type GridShape2D = GridShape<[usize; 2]>;
pub type GridShape3D = GridShape<[usize; 3]>;

impl From<[usize; 1]> for GridShape1D {
    fn from(shape: [usize; 1]) -> Self {
        GridShape1D { shape_array: shape }
    }
}

impl From<[usize; 2]> for GridShape2D {
    fn from(shape: [usize; 2]) -> Self {
        GridShape2D { shape_array: shape }
    }
}

impl From<[usize; 3]> for GridShape3D {
    fn from(shape: [usize; 3]) -> Self {
        GridShape3D { shape_array: shape }
    }
}

impl GridSize for GridShape1D {
    type ShapeArray = [usize; 1];

    const NDIM: usize = 1;

    fn axis_size(&self) -> Self::ShapeArray {
        self.shape_array
    }

    fn number_of_elements(&self) -> usize {
        let [a] = self.axis_size();
        a
    }
}

impl GridSpaceToLinearSpace for GridShape1D {
    type IndexArray = [isize; 1];

    fn strides(&self) -> Self::ShapeArray {
        [1]
    }

    fn linear_space_index_unchecked<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> usize {
        let real_idx = index.into();
        debug_assert!(self.contains(&real_idx));

        let GridIdx([x]) = real_idx;
        x as usize
    }

    fn linear_space_index<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> Result<usize> {
        let real_index = index.into();
        ensure!(
            self.contains(&real_index),
            error::GridIndexOutOfBounds {
                index: Vec::from(real_index.0),
                min_index: Vec::from(self.min_index().0),
                max_index: Vec::from(self.max_index().0)
            }
        );

        Ok(self.linear_space_index_unchecked(real_index))
    }

    fn grid_idx_unchecked(&self, linear_idx: usize) -> GridIdx<[isize; 1]> {
        let grid_idx = GridIdx([(linear_idx) as isize]);
        debug_assert!(self.contains(&grid_idx));
        grid_idx
    }
}

impl GridBounds for GridShape1D {
    type IndexArray = [isize; 1];

    fn min_index(&self) -> GridIdx<Self::IndexArray> {
        GridIdx::<[isize; 1]>::zero()
    }

    fn max_index(&self) -> GridIdx<Self::IndexArray> {
        let [x_size] = self.shape_array;
        GridIdx([x_size as isize]) - 1
    }
}

impl GridSize for GridShape2D {
    type ShapeArray = [usize; 2];

    const NDIM: usize = 2;

    fn axis_size(&self) -> Self::ShapeArray {
        self.shape_array
    }

    fn number_of_elements(&self) -> usize {
        let [a, b] = self.axis_size();
        a * b
    }
}

impl GridSpaceToLinearSpace for GridShape2D {
    type IndexArray = [isize; 2];

    fn strides(&self) -> Self::ShapeArray {
        [self.axis_size_x(), 1]
    }

    fn linear_space_index_unchecked<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> usize {
        let real_idx = index.into();
        debug_assert!(self.contains(&real_idx));
        let GridIdx([y, x]) = real_idx;
        let [stride_y, stride_x] = self.strides();
        y as usize * stride_y + x as usize * stride_x
    }

    fn linear_space_index<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> Result<usize> {
        let real_index = index.into();
        ensure!(
            self.contains(&real_index),
            error::GridIndexOutOfBounds {
                index: Vec::from(real_index.0),
                min_index: Vec::from(self.min_index().0),
                max_index: Vec::from(self.max_index().0)
            }
        );
        Ok(self.linear_space_index_unchecked(real_index))
    }

    fn grid_idx_unchecked(&self, linear_idx: usize) -> GridIdx<[isize; 2]> {
        let [stride_y, _stride_x] = self.strides();
        let (y, x) = linear_idx.div_rem(&stride_y);

        let grid_idx = GridIdx([y as isize, x as isize]);
        debug_assert!(self.contains(&grid_idx));
        grid_idx
    }
}

impl GridBounds for GridShape2D {
    type IndexArray = [isize; 2];

    fn min_index(&self) -> GridIdx<Self::IndexArray> {
        GridIdx::<[isize; 2]>::zero()
    }

    fn max_index(&self) -> GridIdx<Self::IndexArray> {
        let [y_size, x_size] = self.shape_array;
        GridIdx([y_size as isize, x_size as isize]) - 1
    }
}

impl GridSize for GridShape3D {
    type ShapeArray = [usize; 3];

    const NDIM: usize = 3;

    fn axis_size(&self) -> Self::ShapeArray {
        self.shape_array
    }

    fn number_of_elements(&self) -> usize {
        let [a, b, c] = self.axis_size();
        a * b * c
    }
}

impl GridSpaceToLinearSpace for GridShape3D {
    type IndexArray = [isize; 3];

    fn strides(&self) -> Self::ShapeArray {
        [
            self.axis_size_y() * self.axis_size_x(),
            self.axis_size_x(),
            1,
        ]
    }

    fn linear_space_index_unchecked<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> usize {
        let real_idx = index.into();
        debug_assert!(self.contains(&real_idx));
        let GridIdx([z, y, x]) = real_idx;

        let [stride_z, stride_y, stride_x] = self.strides();
        z as usize * stride_z + y as usize * stride_y + x as usize * stride_x
    }

    fn linear_space_index<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> Result<usize> {
        let real_index = index.into();
        ensure!(
            self.contains(&real_index),
            error::GridIndexOutOfBounds {
                index: Vec::from(real_index.0),
                min_index: Vec::from(self.min_index().0),
                max_index: Vec::from(self.max_index().0)
            }
        );
        Ok(self.linear_space_index_unchecked(real_index))
    }

    fn grid_idx_unchecked(&self, linear_idx: usize) -> GridIdx<[isize; 3]> {
        let [stride_z, stride_y, _stride_x] = self.strides();
        let grid_idx = GridIdx([
            (linear_idx / stride_z) as isize,
            ((linear_idx % stride_z) / stride_y) as isize,
            (linear_idx % stride_y) as isize,
        ]);
        debug_assert!(self.contains(&grid_idx));
        grid_idx
    }
}

impl GridBounds for GridShape3D {
    type IndexArray = [isize; 3];

    fn min_index(&self) -> GridIdx<Self::IndexArray> {
        GridIdx::<[isize; 3]>::zero()
    }

    fn max_index(&self) -> GridIdx<Self::IndexArray> {
        let [z_size, y_size, x_size] = self.shape_array;
        GridIdx([z_size as isize, y_size as isize, x_size as isize]) - 1
    }
}

/// Method to generate an `Iterator` over all `GridIdx2D` in `GridBounds`
pub fn grid_idx_iter_2d<B>(bounds: &B) -> impl Iterator<Item = GridIdx2D>
where
    B: GridBounds<IndexArray = [isize; 2]>,
{
    let GridIdx([y_s, x_s]) = bounds.min_index();
    let GridIdx([y_e, x_e]) = bounds.max_index();

    (y_s..=y_e).flat_map(move |y| (x_s..=x_e).map(move |x| [y, x].into()))
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Grid<D, T> {
    pub shape: D,
    pub data: Vec<T>,
}

pub type Grid1D<T> = Grid<GridShape1D, T>;
pub type Grid2D<T> = Grid<GridShape2D, T>;
pub type Grid3D<T> = Grid<GridShape3D, T>;

impl<D, T> Grid<D, T>
where
    D: GridSize,
    T: Clone,
{
    /// Creates a new `Grid`
    ///
    /// # Errors
    ///
    /// This constructor fails if the data container's capacity is different from the grid's dimension number
    ///
    pub fn new(shape: D, data: Vec<T>) -> Result<Self> {
        ensure!(
            shape.number_of_elements() == data.len(),
            error::DimensionCapacityDoesNotMatchDataCapacity {
                dimension_cap: shape.number_of_elements(),
                data_cap: data.len()
            }
        );

        Ok(Self { shape, data })
    }

    #[allow(clippy::missing_panics_doc)]
    pub fn new_filled(shape: D, fill_value: T) -> Self {
        let data = vec![fill_value; shape.number_of_elements()];
        Self::new(shape, data).expect("sizes must match")
    }

    pub fn inner_ref(&self) -> &Vec<T> {
        &self.data
    }

    /// reverse this grid along the y-axis. Returns an "up-side-down" `Grid`.
    #[must_use]
    pub fn reversed_y_axis_grid(&self) -> Grid<D, T>
    where
        D: Clone,
    {
        let mut reversed_data_vec = Vec::with_capacity(self.data.len());

        self.data
            .chunks(self.shape.axis_size_x() * self.shape.axis_size_y())
            .for_each(|big_chunk| {
                big_chunk
                    .chunks_exact(self.shape.axis_size_x())
                    .rev()
                    .for_each(|c| reversed_data_vec.extend_from_slice(c));
            });

        Grid {
            data: reversed_data_vec,
            shape: self.shape.clone(),
        }
    }
}

impl<D, T> GridSize for Grid<D, T>
where
    D: GridSize + GridSpaceToLinearSpace,
{
    type ShapeArray = D::ShapeArray;

    const NDIM: usize = D::NDIM;

    fn axis_size(&self) -> Self::ShapeArray {
        self.shape.axis_size()
    }

    fn number_of_elements(&self) -> usize {
        self.shape.number_of_elements()
    }
}

impl<T, D> GridIndexAccess<T, usize> for Grid<D, T>
where
    T: Copy,
{
    fn get_at_grid_index(&self, grid_index: usize) -> Result<T> {
        ensure!(
            grid_index < self.data.len(),
            error::LinearIndexOutOfBounds {
                index: grid_index,
                max_index: self.data.len(),
            }
        );
        Ok(self.get_at_grid_index_unchecked(grid_index))
    }

    fn get_at_grid_index_unchecked(&self, grid_index: usize) -> T {
        self.data[grid_index]
    }
}

impl<T, D, I, A> GridIndexAccess<T, I> for Grid<D, T>
where
    D: GridSize + GridSpaceToLinearSpace<IndexArray = A> + GridBounds<IndexArray = A>,
    I: Into<GridIdx<A>>,
    A: AsRef<[isize]> + Into<GridIdx<A>> + Clone,
    T: Copy,
{
    fn get_at_grid_index(&self, grid_index: I) -> Result<T> {
        let index = grid_index.into();
        ensure!(
            self.shape.contains(&index),
            error::GridIndexOutOfBounds {
                index: index.as_slice(),
                min_index: self.shape.min_index().as_slice(),
                max_index: self.shape.max_index().as_slice()
            }
        );
        Ok(self.get_at_grid_index_unchecked(index))
    }

    fn get_at_grid_index_unchecked(&self, grid_index: I) -> T {
        let index = grid_index.into();
        let lin_space_idx = self.shape.linear_space_index_unchecked(index);
        self.get_at_grid_index_unchecked(lin_space_idx)
    }
}

impl<T, D> GridIndexAccessMut<T, usize> for Grid<D, T>
where
    T: Copy,
{
    fn set_at_grid_index(&mut self, grid_index: usize, value: T) -> Result<()> {
        ensure!(
            grid_index < self.data.len(),
            error::LinearIndexOutOfBounds {
                index: grid_index,
                max_index: self.data.len(),
            }
        );
        self.set_at_grid_index_unchecked(grid_index, value);
        Ok(())
    }

    fn set_at_grid_index_unchecked(&mut self, grid_index: usize, value: T) {
        self.data[grid_index] = value;
    }
}

impl<T, D, I, A> GridIndexAccessMut<T, I> for Grid<D, T>
where
    D: GridSize + GridSpaceToLinearSpace<IndexArray = A> + GridBounds<IndexArray = A>,
    I: Into<GridIdx<A>>,
    A: AsRef<[isize]> + Into<GridIdx<A>> + Clone,
    T: Copy,
{
    fn set_at_grid_index(&mut self, grid_index: I, value: T) -> Result<()> {
        let index = grid_index.into();
        ensure!(
            self.shape.contains(&index),
            error::GridIndexOutOfBounds {
                index: index.as_slice(),
                min_index: self.shape.min_index().as_slice(),
                max_index: self.shape.max_index().as_slice()
            }
        );
        self.set_at_grid_index_unchecked(index, value);
        Ok(())
    }

    fn set_at_grid_index_unchecked(&mut self, grid_index: I, value: T) {
        let index = grid_index.into();
        let lin_space_idx = self.shape.linear_space_index_unchecked(index);
        self.data[lin_space_idx] = value;
    }
}

impl<T, D> GridBounds for Grid<D, T>
where
    D: GridBounds,
{
    type IndexArray = D::IndexArray;
    fn min_index(&self) -> GridIdx<<Self as GridBounds>::IndexArray> {
        self.shape.min_index()
    }
    fn max_index(&self) -> GridIdx<<Self as GridBounds>::IndexArray> {
        self.shape.max_index()
    }
}

impl<D, T> GridShapeAccess for Grid<D, T>
where
    D: GridSize,
    D::ShapeArray: Into<GridShape<D::ShapeArray>>,
    T: Copy,
{
    type ShapeArray = D::ShapeArray;

    fn grid_shape_array(&self) -> Self::ShapeArray {
        self.shape.axis_size()
    }
}

impl<D, T, I> ChangeGridBounds<I> for Grid<D, T>
where
    I: AsRef<[isize]> + Clone,
    D: GridBounds<IndexArray = I> + Clone,
    T: Clone,
    GridBoundingBox<I>: GridSize,
    GridIdx<I>: Add<Output = GridIdx<I>> + From<I>,
{
    type Output = Grid<GridBoundingBox<I>, T>;

    fn shift_by_offset(self, offset: GridIdx<I>) -> Self::Output {
        Grid {
            shape: self.shift_bounding_box(offset),
            data: self.data,
        }
    }

    fn set_grid_bounds(self, bounds: GridBoundingBox<I>) -> Result<Self::Output> {
        Grid::new(bounds, self.data)
    }
}

impl<D, P> ByteSize for Grid<D, P>
where
    Vec<P>: ByteSize,
{
    fn heap_byte_size(&self) -> usize {
        self.data.heap_byte_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::{Grid2D, Grid3D, GridIndexAccess, GridIndexAccessMut};
    use crate::raster::{
        BoundedGrid, GridBoundingBox1D, GridBoundingBox2D, GridBoundingBox3D, GridBounds,
        GridContains, GridIdx, GridIdx1D, GridIdx2D, GridIdx3D, GridShape, GridShape1D,
        GridShape2D, GridShape3D, GridSpaceToLinearSpace,
    };

    #[test]
    fn simple_raster_2d() {
        let dim = [3, 2];
        let data = vec![1, 2, 3, 4, 5, 6];
        Grid2D::new(dim.into(), data).unwrap();
    }

    #[test]
    fn simple_raster_2d_at_tuple() {
        let index = [1, 1];
        let dim = [3, 2].into();
        let data = vec![1, 2, 3, 4, 5, 6];
        let raster2d = Grid2D::new(dim, data).unwrap();
        assert_eq!(raster2d.get_at_grid_index(index).unwrap(), 4);
    }

    #[test]
    fn simple_raster_2d_at_arr() {
        let index = [1, 1];
        let dim = [3, 2].into();
        let data = vec![1, 2, 3, 4, 5, 6];
        let raster2d = Grid2D::new(dim, data).unwrap();
        let value = raster2d.get_at_grid_index(index).unwrap();
        assert_eq!(value, 4);
    }

    #[test]
    fn simple_raster_2d_set_at_tuple() {
        let index = [1, 1];
        let dim = [3, 2].into();
        let data = vec![1, 2, 3, 4, 5, 6];
        let mut raster2d = Grid2D::new(dim, data).unwrap();

        raster2d.set_at_grid_index(index, 9).unwrap();
        let value = raster2d.get_at_grid_index(index).unwrap();
        assert_eq!(value, 9);
        assert_eq!(raster2d.data, [1, 2, 3, 9, 5, 6]);
    }

    #[test]
    fn simple_raster_3d() {
        let dim = [3, 2, 1];
        let data = vec![1, 2, 3, 4, 5, 6];
        Grid3D::new(dim.into(), data).unwrap();
    }

    #[test]
    fn simple_raster_3d_at_tuple() {
        let index = [1, 1, 0];
        let dim = [3, 2, 1].into();
        let data = vec![1, 2, 3, 4, 5, 6];
        let raster3d = Grid3D::new(dim, data).unwrap();
        assert_eq!(raster3d.get_at_grid_index(index).unwrap(), 4);
    }

    #[test]
    fn simple_raster_3d_at_arr() {
        let index = [1, 1, 0];
        let dim = [3, 2, 1].into();
        let data = vec![1, 2, 3, 4, 5, 6];
        let raster3d = Grid3D::new(dim, data).unwrap();
        let value = raster3d.get_at_grid_index(index).unwrap();
        assert_eq!(value, 4);
    }

    #[test]
    fn simple_raster_3d_set_at_tuple() {
        let index = [1, 1, 0];
        let dim = [3, 2, 1].into();
        let data = vec![1, 2, 3, 4, 5, 6];
        let mut raster3d = Grid3D::new(dim, data).unwrap();

        raster3d.set_at_grid_index(index, 9).unwrap();
        let value = raster3d.get_at_grid_index(index).unwrap();
        assert_eq!(value, 9);
        assert_eq!(raster3d.data, [1, 2, 3, 9, 5, 6]);
    }

    #[test]
    fn grid_bounds_2d() {
        let dim = [3, 2].into();
        let data = vec![1, 2, 3, 4, 5, 6];
        let raster2d = Grid2D::new(dim, data).unwrap();

        assert_eq!(raster2d.min_index(), GridIdx([0, 0]));
        assert_eq!(raster2d.max_index(), GridIdx([2, 1]));

        let exp_bbox = GridBoundingBox2D::new([0, 0], [2, 1]).unwrap();
        assert_eq!(raster2d.bounding_box(), exp_bbox);
    }

    #[test]
    fn grid_shape_1d() {
        let grid_shp: GridShape1D = [3].into();

        assert_eq!(grid_shp.min_index(), GridIdx([0]));
        assert_eq!(grid_shp.max_index(), GridIdx([2]));

        assert!(grid_shp.contains(&GridIdx1D::from([0])));
        assert!(grid_shp.contains(&GridIdx1D::from([2])));
        assert!(!grid_shp.contains(&GridIdx1D::from([3])));

        let exp_bbox = GridBoundingBox1D::new([0], [2]).unwrap();
        assert_eq!(grid_shp.bounding_box(), exp_bbox);
    }

    #[test]
    fn grid_shape_2d() {
        let grid_shp: GridShape2D = [3, 2].into();

        assert_eq!(grid_shp.min_index(), GridIdx([0, 0]));
        assert_eq!(grid_shp.max_index(), GridIdx([2, 1]));

        assert!(grid_shp.contains(&GridIdx2D::from([0, 0])));
        assert!(grid_shp.contains(&GridIdx2D::from([2, 1])));
        assert!(!grid_shp.contains(&GridIdx2D::from([3, 0])));
        assert!(!grid_shp.contains(&GridIdx2D::from([2, 2])));

        let exp_bbox = GridBoundingBox2D::new([0, 0], [2, 1]).unwrap();
        assert_eq!(grid_shp.bounding_box(), exp_bbox);
    }

    #[test]
    fn grid_shape_3d() {
        let grid_shp: GridShape3D = [3, 2, 2].into();

        assert_eq!(grid_shp.min_index(), GridIdx([0, 0, 0]));
        assert_eq!(grid_shp.max_index(), GridIdx([2, 1, 1]));

        assert!(grid_shp.contains(&GridIdx3D::from([0, 0, 0])));
        assert!(grid_shp.contains(&GridIdx3D::from([2, 1, 1])));
        assert!(!grid_shp.contains(&GridIdx3D::from([3, 0, 0])));
        assert!(!grid_shp.contains(&GridIdx3D::from([2, 1, 2])));
        assert!(!grid_shp.contains(&GridIdx3D::from([2, 2, 1])));

        let exp_bbox = GridBoundingBox3D::new([0, 0, 0], [2, 1, 1]).unwrap();
        assert_eq!(grid_shp.bounding_box(), exp_bbox);
    }

    #[test]
    fn grid_shape_1d_linear_space_and_back() {
        let a = GridShape::new([42]);

        let l = a.linear_space_index([1]).unwrap();
        assert_eq!(l, 1);
        assert_eq!(a.grid_idx(l).unwrap(), [1].into());

        let l = a.linear_space_index([42]);
        assert!(l.is_err());
    }

    #[test]
    fn grid_shape_2d_linear_space_and_back() {
        let a = GridShape::new([42, 42]);
        let l = a.linear_space_index([1, 1]).unwrap();
        assert_eq!(l, 43);
        assert_eq!(a.grid_idx(l).unwrap(), [1, 1].into());

        let l = a.linear_space_index([42, 0]);
        assert!(l.is_err());

        let l = a.linear_space_index([0, 42]);
        assert!(l.is_err());
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn grid_shape_3d_linear_space_and_back() {
        let a = GridShape::new([42, 42, 42]);
        let l = a.linear_space_index([1, 1, 1]).unwrap();
        assert_eq!(l, 1 * 42 * 42 + 1 * 42 + 1);
        assert_eq!(a.grid_idx(l).unwrap(), [1, 1, 1].into());

        let l = a.linear_space_index([42, 0, 0]);
        assert!(l.is_err());

        let l = a.linear_space_index([0, 42, 0]);
        assert!(l.is_err());

        let l = a.linear_space_index([0, 0, 42]);
        assert!(l.is_err());
    }

    #[test]
    fn grid_shape_1d_linear_space_unchecked_and_back() {
        let a = GridShape::new([42]);
        let l = a.linear_space_index_unchecked([1]);
        assert_eq!(l, 1);
        assert_eq!(a.grid_idx_unchecked(l), [1].into());
    }

    #[test]
    fn grid_shape_2d_linear_space_unchecked_and_back() {
        let a = GridShape::new([42, 42]);
        let l = a.linear_space_index_unchecked([1, 1]);
        assert_eq!(l, 43);
        assert_eq!(a.grid_idx_unchecked(l), [1, 1].into());
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn grid_shape_3d_linear_space_unchecked_and_back() {
        let a = GridShape::new([42, 42, 42]);
        let l = a.linear_space_index_unchecked([1, 1, 1]);
        assert_eq!(l, 1 * 42 * 42 + 1 * 42 + 1);
        assert_eq!(a.grid_idx_unchecked(l), [1, 1, 1].into());
    }

    #[test]
    fn reversed_y_axis_grid_2d() {
        let g2d = Grid2D::new([2, 3].into(), vec![1, 1, 1, 2, 2, 2]).unwrap();
        let g2d_flipped_y = g2d.reversed_y_axis_grid();
        assert_eq!(g2d_flipped_y.shape, [2, 3].into());
        assert_eq!(g2d_flipped_y.data, vec![2, 2, 2, 1, 1, 1]);
    }

    #[test]
    fn reversed_y_axis_grid_3d() {
        let g2d = Grid3D::new([2, 2, 3].into(), vec![1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4]).unwrap();
        let g2d_flipped_y = g2d.reversed_y_axis_grid();
        assert_eq!(g2d_flipped_y.shape, [2, 2, 3].into());
        assert_eq!(g2d_flipped_y.data, vec![2, 2, 2, 1, 1, 1, 4, 4, 4, 3, 3, 3]);
    }

    #[test]
    fn size_of_grid() {
        // (8+8) = 16 bytes for the shape
        // 24 byte for the vec
        // 6 * 4 = 24 bytes for the data
        // 16 + 24 + 24 = 64 bytes
        assert_eq!(
            Grid2D::new([2, 3].into(), vec![1_i32, 1, 1, 2, 2, 2])
                .unwrap()
                .byte_size(),
            64
        );
        // additional 3 * 4 = 12 bytes for the data
        assert_eq!(
            Grid2D::new([3, 3].into(), vec![1_i32, 1, 1, 2, 2, 2, 3, 3, 3])
                .unwrap()
                .byte_size(),
            76
        );
    }
}
