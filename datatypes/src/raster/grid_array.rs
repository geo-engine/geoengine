use num_traits::{AsPrimitive, Zero};
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::error;
use crate::util::Result;

use super::{
    BoundedGrid, GridBoundingBox, GridBounds, GridContains, GridIdx, GridIndexAccess,
    GridIndexAccessMut, GridSize, GridSpaceToLinearSpace,
};

#[derive(PartialEq, Debug, Copy, Clone, Serialize, Deserialize)]
pub struct ArrayShape<A>
where
    A: AsRef<[usize]>,
{
    pub shape_array: A,
}

impl<A> ArrayShape<A>
where
    A: AsRef<[usize]>,
{
    pub fn new(shape: A) -> Self {
        Self { shape_array: shape }
    }

    pub fn inner_ref(&self) -> &A {
        &self.shape_array
    }

    pub fn into_inner(self) -> A {
        self.shape_array
    }
}

pub type ArrayShape1D = ArrayShape<[usize; 1]>;
pub type ArrayShape2D = ArrayShape<[usize; 2]>;
pub type ArrayShape3D = ArrayShape<[usize; 3]>;

impl From<[usize; 1]> for ArrayShape1D {
    fn from(shape: [usize; 1]) -> Self {
        ArrayShape1D { shape_array: shape }
    }
}

impl From<[usize; 2]> for ArrayShape2D {
    fn from(shape: [usize; 2]) -> Self {
        ArrayShape2D { shape_array: shape }
    }
}

impl From<[usize; 3]> for ArrayShape3D {
    fn from(shape: [usize; 3]) -> Self {
        ArrayShape3D { shape_array: shape }
    }
}

impl GridSize for ArrayShape1D {
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

impl GridSpaceToLinearSpace for ArrayShape1D {
    type IndexArray = [isize; 1];

    fn strides(&self) -> Self::ShapeArray {
        [1]
    }

    fn linear_space_index_unchecked<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> usize {
        let GridIdx([x]) = index.into();
        let [stride_x] = self.strides();
        x as usize * stride_x
    }

    fn linear_space_index<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> Result<usize> {
        let real_index = index.into();
        ensure!(
            self.bounding_box().contains(real_index),
            error::GridIndexOutOfBounds {
                index: Vec::from(real_index.0),
                min_index: Vec::from(self.min_index().0),
                max_index: Vec::from(self.max_index().0)
            }
        );

        Ok(self.linear_space_index_unchecked(real_index))
    }
}

impl GridBounds for ArrayShape1D {
    type IndexArray = [isize; 1];

    fn min_index(&self) -> GridIdx<Self::IndexArray> {
        GridIdx::<[isize; 1]>::zero()
    }

    fn max_index(&self) -> GridIdx<Self::IndexArray> {
        let [x_size] = self.shape_array;
        GridIdx([x_size as isize]) - 1
    }
}

impl GridSize for ArrayShape2D {
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

impl GridSpaceToLinearSpace for ArrayShape2D {
    type IndexArray = [isize; 2];

    fn strides(&self) -> Self::ShapeArray {
        [self.axis_size_x(), 1]
    }

    fn linear_space_index_unchecked<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> usize {
        let GridIdx([y, x]) = index.into();
        let [stride_y, stride_x] = self.strides();
        y as usize * stride_y + x as usize * stride_x
    }

    fn linear_space_index<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> Result<usize> {
        let real_index = index.into();
        ensure!(
            self.bounding_box().contains(real_index),
            error::GridIndexOutOfBounds {
                index: Vec::from(real_index.0),
                min_index: Vec::from(self.min_index().0),
                max_index: Vec::from(self.max_index().0)
            }
        );
        Ok(self.linear_space_index_unchecked(real_index))
    }
}

impl GridBounds for ArrayShape2D {
    type IndexArray = [isize; 2];

    fn min_index(&self) -> GridIdx<Self::IndexArray> {
        GridIdx::<[isize; 2]>::zero()
    }

    fn max_index(&self) -> GridIdx<Self::IndexArray> {
        let [y_size, x_size] = self.shape_array;
        GridIdx([y_size as isize, x_size as isize]) - 1
    }
}

impl GridSize for ArrayShape3D {
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

impl GridSpaceToLinearSpace for ArrayShape3D {
    type IndexArray = [isize; 3];

    fn strides(&self) -> Self::ShapeArray {
        [self.axis_size_y(), self.axis_size_x(), 1]
    }

    fn linear_space_index_unchecked<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> usize {
        let GridIdx([z, y, x]) = index.into();
        let [stride_z, stride_y, stride_x] = self.strides();
        z as usize * stride_z * y as usize * stride_y + x as usize * stride_x
    }

    fn linear_space_index<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> Result<usize> {
        let real_index = index.into();
        ensure!(
            self.bounding_box().contains(real_index),
            error::GridIndexOutOfBounds {
                index: Vec::from(real_index.0),
                min_index: Vec::from(self.min_index().0),
                max_index: Vec::from(self.max_index().0)
            }
        );
        Ok(self.linear_space_index_unchecked(real_index))
    }
}

impl GridBounds for ArrayShape3D {
    type IndexArray = [isize; 3];

    fn min_index(&self) -> GridIdx<Self::IndexArray> {
        GridIdx::<[isize; 3]>::zero()
    }

    fn max_index(&self) -> GridIdx<Self::IndexArray> {
        let [z_size, y_size, x_size] = self.shape_array;
        GridIdx([z_size as isize, y_size as isize, x_size as isize]) - 1
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct GridArray<D, T> {
    pub shape: D,
    pub data: Vec<T>,
    pub no_data_value: Option<T>,
}

pub type GridArray1D<T> = GridArray<ArrayShape1D, T>;
pub type GridArray2D<T> = GridArray<ArrayShape2D, T>;
pub type GridArray3D<T> = GridArray<ArrayShape3D, T>;

impl<D, T> GridArray<D, T>
where
    D: GridSize + GridSpaceToLinearSpace,
    T: Clone,
{
    /// Creates a new `GridArray`
    ///
    /// # Errors
    ///
    /// This constructor fails if the data container's capacity is different from the grid's dimension number
    ///
    pub fn new(shape: D, data: Vec<T>, no_data_value: Option<T>) -> Result<Self> {
        ensure!(
            shape.number_of_elements() == data.len(),
            error::DimensionCapacityDoesNotMatchDataCapacity {
                dimension_cap: shape.number_of_elements(),
                data_cap: data.len()
            }
        );

        Ok(Self {
            shape,
            data,
            no_data_value,
        })
    }

    pub fn new_filled(shape: D, fill_value: T, no_data_value: Option<T>) -> Self {
        let data = vec![fill_value; shape.number_of_elements()];
        Self::new(shape, data, no_data_value).expect("sizes must match")
    }

    /// Converts the data type of the raster by converting it pixel-wise
    pub fn convert_dtype<To>(self) -> GridArray<D, To>
    where
        T: AsPrimitive<To> + Copy + 'static,
        To: Copy + 'static,
    {
        GridArray::new(
            self.shape,
            self.data.iter().map(|&pixel| pixel.as_()).collect(),
            self.no_data_value.map(AsPrimitive::as_),
        )
        .expect("grid array type conversion cannot fail")
    }

    pub fn inner_ref(&self) -> &Vec<T> {
        &self.data
    }
}

impl<D, T> GridSize for GridArray<D, T>
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

impl<T, D, I> GridIndexAccess<T, I> for GridArray<D, T>
where
    D: GridSize + GridSpaceToLinearSpace,
    I: Into<GridIdx<D::IndexArray>>,
    T: Copy,
{
    fn get_at_grid_index(&self, grid_index: I) -> Result<T> {
        Ok(self.get_at_grid_index_unchecked(grid_index))
    }

    fn get_at_grid_index_unchecked(&self, grid_index: I) -> T {
        let index = grid_index.into();
        let lin_space_idx = self.shape.linear_space_index_unchecked(index);
        self.data[lin_space_idx]
    }
}

impl<T, D, I> GridIndexAccessMut<T, I> for GridArray<D, T>
where
    D: GridSize + GridSpaceToLinearSpace,
    I: Into<GridIdx<D::IndexArray>>,
    T: Copy,
{
    fn set_at_grid_index(&mut self, grid_index: I, value: T) -> Result<()> {
        self.set_at_grid_index_unchecked(grid_index, value);
        Ok(())
    }

    fn set_at_grid_index_unchecked(&mut self, grid_index: I, value: T) {
        let index = grid_index.into();
        let lin_space_idx = self.shape.linear_space_index_unchecked(index);
        self.data[lin_space_idx] = value;
    }
}

impl<T, D> BoundedGrid for GridArray<D, T>
where
    D: GridSize + GridBounds,
{
    type IndexArray = D::IndexArray;

    fn bounding_box(&self) -> GridBoundingBox<Self::IndexArray> {
        GridBoundingBox::new_unchecked(self.shape.min_index(), self.shape.max_index())
    }
}

#[cfg(test)]
mod tests {
    use super::{GridArray2D, GridIndexAccess, GridIndexAccessMut};

    #[test]
    fn simple_raster_2d() {
        let dim = [3, 2];
        let data = vec![1, 2, 3, 4, 5, 6];
        GridArray2D::new(dim.into(), data, None).unwrap();
    }

    #[test]
    fn simple_raster_2d_at_tuple() {
        let index = [1, 1];
        let dim = [3, 2].into();
        let data = vec![1, 2, 3, 4, 5, 6];
        let raster2d = GridArray2D::new(dim, data, None).unwrap();
        assert_eq!(raster2d.get_at_grid_index(index).unwrap(), 4);
    }

    #[test]
    fn simple_raster_2d_at_arr() {
        let index = [1, 1];
        let dim = [3, 2].into();
        let data = vec![1, 2, 3, 4, 5, 6];
        let raster2d = GridArray2D::new(dim, data, None).unwrap();
        let value = raster2d.get_at_grid_index(index).unwrap();
        assert_eq!(value, 4);
    }

    #[test]
    fn simple_raster_2d_set_at_tuple() {
        let index = [1, 1];
        let dim = [3, 2].into();
        let data = vec![1, 2, 3, 4, 5, 6];
        let mut raster2d = GridArray2D::new(dim, data, None).unwrap();

        raster2d.set_at_grid_index(index, 9).unwrap();
        let value = raster2d.get_at_grid_index(index).unwrap();
        assert_eq!(value, 9);
        assert_eq!(raster2d.data, [1, 2, 3, 9, 5, 6]);
    }
}
