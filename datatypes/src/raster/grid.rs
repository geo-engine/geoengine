use std::{
    fmt::Debug,
    ops::{Add, Div, Mul, Rem, Sub},
};

use num_traits::{One, Zero};
use serde::{Deserialize, Serialize};

use crate::util::Result;

use super::grid_bounds::GridBoundingBox;

pub trait GridSize {
    /// An array with one entry for each axis of the `GridRect`
    type ShapeArray: AsRef<[usize]>;

    /// The number of axis of the `GridRect`
    const NDIM: usize;

    // size per axis in [.., y, x] order
    fn axis_size(&self) -> Self::ShapeArray;
    /// Size of the x-axis
    fn axis_size_x(&self) -> usize {
        match *self.axis_size().as_ref() {
            [] => 0,
            [.., a] => a,
        }
    }

    /// Size of the y-axis
    fn axis_size_y(&self) -> usize {
        match *self.axis_size().as_ref() {
            [] => 0,
            [_] => 1,
            [.., b, _] => b,
        }
    }

    /// The number of elements the `GridRect` contains. This is bounded by size(usize).
    fn number_of_elements(&self) -> usize;
}
/// A n-dimensional rectangle defining the bounds of a grid
pub trait GridBounds: GridSize {
    /// An array with one entry for each axis of the `GridRect`
    type IndexArray: AsRef<[isize]> + Into<GridIdx<Self::IndexArray>>;

    /// The minimal valid index. This has the min valid value for each axis in the `GridRect`.
    fn min_index(&self) -> GridIdx<Self::IndexArray>;
    /// The max valid index. This has the max valid value for each axis in the `GridRect`.
    fn max_index(&self) -> GridIdx<Self::IndexArray>;
}

pub trait BoundedGrid {
    /// An array with one entry for each axis of the `GridRect`
    type IndexArray: AsRef<[isize]> + Into<GridIdx<Self::IndexArray>>;
    fn bounding_box(&self) -> GridBoundingBox<Self::IndexArray>;
}

pub trait GridContains<Rhs = Self> {
    fn contains(&self, rhs: Rhs) -> bool;
}

pub trait GridIntersection<Rhs = Self, Out = Self> {
    fn intersection(&self, other: &Rhs) -> Option<Out>;
}

pub trait GridSpaceToLinearSpace: GridSize {
    /// An array with one entry for each axis of the `GridRect`
    type IndexArray: AsRef<[isize]>;
    /// The number of axis of the `GridRect`

    /// Strides indicate how many linear space elements the next element at the same position of any axis is away.
    fn strides(&self) -> Self::ShapeArray;
    /// Calculate the zero based linear space location of an index in a `GridRect`.
    fn linear_space_index_unchecked<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> usize;
    /// Calculate the zero based linear space location of an index in a `GridRect`.
    /// # Errors
    /// This method fails if the grid index is out of bounds.
    fn linear_space_index<I: Into<GridIdx<Self::IndexArray>>>(&self, index: I) -> Result<usize>;
}

pub trait GridIndexAccessMut<T, I> {
    /// Sets the value at a grid index
    ///
    /// # Errors
    ///
    /// The method fails if the grid index is out of bounds.
    ///
    fn set_at_grid_index(&mut self, grid_index: I, value: T) -> Result<()>;
    fn set_at_grid_index_unchecked(&mut self, grid_index: I, value: T);
}

pub trait GridIndexAccess<T, I> {
    /// Gets a reference to the value at a grid index
    ///
    /// The method fails if the grid index is out of bounds.
    ///
    fn get_at_grid_index(&self, grid_index: I) -> Result<T>;

    fn get_at_grid_index_unchecked(&self, grid_index: I) -> T;
}

///
/// The grid index. This is a wrapper for arrays wih added methods and traits, e.g. Add, Sub...
///
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub struct GridIdx<A>(pub A)
where
    A: AsRef<[isize]>;

pub type GridIdx1D = GridIdx<[isize; 1]>;
pub type GridIdx2D = GridIdx<[isize; 2]>;
pub type GridIdx3D = GridIdx<[isize; 3]>;

impl<A> GridIdx<A>
where
    A: AsRef<[isize]>,
{
    pub fn inner(&self) -> &A {
        &self.0
    }

    pub fn as_slice(&self) -> &[isize] {
        self.inner().as_ref()
    }

    pub fn new(inner: A) -> Self {
        GridIdx(inner)
    }

    pub fn get(&self, index: usize) -> Option<isize> {
        self.as_slice().get(index).copied()
    }
}

impl From<[isize; 1]> for GridIdx1D {
    fn from(array: [isize; 1]) -> GridIdx1D {
        GridIdx(array)
    }
}

impl From<isize> for GridIdx1D {
    fn from(scalar: isize) -> GridIdx1D {
        GridIdx([scalar])
    }
}

impl From<[isize; 2]> for GridIdx2D {
    fn from(array: [isize; 2]) -> GridIdx2D {
        GridIdx(array)
    }
}

impl From<isize> for GridIdx2D {
    fn from(scalar: isize) -> GridIdx2D {
        GridIdx([scalar, scalar])
    }
}

impl From<[isize; 3]> for GridIdx3D {
    fn from(array: [isize; 3]) -> GridIdx3D {
        GridIdx(array)
    }
}

impl From<isize> for GridIdx3D {
    fn from(scalar: isize) -> GridIdx3D {
        GridIdx([scalar, scalar, scalar])
    }
}

impl One for GridIdx1D {
    fn one() -> Self {
        Self::from(1)
    }
}

impl One for GridIdx2D {
    fn one() -> Self {
        Self::from(1)
    }
}

impl One for GridIdx3D {
    fn one() -> Self {
        Self::from(1)
    }
}

impl Zero for GridIdx1D {
    fn zero() -> Self {
        Self::from(0)
    }

    fn is_zero(&self) -> bool {
        let [a] = self.0;
        a == 0
    }
}

impl Zero for GridIdx2D {
    fn zero() -> Self {
        Self::from(0)
    }

    fn is_zero(&self) -> bool {
        let [a, b] = self.0;
        a == 0 && b == 0
    }
}

impl Zero for GridIdx3D {
    fn zero() -> Self {
        Self::from(0)
    }

    fn is_zero(&self) -> bool {
        let [a, b, c] = self.0;
        a == 0 && b == 0 && c == 0
    }
}

impl<I> Sub<I> for GridIdx1D
where
    I: Into<GridIdx1D>,
{
    type Output = Self;

    fn sub(self, rhs: I) -> Self::Output {
        let GridIdx([a]) = self;
        let GridIdx([a_other]) = rhs.into();
        GridIdx([a - a_other])
    }
}

impl<I> Sub<I> for GridIdx2D
where
    I: Into<GridIdx2D>,
{
    type Output = Self;

    fn sub(self, rhs: I) -> Self::Output {
        let GridIdx([a, b]) = self;
        let GridIdx([a_other, b_other]) = rhs.into();
        GridIdx([a - a_other, b - b_other])
    }
}

impl<I> Sub<I> for GridIdx3D
where
    I: Into<GridIdx3D>,
{
    type Output = Self;

    fn sub(self, rhs: I) -> Self::Output {
        let GridIdx([a, b, c]) = self;
        let GridIdx([a_other, b_other, c_other]) = rhs.into();
        GridIdx([a - a_other, b - b_other, c - c_other])
    }
}

impl<I> Add<I> for GridIdx1D
where
    I: Into<GridIdx1D>,
{
    type Output = Self;

    fn add(self, rhs: I) -> Self::Output {
        let GridIdx([a]) = self;
        let GridIdx([a_other]) = rhs.into();
        GridIdx([a + a_other])
    }
}

impl<I> Add<I> for GridIdx2D
where
    I: Into<GridIdx2D>,
{
    type Output = Self;

    fn add(self, rhs: I) -> Self::Output {
        let GridIdx([a, b]) = self;
        let GridIdx([a_other, b_other]) = rhs.into();
        GridIdx([a + a_other, b + b_other])
    }
}

impl<I> Add<I> for GridIdx3D
where
    I: Into<GridIdx3D>,
{
    type Output = Self;

    fn add(self, rhs: I) -> Self::Output {
        let GridIdx([a, b, c]) = self;
        let GridIdx([a_other, b_other, c_other]) = rhs.into();
        GridIdx([a + a_other, b + b_other, c + c_other])
    }
}

impl<I> Mul<I> for GridIdx1D
where
    I: Into<GridIdx1D>,
{
    type Output = Self;

    fn mul(self, rhs: I) -> Self::Output {
        let GridIdx([a]) = self;
        let GridIdx([a_other]) = rhs.into();
        GridIdx([a * a_other])
    }
}

impl<I> Mul<I> for GridIdx2D
where
    I: Into<GridIdx2D>,
{
    type Output = Self;

    fn mul(self, rhs: I) -> Self::Output {
        let GridIdx([a, b]) = self;
        let GridIdx([a_other, b_other]) = rhs.into();
        GridIdx([a * a_other, b * b_other])
    }
}

impl<I> Mul<I> for GridIdx3D
where
    I: Into<GridIdx3D>,
{
    type Output = Self;

    fn mul(self, rhs: I) -> Self::Output {
        let GridIdx([a, b, c]) = self;
        let GridIdx([a_other, b_other, c_other]) = rhs.into();
        GridIdx([a * a_other, b * b_other, c * c_other])
    }
}

impl<I> Div<I> for GridIdx1D
where
    I: Into<GridIdx1D>,
{
    type Output = Self;

    fn div(self, rhs: I) -> Self::Output {
        let GridIdx([a]) = self;
        let GridIdx([a_other]) = rhs.into();
        GridIdx([a / a_other])
    }
}

impl<I> Div<I> for GridIdx2D
where
    I: Into<GridIdx2D>,
{
    type Output = Self;

    fn div(self, rhs: I) -> Self::Output {
        let GridIdx([a, b]) = self;
        let GridIdx([a_other, b_other]) = rhs.into();
        GridIdx([a / a_other, b / b_other])
    }
}

impl<I> Div<I> for GridIdx3D
where
    I: Into<GridIdx3D>,
{
    type Output = Self;

    fn div(self, rhs: I) -> Self::Output {
        let GridIdx([a, b, c]) = self;
        let GridIdx([a_other, b_other, c_other]) = rhs.into();
        GridIdx([a / a_other, b / b_other, c / c_other])
    }
}

impl<I> Rem<I> for GridIdx1D
where
    I: Into<GridIdx1D>,
{
    type Output = Self;

    fn rem(self, rhs: I) -> Self::Output {
        let GridIdx([a]) = self;
        let GridIdx([a_other]) = rhs.into();
        GridIdx([a % a_other])
    }
}

impl<I> Rem<I> for GridIdx2D
where
    I: Into<GridIdx2D>,
{
    type Output = Self;

    fn rem(self, rhs: I) -> Self::Output {
        let GridIdx([a, b]) = self;
        let GridIdx([a_other, b_other]) = rhs.into();
        GridIdx([a % a_other, b % b_other])
    }
}

impl<I> Rem<I> for GridIdx3D
where
    I: Into<GridIdx3D>,
{
    type Output = Self;

    fn rem(self, rhs: I) -> Self::Output {
        let GridIdx([a, b, c]) = self;
        let GridIdx([a_other, b_other, c_other]) = rhs.into();
        GridIdx([a % a_other, b % b_other, c % c_other])
    }
}
