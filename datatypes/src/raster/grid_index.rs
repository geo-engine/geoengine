use std::ops::{Add, Div, Mul, Rem, Sub};

use num_traits::{One, Zero};
use serde::{Deserialize, Serialize};

///
/// The grid index struct. This is a wrapper for arrays with added methods and traits, e.g. Add, Sub...
///
#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
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
