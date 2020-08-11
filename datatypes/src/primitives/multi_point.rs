use crate::primitives::error;
use crate::primitives::{Coordinate2D, Geometry};
use crate::util::arrow::ArrowTyped;
use crate::util::Result;
use snafu::ensure;

/// A trait that allows a common access to lines of `MultiPoint`s and its references
pub trait MultiPointAccess {
    fn points(&self) -> &[Coordinate2D];
}

pub struct MultiPoint {
    coordinates: Vec<Coordinate2D>,
}

impl MultiPoint {
    pub fn new(coordinates: Vec<Coordinate2D>) -> Result<Self> {
        ensure!(!coordinates.is_empty(), error::UnallowedEmpty);

        Ok(Self::new_unchecked(coordinates))
    }

    pub(crate) fn new_unchecked(coordinates: Vec<Coordinate2D>) -> Self {
        Self { coordinates }
    }
}

impl MultiPointAccess for MultiPoint {
    fn points(&self) -> &[Coordinate2D] {
        &self.coordinates
    }
}

impl Geometry for MultiPoint {}

impl AsRef<[Coordinate2D]> for MultiPoint {
    fn as_ref(&self) -> &[Coordinate2D] {
        &self.coordinates
    }
}

impl From<Coordinate2D> for MultiPoint {
    fn from(point: Coordinate2D) -> Self {
        Self::new_unchecked(vec![point])
    }
}

impl ArrowTyped for MultiPoint {
    type ArrowArray = arrow::array::ListArray;
    type ArrowBuilder =
        arrow::array::ListBuilder<arrow::array::FixedSizeListBuilder<arrow::array::Float64Builder>>;

    fn arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::List(Box::new(arrow::datatypes::DataType::FixedSizeList(
            Box::new(arrow::datatypes::DataType::Float64),
            2,
        )))
    }

    fn arrow_builder(capacity: usize) -> Self::ArrowBuilder {
        arrow::array::ListBuilder::new(arrow::array::FixedSizeListBuilder::new(
            arrow::array::Float64Builder::new(capacity * 2),
            2,
        ))
    }
}

pub struct MultiPointRef<'g> {
    point_coordinates: &'g [Coordinate2D],
}

impl<'g> MultiPointRef<'g> {
    pub fn new(coordinates: &'g [Coordinate2D]) -> Result<Self> {
        ensure!(!coordinates.is_empty(), error::UnallowedEmpty);

        Ok(Self::new_unchecked(coordinates))
    }

    pub(crate) fn new_unchecked(coordinates: &'g [Coordinate2D]) -> Self {
        Self {
            point_coordinates: coordinates,
        }
    }
}

impl<'g> MultiPointAccess for MultiPointRef<'g> {
    fn points(&self) -> &[Coordinate2D] {
        &self.point_coordinates
    }
}

impl<'g> Into<geojson::Geometry> for MultiPointRef<'g> {
    fn into(self) -> geojson::Geometry {
        geojson::Geometry::new(match self.point_coordinates.len() {
            1 => {
                let floats: [f64; 2] = self.point_coordinates[0].into();
                geojson::Value::Point(floats.to_vec())
            }
            _ => geojson::Value::MultiPoint(
                self.point_coordinates
                    .iter()
                    .map(|&c| {
                        let floats: [f64; 2] = c.into();
                        floats.to_vec()
                    })
                    .collect(),
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access() {
        fn aggregate<T: MultiPointAccess>(multi_point: &T) -> Coordinate2D {
            let (x, y) = multi_point
                .points()
                .iter()
                .fold((0., 0.), |(x, y), c| (x + c.x, y + c.y));
            (x, y).into()
        }

        let coordinates = vec![(0.0, 0.1).into(), (1.0, 1.1).into()];
        let multi_point = MultiPoint::new(coordinates.clone()).unwrap();
        let multi_point_ref = MultiPointRef::new(&coordinates).unwrap();

        let Coordinate2D { x, y } = aggregate(&multi_point);
        float_cmp::approx_eq!(f64, x, 1.0);
        float_cmp::approx_eq!(f64, y, 1.2);
        assert_eq!(aggregate(&multi_point), aggregate(&multi_point_ref));
    }
}
