use crate::primitives::error;
use crate::primitives::{Coordinate2D, Geometry};
use crate::util::arrow::ArrowTyped;
use crate::util::Result;
use snafu::ensure;

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

    pub fn points(&self) -> &[Coordinate2D] {
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

    pub fn points(&self) -> &[Coordinate2D] {
        self.point_coordinates
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
