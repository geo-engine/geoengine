use crate::primitives::error;
use crate::primitives::{Coordinate2D, Geometry};
use crate::util::arrow::ArrowTyped;
use crate::util::Result;
use snafu::ensure;

/// A representation of a simple feature multi line string
#[derive(Debug, PartialEq)]
pub struct MultiLineString {
    coordinates: Vec<Vec<Coordinate2D>>,
}

impl MultiLineString {
    pub fn new(coordinates: Vec<Vec<Coordinate2D>>) -> Result<Self> {
        ensure!(
            !coordinates.is_empty() && coordinates.iter().all(|c| c.len() >= 2),
            error::UnallowedEmpty
        );

        Ok(Self::new_unchecked(coordinates))
    }

    pub(crate) fn new_unchecked(coordinates: Vec<Vec<Coordinate2D>>) -> Self {
        Self { coordinates }
    }

    pub fn lines(&self) -> &[Vec<Coordinate2D>] {
        &self.coordinates
    }
}

impl Geometry for MultiLineString {}

impl AsRef<[Vec<Coordinate2D>]> for MultiLineString {
    fn as_ref(&self) -> &[Vec<Coordinate2D>] {
        &self.coordinates
    }
}

impl ArrowTyped for MultiLineString {
    type ArrowArray = arrow::array::ListArray;
    type ArrowBuilder = arrow::array::ListBuilder<
        arrow::array::ListBuilder<<Coordinate2D as ArrowTyped>::ArrowBuilder>,
    >;

    fn arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::List(
            arrow::datatypes::DataType::List(Coordinate2D::arrow_data_type().into()).into(),
        )
    }

    fn arrow_builder(capacity: usize) -> Self::ArrowBuilder {
        let minimal_number_of_coordinates = 2 * capacity; // at least 2 coordinates per line string
        let coordinate_builder = Coordinate2D::arrow_builder(minimal_number_of_coordinates);
        let line_string_builder = arrow::array::ListBuilder::new(coordinate_builder);
        arrow::array::ListBuilder::new(line_string_builder) // multi line strings = lists of line strings
    }
}

#[derive(Debug, PartialEq)]
pub struct MultiLineStringRef<'g> {
    point_coordinates: Vec<&'g [Coordinate2D]>,
}

impl<'g> MultiLineStringRef<'g> {
    pub fn new(coordinates: Vec<&'g [Coordinate2D]>) -> Result<Self> {
        ensure!(!coordinates.is_empty(), error::UnallowedEmpty);

        Ok(Self::new_unchecked(coordinates))
    }

    pub(crate) fn new_unchecked(coordinates: Vec<&'g [Coordinate2D]>) -> Self {
        Self {
            point_coordinates: coordinates,
        }
    }

    pub fn lines(&self) -> &[&[Coordinate2D]] {
        &self.point_coordinates
    }
}

impl<'g> Into<geojson::Geometry> for MultiLineStringRef<'g> {
    fn into(self) -> geojson::Geometry {
        geojson::Geometry::new(match self.point_coordinates.len() {
            1 => {
                let coordinates = self.point_coordinates[0];
                let positions = coordinates.iter().map(|c| vec![c.x, c.y]).collect();
                geojson::Value::LineString(positions)
            }
            _ => geojson::Value::MultiLineString(
                self.point_coordinates
                    .iter()
                    .map(|&coordinates| coordinates.iter().map(|c| vec![c.x, c.y]).collect())
                    .collect(),
            ),
        })
    }
}
