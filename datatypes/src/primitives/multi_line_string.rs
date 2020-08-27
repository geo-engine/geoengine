use crate::primitives::{error, GeometryRef};
use crate::primitives::{Coordinate2D, Geometry};
use crate::util::arrow::{downcast_array, ArrowTyped};
use crate::util::Result;
use arrow::array::BooleanArray;
use arrow::error::ArrowError;
use snafu::ensure;

/// A trait that allows a common access to lines of `MultiLineString`s and its references
pub trait MultiLineStringAccess<L>
where
    L: AsRef<[Coordinate2D]>,
{
    fn lines(&self) -> &[L];
}

/// A representation of a simple feature multi line string
#[derive(Clone, Debug, PartialEq)]
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
}

impl MultiLineStringAccess<Vec<Coordinate2D>> for MultiLineString {
    fn lines(&self) -> &[Vec<Coordinate2D>] {
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

    fn concat(a: &Self::ArrowArray, b: &Self::ArrowArray) -> Result<Self::ArrowArray, ArrowError> {
        use arrow::array::{Array, FixedSizeListArray, Float64Array, ListArray};

        let mut multi_line_builder = Self::arrow_builder(a.len() + b.len());

        for multi_lines in &[a, b] {
            for multi_line_index in 0..multi_lines.len() {
                let line_builder = multi_line_builder.values();

                let lines_ref = multi_lines.value(multi_line_index);
                let lines = downcast_array::<ListArray>(&lines_ref);

                for line_index in 0..lines.len() {
                    let coordinate_builder = line_builder.values();

                    let coordinates_ref = lines.value(line_index);
                    let coordinates = downcast_array::<FixedSizeListArray>(&coordinates_ref);

                    for coordinate_index in 0..(coordinates.len() as usize) {
                        let floats_ref = coordinates.value(coordinate_index);
                        let floats: &Float64Array = downcast_array(&floats_ref);

                        coordinate_builder
                            .values()
                            .append_slice(floats.value_slice(0, 2))?;

                        coordinate_builder.append(true)?;
                    }

                    line_builder.append(true)?;
                }

                multi_line_builder.append(true)?;
            }
        }

        Ok(multi_line_builder.finish())
    }

    fn filter(
        multi_lines: &Self::ArrowArray,
        filter_array: &BooleanArray,
    ) -> Result<Self::ArrowArray, ArrowError> {
        use arrow::array::{Array, FixedSizeListArray, Float64Array, ListArray};

        let mut multi_line_builder = Self::arrow_builder(0);

        for multi_line_index in 0..multi_lines.len() {
            if !filter_array.value(multi_line_index) {
                continue;
            }

            let line_builder = multi_line_builder.values();

            let lines_ref = multi_lines.value(multi_line_index);
            let lines = downcast_array::<ListArray>(&lines_ref);

            for line_index in 0..lines.len() {
                let coordinate_builder = line_builder.values();

                let coordinates_ref = lines.value(line_index);
                let coordinates = downcast_array::<FixedSizeListArray>(&coordinates_ref);

                for coordinate_index in 0..(coordinates.len() as usize) {
                    let floats_ref = coordinates.value(coordinate_index);
                    let floats: &Float64Array = downcast_array(&floats_ref);

                    coordinate_builder
                        .values()
                        .append_slice(floats.value_slice(0, 2))?;

                    coordinate_builder.append(true)?;
                }

                line_builder.append(true)?;
            }

            multi_line_builder.append(true)?;
        }

        Ok(multi_line_builder.finish())
    }

    fn from_vec(multi_line_strings: Vec<Self>) -> Result<Self::ArrowArray, ArrowError>
    where
        Self: Sized,
    {
        let mut builder = Self::arrow_builder(multi_line_strings.len());
        for multi_line_string in multi_line_strings {
            let line_string_builder = builder.values();

            for line_string in multi_line_string.as_ref() {
                let coordinate_builder = line_string_builder.values();

                for coordinate in line_string {
                    let float_builder = coordinate_builder.values();
                    float_builder.append_value(coordinate.x)?;
                    float_builder.append_value(coordinate.y)?;
                    coordinate_builder.append(true)?;
                }

                line_string_builder.append(true)?;
            }

            builder.append(true)?;
        }

        Ok(builder.finish())
    }
}

#[derive(Debug, PartialEq)]
pub struct MultiLineStringRef<'g> {
    point_coordinates: Vec<&'g [Coordinate2D]>,
}

impl<'r> GeometryRef for MultiLineStringRef<'r> {}

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
}

impl<'g> MultiLineStringAccess<&'g [Coordinate2D]> for MultiLineStringRef<'g> {
    fn lines(&self) -> &[&'g [Coordinate2D]] {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access() {
        fn aggregate<T: MultiLineStringAccess<L>, L: AsRef<[Coordinate2D]>>(
            multi_line_string: &T,
        ) -> (usize, usize) {
            let number_of_lines = multi_line_string.lines().len();
            let number_of_coordinates = multi_line_string
                .lines()
                .iter()
                .map(AsRef::as_ref)
                .map(<[Coordinate2D]>::len)
                .sum();

            (number_of_lines, number_of_coordinates)
        }

        let coordinates = vec![
            vec![(0.0, 0.1).into(), (1.0, 1.1).into()],
            vec![(3.0, 3.1).into(), (4.0, 4.1).into()],
        ];
        let multi_line_string = MultiLineString::new(coordinates.clone()).unwrap();
        let multi_line_string_ref =
            MultiLineStringRef::new(coordinates.iter().map(AsRef::as_ref).collect()).unwrap();

        assert_eq!(aggregate(&multi_line_string), (2, 4));
        assert_eq!(
            aggregate(&multi_line_string),
            aggregate(&multi_line_string_ref)
        );
    }
}
