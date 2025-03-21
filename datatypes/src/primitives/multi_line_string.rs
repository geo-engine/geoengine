use std::convert::TryFrom;

use arrow::array::BooleanArray;
use arrow::error::ArrowError;
use fallible_iterator::FallibleIterator;
use float_cmp::{ApproxEq, F64Margin};
use geo::algorithm::intersects::Intersects;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use wkt::{ToWkt, Wkt};

use crate::collections::VectorDataType;
use crate::error::Error;
use crate::primitives::{
    BoundingBox2D, GeometryRef, MultiPoint, PrimitivesError, TypedGeometry, error,
};
use crate::primitives::{Coordinate2D, Geometry};
use crate::util::Result;
use crate::util::arrow::{ArrowTyped, downcast_array, padded_buffer_size};

/// A trait that allows a common access to lines of `MultiLineString`s and its references
pub trait MultiLineStringAccess {
    type L: AsRef<[Coordinate2D]>;
    fn lines(&self) -> &[Self::L];
}

/// A representation of a simple feature multi line string
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

impl MultiLineStringAccess for MultiLineString {
    type L = Vec<Coordinate2D>;
    fn lines(&self) -> &[Vec<Coordinate2D>] {
        &self.coordinates
    }
}

impl Geometry for MultiLineString {
    const DATA_TYPE: VectorDataType = VectorDataType::MultiLineString;

    fn intersects_bbox(&self, bbox: &BoundingBox2D) -> bool {
        let geo::MultiLineString::<f64>(geo_line_strings) = self.into();
        let geo_rect: geo::Rect<f64> = bbox.into();

        for line_string in geo_line_strings {
            for line in line_string.lines() {
                if line.intersects(&geo_rect) {
                    return true;
                }
            }
        }

        false
    }
}

impl From<&MultiLineString> for geo::MultiLineString<f64> {
    fn from(geometry: &MultiLineString) -> geo::MultiLineString<f64> {
        let line_strings = geometry
            .coordinates
            .iter()
            .map(|coordinates| {
                let geo_coordinates = coordinates.iter().map(Into::into).collect();
                geo::LineString(geo_coordinates)
            })
            .collect();
        geo::MultiLineString(line_strings)
    }
}

impl From<geo::MultiLineString<f64>> for MultiLineString {
    fn from(geo_geometry: geo::MultiLineString<f64>) -> MultiLineString {
        let coordinates = geo_geometry
            .0
            .into_iter()
            .map(|geo_line_string| {
                geo_line_string
                    .0
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<_>>()
            })
            .collect();
        MultiLineString::new_unchecked(coordinates)
    }
}

impl TryFrom<TypedGeometry> for MultiLineString {
    type Error = Error;

    fn try_from(value: TypedGeometry) -> Result<Self, Self::Error> {
        if let TypedGeometry::MultiLineString(geometry) = value {
            Ok(geometry)
        } else {
            Err(PrimitivesError::InvalidConversion.into())
        }
    }
}

impl AsRef<[Vec<Coordinate2D>]> for MultiLineString {
    fn as_ref(&self) -> &[Vec<Coordinate2D>] {
        &self.coordinates
    }
}

impl ApproxEq for &MultiLineString {
    type Margin = F64Margin;

    fn approx_eq<M: Into<Self::Margin>>(self, other: Self, margin: M) -> bool {
        let m = margin.into();
        self.lines().len() == other.lines().len()
            && self
                .lines()
                .iter()
                .zip(other.lines().iter())
                .all(|(line_a, line_b)| line_a.len() == line_b.len() && line_a.approx_eq(line_b, m))
    }
}

impl ToSql for MultiLineString {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        w: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let postgres_types::Kind::Array(member_type) = ty.kind() else {
            panic!("expected array type");
        };

        let dimension = postgres_protocol::types::ArrayDimension {
            len: self.coordinates.len() as i32,
            lower_bound: 1, // arrays are one-indexed
        };

        postgres_protocol::types::array_to_sql(
            Some(dimension),
            member_type.oid(),
            self.coordinates.iter(),
            |coordinates, w| {
                postgres_protocol::types::path_to_sql(
                    false,
                    coordinates.iter().map(|p| (p.x, p.y)),
                    w,
                )?;

                Ok(postgres_protocol::IsNull::No)
            },
            w,
        )?;

        Ok(postgres_types::IsNull::No)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        let postgres_types::Kind::Array(inner_type) = ty.kind() else {
            return false;
        };

        matches!(inner_type, &postgres_types::Type::PATH)
    }

    postgres_types::to_sql_checked!();
}

impl<'a> FromSql<'a> for MultiLineString {
    fn from_sql(
        _ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let array = postgres_protocol::types::array_from_sql(raw)?;
        if array.dimensions().count()? > 1 {
            return Err("array contains too many dimensions".into());
        }

        let coordinates = array
            .values()
            .map(|raw| {
                let Some(raw) = raw else {
                    return Err("array contains NULL values".into());
                };
                let path = postgres_protocol::types::path_from_sql(raw)?;

                let coordinates = path
                    .points()
                    .map(|point| {
                        Ok(Coordinate2D {
                            x: point.x(),
                            y: point.y(),
                        })
                    })
                    .collect()?;
                Ok(coordinates)
            })
            .collect()?;

        Ok(Self { coordinates })
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        let postgres_types::Kind::Array(inner_type) = ty.kind() else {
            return false;
        };

        matches!(inner_type, &postgres_types::Type::PATH)
    }
}

impl ArrowTyped for MultiLineString {
    type ArrowArray = arrow::array::ListArray;
    type ArrowBuilder = arrow::array::ListBuilder<
        arrow::array::ListBuilder<<Coordinate2D as ArrowTyped>::ArrowBuilder>,
    >;

    fn arrow_data_type() -> arrow::datatypes::DataType {
        MultiPoint::arrow_list_data_type()
    }

    fn estimate_array_memory_size(builder: &mut Self::ArrowBuilder) -> usize {
        let static_size = std::mem::size_of::<Self::ArrowArray>()
            + std::mem::size_of::<<MultiPoint as ArrowTyped>::ArrowArray>();

        let feature_offset_bytes_size = std::mem::size_of_val(builder.offsets_slice());

        let line_builder = builder.values();

        let line_offset_bytes_size = std::mem::size_of_val(line_builder.offsets_slice());

        let coordinates_builder = line_builder.values();

        let coords_size = Coordinate2D::estimate_array_memory_size(coordinates_builder);

        static_size
            + coords_size
            + padded_buffer_size(line_offset_bytes_size, 64)
            + padded_buffer_size(feature_offset_bytes_size, 64)
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

                    for coordinate_index in 0..coordinates.len() {
                        let floats_ref = coordinates.value(coordinate_index);
                        let floats: &Float64Array = downcast_array(&floats_ref);

                        coordinate_builder.values().append_slice(floats.values());

                        coordinate_builder.append(true);
                    }

                    line_builder.append(true);
                }

                multi_line_builder.append(true);
            }
        }

        Ok(multi_line_builder.finish_cloned())
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

                for coordinate_index in 0..coordinates.len() {
                    let floats_ref = coordinates.value(coordinate_index);
                    let floats: &Float64Array = downcast_array(&floats_ref);

                    coordinate_builder.values().append_slice(floats.values());

                    coordinate_builder.append(true);
                }

                line_builder.append(true);
            }

            multi_line_builder.append(true);
        }

        Ok(multi_line_builder.finish_cloned())
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
                    float_builder.append_value(coordinate.x);
                    float_builder.append_value(coordinate.y);
                    coordinate_builder.append(true);
                }

                line_string_builder.append(true);
            }

            builder.append(true);
        }

        Ok(builder.finish_cloned())
    }
}

#[derive(Debug, PartialEq)]
pub struct MultiLineStringRef<'g> {
    point_coordinates: Vec<&'g [Coordinate2D]>,
}

impl GeometryRef for MultiLineStringRef<'_> {
    type GeometryType = MultiLineString;

    fn as_geometry(&self) -> Self::GeometryType {
        self.into()
    }

    fn bbox(&self) -> Option<BoundingBox2D> {
        self.bbox()
    }
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

    pub fn bbox(&self) -> Option<BoundingBox2D> {
        self.lines().iter().fold(None, |bbox, line| {
            let lbox = BoundingBox2D::from_coord_ref_iter(line.iter());
            match (bbox, lbox) {
                (None, Some(lbox)) => Some(lbox),
                (Some(bbox), Some(lbox)) => Some(bbox.union(&lbox)),
                (bbox, None) => bbox,
            }
        })
    }
}

impl<'g> MultiLineStringAccess for MultiLineStringRef<'g> {
    type L = &'g [Coordinate2D];
    fn lines(&self) -> &[&'g [Coordinate2D]] {
        &self.point_coordinates
    }
}

impl ToWkt<f64> for MultiLineStringRef<'_> {
    fn to_wkt(&self) -> Wkt<f64> {
        let line_strings = self.lines();
        let mut multi_line_string =
            wkt::types::MultiLineString(Vec::with_capacity(line_strings.len()));

        for line_string in line_strings {
            let mut line_strings = wkt::types::LineString(Vec::with_capacity(line_string.len()));

            for coord in *line_string {
                line_strings.0.push(coord.into());
            }

            multi_line_string.0.push(line_strings);
        }

        Wkt::MultiLineString(multi_line_string)
    }
}

impl<'g> From<MultiLineStringRef<'g>> for geojson::Geometry {
    fn from(geometry: MultiLineStringRef<'g>) -> geojson::Geometry {
        geojson::Geometry::new(match geometry.point_coordinates.len() {
            1 => {
                let coordinates = geometry.point_coordinates[0];
                let positions = coordinates.iter().map(|c| vec![c.x, c.y]).collect();
                geojson::Value::LineString(positions)
            }
            _ => geojson::Value::MultiLineString(
                geometry
                    .point_coordinates
                    .iter()
                    .map(|&coordinates| coordinates.iter().map(|c| vec![c.x, c.y]).collect())
                    .collect(),
            ),
        })
    }
}

impl<'g> From<MultiLineStringRef<'g>> for MultiLineString {
    fn from(multi_line_string_ref: MultiLineStringRef<'g>) -> Self {
        MultiLineString::from(&multi_line_string_ref)
    }
}

impl<'g> From<&MultiLineStringRef<'g>> for MultiLineString {
    fn from(multi_line_string_ref: &MultiLineStringRef<'g>) -> Self {
        MultiLineString::new_unchecked(
            multi_line_string_ref
                .point_coordinates
                .iter()
                .copied()
                .map(ToOwned::to_owned)
                .collect(),
        )
    }
}

impl<'g> From<&'g MultiLineString> for MultiLineStringRef<'g> {
    fn from(multi_line_string: &'g MultiLineString) -> Self {
        MultiLineStringRef::new_unchecked(
            multi_line_string
                .lines()
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>(),
        )
    }
}

impl<'g> From<&MultiLineStringRef<'g>> for geo::MultiLineString<f64> {
    fn from(geometry: &MultiLineStringRef<'g>) -> Self {
        let line_strings = geometry
            .point_coordinates
            .iter()
            .map(|coordinates| {
                let geo_coordinates = coordinates.iter().map(Into::into).collect();
                geo::LineString(geo_coordinates)
            })
            .collect();
        geo::MultiLineString(line_strings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayBuilder};
    use float_cmp::approx_eq;

    #[test]
    fn access() {
        fn aggregate<T: MultiLineStringAccess>(multi_line_string: &T) -> (usize, usize) {
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

    #[test]
    fn approx_equal() {
        let a = MultiLineString::new(vec![
            vec![(0.1, 0.1).into(), (0.5, 0.5).into()],
            vec![(0.5, 0.5).into(), (0.6, 0.6).into()],
            vec![(0.6, 0.6).into(), (0.9, 0.9).into()],
        ])
        .unwrap();

        let b = MultiLineString::new(vec![
            vec![(0.099_999_999, 0.1).into(), (0.5, 0.5).into()],
            vec![(0.5, 0.5).into(), (0.6, 0.6).into()],
            vec![(0.6, 0.6).into(), (0.9, 0.9).into()],
        ])
        .unwrap();

        assert!(approx_eq!(&MultiLineString, &a, &b, epsilon = 0.000_001));
    }

    #[test]
    fn not_approx_equal_outer_len() {
        let a = MultiLineString::new(vec![
            vec![(0.1, 0.1).into(), (0.5, 0.5).into()],
            vec![(0.5, 0.5).into(), (0.6, 0.6).into()],
            vec![(0.6, 0.6).into(), (0.9, 0.9).into()],
        ])
        .unwrap();

        let b = MultiLineString::new(vec![
            vec![(0.1, 0.1).into(), (0.5, 0.5).into()],
            vec![(0.5, 0.5).into(), (0.6, 0.6).into()],
            vec![(0.6, 0.6).into(), (0.9, 0.9).into()],
            vec![(0.9, 0.9).into(), (123_456_789.9, 123_456_789.9).into()],
        ])
        .unwrap();

        assert!(!approx_eq!(&MultiLineString, &a, &b, F64Margin::default()));
    }

    #[test]
    fn not_approx_equal_inner_len() {
        let a = MultiLineString::new(vec![
            vec![(0.1, 0.1).into(), (0.5, 0.5).into()],
            vec![(0.5, 0.5).into(), (0.6, 0.6).into(), (0.7, 0.7).into()],
            vec![(0.7, 0.7).into(), (0.9, 0.9).into()],
        ])
        .unwrap();

        let b = MultiLineString::new(vec![
            vec![(0.1, 0.1).into(), (0.5, 0.5).into()],
            vec![(0.5, 0.5).into(), (0.6, 0.6).into()],
            vec![(0.6, 0.6).into(), (0.7, 0.7).into(), (0.9, 0.9).into()],
        ])
        .unwrap();

        assert!(!approx_eq!(&MultiLineString, &a, &b, F64Margin::default()));
    }

    #[test]
    fn test_to_wkt() {
        let a = MultiLineString::new(vec![
            vec![(0.1, 0.1).into(), (0.5, 0.5).into()],
            vec![(0.5, 0.5).into(), (0.6, 0.6).into(), (0.7, 0.7).into()],
            vec![(0.7, 0.7).into(), (0.9, 0.9).into()],
        ])
        .unwrap();

        let a_ref = MultiLineStringRef::from(&a);

        assert_eq!(
            a_ref.wkt_string(),
            "MULTILINESTRING((0.1 0.1,0.5 0.5),(0.5 0.5,0.6 0.6,0.7 0.7),(0.7 0.7,0.9 0.9))"
        );
    }

    #[test]
    fn test_to_geo_and_back() {
        let line_string = MultiLineString::new(vec![
            vec![(0.1, 0.1).into(), (0.5, 0.5).into()],
            vec![(0.5, 0.5).into(), (0.6, 0.6).into(), (0.7, 0.7).into()],
            vec![(0.7, 0.7).into(), (0.9, 0.9).into()],
        ])
        .unwrap();

        let geo_line_string = geo::MultiLineString::<f64>::from(&line_string);

        let line_string_back = MultiLineString::from(geo_line_string);

        assert_eq!(line_string, line_string_back);
    }

    #[test]
    fn arrow_builder_size() {
        fn push_geometry(
            geometries_builder: &mut <MultiLineString as ArrowTyped>::ArrowBuilder,
            geometry: &MultiLineString,
        ) {
            let line_builder = geometries_builder.values();

            for line in geometry.lines() {
                let coordinate_builder = line_builder.values();

                for coordinate in line {
                    coordinate_builder
                        .values()
                        .append_slice(coordinate.as_ref());

                    coordinate_builder.append(true);
                }

                line_builder.append(true);
            }

            geometries_builder.append(true);
        }

        for num_multi_lines in 0..64 {
            for capacity in [0, num_multi_lines] {
                let mut builder = MultiLineString::arrow_builder(capacity);

                for i in 0..num_multi_lines {
                    match i % 3 {
                        0 => {
                            push_geometry(
                                &mut builder,
                                &MultiLineString::new(vec![
                                    vec![(0.1, 0.1).into(), (0.5, 0.5).into()],
                                    vec![(0.5, 0.5).into(), (0.6, 0.6).into()],
                                    vec![(0.6, 0.6).into(), (0.9, 0.9).into()],
                                ])
                                .unwrap(),
                            );
                        }
                        1 => {
                            push_geometry(
                                &mut builder,
                                &MultiLineString::new(vec![
                                    vec![(0.0, 0.1).into(), (1.0, 1.1).into()],
                                    vec![(3.0, 3.1).into(), (4.0, 4.1).into()],
                                ])
                                .unwrap(),
                            );
                        }
                        2 => {
                            push_geometry(
                                &mut builder,
                                &MultiLineString::new(vec![
                                    vec![(0.1, 0.1).into(), (0.5, 0.5).into()],
                                    vec![(0.5, 0.5).into(), (0.6, 0.6).into(), (0.7, 0.7).into()],
                                    vec![(0.7, 0.7).into(), (0.9, 0.9).into()],
                                ])
                                .unwrap(),
                            );
                        }
                        _ => unreachable!(),
                    }
                }

                assert_eq!(builder.len(), num_multi_lines);

                let builder_byte_size = MultiLineString::estimate_array_memory_size(&mut builder);

                let array = builder.finish_cloned();

                assert_eq!(
                    builder_byte_size,
                    array.get_array_memory_size(),
                    "{num_multi_lines}"
                );
            }
        }
    }
}
