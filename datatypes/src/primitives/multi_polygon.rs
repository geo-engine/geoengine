use crate::collections::VectorDataType;
use crate::error::Error;
use crate::primitives::{error, BoundingBox2D, GeometryRef, PrimitivesError, TypedGeometry};
use crate::primitives::{Coordinate2D, Geometry};
use crate::util::arrow::{downcast_array, ArrowTyped};
use crate::util::Result;
use arrow::array::{ArrayBuilder, BooleanArray};
use arrow::error::ArrowError;
use geo::intersects::Intersects;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::convert::TryFrom;

/// A trait that allows a common access to polygons of `MultiPolygon`s and its references
pub trait MultiPolygonAccess<R, L>
where
    R: AsRef<[L]>,
    L: AsRef<[Coordinate2D]>,
{
    fn polygons(&self) -> &[R];
}

type Ring = Vec<Coordinate2D>;
type Polygon = Vec<Ring>;

/// A representation of a simple feature multi polygon
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MultiPolygon {
    polygons: Vec<Polygon>,
}

impl MultiPolygon {
    pub fn new(polygons: Vec<Polygon>) -> Result<Self> {
        ensure!(
            !polygons.is_empty() && polygons.iter().all(|polygon| !polygon.is_empty()),
            error::UnallowedEmpty
        );
        ensure!(
            polygons
                .iter()
                .all(|polygon| Self::polygon_is_valid(polygon)),
            error::UnclosedPolygonRing
        );

        Ok(Self::new_unchecked(polygons))
    }

    fn polygon_is_valid(polygon: &[Ring]) -> bool {
        for ring in polygon {
            if !Self::ring_is_valid(ring) {
                return false;
            }
        }
        true
    }

    fn ring_is_valid(ring: &[Coordinate2D]) -> bool {
        if ring.len() < 4 {
            // must have at least four coordinates
            return false;
        }
        if ring.first() != ring.last() {
            // first and last coordinate must match, i.e., it is closed
            return false;
        }

        true
    }

    pub(crate) fn new_unchecked(polygons: Vec<Polygon>) -> Self {
        Self { polygons }
    }
}

impl MultiPolygonAccess<Polygon, Ring> for MultiPolygon {
    fn polygons(&self) -> &[Polygon] {
        &self.polygons
    }
}

impl Geometry for MultiPolygon {
    const DATA_TYPE: VectorDataType = VectorDataType::MultiPolygon;

    fn intersects_bbox(&self, bbox: &BoundingBox2D) -> bool {
        let geo_multi_polygon: geo::MultiPolygon<f64> = self.into();
        let geo_rect: geo::Rect<f64> = bbox.into();

        for polygon in geo_multi_polygon {
            if polygon.intersects(&geo_rect) {
                return true;
            }
        }

        false
    }
}

impl Into<geo::MultiPolygon<f64>> for &MultiPolygon {
    fn into(self) -> geo::MultiPolygon<f64> {
        let polygons: Vec<geo::Polygon<f64>> = self
            .polygons()
            .iter()
            .map(|polygon| {
                let mut line_strings: Vec<geo::LineString<f64>> = polygon
                    .iter()
                    .map(|ring| geo::LineString(ring.iter().map(Into::into).collect()))
                    .collect();

                let exterior = line_strings.remove(0);

                geo::Polygon::new(exterior, line_strings)
            })
            .collect();
        geo::MultiPolygon(polygons)
    }
}

impl TryFrom<TypedGeometry> for MultiPolygon {
    type Error = Error;

    fn try_from(value: TypedGeometry) -> Result<Self, Self::Error> {
        if let TypedGeometry::MultiPolygon(geometry) = value {
            Ok(geometry)
        } else {
            Err(PrimitivesError::InvalidConversion.into())
        }
    }
}

impl AsRef<[Polygon]> for MultiPolygon {
    fn as_ref(&self) -> &[Polygon] {
        &self.polygons
    }
}

impl ArrowTyped for MultiPolygon {
    type ArrowArray = arrow::array::ListArray;
    type ArrowBuilder = arrow::array::ListBuilder<
        arrow::array::ListBuilder<
            arrow::array::ListBuilder<<Coordinate2D as ArrowTyped>::ArrowBuilder>,
        >,
    >;

    fn arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::List(
            arrow::datatypes::DataType::List(
                arrow::datatypes::DataType::List(Coordinate2D::arrow_data_type().into()).into(),
            )
            .into(),
        )
    }

    fn builder_byte_size(builder: &mut Self::ArrowBuilder) -> usize {
        let multi_polygon_indices_size = builder.len() * std::mem::size_of::<i32>();

        let ring_builder = builder.values();
        let ring_indices_size = ring_builder.len() * std::mem::size_of::<i32>();

        let line_builder = ring_builder.values();
        let line_indices_size = line_builder.len() * std::mem::size_of::<i32>();

        let point_builder = line_builder.values();
        let point_indices_size = point_builder.len() * std::mem::size_of::<i32>();

        let coordinates_size = Coordinate2D::builder_byte_size(point_builder);

        multi_polygon_indices_size
            + ring_indices_size
            + line_indices_size
            + point_indices_size
            + coordinates_size
    }

    fn arrow_builder(_capacity: usize) -> Self::ArrowBuilder {
        let coordinate_builder = Coordinate2D::arrow_builder(0);
        let ring_builder = arrow::array::ListBuilder::new(coordinate_builder);
        let polygon_builder = arrow::array::ListBuilder::new(ring_builder);
        arrow::array::ListBuilder::new(polygon_builder)
    }

    fn concat(a: &Self::ArrowArray, b: &Self::ArrowArray) -> Result<Self::ArrowArray, ArrowError> {
        use arrow::array::{Array, FixedSizeListArray, Float64Array, ListArray};

        let mut multi_polygon_builder = Self::arrow_builder(a.len() + b.len());

        for multi_polygons in &[a, b] {
            for multi_polygon_index in 0..multi_polygons.len() {
                let polygon_builder = multi_polygon_builder.values();

                let polygons_ref = multi_polygons.value(multi_polygon_index);
                let polygons = downcast_array::<ListArray>(&polygons_ref);

                for polygon_index in 0..polygons.len() {
                    let ring_builder = polygon_builder.values();

                    let rings_ref = polygons.value(polygon_index);
                    let rings = downcast_array::<ListArray>(&rings_ref);

                    for ring_index in 0..rings.len() {
                        let coordinate_builder = ring_builder.values();

                        let coordinates_ref = rings.value(ring_index);
                        let coordinates = downcast_array::<FixedSizeListArray>(&coordinates_ref);

                        for coordinate_index in 0..(coordinates.len() as usize) {
                            let floats_ref = coordinates.value(coordinate_index);
                            let floats: &Float64Array = downcast_array(&floats_ref);

                            coordinate_builder
                                .values()
                                .append_slice(floats.value_slice(0, 2))?;

                            coordinate_builder.append(true)?;
                        }

                        ring_builder.append(true)?;
                    }

                    polygon_builder.append(true)?;
                }

                multi_polygon_builder.append(true)?;
            }
        }

        Ok(multi_polygon_builder.finish())
    }

    fn filter(
        multi_polygons: &Self::ArrowArray,
        filter_array: &BooleanArray,
    ) -> Result<Self::ArrowArray, ArrowError> {
        use arrow::array::{Array, FixedSizeListArray, Float64Array, ListArray};

        let mut multi_polygon_builder = Self::arrow_builder(0);

        for multi_polygon_index in 0..multi_polygons.len() {
            if !filter_array.value(multi_polygon_index) {
                continue;
            }

            let polygon_builder = multi_polygon_builder.values();

            let polygons_ref = multi_polygons.value(multi_polygon_index);
            let polygons = downcast_array::<ListArray>(&polygons_ref);

            for polygon_index in 0..polygons.len() {
                let ring_builder = polygon_builder.values();

                let rings_ref = polygons.value(polygon_index);
                let rings = downcast_array::<ListArray>(&rings_ref);

                for ring_index in 0..rings.len() {
                    let coordinate_builder = ring_builder.values();

                    let coordinates_ref = rings.value(ring_index);
                    let coordinates = downcast_array::<FixedSizeListArray>(&coordinates_ref);

                    for coordinate_index in 0..(coordinates.len() as usize) {
                        let floats_ref = coordinates.value(coordinate_index);
                        let floats: &Float64Array = downcast_array(&floats_ref);

                        coordinate_builder
                            .values()
                            .append_slice(floats.value_slice(0, 2))?;

                        coordinate_builder.append(true)?;
                    }

                    ring_builder.append(true)?;
                }

                polygon_builder.append(true)?;
            }

            multi_polygon_builder.append(true)?;
        }

        Ok(multi_polygon_builder.finish())
    }

    fn from_vec(multi_polygons: Vec<Self>) -> Result<Self::ArrowArray, ArrowError>
    where
        Self: Sized,
    {
        let mut builder = Self::arrow_builder(multi_polygons.len());
        for multi_polygon in multi_polygons {
            let polygon_builder = builder.values();

            for polygon in multi_polygon.as_ref() {
                let ring_builder = polygon_builder.values();

                for ring in polygon {
                    let coordinate_builder = ring_builder.values();

                    for coordinate in ring {
                        let float_builder = coordinate_builder.values();
                        float_builder.append_value(coordinate.x)?;
                        float_builder.append_value(coordinate.y)?;
                        coordinate_builder.append(true)?;
                    }

                    ring_builder.append(true)?;
                }

                polygon_builder.append(true)?;
            }

            builder.append(true)?;
        }

        Ok(builder.finish())
    }
}

type RingRef<'g> = &'g [Coordinate2D];
type PolygonRef<'g> = Vec<RingRef<'g>>;

#[derive(Debug, PartialEq)]
pub struct MultiPolygonRef<'g> {
    polygons: Vec<PolygonRef<'g>>,
}

impl<'r> GeometryRef for MultiPolygonRef<'r> {}

impl<'g> MultiPolygonRef<'g> {
    pub fn new(polygons: Vec<PolygonRef<'g>>) -> Result<Self> {
        ensure!(!polygons.is_empty(), error::UnallowedEmpty);
        ensure!(
            polygons
                .iter()
                .all(|polygon| Self::polygon_is_valid(polygon)),
            error::UnclosedPolygonRing
        );

        Ok(Self::new_unchecked(polygons))
    }

    fn polygon_is_valid(polygon: &[RingRef<'g>]) -> bool {
        for ring in polygon {
            if !MultiPolygon::ring_is_valid(ring) {
                return false;
            }
        }
        true
    }

    pub(crate) fn new_unchecked(polygons: Vec<PolygonRef<'g>>) -> Self {
        Self { polygons }
    }
}

impl<'g> MultiPolygonAccess<PolygonRef<'g>, RingRef<'g>> for MultiPolygonRef<'g> {
    fn polygons(&self) -> &[PolygonRef<'g>] {
        &self.polygons
    }
}

impl<'g> Into<geojson::Geometry> for MultiPolygonRef<'g> {
    fn into(self) -> geojson::Geometry {
        geojson::Geometry::new(match self.polygons.len() {
            1 => {
                let polygon = &self.polygons[0];
                geojson::Value::Polygon(
                    polygon
                        .iter()
                        .map(|coordinates| coordinates.iter().map(|c| vec![c.x, c.y]).collect())
                        .collect(),
                )
            }
            _ => geojson::Value::MultiPolygon(
                self.polygons
                    .iter()
                    .map(|polygon| {
                        polygon
                            .iter()
                            .map(|coordinates| coordinates.iter().map(|c| vec![c.x, c.y]).collect())
                            .collect()
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
        fn aggregate<T: MultiPolygonAccess<R, L>, R: AsRef<[L]>, L: AsRef<[Coordinate2D]>>(
            multi_line_string: &T,
        ) -> (usize, usize, usize) {
            let number_of_polygons = multi_line_string.polygons().len();
            let number_of_rings = multi_line_string
                .polygons()
                .iter()
                .map(AsRef::as_ref)
                .map(<[L]>::len)
                .sum();
            let number_of_coordinates = multi_line_string
                .polygons()
                .iter()
                .map(AsRef::as_ref)
                .flat_map(<[L]>::iter)
                .map(AsRef::as_ref)
                .map(<[Coordinate2D]>::len)
                .sum();

            (number_of_polygons, number_of_rings, number_of_coordinates)
        }

        let coordinates = vec![vec![
            vec![
                (0.0, 0.1).into(),
                (1.0, 1.1).into(),
                (1.0, 0.1).into(),
                (0.0, 0.1).into(),
            ],
            vec![
                (3.0, 3.1).into(),
                (4.0, 4.1).into(),
                (4.0, 3.1).into(),
                (3.0, 3.1).into(),
            ],
        ]];
        let multi_polygon = MultiPolygon::new(coordinates.clone()).unwrap();
        let multi_polygon_ref = MultiPolygonRef::new(
            coordinates
                .iter()
                .map(|r| r.iter().map(AsRef::as_ref).collect())
                .collect(),
        )
        .unwrap();

        assert_eq!(aggregate(&multi_polygon), (1, 2, 8));
        assert_eq!(aggregate(&multi_polygon), aggregate(&multi_polygon_ref));
    }
}
