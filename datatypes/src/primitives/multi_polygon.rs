use std::convert::TryFrom;

use arrow::array::{ArrayBuilder, BooleanArray};
use arrow::error::ArrowError;
use float_cmp::{ApproxEq, F64Margin};
use geo::intersects::Intersects;
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::collections::VectorDataType;
use crate::error::Error;
use crate::primitives::{
    error, BoundingBox2D, GeometryRef, MultiLineString, PrimitivesError, SpatialBounded,
    TypedGeometry,
};
use crate::primitives::{Coordinate2D, Geometry};
use crate::util::arrow::{downcast_array, ArrowTyped};
use crate::util::Result;
use arrow::datatypes::DataType;

/// A trait that allows a common access to polygons of `MultiPolygon`s and its references
pub trait MultiPolygonAccess {
    type L: AsRef<[Coordinate2D]>;
    type R: AsRef<[Self::L]>;
    fn polygons(&self) -> &[Self::R];
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

impl MultiPolygonAccess for MultiPolygon {
    type R = Polygon;
    type L = Ring;
    fn polygons(&self) -> &[Self::R] {
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

impl From<&MultiPolygon> for geo::MultiPolygon<f64> {
    fn from(geometry: &MultiPolygon) -> geo::MultiPolygon<f64> {
        let polygons: Vec<geo::Polygon<f64>> = geometry
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

    fn arrow_data_type() -> DataType {
        MultiLineString::arrow_list_data_type()
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

                            coordinate_builder.values().append_slice(floats.values())?;

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

                        coordinate_builder.values().append_slice(floats.values())?;

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

impl<'g> MultiPolygonAccess for MultiPolygonRef<'g> {
    type R = PolygonRef<'g>;
    type L = RingRef<'g>;

    fn polygons(&self) -> &[Self::R] {
        &self.polygons
    }
}

impl<'g> SpatialBounded for MultiPolygonRef<'g> {
    fn spatial_bounds(&self) -> BoundingBox2D {
        let outer_ring_coords = self
            .polygons
            .iter()
            // Use exterior ring (first ring of a polygon)
            .filter_map(|p| p.iter().next())
            .flat_map(|&exterior| exterior.iter());
        BoundingBox2D::from_coord_ref_iter(outer_ring_coords)
            .expect("there must be at least one coordinate in a multipolygon")
    }
}

impl<'g> Intersects<BoundingBox2D> for MultiPolygonRef<'g> {
    fn intersects(&self, rhs: &BoundingBox2D) -> bool {
        self.spatial_bounds().intersects_bbox(rhs)
    }
}

impl<'g> From<MultiPolygonRef<'g>> for geojson::Geometry {
    fn from(geometry: MultiPolygonRef<'g>) -> geojson::Geometry {
        geojson::Geometry::new(match geometry.polygons.len() {
            1 => {
                let polygon = &geometry.polygons[0];
                geojson::Value::Polygon(
                    polygon
                        .iter()
                        .map(|coordinates| coordinates.iter().map(|c| vec![c.x, c.y]).collect())
                        .collect(),
                )
            }
            _ => geojson::Value::MultiPolygon(
                geometry
                    .polygons
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

impl<'g> From<MultiPolygonRef<'g>> for MultiPolygon {
    fn from(multi_point_ref: MultiPolygonRef<'g>) -> Self {
        MultiPolygon::from(&multi_point_ref)
    }
}

impl<'g> From<&MultiPolygonRef<'g>> for MultiPolygon {
    fn from(multi_point_ref: &MultiPolygonRef<'g>) -> Self {
        MultiPolygon::new_unchecked(
            multi_point_ref
                .polygons
                .iter()
                .map(|polygon| polygon.iter().copied().map(ToOwned::to_owned).collect())
                .collect(),
        )
    }
}

impl ApproxEq for &MultiPolygon {
    type Margin = F64Margin;

    fn approx_eq<M: Into<Self::Margin>>(self, other: Self, margin: M) -> bool {
        let m = margin.into();
        self.polygons().len() == other.polygons().len()
            && self
                .polygons()
                .iter()
                .zip(other.polygons())
                .all(|(polygon_a, polygon_b)| {
                    polygon_a.len() == polygon_b.len()
                        && polygon_a.iter().zip(polygon_b).all(|(ring_a, ring_b)| {
                            ring_a.len() == ring_b.len()
                                && ring_a.iter().zip(ring_b).all(
                                    |(&coordinate_a, &coordinate_b)| {
                                        coordinate_a.approx_eq(coordinate_b, m)
                                    },
                                )
                        })
                })
    }
}

#[cfg(test)]
mod tests {
    use float_cmp::approx_eq;

    use super::*;

    #[test]
    fn access() {
        fn aggregate<T: MultiPolygonAccess>(multi_line_string: &T) -> (usize, usize, usize) {
            let number_of_polygons = multi_line_string.polygons().len();
            let number_of_rings = multi_line_string
                .polygons()
                .iter()
                .map(AsRef::as_ref)
                .map(<[_]>::len)
                .sum();
            let number_of_coordinates = multi_line_string
                .polygons()
                .iter()
                .map(AsRef::as_ref)
                .flat_map(<[_]>::iter)
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

    #[test]
    fn test_ref_intersects() {
        let coordinates = vec![vec![
            vec![
                (0.0, 0.0).into(),
                (10.0, 0.0).into(),
                (10.0, 10.0).into(),
                (0.0, 10.0).into(),
                (0.0, 0.0).into(),
            ],
            vec![
                (4.0, 4.0).into(),
                (6.0, 4.0).into(),
                (6.0, 6.0).into(),
                (4.0, 6.0).into(),
                (4.0, 4.0).into(),
            ],
        ]];
        let multi_polygon_ref = MultiPolygonRef::new(
            coordinates
                .iter()
                .map(|r| r.iter().map(AsRef::as_ref).collect())
                .collect(),
        )
        .unwrap();

        assert!(multi_polygon_ref.intersects(&BoundingBox2D::new_unchecked(
            (-1., -1.,).into(),
            (11., 11.).into()
        )));

        assert!(multi_polygon_ref.intersects(&BoundingBox2D::new_unchecked(
            (4.5, 4.5,).into(),
            (5.5, 5.5).into()
        )));

        assert!(!multi_polygon_ref.intersects(&BoundingBox2D::new_unchecked(
            (-11., -1.,).into(),
            (-1., 11.).into()
        )));
    }

    #[test]
    fn approx_equal() {
        let a = MultiPolygon::new(vec![
            vec![
                vec![
                    (0.1, 0.1).into(),
                    (0.8, 0.1).into(),
                    (0.8, 0.8).into(),
                    (0.1, 0.1).into(),
                ],
                vec![
                    (0.2, 0.2).into(),
                    (0.9, 0.2).into(),
                    (0.9, 0.9).into(),
                    (0.2, 0.2).into(),
                ],
            ],
            vec![
                vec![
                    (1.1, 1.1).into(),
                    (1.8, 1.1).into(),
                    (1.8, 1.8).into(),
                    (1.1, 1.1).into(),
                ],
                vec![
                    (1.2, 1.2).into(),
                    (1.9, 1.2).into(),
                    (1.9, 1.9).into(),
                    (1.2, 1.2).into(),
                ],
            ],
        ])
        .unwrap();

        let b = MultiPolygon::new(vec![
            vec![
                vec![
                    (0.1, 0.099_999_999).into(),
                    (0.8, 0.1).into(),
                    (0.8, 0.8).into(),
                    (0.1, 0.099_999_999).into(),
                ],
                vec![
                    (0.2, 0.2).into(),
                    (0.9, 0.2).into(),
                    (0.9, 0.9).into(),
                    (0.2, 0.2).into(),
                ],
            ],
            vec![
                vec![
                    (1.1, 1.1).into(),
                    (1.8, 1.1).into(),
                    (1.8, 1.8).into(),
                    (1.1, 1.1).into(),
                ],
                vec![
                    (1.2, 1.2).into(),
                    (1.9, 1.2).into(),
                    (1.9, 1.9).into(),
                    (1.2, 1.2).into(),
                ],
            ],
        ])
        .unwrap();

        assert!(approx_eq!(&MultiPolygon, &a, &b, epsilon = 0.000_001));
    }

    #[test]
    fn not_approx_equal_ring_len() {
        let a = MultiPolygon::new(vec![
            vec![
                vec![
                    (0.1, 0.1).into(),
                    (0.8, 0.1).into(),
                    (0.8, 0.8).into(),
                    (0.1, 0.1).into(),
                ],
                vec![
                    (0.2, 0.2).into(),
                    (0.9, 0.2).into(),
                    (0.9, 0.9).into(),
                    (0.2, 0.2).into(),
                ],
            ],
            vec![vec![
                (1.1, 1.1).into(),
                (1.8, 1.1).into(),
                (1.8, 1.8).into(),
                (1.1, 1.1).into(),
            ]],
        ])
        .unwrap();

        let b = MultiPolygon::new(vec![
            vec![
                vec![
                    (0.1, 0.1).into(),
                    (0.8, 0.1).into(),
                    (0.8, 0.8).into(),
                    (0.1, 0.1).into(),
                ],
                vec![
                    (0.2, 0.2).into(),
                    (0.9, 0.2).into(),
                    (0.9, 0.9).into(),
                    (0.2, 0.2).into(),
                ],
            ],
            vec![
                vec![
                    (1.1, 1.1).into(),
                    (1.8, 1.1).into(),
                    (1.8, 1.8).into(),
                    (1.1, 1.1).into(),
                ],
                vec![
                    (1.2, 1.2).into(),
                    (1.9, 1.2).into(),
                    (1.9, 1.9).into(),
                    (1.2, 1.2).into(),
                ],
            ],
        ])
        .unwrap();

        assert!(!approx_eq!(&MultiPolygon, &a, &b, F64Margin::default()));
    }

    #[test]
    fn not_approx_equal_inner_len() {
        let a = MultiPolygon::new(vec![
            vec![
                vec![
                    (0.1, 0.1).into(),
                    (0.8, 0.1).into(),
                    (0.8, 0.8).into(),
                    (0.1, 0.1).into(),
                ],
                vec![
                    (0.2, 0.2).into(),
                    (0.9, 0.2).into(),
                    (0.9, 0.9).into(),
                    (0.2, 0.2).into(),
                ],
            ],
            vec![
                vec![
                    (1.1, 1.1).into(),
                    (1.8, 1.1).into(),
                    (1.8, 1.8).into(),
                    (1.1, 1.1).into(),
                ],
                vec![
                    (1.2, 1.2).into(),
                    (1.9, 1.2).into(),
                    (1.9, 1.9).into(),
                    (1.2, 1.2).into(),
                ],
            ],
        ])
        .unwrap();

        let b = MultiPolygon::new(vec![
            vec![
                vec![
                    (0.1, 0.1).into(),
                    (0.8, 0.1).into(),
                    (0.8, 0.8).into(),
                    (0.1, 0.1).into(),
                ],
                vec![
                    (0.2, 0.2).into(),
                    (0.9, 0.2).into(),
                    (0.9, 0.9).into(),
                    (0.2, 0.2).into(),
                ],
            ],
            vec![
                vec![
                    (1.1, 1.1).into(),
                    (1.8, 1.1).into(),
                    (1.8, 1.8).into(),
                    (1.1, 1.1).into(),
                ],
                vec![
                    (1.2, 1.2).into(),
                    (1.7, 1.2).into(),
                    (1.9, 1.2).into(),
                    (1.9, 1.9).into(),
                    (1.2, 1.2).into(),
                ],
            ],
        ])
        .unwrap();

        assert!(!approx_eq!(&MultiPolygon, &a, &b, F64Margin::default()));
    }
}
