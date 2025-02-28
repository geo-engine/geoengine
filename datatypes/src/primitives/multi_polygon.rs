use arrow::array::BooleanArray;
use arrow::error::ArrowError;
use fallible_iterator::FallibleIterator;
use float_cmp::{ApproxEq, F64Margin};
use geo::intersects::Intersects;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::convert::TryFrom;
use wkt::{ToWkt, Wkt};

use super::MultiPoint;
use crate::collections::VectorDataType;
use crate::error::Error;
use crate::primitives::{
    BoundingBox2D, GeometryRef, MultiLineString, PrimitivesError, TypedGeometry, error,
};
use crate::primitives::{Coordinate2D, Geometry};
use crate::util::Result;
use crate::util::arrow::{ArrowTyped, downcast_array, padded_buffer_size};
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

impl From<geo::MultiPolygon<f64>> for MultiPolygon {
    fn from(geometry: geo::MultiPolygon<f64>) -> MultiPolygon {
        let geo::MultiPolygon(geo_polygons) = geometry;

        let mut polygons = Vec::with_capacity(geo_polygons.len());

        for geo_polygon in geo_polygons {
            let (exterior, interiors) = geo_polygon.into_inner();

            let mut rings = Vec::with_capacity(1 + interiors.len());

            let geo::LineString(exterior_coords) = exterior;
            let ring: Ring = exterior_coords.into_iter().map(Into::into).collect();
            rings.push(ring);

            for geo::LineString(interior_coords) in interiors {
                let ring: Ring = interior_coords.into_iter().map(Into::into).collect();
                rings.push(ring);
            }

            polygons.push(rings);
        }

        MultiPolygon::new_unchecked(polygons)
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

mod db_types {
    use super::*;

    #[derive(Debug)]
    pub struct PolygonRef<'p> {
        pub rings: &'p [Vec<Coordinate2D>],
    }

    #[derive(Debug)]
    pub struct PolygonOwned {
        pub rings: Vec<Vec<Coordinate2D>>,
    }

    impl ToSql for PolygonRef<'_> {
        fn to_sql(
            &self,
            ty: &postgres_types::Type,
            w: &mut bytes::BytesMut,
        ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
            let postgres_types::Kind::Domain(domain_type) = ty.kind() else {
                panic!("expected domain type");
            };

            let postgres_types::Kind::Array(member_type) = domain_type.kind() else {
                panic!("expected array type");
            };

            let dimension = postgres_protocol::types::ArrayDimension {
                len: self.rings.len() as i32,
                lower_bound: 1, // arrays are one-indexed
            };

            postgres_protocol::types::array_to_sql(
                Some(dimension),
                member_type.oid(),
                self.rings.iter(),
                |coordinates, w| {
                    postgres_protocol::types::path_to_sql(
                        true,
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
            if ty.name() != "Polygon" {
                return false;
            }

            let postgres_types::Kind::Domain(inner_type) = ty.kind() else {
                return false;
            };

            let postgres_types::Kind::Array(inner_type) = inner_type.kind() else {
                return false;
            };

            matches!(inner_type, &postgres_types::Type::PATH)
        }

        postgres_types::to_sql_checked!();
    }

    impl<'a> FromSql<'a> for PolygonOwned {
        fn from_sql(
            _ty: &postgres_types::Type,
            raw: &'a [u8],
        ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
            let array = postgres_protocol::types::array_from_sql(raw)?;
            if array.dimensions().count()? > 1 {
                return Err("array contains too many dimensions".into());
            }

            let rings = array
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

            Ok(Self { rings })
        }

        fn accepts(ty: &postgres_types::Type) -> bool {
            if ty.name() != "Polygon" {
                return false;
            }

            let postgres_types::Kind::Domain(inner_type) = ty.kind() else {
                return false;
            };

            let postgres_types::Kind::Array(inner_type) = inner_type.kind() else {
                return false;
            };

            matches!(inner_type, &postgres_types::Type::PATH)
        }
    }

    impl ToSql for MultiPolygon {
        fn to_sql(
            &self,
            ty: &postgres_types::Type,
            w: &mut bytes::BytesMut,
        ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
            let postgres_types::Kind::Array(member_type) = ty.kind() else {
                panic!("expected array type");
            };

            let dimension = postgres_protocol::types::ArrayDimension {
                len: self.polygons.len() as i32,
                lower_bound: 1, // arrays are one-indexed
            };

            postgres_protocol::types::array_to_sql(
                Some(dimension),
                member_type.oid(),
                self.polygons.iter(),
                |rings, w| match <PolygonRef as ToSql>::to_sql(
                    &PolygonRef { rings },
                    member_type,
                    w,
                )? {
                    postgres_types::IsNull::No => Ok(postgres_protocol::IsNull::No),
                    postgres_types::IsNull::Yes => Ok(postgres_protocol::IsNull::Yes),
                },
                w,
            )?;

            Ok(postgres_types::IsNull::No)
        }

        fn accepts(ty: &postgres_types::Type) -> bool {
            let postgres_types::Kind::Array(inner_type) = ty.kind() else {
                return false;
            };

            <PolygonRef as ToSql>::accepts(inner_type)
        }

        postgres_types::to_sql_checked!();
    }

    impl<'a> FromSql<'a> for MultiPolygon {
        fn from_sql(
            ty: &postgres_types::Type,
            raw: &'a [u8],
        ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
            let postgres_types::Kind::Array(inner_type) = ty.kind() else {
                return Err("inner type is not of type array".into());
            };

            let array = postgres_protocol::types::array_from_sql(raw)?;
            if array.dimensions().count()? > 1 {
                return Err("array contains too many dimensions".into());
            }

            let polygons = array
                .values()
                .map(|raw| {
                    let Some(raw) = raw else {
                        return Err("array contains NULL values".into());
                    };
                    let polygon = <PolygonOwned as FromSql>::from_sql(inner_type, raw)?;
                    Ok(polygon.rings)
                })
                .collect()?;

            Ok(Self { polygons })
        }

        fn accepts(ty: &postgres_types::Type) -> bool {
            let postgres_types::Kind::Array(inner_type) = ty.kind() else {
                return false;
            };

            inner_type.name() == "Polygon"
        }
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

    fn estimate_array_memory_size(builder: &mut Self::ArrowBuilder) -> usize {
        let static_size = std::mem::size_of::<Self::ArrowArray>()
            + std::mem::size_of::<<MultiLineString as ArrowTyped>::ArrowArray>()
            + std::mem::size_of::<<MultiPoint as ArrowTyped>::ArrowArray>();

        let feature_offset_bytes_size = std::mem::size_of_val(builder.offsets_slice());

        let polygon_builder = builder.values();

        let polygon_offset_bytes_size = std::mem::size_of_val(polygon_builder.offsets_slice());

        let ring_builder = polygon_builder.values();

        let ring_offset_bytes_size = std::mem::size_of_val(ring_builder.offsets_slice());

        let coordinates_builder = ring_builder.values();

        let coords_size = Coordinate2D::estimate_array_memory_size(coordinates_builder);

        static_size
            + coords_size
            + padded_buffer_size(ring_offset_bytes_size, 64)
            + padded_buffer_size(polygon_offset_bytes_size, 64)
            + padded_buffer_size(feature_offset_bytes_size, 64)
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

                        for coordinate_index in 0..coordinates.len() {
                            let floats_ref = coordinates.value(coordinate_index);
                            let floats: &Float64Array = downcast_array(&floats_ref);

                            coordinate_builder.values().append_slice(floats.values());

                            coordinate_builder.append(true);
                        }

                        ring_builder.append(true);
                    }

                    polygon_builder.append(true);
                }

                multi_polygon_builder.append(true);
            }
        }

        Ok(multi_polygon_builder.finish_cloned())
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

                    for coordinate_index in 0..coordinates.len() {
                        let floats_ref = coordinates.value(coordinate_index);
                        let floats: &Float64Array = downcast_array(&floats_ref);

                        coordinate_builder.values().append_slice(floats.values());

                        coordinate_builder.append(true);
                    }

                    ring_builder.append(true);
                }

                polygon_builder.append(true);
            }

            multi_polygon_builder.append(true);
        }

        Ok(multi_polygon_builder.finish_cloned())
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
                        float_builder.append_value(coordinate.x);
                        float_builder.append_value(coordinate.y);
                        coordinate_builder.append(true);
                    }

                    ring_builder.append(true);
                }

                polygon_builder.append(true);
            }

            builder.append(true);
        }

        Ok(builder.finish_cloned())
    }
}

type RingRef<'g> = &'g [Coordinate2D];
type PolygonRef<'g> = Vec<RingRef<'g>>;

#[derive(Debug, PartialEq)]
pub struct MultiPolygonRef<'g> {
    polygons: Vec<PolygonRef<'g>>,
}

impl GeometryRef for MultiPolygonRef<'_> {
    type GeometryType = MultiPolygon;

    fn as_geometry(&self) -> Self::GeometryType {
        MultiPolygon::from(self)
    }

    fn bbox(&self) -> Option<BoundingBox2D> {
        self.bbox()
    }
}

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

    pub fn bbox(&self) -> Option<BoundingBox2D> {
        self.polygons().iter().fold(None, |bbox, rings| {
            let lbox = BoundingBox2D::from_coord_ref_iter(rings[0].iter()); // we only need to look at the outer ring coords
            match (bbox, lbox) {
                (None, Some(lbox)) => Some(lbox),
                (Some(bbox), Some(lbox)) => Some(bbox.union(&lbox)),
                (bbox, None) => bbox,
            }
        })
    }
}

impl<'g> MultiPolygonAccess for MultiPolygonRef<'g> {
    type R = PolygonRef<'g>;
    type L = RingRef<'g>;

    fn polygons(&self) -> &[Self::R] {
        &self.polygons
    }
}

impl ToWkt<f64> for MultiPolygonRef<'_> {
    fn to_wkt(&self) -> Wkt<f64> {
        let multi_polygon = self.polygons();
        let mut wkt_multi_polygon =
            wkt::types::MultiPolygon(Vec::with_capacity(multi_polygon.len()));

        for polygon in multi_polygon {
            let mut wkt_polygon = wkt::types::Polygon(Vec::with_capacity(polygon.len()));

            for ring in polygon {
                let mut wkt_line_string = wkt::types::LineString(Vec::with_capacity(ring.len()));

                for coord in *ring {
                    wkt_line_string.0.push(coord.into());
                }

                wkt_polygon.0.push(wkt_line_string);
            }

            wkt_multi_polygon.0.push(wkt_polygon);
        }

        Wkt::MultiPolygon(wkt_multi_polygon)
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

impl<'g> From<&'g MultiPolygon> for MultiPolygonRef<'g> {
    fn from(multi_point_ref: &'g MultiPolygon) -> Self {
        let mut polygons = Vec::with_capacity(multi_point_ref.polygons().len());

        for polygon in multi_point_ref.polygons() {
            let mut rings = Vec::with_capacity(polygon.len());

            for ring in polygon {
                rings.push(ring.as_ref());
            }

            polygons.push(rings);
        }

        MultiPolygonRef::new_unchecked(polygons)
    }
}

impl<'g> From<&MultiPolygonRef<'g>> for geo::MultiPolygon<f64> {
    fn from(geometry: &MultiPolygonRef<'g>) -> Self {
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
    use super::*;
    use arrow::array::{Array, ArrayBuilder};
    use float_cmp::approx_eq;

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

    #[test]
    fn test_to_wkt() {
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

        let a_ref = MultiPolygonRef::from(&a);

        assert_eq!(
            a_ref.wkt_string(),
            "MULTIPOLYGON(((0.1 0.1,0.8 0.1,0.8 0.8,0.1 0.1),(0.2 0.2,0.9 0.2,0.9 0.9,0.2 0.2)),((1.1 1.1,1.8 1.1,1.8 1.8,1.1 1.1),(1.2 1.2,1.9 1.2,1.9 1.9,1.2 1.2)))"
        );
    }

    #[test]
    fn test_to_geo_and_back() {
        let polygon = MultiPolygon::new(vec![
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

        let geo_polygon = geo::MultiPolygon::<f64>::from(&polygon);

        let polygon_back = MultiPolygon::from(geo_polygon);

        assert_eq!(polygon, polygon_back);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn arrow_builder_size() {
        fn push_geometry(
            geometries_builder: &mut <MultiPolygon as ArrowTyped>::ArrowBuilder,
            geometry: &MultiPolygon,
        ) {
            let polygon_builder = geometries_builder.values();

            for polygon in geometry.polygons() {
                let ring_builder = polygon_builder.values();

                for ring in polygon {
                    let coordinate_builder = ring_builder.values();

                    for coordinate in ring {
                        coordinate_builder
                            .values()
                            .append_slice(coordinate.as_ref());

                        coordinate_builder.append(true);
                    }

                    ring_builder.append(true);
                }

                polygon_builder.append(true);
            }

            geometries_builder.append(true);
        }

        for num_multi_polygons in 0..64 {
            for capacity in [0, num_multi_polygons] {
                let mut builder = MultiPolygon::arrow_builder(capacity);

                for i in 0..num_multi_polygons {
                    match i % 3 {
                        0 => {
                            push_geometry(
                                &mut builder,
                                &MultiPolygon::new(vec![
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
                                .unwrap(),
                            );
                        }
                        1 => {
                            push_geometry(
                                &mut builder,
                                &MultiPolygon::new(vec![
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
                                .unwrap(),
                            );
                        }
                        2 => {
                            push_geometry(
                                &mut builder,
                                &MultiPolygon::new(vec![vec![
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
                                ]])
                                .unwrap(),
                            );
                        }
                        _ => unreachable!(),
                    }
                }

                assert_eq!(builder.len(), num_multi_polygons);

                let builder_byte_size = MultiPolygon::estimate_array_memory_size(&mut builder);

                let array = builder.finish_cloned();

                assert_eq!(
                    builder_byte_size,
                    array.get_array_memory_size(),
                    "{num_multi_polygons}"
                );
            }
        }
    }
}
