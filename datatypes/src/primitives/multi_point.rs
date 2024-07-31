use super::SpatialBounded;
use crate::collections::VectorDataType;
use crate::error::Error;
use crate::primitives::{error, BoundingBox2D, GeometryRef, PrimitivesError, TypedGeometry};
use crate::primitives::{Coordinate2D, Geometry};
use crate::util::arrow::{downcast_array, padded_buffer_size, ArrowTyped};
use crate::util::Result;
use arrow::array::{BooleanArray, FixedSizeListArray, Float64Array};
use arrow::error::ArrowError;
use fallible_iterator::FallibleIterator;
use float_cmp::{ApproxEq, F64Margin};
use postgres_types::{FromSql, ToSql};
use proj::Coord;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::convert::{TryFrom, TryInto};
use wkt::{ToWkt, Wkt};

/// A trait that allows a common access to points of `MultiPoint`s and its references
pub trait MultiPointAccess {
    fn points(&self) -> &[Coordinate2D];
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

    pub fn many<M, E>(raw_multi_points: Vec<M>) -> Result<Vec<Self>>
    where
        M: TryInto<MultiPoint, Error = E>,
        E: Into<crate::error::Error>,
    {
        raw_multi_points
            .into_iter()
            .map(|m| m.try_into().map_err(Into::into))
            .collect()
    }
}

impl MultiPointAccess for MultiPoint {
    fn points(&self) -> &[Coordinate2D] {
        &self.coordinates
    }
}

impl Geometry for MultiPoint {
    const DATA_TYPE: VectorDataType = VectorDataType::MultiPoint;

    fn intersects_bbox(&self, bbox: &BoundingBox2D) -> bool {
        self.coordinates.iter().any(|c| bbox.contains_coordinate(c))
    }
}

impl TryFrom<TypedGeometry> for MultiPoint {
    type Error = Error;

    fn try_from(value: TypedGeometry) -> Result<Self, Self::Error> {
        if let TypedGeometry::MultiPoint(geometry) = value {
            Ok(geometry)
        } else {
            Err(PrimitivesError::InvalidConversion.into())
        }
    }
}

impl AsRef<[Coordinate2D]> for MultiPoint {
    fn as_ref(&self) -> &[Coordinate2D] {
        &self.coordinates
    }
}

impl<C> From<C> for MultiPoint
where
    C: Into<Coordinate2D>,
{
    fn from(c: C) -> Self {
        Self::new_unchecked(vec![c.into()])
    }
}

impl From<&Coordinate2D> for MultiPoint {
    fn from(point: &Coordinate2D) -> Self {
        Self::new_unchecked(vec![*point])
    }
}

impl TryFrom<Vec<Coordinate2D>> for MultiPoint {
    type Error = crate::error::Error;

    fn try_from(coordinates: Vec<Coordinate2D>) -> Result<Self, Self::Error> {
        MultiPoint::new(coordinates)
    }
}

impl TryFrom<Vec<(f64, f64)>> for MultiPoint {
    type Error = crate::error::Error;

    fn try_from(coordinates: Vec<(f64, f64)>) -> Result<Self, Self::Error> {
        MultiPoint::new(coordinates.into_iter().map(Into::into).collect())
    }
}

impl<'g> From<&MultiPointRef<'g>> for geo::MultiPoint<f64> {
    fn from(geometry: &MultiPointRef<'g>) -> Self {
        let points: Vec<geo::Point<f64>> = geometry
            .point_coordinates
            .iter()
            .map(|coordinate| geo::Point::new(coordinate.x(), coordinate.y()))
            .collect();
        geo::MultiPoint(points)
    }
}

impl TryFrom<geo::MultiPoint<f64>> for MultiPoint {
    type Error = crate::error::Error;

    fn try_from(geometry: geo::MultiPoint<f64>) -> Result<Self> {
        let points = geometry.0;
        let coordinates = points
            .into_iter()
            .map(|point| Coordinate2D::new(point.x(), point.y()))
            .collect();
        MultiPoint::new(coordinates)
    }
}

impl ToSql for MultiPoint {
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
            |c, w| {
                postgres_protocol::types::point_to_sql(c.x, c.y, w);

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

        matches!(inner_type, &postgres_types::Type::POINT)
    }

    postgres_types::to_sql_checked!();
}

impl<'a> FromSql<'a> for MultiPoint {
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
                let point = postgres_protocol::types::point_from_sql(raw)?;
                Ok(Coordinate2D {
                    x: point.x(),
                    y: point.y(),
                })
            })
            .collect()?;

        Ok(Self { coordinates })
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        let postgres_types::Kind::Array(inner_type) = ty.kind() else {
            return false;
        };

        matches!(inner_type, &postgres_types::Type::POINT)
    }
}

impl ArrowTyped for MultiPoint {
    type ArrowArray = arrow::array::ListArray;
    type ArrowBuilder = arrow::array::ListBuilder<<Coordinate2D as ArrowTyped>::ArrowBuilder>;

    fn arrow_data_type() -> arrow::datatypes::DataType {
        Coordinate2D::arrow_list_data_type()
    }

    fn estimate_array_memory_size(builder: &mut Self::ArrowBuilder) -> usize {
        let static_size = std::mem::size_of::<Self::ArrowArray>();

        let coords_size = Coordinate2D::estimate_array_memory_size(builder.values());

        let offset_bytes = builder.offsets_slice();
        let offset_bytes_size = std::mem::size_of_val(offset_bytes);

        static_size + coords_size + padded_buffer_size(offset_bytes_size, 64)
    }

    fn arrow_builder(capacity: usize) -> Self::ArrowBuilder {
        arrow::array::ListBuilder::new(Coordinate2D::arrow_builder(capacity))
    }

    fn concat(a: &Self::ArrowArray, b: &Self::ArrowArray) -> Result<Self::ArrowArray, ArrowError> {
        use arrow::array::Array;

        let mut new_multipoints = Self::arrow_builder(a.len() + b.len());

        for old_multipoints in &[a, b] {
            for multipoint_index in 0..old_multipoints.len() {
                let multipoint_ref = old_multipoints.value(multipoint_index);
                let multipoint: &FixedSizeListArray = downcast_array(&multipoint_ref);

                let new_points = new_multipoints.values();

                for point_index in 0..multipoint.len() {
                    let floats_ref = multipoint.value(point_index);
                    let floats: &Float64Array = downcast_array(&floats_ref);

                    let new_floats = new_points.values();
                    new_floats.append_slice(floats.values());

                    new_points.append(true);
                }

                new_multipoints.append(true);
            }
        }

        Ok(new_multipoints.finish_cloned())
    }

    fn filter(
        features: &Self::ArrowArray,
        filter_array: &BooleanArray,
    ) -> Result<Self::ArrowArray, ArrowError> {
        use arrow::array::Array;

        let mut new_features = Self::arrow_builder(0);

        for feature_index in 0..features.len() {
            if filter_array.value(feature_index) {
                let coordinate_builder = new_features.values();

                let old_coordinates = features.value(feature_index);

                for coordinate_index in 0..features.value_length(feature_index) {
                    let old_floats_array = downcast_array::<FixedSizeListArray>(&old_coordinates)
                        .value(coordinate_index as usize);

                    let old_floats: &Float64Array = downcast_array(&old_floats_array);

                    let float_builder = coordinate_builder.values();
                    float_builder.append_slice(old_floats.values());

                    coordinate_builder.append(true);
                }

                new_features.append(true);
            }
        }

        Ok(new_features.finish_cloned())
    }

    fn from_vec(multi_points: Vec<Self>) -> Result<Self::ArrowArray, ArrowError>
    where
        Self: Sized,
    {
        let mut builder = Self::arrow_builder(multi_points.len());
        for multi_point in multi_points {
            let coordinate_builder = builder.values();
            for coordinate in multi_point.as_ref() {
                let float_builder = coordinate_builder.values();
                float_builder.append_value(coordinate.x);
                float_builder.append_value(coordinate.y);
                coordinate_builder.append(true);
            }
            builder.append(true);
        }

        Ok(builder.finish_cloned())
    }
}

#[derive(Debug, PartialEq)]
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

    pub fn bbox(&self) -> Option<BoundingBox2D> {
        BoundingBox2D::from_coord_ref_iter(self.point_coordinates)
    }
}

impl<'r> GeometryRef for MultiPointRef<'r> {
    type GeometryType = MultiPoint;

    fn as_geometry(&self) -> Self::GeometryType {
        MultiPoint::from(self)
    }

    fn bbox(&self) -> Option<BoundingBox2D> {
        self.bbox()
    }
}

impl<'g> MultiPointAccess for MultiPointRef<'g> {
    fn points(&self) -> &[Coordinate2D] {
        self.point_coordinates
    }
}

impl<'r> ToWkt<f64> for MultiPointRef<'r> {
    fn to_wkt(&self) -> Wkt<f64> {
        let points = self.points();
        let mut multi_point = wkt::types::MultiPoint(Vec::with_capacity(points.len()));

        for point in points {
            multi_point.0.push(wkt::types::Point(Some(point.into())));
        }

        Wkt::MultiPoint(multi_point)
    }
}

impl<'g> From<MultiPointRef<'g>> for geojson::Geometry {
    fn from(geometry: MultiPointRef<'g>) -> geojson::Geometry {
        geojson::Geometry::new(match geometry.point_coordinates.len() {
            1 => {
                let floats: [f64; 2] = geometry.point_coordinates[0].into();
                geojson::Value::Point(floats.to_vec())
            }
            _ => geojson::Value::MultiPoint(
                geometry
                    .point_coordinates
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

impl<'g> From<MultiPointRef<'g>> for MultiPoint {
    fn from(multi_point_ref: MultiPointRef<'g>) -> Self {
        MultiPoint::from(&multi_point_ref)
    }
}

impl<'g> From<&MultiPointRef<'g>> for MultiPoint {
    fn from(multi_point_ref: &MultiPointRef<'g>) -> Self {
        MultiPoint::new_unchecked(multi_point_ref.point_coordinates.to_owned())
    }
}

impl<'g> From<&'g MultiPoint> for MultiPointRef<'g> {
    fn from(multi_point: &'g MultiPoint) -> Self {
        MultiPointRef::new_unchecked(&multi_point.coordinates)
    }
}

impl<A> SpatialBounded for A
where
    A: MultiPointAccess,
{
    fn spatial_bounds(&self) -> BoundingBox2D {
        BoundingBox2D::from_coord_ref_iter(self.points())
            .expect("there must be at least one cordinate in a multipoint")
    }
}

impl ApproxEq for &MultiPoint {
    type Margin = F64Margin;

    fn approx_eq<M: Into<Self::Margin>>(self, other: Self, margin: M) -> bool {
        let m = margin.into();
        self.coordinates.len() == other.coordinates.len()
            && self
                .coordinates
                .iter()
                .zip(other.coordinates.iter())
                .all(|(&a, &b)| a.approx_eq(b, m))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, ArrayBuilder};
    use float_cmp::approx_eq;

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

    #[test]
    fn intersects_bbox() -> Result<()> {
        let bbox = BoundingBox2D::new((0.0, 0.0).into(), (1.0, 1.0).into())?;

        assert!(MultiPoint::new(vec![(0.5, 0.5).into()])?.intersects_bbox(&bbox));
        assert!(MultiPoint::new(vec![(1.0, 1.0).into()])?.intersects_bbox(&bbox));
        assert!(MultiPoint::new(vec![(0.5, 0.5).into(), (1.5, 1.5).into()])?.intersects_bbox(&bbox));
        assert!(!MultiPoint::new(vec![(1.1, 1.1).into()])?.intersects_bbox(&bbox));
        assert!(
            !MultiPoint::new(vec![(-0.1, -0.1).into(), (1.1, 1.1).into()])?.intersects_bbox(&bbox)
        );

        Ok(())
    }

    #[test]
    fn spatial_bounds() {
        let expected = BoundingBox2D::new_unchecked((0., 0.).into(), (1., 1.).into());
        let coordinates: Vec<Coordinate2D> = Vec::from([
            (1., 0.4).into(),
            (0.8, 0.0).into(),
            (0.3, 0.1).into(),
            (0.0, 1.0).into(),
        ]);
        let mp = MultiPoint { coordinates };
        assert_eq!(mp.spatial_bounds(), expected);
    }

    #[test]
    fn approx_equal() {
        let a = MultiPoint::new(vec![
            (0.5, 0.5).into(),
            (0.5, 0.5).into(),
            (0.5, 0.5).into(),
        ])
        .unwrap();

        let b = MultiPoint::new(vec![
            (0.5, 0.499_999_999).into(),
            (0.5, 0.5).into(),
            (0.5, 0.5).into(),
        ])
        .unwrap();

        assert!(approx_eq!(&MultiPoint, &a, &b, epsilon = 0.000_001));
    }

    #[test]
    fn not_approx_equal_len() {
        let a = MultiPoint::new(vec![
            (0.5, 0.5).into(),
            (0.5, 0.5).into(),
            (0.5, 0.5).into(),
        ])
        .unwrap();

        let b = MultiPoint::new(vec![
            (0.5, 0.5).into(),
            (0.5, 0.5).into(),
            (0.5, 0.5).into(),
            (123_456_789.5, 123_456_789.5).into(),
        ])
        .unwrap();

        assert!(!approx_eq!(&MultiPoint, &a, &b, F64Margin::default()));
    }

    #[test]
    fn test_to_wkt() {
        let a = MultiPoint::new(vec![
            (0.5, 0.6).into(),
            (0.7, 0.8).into(),
            (0.9, 0.99).into(),
        ])
        .unwrap();

        let a_ref = MultiPointRef::from(&a);

        assert_eq!(
            a_ref.wkt_string(),
            "MULTIPOINT((0.5 0.6),(0.7 0.8),(0.9 0.99))"
        );
    }

    #[test]
    fn arrow_builder_size_points() {
        for num_multipoints in 0..514 {
            for capacity in [0, num_multipoints] {
                let mut multi_points_builder = MultiPoint::arrow_builder(capacity);

                for _ in 0..num_multipoints {
                    multi_points_builder
                        .values()
                        .values()
                        .append_values(&[1., 2.], &[true, true]);

                    multi_points_builder.values().append(true);

                    multi_points_builder.append(true);
                }

                assert_eq!(multi_points_builder.len(), num_multipoints);

                let builder_byte_size =
                    MultiPoint::estimate_array_memory_size(&mut multi_points_builder);

                let array = multi_points_builder.finish_cloned();

                assert_eq!(
                    builder_byte_size,
                    array.get_array_memory_size(),
                    "{num_multipoints}"
                );
            }
        }
    }

    #[test]
    fn arrow_builder_size_multi_points() {
        // TODO: test fewer multipoints?
        for num_multipoints in 0..514 {
            for capacity in [0, num_multipoints] {
                let mut multi_points_builder = MultiPoint::arrow_builder(capacity);

                // we have 1 point for the first multipoint, 2 for the second, etc.
                for num_points in 0..num_multipoints {
                    for _ in 0..num_points {
                        multi_points_builder
                            .values()
                            .values()
                            .append_values(&[1., 2.], &[true, true]);

                        multi_points_builder.values().append(true);
                    }

                    multi_points_builder.append(true);
                }

                assert_eq!(multi_points_builder.len(), num_multipoints);

                let builder_byte_size =
                    MultiPoint::estimate_array_memory_size(&mut multi_points_builder);

                let array = multi_points_builder.finish_cloned();

                assert_eq!(
                    builder_byte_size,
                    array.get_array_memory_size(),
                    "{num_multipoints}"
                );
            }
        }
    }
}
