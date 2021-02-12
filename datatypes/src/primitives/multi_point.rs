use std::convert::{TryFrom, TryInto};

use arrow::array::{ArrayBuilder, BooleanArray};
use arrow::error::ArrowError;
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::collections::VectorDataType;
use crate::error::Error;
use crate::primitives::{error, BoundingBox2D, GeometryRef, PrimitivesError, TypedGeometry};
use crate::primitives::{Coordinate2D, Geometry};
use crate::util::arrow::{downcast_array, ArrowTyped};
use crate::util::Result;

use super::SpatialBounded;

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
        let mut multi_points = Vec::with_capacity(raw_multi_points.len());

        for multi_point in raw_multi_points {
            multi_points.push(multi_point.try_into().map_err(Into::into)?);
        }

        Ok(multi_points)
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

impl ArrowTyped for MultiPoint {
    type ArrowArray = arrow::array::ListArray;
    type ArrowBuilder = arrow::array::ListBuilder<<Coordinate2D as ArrowTyped>::ArrowBuilder>;

    fn arrow_data_type() -> arrow::datatypes::DataType {
        Coordinate2D::arrow_list_data_type()
    }

    fn builder_byte_size(builder: &mut Self::ArrowBuilder) -> usize {
        let multi_point_indices_size = builder.len() * std::mem::size_of::<i32>();

        let point_builder = builder.values();
        let point_indices_size = point_builder.len() * std::mem::size_of::<i32>();

        let coordinates_size = Coordinate2D::builder_byte_size(point_builder);

        multi_point_indices_size + point_indices_size + coordinates_size
    }

    fn arrow_builder(capacity: usize) -> Self::ArrowBuilder {
        arrow::array::ListBuilder::new(Coordinate2D::arrow_builder(capacity))
    }

    fn concat(a: &Self::ArrowArray, b: &Self::ArrowArray) -> Result<Self::ArrowArray, ArrowError> {
        use arrow::array::{Array, FixedSizeListArray, Float64Array};

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
                    new_floats.append_slice(floats.values())?;

                    new_points.append(true)?;
                }

                new_multipoints.append(true)?;
            }
        }

        Ok(new_multipoints.finish())
    }

    fn filter(
        features: &Self::ArrowArray,
        filter_array: &BooleanArray,
    ) -> Result<Self::ArrowArray, ArrowError> {
        use arrow::array::{Array, FixedSizeListArray, Float64Array};

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
                    float_builder.append_slice(old_floats.values())?;

                    coordinate_builder.append(true)?;
                }

                new_features.append(true)?;
            }
        }

        Ok(new_features.finish())
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
                float_builder.append_value(coordinate.x)?;
                float_builder.append_value(coordinate.y)?;
                coordinate_builder.append(true)?;
            }
            builder.append(true)?;
        }

        Ok(builder.finish())
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

impl<'r> GeometryRef for MultiPointRef<'r> {}

impl<'g> MultiPointAccess for MultiPointRef<'g> {
    fn points(&self) -> &[Coordinate2D] {
        &self.point_coordinates
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

impl<A> SpatialBounded for A
where
    A: MultiPointAccess,
{
    fn spatial_bounds(&self) -> BoundingBox2D {
        BoundingBox2D::from_coord_ref_iter(self.points())
            .expect("there must be at least one cordinate in a multipoint")
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
        assert_eq!(mp.spatial_bounds(), expected)
    }
}
