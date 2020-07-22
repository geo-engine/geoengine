use crate::primitives::error;
use crate::primitives::{Coordinate2D, Geometry};
use crate::util::arrow::ArrowTyped;
use crate::util::Result;
use snafu::ensure;

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
#[derive(Debug, PartialEq)]
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

impl Geometry for MultiPolygon {}

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

    fn arrow_builder(_capacity: usize) -> Self::ArrowBuilder {
        let coordinate_builder = Coordinate2D::arrow_builder(0);
        let ring_builder = arrow::array::ListBuilder::new(coordinate_builder);
        let polygon_builder = arrow::array::ListBuilder::new(ring_builder);
        arrow::array::ListBuilder::new(polygon_builder)
    }
}

type RingRef<'g> = &'g [Coordinate2D];
type PolygonRef<'g> = Vec<RingRef<'g>>;

#[derive(Debug, PartialEq)]
pub struct MultiPolygonRef<'g> {
    polygons: Vec<PolygonRef<'g>>,
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
