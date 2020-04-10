use crate::error;
use crate::primitives::Coordinate2D;
use crate::util::Result;
use geojson::Geometry;
use snafu::ensure;

pub struct MultiPointRef<'g> {
    point_coordinates: &'g [Coordinate2D],
}

impl<'g> MultiPointRef<'g> {
    pub fn new(coordinates: &'g [Coordinate2D]) -> Result<Self> {
        ensure!(
            !coordinates.is_empty(),
            error::FeatureCollectionOld {
                details: "MultiPoint must not be empty"
            }
        );

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
    fn into(self) -> Geometry {
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
