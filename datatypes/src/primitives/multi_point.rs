use crate::error;
use crate::primitives::Coordinate2D;
use crate::util::Result;
use geojson::Geometry;
use snafu::ensure;

pub struct MultiPoint<'g> {
    coordinates: &'g [Coordinate2D],
}

impl<'g> MultiPoint<'g> {
    pub fn new(coordinates: &'g [Coordinate2D]) -> Result<Self> {
        ensure!(
            !coordinates.is_empty(),
            error::FeatureCollection {
                details: "MultiPoint must not be empty"
            }
        );

        Ok(Self::new_unchecked(coordinates))
    }

    pub fn new_unchecked(coordinates: &'g [Coordinate2D]) -> Self {
        Self { coordinates }
    }
}

impl<'g> Into<geojson::Geometry> for MultiPoint<'g> {
    fn into(self) -> Geometry {
        geojson::Geometry::new(match self.coordinates.len() {
            1 => {
                let floats: [f64; 2] = self.coordinates[0].into();
                geojson::Value::Point(floats.to_vec())
            }
            _ => geojson::Value::MultiPoint(
                self.coordinates
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
