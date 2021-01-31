use geo::{coords_iter::CoordsIter, Point, Rect};
use proj::{Proj, ProjError};

use crate::{primitives::BoundingBox2D, spatial_reference::SpatialReference};

pub trait Reproject<Out = Self> {
    fn reproject(
        &self,
        source_spatial_ref: SpatialReference,
        target_spatial_ref: SpatialReference,
    ) -> Result<Out, ()>;
}

impl Reproject for BoundingBox2D {
    fn reproject(
        &self,
        source_spatial_ref: SpatialReference,
        target_spatial_ref: SpatialReference,
    ) -> Result<Self, ()> {
        dbg!(source_spatial_ref, target_spatial_ref);
        let proj = Proj::new_known_crs(
            &source_spatial_ref.to_string(),
            &target_spatial_ref.to_string(),
            None,
        )
        .expect("handle ProjError");

        let rect: Rect<f64> = self.into();
        let res: std::result::Result<Vec<Point<f64>>, ProjError> =
            rect.coords_iter().map(|c| proj.convert(c)).collect();

        // then build new rect from the points here...
        Ok(self.clone())
    }
}
