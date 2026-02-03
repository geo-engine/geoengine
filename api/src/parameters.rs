use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, PartialOrd, Serialize, Default, ToSchema)]
pub struct Coordinate2D {
    pub x: f64,
    pub y: f64,
}
impl From<Coordinate2D> for geoengine_datatypes::primitives::Coordinate2D {
    fn from(value: Coordinate2D) -> Self {
        geoengine_datatypes::primitives::Coordinate2D {
            x: value.x,
            y: value.y,
        }
    }
}
