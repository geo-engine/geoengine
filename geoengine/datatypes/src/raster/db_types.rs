use postgres_types::{FromSql, ToSql};

use crate::delegate_from_to_sql;

use super::GridBoundingBox2D;

#[derive(Debug, PartialEq, ToSql, FromSql)]
#[postgres(name = "GridBoundingBox2D")]
pub struct GridBoundingBox2DDbType {
    y_min: i64,
    y_max: i64,
    x_min: i64,
    x_max: i64,
}

impl From<&GridBoundingBox2D> for GridBoundingBox2DDbType {
    fn from(value: &GridBoundingBox2D) -> Self {
        Self {
            y_min: value.y_min() as i64,
            y_max: value.y_max() as i64,
            x_min: value.x_min() as i64,
            x_max: value.x_max() as i64,
        }
    }
}

impl From<GridBoundingBox2DDbType> for GridBoundingBox2D {
    fn from(value: GridBoundingBox2DDbType) -> Self {
        GridBoundingBox2D::new_min_max(
            value.y_min as isize,
            value.y_max as isize,
            value.x_min as isize,
            value.x_max as isize,
        )
        .expect("conversion must be correct")
    }
}

delegate_from_to_sql!(GridBoundingBox2D, GridBoundingBox2DDbType);
