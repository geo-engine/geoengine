use crate::primitives::Coordinate;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PointCollection {
    points: Vec<Coordinate>,
}

impl PointCollection {
    /// Returns the number of point features
    pub fn len(&self) -> usize {
        self.points.len()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic() {
        let pc = PointCollection {
            points: Vec::new(),
        };

        assert_eq!(pc.len(), 0);
    }
}
