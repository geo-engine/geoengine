use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
pub struct Coordinate {
    pub x: f64,
    pub y: f64,
}

impl Coordinate {
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }
}

impl From<(f64, f64)> for Coordinate {
    fn from(tuple: (f64, f64)) -> Self {
        let (x, y) = tuple;
        Self { x, y }
    }
}

impl From<[f64; 2]> for Coordinate {
    fn from(array: [f64; 2]) -> Self {
        let [x, y] = array;
        Self { x, y }
    }
}

impl Into<(f64, f64)> for Coordinate {
    fn into(self) -> (f64, f64) {
        (self.x, self.y)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn constructor() {
        let c = Coordinate::new(1.0, 0.0);

        assert_eq!(c.x, 1.0);
        assert_eq!(c.y, 0.0);
    }

    #[test]
    fn from_tuple() {
        let c = Coordinate::from((5.0, 4.2));

        assert_eq!(c.x, 5.0);
        assert_eq!(c.y, 4.2);

        let c: Coordinate = (5.1, -3.0).into();

        assert_eq!(c.x, 5.1);
        assert_eq!(c.y, -3.0);
    }

    #[test]
    fn to_tuple() {
        let c = Coordinate::new(-1.9, 0.04);

        let (x, y) = c.into();

        assert_eq!(x, -1.9);
        assert_eq!(y, 0.04);
    }

    #[test]
    fn from_array() {
        let c = Coordinate::from([5.0, 4.2]);

        assert_eq!(c.x, 5.0);
        assert_eq!(c.y, 4.2);

        let c: Coordinate = [5.1, -3.0].into();

        assert_eq!(c.x, 5.1);
        assert_eq!(c.y, -3.0);
    }
}
