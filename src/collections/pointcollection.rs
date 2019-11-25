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

    /// Add a new point to the colleciton;
    pub fn add_point(&mut self, coordinate: Coordinate) {
        self.points.push(coordinate);
    }

    /// Filters the collection by copying the data into a new collection
    pub fn filter(&self, mask: &[bool]) -> Self {
        let filtered_points = self.points.iter()
            .zip(mask)
            .filter_map(|(&point, &flag)| if flag { Some(point) } else { None })
            .collect();
        Self {
            points: filtered_points,
        }
    }

    /// Filters the collection using a predicate function by copying the data into a new collection
    pub fn filter_with_predicate<P>(&self, mut predicate: P) -> Self
        where P: FnMut(&Coordinate) -> bool {
        let filtered_points = self.points.iter()
            .filter_map(|point| if predicate(point) { Some(*point) } else { None })
            .collect();
        Self {
            points: filtered_points,
        }
    }

    /// Filters the collection inplace
    pub fn filter_inplace(&mut self, mask: &[bool]) {
        let mut i = 0;
        for (j, &flag) in mask.iter().enumerate() {
            if flag {
                self.points.swap(i, j);
                i += 1;
            }
        }
        self.points.resize_with(i, || unreachable!());
    }

    /// Filters the collection inplace using a predicate function
    pub fn filter_inplace_with_predicate<P>(&mut self, mut predicate: P)
        where P: FnMut(&Coordinate) -> bool {
        let mut i = 0;
        for j in 0..self.points.len() {
            if predicate(&self.points[j]) {
                self.points.swap(i, j);
                i += 1;
            }
        }
        self.points.resize_with(i, || unreachable!());
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

    #[test]
    fn filter() {
        let pc = PointCollection {
            points: vec![(0.0, 0.0).into(), (1.0, 1.0).into(), (2.0, 2.0).into()],
        };

        let filtered = pc.filter(&[true, false, true]);

        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered.points, vec![(0.0, 0.0).into(), (2.0, 2.0).into()]);
        assert_eq!(pc.len(), 3);
    }

    #[test]
    fn filter_with_predicate() {
        let pc = PointCollection {
            points: vec![(0.0, 0.0).into(), (1.0, 1.0).into(), (2.0, 2.0).into()],
        };

        let filtered = pc.filter_with_predicate(|&point| point != (1.0, 1.0).into());

        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered.points, vec![(0.0, 0.0).into(), (2.0, 2.0).into()]);
        assert_eq!(pc.len(), 3);
    }

    #[test]
    fn filter_inplace() {
        let pc = PointCollection {
            points: vec![(0.0, 0.0).into(), (1.0, 1.0).into(), (2.0, 2.0).into()],
        };

        let filtered = pc.filter(&[true, false, true]);

        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered.points, vec![(0.0, 0.0).into(), (2.0, 2.0).into()]);
        assert_eq!(pc.len(), 3);
    }

    #[test]
    fn filter_inplace_with_predicate() {
        let mut pc = PointCollection {
            points: vec![(0.0, 0.0).into(), (1.0, 1.0).into(), (2.0, 2.0).into()],
        };

        pc.filter_inplace_with_predicate(|&point| point != (1.0, 1.0).into());

        assert_eq!(pc.len(), 2);
        assert_eq!(pc.points, vec![(0.0, 0.0).into(), (2.0, 2.0).into()]);
    }


    #[test]
    fn add_point() {
        let mut pc = PointCollection {
            points: vec![],
        };

        pc.add_point((0.0, 0.0).into());

        assert_eq!(pc.len(), 1);
        assert_eq!(pc.points, vec![(0.0, 0.0).into()]);
    }
}
