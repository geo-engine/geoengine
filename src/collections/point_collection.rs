use crate::collections::FeatureCollection;
use crate::operations::Filterable;
use crate::primitives::Coordinate;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PointCollection {
    coordinates: Vec<Coordinate>,
}

impl PointCollection {
    /// Add a new point to the colleciton;
    pub fn add_point(&mut self, coordinate: Coordinate) {
        self.coordinates.push(coordinate);
    }
}

impl FeatureCollection for PointCollection {
    fn len(&self) -> usize {
        self.coordinates.len()
    }
}

impl Filterable for PointCollection {
    fn filter(&self, mask: &[bool]) -> Self {
        let filtered_points = self
            .coordinates
            .iter()
            .zip(mask)
            .filter_map(|(&point, &flag)| if flag { Some(point) } else { None })
            .collect();
        Self {
            coordinates: filtered_points,
        }
    }

    fn filter_with_predicate<P>(&self, mut predicate: P) -> Self
    where
        P: FnMut(&Coordinate) -> bool,
    {
        let filtered_points = self
            .coordinates
            .iter()
            .filter_map(|point| if predicate(point) { Some(*point) } else { None })
            .collect();
        Self {
            coordinates: filtered_points,
        }
    }

    fn filter_inplace(&mut self, mask: &[bool]) {
        let mut i = 0;
        for (j, &flag) in mask.iter().enumerate() {
            if flag {
                self.coordinates.swap(i, j);
                i += 1;
            }
        }
        self.coordinates.resize_with(i, || unreachable!());
    }

    fn filter_inplace_with_predicate<P>(&mut self, mut predicate: P)
    where
        P: FnMut(&Coordinate) -> bool,
    {
        let mut i = 0;
        for j in 0..self.coordinates.len() {
            if predicate(&self.coordinates[j]) {
                self.coordinates.swap(i, j);
                i += 1;
            }
        }
        self.coordinates.resize_with(i, || unreachable!());
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic() {
        let pc = PointCollection {
            coordinates: Vec::new(),
        };

        assert_eq!(pc.len(), 0);
    }

    #[test]
    fn filter() {
        let pc = PointCollection {
            coordinates: vec![(0.0, 0.0).into(), (1.0, 1.0).into(), (2.0, 2.0).into()],
        };

        let filtered = pc.filter(&[true, false, true]);

        assert_eq!(filtered.len(), 2);
        assert_eq!(
            filtered.coordinates,
            vec![(0.0, 0.0).into(), (2.0, 2.0).into()]
        );
        assert_eq!(pc.len(), 3);
    }

    #[test]
    fn filter_with_predicate() {
        let pc = PointCollection {
            coordinates: vec![(0.0, 0.0).into(), (1.0, 1.0).into(), (2.0, 2.0).into()],
        };

        let filtered = pc.filter_with_predicate(|&point| point != (1.0, 1.0).into());

        assert_eq!(filtered.len(), 2);
        assert_eq!(
            filtered.coordinates,
            vec![(0.0, 0.0).into(), (2.0, 2.0).into()]
        );
        assert_eq!(pc.len(), 3);
    }

    #[test]
    fn filter_inplace() {
        let pc = PointCollection {
            coordinates: vec![(0.0, 0.0).into(), (1.0, 1.0).into(), (2.0, 2.0).into()],
        };

        let filtered = pc.filter(&[true, false, true]);

        assert_eq!(filtered.len(), 2);
        assert_eq!(
            filtered.coordinates,
            vec![(0.0, 0.0).into(), (2.0, 2.0).into()]
        );
        assert_eq!(pc.len(), 3);
    }

    #[test]
    fn filter_inplace_with_predicate() {
        let mut pc = PointCollection {
            coordinates: vec![(0.0, 0.0).into(), (1.0, 1.0).into(), (2.0, 2.0).into()],
        };

        pc.filter_inplace_with_predicate(|&point| point != (1.0, 1.0).into());

        assert_eq!(pc.len(), 2);
        assert_eq!(pc.coordinates, vec![(0.0, 0.0).into(), (2.0, 2.0).into()]);
    }

    #[test]
    fn add_point() {
        let mut pc = PointCollection {
            coordinates: vec![],
        };

        pc.add_point((0.0, 0.0).into());

        assert_eq!(pc.len(), 1);
        assert_eq!(pc.coordinates, vec![(0.0, 0.0).into()]);
    }

    #[test]
    fn empty() {
        let mut pc = PointCollection {
            coordinates: vec![],
        };

        assert!(pc.is_empty());

        pc.add_point((0.1, 2.3).into());

        assert!(!pc.is_empty());
    }
}
