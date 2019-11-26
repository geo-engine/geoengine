use failure::_core::cmp::Ordering;
use failure::_core::fmt::{Error, Formatter};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Stores time intervals in ms in close-open semantic [start, end)
#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub struct TimeInterval {
    start: i64,
    end: i64,
}

impl TimeInterval {
    /// Create a new time interval and check bounds
    pub fn new(start: i64, end: i64) -> Result<Self, ()> {
        if start <= end {
            Ok(Self { start, end })
        } else {
            Err(()) // TODO: error type
        }
    }

    /// Create a new time interval without bound checks
    pub unsafe fn new_unchecked(start: i64, end: i64) -> Self {
        Self { start, end }
    }

    /// Returns whether the other TimeInterval is contained (smaller or equal) within this interval
    pub fn contains(&self, other: &Self) -> bool {
        self.start <= other.start && self.end >= other.end
    }

    /// Returns whether the given interval intersects this interval
    pub fn intersects(&self, other: &Self) -> bool {
        self.start < other.end && self.end > other.start
    }

    pub fn union(&self, other: &Self) -> Result<Self, ()> {
        if self.intersects(other) || self.start == other.end || self.end == other.start {
            Ok(Self {
                start: i64::min(self.start, other.start),
                end: i64::max(self.end, other.end),
            })
        } else {
            Err(()) // TODO: error type
        }
    }
}

impl Debug for TimeInterval {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "TimeInterval [{}, {})", self.start, self.end)
    }
}

impl PartialOrd for TimeInterval {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.eq(other) {
            Some(Ordering::Equal)
        } else if self.end <= other.start {
            Some(Ordering::Less)
        } else if self.start >= other.end {
            Some(Ordering::Greater)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use crate::primitives::TimeInterval;

    #[test]
    fn construct() {
        TimeInterval::new(0, 0).unwrap();
        TimeInterval::new(0, 1).unwrap();

        TimeInterval::new(1, 0).unwrap_err();

        assert_eq!(TimeInterval::new(0, 1).unwrap(), unsafe {
            TimeInterval::new_unchecked(0, 1)
        });
    }

    #[test]
    fn ord() {
        assert_eq!(TimeInterval::new(0, 1), TimeInterval::new(0, 1));
        assert_ne!(TimeInterval::new(0, 1), TimeInterval::new(1, 2));

        assert!(TimeInterval::new(0, 1) <= TimeInterval::new(0, 1));
        assert!(TimeInterval::new(0, 1) <= TimeInterval::new(1, 2));
        assert!(TimeInterval::new(0, 1) < TimeInterval::new(1, 2));

        assert!(TimeInterval::new(0, 1) >= TimeInterval::new(0, 1));
        assert!(TimeInterval::new(1, 2) >= TimeInterval::new(0, 1));
        assert!(TimeInterval::new(1, 2) > TimeInterval::new(0, 1));

        assert!(TimeInterval::new(0, 2)
            .partial_cmp(&TimeInterval::new(1, 3))
            .is_none());

        assert!(TimeInterval::new(0, 1)
            .partial_cmp(&TimeInterval::new(0, 2))
            .is_none());
    }

    #[test]
    fn contains() {
        let valid_pairs = vec![
            ((0, 1), (0, 1)),
            ((0, 3), (1, 2)),
            ((0, 2), (0, 1)),
            ((0, 2), (1, 2)),
        ];

        for ((t1, t2), (t3, t4)) in valid_pairs {
            let i1 = TimeInterval::new(t1, t2).unwrap();
            let i2 = TimeInterval::new(t3, t4).unwrap();
            assert!(i1.contains(&i2), "{:?} should contain {:?}", i1, i2);
        }

        let invalid_pairs = vec![((0, 1), (-1, 2))];

        for ((t1, t2), (t3, t4)) in invalid_pairs {
            let i1 = TimeInterval::new(t1, t2).unwrap();
            let i2 = TimeInterval::new(t3, t4).unwrap();
            assert!(!i1.contains(&i2), "{:?} should not contain {:?}", i1, i2);
        }
    }

    #[test]
    fn intersects() {
        let valid_pairs = vec![
            ((0, 1), (0, 1)),
            ((0, 3), (1, 2)),
            ((0, 2), (1, 3)),
            ((0, 1), (0, 2)),
            ((0, 2), (-2, 1)),
        ];

        for ((t1, t2), (t3, t4)) in valid_pairs {
            let i1 = TimeInterval::new(t1, t2).unwrap();
            let i2 = TimeInterval::new(t3, t4).unwrap();
            assert!(i1.intersects(&i2), "{:?} should intersect {:?}", i1, i2);
        }

        let invalid_pairs = vec![
            ((0, 1), (-1, 0)), //
            ((0, 1), (1, 2)),
            ((0, 1), (2, 3)),
        ];

        for ((t1, t2), (t3, t4)) in invalid_pairs {
            let i1 = TimeInterval::new(t1, t2).unwrap();
            let i2 = TimeInterval::new(t3, t4).unwrap();
            assert!(
                !i1.intersects(&i2),
                "{:?} should not intersect {:?}",
                i1,
                i2
            );
        }
    }

    #[test]
    fn union() {
        let i1 = TimeInterval::new(0, 2).unwrap();
        let i2 = TimeInterval::new(1, 3).unwrap();
        let i3 = TimeInterval::new(2, 4).unwrap();
        let i4 = TimeInterval::new(3, 5).unwrap();

        assert_eq!(i1.union(&i2).unwrap(), TimeInterval::new(0, 3).unwrap());
        assert_eq!(i1.union(&i3).unwrap(), TimeInterval::new(0, 4).unwrap());
        i1.union(&i4).unwrap_err();
    }
}
