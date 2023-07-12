use std::cmp::{max, min};

pub fn value_in_range<T>(value: T, min: T, max: T) -> bool
where
    T: PartialOrd + Copy,
{
    (value >= min) && (value < max)
}

pub fn value_in_range_inclusive<T>(value: T, min: T, max: T) -> bool
where
    T: PartialOrd + Copy,
{
    (value >= min) && (value <= max)
}

pub fn value_in_range_inv<T>(value: T, min: T, max: T) -> bool
where
    T: PartialOrd + Copy,
{
    (value > min) && (value <= max)
}

pub fn overlap_inclusive<T: Copy + PartialOrd + Ord>(a: (T, T), b: (T, T)) -> Option<(T, T)> {
    if value_in_range_inclusive(a.0, b.0, b.1) || value_in_range_inclusive(b.0, a.0, a.1) {
        let start = max(a.0, b.0);
        let end = min(a.1, b.1);
        return Some((start, end));
    }
    None
}

#[cfg(test)]
mod tests {

    #[test]
    fn value_in_range() {
        assert!(super::value_in_range::<u64>(0, 0, 1));
        assert!(!super::value_in_range::<u64>(1, 0, 1));

        assert!(super::value_in_range::<f64>(0., 0., 1.));
        assert!(!super::value_in_range::<f64>(1., 0., 1.));
    }

    #[test]
    fn value_in_range_inv() {
        assert!(!super::value_in_range_inv::<u64>(0, 0, 1));
        assert!(super::value_in_range_inv::<u64>(1, 0, 1));

        assert!(!super::value_in_range_inv::<f64>(0., 0., 1.));
        assert!(super::value_in_range_inv::<f64>(1., 0., 1.));
    }

    #[test]
    fn value_in_range_inclusive() {
        assert!(super::value_in_range_inclusive::<u64>(0, 0, 1));
        assert!(super::value_in_range_inclusive::<u64>(1, 0, 1));
        assert!(!super::value_in_range_inclusive::<u64>(2, 0, 1));

        assert!(super::value_in_range_inclusive::<f64>(0., 0., 1.));
        assert!(super::value_in_range_inclusive::<f64>(1., 0., 1.));
        assert!(!super::value_in_range_inclusive::<f64>(2., 0., 1.));
    }

    #[test]
    fn overlap_inclusive() {
        assert_eq!(super::overlap_inclusive((0, 1), (0, 1)), Some((0, 1)));
        assert_eq!(super::overlap_inclusive((0, 1), (0, 2)), Some((0, 1)));
        assert_eq!(super::overlap_inclusive((0, 1), (1, 2)), Some((1, 1)));
        assert_eq!(super::overlap_inclusive((0, 1), (2, 3)), None);
        assert_eq!(super::overlap_inclusive((0, 1), (1, 1)), Some((1, 1)));
        assert_eq!(super::overlap_inclusive((0, 1), (-1, 0)), Some((0, 0)));
        assert_eq!(super::overlap_inclusive((0, 1), (-1, -1)), None);
        assert_eq!(super::overlap_inclusive((0, 1), (-1, 1)), Some((0, 1)));
        assert_eq!(super::overlap_inclusive((0, 1), (-1, 2)), Some((0, 1)));
    }
}
