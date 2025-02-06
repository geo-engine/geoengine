use std::ops::{Add, BitAnd, BitOr, BitXor, Shr};

/// From `num_integer`.
/// Returns the floor value of the average of `a` and `b` without overflow problems.
#[inline]
pub fn average_floor<I>(a: I, b: I) -> I
where
    I: Copy
        + Add<I, Output = I>
        + Shr<usize, Output = I>
        + BitAnd<I, Output = I>
        + BitOr<I, Output = I>
        + BitXor<I, Output = I>,
{
    (a & b) + ((a ^ b) >> 1)
}

/// Check if a number is a power of two.
pub fn is_power_of_two(n: u32) -> bool {
    n > 0 && (n & (n - 1)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn average_floor_checks() {
        assert_eq!(
            average_floor(631_152_000_000_i64, 946_684_800_001_i64),
            788_918_400_000_i64
        );

        assert_eq!(average_floor(i64::MIN, i64::MAX), -1);
    }

    #[test]
    fn it_checks_power_of_two() {
        assert!(is_power_of_two(1));
        assert!(is_power_of_two(2));
        assert!(is_power_of_two(4));
        assert!(is_power_of_two(8));
        assert!(is_power_of_two(16));

        assert!(!is_power_of_two(3));
        assert!(!is_power_of_two(5));
    }
}
