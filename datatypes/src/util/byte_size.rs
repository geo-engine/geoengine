use crate::raster::Pixel;
use std::collections::{hash_map::RandomState, HashMap};

/// A trait for types that have a size in bytes
/// that it takes up in memory
pub trait ByteSize: Sized {
    /// The memory size in bytes, which is allocated on the stack and known at compile time.
    fn stack_byte_size() -> usize {
        std::mem::size_of::<Self>()
    }

    /// The heap memory size in bytes.
    /// Must be implemented for types that allocate data on the heap.
    fn heap_byte_size(&self) -> usize {
        0
    }

    /// The memory size in bytes
    fn byte_size(&self) -> usize {
        Self::stack_byte_size() + self.heap_byte_size()
    }
}

impl ByteSize for String {
    fn heap_byte_size(&self) -> usize {
        self.capacity()
    }
}

impl ByteSize for Option<String> {
    fn heap_byte_size(&self) -> usize {
        self.as_ref().map_or(0, ByteSize::heap_byte_size)
    }
}

impl<T> ByteSize for Vec<T>
where
    T: ByteSize,
{
    fn heap_byte_size(&self) -> usize {
        self.iter().map(ByteSize::byte_size).sum::<usize>()
    }
}

impl ByteSize for Vec<bool> {
    fn heap_byte_size(&self) -> usize {
        self.capacity() * std::mem::size_of::<bool>()
    }
}

impl<K, V> ByteSize for HashMap<K, V, RandomState>
where
    K: ByteSize,
    V: ByteSize,
{
    fn heap_byte_size(&self) -> usize {
        self.iter()
            .map(|(k, v)| k.byte_size() + v.byte_size())
            .sum::<usize>()
    }
}

impl<P> ByteSize for P where P: Pixel {}

impl<P> ByteSize for Option<P> where P: Pixel {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_size_of_string() {
        assert_eq!(std::mem::size_of::<String>(), 24);
        assert_eq!(std::mem::size_of::<Option<String>>(), 24);

        assert_eq!(String::from("hello").byte_size(), 24 + 5);
        assert_eq!(Some(String::from("hello")).byte_size(), 24 + 5);
    }

    #[test]
    fn byte_size_of_vec() {
        assert_eq!(std::mem::size_of::<Vec<f64>>(), 24);
        assert_eq!(std::mem::size_of::<Vec<String>>(), 24);

        assert_eq!(vec![1_i32, 2, 3].byte_size(), 24 + 3 * 4);
        assert_eq!(
            vec![String::from("hello"), String::from("there")].byte_size(),
            24 + 2 * 24 + 2 * 5
        );

        assert_eq!(vec![true, false].byte_size(), 24 + 2);
    }

    #[test]
    fn byte_size_of_option() {
        assert_eq!(std::mem::size_of::<Option<u8>>(), 2);
        assert_eq!(std::mem::size_of::<Option<i32>>(), 8);
        assert_eq!(std::mem::size_of::<Option<i64>>(), 16);

        assert_eq!(Some(8_u8).byte_size(), 2);
        assert_eq!(Some(8_i16).byte_size(), 4);
        assert_eq!(Some(8_u32).byte_size(), 8);
    }

    #[test]
    fn byte_size_of_hash_map() {
        assert_eq!(std::mem::size_of::<HashMap<u8, i32>>(), 48);
        assert_eq!(std::mem::size_of::<HashMap<i64, String>>(), 48);

        assert_eq!(HashMap::from_iter([(1_u8, 2_i32)]).byte_size(), 48 + 5);
        assert_eq!(
            HashMap::from_iter([(1_u8, 2_i32), (3_u8, 4_i32)]).byte_size(),
            48 + 2 * 5
        );
    }
}
