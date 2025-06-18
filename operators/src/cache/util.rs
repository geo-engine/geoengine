use geoengine_datatypes::util::ByteSize;
use snafu::ensure;

use crate::ge_tracing_removed_trace;

use super::error::CacheError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheSize {
    byte_size_total: usize,
    byte_size_used: usize,
}

impl CacheSize {
    pub fn new(byte_size_total: usize) -> Self {
        Self {
            byte_size_total,
            byte_size_used: 0,
        }
    }

    #[inline]
    pub fn total_byte_size(&self) -> usize {
        self.byte_size_total
    }

    #[inline]
    pub fn byte_size_used(&self) -> usize {
        self.byte_size_used
    }

    #[inline]
    pub fn byte_size_free(&self) -> usize {
        if self.byte_size_used > self.byte_size_total {
            0
        } else {
            self.byte_size_total - self.byte_size_used
        }
    }

    #[inline]
    pub fn size_used_fraction(&self) -> f64 {
        self.byte_size_used as f64 / self.byte_size_total as f64
    }

    #[inline]
    pub fn can_fit_bytes(&self, bytes: usize) -> bool {
        !self.is_overflowing() && self.byte_size_free() >= bytes
    }

    #[inline]
    pub fn can_fit_element_bytes<T: ByteSize>(&self, element: &T) -> bool {
        self.can_fit_bytes(element.byte_size())
    }

    #[inline]
    pub fn is_overflowing(&self) -> bool {
        self.byte_size_used > self.byte_size_total
    }

    #[inline]
    pub fn add_bytes(&mut self, bytes: usize) {
        debug_assert!(
            self.can_fit_bytes(bytes),
            "adding too many bytes {} for free capacity {}",
            bytes,
            self.byte_size_free()
        );
        self.byte_size_used += bytes;
    }

    #[inline]
    pub fn add_element_bytes<T: ByteSize>(&mut self, element: &T) {
        self.add_bytes(element.byte_size());
    }

    #[inline]
    pub fn remove_bytes(&mut self, bytes: usize) {
        debug_assert!(
            self.byte_size_used >= bytes,
            "removing more bytes {} than used {}",
            bytes,
            self.byte_size_used
        );
        self.byte_size_used -= bytes;
    }

    #[inline]
    pub fn remove_element_bytes<T: ByteSize>(&mut self, element: &T) {
        self.remove_bytes(element.byte_size());
    }

    #[inline]
    pub fn try_add_bytes(&mut self, bytes: usize) -> Result<(), CacheError> {
        ensure!(
            self.can_fit_bytes(bytes),
            crate::cache::error::NotEnoughSpaceInCache
        );
        self.add_bytes(bytes);
        Ok(())
    }

    #[inline]
    pub fn try_add_element_bytes<T: ByteSize>(&mut self, element: &T) -> Result<(), CacheError> {
        let bytes = element.byte_size();
        self.try_add_bytes(bytes)
    }

    #[inline]
    pub fn try_remove_bytes(&mut self, bytes: usize) -> Result<(), CacheError> {
        ensure!(
            self.byte_size_used >= bytes,
            crate::cache::error::NegativeSizeOfCache
        );
        self.remove_bytes(bytes);
        Ok(())
    }

    #[inline]
    pub fn try_remove_element_bytes<T: ByteSize>(&mut self, element: &T) -> Result<(), CacheError> {
        let bytes = element.byte_size();
        self.try_remove_bytes(bytes)
    }

    #[inline]
    pub fn add_bytes_allow_overflow(&mut self, bytes: usize) {
        self.byte_size_used += bytes;
        if self.is_overflowing() {
            ge_tracing_removed_trace!(
                "overflowing cache size by {} bytes, total size: {}, added bytes: {}",
                self.byte_size_used - self.byte_size_total,
                self.byte_size_total,
                bytes
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tile_cache_backend_size_init() {
        let CacheSize {
            byte_size_total,
            byte_size_used,
        } = CacheSize::new(123);
        assert_eq!(byte_size_total, 123);
        assert_eq!(byte_size_used, 0);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn tile_cache_backend_size_add_remove() {
        let mut size = CacheSize::new(100);
        size.add_bytes(10);
        assert_eq!(size.byte_size_total, 100);
        assert_eq!(size.byte_size_used, 10);
        assert_eq!(size.total_byte_size(), 100);
        assert_eq!(size.byte_size_used(), 10);
        assert_eq!(size.byte_size_free(), 100 - 10);
        assert_eq!(size.size_used_fraction(), 0.1);

        size.add_bytes(20);
        assert_eq!(size.byte_size_total, 100);
        assert_eq!(size.byte_size_used, 30);
        assert_eq!(size.total_byte_size(), 100);
        assert_eq!(size.byte_size_used(), 30);
        assert_eq!(size.byte_size_free(), 100 - 30);
        assert_eq!(size.size_used_fraction(), 0.3);

        size.remove_bytes(10);
        assert_eq!(size.byte_size_total, 100);
        assert_eq!(size.byte_size_used, 20);
        assert_eq!(size.total_byte_size(), 100);
        assert_eq!(size.byte_size_used(), 20);
        assert_eq!(size.byte_size_free(), 100 - 20);
        assert_eq!(size.size_used_fraction(), 0.2);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn tile_cache_backend_size_add_remove_overflow() {
        let mut size = CacheSize::new(100);
        size.add_bytes_allow_overflow(110);
        assert_eq!(size.byte_size_total, 100);
        assert_eq!(size.byte_size_used, 110);
        assert_eq!(size.total_byte_size(), 100);
        assert_eq!(size.byte_size_used(), 110);
        assert_eq!(size.byte_size_free(), 0);
        assert!(size.is_overflowing());

        size.remove_bytes(110);
        assert_eq!(size.byte_size_total, 100);
        assert_eq!(size.byte_size_used, 0);
        assert_eq!(size.total_byte_size(), 100);
        assert_eq!(size.byte_size_used(), 0);
        assert_eq!(size.byte_size_free(), 100);
        assert_eq!(size.size_used_fraction(), 0.0);
        assert!(!size.is_overflowing());
    }

    #[test]
    fn tile_cache_backend_element_size() {
        let tile = vec![0u8; 1001];
        let tile_size = tile.byte_size();
        let mut size = CacheSize::new(100_000);
        size.add_element_bytes(&tile);
        assert!(size.byte_size_used() == tile_size);
        assert!(size.byte_size_free() == 100_000 - tile_size);
        size.remove_element_bytes(&tile);
        assert!(size.byte_size_used() == 0);
    }

    #[test]
    fn tile_cache_backend_can_fit() {
        let tile_size = 1001;
        let mut size = CacheSize::new(2000);
        assert!(size.can_fit_bytes(tile_size));
        size.add_bytes(tile_size);
        assert!(!size.can_fit_bytes(tile_size));
        size.remove_bytes(tile_size);
        assert!(size.can_fit_bytes(tile_size));
    }
}
