use std::{fmt::Debug, hash::Hasher};

use num_traits::AsPrimitive;
use rustc_hash::FxHasher;

/// A hash map implementation that uses separate chaining for collision detection.
/// This allows stable iteration over the keys independent of the order of insertion.
#[derive(Debug, Clone)]
pub struct SeparateChainingHashMap<K, V>
where
    K: Copy + Debug,
    V: Clone + Debug,
{
    table: Vec<Bucket<K, V>>,
    bucket_mask: usize,
    entries_left: usize,
}

/// A bucket in the hash map.
/// A filled bucket is most of the time a single element,
/// but may contain more elements in the case of collisions.
#[derive(Debug, Clone)]
pub enum Bucket<K, V>
where
    K: Copy + Debug,
    V: Clone + Debug,
{
    Empty,
    Single(Entry<K, V>),
    Multi(Vec<Entry<K, V>>),
}

impl<K, V> Default for Bucket<K, V>
where
    K: Copy + Debug,
    V: Clone + Debug,
{
    fn default() -> Self {
        Bucket::Empty
    }
}

impl<K, V> Bucket<K, V>
where
    K: Copy + Debug,
    V: Clone + Debug,
{
    fn insert_entry(&mut self, entry: Entry<K, V>) {
        match self {
            Bucket::Empty => *self = Bucket::Single(entry),
            Bucket::Single(_) => {
                if let Bucket::Single(old_entry) = std::mem::replace(self, Bucket::Empty) {
                    *self = Bucket::Multi(vec![old_entry, entry]);
                }
            }
            Bucket::Multi(entries) => entries.push(entry),
        };
    }
}

/// A hash map entry that stores its key.
#[derive(Debug, Clone)]
pub struct Entry<K, V>
// where
//     K: PartialOrd + PartialEq,
{
    key: K,
    value: V,
}

/// Equality by key
impl<K, V> PartialEq for Entry<K, V>
where
    K: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<K, V> Eq for Entry<K, V> where K: Eq {}

/// Ordering by key
impl<K, V> PartialOrd for Entry<K, V>
where
    K: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

/// Ordering by key
impl<K, V> Ord for Entry<K, V>
where
    K: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

#[derive(Debug)]
pub enum ValueRef<'e, K, V> {
    Vacant(VacantEntryPos<K>),
    Occupied(&'e mut V),
}

#[derive(Debug, Clone, Copy)]
pub struct VacantEntryPos<K> {
    hash: usize,
    bucket_index: usize,
    key: K,
}

impl<K, V> SeparateChainingHashMap<K, V>
where
    K: Copy + Debug + AsPrimitive<usize> + Eq + Ord,
    V: Clone + Debug,
{
    pub fn new() -> Self {
        Self {
            table: vec![Bucket::default(); 2],
            bucket_mask: 1,
            entries_left: Self::bucket_mask_to_capacity(1),
        }
    }

    pub fn entry(&mut self, key: K) -> ValueRef<'_, K, V> {
        let hash = Self::hash(key);

        let bucket_index = hash & self.bucket_mask;

        for entry in &mut self.table[bucket_index] {
            if entry.key == key {
                return ValueRef::Occupied(&mut entry.value);
            }
        }

        ValueRef::Vacant(VacantEntryPos {
            hash,
            bucket_index,
            key,
        })
    }

    pub fn insert_unchecked(&mut self, entry_pos: VacantEntryPos<K>, value: V) {
        let VacantEntryPos {
            hash,
            mut bucket_index,
            key,
        } = entry_pos;

        if self.entries_left == 0 {
            // would insert, so resize
            self.resize();

            // update bucket_index due to new hash table size
            bucket_index = hash & self.bucket_mask;
        }

        self.table[bucket_index].insert_entry(Entry { key, value });

        self.entries_left -= 1;
    }

    fn resize(&mut self) {
        let table_size = 2 * self.table.len();

        let old_table = std::mem::replace(&mut self.table, vec![Bucket::default(); table_size]);

        self.bucket_mask = table_size - 1;
        self.entries_left = Self::bucket_mask_to_capacity(self.bucket_mask);

        // re-insert all entries into larger table
        for entry in old_table.into_iter().flat_map(Bucket::into_iter) {
            let bucket_index = Self::hash(entry.key) & self.bucket_mask;
            self.table[bucket_index].insert_entry(entry);
            self.entries_left -= 1;
        }
    }

    #[inline]
    fn hash(key: K) -> usize {
        let mut hasher = FxHasher::default();
        hasher.write_usize(key.as_());
        hasher.finish() as usize
    }

    #[inline]
    fn bucket_mask_to_capacity(bucket_mask: usize) -> usize {
        if bucket_mask < 8 {
            // For tables with 1/2/4/8 buckets, we always reserve one empty slot.
            // Keep in mind that the bucket mask is one less than the bucket count.
            bucket_mask
        } else {
            // For larger tables we reserve 12.5% of the slots as empty.
            ((bucket_mask + 1) / 8) * 7
        }
    }

    #[allow(unused)]
    pub fn capacity(&self) -> usize {
        self.table.len()
    }
}

type BucketVecIter<K, V> = std::vec::IntoIter<Bucket<K, V>>;
type BucketEntriesIter<K, V> = std::iter::FlatMap<
    BucketVecIter<K, V>,
    BucketIterator<K, V>,
    fn(Bucket<K, V>) -> BucketIterator<K, V>,
>;
type BucketEntryValuesIter<K, V> = std::iter::Map<BucketEntriesIter<K, V>, fn(Entry<K, V>) -> V>;

impl<K, V> IntoIterator for SeparateChainingHashMap<K, V>
where
    K: Copy + Debug + Ord,
    V: Clone + Debug,
{
    type Item = V;

    type IntoIter = BucketEntryValuesIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        let buckets: BucketVecIter<K, V> = self.table.into_iter();
        let entries: BucketEntriesIter<K, V> = buckets.flat_map(Bucket::into_iter);

        entries.map(Entry::value)
    }
}

impl<K, V> Entry<K, V> {
    fn value(self) -> V {
        self.value
    }
}

pub enum BucketIterator<K, V> {
    Empty,
    Single(std::iter::Once<Entry<K, V>>),
    Multi(std::vec::IntoIter<Entry<K, V>>),
}

impl<K, V> Iterator for BucketIterator<K, V> {
    type Item = Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BucketIterator::Empty => None,
            BucketIterator::Single(iter) => iter.next(),
            BucketIterator::Multi(iter) => iter.next(),
        }
    }
}

impl<K, V> IntoIterator for Bucket<K, V>
where
    K: Copy + Debug + Ord,
    V: Clone + Debug,
{
    type Item = Entry<K, V>;

    type IntoIter = BucketIterator<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Bucket::Empty => BucketIterator::Empty,
            Bucket::Single(e) => BucketIterator::Single(std::iter::once(e)),
            Bucket::Multi(mut es) => {
                es.sort_unstable();
                BucketIterator::Multi(es.into_iter())
            }
        }
    }
}

pub enum BucketRefMutIterator<'e, K, V> {
    Empty,
    Single(std::iter::Once<&'e mut Entry<K, V>>),
    Multi(std::slice::IterMut<'e, Entry<K, V>>),
}

impl<'e, K, V> Iterator for BucketRefMutIterator<'e, K, V> {
    type Item = &'e mut Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BucketRefMutIterator::Empty => None,
            BucketRefMutIterator::Single(iter) => iter.next(),
            BucketRefMutIterator::Multi(iter) => iter.next(),
        }
    }
}

impl<'e, K, V> IntoIterator for &'e mut Bucket<K, V>
where
    K: Copy + Debug,
    V: Clone + Debug,
{
    type Item = &'e mut Entry<K, V>;

    type IntoIter = BucketRefMutIterator<'e, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Bucket::Empty => BucketRefMutIterator::Empty,
            Bucket::Single(e) => BucketRefMutIterator::Single(std::iter::once(e)),
            Bucket::Multi(es) => BucketRefMutIterator::Multi(es.iter_mut()),
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::prelude::SliceRandom;
    use rand::SeedableRng;

    use super::*;

    fn insert<K, V>(map: &mut SeparateChainingHashMap<K, V>, key: K, value: V)
    where
        K: Copy + Debug + Ord + AsPrimitive<usize>,
        V: Clone + Debug,
    {
        match map.entry(key) {
            ValueRef::Occupied(_) => panic!("key already exists: {:?}", key),
            ValueRef::Vacant(entry_pos) => {
                map.insert_unchecked(entry_pos, value);
            }
        }
    }

    #[test]
    fn capacity() {
        let mut map = SeparateChainingHashMap::<u32, u32>::new();

        let mut capacities = Vec::new();
        for i in 0..5 {
            insert(&mut map, i, i);
            capacities.push(map.capacity());
        }

        assert_eq!(capacities, vec![2, 4, 4, 8, 8]);

        for i in 5..113 {
            insert(&mut map, i, i);
        }

        assert_eq!(map.capacity(), 256);

        for i in 113..128 {
            insert(&mut map, i, i);
        }

        assert_eq!(map.capacity(), 256);
    }

    #[test]
    fn insert_and_retrieve() {
        let mut map = SeparateChainingHashMap::<u32, u32>::new();

        let mut result = Vec::new();
        for i in 0..128 {
            insert(&mut map, i, i);
            result.push(i);
        }

        let mut all_values = map.into_iter().collect::<Vec<_>>();
        all_values.sort_unstable();
        assert_eq!(all_values, result);
    }

    #[test]
    fn insert_and_lookup() {
        let mut map = SeparateChainingHashMap::<u32, u32>::new();

        for i in 0..128 {
            insert(&mut map, i, i);

            match map.entry(i) {
                ValueRef::Vacant(_) => panic!("key not found: {}", i),
                ValueRef::Occupied(_) => (), // okay
            };
        }
    }

    #[test]
    fn insertion_order() {
        let values: Vec<u16> = (0..256).collect();
        let shuffled_values = {
            let mut values = values.clone();
            let mut rng = rand::rngs::StdRng::seed_from_u64(42);
            values.shuffle(&mut rng);
            values
        };

        let mut map1 = SeparateChainingHashMap::<u16, u16>::new();
        for v in values {
            insert(&mut map1, v, v);
        }

        let mut map2 = SeparateChainingHashMap::<u16, u16>::new();
        for v in shuffled_values {
            insert(&mut map2, v, v);
        }

        assert_eq!(
            map1.into_iter().collect::<Vec<_>>(),
            map2.into_iter().collect::<Vec<_>>()
        );
    }
}
