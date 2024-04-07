#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    pub fn print_map(&self) {
        for v in self.map.iter() {
            println!("{:?} {:?}", v.key(), v.value());
        }
    }

    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let wal = Wal::create(path)?;
        Ok(Self {
            map: Arc::new(SkipMap::new()),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let map = SkipMap::new();
        let wal = Wal::recover(path, &map)?;
        Ok(Self {
            map: Arc::new(map),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(key, value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(lower, upper)
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        Some(self.map.get(key)?.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let len = key.len() + value.len();
        let value_len = value.len();

        let key = Bytes::copy_from_slice(key);
        let value = Bytes::copy_from_slice(value);

        if let Some(ref wal) = self.wal {
            wal.put(key.as_ref(), value.as_ref())?;
        }

        let res = self.map.insert(key, value);

        // Update the approximate size.
        if res.is_removed() {
            // Process the case that the key already exists in the map.
            let update_len = value_len as i64 - res.value().len() as i64;
            match update_len {
                x if x < 0 => {
                    self.approximate_size
                        .fetch_sub(-update_len as usize, std::sync::atomic::Ordering::Relaxed);
                }
                x if x > 0 => {
                    self.approximate_size
                        .fetch_add(update_len as usize, std::sync::atomic::Ordering::Relaxed);
                }
                _ => {}
            }
        } else {
            self.approximate_size
                .fetch_add(len, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        MemTableIterator::create(self.map.clone(), lower, upper)
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        self.map.iter().for_each(|e| {
            builder.add(KeySlice::from_slice(e.key()), e.value());
        });
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl MemTableIterator {
    fn create(map: Arc<SkipMap<Bytes, Bytes>>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Self {
        let mut iterator = MemTableIteratorBuilder {
            map,
            iter_builder: |map: &Arc<SkipMap<Bytes, Bytes>>| {
                map.range((map_bound(lower), map_bound(upper)))
            },
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        iterator.with_mut(|v| {
            // Poll the first element.
            if let Some(res) = v.iter.next() {
                *v.item = (res.key().clone(), res.value().clone());
            }
        });
        iterator
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.with(|v| v.item.1.as_ref())
    }

    fn key(&self) -> KeySlice {
        self.with(|v| KeySlice::from_slice(v.item.0.as_ref()))
    }

    /// If the item is (empty, empty), then the iterator is invalid.
    fn is_valid(&self) -> bool {
        self.with(|v| (!v.item.0.is_empty()))
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|v| {
            if let Some(res) = v.iter.next() {
                *v.item = (res.key().clone(), res.value().clone());
            } else {
                *v.item = (Bytes::new(), Bytes::new());
            }
        });
        Ok(())
    }
}
