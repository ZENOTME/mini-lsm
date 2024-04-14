#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{atomic::AtomicBool, Arc},
};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
};

use super::{watermark::Watermark, CommittedTxnData};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
    pub(crate) ts: Arc<Mutex<(u64, Watermark)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(key_hashes) = &self.key_hashes {
            key_hashes.lock().1.insert(farmhash::hash32(key));
        }
        if let Some(entry) = self.local_storage.get(key) {
            if entry.value().is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(entry.value().clone()));
            }
        }
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let local_iter = TxnLocalIterator::create(self.local_storage.clone(), lower, upper);
        let lsm_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;
        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(local_iter, lsm_iter)?,
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if let Some(key_hashes) = &self.key_hashes {
            key_hashes.lock().0.insert(farmhash::hash32(key));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        if let Some(key_hashes) = &self.key_hashes {
            key_hashes.lock().0.insert(farmhash::hash32(key));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
    }

    pub fn commit(&self) -> Result<()> {
        self.committed
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let commit_guard = self.inner.mvcc.as_ref().unwrap().commit_lock.lock();
        let commit_ts = self.ts.lock().0 + 1;

        // Check
        let mut has_overlap = false;
        if let Some(key_hashes) = &self.key_hashes {
            // Skip check for empty write set.
            if key_hashes.lock().0.is_empty() {
                return Ok(());
            }

            for (&ts, other_ws) in self
                .inner
                .mvcc
                .as_ref()
                .unwrap()
                .committed_txns
                .lock()
                .iter()
            {
                if self.read_ts < ts && ts < commit_ts {
                    let (ws, rs) = &*key_hashes.lock();
                    if rs.is_disjoint(&other_ws.key_hashes) {
                        continue;
                    } else {
                        has_overlap = true;
                        break;
                    }
                }
            }
        }

        if has_overlap {
            return Err(anyhow::anyhow!(
                "serializability check failed. abort transaction."
            ));
        }

        let batches: Vec<_> = self
            .local_storage
            .iter()
            .map(|e| {
                if e.value().is_empty() {
                    WriteBatchRecord::Del(e.key().clone())
                } else {
                    WriteBatchRecord::Put(e.key().clone(), e.value().clone())
                }
            })
            .collect();
        self.inner.write_batch_with_ts(&batches, commit_ts)?;

        if let Some(key_hashes) = &self.key_hashes {
            let mut key_hashes = key_hashes.lock();
            let (write_set, _) = &mut *key_hashes;
            let mut committed_txns = self.inner.mvcc.as_ref().unwrap().committed_txns.lock();
            committed_txns.insert(
                commit_ts,
                CommittedTxnData {
                    key_hashes: std::mem::take(write_set),
                    read_ts: self.read_ts,
                    commit_ts,
                },
            );

            // Garbage collection for unneeded snapshots.
            let watermark = self.inner.mvcc.as_ref().unwrap().watermark();
            let mut to_remove = vec![];
            for (&ts, _) in committed_txns.iter() {
                if ts <= watermark {
                    to_remove.push(ts);
                }
            }
            for ts in to_remove {
                committed_txns.remove(&ts);
            }
        }

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.ts.lock().1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    fn create(
        map: Arc<SkipMap<Bytes, Bytes>>,
        lower_bound: Bound<&[u8]>,
        upper_bound: Bound<&[u8]>,
    ) -> Self {
        let mut iterator = TxnLocalIteratorBuilder {
            map,
            iter_builder: |map: &Arc<SkipMap<Bytes, Bytes>>| {
                map.range((
                    lower_bound.map(Bytes::copy_from_slice),
                    upper_bound.map(Bytes::copy_from_slice),
                ))
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

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.with(|v| v.item.1.as_ref())
    }

    fn key(&self) -> &[u8] {
        self.with(|v| v.item.0.as_ref())
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

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        mut iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        while iter.is_valid() && iter.value().is_empty() {
            txn.key_hashes
                .as_ref()
                .map(|key_hashes| key_hashes.lock().1.insert(farmhash::hash32(iter.key())));
            iter.next()?;
        }
        Ok(Self { txn, iter })
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.txn.key_hashes.as_ref().map(|key_hashes| {
            key_hashes
                .lock()
                .1
                .insert(farmhash::hash32(self.iter.key()))
        });
        self.iter.next()?;
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.txn.key_hashes.as_ref().map(|key_hashes| {
                key_hashes
                    .lock()
                    .1
                    .insert(farmhash::hash32(self.iter.key()))
            });
            self.iter.next()?;
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
