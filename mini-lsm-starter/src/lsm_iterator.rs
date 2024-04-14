#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;

use anyhow::Result;

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    key::{KeySlice, KeyVec},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<MergeIterator<MemTableIterator>, LevelsIteratorInner>;
/// One for l0, one for others.
type LevelsIteratorInner =
    TwoMergeIterator<MergeIterator<SstConcatIterator>, MergeIterator<SstConcatIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end: Bound<KeyVec>,
    ts: u64,
}

impl LsmIterator {
    pub(crate) fn new(
        mem_iter: MergeIterator<MemTableIterator>,
        levels_iter: LevelsIteratorInner,
        lower: Bound<KeySlice>,
        upper: Bound<KeySlice>,
        ts: u64,
    ) -> Result<Self> {
        let mut iter = Self {
            inner: TwoMergeIterator::create(mem_iter, levels_iter)?,
            end: match upper {
                Bound::Included(upper) => Bound::Included(upper.to_key_vec()),
                Bound::Excluded(upper) => Bound::Excluded(upper.to_key_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
            ts,
        };

        iter.move_key()?;
        while iter.inner.is_valid() && iter.inner.value().is_empty() {
            iter.next_key()?;
            iter.move_key()?;
        }
        if let Bound::Excluded(lower) = lower {
            if iter.is_valid() && iter.inner.key().key_ref() == lower.key_ref() {
                iter.next_key()?;
            }
        }
        if let Bound::Excluded(upper) = upper {
            if iter.is_valid() && iter.inner.key().key_ref() == upper.key_ref() {
                iter.next_key()?;
            }
        }
        iter.move_key()?;
        while iter.inner.is_valid() && iter.inner.value().is_empty() {
            iter.next_key()?;
            iter.move_key()?;
        }

        Ok(iter)
    }

    fn move_key(&mut self) -> Result<()> {
        while self.inner.is_valid() && self.inner.key().ts() > self.ts {
            self.inner.next()?;
        }
        Ok(())
    }

    fn next_key(&mut self) -> Result<()> {
        let cur = self.inner.key().key_ref().to_vec();
        self.inner.next()?;
        while self.inner.is_valid() && self.inner.key().key_ref() == cur {
            self.inner.next()?;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        match &self.end {
            Bound::Included(upper) => {
                self.inner.is_valid() && self.inner.key() <= upper.as_key_slice()
            }
            Bound::Excluded(upper) => {
                self.inner.is_valid() && self.inner.key().key_ref() != upper.key_ref()
            }
            Bound::Unbounded => self.inner.is_valid(),
        }
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.next_key()?;
        self.move_key()?;
        while self.inner.is_valid() && self.inner.value().is_empty() {
            self.next_key()?;
            self.move_key()?;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    happend_err: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            happend_err: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.happend_err && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.happend_err {
            return Err(anyhow::anyhow!(
                "Iterator happend error before, should not call next again"
            ));
        }
        if !self.iter.is_valid() {
            // Do nothing
            return Ok(());
        }
        if let Err(e) = self.iter.next() {
            self.happend_err = true;
            return Err(e);
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
