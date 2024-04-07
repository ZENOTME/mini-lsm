#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;

use anyhow::Result;

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    key::KeyVec,
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
}

impl LsmIterator {
    pub(crate) fn new(
        mem_iter: MergeIterator<MemTableIterator>,
        levels_iter: LevelsIteratorInner,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<Self> {
        let mut res = Self {
            inner: TwoMergeIterator::create(mem_iter, levels_iter)?,
            end: match upper {
                Bound::Included(upper) => Bound::Included(KeyVec::from_vec(upper.to_vec())),
                Bound::Excluded(upper) => Bound::Excluded(KeyVec::from_vec(upper.to_vec())),
                Bound::Unbounded => Bound::Unbounded,
            },
        };
        res.skip_delete_value()?;
        // Skip lower bound
        if let Bound::Excluded(lower) = lower {
            if res.is_valid() && res.key() == lower {
                res.next()?;
            }
        }
        Ok(res)
    }

    fn skip_delete_value(&mut self) -> Result<()> {
        while self.inner.is_valid() && self.inner.value().is_empty() {
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
                self.inner.is_valid() && self.inner.key() < upper.as_key_slice()
            }
            Bound::Unbounded => self.inner.is_valid(),
        }
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        self.skip_delete_value()?;
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
