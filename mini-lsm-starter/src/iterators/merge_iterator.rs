#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let iters = iters
            .into_iter()
            .filter(|iter| iter.is_valid())
            .enumerate()
            .map(|(idx, iter)| HeapWrapper(idx, iter));
        let iters = BinaryHeap::from_iter(iters);
        Self { iters }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.iters
            .peek()
            .expect("Only call this when it's valid.")
            .1
            .key()
    }

    fn value(&self) -> &[u8] {
        self.iters
            .peek()
            .expect("Only call this when it's valid.")
            .1
            .value()
    }

    fn is_valid(&self) -> bool {
        !self.iters.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let mut top = self.iters.pop().unwrap();
        let key = top.1.key();

        // Remove duplicate
        while let Some(item) = self.iters.peek() {
            if item.1.key() == key {
                let mut item = self.iters.pop().unwrap();
                item.1.next()?;
                if item.1.is_valid() {
                    self.iters.push(item);
                }
            } else {
                break;
            }
        }

        // Push back
        top.1.next()?;
        if top.1.is_valid() {
            self.iters.push(top);
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters.len()
    }
}
