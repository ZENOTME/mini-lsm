#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the value range from the block
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    fn entry_from_index(block: &Block, index: usize) -> (KeySlice, (usize, usize)) {
        assert!(index < block.offsets.len());
        let offset = block.offsets[index] as usize;
        let key_len = i16::from_be_bytes([block.data[offset], block.data[offset + 1]]);
        let key = &block.data[offset + 2..offset + 2 + key_len as usize];
        let value_len = i16::from_be_bytes([
            block.data[offset + 2 + key_len as usize],
            block.data[offset + 2 + key_len as usize + 1],
        ]);
        let value_start = offset + 2 + key_len as usize + 2;
        let value_end = value_start + value_len as usize;
        (KeySlice::from_slice(key), (value_start, value_end))
    }

    fn set_state_index(&mut self, index: usize) {
        assert!(index < self.block.offsets.len());
        let (key, value_range) = Self::entry_from_index(&self.block, index);
        self.key.set_from_slice(key);
        self.value_range = value_range;
        self.idx = index;
    }

    fn set_state_invalid(&mut self) {
        self.key.clear();
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        if iter.block.offsets.is_empty() {
            return iter;
        }
        iter.set_state_index(0);
        iter.first_key = iter.key.clone();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        if iter.block.offsets.is_empty() {
            return iter;
        }
        let (first_key, _) = Self::entry_from_index(&iter.block, 0);
        iter.first_key.set_from_slice(first_key);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        if self.is_valid() {
            &self.block.data[self.value_range.0..self.value_range.1]
        } else {
            &[]
        }
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        if self.block.offsets.is_empty() {
            return;
        }
        self.set_state_index(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx + 1 >= self.block.offsets.len() {
            self.set_state_invalid();
            return;
        }
        self.set_state_index(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut idx = 0;
        while idx < self.block.offsets.len() {
            let (cur_key, _) = Self::entry_from_index(&self.block, idx);
            if cur_key >= key {
                break;
            }
            idx += 1;
        }
        if idx >= self.block.offsets.len() {
            self.set_state_invalid();
            return;
        }
        self.set_state_index(idx);
    }
}
