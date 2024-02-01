use crate::key::{KeySlice, KeyVec};

use super::Block;

const NUM_LEN: usize = 2;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
        }
    }

    #[inline]
    fn current_size(&self) -> usize {
        self.data.len() + self.offsets.len() * NUM_LEN + NUM_LEN
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let added_len = NUM_LEN * 2 + key.len() + value.len() + NUM_LEN;
        if !self.is_empty() && self.current_size() + added_len > self.block_size {
            return false;
        }
        self.offsets.push(self.data.len() as u16);
        self.data
            .extend_from_slice(&(key.len() as u16).to_be_bytes());
        self.data.extend_from_slice(key.raw_ref());
        self.data
            .extend_from_slice(&(value.len() as u16).to_be_bytes());
        self.data.extend_from_slice(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
