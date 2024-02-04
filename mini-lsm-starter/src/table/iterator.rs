#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{cmp::min, sync::Arc};

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    fn get_block_iter_by_index(
        table: &SsTable,
        index: usize,
        key: Option<KeySlice>,
    ) -> Result<BlockIterator> {
        let block = table.read_block_cached(index)?;

        if let Some(key) = key {
            Ok(BlockIterator::create_and_seek_to_key(block, key))
        } else {
            Ok(BlockIterator::create_and_seek_to_first(block))
        }
    }

    fn find_block_index(table: &SsTable, key: KeySlice) -> usize {
        let possible_index = table.find_block_idx(key);
        if key <= table.block_meta[possible_index].last_key.as_key_slice() {
            possible_index
        } else {
            min(possible_index + 1, table.block_meta.len() - 1)
        }
    }

    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let blk_iter = Self::get_block_iter_by_index(&table, 0, None)?;
        Ok(Self {
            table,
            blk_iter,
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let blk_iter = Self::get_block_iter_by_index(&self.table, 0, None)?;
        self.blk_iter = blk_iter;
        self.blk_idx = 0;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let index = Self::find_block_index(&table, key);
        let blk_iter = Self::get_block_iter_by_index(&table, index, Some(key)).expect(
            "
        Should be able to seek to the block for valid index",
        );
        Ok(Self {
            table,
            blk_iter,
            blk_idx: index,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let index = Self::find_block_index(&self.table, key);
        let blk_iter = Self::get_block_iter_by_index(&self.table, index, Some(key)).expect(
            "
        Should be able to seek to the block for valid index",
        );
        self.blk_iter = blk_iter;
        self.blk_idx = index;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx < self.table.block_meta.len() {
                self.blk_iter = Self::get_block_iter_by_index(&self.table, self.blk_idx, None)
                    .expect("Should be able to seek to the block for valid index");
            }
        }
        Ok(())
    }
}
