use std::sync::Arc;
use std::{path::Path, vec};

use anyhow::Result;
use bytes::Bytes;

use super::{BlockMeta, FileObject, SsTable};
use crate::key::KeyBytes;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: vec![],
            last_key: vec![],
            data: Vec::with_capacity(block_size),
            meta: vec![],
            block_size,
        }
    }

    fn flush_block(&mut self) {
        let old_builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = old_builder.build();

        let block_first_key = Bytes::from(std::mem::take(&mut self.first_key));
        let block_last_key = Bytes::from(std::mem::take(&mut self.last_key));
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(block_first_key),
            last_key: KeyBytes::from_bytes(block_last_key),
        });

        self.data.extend_from_slice(&block.encode());
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec().into_inner();
        }
        // Flush current block
        if !self.builder.add(key, value) {
            self.flush_block();
            assert!(self.builder.add(key, value));
        }

        // Record key
        let key_vec = key.to_key_vec().into_inner();
        if self.first_key.is_empty() {
            self.first_key = key_vec.clone();
        }
        self.last_key = key_vec;
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.flush_block();

        // Encode data
        let meta_block_offset = self.data.len() as u32;
        let mut data = self.data;
        BlockMeta::encode_block_meta(&self.meta, &mut data);
        data.extend_from_slice(&meta_block_offset.to_be_bytes());

        let file = FileObject::create(path.as_ref(), data)?;

        let first_key_in_sst = self
            .meta
            .first()
            .map(|x| x.first_key.clone())
            .unwrap_or_default();
        let last_key_in_sst = self
            .meta
            .last()
            .map(|x| x.last_key.clone())
            .unwrap_or_default();

        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: meta_block_offset as usize,
            id,
            block_cache,
            first_key: first_key_in_sst,
            last_key: last_key_in_sst,
            bloom: None,
            // # TODO
            // FIXME: Add max_ts field to SsTable
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
