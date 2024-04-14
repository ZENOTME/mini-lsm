use std::sync::Arc;
use std::{path::Path, vec};

use anyhow::Result;

use super::{BlockMeta, FileObject, SsTable};
use crate::key::KeyBytes;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    meta_builder: BlockMetaBuilder,
    max_ts: u64,
    contain_data: bool,
}

/// Help to build block meta.
pub struct BlockMetaBuilder {
    first_key: Option<KeyBytes>,
    last_key: Option<KeyBytes>,
}

impl BlockMetaBuilder {
    pub fn new() -> Self {
        Self {
            first_key: None,
            last_key: None,
        }
    }

    pub fn update(&mut self, key: KeySlice) {
        if self.first_key.is_none() {
            self.first_key = Some(key.to_key_bytes());
        }
        self.last_key = Some(key.to_key_bytes());
    }

    pub fn build(self, offset: usize) -> BlockMeta {
        BlockMeta {
            offset,
            first_key: self.first_key.clone().unwrap_or_default(),
            last_key: self.last_key.clone().unwrap_or_default(),
        }
    }
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            meta_builder: BlockMetaBuilder::new(),
            data: Vec::with_capacity(block_size),
            meta: vec![],
            block_size,
            max_ts: 0,
            contain_data: false,
        }
    }

    fn flush_block(&mut self) {
        let old_builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = old_builder.build();

        let old_meta_builder = std::mem::replace(&mut self.meta_builder, BlockMetaBuilder::new());
        let meta = old_meta_builder.build(self.data.len());

        if block.data.is_empty() {
            return;
        }

        self.meta.push(meta);

        self.data.extend_from_slice(&block.encode());
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        self.contain_data = true;
        self.max_ts = std::cmp::max(self.max_ts, key.ts());

        if !self.builder.add(key, value) {
            self.flush_block();
            assert!(self.builder.add(key, value));
        }

        self.meta_builder.update(key);
    }

    pub fn is_empty(&self) -> bool {
        !self.contain_data
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
        data.extend_from_slice(&self.max_ts.to_be_bytes());

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
            max_ts: self.max_ts,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
