#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::Buf;
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        for meta in block_meta {
            buf.extend_from_slice(&meta.offset.to_be_bytes());
            meta.first_key.encode(buf);
            meta.last_key.encode(buf);
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let mut res = vec![];
        let mut buf = buf;
        while buf.has_remaining() {
            let offset = buf.get_u64() as usize;
            let first_key = KeyBytes::decode(&mut buf);
            let last_key = KeyBytes::decode(&mut buf);
            res.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        res
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        // sst format:
        // | data | block meta | block meta offset | max_ts |
        let file_len = file.size();

        let mut offset = file_len;
        let max_ts = {
            offset -= 8;
            let buf = file.read(offset, 8)?;
            u64::from_be_bytes([
                buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
            ])
        };
        let block_meta_offset = {
            offset -= 4;
            let buf = file.read(offset, 4)?;
            u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]])
        };
        let meta = {
            let meta_buf_len = file_len - block_meta_offset as u64 - 4 - 8;
            let buf = file.read(block_meta_offset as u64, meta_buf_len)?;
            BlockMeta::decode_block_meta(buf.as_slice())
        };

        let first_key_in_sst = meta
            .first()
            .map(|x| x.first_key.clone())
            .unwrap_or_default();
        let last_key_in_sst = meta.last().map(|x| x.last_key.clone()).unwrap_or_default();
        Ok(SsTable {
            file,
            block_meta: meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            first_key: first_key_in_sst,
            last_key: last_key_in_sst,
            bloom: None,
            max_ts,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx >= self.block_meta.len() {
            return Err(anyhow::anyhow!("Block index out of range"));
        }
        let start_offset = self.block_meta[block_idx].offset as u64;
        let end_offset = self
            .block_meta
            .get(block_idx + 1)
            .map(|meta| meta.offset)
            .unwrap_or(self.block_meta_offset) as u64;
        let len: u64 = end_offset - start_offset;

        let block_buf = self.file.read(start_offset, len)?;
        Ok(Arc::new(Block::decode(&block_buf)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(cache) = &self.block_cache {
            if let Some(block) = cache.get(&(self.id, block_idx)) {
                return Ok(block);
            }
        }
        let block = self.read_block(block_idx)?;
        if let Some(cache) = &self.block_cache {
            cache.insert((self.id, block_idx), block.clone());
        }
        Ok(block)
    }

    /// Find the block that may contain `key`.
    ///
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }

    // Check whether there is an overlap between the range of this SSTable and the given range.
    //
    // Check using the reverse condition, there are 2 cases that there is no overlap:
    // --1.[firstkey,lastkey]-- lower ------------ upper --2.[firstkey,lastkey]--
    pub fn overlap_range(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
        let out_of_range_left = match lower {
            Bound::Included(l) => l > self.last_key.as_key_slice().key_ref(),
            Bound::Excluded(l) => l >= self.last_key.as_key_slice().key_ref(),
            Bound::Unbounded => false,
        };
        let out_of_range_right = match upper {
            Bound::Included(u) => u < self.first_key.as_key_slice().key_ref(),
            Bound::Excluded(u) => u <= self.first_key.as_key_slice().key_ref(),
            Bound::Unbounded => false,
        };
        !(out_of_range_left || out_of_range_right)
    }
}
