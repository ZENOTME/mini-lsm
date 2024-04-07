#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let mut next_sst_idx = 0;
        let current = sstables
            .get(next_sst_idx)
            .map(|sst| SsTableIterator::create_and_seek_to_first(sst.clone()))
            .transpose()?;
        if current.is_some() {
            next_sst_idx += 1;
        }
        Ok(SstConcatIterator {
            current,
            next_sst_idx,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut next_sst_idx = 0;
        while sstables
            .get(next_sst_idx)
            .map(|sst| sst.last_key())
            .map_or(false, |max_key| key > max_key.as_key_slice())
        {
            next_sst_idx += 1;
        }

        let current = sstables
            .get(next_sst_idx)
            .map(|sst| SsTableIterator::create_and_seek_to_key(sst.clone(), key))
            .transpose()?;
        if current.is_some() {
            next_sst_idx += 1;
        }
        Ok(SstConcatIterator {
            current,
            next_sst_idx,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        let mut need_next_sst = false;
        if let Some(current) = &mut self.current {
            current.next()?;
            need_next_sst = !current.is_valid();
        }
        if need_next_sst {
            if self.next_sst_idx < self.sstables.len() {
                self.current = Some(SsTableIterator::create_and_seek_to_first(
                    self.sstables[self.next_sst_idx].clone(),
                )?);
                self.next_sst_idx += 1;
            } else {
                self.current = None;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
