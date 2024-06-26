#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::cmp::max;
use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{KeySlice, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::MemTable;
use crate::mvcc::txn::{Transaction, TxnIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }

    fn max_sst_id(&self) -> Option<usize> {
        self.l0_sstables.iter().max().cloned()
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
        self.flush_thread.lock().take().map(|h| h.join());
        self.compaction_thread.lock().take().map(|h| h.join());
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.close()?;
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let mut state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let manifest_path = Self::path_of_manifest(path);
        let (manifest, records) = match std::fs::metadata(&manifest_path) {
            Err(_) => (Manifest::create(&manifest_path)?, vec![]),
            Ok(_) => Manifest::recover(&manifest_path)?,
        };

        let mut mem_ids = BTreeSet::new();
        for record in records {
            match record {
                ManifestRecord::Flush(sst_id) => {
                    mem_ids.remove(&sst_id);
                    state.l0_sstables.push(sst_id);
                }
                ManifestRecord::Compaction(task, output) => {
                    let (new_state, _del) =
                        compaction_controller.apply_compaction_result(&state, &task, &output);
                    // #TODO
                    // Delete File
                    state = new_state;
                }
                ManifestRecord::NewMemtable(mem_id) => {
                    mem_ids.insert(mem_id);
                }
            }
        }

        let mut max_ts = 0;

        // Recover SSTables
        for &id in state
            .l0_sstables
            .iter()
            .chain(state.levels.iter().flat_map(|(_, sst_ids)| sst_ids))
        {
            let sst_path = LsmStorageInner::path_of_sst_static(path, id);
            let file_object = FileObject::open(&sst_path)?;
            let sst = SsTable::open(id, None, file_object)?;
            max_ts = max(max_ts, sst.max_ts());
            state.sstables.insert(sst.sst_id(), Arc::new(sst));
        }

        let mut next_sst_id = max(
            mem_ids.last().cloned().unwrap_or_default(),
            state.max_sst_id().unwrap_or_default(),
        ) + 1;

        // Recover memtables
        if options.enable_wal {
            for &id in mem_ids.iter().rev() {
                assert!(state.sstables.get(&id).is_none());
                let memtable = MemTable::recover_from_wal(id, Self::path_of_wal_static(path, id))?;

                // Update max_ts
                let mut iter = memtable.scan(Bound::Unbounded, Bound::Unbounded);
                while iter.is_valid() {
                    max_ts = max(max_ts, iter.key().ts());
                    iter.next()?;
                }

                state.imm_memtables.push(Arc::new(memtable));
            }
            state.memtable = Arc::new(MemTable::create_with_wal(
                next_sst_id,
                Self::path_of_wal_static(path, next_sst_id),
            )?);
            manifest.add_record_when_init(ManifestRecord::NewMemtable(next_sst_id))?;
            next_sst_id += 1;
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: Some(LsmMvccInner::new(max_ts)),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        let state = self.state.read();
        state.memtable.sync_wal()?;
        for memtable in &state.imm_memtables {
            memtable.sync_wal()?;
        }
        Ok(())
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    // `Ok(None)` indicates that can't find the value.
    fn get_from_mem(&self, snapshot: &LsmStorageState, key: KeySlice) -> Result<Option<Bytes>> {
        if let Some(value) = snapshot.memtable.get(key) {
            return Ok(Some(value));
        }
        for memtable in &snapshot.imm_memtables {
            if let Some(value) = memtable.get(key) {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    pub(crate) fn get_with_ts(&self, key: &[u8], read_ts: u64) -> Result<Option<Bytes>> {
        let iter = self.scan_with_ts(Bound::Included(key), Bound::Unbounded, read_ts)?;
        if iter.is_valid() && iter.key() == key {
            Ok(Some(Bytes::copy_from_slice(iter.value())))
        } else {
            Ok(None)
        }
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self
            .mvcc
            .as_ref()
            .unwrap()
            .new_txn(self.clone(), self.options.serializable);
        txn.get(key)
    }

    pub fn write_batch_with_ts<T: AsRef<[u8]>>(
        &self,
        batch: &[WriteBatchRecord<T>],
        ts: u64,
    ) -> Result<()> {
        let put_batch = |batch: &[WriteBatchRecord<T>], ts: u64| -> Result<bool> {
            let state = self.state.read();
            for batch in batch {
                match batch {
                    WriteBatchRecord::Put(key, value) => {
                        state
                            .memtable
                            .put(KeySlice::from_slice(key.as_ref(), ts), value.as_ref())?;
                    }
                    WriteBatchRecord::Del(key) => {
                        state
                            .memtable
                            .put(KeySlice::from_slice(key.as_ref(), ts), &[])?;
                    }
                }
            }
            Ok(state.memtable.approximate_size() >= self.options.target_sst_size)
        };
        let may_need_freeze = {
            let _guard = self.mvcc.as_ref().unwrap().write_lock.lock();
            let may_need_freeze = put_batch(batch, ts)?;
            self.mvcc.as_ref().unwrap().update_commit_ts(ts);
            may_need_freeze
        };
        if may_need_freeze {
            self.try_freeze()?;
        }
        Ok(())
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(
        self: &Arc<Self>,
        batch: &[WriteBatchRecord<T>],
    ) -> Result<()> {
        let txn = self
            .mvcc
            .as_ref()
            .unwrap()
            .new_txn(self.clone(), self.options.serializable);
        for record in batch {
            match record {
                WriteBatchRecord::Put(key, value) => txn.put(key.as_ref(), value.as_ref()),
                WriteBatchRecord::Del(key) => txn.delete(key.as_ref()),
            }
        }
        txn.commit()?;
        Ok(())
    }

    fn try_freeze(&self) -> Result<()> {
        let lock = self.state_lock.lock();
        let must_freeze = {
            let state = self.state.read();
            state.memtable.approximate_size() >= self.options.target_sst_size
        };
        if must_freeze {
            self.force_freeze_memtable(&lock)?;
        }
        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(self: &Arc<Self>, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(self: &Arc<Self>, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    pub(crate) fn path_of_manifest(path: impl AsRef<Path>) -> PathBuf {
        path.as_ref().join("MANIFEST")
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    // Create a new state with the current memtable frozen
    fn new_state_for_freeze(&self) -> Arc<LsmStorageState> {
        let mut snapshot = self.state.read().as_ref().clone();

        let next_sst_id = self.next_sst_id();
        let new_mem_table = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(next_sst_id, self.path_of_wal(next_sst_id)).unwrap())
        } else {
            Arc::new(MemTable::create(next_sst_id))
        };

        let old_memtable = std::mem::replace(&mut snapshot.memtable, new_mem_table);
        snapshot.imm_memtables.insert(0, old_memtable);

        Arc::new(snapshot)
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_state = self.new_state_for_freeze();
        if self.options.enable_wal {
            if let Some(manifest) = &self.manifest {
                manifest.add_record(
                    _state_lock_observer,
                    ManifestRecord::NewMemtable(new_state.memtable.id()),
                )?;
                manifest.sync()?;
            }
        }
        *self.state.write() = new_state;
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _guard = self.state_lock.lock();

        let mut snapshot = self.state.read().as_ref().clone();

        // Create a new SSTable
        let sst_table = if let Some(memtable) = snapshot.imm_memtables.last() {
            let sst_path = self.path_of_sst(memtable.id());
            let mut sst_builder = SsTableBuilder::new(self.options.block_size);
            memtable.flush(&mut sst_builder)?;
            Arc::new(sst_builder.build(memtable.id(), Some(self.block_cache.clone()), sst_path)?)
        } else {
            return Err(anyhow::anyhow!("No imm memtable to flush"));
        };
        self.sync_dir()?;

        // Update Manifest
        if let Some(manifest) = &self.manifest {
            manifest.add_record(&_guard, ManifestRecord::Flush(sst_table.sst_id()))?;
            manifest.sync()?;
        }

        // Update snapshot
        snapshot.imm_memtables.pop();
        snapshot.l0_sstables.insert(0, sst_table.sst_id());
        snapshot.sstables.insert(sst_table.sst_id(), sst_table);

        // Update state
        let mut write_state = self.state.write();
        *write_state = Arc::new(snapshot);

        Ok(())
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        Ok(self
            .mvcc
            .as_ref()
            .unwrap()
            .new_txn(self.clone(), self.options.serializable))
    }

    pub(crate) fn create_sort_run_iter(
        snapshot: &LsmStorageState,
        sstable_id: &[usize],
        lower: Bound<KeySlice>,
    ) -> Result<SstConcatIterator> {
        let sstables = sstable_id
            .iter()
            .map(|id| {
                snapshot
                    .sstables
                    .get(id)
                    .ok_or(anyhow::anyhow!("sstable not found"))
                    .cloned()
            })
            .collect::<Result<Vec<_>>>()?;
        match lower {
            Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(sstables, key),
            Bound::Excluded(key) => {
                let mut iter = SstConcatIterator::create_and_seek_to_key(sstables, key)?;
                if iter.is_valid() && iter.key() == key {
                    iter.next()?;
                }
                Ok(iter)
            }
            Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(sstables),
        }
    }

    pub(crate) fn crate_level0_iter(
        snapshot: &LsmStorageState,
        sstable_ids: &[usize],
        lower: Bound<KeySlice>,
        upper: Bound<KeySlice>,
    ) -> Result<MergeIterator<SstConcatIterator>> {
        // Filter out sstables that overlap with the range
        let l0_sst_ids = sstable_ids
            .iter()
            .filter_map(|id| {
                let sst = snapshot.sstables.get(id).unwrap();
                if sst.overlap_range(lower.map(|v| v.key_ref()), upper.map(|v| v.key_ref())) {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        // Create SstConcatIterator for each sstable
        let sst0_sort_run_iters = l0_sst_ids
            .iter()
            .map(|&id| {
                Ok(Box::new(Self::create_sort_run_iter(
                    snapshot,
                    &[id],
                    lower,
                )?))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(MergeIterator::create(sst0_sort_run_iters))
    }

    // Create iterator for all sstables, we seperate the sstables into multiple sort run and create SstConcatIterator for each sort run.
    // Finally, we merge all SstConcatIterator into one MergeIterator.
    pub(crate) fn create_full_iter(
        &self,
        snapshot: &LsmStorageState,
        l0_sstables: &[usize],
        levels_sstable: &[(usize, Vec<usize>)],
        lower: Bound<KeySlice>,
        upper: Bound<KeySlice>,
    ) -> Result<TwoMergeIterator<MergeIterator<SstConcatIterator>, MergeIterator<SstConcatIterator>>>
    {
        let sst0_sort_run_iters = Self::crate_level0_iter(snapshot, l0_sstables, lower, upper)?;

        let other_levels_sort_run_iters = MergeIterator::create(
            levels_sstable
                .iter()
                .map(|(_, sst_ids)| {
                    Ok(Box::new(Self::create_sort_run_iter(
                        snapshot, sst_ids, lower,
                    )?))
                })
                .collect::<Result<Vec<_>>>()?,
        );

        TwoMergeIterator::create(sst0_sort_run_iters, other_levels_sort_run_iters)
    }

    pub(crate) fn scan_with_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let state = self.state.read();
            state.as_ref().clone()
        };

        let lower = lower.map(|x| KeySlice::from_slice(x, TS_RANGE_BEGIN));
        let upper = upper.map(|x| KeySlice::from_slice(x, TS_RANGE_END));

        let mem_iter = {
            let mut iters = Vec::with_capacity(1 + snapshot.imm_memtables.len());
            iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
            iters.extend(
                snapshot
                    .imm_memtables
                    .iter()
                    .map(|imm_mem| Box::new(imm_mem.scan(lower, upper)))
                    .collect::<Vec<_>>(),
            );
            MergeIterator::create(iters)
        };

        let sst_iter = self.create_full_iter(
            &snapshot,
            &snapshot.l0_sstables,
            &snapshot.levels,
            lower,
            upper,
        )?;

        let lsm_iterator = LsmIterator::new(mem_iter, sst_iter, lower, upper, read_ts)?;

        Ok(FusedIterator::new(lsm_iterator))
    }

    /// Create an iterator over a range of keys.
    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let tx = self
            .mvcc
            .as_ref()
            .unwrap()
            .new_txn(self.clone(), self.options.serializable);
        tx.scan(lower, upper)
    }

    pub fn close(&self) -> Result<()> {
        if self.options.enable_wal {
            let state = self.state.read();
            state.memtable.sync_wal()?;
            for memtable in &state.imm_memtables {
                memtable.sync_wal()?;
            }
        } else {
            // Flush all memtables to SSTables if WAL is disabled to avoid data loss
            {
                let guard = self.state_lock.lock();
                self.force_freeze_memtable(&guard)?;
            }
            while !self.state.read().imm_memtables.is_empty() {
                self.force_flush_next_imm_memtable()?;
            }
        }
        if let Some(manifest) = &self.manifest {
            manifest.close()?;
        }
        Ok(())
    }
}
