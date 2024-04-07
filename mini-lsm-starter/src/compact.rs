#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest;
use crate::table::{SsTable, SsTableBuilder};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    // Build a new sstable using sstable builder.
    #[inline]
    fn build_sst_builder(&self, builder: SsTableBuilder) -> Result<SsTable> {
        let sst_id = self.next_sst_id();
        let sst_path = self.path_of_sst(sst_id);
        let sstable = builder.build(sst_id, Some(self.block_cache.clone()), sst_path)?;
        Ok(sstable)
    }

    fn generate_sst_from_iter(
        &self,
        mut merge_iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut res_sstables = Vec::new();
        let mut builder = SsTableBuilder::new(self.options.block_size);
        while merge_iter.is_valid() {
            if merge_iter.value().is_empty() {
                merge_iter.next()?;
                continue;
            }
            builder.add(merge_iter.key(), merge_iter.value());
            if builder.estimated_size() >= self.options.target_sst_size {
                let new_sstable = self.build_sst_builder(std::mem::replace(
                    &mut builder,
                    SsTableBuilder::new(self.options.block_size),
                ))?;
                res_sstables.push(Arc::new(new_sstable));
            }
            merge_iter.next()?;
        }
        let new_sstable = self.build_sst_builder(builder)?;
        res_sstables.push(Arc::new(new_sstable));
        Ok(res_sstables)
    }

    fn compact_based_on_l0(
        &self,
        level0_sst_ids: &[usize],
        lower_level_sst_ids: &[usize],
    ) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = self.state.read().clone();
        let level0_iter = Self::crate_level0_iter(
            &snapshot,
            level0_sst_ids,
            std::ops::Bound::Unbounded,
            std::ops::Bound::Unbounded,
        )?;
        let lower_level_iter =
            Self::create_sort_run_iter(&snapshot, lower_level_sst_ids, std::ops::Bound::Unbounded)?;
        let merge_iter = TwoMergeIterator::create(level0_iter, lower_level_iter)?;

        self.generate_sst_from_iter(merge_iter)
    }

    fn compact_based_on_other(
        &self,
        upper_level: usize,
        upper_level_sst_ids: &[usize],
        lower_level: usize,
        lower_level_sst_ids: &[usize],
    ) -> Result<Vec<Arc<SsTable>>> {
        assert!(upper_level > 0);
        assert!(lower_level == upper_level + 1);
        let snapshot = self.state.read().clone();
        let upper_level_iter =
            Self::create_sort_run_iter(&snapshot, upper_level_sst_ids, std::ops::Bound::Unbounded)?;
        let lower_level_iter =
            Self::create_sort_run_iter(&snapshot, lower_level_sst_ids, std::ops::Bound::Unbounded)?;
        let merge_iter =
            MergeIterator::create(vec![Box::new(upper_level_iter), Box::new(lower_level_iter)]);

        self.generate_sst_from_iter(merge_iter)
    }

    // Compact the sstables and generate the set of new sstables.
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::Leveled(_) => todo!(),
            CompactionTask::Tiered(_) => todo!(),
            CompactionTask::Simple(task) => {
                if task.upper_level.is_none() {
                    self.compact_based_on_l0(&task.upper_level_sst_ids, &task.lower_level_sst_ids)
                } else {
                    self.compact_based_on_other(
                        task.upper_level.unwrap(),
                        &task.upper_level_sst_ids,
                        task.lower_level,
                        &task.lower_level_sst_ids,
                    )
                }
            }
            // # TODO
            // Fix it
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => self.compact_based_on_l0(l0_sstables, l1_sstables),
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sst, l1_sst) = {
            let state = self.state.read();
            // # TODO: Only choose the first one now.
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };

        if l0_sst.is_empty() {
            return Ok(());
        }

        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sst.clone(),
            l1_sstables: l1_sst,
        };
        let new_sstables = self.compact(&task)?;
        assert!(!new_sstables.is_empty());

        let _guard = self.state_lock.lock();
        let mut state = self.state.read().as_ref().clone();
        state.l0_sstables.retain(|id| !l0_sst.contains(id));
        state.levels[0] = (1, new_sstables.iter().map(|s| s.sst_id()).collect());
        state
            .sstables
            .extend(new_sstables.into_iter().map(|s| (s.sst_id(), s)));
        *self.state.write() = Arc::new(state);

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let gurad = self.state_lock.lock();
        let task = self
            .compaction_controller
            .generate_compaction_task(&self.state.read());
        if let Some(task) = task {
            let new_sstables = self.compact(&task)?;
            let output_sst_ids = new_sstables.iter().map(|s| s.sst_id()).collect::<Vec<_>>();
            let (mut new_state, _delete) = self.compaction_controller.apply_compaction_result(
                &self.state.read(),
                &task,
                &output_sst_ids,
            );
            new_state
                .sstables
                .extend(new_sstables.into_iter().map(|s| (s.sst_id(), s)));
            *self.state.write() = Arc::new(new_state);

            self.sync_dir()?;
            if let Some(manifest) = &self.manifest {
                manifest.add_record(
                    &gurad,
                    manifest::ManifestRecord::Compaction(task, output_sst_ids),
                )?;
            }
            // # TOOD:
            // Delete here
        }

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let should_flush = self.state.read().imm_memtables.len() >= self.options.num_memtable_limit;
        if should_flush {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
