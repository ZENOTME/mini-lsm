use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    // fn find_next_level_overlap(
    //     snapshot: &LsmStorageState,
    //     upper_level: usize,
    //     upper_level_sst_ids: &[usize],
    // ) -> Vec<usize> {
    //     let min_key = upper_level_sst_ids
    //         .iter()
    //         .map(|id| snapshot.sstables.get(id).unwrap().first_key())
    //         .min()
    //         .unwrap();
    //     let max_key = upper_level_sst_ids
    //         .iter()
    //         .map(|id| snapshot.sstables.get(id).unwrap().last_key())
    //         .max()
    //         .unwrap();

    //     // index is upper_levle - 1, so to get the next level here we can use upper_level directly
    //     let next_level_ssts = snapshot
    //         .levels
    //         .get(upper_level)
    //         .unwrap()
    //         .1
    //         .iter()
    //         .filter(|id| {
    //             let sst = snapshot.sstables.get(id).unwrap();
    //             sst.overlap_range(
    //                 std::ops::Bound::Included(min_key.as_key_slice().into_inner()),
    //                 std::ops::Bound::Included(max_key.as_key_slice().into_inner()),
    //             )
    //         })
    //         .cloned()
    //         .collect::<Vec<_>>();

    //     next_level_ssts
    // }

    fn minor_compact(snapshot: &LsmStorageState) -> SimpleLeveledCompactionTask {
        SimpleLeveledCompactionTask {
            upper_level: None,
            upper_level_sst_ids: snapshot.l0_sstables.clone(),
            lower_level: 1,
            lower_level_sst_ids: snapshot.levels.first().unwrap().1.clone(),
            is_lower_level_bottom_level: false,
        }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(Self::minor_compact(snapshot));
        }
        let mut upper_level = 1;
        let mut lower_level = 2;
        while lower_level <= self.options.max_levels {
            if !snapshot.levels.get(upper_level - 1).unwrap().1.is_empty() {
                let radio = snapshot.levels.get(lower_level - 1).unwrap().1.len()
                    / snapshot.levels.get(upper_level - 1).unwrap().1.len()
                    * 100;
                if radio < self.options.size_ratio_percent {
                    return Some(SimpleLeveledCompactionTask {
                        upper_level: Some(upper_level),
                        upper_level_sst_ids: snapshot
                            .levels
                            .get(upper_level - 1)
                            .unwrap()
                            .1
                            .clone(),
                        lower_level,
                        lower_level_sst_ids: snapshot
                            .levels
                            .get(lower_level - 1)
                            .unwrap()
                            .1
                            .clone(),
                        is_lower_level_bottom_level: lower_level == self.options.max_levels,
                    });
                }
            }
            upper_level += 1;
            lower_level += 1;
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = snapshot.clone();
        if let Some(upper_level_id) = task.upper_level {
            new_state
                .levels
                .get_mut(upper_level_id - 1)
                .unwrap()
                .1
                .retain(|id| !task.upper_level_sst_ids.contains(id));
        } else {
            new_state
                .l0_sstables
                .retain(|id| !task.upper_level_sst_ids.contains(id));
        }
        new_state
            .levels
            .get_mut(task.lower_level - 1)
            .unwrap()
            .1
            .retain(|id| !task.lower_level_sst_ids.contains(id));
        new_state
            .levels
            .get_mut(task.lower_level - 1)
            .unwrap()
            .1
            .extend(output.iter().cloned());
        let del_sst = task
            .upper_level_sst_ids
            .iter()
            .chain(task.lower_level_sst_ids.iter())
            .cloned()
            .collect::<Vec<_>>();
        del_sst.iter().for_each(|sst_id| {
            let _ = new_state.sstables.remove(sst_id);
        });
        (new_state, del_sst)
    }
}
