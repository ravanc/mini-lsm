// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

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

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

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
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
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
    /// `compact` does the actual compaction job that merges some SST files and return a set of new SST files
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let block_size = self.options.block_size;
        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let state = self.state.read().clone();
                let l0_sst_iters = l0_sstables
                    .iter()
                    .map(|sst_id| {
                        SsTableIterator::create_and_seek_to_first(state.sstables[sst_id].clone())
                            .map(Box::new)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let l0_merge_iter = MergeIterator::create(l0_sst_iters);
                let l1_concat_iter = SstConcatIterator::create_and_seek_to_first(
                    state.levels[0]
                        .1
                        .iter()
                        .map(|sst_id| state.sstables[sst_id].clone())
                        .collect::<Vec<_>>(),
                )?;
                let mut merge_iter = TwoMergeIterator::create(l0_merge_iter, l1_concat_iter)?;
                let mut builder = SsTableBuilder::new(block_size);
                let mut output = vec![];
                while merge_iter.is_valid() {
                    if merge_iter.value() != b"" {
                        builder.add(merge_iter.key(), merge_iter.value());
                        if builder.estimated_size() > self.options.target_sst_size {
                            let sst_id = self.next_sst_id();
                            let old_builder = std::mem::replace(
                                &mut builder,
                                SsTableBuilder::new(self.options.block_size),
                            );
                            let sst = old_builder.build(
                                sst_id,
                                Some(self.block_cache.clone()),
                                self.path_of_sst(sst_id),
                            )?;
                            output.push(Arc::new(sst));
                        }
                    }
                    merge_iter.next()?;
                }

                if !builder.is_empty() {
                    let sst_id = self.next_sst_id();
                    let sst = builder.build(
                        sst_id,
                        Some(self.block_cache.clone()),
                        self.path_of_sst(sst_id),
                    )?;
                    output.push(Arc::new(sst));
                }

                Ok(output)
            }
            CompactionTask::Leveled(task) => {
                unimplemented!()
            }
            CompactionTask::Simple(task) => {
                if let Some(upper_level) = task.upper_level
                    && upper_level == 0
                {
                    self.compact(&CompactionTask::ForceFullCompaction {
                        l0_sstables: task.upper_level_sst_ids.clone(),
                        l1_sstables: task.lower_level_sst_ids.clone(),
                    })
                } else {
                    let state = self.state.read().clone();
                    let upper_concat_iter = SstConcatIterator::create_and_seek_to_first(
                        task.upper_level_sst_ids
                            .iter()
                            .map(|sst_id| state.sstables[sst_id].clone())
                            .collect::<Vec<_>>(),
                    )?;
                    let lower_concat_iter = SstConcatIterator::create_and_seek_to_first(
                        task.lower_level_sst_ids
                            .iter()
                            .map(|sst_id| state.sstables[sst_id].clone())
                            .collect::<Vec<_>>(),
                    )?;
                    let mut merge_iter =
                        TwoMergeIterator::create(upper_concat_iter, lower_concat_iter)?;
                    let mut builder = SsTableBuilder::new(self.options.block_size);
                    let mut output = vec![];
                    while merge_iter.is_valid() {
                        if merge_iter.value() != b"" {
                            builder.add(merge_iter.key(), merge_iter.value());
                            if builder.estimated_size() > self.options.target_sst_size {
                                let sst_id = self.next_sst_id();
                                let old_builder = std::mem::replace(
                                    &mut builder,
                                    SsTableBuilder::new(self.options.block_size),
                                );
                                let sst = old_builder.build(
                                    sst_id,
                                    Some(self.block_cache.clone()),
                                    self.path_of_sst(sst_id),
                                )?;
                                output.push(Arc::new(sst));
                            }
                        }
                        merge_iter.next()?;
                    }

                    if !builder.is_empty() {
                        let sst_id = self.next_sst_id();
                        let sst = builder.build(
                            sst_id,
                            Some(self.block_cache.clone()),
                            self.path_of_sst(sst_id),
                        )?;
                        output.push(Arc::new(sst));
                    }

                    Ok(output)
                }
            }
            CompactionTask::Tiered(task) => {
                unimplemented!()
            }
        }
    }

    /// `force_full_compaction` is the compaction trigger that decides which files to compact and update the LSM state
    pub fn force_full_compaction(&self) -> Result<()> {
        let state = self.state.read();
        let original_l0 = state.l0_sstables.clone();
        let original_l1 = state.levels[0].1.clone();
        let combined_sst_ids = original_l0
            .iter()
            .copied()
            .chain(original_l1.iter().copied())
            .collect::<Vec<_>>();
        let compacted_ssts = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: original_l0.clone(),
            l1_sstables: original_l1,
        })?;
        drop(state);

        let l1_sst_ids = compacted_ssts
            .iter()
            .map(|sst| sst.sst_id())
            .collect::<Vec<_>>();
        let mut write_lock = self.state.write();
        let mut state_clone = write_lock.as_ref().clone();

        let mut new_sstables = state_clone.sstables.clone();
        let mut new_l0 = state_clone.l0_sstables.clone();
        new_l0.retain(|sst_id| !original_l0.contains(sst_id));
        for sst_id in &combined_sst_ids {
            new_sstables.remove(sst_id);
        }

        for sst_arc in &compacted_ssts {
            new_sstables.insert(sst_arc.sst_id(), sst_arc.clone());
        }

        state_clone.l0_sstables = new_l0;
        state_clone.levels[0] = (1, l1_sst_ids);
        state_clone.sstables = new_sstables;

        // apparently std::mem::replace does not work here?
        *write_lock = Arc::new(state_clone);
        // *write_lock = Arc::new(new_state);
        // my original implementation was wrong because i used new_state cloned
        // at the start, which does not take into account changes in memtable
        // found this issue by asking claude the follow on question

        drop(write_lock);

        for removed_sst_id in combined_sst_ids {
            std::fs::remove_file(self.path_of_sst(removed_sst_id))?; // need this, if not the files will start bloating
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        // while let Some(task) = self
        //     .compaction_controller
        //     .generate_compaction_task(&self.state.read().clone())
        // {
        //     let sst_vec = self.compact(&task)?;
        //     let sst_ids = sst_vec.iter().map(|sst| sst.sst_id()).collect::<Vec<_>>();
        //     let mut write_lock = self.state.write();
        //     let (mut new_state, to_delete) = self.compaction_controller.apply_compaction_result(
        //         &write_lock,
        //         &task,
        //         &sst_ids[..],
        //         false,
        //     );
        //     for sst in sst_vec {
        //         new_state.sstables.insert(sst.sst_id(), sst.clone());
        //     }
        //     *write_lock = Arc::new(new_state);
        //     drop(write_lock);
        // }
        loop {
            let snapshot = self.state.read().clone();
            let Some(task) = self
                .compaction_controller
                .generate_compaction_task(&snapshot)
            else {
                break;
            };
            let sst_vec = self.compact(&task)?;
            let sst_ids = sst_vec.iter().map(|sst| sst.sst_id()).collect::<Vec<_>>();
            let state_lock = self.state_lock.lock();
            let mut write_lock = self.state.write();
            let (mut new_state, to_delete) = self.compaction_controller.apply_compaction_result(
                &write_lock,
                &task,
                &sst_ids[..],
                false,
            );
            for sst in sst_vec {
                new_state.sstables.insert(sst.sst_id(), sst.clone());
            }
            *write_lock = Arc::new(new_state);

            drop(write_lock);
            drop(state_lock);
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
        let state_clone = self.state.read().clone();
        if state_clone.imm_memtables.len() + 1 > self.options.num_memtable_limit {
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
