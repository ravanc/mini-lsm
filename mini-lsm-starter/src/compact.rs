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
use crate::key::KeySlice;
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
    /// Drains a merged iterator into a list of newly-built SSTs, splitting on
    /// `target_sst_size`. When `compact_to_bottom` is true, deleted entries
    /// (empty values / tombstones) are dropped, since no older version can live
    /// below the bottom level.
    fn build_ssts_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = SsTableBuilder::new(self.options.block_size);
        let mut output = vec![];
        while iter.is_valid() {
            if !compact_to_bottom || !iter.value().is_empty() {
                builder.add(iter.key(), iter.value());
                if builder.estimated_size() > self.options.target_sst_size {
                    let sst_id = self.next_sst_id();
                    let old_builder = std::mem::replace(
                        &mut builder,
                        SsTableBuilder::new(self.options.block_size),
                    );
                    output.push(Arc::new(old_builder.build(
                        sst_id,
                        Some(self.block_cache.clone()),
                        self.path_of_sst(sst_id),
                    )?));
                }
            }
            iter.next()?;
        }
        if !builder.is_empty() {
            let sst_id = self.next_sst_id();
            output.push(Arc::new(builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?));
        }
        Ok(output)
    }

    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let state = self.state.read().clone();
        // Build a `SstConcatIterator` over a sorted, non-overlapping run of ssts.
        let concat = |sst_ids: &[usize]| {
            SstConcatIterator::create_and_seek_to_first(
                sst_ids
                    .iter()
                    .map(|sst_id| state.sstables[sst_id].clone())
                    .collect::<Vec<_>>(),
            )
        };
        // Build a `MergeIterator` over (possibly overlapping) ssts, e.g. L0.
        let merge = |sst_ids: &[usize]| -> Result<MergeIterator<SsTableIterator>> {
            let iters = sst_ids
                .iter()
                .map(|sst_id| {
                    SsTableIterator::create_and_seek_to_first(state.sstables[sst_id].clone())
                        .map(Box::new)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(MergeIterator::create(iters))
        };

        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let iter = TwoMergeIterator::create(merge(l0_sstables)?, concat(l1_sstables)?)?;
                self.build_ssts_from_iter(iter, true)
            }
            CompactionTask::Leveled(task) => {
                let lower = concat(&task.lower_level_sst_ids)?;
                // L0 ssts (upper_level == None) overlap each other, so they need a
                // MergeIterator; a real level is a sorted run, so a ConcatIterator works.
                if task.upper_level.is_none() {
                    let iter = TwoMergeIterator::create(merge(&task.upper_level_sst_ids)?, lower)?;
                    self.build_ssts_from_iter(iter, task.is_lower_level_bottom_level)
                } else {
                    let iter = TwoMergeIterator::create(concat(&task.upper_level_sst_ids)?, lower)?;
                    self.build_ssts_from_iter(iter, task.is_lower_level_bottom_level)
                }
            }
            CompactionTask::Simple(task) => {
                // Simple encodes L0 as `Some(0)`. L0 overlaps; otherwise a sorted level.
                let lower = concat(&task.lower_level_sst_ids)?;
                if task.upper_level == Some(0) {
                    let iter = TwoMergeIterator::create(merge(&task.upper_level_sst_ids)?, lower)?;
                    self.build_ssts_from_iter(iter, true)
                } else {
                    let iter = TwoMergeIterator::create(concat(&task.upper_level_sst_ids)?, lower)?;
                    self.build_ssts_from_iter(iter, true)
                }
            }
            CompactionTask::Tiered(task) => {
                let iters = task
                    .tiers
                    .iter()
                    .map(|tier| concat(&tier.1).map(Box::new))
                    .collect::<Result<Vec<_>>>()?;
                let iter = MergeIterator::create(iters);
                self.build_ssts_from_iter(iter, task.bottom_tier_included)
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
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: original_l0.clone(),
            l1_sstables: original_l1,
        };
        let compacted_ssts = self.compact(&task)?;
        drop(state);

        let l1_sst_ids = compacted_ssts
            .iter()
            .map(|sst| sst.sst_id())
            .collect::<Vec<_>>();
        let state_lock_observer = self.state_lock.lock();
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
        state_clone.levels[0] = (1, l1_sst_ids.clone());
        state_clone.sstables = new_sstables;

        // apparently std::mem::replace does not work here?
        *write_lock = Arc::new(state_clone);
        // *write_lock = Arc::new(new_state);
        // my original implementation was wrong because i used new_state cloned
        // at the start, which does not take into account changes in memtable
        // found this issue by asking claude the follow on question
        if let Some(manifest) = &self.manifest {
            manifest.add_record(
                &state_lock_observer,
                crate::manifest::ManifestRecord::Compaction(task, l1_sst_ids),
            )?;
        }

        drop(write_lock);
        drop(state_lock_observer);

        for removed_sst_id in combined_sst_ids {
            std::fs::remove_file(self.path_of_sst(removed_sst_id))?; // need this, if not the files will start bloating
        }

        self.sync_dir()?;

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

            // in order to allow leveled compaction to have a reference on where to insert the compacted
            // ssts, we need to get the sst into snapshot.sstables first
            let mut snapshot = write_lock.as_ref().clone();

            for sst in sst_vec {
                snapshot.sstables.insert(sst.sst_id(), sst.clone());
            }

            let (new_state, to_delete) = self.compaction_controller.apply_compaction_result(
                &snapshot,
                &task,
                &sst_ids[..],
                false,
            );
            // for sst in sst_vec {
            //     new_state.sstables.insert(sst.sst_id(), sst.clone());
            // }
            *write_lock = Arc::new(new_state);

            if let Some(manifest) = &self.manifest {
                manifest.add_record(
                    &state_lock,
                    crate::manifest::ManifestRecord::Compaction(task, sst_ids),
                )?;
            }

            drop(write_lock);
            drop(state_lock);

            for removed_sst_id in to_delete {
                std::fs::remove_file(self.path_of_sst(removed_sst_id))?;
            }

            self.sync_dir()?;
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

// ============================================================================
// ARCHIVE: original hand-written `compact` function (pre generic-helper refactor)
// Kept for reference. Superseded by `build_ssts_from_iter` + `compact` above.
// ============================================================================
//     fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
//         let block_size = self.options.block_size;
//         match _task {
//             CompactionTask::ForceFullCompaction {
//                 l0_sstables,
//                 l1_sstables,
//             } => {
//                 let state = self.state.read().clone();
//                 let l0_sst_iters = l0_sstables
//                     .iter()
//                     .map(|sst_id| {
//                         SsTableIterator::create_and_seek_to_first(state.sstables[sst_id].clone())
//                             .map(Box::new)
//                     })
//                     .collect::<Result<Vec<_>>>()?;
//                 let l0_merge_iter = MergeIterator::create(l0_sst_iters);
//                 let l1_concat_iter = SstConcatIterator::create_and_seek_to_first(
//                     state.levels[0]
//                         .1
//                         .iter()
//                         .map(|sst_id| state.sstables[sst_id].clone())
//                         .collect::<Vec<_>>(),
//                 )?;
//                 let mut merge_iter = TwoMergeIterator::create(l0_merge_iter, l1_concat_iter)?;
//                 let mut builder = SsTableBuilder::new(block_size);
//                 let mut output = vec![];
//                 while merge_iter.is_valid() {
//                     if merge_iter.value() != b"" {
//                         builder.add(merge_iter.key(), merge_iter.value());
//                         if builder.estimated_size() > self.options.target_sst_size {
//                             let sst_id = self.next_sst_id();
//                             let old_builder = std::mem::replace(
//                                 &mut builder,
//                                 SsTableBuilder::new(self.options.block_size),
//                             );
//                             let sst = old_builder.build(
//                                 sst_id,
//                                 Some(self.block_cache.clone()),
//                                 self.path_of_sst(sst_id),
//                             )?;
//                             output.push(Arc::new(sst));
//                         }
//                     }
//                     merge_iter.next()?;
//                 }
//
//                 if !builder.is_empty() {
//                     let sst_id = self.next_sst_id();
//                     let sst = builder.build(
//                         sst_id,
//                         Some(self.block_cache.clone()),
//                         self.path_of_sst(sst_id),
//                     )?;
//                     output.push(Arc::new(sst));
//                 }
//
//                 Ok(output)
//             }
//             CompactionTask::Leveled(task) => {
//                 let state = self.state.read().clone();
//                 if task.upper_level.is_none() {
//                     let upper_iter = MergeIterator::create(task.upper_level_sst_ids
//                             .iter()
//                             .map(|sst_id| SsTableIterator::create_and_seek_to_first(state.sstables[sst_id].clone()).map(Box::new))
//                             .collect::<Result<Vec<_>>>()?
//                         );
//                 } else {
//                     let upper_iter = SstConcatIterator::create_and_seek_to_first(
//                         task.upper_level_sst_ids
//                             .iter()
//                             .map(|sst_id| state.sstables[sst_id].clone())
//                             .collect::<Vec<_>>(),
//                     )?;
//                 }
//                 let lower_concat_iter = SstConcatIterator::create_and_seek_to_first(
//                     task.lower_level_sst_ids
//                         .iter()
//                         .map(|sst_id| state.sstables[sst_id].clone())
//                         .collect::<Vec<_>>(),
//                 )?;
//                 let mut merge_iter =
//                     TwoMergeIterator::create(upper_iter, lower_concat_iter)?;
//                 let mut builder = SsTableBuilder::new(self.options.block_size);
//                 let mut output = vec![];
//                 while merge_iter.is_valid() {
//                     if merge_iter.value() != b"" || !task.is_lower_level_bottom_level {
//                         builder.add(merge_iter.key(), merge_iter.value());
//                         if builder.estimated_size() > self.options.target_sst_size {
//                             let sst_id = self.next_sst_id();
//                             let old_builder = std::mem::replace(
//                                 &mut builder,
//                                 SsTableBuilder::new(self.options.block_size),
//                             );
//                             let sst = old_builder.build(
//                                 sst_id,
//                                 Some(self.block_cache.clone()),
//                                 self.path_of_sst(sst_id),
//                             )?;
//                             output.push(Arc::new(sst));
//                         }
//                     }
//                     merge_iter.next()?;
//                 }
//
//                 if !builder.is_empty() {
//                     let sst_id = self.next_sst_id();
//                     let sst = builder.build(
//                         sst_id,
//                         Some(self.block_cache.clone()),
//                         self.path_of_sst(sst_id),
//                     )?;
//                     output.push(Arc::new(sst));
//                 }
//
//                 Ok(output)
//             }
//             CompactionTask::Simple(task) => {
//                 if let Some(upper_level) = task.upper_level
//                     && upper_level == 0
//                 {
//                     self.compact(&CompactionTask::ForceFullCompaction {
//                         l0_sstables: task.upper_level_sst_ids.clone(),
//                         l1_sstables: task.lower_level_sst_ids.clone(),
//                     })
//                 } else {
//                     let state = self.state.read().clone();
//                     let upper_concat_iter = SstConcatIterator::create_and_seek_to_first(
//                         task.upper_level_sst_ids
//                             .iter()
//                             .map(|sst_id| state.sstables[sst_id].clone())
//                             .collect::<Vec<_>>(),
//                     )?;
//                     let lower_concat_iter = SstConcatIterator::create_and_seek_to_first(
//                         task.lower_level_sst_ids
//                             .iter()
//                             .map(|sst_id| state.sstables[sst_id].clone())
//                             .collect::<Vec<_>>(),
//                     )?;
//                     let mut merge_iter =
//                         TwoMergeIterator::create(upper_concat_iter, lower_concat_iter)?;
//                     let mut builder = SsTableBuilder::new(self.options.block_size);
//                     let mut output = vec![];
//                     while merge_iter.is_valid() {
//                         if merge_iter.value() != b"" {
//                             builder.add(merge_iter.key(), merge_iter.value());
//                             if builder.estimated_size() > self.options.target_sst_size {
//                                 let sst_id = self.next_sst_id();
//                                 let old_builder = std::mem::replace(
//                                     &mut builder,
//                                     SsTableBuilder::new(self.options.block_size),
//                                 );
//                                 let sst = old_builder.build(
//                                     sst_id,
//                                     Some(self.block_cache.clone()),
//                                     self.path_of_sst(sst_id),
//                                 )?;
//                                 output.push(Arc::new(sst));
//                             }
//                         }
//                         merge_iter.next()?;
//                     }
//
//                     if !builder.is_empty() {
//                         let sst_id = self.next_sst_id();
//                         let sst = builder.build(
//                             sst_id,
//                             Some(self.block_cache.clone()),
//                             self.path_of_sst(sst_id),
//                         )?;
//                         output.push(Arc::new(sst));
//                     }
//
//                     Ok(output)
//                 }
//             }
//             CompactionTask::Tiered(task) => {
//                 let state = self.state.read().clone();
//                 let tiers = &task.tiers;
//
//                 let concat_iters = task
//                     .tiers
//                     .iter()
//                     .map(|tier| {
//                         SstConcatIterator::create_and_seek_to_first(
//                             tier.1
//                                 .iter()
//                                 .map(|sst_id| state.sstables[sst_id].clone())
//                                 .collect::<Vec<_>>(),
//                         )
//                     })
//                     .collect::<Result<Vec<_>>>()?
//                     .into_iter()
//                     .map(Box::new)
//                     .collect::<Vec<_>>();
//
//                 let mut merge_iter = MergeIterator::create(concat_iters);
//
//                 let mut builder = SsTableBuilder::new(block_size);
//                 let mut output = vec![];
//                 while merge_iter.is_valid() {
//                     if merge_iter.value() != b"" || !task.bottom_tier_included {
//                         builder.add(merge_iter.key(), merge_iter.value());
//                         if builder.estimated_size() > self.options.target_sst_size {
//                             let sst_id = self.next_sst_id();
//                             let old_builder = std::mem::replace(
//                                 &mut builder,
//                                 SsTableBuilder::new(self.options.block_size),
//                             );
//                             let sst = old_builder.build(
//                                 sst_id,
//                                 Some(self.block_cache.clone()),
//                                 self.path_of_sst(sst_id),
//                             )?;
//                             output.push(Arc::new(sst));
//                         }
//                     }
//                     merge_iter.next()?;
//                 }
//
//                 if !builder.is_empty() {
//                     let sst_id = self.next_sst_id();
//                     let sst = builder.build(
//                         sst_id,
//                         Some(self.block_cache.clone()),
//                         self.path_of_sst(sst_id),
//                     )?;
//                     output.push(Arc::new(sst));
//                 }
//
//                 Ok(output)
//             }
//         }
//     }
