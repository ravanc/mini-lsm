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
use crate::iterators::merge_iterator::MergeIterator;
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
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        unimplemented!()
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let new_state = self.state.read().as_ref().clone();
        let original_l0 = new_state.l0_sstables.clone();
        let combined_sst_ids = new_state
            .l0_sstables
            .iter()
            .chain(new_state.levels[0].1.iter())
            .copied() // need to have this for some reason
            .collect::<Vec<_>>();

        let combined_sst = combined_sst_ids
            .iter()
            .map(|sst_id| new_state.sstables[sst_id].clone())
            .map(|sst| SsTableIterator::create_and_seek_to_first(sst).map(Box::new))
            .collect::<Result<Vec<_>>>()?;

        println!("COMBINED_SST LEN {}", combined_sst.len());

        let mut merge_iter = MergeIterator::create(combined_sst);
        let mut builder = SsTableBuilder::new(self.options.block_size);
        let mut new_l1 = vec![];
        let mut compacted_sst = vec![];

        while merge_iter.is_valid() {
            println!("ADDED KEY {:?}", merge_iter.key());
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
                    new_l1.push(sst_id);
                    compacted_sst.push((sst_id, Arc::new(sst)));
                }
            }
            merge_iter.next()?;
        }

        if builder.estimated_size() > 0 {
            let sst_id = self.next_sst_id();
            let sst = builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?;
            new_l1.push(sst_id);
            compacted_sst.push((sst_id, Arc::new(sst)));
            println!("BLOCK ADDED CASE 2");
        }

        let mut write_lock = self.state.write();
        let mut state_clone = write_lock.as_ref().clone();

        let mut new_sstables = state_clone.sstables.clone();
        let mut new_l0 = state_clone.l0_sstables.clone();
        new_l0.retain(|sst_id| !original_l0.contains(sst_id));
        for sst_id in &combined_sst_ids {
            new_sstables.remove(sst_id);
        }

        for (sst_id, sst_arc) in compacted_sst {
            new_sstables.insert(sst_id, sst_arc);
        }

        state_clone.l0_sstables = new_l0;
        println!("L1 LEN {}", new_l1.len());
        state_clone.levels = vec![(1, new_l1)];
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
        unimplemented!()
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
