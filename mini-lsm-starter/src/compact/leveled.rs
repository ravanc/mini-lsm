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

use std::cmp::{max, min};

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        let first_key = _sst_ids.iter().fold(
            _snapshot.sstables[&_sst_ids[0]].first_key(),
            |key, sst_id| min(_snapshot.sstables[sst_id].first_key(), key),
        );
        let last_key = _sst_ids.iter().fold(
            _snapshot.sstables[&_sst_ids[0]].last_key(),
            |key, sst_id| max(_snapshot.sstables[sst_id].last_key(), key),
        );

        let level = &_snapshot.levels[_in_level].1;
        let start = level.partition_point(|id| _snapshot.sstables[id].last_key() < first_key);
        let end = level.partition_point(|id| _snapshot.sstables[id].first_key() <= last_key);
        level[start..end].to_vec()
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let last_level_size_b = _snapshot.levels.last()?.1.iter().fold(0, |size, sst_id| {
            size + _snapshot.sstables[sst_id].file.size()
        }) as usize;
        let mut target_sizes = vec![0; _snapshot.levels.len()];
        target_sizes[_snapshot.levels.len() - 1] = last_level_size_b;
        if last_level_size_b > self.options.base_level_size_mb * 1024 * 1024 {
            let mut cur_size = last_level_size_b;
            target_sizes.reverse();
            // more idiomatic rust
            for slot in &mut target_sizes {
                *slot = cur_size;
                cur_size /= self.options.level_size_multiplier;
                if cur_size == 0 {
                    break;
                }
            }
            target_sizes.reverse();
        }

        // trigger 1: flush l0 sstables once past threshold
        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let flush_target = target_sizes
                .iter()
                .position(|size| *size > 0)
                .unwrap_or(target_sizes.len() - 1);
            let overlapping_ssts =
                self.find_overlapping_ssts(_snapshot, &_snapshot.l0_sstables, flush_target);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: flush_target,
                lower_level_sst_ids: overlapping_ssts,
                is_lower_level_bottom_level: flush_target == target_sizes.len() - 1,
            });
        }

        // trigger 2: decide by ratio past 1.0
        let cur_sizes = _snapshot
            .levels
            .iter()
            .map(|level| {
                level.1.iter().fold(0, |size, sst_id| {
                    size + _snapshot.sstables[sst_id].file.size()
                }) as usize
            })
            .collect::<Vec<_>>();
        let ratios = cur_sizes[..cur_sizes.len() - 1]
            .iter()
            .zip(target_sizes[..target_sizes.len() - 1].iter())
            .map(|(cur, target)| if *target == 0 { 0 } else { cur * 10 / target })
            .collect::<Vec<_>>();
        let max_ratio = ratios.iter().max()?;
        if *max_ratio > 10 {
            let to_compact = ratios.iter().position(|ratio| ratio == max_ratio)?;
            let sst_ids: &Vec<usize> = &_snapshot.levels[to_compact].1;
            let min_sst_id = *sst_ids.iter().min().unwrap_or(&sst_ids[0]);
            let overlapping_ssts =
                self.find_overlapping_ssts(_snapshot, &[min_sst_id], to_compact + 1);
            return Some(LeveledCompactionTask {
                upper_level: Some(to_compact),
                upper_level_sst_ids: vec![min_sst_id],
                lower_level: to_compact + 1,
                lower_level_sst_ids: overlapping_ssts,
                is_lower_level_bottom_level: to_compact == target_sizes.len() - 2,
            });
        }

        // l0 within threshold, all ratios below 1
        None
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = _snapshot.clone();

        // need to remember the case where lower_level_sst_ids can now be empty if there is no overlap
        // if _task.lower_level_sst_ids.len() == 0 {
        // } else {
        //     let insert_position = new_state.levels[_task.lower_level]
        //         .1
        //         .iter()
        //         .position(|sst_id| *sst_id == _task.lower_level_sst_ids[0])
        //         .unwrap_or(0);
        //     new_state.levels[_task.lower_level]
        //         .1
        //         .splice(insert_position..insert_position, _output.to_vec());
        // }

        let mut insert_position = 0;

        if !_in_recovery {
            let first_key = _snapshot.sstables[&_output[0]].first_key();
            insert_position = _snapshot.levels[_task.lower_level]
                .1
                .partition_point(|id| _snapshot.sstables[id].last_key() < first_key);
        }
        new_state.levels[_task.lower_level]
            .1
            .splice(insert_position..insert_position, _output.to_vec());

        if let Some(upper_level) = _task.upper_level {
            new_state.levels[upper_level]
                .1
                .retain(|sst_id| !_task.upper_level_sst_ids.contains(sst_id));
        } else {
            new_state
                .l0_sstables
                .retain(|sst_id| !_task.upper_level_sst_ids.contains(sst_id));
        }

        new_state.levels[_task.lower_level]
            .1
            .retain(|sst_id| !_task.lower_level_sst_ids.contains(sst_id));

        for removed_sst_id in &_task.lower_level_sst_ids {
            new_state.sstables.remove(removed_sst_id);
        }

        for removed_sst_id in &_task.upper_level_sst_ids {
            new_state.sstables.remove(removed_sst_id);
        }

        let to_delete = [
            _task.upper_level_sst_ids.as_slice(),
            _task.lower_level_sst_ids.as_slice(),
        ]
        .concat();

        (new_state, to_delete)
    }
}
