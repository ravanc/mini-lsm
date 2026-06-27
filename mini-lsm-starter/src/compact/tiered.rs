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

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        // precondition
        if _snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // first trigger: space amplification
        let engine_size = _snapshot.levels[.._snapshot.levels.len() - 1]
            .iter()
            .fold(0, |size, level| size + level.1.len());
        let last_level_size = _snapshot.levels[_snapshot.levels.len() - 1].1.len();

        // println!("SPACE AMP EVAL {}, THRESHOLD {}", engine_size * 100 / last_level_size, self.options.max_size_amplification_percent);
        if (last_level_size == 0 && engine_size > 0)
            || (engine_size * 100 / last_level_size >= self.options.max_size_amplification_percent)
        {
            // println!("\nSPACE AMP TRIGGER\n");
            return Some(TieredCompactionTask {
                tiers: _snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // second trigger: size ratio
        let mut prev = 0;
        for i in 0.._snapshot.levels.len() {
            if prev == 0 {
                prev += _snapshot.levels[i].1.len();
                continue;
            }
            // println!("SIZE RATIO EVAL {}, THRESHOLD {}", _snapshot.levels[i].1.len() * 100 / prev, self.options.size_ratio);
            if _snapshot.levels[i].1.len() * 100 / prev > 100 + self.options.size_ratio
                && i >= self.options.min_merge_width
            {
                // println!("\nSIZE RATIO TRIGGER\n");
                return Some(TieredCompactionTask {
                    tiers: _snapshot.levels[..i].to_vec(),
                    bottom_tier_included: false,
                });
            }
            prev += _snapshot.levels[i].1.len();
        }

        // println!("\nFALLBACK TRIGGER\n");
        // third trigger (fallback): major compaction
        let max_merge = self
            .options
            .max_merge_width
            .unwrap_or(_snapshot.levels.len());
        Some(TieredCompactionTask {
            tiers: _snapshot.levels[..max_merge].to_vec(),
            bottom_tier_included: max_merge >= _snapshot.levels.len(),
        })
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = _snapshot.clone();

        let tier_ids = _task.tiers.iter().map(|tier| tier.0).collect::<Vec<_>>();

        let to_delete = _task
            .tiers
            .iter()
            .flat_map(|tier| tier.1.iter().copied())
            .collect::<Vec<_>>();

        new_state.levels.insert(
            new_state
                .levels
                .iter()
                .position(|level| level.0 == tier_ids[0])
                .unwrap_or(0),
            (_output[0], _output.to_vec()),
        );

        new_state
            .levels
            .retain(|level| !tier_ids.contains(&level.0));

        // new_state.levels.insert(0, (_output[0], _output.to_vec()));

        (new_state, to_delete)
    }
}
