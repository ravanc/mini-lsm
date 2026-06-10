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
        let current = SsTableIterator::create_and_seek_to_first(sstables[0].clone())?;
        Ok(Self {
            current: Some(current),
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        // claude given idiomatic way of finding first index where first_key < key
        let mut idx = sstables.partition_point(|sst| sst.first_key().as_key_slice() < key);
        idx = if idx == 0 { 0 } else { idx - 1 };
        let mut current = SsTableIterator::create_and_seek_to_key(sstables[idx].clone(), key)?;
        if !current.is_valid() && idx + 1 < sstables.len() {
            idx += 1;
            current = SsTableIterator::create_and_seek_to_key(sstables[idx].clone(), key)?;
        }

        Ok(Self {
            current: Some(current),
            next_sst_idx: idx + 1,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice<'_> {
        // calling unwrap safely as calling self.key() requires self.is_valid() which requires current to be Some and valid
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        // calling unwrap safely as calling self.value() requires self.is_valid() which requires current to be Some and valid
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some() && self.current.as_ref().is_some_and(|iter| iter.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        if !self.current.as_ref().unwrap().is_valid() {
            if self.next_sst_idx == self.sstables.len() {
                self.current = None;
            } else {
                let new_iter = SsTableIterator::create_and_seek_to_first(
                    self.sstables[self.next_sst_idx].clone(),
                )?;
                self.current.replace(new_iter);
                self.next_sst_idx += 1;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
