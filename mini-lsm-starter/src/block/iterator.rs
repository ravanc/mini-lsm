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

use crate::key::{KeySlice, KeyVec, TS_DEFAULT};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn get_key_overlap_len_at(&self, position: u16) -> u16 {
        let offset = self.block.offsets[position as usize] as usize;
        u16::from_be_bytes([self.block.data[offset], self.block.data[offset + 1]])
    }

    fn get_rest_key_len_at(&self, position: u16) -> u16 {
        let offset = self.block.offsets[position as usize] as usize;
        u16::from_be_bytes([self.block.data[offset + 2], self.block.data[offset + 3]])
    }

    fn get_key_at(&self, position: u16) -> KeyVec {
        let offset = self.block.offsets[position as usize] as usize;
        let key_overlap_len = self.get_key_overlap_len_at(position) as usize;
        let rest_key_len = self.get_rest_key_len_at(position) as usize;
        let rest_key = &self.block.data[offset + 4..offset + rest_key_len + 4];
        let overlap_key = &self.first_key.key_ref()[..key_overlap_len];
        let key = [overlap_key, rest_key].concat();
        let ts = TS_DEFAULT;
        KeyVec::from_vec_with_ts(key, ts)
    }

    fn get_value_len_at(&self, position: u16) -> u16 {
        let offset = self.block.offsets[position as usize] as usize;
        let key_len = self.get_rest_key_len_at(position) as usize;
        u16::from_be_bytes([
            // have to add 8 for extra timestamp
            self.block.data[offset + 4 + 8 + key_len],
            self.block.data[offset + 5 + 8 + key_len],
        ])
    }

    fn get_value_range_at(&self, position: u16) -> (usize, usize) {
        let offset = self.block.offsets[position as usize] as usize;
        let key_len = self.get_rest_key_len_at(position) as usize;
        let value_len = self.get_value_len_at(position) as usize;
        (
            // have to add 8 for extra timestamp
            offset + 4 + 8 + key_len + 2,
            offset + 4 + 8 + key_len + 2 + value_len,
        )
    }

    fn new(block: Arc<Block>) -> Self {
        let rest_key_len = u16::from_be_bytes([block.data[2], block.data[3]]) as usize;
        let ts = TS_DEFAULT;
        let first_key = KeyVec::from_vec_with_ts(block.data[4..4 + rest_key_len].to_vec(), ts);

        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_first();
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice<'_> {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        let lower_bound = self.value_range.0;
        let upper_bound = self.value_range.1;
        &self.block.data[lower_bound..upper_bound]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        let first_key = self.get_key_at(0);
        self.key = first_key;
        let value_range = self.get_value_range_at(0);
        self.value_range = value_range;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        if self.idx >= self.block.offsets.len() {
            self.key = KeyVec::new();
        } else {
            let key = self.get_key_at(self.idx as u16);
            let value_range = self.get_value_range_at(self.idx as u16);
            self.key = key;
            self.value_range = value_range
        }
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek_to_first();
        while self.is_valid() && self.key() < key {
            self.next();
        }
    }
}
