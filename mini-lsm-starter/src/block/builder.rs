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

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec, TS_DEFAULT};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let offset = self.data.len();
        let offset_len = (self.offsets.len() + 1) * 2;
        let key_len = key.key_len();
        let value_len = value.len();
        let num_elements_size = 2;
        let after_len =
            offset + offset_len + key_len + 8 /* timestamp size */ + value_len + num_elements_size;
        if after_len > self.block_size && !self.first_key.is_empty() {
            return false;
        }
        self.offsets.push(offset as u16);
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
            self.data.put_u16(0); // overlap_len
            self.data.put_u16(key_len as u16); // rest_key_len
            self.data.put(key.key_ref());
            self.data.put_u64(TS_DEFAULT);
            self.data.put_u16(value_len as u16);
            self.data.put(value);
        }
        /*
        pre-compression optimisation
        self.data.put_u16(key_len as u16);
        self.data.put(key.key_ref());
        self.data.put_u16(value_len as u16);
        self.data.put(value);
        */
        else {
            let key_overlap_len = key
                .key_ref()
                .iter()
                .zip(self.first_key.key_ref().iter())
                .take_while(|(a, b)| a == b)
                .count();
            self.data.put_u16(key_overlap_len as u16);
            let rest_key_len = key.key_len() - key_overlap_len;
            self.data.put_u16(rest_key_len as u16);
            self.data.put(&key.key_ref()[key_overlap_len..]);
            self.data.put_u64(TS_DEFAULT);
            self.data.put_u16(value_len as u16);
            self.data.put(value);
        }
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
