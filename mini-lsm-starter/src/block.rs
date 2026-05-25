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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

use crate::key::KeyVec;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    fn get_key_len_at(&self, position: u16) -> u16 {
        let offset = self.offsets[position as usize] as usize;
        u16::from_be_bytes([self.data[offset], self.data[offset + 1]])
    }

    fn get_key_at(&self, position: u16) -> KeyVec {
        let offset = self.offsets[position as usize] as usize;
        let key_len = self.get_key_len_at(position) as usize;
        KeyVec::from_vec(self.data[offset + 2..offset + key_len + 2].to_vec())
    }

    fn get_value_len_at(&self, position: u16) -> u16 {
        let offset = self.offsets[position as usize] as usize;
        let key_len = self.get_key_len_at(position) as usize;
        u16::from_be_bytes([
            self.data[offset + 2 + key_len],
            self.data[offset + 3 + key_len],
        ])
    }

    fn get_value_range_at(&self, position: u16) -> (usize, usize) {
        let offset = self.offsets[position as usize] as usize;
        let key_len = self.get_key_len_at(position) as usize;
        let value_len = self.get_value_len_at(position) as usize;
        (
            offset + 2 + key_len + 2,
            offset + 2 + key_len + 2 + value_len,
        )
    }

    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        // let data_in_bytes: Bytes = Bytes::from(self.data.clone());
        // let offset_in_bytes = Bytes::from(self
        //     .offsets
        //     .iter()
        //     .flat_map(|offset| offset.to_be_bytes())
        //     .collect::<Vec<_>>()
        // );

        let mut buf = BytesMut::with_capacity(self.data.len() + self.offsets.len() * 2);
        let num_elements = self.offsets.len();
        buf.extend_from_slice(&self.data);
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.put_u16(num_elements as u16);
        buf.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let data_len = data.len();
        let num_elements: usize =
            u16::from_be_bytes([data[data_len - 2], data[data_len - 1]]) as usize;
        let offsets_start = data_len - 2 - num_elements * 2;
        let block_data = data[..offsets_start].to_vec();

        let offsets = data[offsets_start..data_len - 2]
            .chunks_exact(2)
            .map(|c| u16::from_be_bytes([c[0], c[1]]))
            .collect::<Vec<u16>>();

        Block {
            data: block_data,
            offsets,
        }
    }
}
