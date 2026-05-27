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
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use super::{BlockMeta, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
    table::FileObject,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: vec![],
            last_key: vec![],
            data: vec![],
            meta: vec![],
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // add key-value pair into builder
        // if block-builder returns false on adding,
        // std::mem::replace builder with a new one and add to that one
        // after getting the old block builder, take first_key, find last_key, get offset, and write to metadata
        // if first_key is empty, set first_key
        // replace last_key
        let block_full = !self.builder.add(key, value);
        if block_full {
            let old_block_builder =
                std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            let old_block = old_block_builder.build();

            let block_offset = self.data.len();
            self.data.put(old_block.encode());

            let first_key_len = u16::from_be_bytes([old_block.data[0], old_block.data[1]]) as usize;
            let first_key = KeyBytes::from_bytes(bytes::Bytes::copy_from_slice(
                &old_block.data[2..first_key_len + 2],
            ));

            let last_key_offset = old_block.offsets[old_block.offsets.len() - 1] as usize;
            let last_key_len = u16::from_be_bytes([
                old_block.data[last_key_offset],
                old_block.data[last_key_offset + 1],
            ]) as usize;
            let last_key = KeyBytes::from_bytes(bytes::Bytes::copy_from_slice(
                &old_block.data[last_key_offset + 2..last_key_offset + last_key_len + 2],
            ));

            let old_block_meta = BlockMeta {
                offset: block_offset,
                first_key,
                last_key,
            };

            self.meta.push(old_block_meta);

            let _ = self.builder.add(key, value);
        }

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec().into_inner();
        }

        self.last_key = key.to_key_vec().into_inner();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        // since one BlockMeta per block, we estimate there to be BlockMeta + 1 blocks, 1 for the builder
        (self.meta.len() + 1) * self.block_size
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        #[allow(unused_mut)] mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.builder.is_empty() {
            let old_block = self.builder.build();
            let block_offset: usize = self.data.len();

            self.data.put(old_block.encode());

            let first_key_len = u16::from_be_bytes([old_block.data[0], old_block.data[1]]) as usize;
            let first_key = KeyBytes::from_bytes(bytes::Bytes::copy_from_slice(
                &old_block.data[2..first_key_len + 2],
            ));

            let last_key_offset = old_block.offsets[old_block.offsets.len() - 1] as usize;
            let last_key_len = u16::from_be_bytes([
                old_block.data[last_key_offset],
                old_block.data[last_key_offset + 1],
            ]) as usize;
            let last_key = KeyBytes::from_bytes(bytes::Bytes::copy_from_slice(
                &old_block.data[last_key_offset + 2..last_key_offset + last_key_len + 2],
            ));

            let old_block_meta = BlockMeta {
                offset: block_offset,
                first_key,
                last_key,
            };

            self.meta.push(old_block_meta);
        }

        let mut metadata_buf = vec![];
        BlockMeta::encode_block_meta(&self.meta, &mut metadata_buf);
        let mut data = self.data.clone();
        data.extend_from_slice(&metadata_buf);
        let block_meta_offset: usize = self.data.len();
        data.extend_from_slice(&(block_meta_offset as u32).to_be_bytes());

        Ok(SsTable {
            file: FileObject::create(path.as_ref(), data)?,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key: KeyBytes::from_bytes(bytes::Bytes::from(self.first_key)),
            last_key: KeyBytes::from_bytes(bytes::Bytes::from(self.last_key)),
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
