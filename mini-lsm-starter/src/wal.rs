// REMOVE THIS LINE after fully implementing this functionality
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

use anyhow::Result;
use bytes::Buf;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::KeySlice;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        // let file = if _path.as_ref().exists() {
        //     std::fs::File::options().read(true).write(true).open(_path)?
        // } else {
        //     std::fs::File::options().read(true).write(true).create(true).create(_path)?
        // };
        // Ok(Wal {
        //     file: Arc::new(Mutex::new(BufWriter::new(file))),
        // })

        // important, if not unable to read/write
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(_path)?;
        Ok(Wal {
        file: Arc::new(Mutex::new(BufWriter::new(file))),
    })
    }

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        // important, otherwise multi-sessions wouldnt work
        let file = std::fs::File::options().read(true).append(true).open(&_path)?;
        let binding = std::fs::read(_path)?;
        let mut wal = binding.as_slice();
        while wal.has_remaining() {
            let key_len = wal.get_u16() as usize;
            let key = wal.copy_to_bytes(key_len);
            let value_len = wal.get_u16() as usize;
            let value = wal.copy_to_bytes(value_len);
            _skiplist.insert(key, value);
        }
        Ok(Wal {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let mut lock = self.file.lock();
        let key_len = (_key.len() as u16).to_be_bytes();
        let value_len = (_value.len() as u16).to_be_bytes();
        lock.write_all(&key_len)?;
        lock.write_all(_key)?;
        lock.write_all(&value_len)?;
        lock.write_all(_value)?;
        Ok(())
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
