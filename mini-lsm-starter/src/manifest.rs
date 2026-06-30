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

use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Seek};

use anyhow::{Result, bail};
use bytes::{Buf, Bytes};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        let file = if _path.as_ref().exists() {
            std::fs::File::open(_path)?
        } else {
            std::fs::File::create(_path)?
        };
        Ok(Manifest {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(_path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let file = std::fs::File::open(&_path)?;
        let mut buf = Bytes::from(std::fs::read(_path)?);
        let mut manifest_records = vec![];

        while buf.has_remaining() {
            let record_len = buf.get_u32();
            let encoded_record = buf.copy_to_bytes(record_len as usize);
            let checksum = buf.get_u32();
            let computed_checksum = crc32fast::hash(&encoded_record);

            if checksum != computed_checksum {
                bail!("manifest checksum err");
            }
            let record: ManifestRecord = serde_json::from_slice(&encoded_record)?;
            manifest_records.push(record);
        }

        // let manifest_records = serde_json::Deserializer::from_slice(&buf)
        //     .into_iter::<ManifestRecord>()
        //     .collect::<Result<Vec<ManifestRecord>, _>>()?;
        Ok((
            Manifest {
                file: Arc::new(Mutex::new(file)),
            },
            manifest_records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, _record: ManifestRecord) -> Result<()> {
        let encoded_record = serde_json::to_vec(&_record)?;
        let record_len = encoded_record.len() as u32;
        let checksum = crc32fast::hash(&encoded_record);

        let mut combined_record = vec![];
        combined_record.extend_from_slice(&record_len.to_be_bytes());
        combined_record.extend_from_slice(&encoded_record);
        combined_record.extend_from_slice(&checksum.to_be_bytes());

        let mut file_guard = self.file.lock();
        file_guard.seek(std::io::SeekFrom::End(0))?;
        file_guard.write_all(combined_record.as_slice())?;
        file_guard.sync_all()?;
        Ok(())
    }
}
