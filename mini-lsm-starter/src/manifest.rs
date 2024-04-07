#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
    records: Arc<Mutex<Vec<ManifestRecord>>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = Arc::new(Mutex::new(File::create_new(path)?));
        Ok(Self {
            file,
            records: Arc::new(Mutex::new(vec![])),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let records = {
            let file = File::open(&path)?;
            let deserializer = serde_json::Deserializer::from_reader(BufReader::new(&file));
            deserializer
                .into_iter::<ManifestRecord>()
                .map(|record| Ok(record?))
                .collect::<Result<Vec<ManifestRecord>>>()?
        };
        let file = Arc::new(Mutex::new(OpenOptions::new().append(true).open(&path)?));
        Ok((
            Self {
                file,
                records: Arc::new(Mutex::new(Vec::new())),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();
        let mut records = self.records.lock();
        let content = serde_json::to_vec(&record)?;
        file.write(&content)?;
        records.push(record);

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.file.lock().sync_all()?;
        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        self.file.lock().flush()?;
        Ok(())
    }
}
