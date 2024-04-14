#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = Arc::new(Mutex::new(BufWriter::new(File::create(path)?)));
        Ok(Self { file })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut buf = [0; size_of::<u64>()];
        let mut read_u64_be = |reader: &mut File| -> Result<Option<u64>> {
            let len = reader.read(&mut buf)?;
            if len != size_of::<u64>() {
                if len == 0 {
                    return Ok(None);
                }
                return Err(anyhow::anyhow!("read_u64_be: read {} bytes", len));
            }
            Ok(Some(u64::from_be_bytes(buf)))
        };

        {
            let mut reader = File::open(&path)?;
            loop {
                let Some(key_len) = read_u64_be(&mut reader)? else {
                    break;
                };
                let mut key = vec![0; key_len as usize];
                reader.read_exact(&mut key)?;

                let Some(ts) = read_u64_be(&mut reader)? else {
                    return Err(anyhow::anyhow!("EOF while reading ts"));
                };

                let Some(value_len) = read_u64_be(&mut reader)? else {
                    return Err(anyhow::anyhow!("EOF while reading value"));
                };
                let mut value = vec![0; value_len as usize];
                reader.read_exact(&mut value)?;

                skiplist.insert(
                    KeyBytes::from_bytes_with_ts(Bytes::from(key), ts),
                    Bytes::from(value),
                );
            }
        }
        let file = Arc::new(Mutex::new(BufWriter::new(
            OpenOptions::new().append(true).open(path)?,
        )));
        Ok(Self { file })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        // | key_len(u64) | key | ts(u64) | value_len(u64) | value |
        let mut content =
            BytesMut::with_capacity(size_of::<u64>() * 3 + key.key_len() + value.len());
        content.put_u64(key.key_len() as u64);
        content.put(key.key_ref());
        content.put_u64(key.ts());
        content.put_u64(value.len() as u64);
        content.put(value);

        let mut file = self.file.lock();
        file.write_all(content.as_ref())?;

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_ref().sync_all()?;

        Ok(())
    }
}
