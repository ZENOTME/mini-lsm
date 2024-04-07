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

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = Arc::new(Mutex::new(BufWriter::new(File::create(path)?)));
        Ok(Self { file })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        {
            let mut reader = File::open(&path)?;
            let mut len_buf = [0; size_of::<u64>()];
            loop {
                let len = reader.read(&mut len_buf)?;
                if len != size_of::<u64>() {
                    if len == 0 {
                        break;
                    }
                    return Err(anyhow::anyhow!("read len failed"));
                }
                let key_len = u64::from_be_bytes(len_buf);
                let mut key = vec![0; key_len as usize];
                reader.read_exact(&mut key)?;

                let len = reader.read(&mut len_buf)?;
                if len != size_of::<u64>() {
                    return Err(anyhow::anyhow!("read len failed"));
                }
                let value_len = u64::from_be_bytes(len_buf);
                let mut value = vec![0; value_len as usize];
                reader.read_exact(&mut value)?;

                skiplist.insert(Bytes::from(key), Bytes::from(value));
            }
        }
        let file = Arc::new(Mutex::new(BufWriter::new(
            OpenOptions::new().append(true).open(path)?,
        )));
        Ok(Self { file })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut content = BytesMut::with_capacity(size_of::<u64>() * 2 + key.len() + value.len());
        content.put_u64(key.len() as u64);
        content.put(key);
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
