#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::BTreeMap;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Default for Watermark {
    fn default() -> Self {
        Self::new()
    }
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        *self.readers.entry(ts).or_default() += 1;
    }

    pub fn remove_reader(&mut self, ts: u64) {
        if let Some(count) = self.readers.get_mut(&ts) {
            *count -= 1;
            if *count == 0 {
                self.readers.remove(&ts);
            }
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.iter().min().map(|(ts, _)| *ts)
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }
}
