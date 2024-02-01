#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let len = self.offsets.len() as u16;
        let mut buf = BytesMut::with_capacity(self.data.len() + self.offsets.len() * 2 + 2);
        buf.extend_from_slice(&self.data);
        buf.extend_from_slice(
            &self
                .offsets
                .iter()
                .map(|offset| offset.to_be_bytes())
                .flatten()
                .collect::<Vec<u8>>(),
        );
        buf.extend_from_slice(&len.to_be_bytes());
        buf.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let len = data.len();
        assert!(len >= 2);

        // parse entry len
        let entry_len = u16::from_be_bytes([data[len - 2], data[len - 1]]) as usize;
        assert!(len >= 2 + entry_len * 2);

        // parse offset
        let offset_data = &data[len - 2 - entry_len * 2..len - 2];
        assert!(offset_data.len() == entry_len * 2);
        let offsets = offset_data
            .chunks_exact(2)
            .map(|x| u16::from_be_bytes([x[0], x[1]]))
            .collect::<Vec<u16>>();

        // parse data
        let data = data[..len - 2 - entry_len * 2].to_vec();

        Self { data, offsets }
    }
}
