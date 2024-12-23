use std::{collections::BTreeMap, fs::File, io::Write, num::NonZeroU32};

use crate::utils::bytes_to_u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WALRecord {
    Insert(NonZeroU32, u32),
    Delete(NonZeroU32),
}

// serialize and deserialize a WAL Record
impl WALRecord {
    pub fn from_bytes(bytes: &[u8; 8]) -> Self {
        if bytes_to_u32(&bytes[0..4]) == 0 {
            let id = bytes_to_u32(&bytes[4..8]);
            if id == 0 {
                panic!("Invalid id of 0 to delete provided");
            }
            Self::Delete(id.try_into().unwrap())
        } else {
            Self::Insert(
                bytes_to_u32(&bytes[0..4]).try_into().unwrap(),
                bytes_to_u32(&bytes[4..8]),
            )
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            WALRecord::Insert(id, val) => {
                let mut bytes = vec![];

                bytes.extend(id.get().to_le_bytes());
                bytes.extend(val.to_le_bytes());

                bytes
            }
            WALRecord::Delete(id) => {
                let mut bytes = vec![];
                bytes.extend(0u32.to_le_bytes());
                bytes.extend(id.get().to_le_bytes());
                bytes
            }
        }
    }
}

pub fn deserialize_wal(bytes: &[u8]) -> Vec<WALRecord> {
    assert!(bytes.len() % 8 == 0);

    let mut records = vec![];

    for i in 0..(bytes.len() / 8) {
        records.push(WALRecord::from_bytes(
            &bytes[i * 8..(i + 1) * 8].try_into().unwrap(),
        ));
    }

    records
}

// The Wal itself needs to have a file handle to append to
#[derive(Debug)]
pub struct WAL {
    pub file: File,
    pub records: BTreeMap<NonZeroU32, u32>,
}

impl WAL {
    pub fn insert(&mut self, id: NonZeroU32, val: u32) -> bool {
        let res = self.records.insert(id, val).is_some();
        let _ = self.file.write_all(&WALRecord::Insert(id, val).to_bytes());

        res
    }
    pub fn remove(&mut self, id: NonZeroU32) -> Option<u32> {
        let res = self.records.remove(&id);
        let _ = self.file.write_all(&WALRecord::Delete(id).to_bytes());
        res
    }
    pub fn get(&self, id: NonZeroU32) -> Option<u32> {
        self.records.get(&id).copied()
    }
}
