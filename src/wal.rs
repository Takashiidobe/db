use std::{collections::BTreeMap, fs::File, io::Write, num::NonZeroU32};

#[cfg(test)]
use serde::{Deserialize, Serialize};

use crate::row::{bytes_to_id, bytes_to_values, RowType, RowVal};

#[cfg_attr(test, derive(Serialize, Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WALRecord {
    Insert(NonZeroU32, Vec<RowVal>),
    Delete(NonZeroU32),
}

impl WALRecord {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            WALRecord::Insert(id, row) => {
                let mut res = vec![];
                res.extend(id.get().to_le_bytes());
                let row_val: Vec<_> = row.iter().flat_map(|x| x.clone().to_bytes()).collect();
                res.extend(row_val);
                res
            }
            WALRecord::Delete(id) => {
                let mut res = vec![0, 0, 0, 0];
                res.extend(id.get().to_le_bytes());
                res
            }
        }
    }

    pub fn from_bytes(bytes: &[u8], schema: &[RowType]) -> (Self, usize) {
        match bytes[0..4] {
            [0, 0, 0, 0] => {
                let id = bytes_to_id(&bytes[4..8]);
                (WALRecord::Delete(id), 8)
            }
            _ => {
                let (rows, incr) = bytes_to_values(bytes, schema);
                if let RowVal::Id(id) = rows[0] {
                    return (WALRecord::Insert(id, rows[1..].to_vec()), incr + 4);
                }
                panic!("Id must be the first row in the byte array")
            }
        }
    }
}

pub fn deserialize_wal(bytes: &[u8], schema: &[RowType]) -> Vec<WALRecord> {
    let mut records = vec![];
    let mut i = 0;

    if bytes.len() < 4 {
        return records;
    }

    while i < bytes.len() - 4 {
        let (wal_record, incr) = WALRecord::from_bytes(&bytes[i..], schema);
        records.push(wal_record);
        i += incr;
    }

    records
}

#[derive(Debug)]
pub struct WAL {
    pub file: File,
    pub records: BTreeMap<NonZeroU32, Vec<RowVal>>,
}

impl WAL {
    pub fn insert(&mut self, id: NonZeroU32, values: &[RowVal]) -> bool {
        self.records.insert(id, values.to_vec());
        let _ = self
            .file
            .write_all(&WALRecord::Insert(id, values.to_vec()).to_bytes());
        true
    }
    pub fn remove(&mut self, id: NonZeroU32) -> Option<Vec<RowVal>> {
        let res = self.records.remove(&id);
        let _ = self.file.write_all(&WALRecord::Delete(id).to_bytes());
        res
    }
    pub fn get(&self, id: NonZeroU32) -> Option<Vec<RowVal>> {
        self.records.get(&id).cloned()
    }
}
