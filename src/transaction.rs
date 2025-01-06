use std::fs::File;

use crate::{
    row::{bytes_to_id, RowType, RowVal},
    utils::{bytes_to_u16, bytes_to_u32},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TransactionItem {
    Start(u32),          // start transaction $num
    Rollback(u32),       // rollback transaction $num
    Commit(u32),         // commit transaction $num
    Checkpoint,          // there are no active transactions at this point
    Insert(Vec<RowVal>), // an update that inserts these items (id + values needs to be set)
    Delete(Vec<RowVal>), // an update that deletes these items (id + values needs to be set)
}

pub struct Transactions {
    transactions: Vec<TransactionItem>,
    file: File,
}

fn serialize_rows(rows: &[RowVal]) -> Vec<u8> {
    let mut res = vec![];

    if rows.len() > u16::MAX.into() {
        panic!("only up to u16::MAX updates are allowed");
    }
    res.extend((rows.len() as u16).to_le_bytes()); // only 2^16 updates
    for val in rows {
        match val {
            RowVal::Id(_) => {
                res.extend(RowType::Id.to_bytes());
            }
            RowVal::U32(_) => {
                res.extend(RowType::U32.to_bytes());
            }
            RowVal::Bytes(_) => {
                res.extend(RowType::Bytes.to_bytes());
            }
            RowVal::Bool(_) => {
                res.extend(RowType::Bool.to_bytes());
            }
        }
        res.extend(val.to_bytes());
    }
    res
}

impl TransactionItem {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            TransactionItem::Start(n) => {
                let mut res = vec![0];
                res.extend(n.to_le_bytes());
                res
            }
            TransactionItem::Rollback(n) => {
                let mut res = vec![1];
                res.extend(n.to_le_bytes());
                res
            }
            TransactionItem::Commit(n) => {
                let mut res = vec![2];
                res.extend(n.to_le_bytes());
                res
            }
            TransactionItem::Checkpoint => vec![3],
            TransactionItem::Insert(row_vals) => {
                let mut res = vec![4];
                res.extend(serialize_rows(row_vals));
                res
            }
            TransactionItem::Delete(row_vals) => {
                let mut res = vec![5];
                res.extend(serialize_rows(row_vals));
                res
            }
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let marker = bytes[0];

        let bytes = &bytes[1..];

        match marker {
            0 => Self::Start(bytes_to_u32(&bytes[..4])),
            1 => Self::Rollback(bytes_to_u32(&bytes[..4])),
            2 => Self::Commit(bytes_to_u32(&bytes[..4])),
            3 => Self::Checkpoint,
            4 => Self::Insert(deserialize_bytes(bytes)),
            5 => Self::Delete(deserialize_bytes(bytes)),
            _ => panic!("invalid transaction"),
        }
    }
}

fn deserialize_bytes(bytes: &[u8]) -> Vec<RowVal> {
    let len = bytes_to_u16(&bytes[0..2]);
    let mut items = vec![];
    let mut i = 2;
    for _ in 0..len {
        let row_type = RowType::from_bytes(&bytes[i..i + 1].try_into().unwrap());
        i += 1;
        match row_type {
            RowType::Id => {
                let id = bytes_to_id(&bytes[i..i + 4]);
                items.push(RowVal::Id(id));
                i += 4
            }
            RowType::U32 => {
                let num = bytes_to_u32(&bytes[i..i + 4]);
                items.push(RowVal::U32(num));
                i += 4
            }
            RowType::Bytes => {
                let len = bytes_to_u16(&bytes[i..i + 2]) as usize;
                i += 2;
                items.push(RowVal::Bytes(bytes[i..i + len].to_vec()));
                i += len;
            }
            RowType::Bool => {
                items.push(RowVal::Bool(bytes[i] == 1));
                i += 1;
            }
        }
    }
    items
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use quickcheck::Arbitrary;
    use quickcheck_macros::quickcheck;

    use crate::row::{RowType, RowVal};

    use super::TransactionItem;

    impl Arbitrary for RowType {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            let choice = g.choose(&[0u8, 1, 2, 3]).unwrap();
            match choice {
                0 => RowType::Id,
                1 => RowType::U32,
                2 => RowType::Bool,
                3 => RowType::Bytes,
                _ => unreachable!(),
            }
        }
    }

    impl Arbitrary for RowVal {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            let row_type = RowType::arbitrary(g);
            match row_type {
                RowType::Id => RowVal::Id(NonZeroU32::arbitrary(g)),
                RowType::U32 => RowVal::U32(u32::arbitrary(g)),
                RowType::Bytes => RowVal::Bytes(Vec::arbitrary(g)),
                RowType::Bool => RowVal::Bool(bool::arbitrary(g)),
            }
        }
    }

    impl Arbitrary for TransactionItem {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            let choice = g.choose(&[0u8, 1, 2, 3, 4, 5]).unwrap();
            match choice {
                0 => Self::Start(u32::arbitrary(g)),
                1 => Self::Rollback(u32::arbitrary(g)),
                2 => Self::Commit(u32::arbitrary(g)),
                3 => Self::Checkpoint,
                4 => Self::Insert(Vec::arbitrary(g)),
                5 => Self::Delete(Vec::arbitrary(g)),
                _ => unreachable!(),
            }
        }
    }

    #[quickcheck]
    fn serde(transaction: TransactionItem) -> bool {
        TransactionItem::from_bytes(&transaction.to_bytes()) == transaction
    }
}
