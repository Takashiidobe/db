use std::num::NonZeroU32;

use crate::utils::bytes_to_u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Record {
    pub id: NonZeroU32,
    pub val: u32,
}

impl Record {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut res = self.id.get().to_le_bytes().to_vec();
        res.extend(self.val.to_le_bytes());
        res
    }

    pub fn from_bytes(bytes: &[u8; 8]) -> Self {
        let id = bytes_to_u32(&bytes[0..4]);

        let id = if id == 0 {
            NonZeroU32::new(1).unwrap()
        } else {
            id.try_into().unwrap()
        };

        let val = bytes_to_u32(&bytes[4..8]);

        Self { id, val }
    }
}
