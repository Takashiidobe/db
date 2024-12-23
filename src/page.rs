use crate::{record::Record, utils::bytes_to_u32};
use std::{collections::BTreeMap, num::NonZeroU32};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageHeader {
    pub end: NonZeroU32,
    pub start: NonZeroU32,
    pub count: u32,
}

impl PageHeader {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut res = self.end.get().to_le_bytes().to_vec();
        res.extend(self.start.get().to_le_bytes());
        res.extend(self.count.to_le_bytes());
        res
    }

    pub fn from_bytes(bytes: &[u8; 12]) -> Self {
        let end = NonZeroU32::new(bytes_to_u32(&bytes[0..4])).unwrap();
        let start = NonZeroU32::new(bytes_to_u32(&bytes[4..8])).unwrap();
        let count = bytes_to_u32(&bytes[8..12]);

        Self { end, start, count }
    }

    pub fn size() -> usize {
        12
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Page {
    pub header: PageHeader,
    pub data: BTreeMap<NonZeroU32, u32>,
    pub dirty: bool,
}

pub const PAGE_SIZE: usize = if cfg!(feature = "small_pages") {
    28
} else {
    4096
};

impl Page {
    pub fn new(data: &[Record]) -> Self {
        let data = BTreeMap::from_iter(data.iter().map(|record| (record.id, record.val)));

        let start = *data
            .first_key_value()
            .unwrap_or((&1.try_into().unwrap(), &0))
            .0;
        let end = *data
            .last_key_value()
            .unwrap_or((&1.try_into().unwrap(), &0))
            .0;

        let header = PageHeader {
            count: data.len() as u32,
            start,
            end,
        };

        Page {
            header,
            data,
            dirty: false,
        }
    }

    pub fn new_dirty(data: &[Record]) -> Self {
        let mut page = Page::new(data);
        page.dirty = true;
        page
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut res = self.header.to_bytes();
        for (id, val) in &self.data {
            res.extend(Record { id: *id, val: *val }.to_bytes());
        }
        res
    }

    pub fn to_page_bytes(&self) -> Vec<u8> {
        let mut res = self.header.to_bytes();
        for (id, val) in &self.data {
            res.extend(Record { id: *id, val: *val }.to_bytes());
        }
        if res.len() > PAGE_SIZE {
            panic!("The page is larger than the page boundary");
        }
        let bytes_to_pad = PAGE_SIZE - res.len();
        res.extend(vec![0; bytes_to_pad]);
        res
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let header_bytes: &[u8; 12] = bytes[0..12].try_into().unwrap();

        let header = PageHeader::from_bytes(header_bytes);
        let mut data = vec![];

        let mut offset = PageHeader::size();

        for _ in 0..header.count {
            let record_bytes: &[u8; 8] = bytes[offset..offset + 8].try_into().unwrap();
            let record = Record::from_bytes(record_bytes);
            data.push(record);
            offset += 8;
        }

        Page::new(&data)
    }

    pub fn size(&self) -> usize {
        std::mem::size_of::<Record>() * self.data.len() + PageHeader::size()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn split(&self) -> (Self, Self) {
        let len = self.len();
        let mid = len / 2;
        let vec_data: Vec<Record> = self
            .data
            .clone()
            .into_iter()
            .map(|(id, val)| Record { id, val })
            .collect();
        let (head, tail) = vec_data.split_at(mid);

        (Self::new_dirty(head), Self::new_dirty(tail))
    }

    pub fn merge(&mut self, other: Page) {
        let mut new_data = self.data.clone();
        new_data.extend(other.data);
        let vec_data: Vec<Record> = new_data
            .into_iter()
            .map(|(id, val)| Record { id, val })
            .collect();
        *self = Self::new_dirty(&vec_data)
    }

    pub fn get(&self, id: NonZeroU32) -> Option<Record> {
        self.data.get(&id).map(|val| Record { id, val: *val })
    }

    pub fn insert(&mut self, record: Record) {
        self.header.start = self.header.start.min(record.id);
        self.header.end = self.header.end.max(record.id);
        self.dirty = true;
        self.data.insert(record.id, record.val);
        self.header.count = self.data.len() as u32;
    }

    pub fn remove(&mut self, id: NonZeroU32) -> Option<u32> {
        match self.data.remove(&id) {
            Some(val) => {
                self.header.start = match self.data.first_key_value() {
                    Some((id, _)) => *id,
                    None => NonZeroU32::MIN,
                };
                self.header.end = match self.data.last_key_value() {
                    Some((id, _)) => *id,
                    None => NonZeroU32::MIN,
                };
                self.header.count = self.data.len() as u32;
                self.dirty = true;
                Some(val)
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZero;

    use super::*;
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;

    impl Arbitrary for Record {
        fn arbitrary(g: &mut Gen) -> Self {
            let id = u32::arbitrary(g);
            let val = u32::arbitrary(g);
            let mut bytes: [u8; 8] = [0; 8];
            for (i, b) in id.to_le_bytes().into_iter().enumerate() {
                bytes[i] = b;
            }
            for (i, b) in val.to_le_bytes().into_iter().enumerate() {
                bytes[i + 4] = b;
            }

            Record::from_bytes(&bytes)
        }
    }

    impl Arbitrary for Page {
        fn arbitrary(g: &mut Gen) -> Self {
            let record_count = u8::arbitrary(g);
            let data: Vec<_> = (0..record_count).map(|_| Record::arbitrary(g)).collect();

            Self::new(&data)
        }
    }

    #[test]
    fn split() {
        let data = &[
            Record {
                id: NonZeroU32::new(3).unwrap(),
                val: 30,
            },
            Record {
                id: NonZeroU32::new(4).unwrap(),
                val: 40,
            },
            Record {
                id: NonZeroU32::new(1).unwrap(),
                val: 10,
            },
            Record {
                id: NonZeroU32::new(2).unwrap(),
                val: 20,
            },
        ];

        let page = Page::new(data);
        let (head, tail) = page.split();
        assert_eq!(
            head,
            Page::new_dirty(&[
                Record {
                    id: NonZero::new(1).unwrap(),
                    val: 10
                },
                Record {
                    id: NonZero::new(2).unwrap(),
                    val: 20
                },
            ])
        );
        assert_eq!(
            tail,
            Page::new_dirty(&[
                Record {
                    id: NonZero::new(3).unwrap(),
                    val: 30
                },
                Record {
                    id: NonZero::new(4).unwrap(),
                    val: 40
                }
            ])
        );
    }

    #[test]
    fn merge() {
        let data = &[
            Record {
                id: NonZeroU32::new(3).unwrap(),
                val: 30,
            },
            Record {
                id: NonZeroU32::new(4).unwrap(),
                val: 40,
            },
            Record {
                id: NonZeroU32::new(1).unwrap(),
                val: 10,
            },
            Record {
                id: NonZeroU32::new(2).unwrap(),
                val: 20,
            },
        ];
        let (head, tail) = data.split_at(data.len() / 2);

        let mut head = Page::new_dirty(head);
        head.merge(Page::new(tail));
        assert_eq!(head, Page::new_dirty(data));
    }

    #[test]
    fn get() {
        let data = vec![
            Record {
                id: NonZeroU32::new(3).unwrap(),
                val: 30,
            },
            Record {
                id: NonZeroU32::new(1).unwrap(),
                val: 10,
            },
            Record {
                id: NonZeroU32::new(2).unwrap(),
                val: 20,
            },
        ];
        let id_to_get = NonZero::new(3).unwrap();

        let page = Page::new(&data);
        let item = page.get(id_to_get);
        assert_eq!(
            item,
            Some(Record {
                id: NonZero::new(3).unwrap(),
                val: 30
            })
        );
    }

    #[test]
    fn insert() {
        let mut data = vec![
            Record {
                id: NonZeroU32::new(3).unwrap(),
                val: 30,
            },
            Record {
                id: NonZeroU32::new(1).unwrap(),
                val: 10,
            },
            Record {
                id: NonZeroU32::new(2).unwrap(),
                val: 20,
            },
        ];
        let record_to_add = Record {
            id: NonZeroU32::new(4).unwrap(),
            val: 40,
        };

        let mut head = Page::new(&data);
        head.insert(record_to_add);
        data.push(record_to_add);

        assert_eq!(head, Page::new_dirty(&data));
    }

    #[test]
    fn remove() {
        let mut data = vec![
            Record {
                id: NonZeroU32::new(3).unwrap(),
                val: 30,
            },
            Record {
                id: NonZeroU32::new(1).unwrap(),
                val: 10,
            },
            Record {
                id: NonZeroU32::new(2).unwrap(),
                val: 20,
            },
            Record {
                id: NonZeroU32::new(4).unwrap(),
                val: 40,
            },
        ];

        let id_to_remove = NonZero::new(4).unwrap();

        let mut head = Page::new(&data);
        head.remove(id_to_remove);
        data.pop();
        assert_eq!(head, Page::new_dirty(&data));
    }

    #[test]
    fn serde() {
        let data = &[
            Record {
                id: NonZeroU32::new(3).unwrap(),
                val: 30,
            },
            Record {
                id: NonZeroU32::new(4).unwrap(),
                val: 40,
            },
            Record {
                id: NonZeroU32::new(1).unwrap(),
                val: 10,
            },
            Record {
                id: NonZeroU32::new(2).unwrap(),
                val: 20,
            },
        ];

        let page = Page::new(data);

        assert_eq!(Page::from_bytes(&page.to_bytes()), page);
    }

    #[quickcheck]
    fn fuzz_page_new(records: Vec<Record>) -> bool {
        if records.len() >= u32::MAX as usize {
            return true;
        }
        let page = Page::new(&records);
        Page::from_bytes(&page.to_bytes()) == page
    }

    #[quickcheck]
    fn fuzz_page_split_merge(records: Vec<Record>) -> bool {
        if records.len() >= u32::MAX as usize {
            return true;
        }
        let page = Page::new_dirty(&records);
        let (mut head, tail) = page.split();
        head.merge(tail);
        head == page
    }
}
