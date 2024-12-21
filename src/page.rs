use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageHeader {
    pub end: u32,
    pub start: u32,
    pub count: u32,
}

pub fn bytes_to_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().unwrap())
}

impl PageHeader {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut res = self.end.to_le_bytes().to_vec();
        res.extend(self.start.to_le_bytes());
        res.extend(self.count.to_le_bytes());
        res
    }

    pub fn from_bytes(bytes: &[u8; 12]) -> Self {
        let end = bytes_to_u32(&bytes[0..4]);
        let start = bytes_to_u32(&bytes[4..8]);
        let count = bytes_to_u32(&bytes[8..12]);

        Self { end, start, count }
    }

    pub fn size() -> usize {
        12
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct DiskRecord {
    pub id: u32,
    pub val: u32,
}

impl DiskRecord {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut res = self.id.to_le_bytes().to_vec();
        res.extend(self.val.to_le_bytes());
        res
    }

    pub fn from_bytes(bytes: &[u8; 8]) -> Self {
        let id = bytes_to_u32(&bytes[0..4]);
        let val = bytes_to_u32(&bytes[4..8]);

        Self { id, val }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Page {
    pub header: PageHeader,
    pub data: BTreeMap<u32, u32>,
    pub dirty: bool,
}

pub const PAGE_SIZE: usize = 4096;

impl Page {
    pub fn new(data: &[DiskRecord]) -> Self {
        let data = BTreeMap::from_iter(data.iter().map(|record| (record.id, record.val)));

        let start = *data.first_key_value().unwrap_or((&0, &0)).0;
        let end = *data.last_key_value().unwrap_or((&0, &0)).0;

        let header = PageHeader {
            count: data.len() as u32,
            start,
            end,
        };

        Page {
            header,
            data,
            dirty: true,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut res = self.header.to_bytes();
        for (id, val) in &self.data {
            res.extend(DiskRecord { id: *id, val: *val }.to_bytes());
        }
        res
    }

    pub fn to_page_bytes(&self) -> Vec<u8> {
        let mut res = self.header.to_bytes();
        for (id, val) in &self.data {
            res.extend(DiskRecord { id: *id, val: *val }.to_bytes());
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
            let record = DiskRecord::from_bytes(record_bytes);
            data.push(record);
            offset += 8;
        }

        Page::new(&data)
    }

    pub fn size(&self) -> usize {
        std::mem::size_of::<DiskRecord>() * self.data.len() + PageHeader::size()
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
        let vec_data: Vec<DiskRecord> = self
            .data
            .clone()
            .into_iter()
            .map(|(id, val)| DiskRecord { id, val })
            .collect();
        let (head, tail) = vec_data.split_at(mid);

        (Self::new(head), Self::new(tail))
    }

    pub fn merge(&mut self, other: Page) {
        let mut new_data = self.data.clone();
        new_data.extend(other.data);
        let vec_data: Vec<DiskRecord> = new_data
            .into_iter()
            .map(|(id, val)| DiskRecord { id, val })
            .collect();
        *self = Self::new(&vec_data)
    }

    pub fn get(&self, id: u32) -> Option<DiskRecord> {
        self.data.get(&id).map(|val| DiskRecord { id, val: *val })
    }

    pub fn insert(&mut self, record: DiskRecord) {
        self.header.count += 1;
        self.header.start = self.header.start.min(record.id);
        self.header.end = self.header.end.max(record.id);
        self.dirty = true;
        self.data.insert(record.id, record.val);
    }

    pub fn remove(&mut self, id: u32) -> Option<u32> {
        match self.data.remove(&id) {
            Some(val) => {
                self.header.count -= 1;
                self.header.start = *self.data.first_key_value().unwrap_or((&0, &0)).0;
                self.header.end = *self.data.last_key_value().unwrap_or((&0, &0)).0;
                self.dirty = true;
                Some(val)
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;

    impl Arbitrary for DiskRecord {
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

            DiskRecord::from_bytes(&bytes)
        }
    }

    impl Arbitrary for Page {
        fn arbitrary(g: &mut Gen) -> Self {
            let record_count = u8::arbitrary(g);
            let data: Vec<_> = (0..record_count)
                .map(|_| DiskRecord::arbitrary(g))
                .collect();

            Self::new(&data)
        }
    }

    #[test]
    fn split() {
        let data = &[
            DiskRecord { id: 1, val: 10 },
            DiskRecord { id: 2, val: 20 },
            DiskRecord { id: 4, val: 40 },
            DiskRecord { id: 3, val: 30 },
        ];

        let page = Page::new(data);
        let (head, tail) = page.split();
        assert_eq!(
            head,
            Page::new(&[DiskRecord { id: 1, val: 10 }, DiskRecord { id: 2, val: 20 },])
        );
        assert_eq!(
            tail,
            Page::new(&[DiskRecord { id: 3, val: 30 }, DiskRecord { id: 4, val: 40 }])
        );
    }

    #[test]
    fn merge() {
        let data = &[
            DiskRecord { id: 1, val: 10 },
            DiskRecord { id: 2, val: 20 },
            DiskRecord { id: 4, val: 40 },
            DiskRecord { id: 3, val: 30 },
        ];
        let (head, tail) = data.split_at(data.len() / 2);

        let mut head = Page::new(head);
        head.merge(Page::new(tail));
        assert_eq!(head, Page::new(data));
    }

    #[test]
    fn get() {
        let data = vec![
            DiskRecord { id: 1, val: 10 },
            DiskRecord { id: 2, val: 20 },
            DiskRecord { id: 3, val: 30 },
        ];
        let id_to_get = 3;

        let page = Page::new(&data);
        let item = page.get(id_to_get);
        assert_eq!(item, Some(DiskRecord { id: 3, val: 30 }));
    }

    #[test]
    fn insert() {
        let mut data = vec![
            DiskRecord { id: 1, val: 10 },
            DiskRecord { id: 2, val: 20 },
            DiskRecord { id: 3, val: 30 },
        ];
        let record_to_add = DiskRecord { id: 4, val: 40 };

        let mut head = Page::new(&data);
        head.insert(record_to_add);
        data.push(record_to_add);
        assert_eq!(head, Page::new(&data));
    }

    #[test]
    fn remove() {
        let mut data = vec![
            DiskRecord { id: 1, val: 10 },
            DiskRecord { id: 2, val: 20 },
            DiskRecord { id: 3, val: 30 },
            DiskRecord { id: 4, val: 40 },
        ];
        let id_to_remove = 4;

        let mut head = Page::new(&data);
        head.remove(id_to_remove);
        data.pop();
        assert_eq!(head, Page::new(&data));
    }

    #[test]
    fn serde() {
        let data = &[
            DiskRecord { id: 3, val: 30 },
            DiskRecord { id: 4, val: 40 },
            DiskRecord { id: 1, val: 10 },
            DiskRecord { id: 2, val: 20 },
        ];

        let page = Page::new(data);

        assert_eq!(Page::from_bytes(&page.to_bytes()), page);
    }

    #[quickcheck]
    fn fuzz_page_new(records: Vec<DiskRecord>) -> bool {
        if records.len() >= u32::MAX as usize {
            return true;
        }
        let page = Page::new(&records);
        Page::from_bytes(&page.to_bytes()) == page
    }

    #[quickcheck]
    fn fuzz_page_split_merge(records: Vec<DiskRecord>) -> bool {
        if records.len() >= u32::MAX as usize {
            return true;
        }
        let page = Page::new(&records);
        let (mut head, tail) = page.split();
        head.merge(tail);
        head == page
    }
}
