use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufWriter, Seek as _, SeekFrom, Write as _},
};

use crate::page::{DiskRecord, Page, PageHeader, PAGE_SIZE};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DB {
    pub pages: BTreeSet<Page>,
    pub file_name: String,
}

impl DB {
    pub fn new(file_name: &str) -> Self {
        Self {
            file_name: file_name.to_string(),
            pages: BTreeSet::new(),
        }
    }

    pub fn serialize(&self) {
        let f = File::create(&self.file_name).unwrap();
        let mut f = BufWriter::new(f);
        for (i, page) in self.pages.iter().enumerate() {
            if page.dirty {
                let pos = SeekFrom::Start(i as u64 * 4096);
                let _ = f.seek(pos);
                let _ = f.write_all(&page.to_page_bytes());
            }
        }
    }

    pub fn get(&self, id: u32) -> Option<u32> {
        // if empty, return None
        if self.pages.is_empty() {
            return None;
        }

        // otherwise, find the page where start <= id <= end
        let mut range = self
            .pages
            .range(
                Page {
                    header: PageHeader {
                        end: id,
                        start: u32::MIN,
                        count: u32::MIN,
                    },
                    dirty: false,
                    data: BTreeMap::new(),
                }..=Page {
                    header: PageHeader {
                        end: u32::MAX,
                        start: id,
                        count: u32::MAX,
                    },
                    dirty: true,
                    data: BTreeMap::new(),
                },
            )
            .rev();

        match range.next() {
            Some(next_page) => next_page.get(id).map(|DiskRecord { val, .. }| val),
            None => None,
        }
    }

    pub fn insert(&mut self, id: u32, val: u32) {
        // in case of an empty db
        if self.pages.is_empty() {
            let new_page = Page::new(&[DiskRecord { id, val }]);
            self.pages.insert(new_page);
            return;
        }

        // handle prepend
        if let Some(first_page) = self.pages.first() {
            dbg!(&first_page.size());
            if id < first_page.header.start {
                let mut first_page = self.pages.pop_first().unwrap();
                first_page.insert(DiskRecord { id, val });
                self.pages.insert(first_page);

                // split page that is too big
                if let Some(first_page) = self.pages.first() {
                    if first_page.size() > PAGE_SIZE {
                        let (head, tail) = first_page.split();
                        self.pages.pop_first();
                        self.pages.insert(head);
                        self.pages.insert(tail);
                    }
                }
                return;
            }
        }

        // handle append
        if let Some(last_page) = self.pages.last() {
            dbg!(&last_page.size());
            if id > last_page.header.end {
                let mut last_page = self.pages.pop_last().unwrap();
                last_page.insert(DiskRecord { id, val });
                self.pages.insert(last_page);
                // split page that is too big
                if let Some(last_page) = self.pages.last() {
                    if last_page.size() > PAGE_SIZE {
                        let (head, tail) = last_page.split();
                        self.pages.pop_last();
                        self.pages.insert(head);
                        self.pages.insert(tail);
                    }
                }
                return;
            }
        }

        let mut range = self
            .pages
            .range(
                Page {
                    header: PageHeader {
                        end: id,
                        start: u32::MIN,
                        count: u32::MIN,
                    },
                    dirty: false,
                    data: BTreeMap::new(),
                }..=Page {
                    header: PageHeader {
                        end: u32::MAX,
                        start: id,
                        count: u32::MAX,
                    },
                    dirty: true,
                    data: BTreeMap::new(),
                },
            )
            .rev();

        let next_page = range.next().unwrap();
        let mut fetched_page: Page = next_page.clone();

        self.pages.remove(&fetched_page);
        fetched_page.insert(DiskRecord { id, val });
        dbg!(&fetched_page.size());

        if fetched_page.size() > PAGE_SIZE {
            let (head, tail) = fetched_page.split();
            self.pages.insert(head);
            self.pages.insert(tail);
        } else {
            self.pages.insert(fetched_page);
        }
    }
}

pub fn deserialize(bytes: Vec<u8>) -> BTreeSet<Page> {
    if bytes.len() % 4096 != 0 {
        panic!("Attempting to deserialize from non-page aligned byte array");
    }

    let mut pages: Vec<Page> = vec![];

    for i in 0..(bytes.len() / 4096) {
        pages.push(Page::from_bytes(&bytes[i * 4096..(i + 1) * 4096]));
    }

    BTreeSet::from_iter(pages)
}

impl Drop for DB {
    fn drop(&mut self) {
        self.serialize()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use quickcheck_macros::quickcheck;

    use super::*;
    use crate::page::*;

    #[test]
    fn read_write() {
        let mut data = vec![];

        for i in 1..10 {
            data.push(DiskRecord { id: i, val: i });
        }

        let page = Page::new(&data);
        let (head, tail) = page.split();

        let pages = BTreeSet::from_iter(vec![head, tail]);

        let file = DB {
            pages,
            file_name: "read_write.out".to_string(),
        };

        file.serialize();

        let bytes = fs::read("read_write.out").unwrap();

        assert_eq!(deserialize(bytes), file.pages)
    }

    #[test]
    fn insert() {
        let mut data = vec![];

        for i in 1..=2 {
            data.push(DiskRecord { id: i, val: i });
        }

        for i in 4..=5 {
            data.push(DiskRecord { id: i, val: i });
        }

        let page = Page::new(&data);
        let (head, tail) = page.split();

        let pages = BTreeSet::from_iter(vec![head, tail]);

        let mut db = DB {
            pages,
            file_name: "insert.out".to_string(),
        };
        db.insert(3, 3);
        assert_eq!(
            db.pages,
            BTreeSet::from_iter(vec![
                Page {
                    header: PageHeader {
                        end: 2,
                        start: 1,
                        count: 2
                    },
                    data: BTreeMap::from([(1, 1), (2, 2)]),
                    dirty: true
                },
                Page {
                    header: PageHeader {
                        end: 5,
                        start: 3,
                        count: 3
                    },
                    data: BTreeMap::from([(3, 3), (4, 4), (5, 5)]),
                    dirty: true
                },
            ])
        );
    }

    #[test]
    fn get() {
        let mut data = vec![];

        for i in 1..=10 {
            data.push(DiskRecord { id: i, val: i });
        }

        let page = Page::new(&data);

        let pages = BTreeSet::from_iter(vec![page]);

        let db = DB {
            pages,
            file_name: "insert.out".to_string(),
        };

        assert_eq!(db.get(3), Some(3));
    }

    #[test]
    fn insert_loop() {
        let pages = BTreeSet::new();

        let mut db = DB {
            pages,
            file_name: "insert_loop.out".to_string(),
        };

        let mut iter = vec![];

        for i in 1..=510 {
            iter.push((i, i));
            db.insert(i, i);
        }

        assert_eq!(
            db.pages,
            BTreeSet::from_iter(vec![Page {
                header: PageHeader {
                    end: 510,
                    start: 1,
                    count: 510,
                },
                data: BTreeMap::from_iter(iter),
                dirty: true
            }])
        );
    }

    #[quickcheck]
    fn fuzz_db_inserts(records: Vec<(u32, u32)>) -> bool {
        let mut db = DB {
            pages: BTreeSet::new(),
            file_name: "fuzz_db_inserts.out".to_string(),
        };

        for (id, val) in records {
            db.insert(id, val);
        }

        true
    }

    #[quickcheck]
    fn fuzz_db_get(records: BTreeSet<u32>) -> bool {
        let mut db = DB {
            pages: BTreeSet::new(),
            file_name: "fuzz_db_get.out".to_string(),
        };

        for val in &records {
            db.insert(*val, *val);
        }

        records
            .into_iter()
            .map(|id| db.get(id) == Some(id))
            .all(|f| f)
    }
}
