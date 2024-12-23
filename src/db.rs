use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Seek as _, SeekFrom, Write as _},
    num::NonZeroU32,
};

use crate::{record::Record, wal::WAL};

use crate::page::{Page, PageHeader, PAGE_SIZE};
use indexset::{BTreeSet, Range};

#[derive(Debug)]
pub struct DB {
    pub pages: BTreeSet<(Page, Option<usize>)>,
    pub file: File,
    pub wal: WAL,
    pub epoch: u64,
}

impl DB {
    pub fn new(file_name: &str) -> Self {
        let epoch = 1;
        let (db_file, wal_file) = Self::setup_files(file_name, epoch);
        Self {
            file: db_file,
            pages: BTreeSet::new(),
            wal: WAL {
                file: wal_file,
                records: BTreeMap::new(),
            },
            epoch,
        }
    }

    pub fn new_with_pages(pages: BTreeSet<(Page, Option<usize>)>, file_name: &str) -> Self {
        let epoch = 1;
        let (db_file, wal_file) = Self::setup_files(file_name, epoch);

        Self {
            file: db_file,
            pages,
            wal: WAL {
                file: wal_file,
                records: BTreeMap::new(),
            },
            epoch,
        }
    }

    fn setup_files(file_name: &str, epoch: u64) -> (File, File) {
        let db_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(format!("{file_name}.{epoch}.db"))
            .unwrap();
        let wal_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(format!("{file_name}.{epoch}.wal"))
            .unwrap();
        (db_file, wal_file)
    }

    pub fn sync(&mut self) -> bool {
        // apply all updates in wal to pages
        for (id, val) in self.wal.records.clone() {
            self.insert_to_page(id, val);
        }

        self.serialize();
        self.wal.records.clear();
        self.wal.file.set_len(0).is_ok()
    }

    pub fn serialize(&self) {
        let mut f = BufWriter::new(&self.file);
        for (i, page) in self.pages.iter().enumerate() {
            if page.0.dirty || page.1 != Some(i) {
                let pos = SeekFrom::Start((i * PAGE_SIZE) as u64);
                let _ = f.seek(pos);
                let _ = f.write_all(&page.0.to_page_bytes());
            }
        }
    }

    fn range_iter(&self, id: NonZeroU32) -> Range<(Page, Option<usize>)> {
        self.pages.range(
            (
                Page {
                    header: PageHeader {
                        end: id,
                        start: NonZeroU32::MIN,
                        count: u32::MIN,
                    },
                    dirty: false,
                    data: BTreeMap::new(),
                },
                None,
            )
                ..=(
                    Page {
                        header: PageHeader {
                            end: NonZeroU32::MAX,
                            start: id,
                            count: u32::MAX,
                        },
                        dirty: true,
                        data: BTreeMap::new(),
                    },
                    Some(usize::MAX),
                ),
        )
    }

    pub fn get(&self, id: NonZeroU32) -> Option<u32> {
        // check wal first
        if let Some(val) = self.wal.get(id) {
            return Some(val);
        }

        // if not in pages, return None
        if self.pages.is_empty() {
            return None;
        }

        // otherwise, find the page where start <= id <= end
        let mut range = self.range_iter(id);

        match range.next() {
            Some(next_page) => next_page.0.get(id).map(|Record { val, .. }| val),
            None => None,
        }
    }

    pub fn remove(&mut self, id: NonZeroU32) -> Option<u32> {
        // if in wal, remove from wal
        if let Some(val) = self.wal.remove(id) {
            return Some(val);
        }

        // if empty, return None
        if self.pages.is_empty() {
            return None;
        }

        // handle case when id is too small
        if let Some(first_page) = self.pages.first() {
            if id < first_page.0.header.start {
                return None;
            }
        }

        // handle case when id is too large
        if let Some(last_page) = self.pages.last() {
            if id > last_page.0.header.end {
                return None;
            }
        }

        // otherwise, find the page where start <= id <= end
        let mut range = self.range_iter(id);

        let next_page = range.next().unwrap();
        let mut fetched_page = next_page.clone();

        self.pages.remove(&fetched_page);
        let res = fetched_page.0.remove(id);

        // if the page still has items, readd it in
        if fetched_page.0.header.count != 0 {
            self.pages.insert(fetched_page);
        }

        res
    }

    pub fn insert(&mut self, id: NonZeroU32, val: u32) {
        // if in wal, insert into wal
        if self.wal.insert(id, val) {
            return;
        }

        self.insert_to_page(id, val)
    }

    fn insert_to_page(&mut self, id: NonZeroU32, val: u32) {
        // in case of an empty db
        if self.pages.is_empty() {
            let new_page = (Page::new_dirty(&[Record { id, val }]), None);
            self.pages.insert(new_page);
            return;
        }

        // handle prepend
        if let Some(first_page) = self.pages.first() {
            if id < first_page.0.header.start {
                let mut first_page = self.pages.pop_first().unwrap();
                first_page.0.insert(Record { id, val });
                self.pages.insert(first_page);

                // split page that is too big
                if let Some(first_page) = self.pages.first() {
                    if first_page.0.size() > PAGE_SIZE {
                        let (head, tail) = first_page.0.split();
                        self.pages.pop_first();
                        self.pages.insert((head, None));
                        self.pages.insert((tail, None));
                    }
                }
                return;
            }
        }

        // handle append
        if let Some(last_page) = self.pages.last() {
            if id > last_page.0.header.end {
                let mut last_page = self.pages.pop_last().unwrap();
                last_page.0.insert(Record { id, val });
                self.pages.insert(last_page);
                // split page that is too big
                if let Some(last_page) = self.pages.last() {
                    if last_page.0.size() > PAGE_SIZE {
                        let (head, tail) = last_page.0.split();
                        self.pages.pop_last();
                        self.pages.insert((head, None));
                        self.pages.insert((tail, None));
                    }
                }
                return;
            }
        }

        let mut range = self.range_iter(id);

        let next_page = range.next().unwrap();
        let mut fetched_page = next_page.clone();

        self.pages.remove(&fetched_page);
        fetched_page.0.insert(Record { id, val });

        if fetched_page.0.size() > PAGE_SIZE {
            let (head, tail) = fetched_page.0.split();
            self.pages.insert((head, None));
            self.pages.insert((tail, None));
        } else {
            self.pages.insert(fetched_page);
        }
    }
}

pub fn deserialize(bytes: Vec<u8>) -> BTreeSet<(Page, Option<usize>)> {
    assert!(bytes.len() % PAGE_SIZE == 0);

    let mut pages = vec![];

    for i in 0..(bytes.len() / PAGE_SIZE) {
        pages.push((
            Page::from_bytes(&bytes[i * PAGE_SIZE..(i + 1) * PAGE_SIZE]),
            Some(i),
        ));
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
    use std::{collections::HashSet, fs};

    use quickcheck_macros::quickcheck;

    use super::*;
    use crate::page::*;

    #[test]
    fn read_write() {
        let mut db = DB::new("tests/read_write");

        for i in 1..=5 {
            db.insert(NonZeroU32::new(i).unwrap(), i);
        }

        db.serialize();
        db.sync();

        let bytes = fs::read("tests/read_write.1.db").unwrap();

        let deserialized = deserialize(bytes);

        assert_eq!(
            deserialized,
            BTreeSet::from_iter(vec![Page {
                header: PageHeader {
                    end: NonZeroU32::new(5).unwrap(),
                    start: NonZeroU32::new(1).unwrap(),
                    count: 5
                },
                data: BTreeMap::from([
                    (NonZeroU32::new(1).unwrap(), 1),
                    (NonZeroU32::new(2).unwrap(), 2),
                    (NonZeroU32::new(3).unwrap(), 3),
                    (NonZeroU32::new(4).unwrap(), 4),
                    (NonZeroU32::new(5).unwrap(), 5)
                ]),
                dirty: false
            }])
        );
    }

    #[test]
    fn insert() {
        let mut data = vec![];

        for i in 1..=2 {
            data.push(Record {
                id: NonZeroU32::new(i).unwrap(),
                val: i,
            });
        }

        for i in 4..=5 {
            data.push(Record {
                id: NonZeroU32::new(i).unwrap(),
                val: i,
            });
        }

        let page = Page::new(&data);
        let (head, tail) = page.split();

        let pages = BTreeSet::from_iter(vec![head, tail]);

        let mut db = DB::new_with_pages(pages, "tests/insert");

        db.insert(3.try_into().unwrap(), 3);
        db.sync();

        assert_eq!(
            db.pages,
            BTreeSet::from_iter(vec![
                Page {
                    header: PageHeader {
                        end: NonZeroU32::new(2).unwrap(),
                        start: NonZeroU32::new(1).unwrap(),
                        count: 2
                    },
                    data: BTreeMap::from([
                        (NonZeroU32::new(1).unwrap(), 1),
                        (NonZeroU32::new(2).unwrap(), 2)
                    ]),
                    dirty: true
                },
                Page {
                    header: PageHeader {
                        end: NonZeroU32::new(5).unwrap(),
                        start: NonZeroU32::new(3).unwrap(),
                        count: 3
                    },
                    data: BTreeMap::from([
                        (NonZeroU32::new(3).unwrap(), 3),
                        (NonZeroU32::new(4).unwrap(), 4),
                        (NonZeroU32::new(5).unwrap(), 5),
                    ]),
                    dirty: true
                },
            ])
        );
    }

    #[test]
    fn get() {
        let mut data = vec![];

        for i in 1..=10 {
            data.push(Record {
                id: i.try_into().unwrap(),
                val: i,
            });
        }

        let page = Page::new(&data);

        let pages = BTreeSet::from_iter(vec![page]);

        let db = DB::new_with_pages(pages, "tests/insert");

        assert_eq!(db.get(3.try_into().unwrap()), Some(3));
    }

    #[test]
    fn insert_loop() {
        let mut db = DB::new("tests/insert_loop");

        let mut iter = vec![];

        for i in 1..=510 {
            iter.push((i.try_into().unwrap(), i));
            db.insert(i.try_into().unwrap(), i);
        }

        db.sync();

        assert_eq!(
            db.pages,
            BTreeSet::from_iter(vec![Page {
                header: PageHeader {
                    end: 510.try_into().unwrap(),
                    start: 1.try_into().unwrap(),
                    count: 510,
                },
                data: BTreeMap::from_iter(iter),
                dirty: true
            }])
        );
    }

    #[quickcheck]
    fn fuzz_db_inserts(records: Vec<(NonZeroU32, u32)>) -> bool {
        let mut db = DB::new("tests/fuzz_db_inserts");

        for (id, val) in records {
            db.insert(id, val);
        }

        true
    }

    #[quickcheck]
    fn fuzz_db_get(records: Vec<NonZeroU32>) -> bool {
        let records: HashSet<&NonZeroU32> = HashSet::from_iter(&records);
        let mut db = DB::new("tests/fuzz_db_get");

        for val in &records {
            db.insert(**val, val.get());
        }

        records
            .into_iter()
            .map(|id| db.get(*id) == Some(id.get()))
            .all(|f| f)
    }
}
