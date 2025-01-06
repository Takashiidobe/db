use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Seek as _, SeekFrom, Write as _},
    num::NonZeroU32,
};

use crate::{
    row::{RowType, RowVal, Schema},
    wal::WAL,
};

use crate::page::{Page, PageHeader, PAGE_SIZE};
use indexset::{BTreeSet, Range};

#[derive(Debug)]
pub struct DB {
    pub pages: BTreeSet<(Page, Option<usize>)>,
    pub file: File,
    pub wal: WAL,
    pub epoch: u64,
    pub schema: Schema,
}

impl DB {
    pub fn new(file_name: &str, schema: &[RowType]) -> Self {
        let epoch = 1;
        let (db_file, wal_file, schema_file) = Self::setup_files(file_name, epoch);
        Self {
            file: db_file,
            pages: BTreeSet::new(),
            wal: WAL {
                file: wal_file,
                records: BTreeMap::new(),
            },
            epoch,
            schema: Schema {
                schema: schema.to_vec(),
                file: schema_file,
            },
        }
    }

    pub fn new_with_pages(
        pages: BTreeSet<(Page, Option<usize>)>,
        file_name: &str,
        schema: &[RowType],
    ) -> Self {
        let epoch = 1;
        let (db_file, wal_file, schema_file) = Self::setup_files(file_name, epoch);

        Self {
            file: db_file,
            pages,
            wal: WAL {
                file: wal_file,
                records: BTreeMap::new(),
            },
            epoch,
            schema: Schema {
                schema: schema.to_vec(),
                file: schema_file,
            },
        }
    }

    fn setup_files(file_name: &str, epoch: u64) -> (File, File, File) {
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
        let schema_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(format!("{file_name}.{epoch}.schema"))
            .unwrap();
        (db_file, wal_file, schema_file)
    }

    pub fn sync(&mut self) -> bool {
        // apply all updates in wal to pages
        for (id, val) in self.wal.records.clone() {
            self.insert_to_page(id, &val);
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
        // truncation is required otherwise the page might have stale pages that have been deleted.
        let _ = self.file.set_len((self.pages.len() * PAGE_SIZE) as u64);
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
                    size: 0,
                    schema: vec![],
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
                        size: usize::MAX,
                        schema: vec![],
                    },
                    Some(usize::MAX),
                ),
        )
    }

    pub fn get(&self, id: NonZeroU32) -> Option<Vec<RowVal>> {
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
            Some(next_page) => next_page.0.get(id),
            None => None,
        }
    }

    pub fn remove(&mut self, id: NonZeroU32) -> Option<Vec<RowVal>> {
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

    pub fn insert(&mut self, id: NonZeroU32, val: &[RowVal]) {
        // if in wal, insert into wal
        if self.wal.insert(id, val) {
            return;
        }

        self.insert_to_page(id, val)
    }

    fn insert_to_page(&mut self, id: NonZeroU32, val: &[RowVal]) {
        let mut new_record = vec![RowVal::Id(id)];
        new_record.extend_from_slice(val);
        let row_size = val.iter().map(|x| x.size()).sum::<u16>() as usize + 4;

        // in case of an empty db
        if self.pages.is_empty() {
            let mut new_page = (Page::new_dirty(&[new_record], &self.schema.schema), None);
            new_page.0.size += row_size;
            self.pages.insert(new_page);
            return;
        }

        // handle prepend
        if let Some(first_page) = self.pages.first() {
            if id < first_page.0.header.start {
                let mut first_page = self.pages.pop_first().unwrap();
                first_page.0.size += row_size;
                first_page.0.insert(&new_record);
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
                last_page.0.size += row_size;
                last_page.0.insert(&new_record);
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
        fetched_page.0.insert(&new_record);

        if fetched_page.0.size() > PAGE_SIZE {
            let (head, tail) = fetched_page.0.split();
            self.pages.insert((head, None));
            self.pages.insert((tail, None));
        } else {
            self.pages.insert(fetched_page);
        }
    }
}

pub fn deserialize(bytes: Vec<u8>, schema: &[RowType]) -> BTreeSet<(Page, Option<usize>)> {
    assert!(bytes.len() % PAGE_SIZE == 0);

    let mut pages = vec![];

    for i in 0..(bytes.len() / PAGE_SIZE) {
        pages.push((
            Page::from_bytes(&bytes[i * PAGE_SIZE..(i + 1) * PAGE_SIZE], schema),
            Some(i),
        ));
    }

    BTreeSet::from_iter(pages)
}

impl Drop for DB {
    fn drop(&mut self) {
        self.serialize();
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs, num::NonZero};

    use insta::assert_yaml_snapshot as snapshot;

    use quickcheck_macros::quickcheck;

    use super::*;

    const DEFAULT_SCHEMA: &[RowType] = &[RowType::Id, RowType::U32];

    #[test]
    fn read_write() {
        let mut db = DB::new("tests/read_write", DEFAULT_SCHEMA);

        for i in 1..=5 {
            db.insert(NonZeroU32::new(i).unwrap(), &[RowVal::U32(i)]);
        }

        db.serialize();
        db.sync();

        let bytes = fs::read("tests/read_write.1.db").unwrap();

        let deserialized = deserialize(bytes, DEFAULT_SCHEMA);

        snapshot!(deserialized);
    }

    #[test]
    fn insert_loop() {
        let mut db = DB::new("tests/insert_loop", DEFAULT_SCHEMA);

        for i in 1..=510 {
            db.insert(NonZero::new(i).unwrap(), &[RowVal::U32(i)]);
        }

        db.sync();

        snapshot!(db.pages);
    }

    #[quickcheck]
    fn fuzz_db_get_insert(records: HashMap<NonZeroU32, u32>) -> bool {
        let mut db = DB::new("tests/fuzz_db_get", DEFAULT_SCHEMA);

        for (id, val) in &records {
            db.insert(*id, &[RowVal::U32(*val)]);
        }

        records
            .into_iter()
            .map(|(id, val)| db.get(id) == Some(vec![RowVal::U32(val)]))
            .all(|f| f)
    }
}
