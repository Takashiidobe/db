use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufWriter, Seek as _, SeekFrom, Write as _},
    ops::Bound::Included,
};

use crate::page::{DiskRecord, Page, PageHeader, PAGE_SIZE};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct DB {
    pages: BTreeSet<Page>,
    file_name: String,
}

impl DB {
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

    pub fn insert(&mut self, id: u32, val: u32) {
        // in case of an empty db
        if self.pages.is_empty() {
            let mut new_page = Page::new(&[]);
            new_page.insert(DiskRecord { id, val });
            self.pages.insert(new_page);
            return;
        }

        // handle prepend
        if let Some(first_page) = self.pages.first() {
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

        if let Some(last_page) = self.pages.last() {
            // handle append to end
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

        // otherwise, find the page where start <= id <= end and index into it.
        let mut range = self
            .pages
            .range((
                Included(Page {
                    header: PageHeader {
                        start: 0,
                        end: 0,
                        count: 0,
                    },
                    dirty: false,
                    data: BTreeMap::new(),
                }),
                Included(Page {
                    header: PageHeader {
                        start: 0,
                        end: id,
                        count: u32::MAX,
                    },
                    dirty: false,
                    data: BTreeMap::new(),
                }),
            ))
            .rev();

        let mut fetched_page: Page = range.next().unwrap().clone();

        self.pages.remove(&fetched_page);
        fetched_page.insert(DiskRecord { id, val });
        self.pages.insert(fetched_page);
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
            file_name: "test.out".to_string(),
        };

        file.serialize();

        let bytes = fs::read("test.out").unwrap();

        assert_eq!(deserialize(bytes), file.pages)
    }

    #[test]
    fn insert() {
        let mut data = vec![];

        for i in 1..5 {
            data.push(DiskRecord { id: i, val: i });
        }
        for i in 6..11 {
            data.push(DiskRecord { id: i, val: i });
        }
        for i in 11..21 {
            data.push(DiskRecord { id: i, val: i });
        }

        let page = Page::new(&data);
        let (head, tail) = page.split();
        let (h1, h2) = head.split();
        let (t1, t2) = tail.split();

        let pages = BTreeSet::from_iter(vec![h1, h2, t1, t2]);

        let mut db = DB {
            pages,
            file_name: "file.out".to_string(),
        };
        db.insert(5, 5);
        assert_eq!(
            db.pages,
            BTreeSet::from_iter(vec![
                Page {
                    header: PageHeader {
                        end: 5,
                        start: 1,
                        count: 5
                    },
                    data: BTreeMap::from([(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)]),
                    dirty: true
                },
                Page {
                    header: PageHeader {
                        end: 10,
                        start: 6,
                        count: 5
                    },
                    data: BTreeMap::from_iter([(6, 6), (7, 7), (8, 8), (9, 9), (10, 10)]),
                    dirty: true
                },
                Page {
                    header: PageHeader {
                        end: 15,
                        start: 11,
                        count: 5
                    },
                    data: BTreeMap::from_iter([(11, 11), (12, 12), (13, 13), (14, 14), (15, 15)]),
                    dirty: true
                },
                Page {
                    header: PageHeader {
                        end: 20,
                        start: 16,
                        count: 5
                    },
                    data: BTreeMap::from_iter([(16, 16), (17, 17), (18, 18), (19, 19), (20, 20)]),
                    dirty: true
                }
            ])
        );
    }

    #[test]
    fn insert_loop() {
        let pages = BTreeSet::new();

        let mut db = DB {
            pages,
            file_name: "insert_loop.out".to_string(),
        };

        for i in 0..25 {
            db.insert(i, i);
        }

        assert_eq!(db.pages, BTreeSet::new());
    }
}
