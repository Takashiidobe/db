use std::{
    collections::BTreeSet,
    fs::File,
    io::{BufWriter, Seek as _, SeekFrom, Write as _},
};

use crate::page::Page;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct DBFile {
    pages: BTreeSet<Page>,
}

impl DBFile {
    pub fn serialize(&self, file_name: &str) {
        let f = File::create(file_name).unwrap();
        let mut f = BufWriter::new(f);
        for (i, page) in self.pages.iter().enumerate() {
            if page.dirty {
                let pos = SeekFrom::Start(i as u64 * 4096);
                let _ = f.seek(pos);
                let _ = f.write_all(&page.to_page_bytes());
            }
        }
    }

    // TODO: deserialize, take an array and read the header and data and make a DB File from it
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::*;

    #[test]
    fn files() {
        let data = vec![
            DiskRecord { id: 1, val: 10 },
            DiskRecord { id: 2, val: 20 },
            DiskRecord { id: 3, val: 30 },
            DiskRecord { id: 4, val: 40 },
        ];

        let page1 = Page::new(&data);

        let mut data = data;

        data.pop();
        data.push(DiskRecord { id: 4, val: 50 });

        let page2 = Page::new(&data);

        let pages = BTreeSet::from_iter(vec![page2, page1]);

        let file = DBFile { pages };

        assert_eq!(file, DBFile::default());
    }

    #[test]
    fn write() {
        let mut data = vec![];

        for i in 1..1000 {
            data.push(DiskRecord { id: i, val: i });
        }

        let page = Page::new(&data);
        let (head, tail) = page.split();

        let pages = BTreeSet::from_iter(vec![head, tail]);

        let file = DBFile { pages };
        dbg!(&file.pages);

        file.serialize("file.out");

        assert!(true == false)
    }
}
