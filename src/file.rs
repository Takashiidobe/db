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

    pub fn deserialize(bytes: Vec<u8>) -> Self {
        if bytes.len() % 4096 != 0 {
            panic!("Attempting to deserialize from non-page aligned byte array");
        }

        let mut pages: Vec<Page> = vec![];

        for i in 0..(bytes.len() / 4096) {
            pages.push(Page::from_bytes(&bytes[i * 4096..(i + 1) * 4096]));
        }

        let pages = BTreeSet::from_iter(pages);
        Self { pages }
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

        let file = DBFile { pages };

        file.serialize("file.out");

        let bytes = fs::read("file.out").unwrap();

        assert_eq!(DBFile::deserialize(bytes), file)
    }
}
