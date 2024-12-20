use std::collections::BTreeSet;

use crate::page::Page;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct File {
    pages: BTreeSet<Page>,
}

// If we need to add a new tuple, with (id, val), we find the

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

        let file = File { pages };

        assert_eq!(file, File::default());
    }
}
