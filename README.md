# DB

A toy Key-Value DB I wrote in rust. Data is persisted to disk, and the
in-memory cache only reads pages it needs to read for tuples.

## Limitations/Todos

- There's no schema customization, just a u32 for id, and u32 for value.
- There's only one table per database.
- Currently all pages are always marked as dirty so they are always
  resaved. This needs to be changed so only dirty pages are resaved.
- Since data is stored sorted on disk, if a new page is created, all
  pages afterwards have to be marked as dirty and resaved, since they
  would move down in the file.
