# DB

A toy Key-Value DB I wrote in rust. Data is persisted to disk, and the
in-memory cache only reads pages it needs to read for tuples.

There's a WAL which acts as a cache, and persists insertions/deletions
to disk without needing to affect the individual pages.

## Limitations/Todos

- There's no schema customization, just a u32 for id, and u32 for value.
- There's only one table per database.
