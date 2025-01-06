# DB

A Key-Value DB I wrote in rust. Data is persisted to disk, and pages are
saved in sorted order on disk.

There's a WAL which acts as a cache, and persists insertions/deletions
to disk without needing to affect the individual pages.

## Architecture

This database features two parts, a sorted set of pages (which store a
set of (id, row) tuples), which are saved to a file, and a Write-Ahead
Log (WAL), which appends insertions and deletions to a file.

In the event the database crashes, as long as the last update
(insert/delete) was saved to the WAL, there won't be any data
corruption. On next startup, the DB will populate the WAL and apply the
updates to the database.

Because all pages are stored in sorted order on disk, the WAL acts as a
cache, where inserts go to first, without requiring reordering data on
disk. The DB also has a function, `sync`, which applies updates from the
WAL to the database and then writes that out to disk, only writing out
pages that were dirty or pages that have moved from their original
location in the file, and thus, have to be saved.

## Limitations/Todos

- There's only one table per database.

## Future Plans?

- Make multiple tables per database, which have to be named.
- Joins
- Transactions
- Indexes (to be done after transactions, since writes have to hit
  multiple tables on disk and be confirmed as one unit).
