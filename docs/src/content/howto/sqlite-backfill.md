---
title: "Backfill SQLite flow storage"
---

# Backfill SQLite flow storage

This personal-fork tool imports closed native `.mitm.zst` archives into the
opt-in SQLite storage format. It is an explicit, resumable command, not a startup
action. It does not change `.mitm` recording, enable SQLite recording, evict live
flows, or add historical browsing to mitmweb.

## Usage

Run from the repository checkout. First select files and check paths, database
compatibility, readability, and available disk space without importing:

```sh
uv run python -m mitmproxy.tools.sqlite_backfill \
  --directory "$HOME/.mitmproxy" \
  --database "$HOME/.mitmproxy/flows.sqlite3" \
  --from 2026-10-30_10 \
  --through 2026-11-13_08 \
  --dry-run
```

When ready, omit `--dry-run` to import:

```sh
uv run python -m mitmproxy.tools.sqlite_backfill \
  --directory "$HOME/.mitmproxy" \
  --database "$HOME/.mitmproxy/flows.sqlite3" \
  --from 2026-10-30_10 \
  --through 2026-11-13_08
```

Both filename timestamps are inclusive. Only files named
`flows-YYYY-MM-DD_HH.mitm.zst` or `flows-YYYY-MM-DD_HH.mitm-N.zst`, with a positive
numeric collision suffix `N`, are selected. Missing hours are normal. Raw `.mitm`
files, unrelated `.zst` files, subdirectories, and symlinked archives are not
imported. A matching non-regular file is an error, not a silent skip.

Use only closed archives that remain immutable throughout a run. This is a
pre-cutover tool: do not run it against a database concurrently used by the live
SQLite recorder. Existing independently recorded rows are protected, but a live
write queued behind a backfill insert can encounter the recorder's normal
changed-ID conflict behavior. The tool does not stop or configure the recorder.

Rerun the same command to resume. Advance `--through`, move `--from` earlier, or
change `--directory` while keeping `--database` to add more archives. None of these
argument changes deletes previously stored flows or manifest entries.

Dry runs do not decompress archives, validate their contents, create the target
database, or create import tables. They use the storage layer's permission checks,
which can tighten an existing database and its sidecars to mode `0600`.

## Resume and duplicate snapshots

Each accepted flow is committed separately using the existing checksummed zstd
level 3 payload format, SQLite WAL, and `synchronous=FULL`. Original IDs and native
state are preserved. No archive-wide decompressed file or flow list is created.
The existing `flows` schema and payload version remain unchanged; two additive
tables hold the import manifest and per-flow backfill provenance.

An indexed flow-ID lookup does not fetch the existing payload BLOB. A row without
backfill provenance is always skipped and never overwritten, even if the archive
contains a different snapshot with that ID.

For backfill-owned rows, the deterministically later source snapshot wins. Source
order is filename timestamp, numeric suffix (unsuffixed first), canonical source
path, source revision, then zero-based record position. The path is a stable
tie-breaker for equal timestamps and suffixes from different directories. A
changed archive increments that path's persistent revision. This ordering does
not depend on which run happens first. A later snapshot replaces the complete
native state and metadata transactionally while preserving the insertion
sequence; bodies and headers are not merged. Older conflicting snapshots are
reported and the newer stored snapshot is retained. Identical snapshots are
skipped, with provenance advanced when appropriate. A different state at the
exact same registered source position is an error.

The manifest identifies a source by canonical path, size, nanosecond modification
and change times, device, and inode. An unchanged completed source is skipped
without decompression. Completion is recorded only after every native record
parses, all zstd frames and physical EOF verify, the source identity still matches,
and all flow transactions commit. Valid empty zstd frames complete with zero
flows. A valid zstd checksum alone does not establish valid native flow data.

Interrupted or failed files remain incomplete. Already committed flows remain
valid, and reruns rescan the incomplete file from its start using flow-ID and
provenance checks. Changed files are rescanned, not assumed complete. There is no
compressed byte-offset resume or deletion of rows absent from a changed source.

## Resources, failures, and progress

Decompression uses bounded chunks, but a complete individual flow must fit in
memory. The native parser materializes a record and its state; serialization,
compression, SQLite binding, and codec workspaces add per-flow memory. The
importer avoids an extra flow reconstruction and periodically collects native
reconstruction cycles so bodies do not accumulate across the archive. Declared
native record lengths are read incrementally, without preallocating missing data
from a corrupt length prefix. There is no benchmark-derived 16 MiB flow cap.
Available RAM and SQLite's BLOB/record limits still apply.

Fixture measurements on Linux with CPython 3.14.9 and SQLite 3.54.1 used one HTTP
flow per fresh importer process, with an incompressible response body:

| Body size | Peak traced Python allocations | Process peak RSS |
| --- | --- | --- |
| 1 MiB | 4.45 MiB | 57.5 MiB |
| 8 MiB | 32.5 MiB | 94.2 MiB |
| 32 MiB | 128.5 MiB | 214.3 MiB |

These are examples, not capacity guarantees. Traced allocations exclude some C
allocations; RSS includes the interpreter/import baseline. Memory scales with
individual flow size, not just the 64 KiB decompression chunk, and not with total
archive size. Large native records may require several times their body size in
RAM. No multi-gigabyte production archive was imported for these measurements.

Disk checks run before opening the database, during compressed reads, before
payload writes, and before marking a source complete. The reserve is 64 MiB plus
three times the larger of the new native record and an old compressed payload
being replaced, allowing for database growth and WAL/checkpoint work. This is
advisory headroom, not an archive-size forecast or a guarantee against concurrent
disk consumption or quotas. SQLite full, locked, corrupt, or incompatible errors
fail explicitly. The target is never recreated, truncated, or vacuumed.

The target's parent directory must already exist, be owned by the current user
or root, and not be group/world-writable. The directory chain must pass the storage
layer's trusted-directory checks. Database and sidecar files use restrictive
permissions. No decompressed temporary files are used, and source archives are
opened only for reading.

Progress reports include compressed bytes read within the current archive,
cumulative bytes, and committed flow counts about every two seconds while work
advances. A long individual parse/compression/commit can delay a report. The final
summary includes archives found/completed/skipped/failed, inserted and existing
flows, updated snapshots, newer snapshots retained, conflicts, bytes read, and
database size including any remaining WAL/SHM files. A manifest skip reads zero
compressed bytes. Captured bodies, headers, and parser exception contents are not
logged.

Ctrl+C stops after the current transaction, preserves committed data, closes the
database, and exits `130`. Invalid arguments exit `2`; archive, storage, and disk
errors exit `1`. Success exits `0`. Errors identify the database and current source
archive. Repair an invalid source or resolve a storage problem, then rerun; a
failed archive is never silently marked complete. The tool stops at the first
failure rather than skipping unreadable records.

## Ordering for future historical browsing

`SQLiteFlowReader.newest()` and `older()` paginate by **insertion sequence**, not
capture timestamp. Importing older captures into an existing database appends new
sequences; updating an imported snapshot preserves its original sequence. Thus
"newest inserted" is not necessarily "newest captured", even when each run reads
archives chronologically. Future historical UI work must account for this
explicitly. This tool does not redesign pagination or change mitmweb.
