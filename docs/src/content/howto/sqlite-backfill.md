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

Each accepted flow is committed separately using checksummed zstd level 3,
SQLite WAL, and `synchronous=FULL`. Original IDs and native state are preserved.
No archive-wide decompressed file or flow list is created. Normal records retain
the existing v1 payload format. Oversized records use the additive v2 format
described below. The existing `flows` table definition, application ID, and schema
version remain unchanged. Additive tables hold the manifest, backfill provenance,
original-record identities, and v2 chunks.

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

Original-record SHA-256 identities allow an incomplete source to resume even if
its records cross the v1/v2 size threshold between runs. Older v1 provenance only
hashed reconstructed, reserialized state. When resuming such a record, the importer
trusts its already committed position in the same verified immutable source,
retains the existing payload unchanged, and registers its original-record identity.
It does not reconstruct an enormous old payload merely to compare it.

Duplicate matching uses either the applicable provenance digest or the original
native-record digest. V1 provenance hashes normalized state; v2 provenance hashes
exact native bytes. Across different source positions, equivalent states with
different native encodings can therefore count as updates rather than duplicates.
This preserves a complete state, not a silently truncated approximation. A later
oversized update to a v1 row changes that ID to v2, so read it with `iter_raw()`.
Full semantic canonicalization of oversized records is not implemented.

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

Decompression uses bounded 64 KiB buffers. Records at most 32 MiB, including
native framing, still use `FlowReader` and v1 serialization. These records are
materialized, so their state, serialization, compression, and SQLite workspaces
consume several times the record size. Periodic collection prevents native
reconstruction cycles from retaining bodies across the archive.

Larger records are copied incrementally into an anonymous, mode-`0600` temporary
file in the database directory. A seek-based native scanner validates tnetstring
structure, UTF-8, and required current-version flow fields, extracts metadata,
and skips body bytes without materializing them. It supports version 21 HTTP,
WebSocket, TCP, UDP, and DNS state. Oversized legacy or future versions fail
explicitly: bounded migration is not implemented. This scanner is not a full
replacement for all `Flow.from_state()` semantic checks. Extracted scalar metadata
has a 1 MiB bound and nesting has a depth bound; exceeding either is an explicit
error, not truncation. Arbitrary body bytes and uninspected metadata keys do not
have that scalar limit.

The spool is compressed and stored in independent zstd chunks, normally 1 MiB of
raw bytes each. No complete oversized body, native record, or compressed payload
is constructed in memory. Chunk sizes are also bounded by the connection's SQLite
length limit. A forced-chunking regression uses an 8 MiB incompressible response
and asserts peak traced Python allocations below its body size; C codec and
SQLite allocations are not measured by that assertion. This is a bounded-buffer
design, not a fixed process-RSS guarantee. No multi-gigabyte production import was
run as part of validation.

Declared native lengths are checked without preallocating missing data. There is
no 16 MiB rejection limit, and the oversized format does not depend on raising
SQLite's ordinary single-BLOB limit.

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
permissions. A decompressed temporary file exists only for the current oversized
record, never for the whole archive. It is unlinked automatically, including on
failure or interruption. Source archives are opened only for reading.

Progress reports include compressed bytes read within the current archive,
cumulative bytes, and committed flow counts about every two seconds while work
advances. Oversized spooling and chunk writes also check interruption and report
progress. Metadata scanning or a slow commit can delay a report. The final
summary includes archives found/completed/skipped/failed, inserted and existing
flows, updated snapshots, newer snapshots retained, conflicts, bytes read, and
database size including any remaining WAL/SHM files. A manifest skip reads zero
compressed bytes. Captured bodies, headers, and parser exception contents are not
logged.

Ctrl+C preserves committed data, closes the database, and exits `130`. An
interrupted oversized write rolls back its entire transaction, including its row,
chunks, and provenance. Normal small-flow transactions finish before stopping.
Invalid arguments exit `2`; archive, storage, and disk errors exit `1`. Success
exits `0`. Errors identify the database and current source
archive. Repair an invalid source or resolve a storage problem, then rerun; a
failed archive is never silently marked complete. The tool stops at the first
failure rather than skipping unreadable records. Per-record failures include a
safe stage, zero-based record ordinal, lengths when known, and exception type.
Compression and SQLite binding failures identify their operation separately.

## Chunked format and complete incremental export

A v2 `flows` row has `payload_version=2`, `codec='zstd-chunks'`, no dictionary,
the total native byte length, and a 32-byte SHA-256 in `payload`. Its ordered
`flow_payload_chunks` rows hold raw lengths and independent checksummed zstd
frames. The row, chunks, metadata, and provenance publish in one transaction;
readers cannot see a partially committed flow. Failed updates restore the old
payload and chunks. No separate staging rows or orphan chunks are published.

`SQLiteFlowReader.get_flow()` retains its existing v1 behavior. It explicitly
rejects v2 and directs callers to `iter_raw(flow_id)`, rather than allocating an
enormous `Flow` or substituting missing bodies. `iter_raw()` works for both formats
and yields at most 64 KiB at a time under one consistent read transaction. For v2,
the output is the exact original native record, including its framing. For v1,
it is the native state stored by the existing serializer. The iterator verifies
chunk order, checksums, lengths, and the v2 whole-record digest.

Consume the iterator completely to establish integrity. It can yield a prefix
before detecting later corruption. Export to a private partial file and publish
it only after iteration succeeds, for example:

```python
import os
from pathlib import Path

from mitmproxy.io.sqlite import SQLiteFlowReader

target = Path("exported-flow.mitm")
partial = target.with_suffix(".mitm.partial")
with SQLiteFlowReader(Path.home() / ".mitmproxy/flows.sqlite3") as reader:
    with os.fdopen(os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as out:
        for chunk in reader.iter_raw(flow_id):
            out.write(chunk)
partial.replace(target)
```

On failure, the example deliberately leaves the `.partial` file unpublished.
Close an abandoned iterator explicitly to release its read transaction; long
exports can retain WAL pages while another connection writes. Do not join the
iterator into a single bytes object or pass a multi-gigabyte export to the ordinary
in-memory `FlowReader` if bounded memory is required.

Old binaries do not understand v2 and must not read or write databases containing
those rows. The ordinary live SQLite writer also explicitly reports a conflict for an existing v2
ID, even for a logically identical state; it never overwrites the chunked record.
This does not change v1 duplicate handling or enable chunking in live recording.

## Ordering for future historical browsing

`SQLiteFlowReader.newest()` and `older()` paginate by **insertion sequence**, not
capture timestamp. Importing older captures into an existing database appends new
sequences; updating an imported snapshot preserves its original sequence. Thus
"newest inserted" is not necessarily "newest captured", even when each run reads
archives chronologically. Future historical UI work must account for this
explicitly. This tool does not redesign pagination or change mitmweb.
