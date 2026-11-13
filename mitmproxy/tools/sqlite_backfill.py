"""Resumable, explicit import of closed native .mitm.zst archives.

Pagination remains insertion-sequence based, not capture-time based. Backfilling
older traffic later does not put it before already inserted traffic. See
docs/src/content/howto/sqlite-backfill.md for ordering and resource limits.
"""

import argparse
import gc
import io
import os
import re
import shutil
import signal
import sqlite3
import stat
import sys
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import BinaryIO

from mitmproxy.io import FlowReader
from mitmproxy.io.sqlite import _secure_directory
from mitmproxy.io.sqlite import _secure_path
from mitmproxy.io.sqlite import _validate
from mitmproxy.io.sqlite import StorageError
from mitmproxy.io.sqlite import zstd
from mitmproxy.io.sqlite_backfill import Source
from mitmproxy.io.sqlite_backfill import SQLiteBackfillStore

_FILENAME = re.compile(r"flows-(\d{4}-\d{2}-\d{2}_\d{2})\.mitm(?:-([1-9]\d*))?\.zst")
_REPORT_SECONDS = 2
_RESERVE_BYTES = 64 * 1024 * 1024
_CHUNK_BYTES = 64 * 1024
_GC_BYTES = 4 * 1024 * 1024


class _Interrupted(Exception):
    pass


def _timestamp(value: str) -> str:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d_%H")
        if parsed.strftime("%Y-%m-%d_%H") != value:
            raise ValueError
    except ValueError:
        raise argparse.ArgumentTypeError(
            "Expected a valid YYYY-MM-DD_HH timestamp"
        ) from None
    return value


@dataclass(frozen=True)
class Archive:
    path: Path
    timestamp: str
    suffix: int


def discover(directory: Path, first: str, through: str) -> list[Archive]:
    archives = []
    for path in directory.iterdir():
        match = _FILENAME.fullmatch(path.name)
        if not match:
            continue
        timestamp, suffix = match.groups()
        try:
            _timestamp(timestamp)
        except argparse.ArgumentTypeError:
            continue
        if first <= timestamp <= through:
            if not stat.S_ISREG(path.lstat().st_mode):
                raise StorageError(f"Not a regular archive: {path}")
            archives.append(Archive(path, timestamp, int(suffix or 0)))
    return sorted(archives, key=lambda a: (a.timestamp, a.suffix, str(a.path)))


def _check_space(database: Path, raw_size: int = 0) -> None:
    # Reserve space for both the WAL and checkpointed DB, with encoding overhead.
    available = shutil.disk_usage(database.parent).free
    required = _RESERVE_BYTES + 3 * raw_size
    if available < required:
        raise StorageError(
            f"Insufficient disk space: {available} bytes free, {required} required for DB/WAL headroom"
        )


def _database_size(database: Path) -> int:
    return sum(
        path.stat().st_size
        for path in (database, Path(f"{database}-wal"), Path(f"{database}-shm"))
        if path.exists()
    )


def _preflight(archives: list[Archive], database: Path) -> None:
    _secure_directory(database)
    if not os.access(database.parent, os.W_OK | os.X_OK):
        raise StorageError("Database directory is not writable")
    if _FILENAME.fullmatch(database.name):
        raise StorageError("Database path must not name a flow archive")
    for archive in archives:
        if database.exists() and os.path.samefile(archive.path, database):
            raise StorageError("Database path aliases a source archive")
        try:
            with archive.path.open("rb"):
                pass
        except OSError as e:
            raise StorageError(
                f"Cannot read archive {archive.path}: {e.strerror}"
            ) from None
    if database.exists() or database.is_symlink():
        if not os.access(database, os.W_OK):
            raise StorageError("Database is not writable")
        _secure_path(database, False)
        with closing(
            sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)
        ) as db:
            _validate(db, allow_empty=True)
            SQLiteBackfillStore.check_schema(db)
    _check_space(database)


@dataclass
class _Progress:
    found: int = 0
    completed: int = 0
    skipped: int = 0
    failed: int = 0
    flows: int = 0
    inserted: int = 0
    existing: int = 0
    updated: int = 0
    retained: int = 0
    conflicts: int = 0
    processed_bytes: int = 0
    archive_bytes: int = 0
    archive_size: int = 0
    archive: Path | None = None
    next_report: float = 0
    stopped: bool = False

    def interrupt(self, signum, frame) -> None:
        # SIGINT is deferred so the active SQLite transaction can finish.
        self.stopped = True

    def check_interrupted(self) -> None:
        if self.stopped:
            raise _Interrupted

    def counts(self) -> str:
        return (
            f"flows={self.flows} inserted={self.inserted} existing={self.existing} "
            f"updated={self.updated} newer_retained={self.retained} conflicts={self.conflicts}"
        )

    def report(self, force: bool = False) -> None:
        now = time.monotonic()
        if force or now >= self.next_report:
            print(
                f"archive={self.archive} bytes={self.archive_bytes}/{self.archive_size} "
                f"processed_bytes={self.processed_bytes} {self.counts()}",
                flush=True,
            )
            self.next_report = now + _REPORT_SECONDS


class _ZstdStream(io.RawIOBase):
    """Strict multi-frame decoding, bounded codec buffers, verified physical EOF."""

    def __init__(self, source: BinaryIO, progress: _Progress, database: Path):
        super().__init__()
        self.source = source
        self.progress = progress
        self.database = database
        self.decoder = zstd.ZstdDecompressor()
        self.verified = False
        self.decoded_bytes = 0

    def readable(self) -> bool:
        return True

    def _read_compressed(self) -> bytes:
        self.progress.check_interrupted()
        _check_space(self.database)
        chunk = self.source.read(_CHUNK_BYTES)
        self.progress.archive_bytes += len(chunk)
        self.progress.processed_bytes += len(chunk)
        self.progress.report()
        return chunk

    def readinto(self, buffer) -> int:
        while not self.verified:
            self.progress.check_interrupted()
            if self.decoder.eof:
                chunk = self.decoder.unused_data or self._read_compressed()
                if not chunk:
                    self.verified = True
                    break
                self.decoder = zstd.ZstdDecompressor()
            elif self.decoder.needs_input:
                chunk = self._read_compressed()
                if not chunk:
                    raise EOFError("Missing or truncated zstd frame")
            else:
                chunk = b""
            output = self.decoder.decompress(chunk, min(len(buffer), _CHUNK_BYTES))
            self.decoded_bytes += len(output)
            buffer[: len(output)] = output
            if output:
                return len(output)
        return 0


class _NativeStream(io.BufferedReader):
    def read(self, size: int | None = -1) -> bytes:
        if size is None or size < 0:
            raise StorageError("Unbounded native stream reads are not supported")
        if size <= _CHUNK_BYTES:
            return super().read(size)
        # Do not preallocate a corrupt record's declared length before reading it.
        output = io.BytesIO()
        while size:
            chunk = super().read(min(size, _CHUNK_BYTES))
            if not chunk:
                break
            output.write(chunk)
            size -= len(chunk)
        return output.getvalue()


def _import_archive(
    archive: Archive, store: SQLiteBackfillStore, progress: _Progress, database: Path
) -> None:
    progress.check_interrupted()
    progress.archive = archive.path
    progress.archive_bytes = 0
    with archive.path.open("rb") as source_file:
        source = Source.from_stat(
            archive.path,
            archive.timestamp,
            archive.suffix,
            os.fstat(source_file.fileno()),
        )
        progress.archive_size = source.size
        revision = store.start_source(source)
        if revision is None:
            progress.skipped += 1
            print(f"skip unchanged archive={archive.path}", flush=True)
            return
        progress.report(force=True)
        decoder = _ZstdStream(source_file, progress, database)
        flow_count = 0
        collected_at = 0
        with _NativeStream(decoder) as stream:
            # FlowReader also accepts HAR, which reads the entire input at once.
            prefix = stream.peek(1)[:1]
            if prefix and not prefix.isdigit():
                raise StorageError("Invalid native flow stream (HAR is not supported)")
            for f in FlowReader(stream).stream():
                progress.check_interrupted()
                result = store.write_flow(
                    f,
                    source,
                    revision,
                    flow_count,
                    check_space=lambda size: _check_space(database, size),
                )
                flow_count += 1
                progress.flows += 1
                setattr(progress, result.status, getattr(progress, result.status) + 1)
                progress.conflicts += int(result.conflicting)
                progress.report()
                del f
                # Native reconstruction can leave exception/frame cycles holding bodies.
                if decoder.decoded_bytes - collected_at >= _GC_BYTES:
                    gc.collect()
                    collected_at = decoder.decoded_bytes
            progress.check_interrupted()
            if not decoder.verified:
                raise StorageError("Compressed stream EOF was not verified")
        _check_space(database)
        store.finish_source(source, revision, flow_count)
        progress.completed += 1
        progress.report(force=True)
        progress.check_interrupted()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog="Archives must be closed and immutable during import. No source is deleted. Ctrl+C stops after the current transaction.",
    )
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--from", dest="first", type=_timestamp, required=True)
    parser.add_argument("--through", type=_timestamp, required=True)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Select files and check paths, schema and disk space; do not decompress or import",
    )
    args = parser.parse_args(argv)
    if args.first > args.through:
        parser.error("--from must not be after --through")
    directory = args.directory.expanduser().resolve()
    database = args.database.expanduser().absolute()
    progress = _Progress()
    exit_code = 0
    previous_handler = signal.signal(signal.SIGINT, progress.interrupt)
    try:
        archives = discover(directory, args.first, args.through)
        progress.found = len(archives)
        progress.check_interrupted()
        _preflight(archives, database)
        progress.check_interrupted()
        if args.dry_run:
            for archive in archives:
                progress.check_interrupted()
                print(
                    f"dry-run archive={archive.path} bytes={archive.path.stat().st_size}"
                )
            print(f"dry-run database={database}; no archives decompressed or imported")
        elif archives:
            with SQLiteBackfillStore(database) as store:
                for archive in archives:
                    try:
                        _import_archive(archive, store, progress, database)
                    except _Interrupted:
                        raise
                    except Exception:
                        progress.failed += 1
                        raise
        progress.check_interrupted()
    except (_Interrupted, KeyboardInterrupt):
        exit_code = 130
    except Exception as e:
        if isinstance(e, StorageError):
            detail = str(e)
        elif isinstance(e, sqlite3.Error):
            detail = f"SQLite error: {e}"
        elif isinstance(e, OSError):
            detail = e.strerror or type(e).__name__
        else:
            # Native parser exceptions may embed captured state, so never print them.
            detail = f"Invalid compressed/native flow data ({type(e).__name__})"
        print(
            f"Backfill failed database={database} archive={progress.archive}: {detail}",
            file=sys.stderr,
        )
        exit_code = 1
    finally:
        signal.signal(signal.SIGINT, previous_handler)
        if progress.stopped and exit_code == 0:
            exit_code = 130
        if exit_code == 130:
            print(
                "Interrupted; committed flows are preserved. Rerun to resume.",
                file=sys.stderr,
            )
        print(
            f"archives found={progress.found} completed={progress.completed} "
            f"skipped={progress.skipped} failed={progress.failed} {progress.counts()} "
            f"bytes={progress.processed_bytes} database_bytes={_database_size(database)}",
            flush=True,
        )
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
