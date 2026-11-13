"""Durable, independently compressed native flow snapshots.

All store and reader methods are blocking. Each connection belongs to its
creating thread; SQLiteFlowWriter provides a bounded background writer.

Path safety assumes the effective user and root are trusted, and directory
ownership and permissions cannot be changed by an untrusted principal. The
immediate parent must not be shared-writable, including sticky directories;
trusted sticky ancestors such as /tmp are permitted. Symlink ancestors are not.
"""

import copy
import hashlib
import os
import sqlite3
import stat
import sys
import threading
from collections import deque
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

if sys.version_info >= (3, 14):
    from compression import zstd
else:
    from backports import zstd  # pragma: no cover (Python < 3.14)

from mitmproxy import dns
from mitmproxy import flow
from mitmproxy import http
from mitmproxy import tcp
from mitmproxy import udp
from mitmproxy.coretypes import serializable
from mitmproxy.io import compat
from mitmproxy.io import tnetstring


class StorageError(Exception):
    """A recording could not be safely stored or read."""


class WriterFull(StorageError):
    """The bounded writer queue is full."""


@dataclass(frozen=True)
class FlowMetadata:
    """Small pagination row; sizes exclude HTTP headers and preserve missing bodies."""

    sequence: int
    flow_id: str
    timestamp: float
    flow_type: str
    method: str | None
    host: str | None
    path: str | None
    status_code: int | None
    request_size: int | None
    response_size: int | None


_APP_ID = 0x4D46534C
_SCHEMA = """CREATE TABLE flows (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    flow_id TEXT NOT NULL UNIQUE,
    timestamp REAL NOT NULL,
    flow_type TEXT NOT NULL,
    method TEXT, host TEXT, path TEXT, status_code INTEGER,
    request_size INTEGER, response_size INTEGER,
    payload_version INTEGER NOT NULL, codec TEXT NOT NULL,
    dictionary_id INTEGER, raw_length INTEGER NOT NULL,
    payload BLOB NOT NULL
)"""
_COLUMNS = "sequence, flow_id, timestamp, flow_type, method, host, path, status_code, request_size, response_size"
# Cover ID deduplication as well as chronological seeks without table/BLOB pages.
_HISTORY_INDEX = "CREATE INDEX IF NOT EXISTS flows_chronological ON flows(timestamp,sequence,flow_id)"
_MAX_TIMESTAMP = 8.64e12  # JavaScript Date's range, expressed in seconds.
_LIVE_SEQUENCE = 2**63 - 1
_CHUNKS = """CREATE TABLE flow_payload_chunks (
    sequence INTEGER REFERENCES flows(sequence) ON DELETE CASCADE,
    ordinal INTEGER,
    raw_length INTEGER,
    payload BLOB NOT NULL,
    PRIMARY KEY(sequence, ordinal)
)"""
_MAX_CHUNK = 8 * 1024 * 1024


def _validate(db: sqlite3.Connection, *, allow_empty: bool = False) -> None:
    app = db.execute("PRAGMA application_id").fetchone()[0]
    version = db.execute("PRAGMA user_version").fetchone()[0]
    schema = db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='flows'"
    ).fetchone()
    if allow_empty and app == 0 and version == 0:
        if db.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone() is None:
            return
    if app != _APP_ID or version != 1 or not schema or schema[0] != _SCHEMA:
        raise StorageError("Incompatible or invalid flow database schema")
    _validate_chunks(db)


def _validate_chunks(db: sqlite3.Connection) -> None:
    chunks = db.execute(
        "SELECT type,sql FROM sqlite_master WHERE name='flow_payload_chunks'"
    ).fetchone()
    if chunks and chunks != ("table", _CHUNKS):
        raise StorageError("Incompatible or invalid flow database schema")


def _error_detail(e: Exception) -> str:
    detail = type(e).__name__
    if isinstance(e, sqlite3.Error):
        name = getattr(e, "sqlite_errorname", None)
        if name:
            detail += f" {name}"
        if str(e) in (
            "database or disk is full",
            "disk full",
            "string or blob too big",
        ):
            detail += f" {e}"
    return detail


def _secure_directory(path: Path) -> None:
    if ".." in path.parts:
        raise StorageError(f"Unsafe database directory: {path.parent}")
    parent = path.absolute().parent
    for directory in (parent, *parent.parents):
        info = directory.lstat()
        if (
            not stat.S_ISDIR(info.st_mode)
            or info.st_uid not in (0, os.geteuid())
            or (
                info.st_mode & 0o022
                and (directory == parent or not info.st_mode & stat.S_ISVTX)
            )
        ):
            raise StorageError(f"Unsafe database directory: {directory}")


def _secure_path(path: Path, create: bool) -> None:
    flags = os.O_RDWR if create else os.O_RDONLY
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        _secure_directory(path)
        if create:
            try:
                fd = os.open(path, flags | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                fd = os.open(path, flags)
        else:
            fd = os.open(path, flags)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                raise StorageError(f"Not a regular database file: {path}")
            if info.st_uid != os.geteuid():
                raise StorageError(
                    f"Database file is not owned by current user: {path}"
                )
            if create:
                os.fchmod(fd, 0o600)
        finally:
            os.close(fd)
        for suffix in ("-wal", "-shm"):
            sidecar = Path(f"{path}{suffix}")
            if sidecar.is_symlink():
                raise StorageError(f"Invalid database sidecar: {sidecar}")
            if sidecar.exists():
                info = sidecar.stat()
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
                    raise StorageError(f"Invalid database sidecar: {sidecar}")
                if create:
                    os.chmod(sidecar, 0o600, follow_symlinks=False)
    except OSError as e:
        raise StorageError(f"Cannot open database {path}: {e}") from e


def _decompress(row: tuple) -> bytes:
    version, codec, dictionary_id, length, payload = row
    if (
        version != 1
        or codec != "zstd"
        or dictionary_id is not None
        or not isinstance(length, int)
        or length < 0
    ):
        raise ValueError("unsupported payload format")
    decompressor = zstd.ZstdDecompressor()
    raw = decompressor.decompress(payload, length + 1)
    if len(raw) != length or not decompressor.eof or decompressor.unused_data:
        raise ValueError("invalid compressed payload length or frame")
    return raw


def ensure_history_index(
    path: str | Path, *, cancel: Callable[[], bool] | None = None
) -> None:
    """Add the chronological index to an existing recording, without payload writes.

    Blocking: run in the history worker, not on the web event loop. This does not
    create missing databases, change journal mode, or initialize a flow writer.
    Cancellable requests bound SQLite lock waits as progress callbacks do not run
    while another connection holds the write lock.
    """
    path = Path(path)
    db: sqlite3.Connection | None = None
    try:
        if cancel and cancel():
            raise StorageError("History request cancelled")
        _secure_path(path, False)
        db = sqlite3.connect(
            path.absolute().as_uri() + "?mode=rw",
            uri=True,
            isolation_level=None,
            timeout=0.1 if cancel else 5.0,
        )
        if cancel:
            db.set_progress_handler(cancel, 1000)
        db.execute("BEGIN IMMEDIATE")
        _validate(db)
        if cancel and cancel():
            raise StorageError("History request cancelled")
        db.execute(_HISTORY_INDEX)
        if cancel and cancel():
            raise StorageError("History request cancelled")
        db.execute("COMMIT")
    except sqlite3.Error as e:
        raise StorageError(f"Cannot index flow database {path}: {e}") from e
    finally:
        if db is not None:
            try:
                db.set_progress_handler(None, 0)
                if db.in_transaction:
                    db.rollback()
            finally:
                db.close()


def _metadata(f: flow.Flow) -> tuple:
    method = host = path = status = request_size = response_size = None
    if isinstance(f, http.HTTPFlow):
        if f.request:
            method, host, path = f.request.method, f.request.host, f.request.path
            request_size = (
                None if f.request.raw_content is None else len(f.request.raw_content)
            )
        if f.response:
            status = f.response.status_code
            response_size = (
                None if f.response.raw_content is None else len(f.response.raw_content)
            )
        # WebSocket sizes include HTTP bodies plus directional message contents.
        if f.websocket:
            for msg in f.websocket.messages:
                if msg.from_client and request_size is not None:
                    request_size += len(msg.content)
                elif not msg.from_client and response_size is not None:
                    response_size += len(msg.content)
    elif isinstance(f, (tcp.TCPFlow, udp.UDPFlow)):
        request_size = sum(len(m.content) for m in f.messages if m.from_client)
        response_size = sum(len(m.content) for m in f.messages if not m.from_client)
    elif isinstance(f, dns.DNSFlow):
        request_size = len(f.request.packed) if f.request else None
        response_size = len(f.response.packed) if f.response else None
    return (
        f.id,
        f.timestamp_created,
        f.type,
        method,
        host,
        path,
        status,
        request_size,
        response_size,
    )


class SQLiteFlowStore:
    """Synchronous writer; identical states are idempotent, conflicting IDs fail."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._rollback_failed = False
        db: sqlite3.Connection | None = None
        try:
            _secure_path(self.path, True)
            db = sqlite3.connect(self.path, isolation_level=None)
            self.db = db
            self.db.execute("BEGIN IMMEDIATE")
            app = self.db.execute("PRAGMA application_id").fetchone()[0]
            version = self.db.execute("PRAGMA user_version").fetchone()[0]
            objects = self.db.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone()
            if app == 0 and version == 0 and objects is None:
                self.db.execute(_SCHEMA)
                self.db.execute(f"PRAGMA application_id={_APP_ID}")
                self.db.execute("PRAGMA user_version=1")
            else:
                _validate(self.db)
            self.db.execute(_HISTORY_INDEX)
            self.db.execute("COMMIT")
            if self.db.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
                raise StorageError("Cannot enable SQLite WAL mode")
            self.db.execute("PRAGMA synchronous=FULL")
        except Exception as e:
            if db is not None:
                db.close()
            if isinstance(e, StorageError):
                raise
            raise StorageError(f"Cannot initialize flow database {path}: {e}") from e

    def _write_payload(
        self, f: flow.Flow, raw: bytes, sequence: int | None = None
    ) -> int:
        try:
            payload = zstd.compress(
                raw,
                options={
                    zstd.CompressionParameter.compression_level: 3,
                    zstd.CompressionParameter.checksum_flag: 1,
                },
            )
        except Exception as e:
            raise StorageError(
                f"stage=zstd.compress raw_length={len(raw)} compressed_length=unknown {type(e).__name__}"
            ) from None
        values = (*_metadata(f), len(raw), payload)
        try:
            if sequence is None:
                cursor = self.db.execute(
                    "INSERT INTO flows (flow_id,timestamp,flow_type,method,host,path,status_code,request_size,response_size,payload_version,codec,dictionary_id,raw_length,payload) VALUES (?,?,?,?,?,?,?,?,?,1,'zstd',NULL,?,?)",
                    values,
                )
                assert cursor.lastrowid is not None
                return cursor.lastrowid
            if self.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='flow_payload_chunks'"
            ).fetchone():
                self.db.execute(
                    "DELETE FROM flow_payload_chunks WHERE sequence=?", (sequence,)
                )
            self.db.execute(
                "UPDATE flows SET flow_id=?,timestamp=?,flow_type=?,method=?,host=?,path=?,status_code=?,request_size=?,response_size=?,payload_version=1,codec='zstd',dictionary_id=NULL,raw_length=?,payload=? WHERE sequence=?",
                (*values, sequence),
            )
            return sequence
        except Exception as e:
            raise StorageError(
                f"stage=sqlite.{'insert' if sequence is None else 'update'} raw_length={len(raw)} compressed_length={len(payload)} {_error_detail(e)}"
            ) from None

    def _stored_size(self, sequence: int) -> int:
        row = self.db.execute(
            "SELECT length(payload) FROM flows WHERE sequence=?", (sequence,)
        ).fetchone()
        if row is None:
            raise StorageError("Flow not found")
        if self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='flow_payload_chunks'"
        ).fetchone():
            return (
                row[0]
                + self.db.execute(
                    "SELECT coalesce(sum(length(payload)),0) FROM flow_payload_chunks WHERE sequence=?",
                    (sequence,),
                ).fetchone()[0]
            )
        return row[0]

    def _write_chunked(
        self,
        metadata: tuple,
        raw: BinaryIO,
        raw_length: int,
        digest: bytes,
        sequence: int | None = None,
        *,
        chunk_size: int = 1024 * 1024,
        check: Callable[[], None] | None = None,
    ) -> int:
        if not self.db.in_transaction:
            raise StorageError("Chunked write requires a transaction")
        limit = self.db.getlimit(sqlite3.SQLITE_LIMIT_LENGTH) - 1024
        if chunk_size <= 0 or limit < 1024:
            raise StorageError("Invalid chunk size or SQLite length limit too small")
        chunk_size = min(chunk_size, _MAX_CHUNK, limit // 2)
        if raw_length <= 0 or len(digest) != 32:
            raise StorageError("Invalid chunked payload length or digest")
        raw.seek(0)
        _validate_chunks(self.db)
        self.db.execute(
            _CHUNKS.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1)
        )
        values = (*metadata, raw_length, digest)
        stage = "insert" if sequence is None else "update"
        try:
            if sequence is None:
                cursor = self.db.execute(
                    "INSERT INTO flows (flow_id,timestamp,flow_type,method,host,path,status_code,request_size,response_size,payload_version,codec,dictionary_id,raw_length,payload) VALUES (?,?,?,?,?,?,?,?,?,2,'zstd-chunks',NULL,?,?)",
                    values,
                )
                assert cursor.lastrowid is not None
                sequence = cursor.lastrowid
            else:
                self.db.execute(
                    "DELETE FROM flow_payload_chunks WHERE sequence=?", (sequence,)
                )
                cursor = self.db.execute(
                    "UPDATE flows SET flow_id=?,timestamp=?,flow_type=?,method=?,host=?,path=?,status_code=?,request_size=?,response_size=?,payload_version=2,codec='zstd-chunks',dictionary_id=NULL,raw_length=?,payload=? WHERE sequence=?",
                    (*values, sequence),
                )
                if cursor.rowcount != 1:
                    raise StorageError("Flow not found")
        except StorageError:
            raise
        except Exception as e:
            raise StorageError(
                f"stage=sqlite.flow.{stage} raw_length={raw_length} chunk_index=none chunk_raw_length=none compressed_length=none {_error_detail(e)}"
            ) from None
        hasher = hashlib.sha256()
        remaining = raw_length
        ordinal = 0
        while remaining:
            if check:
                check()
            data = raw.read(min(chunk_size, remaining))
            if not data or len(data) > min(chunk_size, remaining):
                raise StorageError("Invalid or truncated raw payload")
            hasher.update(data)
            try:
                payload = zstd.compress(
                    data,
                    options={
                        zstd.CompressionParameter.compression_level: 3,
                        zstd.CompressionParameter.checksum_flag: 1,
                    },
                )
            except Exception as e:
                raise StorageError(
                    f"stage=zstd.compress.chunk raw_length={raw_length} chunk_index={ordinal} chunk_raw_length={len(data)} compressed_length=unknown {_error_detail(e)}"
                ) from None
            try:
                self.db.execute(
                    "INSERT INTO flow_payload_chunks VALUES (?,?,?,?)",
                    (sequence, ordinal, len(data), payload),
                )
            except Exception as e:
                raise StorageError(
                    f"stage=sqlite.chunk.insert raw_length={raw_length} chunk_index={ordinal} chunk_raw_length={len(data)} compressed_length={len(payload)} {_error_detail(e)}"
                ) from None
            remaining -= len(data)
            ordinal += 1
        if raw.read(1) or hasher.digest() != digest:
            raise StorageError("Raw payload length or digest mismatch")
        return sequence

    def write(self, state: serializable.State) -> int:
        """Return after commit; reject changed IDs and all existing chunked IDs."""
        flow_id = state.get("id", "unknown")
        if self._rollback_failed:
            raise StorageError(
                "Cannot write flow: previous rollback failed; close store"
            )
        try:
            raw = tnetstring.dumps(state)
            f = flow.Flow.from_state(copy.deepcopy(state))
            self.db.execute("BEGIN IMMEDIATE")
            existing = self.db.execute(
                "SELECT sequence,payload_version,codec,dictionary_id,raw_length,payload FROM flows WHERE flow_id=?",
                (flow_id,),
            ).fetchone()
            if existing:
                sequence, *payload_fields = existing
                if payload_fields[0] == 2:
                    raise StorageError(f"conflicting state for flow {flow_id}")
                if _decompress(tuple(payload_fields)) != raw:
                    raise StorageError(f"conflicting state for flow {flow_id}")
            else:
                sequence = self._write_payload(f, raw)
            self.db.execute("COMMIT")
            return sequence
        except Exception as e:
            if self.db.in_transaction:
                try:
                    self.db.execute("ROLLBACK")
                except sqlite3.Error as rollback_error:
                    self._rollback_failed = True
                    raise StorageError(
                        f"Cannot write flow {flow_id}: rollback failed: {rollback_error}"
                    ) from None
            detail = (
                str(e)
                if isinstance(e, (sqlite3.Error, StorageError))
                else type(e).__name__
            )
            raise StorageError(f"Cannot write flow {flow_id}: {detail}") from None

    def close(self) -> None:
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class SQLiteFlowReader:
    """Read snapshots and payload-free metadata; missing HTTP bodies remain None."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        db: sqlite3.Connection | None = None
        try:
            _secure_path(self.path, False)
            db = sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True)
            self.db = db
            _validate(self.db)
        except Exception as e:
            if db is not None:
                db.close()
            if isinstance(e, StorageError):
                raise
            raise StorageError(f"Cannot open flow database {path}: {e}") from e

    def get_flow(self, flow_id: str) -> flow.Flow | None:
        try:
            version = self.db.execute(
                "SELECT payload_version FROM flows WHERE flow_id=?", (flow_id,)
            ).fetchone()
            if version is not None and version[0] == 2:
                raise StorageError("Chunked flow requires iter_raw")
            row = self.db.execute(
                "SELECT payload_version,codec,dictionary_id,raw_length,payload FROM flows WHERE flow_id=? AND payload_version != 2",
                (flow_id,),
            ).fetchone()
            if row is None:
                if version is not None:
                    raise StorageError("Flow changed; use iter_raw for chunked flows")
                return None
            raw = _decompress(row)
            state, rest = tnetstring.pop(memoryview(raw))
            if rest or not isinstance(state, dict):
                raise ValueError("invalid native flow state")
            state = compat.migrate_flow(state)
            if state.get("backup") is not None:
                state["backup"] = flow.Flow.from_state(
                    compat.migrate_flow(state["backup"])
                ).get_state()
            f = flow.Flow.from_state(state)
            if f.id != flow_id:
                raise ValueError("flow ID mismatch")
            return f
        except StorageError:
            raise
        except Exception as e:
            raise StorageError(f"Cannot read flow {flow_id}: {type(e).__name__}") from e

    def iter_raw(self, flow_id: str, *, chunk_size: int = 64 * 1024) -> Iterator[bytes]:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if self.db.in_transaction:
            raise StorageError("Cannot stream within an existing transaction")
        try:
            self.db.execute("BEGIN")
            row = self.db.execute(
                "SELECT sequence,payload_version,codec,dictionary_id,raw_length,length(payload) FROM flows WHERE flow_id=?",
                (flow_id,),
            ).fetchone()
            if row is None:
                raise ValueError("flow not found")
            sequence, version, codec, dictionary, total, payload_size = row
            if dictionary is not None or not isinstance(total, int) or total < 0:
                raise ValueError("invalid payload metadata")
            if version == 1 and codec == "zstd":
                rows: Iterable[tuple[int, int, int]] = [(sequence, 0, total)]
                table = "flows"
                expected_digest = None
            elif version == 2 and codec == "zstd-chunks" and payload_size == 32:
                with self.db.blobopen(
                    "flows", "payload", sequence, readonly=True
                ) as blob:
                    expected_digest = blob.read(32)
                rows = self.db.execute(
                    "SELECT rowid,ordinal,raw_length FROM flow_payload_chunks WHERE sequence=? ORDER BY ordinal",
                    (sequence,),
                )
                table = "flow_payload_chunks"
            else:
                raise ValueError("unsupported payload format")
            hasher = hashlib.sha256()
            count = 0
            ordinal = 0
            for rowid, index, length in rows:
                if (
                    index != ordinal
                    or not isinstance(length, int)
                    or length < 0
                    or (version == 2 and (length == 0 or length > _MAX_CHUNK))
                ):
                    raise ValueError("invalid chunk metadata")
                ordinal += 1
                decoded = 0
                decoder = zstd.ZstdDecompressor()
                with self.db.blobopen(table, "payload", rowid, readonly=True) as blob:
                    while True:
                        data = (
                            blob.read(min(chunk_size, 65536))
                            if decoder.needs_input
                            else b""
                        )
                        if not data and decoder.needs_input:
                            break
                        output = decoder.decompress(
                            data, min(chunk_size, 65536, length - decoded + 1)
                        )
                        decoded += len(output)
                        if decoded > length:
                            raise ValueError("invalid decompressed length")
                        if output:
                            hasher.update(output)
                            count += len(output)
                            yield output
                        if decoder.eof:
                            if decoder.unused_data or blob.tell() != len(blob):
                                raise ValueError("trailing compressed data")
                            break
                if not decoder.eof or decoded != length:
                    raise ValueError("truncated or invalid compressed payload")
            if count != total or (
                version == 2 and (ordinal == 0 or hasher.digest() != expected_digest)
            ):
                raise ValueError("payload length or digest mismatch")
        except Exception as e:
            raise StorageError(
                f"Cannot stream flow {flow_id}: {type(e).__name__}: {e}"
            ) from e
        finally:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")

    def newest(self, limit: int) -> list[FlowMetadata]:
        return self._page(None, limit)

    def older(self, before: int, limit: int) -> list[FlowMetadata]:
        """Return descending sequence entries strictly older than before."""
        return self._page(before, limit)

    def _page(self, before: int | None, limit: int) -> list[FlowMetadata]:
        if limit < 0:
            raise ValueError("limit must be non-negative")
        try:
            if before is None:
                rows = self.db.execute(
                    f"SELECT {_COLUMNS} FROM flows ORDER BY sequence DESC LIMIT ?",
                    (limit,),
                )
            else:
                rows = self.db.execute(
                    f"SELECT {_COLUMNS} FROM flows WHERE sequence < ? ORDER BY sequence DESC LIMIT ?",
                    (before, limit),
                )
            return [FlowMetadata(*row) for row in rows]
        except sqlite3.Error as e:
            raise StorageError(f"Cannot read flow metadata: {e}") from e

    def history_page(
        self,
        *,
        lower: float | None = None,
        upper: float | None = None,
        offset: int | None = None,
        limit: int = 500,
        after: tuple[float, int, str] | None = None,
        before: tuple[float, int, str] | None = None,
        live: list[dict] | None = None,
        include_history: bool = True,
        anchor: str | None = None,
        selected: str | None = None,
        count_only: bool = False,
    ) -> dict:
        """Return bounded metadata in ascending (timestamp, sequence, id) order.

        Bounds are inclusive. An absent offset selects the newest page; an
        absolute offset past the end returns an empty page at count. Cursors
        are exclusive and cannot be combined with an offset or each other.
        Anchor rank and selection eligibility describe the entire result set,
        independently of the returned page. count_only skips row projection.

        Supply every live ID, including nonmatching rows (matches=False), so
        an older durable copy cannot reappear through search/global filters.
        Live metadata wins, but durable sequence numbers are retained. New live
        rows use the signed 64-bit maximum, which clients must preserve exactly
        when constructing cursors. No native flow state or payload is read.
        Install cancellation handlers per request: an interrupted rollback must
        clear the progress handler to release its read snapshot.
        """

        def valid_timestamp(value):
            return (
                type(value) in (int, float)
                and -_MAX_TIMESTAMP <= value <= _MAX_TIMESTAMP
            )

        def valid_id(value):
            return isinstance(value, str) and 0 < len(value) <= 128

        if (
            (lower is not None and not valid_timestamp(lower))
            or (upper is not None and not valid_timestamp(upper))
            or (lower is not None and upper is not None and lower > upper)
        ):
            raise ValueError("Invalid history timestamp bounds")
        if offset is not None and (
            type(offset) is not int or not 0 <= offset <= 2**53 - 1
        ):
            raise ValueError("Invalid history offset")
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError("History limit must be between 1 and 500")
        for cursor_key in (after, before):
            if cursor_key is not None and (
                not isinstance(cursor_key, tuple)
                or len(cursor_key) != 3
                or not valid_timestamp(cursor_key[0])
                or type(cursor_key[1]) is not int
                or not 0 <= cursor_key[1] <= _LIVE_SEQUENCE
                or not valid_id(cursor_key[2])
            ):
                raise ValueError("Invalid history cursor")
        if (after is not None and before is not None) or (
            (after is not None or before is not None) and offset is not None
        ):
            raise ValueError("Use one history cursor or an absolute offset")
        if any(
            value is not None and not valid_id(value) for value in (anchor, selected)
        ):
            raise ValueError("Invalid history flow ID")

        live_values = []
        live_ids = set()
        for row in live or []:
            if (
                not isinstance(row, dict)
                or not valid_id(row.get("id"))
                or row["id"] in live_ids
                or not valid_timestamp(row.get("timestamp_created"))
                or not isinstance(row.get("type"), str)
                or type(row.get("matches")) is not bool
                or type(row.get("highlighted", False)) is not bool
                or any(
                    row.get(key) is not None and not isinstance(row[key], str)
                    for key in ("method", "host", "path")
                )
                or any(
                    row.get(key) is not None
                    and (
                        type(row[key]) is not int or not 0 <= row[key] <= _LIVE_SEQUENCE
                    )
                    for key in (
                        "status_code",
                        "request_size",
                        "response_size",
                        "payload_version",
                    )
                )
            ):
                raise ValueError("Invalid live history metadata")
            live_ids.add(row["id"])
            live_values.append(
                (
                    row["id"],
                    row["timestamp_created"],
                    row["type"],
                    *(
                        row.get(key)
                        for key in (
                            "method",
                            "host",
                            "path",
                            "status_code",
                            "request_size",
                            "response_size",
                            "payload_version",
                        )
                    ),
                    row.get("highlighted", False),
                    row["matches"],
                )
            )

        parameters: dict = {
            "lower": -_MAX_TIMESTAMP if lower is None else lower,
            "upper": _MAX_TIMESTAMP if upper is None else upper,
        }
        bounds = "f.timestamp>=:lower AND f.timestamp<=:upper"
        sources = []
        if include_history:
            source = f"FROM main.flows AS f WHERE {bounds}"
            if live_values:
                source += " AND NOT EXISTS (SELECT 1 FROM temp.history_live AS l WHERE l.flow_id=f.flow_id)"
            sources.append((source, 0))
        if live_values:
            sources.append(
                (f"FROM temp.history_live AS f WHERE {bounds} AND f.matches=1", 1)
            )

        def count(condition="", values=None):
            if not sources:
                return 0
            sql = " UNION ALL ".join(
                f"SELECT count(*) AS n {source} {condition}" for source, _ in sources
            )
            return self.db.execute(
                f"SELECT sum(n) FROM ({sql})", parameters | (values or {})
            ).fetchone()[0]

        def identity(flow_id):
            if flow_id is None or not sources:
                return None
            sql = " UNION ALL ".join(
                f"SELECT f.timestamp,f.sequence,f.flow_id {source} AND f.flow_id=:identity"
                for source, _ in sources
            )
            return self.db.execute(sql, parameters | {"identity": flow_id}).fetchone()

        def position(key, *, inclusive=False):
            return count(
                f"AND (f.timestamp,f.sequence,f.flow_id) {'<=' if inclusive else '<'} (:timestamp,:sequence,:id)",
                dict(zip(("timestamp", "sequence", "id"), key))
                | {"upper": min(parameters["upper"], key[0])},
            )

        own_transaction = False
        try:
            if self.db.in_transaction:
                raise StorageError("Cannot read history within an existing transaction")
            self.db.execute("""CREATE TEMP TABLE IF NOT EXISTS history_live (
                flow_id TEXT PRIMARY KEY, sequence INTEGER, timestamp REAL,
                flow_type TEXT, method TEXT, host TEXT, path TEXT, status_code INTEGER,
                request_size INTEGER, response_size INTEGER, payload_version INTEGER,
                highlighted INTEGER, matches INTEGER
            )""")
            own_transaction = True
            self.db.execute("BEGIN")
            self.db.execute("DELETE FROM temp.history_live")
            if live_values:
                self.db.executemany(
                    "INSERT INTO temp.history_live (flow_id,timestamp,flow_type,method,host,path,status_code,request_size,response_size,payload_version,highlighted,matches) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    live_values,
                )
                self.db.execute(
                    f"UPDATE temp.history_live SET sequence=coalesce((SELECT sequence FROM main.flows WHERE flow_id=history_live.flow_id),{_LIVE_SEQUENCE})"
                )
                self.db.execute(
                    "CREATE INDEX IF NOT EXISTS temp.history_live_chronological ON history_live(timestamp,sequence,flow_id)"
                )
            total = count()
            anchor_key = identity(anchor)
            anchor_position = position(anchor_key) if anchor_key else None
            selected_eligible = identity(selected) is not None
            cursor_condition = ""
            skip = 0
            if after is not None:
                page_offset = position(after, inclusive=True)
                cursor_condition = " AND (f.timestamp,f.sequence,f.flow_id) > (:timestamp,:sequence,:id)"
            elif before is not None:
                page_offset = max(0, position(before) - limit)
                cursor_condition = " AND (f.timestamp,f.sequence,f.flow_id) < (:timestamp,:sequence,:id)"
            else:
                page_offset = (
                    max(0, total - limit) if offset is None else min(offset, total)
                )
                skip = page_offset
            result: dict = {
                "count": total,
                "offset": page_offset,
                "rows": [],
                "anchor_position": anchor_position,
                "selected_eligible": selected_eligible,
            }
            if count_only or not total or not sources:
                return result

            keys = " UNION ALL ".join(
                f"SELECT f.timestamp,f.sequence,f.flow_id,{is_live} AS live {source}{cursor_condition}"
                for source, is_live in sources
            )
            direction = "DESC" if before is not None else "ASC"
            projection = """substr(f.flow_id,1,128) AS id,f.sequence AS sequence,f.timestamp AS timestamp_created,
                substr(f.flow_type,1,32) AS type,substr(f.method,1,64) AS method,
                substr(f.host,1,1024) AS host,substr(f.path,1,4096) AS path,
                f.status_code,f.request_size,f.response_size,f.payload_version"""
            sql = f"""WITH page AS MATERIALIZED (
                {keys} ORDER BY timestamp {direction},sequence {direction},flow_id {direction}
                LIMIT :limit OFFSET :skip
            )
            SELECT {projection},0 AS live,0 AS highlighted
            FROM page AS p JOIN main.flows AS f ON f.sequence=p.sequence WHERE p.live=0
            UNION ALL
            SELECT {projection},1 AS live,f.highlighted
            FROM page AS p JOIN temp.history_live AS f ON f.flow_id=p.flow_id WHERE p.live=1
            ORDER BY timestamp_created,sequence,id"""
            page_parameters = parameters | {"limit": limit, "skip": skip}
            page_cursor = after if after is not None else before
            if page_cursor is not None:
                page_parameters |= dict(
                    zip(("timestamp", "sequence", "id"), page_cursor)
                )
                # The composite key also includes an unindexed ID; give SQLite
                # an explicit timestamp bound so adjacent pages seek directly.
                if after is not None:
                    page_parameters["lower"] = max(parameters["lower"], after[0])
                else:
                    page_parameters["upper"] = min(parameters["upper"], page_cursor[0])
            cursor = self.db.execute(sql, page_parameters)
            columns = [column[0] for column in cursor.description]
            for values in cursor:
                row = dict(zip(columns, values))
                row["live"] = bool(row["live"])
                row["highlighted"] = bool(row["highlighted"])
                result["rows"].append(row)
            return result
        except sqlite3.Error as e:
            raise StorageError(f"Cannot read history metadata: {e}") from e
        finally:
            if own_transaction and self.db.in_transaction:
                try:
                    try:
                        self.db.rollback()
                    except sqlite3.OperationalError as e:
                        if e.sqlite_errorcode != sqlite3.SQLITE_INTERRUPT:
                            raise
                        self.db.set_progress_handler(None, 0)
                        self.db.rollback()
                except sqlite3.Error as e:
                    raise StorageError(
                        f"Cannot end history read transaction: {e}"
                    ) from e

    def close(self) -> None:
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class SQLiteFlowWriter:
    """Bounded background writer; receipts succeed only after commit."""

    def __init__(
        self,
        path: str | Path,
        *,
        queue_size: int = 64,
        after: Future[None] | None = None,
    ):
        if queue_size < 1:
            raise ValueError("queue_size must be positive")
        self.path = path
        self.ready: Future[None] = Future()
        self.closed: Future[None] = Future()
        self.ready.set_running_or_notify_cancel()
        self.closed.set_running_or_notify_cancel()
        self._condition = threading.Condition()
        self._queue: deque[tuple[serializable.State, Future[int]]] = deque()
        self._queue_size = queue_size
        self._closing = False
        self._failure: Exception | None = None
        if after is None:
            self._start()
        else:
            after.add_done_callback(lambda _: self._start())

    def _start(self):
        threading.Thread(
            target=self._run, name="sqlite-flow-writer", daemon=False
        ).start()

    def submit(self, f: flow.Flow) -> Future[int]:
        with self._condition:
            if self._closing or self._failure:
                raise StorageError("Flow writer is closed or failed")
            if len(self._queue) >= self._queue_size:
                raise WriterFull("Flow writer queue is full")
            state = copy.deepcopy(f.get_state())
            result: Future[int] = Future()
            result.set_running_or_notify_cancel()
            self._queue.append((state, result))
            self._condition.notify()
            return result

    def close(self) -> Future[None]:
        with self._condition:
            self._closing = True
            self._condition.notify()
        return self.closed

    def _run(self):
        with self._condition:
            closed_before_start = self._closing and not self._queue
        if closed_before_start:
            self.ready.set_result(None)
            self.closed.set_result(None)
            return
        try:
            store = SQLiteFlowStore(self.path)
        except Exception as e:
            with self._condition:
                self._failure = e
                pending = list(self._queue)
                self._queue.clear()
            self.ready.set_exception(e)
            for _, result in pending:
                result.set_exception(e)
            self.closed.set_result(None)
            return
        self.ready.set_result(None)
        try:
            while True:
                with self._condition:
                    while not self._queue and not self._closing:
                        self._condition.wait()
                    if not self._queue:
                        break
                    state, result = self._queue.popleft()
                try:
                    sequence = store.write(state)
                except Exception as e:
                    result.set_exception(e)
                else:
                    result.set_result(sequence)
        finally:
            try:
                store.close()
            except Exception as e:
                self.closed.set_exception(e)
            else:
                self.closed.set_result(None)
