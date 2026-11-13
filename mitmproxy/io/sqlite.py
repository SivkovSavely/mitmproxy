"""Durable, independently compressed native flow snapshots.

All store and reader methods are blocking. Each connection belongs to its
creating thread; SQLiteFlowWriter provides a bounded background writer.

Path safety assumes the effective user and root are trusted, and directory
ownership and permissions cannot be changed by an untrusted principal. The
immediate parent must not be shared-writable, including sticky directories;
trusted sticky ancestors such as /tmp are permitted. Symlink ancestors are not.
"""

import copy
import os
import sqlite3
import stat
import sys
import threading
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path

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


def _validate(db: sqlite3.Connection) -> None:
    app = db.execute("PRAGMA application_id").fetchone()[0]
    version = db.execute("PRAGMA user_version").fetchone()[0]
    schema = db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='flows'"
    ).fetchone()
    if app != _APP_ID or version != 1 or not schema or schema[0] != _SCHEMA:
        raise StorageError("Incompatible or invalid flow database schema")


def _secure_path(path: Path, create: bool) -> None:
    flags = os.O_RDWR if create else os.O_RDONLY
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
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

    def write(self, state: serializable.State) -> int:
        """Return the sequence only after commit; reject a changed existing ID."""
        flow_id = state.get("id", "unknown")
        if self._rollback_failed:
            raise StorageError(
                "Cannot write flow: previous rollback failed; close store"
            )
        try:
            raw = tnetstring.dumps(state)
            f = flow.Flow.from_state(copy.deepcopy(state))
            metadata = _metadata(f)
            self.db.execute("BEGIN IMMEDIATE")
            existing = self.db.execute(
                "SELECT sequence,payload_version,codec,dictionary_id,raw_length,payload FROM flows WHERE flow_id=?",
                (flow_id,),
            ).fetchone()
            if existing:
                sequence, *payload_fields = existing
                if _decompress(tuple(payload_fields)) != raw:
                    raise StorageError(f"conflicting state for flow {flow_id}")
            else:
                payload = zstd.compress(
                    raw,
                    options={
                        zstd.CompressionParameter.compression_level: 3,
                        zstd.CompressionParameter.checksum_flag: 1,
                    },
                )
                cursor = self.db.execute(
                    "INSERT INTO flows (flow_id,timestamp,flow_type,method,host,path,status_code,request_size,response_size,payload_version,codec,dictionary_id,raw_length,payload) VALUES (?,?,?,?,?,?,?,?,?,1,'zstd',NULL,?,?)",
                    (*metadata, len(raw), payload),
                )
                sequence = cursor.lastrowid
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
            row = self.db.execute(
                "SELECT payload_version,codec,dictionary_id,raw_length,payload FROM flows WHERE flow_id=?",
                (flow_id,),
            ).fetchone()
            if row is None:
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
        except Exception as e:
            raise StorageError(f"Cannot read flow {flow_id}: {type(e).__name__}") from e

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
