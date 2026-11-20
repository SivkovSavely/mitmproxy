"""Bounded, scalar-only live View inventory and change journal.

Attach before the View receives flows. Capture callbacks run on its owning
thread; only immutable JSON and IDs cross to the spool worker.
Readers must publish results only after read_cut exits successfully. The WAL
threshold is checked between bounded transactions, so one batch can overshoot it.
"""

import asyncio
import json
import sqlite3
import tempfile
import threading
import time
from collections import deque
from collections import OrderedDict
from contextlib import contextmanager
from pathlib import Path

from mitmproxy import http

MAX_VALUE = 1024 * 1024


class LiveIndexError(RuntimeError):
    pass


def _value(value):
    if value is None:
        return None, False
    if len(value) > MAX_VALUE:
        return None, True
    if len(value.encode("utf-8", "surrogateescape")) > MAX_VALUE:
        return None, True
    return value, False


def _metadata(flow):
    record = {
        "id": flow.id,
        "timestamp_created": flow.timestamp_created,
        "payload_version": None,
        "live": True,
        "method": None,
        "host": None,
        "path": None,
        "status_code": None,
        "request_size": None,
        "response_size": None,
    }
    record["type"], record["type_oversized"] = _value(flow.type)
    record["method_oversized"] = record["host_oversized"] = False
    if isinstance(flow, http.HTTPFlow):
        request = flow.request
        if request is not None:
            if len(request.data.method) > MAX_VALUE:
                record["method_oversized"] = True
            else:
                record["method"], record["method_oversized"] = _value(request.method)
            record["host"], record["host_oversized"] = _value(request.host)
            record["path"] = request.data.path[: 4096 * 4].decode(
                "utf-8", "surrogateescape"
            )[:4096]
            if request.raw_content is not None:
                record["request_size"] = len(request.raw_content)
        if flow.response is not None:
            record["status_code"] = flow.response.status_code
            if flow.response.raw_content is not None:
                record["response_size"] = len(flow.response.raw_content)
    return record


class _Fence:
    def __init__(self, sequence, pin=False):
        self.sequence = sequence
        self.ready = threading.Event()
        self.release = threading.Event() if pin else None


class LiveIndex:
    def __init__(
        self,
        view,
        *,
        queue_bytes=4 * 1024 * 1024,
        journal_bytes=64 * 1024 * 1024,
        disk_bytes=256 * 1024 * 1024,
        wal_bytes=32 * 1024 * 1024,
        read_timeout=30,
    ):
        if view.store_count():
            raise ValueError("LiveIndex must be attached to an empty View")
        if min(queue_bytes, journal_bytes, disk_bytes, wal_bytes, read_timeout) <= 0:
            raise ValueError("LiveIndex limits must be positive")
        self.view = view
        self.queue_bytes = queue_bytes
        self.journal_bytes = journal_bytes
        self.disk_bytes = disk_bytes
        self.wal_bytes = wal_bytes
        self.read_timeout = read_timeout
        self._directory = tempfile.TemporaryDirectory(prefix="mitm-live-")
        self.path = Path(self._directory.name) / "live.sqlite"
        self._condition = threading.Condition()
        self._queue: deque[OrderedDict | _Fence] = deque()
        self._queued_bytes = 0
        self._sequence = 0
        self._error = None
        self._closed = False
        self._worker = threading.Thread(
            target=self._run, name="live-index", daemon=True
        )
        self._worker.start()
        view.sig_store_add.connect(self.capture)
        view.sig_store_update.connect(self.capture)
        view.sig_store_remove.connect(self.remove)
        view.sig_store_clear.connect(self.clear)

    def _enqueue(self, op, flow_id=None, data=None):
        size = 256 + (len(flow_id) if flow_id else 0) + (len(data) if data else 0)
        with self._condition:
            if self._closed or self._error:
                return
            if not self._queue or not isinstance(self._queue[-1], OrderedDict):
                self._queue.append(OrderedDict())
            segment = self._queue[-1]
            assert isinstance(segment, OrderedDict)
            old_size = (
                sum(item[4] for item in segment.values())
                if op == "clear"
                else (segment[flow_id][4] if flow_id in segment else 0)
            )
            if self._queued_bytes - old_size + size > self.queue_bytes:
                self._error = "live metadata queue capacity exceeded"
                self._condition.notify_all()
                return
            self._sequence += 1
            if op == "clear":
                segment.clear()
            else:
                segment.pop(flow_id, None)
            self._queued_bytes += size - old_size
            segment[flow_id] = (self._sequence, op, flow_id, data, size)
            self._condition.notify()

    def capture(self, flow):
        try:
            data = json.dumps(_metadata(flow), ensure_ascii=True, separators=(",", ":"))
            self._enqueue("upsert", flow.id, data)
        except (ValueError, UnicodeError, TypeError) as exc:
            with self._condition:
                self._error = f"live metadata capture failed: {exc}"
                self._condition.notify_all()

    def remove(self, flow):
        self._enqueue("remove", flow.id)

    def clear(self):
        self._enqueue("clear")

    def _write_batch(self, db, batch):
        db.execute("BEGIN")
        used, floor = db.execute("SELECT bytes, floor FROM meta").fetchone()
        for seq, op, flow_id, data, size in batch:
            db.execute(
                "INSERT INTO journal VALUES (?, ?, ?, ?, ?)",
                (seq, op, flow_id, data, size),
            )
            used += size
            if op == "upsert":
                db.execute(
                    "INSERT OR REPLACE INTO current VALUES (?, ?, ?)",
                    (flow_id, seq, data),
                )
            elif op == "remove":
                db.execute("DELETE FROM current WHERE id = ?", (flow_id,))
            else:
                db.execute("DELETE FROM current")
        while used > self.journal_bytes:
            seq, size = db.execute(
                "SELECT seq, size FROM journal ORDER BY seq LIMIT 1"
            ).fetchone()
            db.execute("DELETE FROM journal WHERE seq = ?", (seq,))
            floor = seq
            used -= size
        db.execute(
            "UPDATE meta SET seq = ?, floor = ?, bytes = ?", (batch[-1][0], floor, used)
        )
        db.commit()

    def _run(self):
        db = None
        active_fence = None
        try:
            db = sqlite3.connect(self.path)
            db.execute("PRAGMA busy_timeout=0")
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA wal_autocheckpoint=0")
            db.execute("PRAGMA cache_spill=OFF")
            db.execute(f"PRAGMA max_page_count={max(1, self.disk_bytes // 4096)}")
            db.executescript("""
                CREATE TABLE current (id TEXT PRIMARY KEY, seq INTEGER, data TEXT);
                CREATE TABLE journal (seq INTEGER PRIMARY KEY, op TEXT, id TEXT, data TEXT, size INTEGER);
                CREATE TABLE unavailable (token TEXT, id TEXT, PRIMARY KEY(token, id));
                CREATE INDEX unavailable_id ON unavailable(id);
                CREATE TABLE meta (seq INTEGER, floor INTEGER, bytes INTEGER);
                INSERT INTO meta VALUES (0, 0, 0);
            """)
            db.commit()
            while True:
                with self._condition:
                    while not self._queue and not self._closed and not self._error:
                        self._condition.wait()
                    if self._closed or self._error:
                        break
                    item = self._queue[0]
                    if isinstance(item, _Fence):
                        self._queue.popleft()
                        self._queued_bytes -= 256
                    else:
                        batch: list = []
                        while item and len(batch) < 256:
                            _, entry = item.popitem(last=False)
                            self._queued_bytes -= entry[4]
                            batch.append(entry)
                        if not item:
                            self._queue.popleft()
                if isinstance(item, _Fence):
                    active_fence = item
                    item.ready.set()
                    if item.release is not None:
                        while not item.release.wait(0.1):
                            with self._condition:
                                if self._closed or self._error:
                                    break
                    active_fence = None
                else:
                    self._write_batch(db, batch)
                    wal = Path(str(self.path) + "-wal")
                    if wal.exists() and wal.stat().st_size > self.wal_bytes:
                        db.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
                        result = db.execute(
                            "PRAGMA wal_checkpoint(TRUNCATE)"
                        ).fetchone()
                        if result[0] or (
                            wal.exists() and wal.stat().st_size > self.wal_bytes
                        ):
                            raise LiveIndexError(
                                "live metadata WAL capacity exceeded by reader"
                            )
        except Exception as exc:
            with self._condition:
                self._error = f"live metadata spool failed: {exc}"
        finally:
            if db is not None:
                db.close()
            with self._condition:
                if active_fence:
                    active_fence.ready.set()
                for item in self._queue:
                    if isinstance(item, _Fence):
                        item.ready.set()
                self._condition.notify_all()

    def _fence(self, cancel=None, pin=False):
        with self._condition:
            if self._closed:
                raise LiveIndexError("live metadata index is closed")
            if self._error:
                raise LiveIndexError(self._error)
            if self._queued_bytes + 256 > self.queue_bytes:
                raise LiveIndexError("live metadata queue capacity exceeded by fence")
            fence = _Fence(self._sequence, pin)
            self._queue.append(fence)
            self._queued_bytes += 256
            self._condition.notify()
        while not fence.ready.wait(0.1):
            if cancel is not None and cancel.is_set():
                if fence.release:
                    fence.release.set()
                raise LiveIndexError("live metadata cut cancelled")
            if not self._worker.is_alive():
                if fence.release:
                    fence.release.set()
                raise LiveIndexError("live metadata worker stopped")
        with self._condition:
            if self._error:
                if fence.release:
                    fence.release.set()
                raise LiveIndexError(self._error)
            if self._closed:
                if fence.release:
                    fence.release.set()
                raise LiveIndexError("live metadata index is closed")
        return fence

    def cut(self, cancel=None):
        return self._fence(cancel).sequence

    async def flush(self):
        return await asyncio.to_thread(self.cut)

    @contextmanager
    def read_cut(self, cancel=None):
        """Pin current and its exact sequence; call from a worker, not the View thread.

        Copy rows promptly into the consumer's own scratch database, then exit.
        """
        fence = self._fence(cancel, pin=True)
        db = None
        deadline = time.monotonic() + self.read_timeout
        try:
            db = sqlite3.connect(self.path)
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            sequence = db.execute("SELECT seq FROM meta").fetchone()[0]
            if sequence != fence.sequence:
                raise LiveIndexError("live metadata fence mismatch")
            db.set_progress_handler(
                lambda: int(
                    time.monotonic() > deadline
                    or (cancel is not None and cancel.is_set())
                ),
                1000,
            )
            fence.release.set()
            yield db, sequence
            if time.monotonic() > deadline:
                raise LiveIndexError("live metadata read deadline exceeded")
            if cancel is not None and cancel.is_set():
                raise LiveIndexError("live metadata read cancelled")
            with self._condition:
                if self._error:
                    raise LiveIndexError(self._error)
        except sqlite3.OperationalError as exc:
            raise LiveIndexError(f"live metadata read failed: {exc}") from exc
        finally:
            fence.release.set()
            if db is not None:
                db.close()

    @staticmethod
    def rows(db, cut):
        """Iterate current records in a read_cut transaction, without materializing them."""
        if db.execute("SELECT seq FROM meta").fetchone()[0] != cut:
            raise LiveIndexError("live metadata cut does not match reader")
        return (
            {
                key: value
                for key, value in json.loads(row[0]).items()
                if key not in ("_filter_inputs", "_body_stamp", "_filter_errors")
            }
            for row in db.execute("SELECT data FROM current ORDER BY seq")
        )

    @staticmethod
    def changes(db, after, cut, *, limit):
        """Return up to limit changes; resume using the last returned sequence."""
        if not isinstance(limit, int) or limit < 1 or limit > 10000:
            raise ValueError("limit must be between 1 and 10000")
        seq, floor = db.execute("SELECT seq, floor FROM meta").fetchone()
        if after < floor:
            raise LiveIndexError(f"live metadata journal expired before {floor}")
        if after > cut or cut > seq:
            raise LiveIndexError("invalid live metadata change range")
        rows = db.execute(
            "SELECT seq, op, id, data FROM journal WHERE seq > ? AND seq <= ? ORDER BY seq LIMIT ?",
            (after, cut, limit),
        )
        return (
            (
                seq,
                op,
                flow_id,
                {
                    key: value
                    for key, value in json.loads(data).items()
                    if key not in ("_filter_inputs", "_body_stamp", "_filter_errors")
                }
                if data
                else None,
            )
            for seq, op, flow_id, data in rows
        )

    async def done(self):
        self.view.sig_store_add.disconnect(self.capture)
        self.view.sig_store_update.disconnect(self.capture)
        self.view.sig_store_remove.disconnect(self.remove)
        self.view.sig_store_clear.disconnect(self.clear)
        with self._condition:
            self._closed = True
            self._condition.notify_all()
        await asyncio.to_thread(self._worker.join)
        self._directory.cleanup()
