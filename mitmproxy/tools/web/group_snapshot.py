"""Metadata-only incremental grouping in a private scratch WAL.

All connections belong to one worker thread. Only cancellation Events may cross
threads. Readers pin scratch transactions, never source transactions. The caller
owns source filtering, deduplication, journal cuts and eligibility. It can use an
empty GroupStore and apply batches to publish progress while loading a source.
"""

import hashlib
import json
import math
import re
import sqlite3
import struct
import sys
import tempfile
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path

GroupValue = str | bytes | int | None
GROUP_CACHE_BYTES = 8 * 1024 * 1024
BLOCK_SIZE = 512
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
SUMMARY_FIELDS = frozenset(
    "type method host port scheme path status status_code timestamp_start timestamp_end "
    "request_size response_size error marked intercepted highlight highlighted "
    "timestamp_created sequence payload_version live".split()
)


class SnapshotError(Exception):
    pass


class SnapshotCancelled(SnapshotError):
    pass


@dataclass(frozen=True)
class Metadata:
    flow_id: str
    value: GroupValue
    summary: Mapping[str, str | int | float | bool | None] = field(default_factory=dict)
    label: str | None = None


@dataclass(frozen=True)
class GroupIdentity:
    key: str
    label: str
    canonical: bytes
    rank: int
    number: int


def group_identity(
    value: GroupValue, *, host: bool = False, max_value_bytes: int | None = 1024 * 1024
) -> GroupIdentity:
    """Hash the entire typed value; labels are not identities.

    Set max_value_bytes=None only for exceptional per-flow values which the
    caller elects to retain. These still consume the scratch disk budget, and
    are never truncated, mapped to null, or merged with other oversized values.
    """
    if max_value_bytes is not None and (
        type(max_value_bytes) is not int or max_value_bytes < 1
    ):
        raise ValueError("Invalid maximum group value bytes")
    if value is None:
        rank, data, label, tag = 4, b"", "(none)", b"n"
    elif type(value) is int and -(2**63) <= value < 2**63:
        rank, data, label, tag = 1, str(value).encode("ascii"), str(value), b"i"
    elif type(value) is str:
        if max_value_bytes is not None and len(value) > max_value_bytes:
            raise ValueError("Group value exceeds byte bound")
        value = value.casefold() if host else value
        rank, data, label, tag = 2, value.encode("utf-8"), value, b"t"
    elif type(value) is bytes:
        rank, data, label, tag = 3, value, repr(value[:160]), b"b"
    else:
        raise ValueError("Group values must be null, text, blob or signed integer")
    if max_value_bytes is not None and len(data) > max_value_bytes:
        raise ValueError("Group value exceeds byte bound")
    if len(label) > 160:
        label = label[:157] + "..."
    canonical = tag + b"\x00" + data
    return GroupIdentity(
        "g1:" + hashlib.sha256(canonical).hexdigest(),
        label,
        canonical,
        rank,
        value if type(value) is int else 0,
    )


def _summary(row: Metadata) -> str | None:
    if type(row.flow_id) is not str or not 0 < len(row.flow_id) <= 128:
        raise ValueError("Invalid metadata flow ID")
    row.flow_id.encode("utf-8")
    if not row.summary:
        return None
    if len(row.summary) > len(SUMMARY_FIELDS):
        raise ValueError("Too many summary fields")
    for key, value in row.summary.items():
        if key not in SUMMARY_FIELDS or type(value) not in (
            str,
            int,
            float,
            bool,
            type(None),
        ):
            raise ValueError("Only scalar metadata summary fields are allowed")
        if isinstance(value, str) and len(value) > (4096 if key == "path" else 1024):
            raise ValueError("Summary text exceeds bound")
        if type(value) is int and not -(2**63) <= value < 2**63:
            raise ValueError("Summary integer exceeds bound")
    result = json.dumps(dict(row.summary), ensure_ascii=True, allow_nan=False)
    if len(result) > 32768:
        raise ValueError("Summary exceeds byte bound")
    return result


def _flow_key(flow_id: str) -> str | bytes:
    return (
        bytes.fromhex(flow_id.replace("-", "")) if _UUID.fullmatch(flow_id) else flow_id
    )


def _flow_id(key: str | bytes) -> str:
    if isinstance(key, str):
        return key
    value = key.hex()
    return f"{value[:8]}-{value[8:12]}-{value[12:16]}-{value[16:20]}-{value[20:]}"


def _group_order(identity: GroupIdentity) -> bytes:
    return bytes([identity.rank]) + (
        (identity.number + 2**63).to_bytes(8, "big")
        if identity.rank == 1
        else identity.canonical[2:]
    )


def _member_order(row: Metadata, ordinal: int) -> bytes:
    timestamp = row.summary.get("timestamp_created", ordinal)
    sequence = row.summary.get("sequence", ordinal)
    if (
        not isinstance(timestamp, (int, float))
        or isinstance(timestamp, bool)
        or not math.isfinite(timestamp)
    ):
        raise ValueError("Invalid member timestamp")
    if type(sequence) is not int or not -(2**63) <= sequence < 2**63:
        raise ValueError("Invalid member sequence")
    # IEEE doubles become mem-comparable by complementing negatives and flipping
    # the sign bit of positives. Normalize -0 so ties use sequence and exact ID.
    bits = int.from_bytes(struct.pack(">d", float(timestamp) or 0.0), "big")
    bits = bits ^ (0xFFFFFFFFFFFFFFFF if bits >> 63 else 0x8000000000000000)
    return (
        bits.to_bytes(8, "big")
        + (sequence + 2**63).to_bytes(8, "big")
        + row.flow_id.encode()
    )


class GroupStore:
    """Mutable scratch owner with bounded blocks and retained immutable readers.

    db is exposed for coordinator-owned metadata spool tables on this same worker.
    Do not modify the core tables, leave transactions open, or use another thread.
    max_bytes bounds SQLite pages; max_wal_bytes separately bounds the WAL using
    conservative write admission. Backpressure raises SnapshotError, never evicts
    unexpired readers. Readers expire on the next apply/publish after grace_seconds,
    or may be explicitly closed. No per-flow Python cache is retained.
    """

    def __init__(
        self,
        *,
        directory: str | Path | None = None,
        max_bytes: int = 256 * 1024 * 1024,
        max_value_bytes: int | None = 1024 * 1024,
        host: bool = False,
        max_versions: int = 16,
        grace_seconds: float = 15,
        max_wal_bytes: int | None = None,
        cancel: threading.Event | None = None,
    ):
        if type(max_bytes) is not int or max_bytes < 32768:
            raise ValueError("Scratch bound must be at least 32768 bytes")
        if max_value_bytes is not None and (
            type(max_value_bytes) is not int or max_value_bytes < 1
        ):
            raise ValueError("Invalid maximum group value bytes")
        if type(max_versions) is not int or max_versions < 1:
            raise ValueError("Invalid version bound")
        if not math.isfinite(grace_seconds) or grace_seconds < 0:
            raise ValueError("Invalid version grace")
        self._write_reserve = min(16 * 1024**2, (max_bytes // 4096 + 16) * 4120)
        if max_wal_bytes is None:
            max_wal_bytes = 256 * 1024**2
        if type(max_wal_bytes) is not int or max_wal_bytes < self._write_reserve * 2:
            raise ValueError("WAL bound must allow two write reserves")
        self.max_wal_bytes = max_wal_bytes
        self.max_versions = max_versions
        self.grace_seconds = grace_seconds
        self.max_value_bytes = max_value_bytes
        self.host = host
        self._closed = False
        self._write_count = 0
        self._progress_count = 0
        self._wal_full = False
        self._versions: list[GroupSnapshot] = []
        self._cancel = cancel if cancel is not None else threading.Event()
        self._scratch = tempfile.TemporaryDirectory(
            prefix="group-snapshot-", dir=directory
        )
        self.path = Path(self._scratch.name) / "snapshot.sqlite"
        self.db: sqlite3.Connection | None = None
        try:
            self._check(self._cancel)
            self.db = db = sqlite3.connect(self.path)
            db.execute("PRAGMA page_size=4096")
            db.execute(f"PRAGMA max_page_count={max_bytes // 4096}")
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=NORMAL")
            db.execute("PRAGMA cache_size=-8192")
            db.execute("PRAGMA mmap_size=0")
            db.execute("PRAGMA temp_store=FILE")
            db.set_progress_handler(self._progress, 1000)
            db.executescript("""
                CREATE TABLE state (members INTEGER, groups INTEGER, ordinal INTEGER);
                INSERT INTO state VALUES (0, 0, 0);
                CREATE TABLE groups (
                    key BLOB PRIMARY KEY, canonical BLOB NOT NULL,
                    label TEXT NOT NULL, sort BLOB NOT NULL, count INTEGER NOT NULL
                ) WITHOUT ROWID;
                CREATE TABLE members (
                    flow_id PRIMARY KEY, group_key BLOB NOT NULL, sort BLOB NOT NULL
                ) WITHOUT ROWID;
                CREATE TABLE blocks (
                    id INTEGER PRIMARY KEY, ns BLOB NOT NULL, first BLOB NOT NULL,
                    count INTEGER NOT NULL, weight INTEGER NOT NULL
                );
                CREATE UNIQUE INDEX block_order ON blocks(ns, first);
                CREATE TABLE items (
                    ns BLOB NOT NULL, sort BLOB NOT NULL, block INTEGER NOT NULL,
                    weight INTEGER NOT NULL, payload NOT NULL, summary TEXT,
                    PRIMARY KEY(ns, sort)
                ) WITHOUT ROWID;
                CREATE INDEX block_items ON items(block, sort);
            """)
            db.commit()
            db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except BaseException as exc:
            self.close()
            if isinstance(exc, sqlite3.Error):
                if self._cancel.is_set():
                    raise SnapshotCancelled("Snapshot cancelled") from exc
                raise SnapshotError("Snapshot scratch initialization failed") from exc
            raise

    @classmethod
    def build(
        cls,
        source: Callable[[threading.Event], Iterable[Metadata]],
        *,
        cancel: threading.Event | None = None,
        **kwargs,
    ) -> "GroupStore":
        event = cancel if cancel is not None else threading.Event()
        store = cls(cancel=event, **kwargs)
        try:
            store._check(event)
            rows = iter(source(event))
            # Partial initial transactions are unpublished and deleted on failure.
            try:
                store.apply(rows, cancel=event, _building=True)
            finally:
                close = getattr(rows, "close", None)
                if close is not None:
                    close()
            return store
        except BaseException:
            store.close()
            raise

    @property
    def wal_bytes(self) -> int:
        path = Path(str(self.path) + "-wal")
        try:
            return path.stat().st_size
        except FileNotFoundError:
            return 0

    @property
    def size_bytes(self) -> int:
        return sum(p.stat().st_size for p in self.path.parent.iterdir() if p.is_file())

    def _check(self, cancel: threading.Event):
        if self._closed:
            raise SnapshotError("Group store is closed")
        if cancel.is_set():
            raise SnapshotCancelled("Snapshot cancelled")

    def _expire(self):
        now = time.monotonic()
        for version in self._versions[:]:
            if now - version._published >= self.grace_seconds:
                version.close()

    def _progress(self):
        self._progress_count += 1
        if self._progress_count % 32 == 0:
            self._wal_full = self.wal_bytes + self._write_reserve > self.max_wal_bytes
        return int(self._cancel.is_set() or self._wal_full)

    def _admit_write(self, estimate: int = 0):
        if self.wal_bytes + max(self._write_reserve, estimate) > self.max_wal_bytes:
            raise SnapshotError(
                "Snapshot scratch WAL bound reached; release retained versions"
            )

    def _write(self, sql: str, args: tuple):
        self._write_count += 1
        if self._write_count % 256 == 0:
            self._admit_write()
        assert self.db is not None
        return self.db.execute(sql, args)

    def _finish_build(self, event: threading.Event):
        """Assign ordered blocks in bounded key batches, only during initial build."""
        db = self.db
        assert db is not None
        db.execute("BEGIN IMMEDIATE")
        last = (b"", b"")
        namespace = None
        block_id = None
        count = total = 0
        pending_rows = pending_bytes = 0
        while True:
            self._check(event)
            # Keyset pagination permits checkpointing: no read cursor survives
            # the commit. Bound both key bytes and row count (large group keys).
            cursor = db.execute(
                "SELECT ns,sort,weight FROM items WHERE (ns,sort)>(?,?) "
                "ORDER BY ns,sort LIMIT 512",
                last,
            )
            batch = []
            size = 0
            for ns, sort, weight in cursor:
                batch.append((ns, sort, weight))
                size += len(sort) + len(ns)
                if size >= 1024**2:
                    break
            cursor.close()
            if not batch:
                break
            updates = []
            for ns, sort, weight in batch:
                if ns != namespace or count == BLOCK_SIZE:
                    if block_id is not None:
                        db.execute(
                            "UPDATE blocks SET count=?,weight=? WHERE id=?",
                            (count, total, block_id),
                        )
                    block_id = db.execute(
                        "INSERT INTO blocks(ns,first,count,weight) VALUES (?,?,0,0)",
                        (ns, sort),
                    ).lastrowid
                    namespace, count, total = ns, 0, 0
                updates.append((block_id, ns, sort))
                count += 1
                total += weight
            db.executemany("UPDATE items SET block=? WHERE ns=? AND sort=?", updates)
            db.execute(
                "UPDATE blocks SET count=?,weight=? WHERE id=?",
                (count, total, block_id),
            )
            last = batch[-1][:2]
            pending_rows += len(batch)
            pending_bytes += size
            if pending_rows >= 8192 or pending_bytes >= 1024**2:
                db.commit()
                db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                db.execute("BEGIN IMMEDIATE")
                pending_rows = pending_bytes = 0
        db.commit()
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def _insert(self, ns: bytes, key: bytes, weight: int, payload, summary=None):
        db = self.db
        assert db is not None
        block = db.execute(
            "SELECT id, first, count, weight FROM blocks WHERE ns=? AND first<=? "
            "ORDER BY first DESC LIMIT 1",
            (ns, key),
        ).fetchone()
        if block is None:
            block = db.execute(
                "SELECT id, first, count, weight FROM blocks WHERE ns=? ORDER BY first LIMIT 1",
                (ns,),
            ).fetchone()
        if block is None:
            block_id = self._write(
                "INSERT INTO blocks(ns,first,count,weight) VALUES (?,?,0,0)", (ns, key)
            ).lastrowid
            block = (block_id, key, 0, 0)
        block_id, first, count, total = block
        self._write(
            "INSERT INTO items VALUES (?,?,?,?,?,?)",
            (ns, key, block_id, weight, payload, summary),
        )
        self._write(
            "UPDATE blocks SET first=?,count=?,weight=? WHERE id=?",
            (min(first, key), count + 1, total + weight, block_id),
        )
        if count + 1 > BLOCK_SIZE:
            self._split(block_id, ns)

    def _split(self, block_id: int, ns: bytes):
        db = self.db
        assert db is not None
        count, total = db.execute(
            "SELECT count,weight FROM blocks WHERE id=?", (block_id,)
        ).fetchone()
        midpoint = count // 2
        first = db.execute(
            "SELECT sort FROM items WHERE block=? ORDER BY sort LIMIT 1 OFFSET ?",
            (block_id, midpoint),
        ).fetchone()[0]
        right_weight = db.execute(
            "SELECT sum(weight) FROM items WHERE block=? AND sort>=?", (block_id, first)
        ).fetchone()[0]
        self._admit_block_rewrite(block_id, first)
        new_id = self._write(
            "INSERT INTO blocks(ns,first,count,weight) VALUES (?,?,?,?)",
            (ns, first, count - midpoint, right_weight),
        ).lastrowid
        self._write(
            "UPDATE items SET block=? WHERE block=? AND sort>=?",
            (new_id, block_id, first),
        )
        self._write(
            "UPDATE blocks SET count=?,weight=? WHERE id=?",
            (midpoint, total - right_weight, block_id),
        )

    def _admit_block_rewrite(self, block_id: int, first: bytes = b""):
        # Large header keys and member summaries make a bounded number of rows
        # an unbounded number of pages. Admit their bytes before a bulk rewrite.
        assert self.db is not None
        size, count = self.db.execute(
            "SELECT coalesce(sum(length(ns)+length(sort)+length(payload)+"
            "coalesce(length(summary),0)),0),count(*) FROM items "
            "WHERE block=? AND sort>=?",
            (block_id, first),
        ).fetchone()
        pages = self.db.execute("PRAGMA page_count").fetchone()[0]
        self._admit_write(min(pages * 4120, size * 8 + count * 16384))

    def _remove(self, ns: bytes, key: bytes):
        db = self.db
        assert db is not None
        if len(key) * 8 > self._write_reserve:
            self._admit_write(len(key) * 8)
        block_id, weight = db.execute(
            "SELECT block,weight FROM items WHERE ns=? AND sort=?", (ns, key)
        ).fetchone()
        self._write("DELETE FROM items WHERE ns=? AND sort=?", (ns, key))
        count, first = db.execute(
            "SELECT count,first FROM blocks WHERE id=?", (block_id,)
        ).fetchone()
        if count == 1:
            self._write("DELETE FROM blocks WHERE id=?", (block_id,))
            return
        new_first = db.execute(
            "SELECT sort FROM items WHERE block=? ORDER BY sort LIMIT 1", (block_id,)
        ).fetchone()[0]
        self._write(
            "UPDATE blocks SET first=?, count=count-1, weight=weight-? WHERE id=?",
            (new_first, weight, block_id),
        )
        if count - 1 >= BLOCK_SIZE // 4:
            return
        neighbor = db.execute(
            "SELECT id FROM blocks WHERE ns=? AND first>? AND id<>? ORDER BY first LIMIT 1",
            (ns, first, block_id),
        ).fetchone()
        if neighbor is None:
            neighbor = db.execute(
                "SELECT id FROM blocks WHERE ns=? AND first<? AND id<>? ORDER BY first DESC LIMIT 1",
                (ns, first, block_id),
            ).fetchone()
        if neighbor is not None:
            other = neighbor[0]
            # Merge two adjacent blocks, splitting again if needed. At most 639
            # bounded headers/items are touched, regardless of namespace size.
            self._admit_block_rewrite(other)
            self._write("UPDATE items SET block=? WHERE block=?", (block_id, other))
            self._write("DELETE FROM blocks WHERE id=?", (other,))
            first, count, total = db.execute(
                "SELECT min(sort),count(*),sum(weight) FROM items WHERE block=?",
                (block_id,),
            ).fetchone()
            self._write(
                "UPDATE blocks SET first=?,count=?,weight=? WHERE id=?",
                (first, count, total, block_id),
            )
            if count > BLOCK_SIZE:
                self._split(block_id, ns)

    def _weight(self, ns: bytes, key: bytes, delta: int):
        db = self.db
        assert db is not None
        if len(key) * 8 > self._write_reserve:
            self._admit_write(len(key) * 8)
        block_id = db.execute(
            "SELECT block FROM items WHERE ns=? AND sort=?", (ns, key)
        ).fetchone()[0]
        self._write(
            "UPDATE items SET weight=weight+? WHERE ns=? AND sort=?", (delta, ns, key)
        )
        self._write("UPDATE blocks SET weight=weight+? WHERE id=?", (delta, block_id))

    def apply(
        self,
        changes: Iterable[Metadata | str] | Mapping[str, Metadata | None],
        *,
        cancel: threading.Event | None = None,
        _building: bool = False,
    ):
        """Atomically upsert metadata/remove IDs, touching only changed memberships.

        Mapping keys must equal Metadata.flow_id. Iterable strings are removals.
        Complete scalar summaries are copied to scratch. Missing timestamps use
        append order for the legacy facade; integration should supply timestamp
        and sequence. Failures roll back the entire call, including block changes.
        At most 8192 changes are accepted; smaller calls may be needed under WAL
        pressure. Coordinator writes through db must monitor wal_bytes separately.
        An explicit label replaces the group's display label (last explicit label
        wins); None keeps the existing label or uses the canonical default for a
        new group. Labels do not affect identity or ordering.
        """
        event = cancel if cancel is not None else threading.Event()
        self._check(event)
        self._expire()
        db = self.db
        assert db is not None
        if db.in_transaction:
            raise SnapshotError("Coordinator must commit its scratch spool transaction")
        self._cancel = event
        cache: OrderedDict[tuple[type, GroupValue], GroupIdentity] = OrderedDict()
        cache_bytes = 0
        self._wal_full = False
        try:
            pairs: Iterable[tuple[str, Metadata | None]] = (
                changes.items()
                if isinstance(changes, Mapping)
                else (
                    (r.flow_id, r) if isinstance(r, Metadata) else (r, None)
                    for r in changes
                )
            )
            if not _building:
                bounded: list[tuple[str, Metadata | None]] = []
                value_bytes = 0
                for pair in pairs:
                    self._check(event)
                    if len(bounded) == 8192:
                        raise SnapshotError("Apply accepts at most 8192 changes")
                    bounded.append(pair)
                    row = pair[1]
                    if isinstance(row, Metadata):
                        value_bytes += (
                            len(row.value) * 4
                            if isinstance(row.value, (str, bytes))
                            else 8
                        )
                        value_bytes += sum(
                            len(v) * 4
                            for v in row.summary.values()
                            if isinstance(v, str)
                        )
                pairs = iter(bounded)
            db.execute(
                "PRAGMA wal_checkpoint(PASSIVE)"
                if self._versions
                else "PRAGMA wal_checkpoint(TRUNCATE)"
            )
            # Estimate dirty existing index paths plus new/overflow pages before
            # starting an atomic update. The runtime guard also covers spill and
            # unusual block merges without stat'ing every SQL statement.
            estimate = 0
            if not _building:
                pages = db.execute("PRAGMA page_count").fetchone()[0]
                estimate = min(pages * 4120, len(bounded) * 128 * 1024)
                estimate += len(bounded) * 16384 + value_bytes * 8
            self._admit_write(estimate)
            if _building:
                # An initial store is unpublished and removed on any failure.
                # Avoid durable checkpoint fsyncs for disposable staging pages.
                db.execute("PRAGMA synchronous=OFF")
            db.execute("BEGIN IMMEDIATE")
            members, groups, ordinal = db.execute("SELECT * FROM state").fetchone()
            chunk_rows = chunk_bytes = 0
            for flow_id, row in pairs:
                self._check(event)
                if type(flow_id) is not str or not 0 < len(flow_id) <= 128:
                    raise ValueError("Invalid metadata flow ID")
                if row is not None and (
                    not isinstance(row, Metadata) or row.flow_id != flow_id
                ):
                    raise ValueError("Metadata flow ID does not match change key")
                packed = _flow_key(flow_id)
                previous = (
                    None
                    if _building
                    else db.execute(
                        "SELECT group_key,sort FROM members WHERE flow_id=?", (packed,)
                    ).fetchone()
                )
                removed_label = None
                if previous:
                    old_group, old_order = previous
                    self._remove(old_group, old_order)
                    self._write("DELETE FROM members WHERE flow_id=?", (packed,))
                    sort, count, old_label = db.execute(
                        "SELECT sort,count,label FROM groups WHERE key=?", (old_group,)
                    ).fetchone()
                    if count == 1:
                        removed_label = old_label
                        self._remove(b"", sort)
                        self._write("DELETE FROM groups WHERE key=?", (old_group,))
                        groups -= 1
                    else:
                        self._weight(b"", sort, -1)
                        self._write(
                            "UPDATE groups SET count=count-1 WHERE key=?", (old_group,)
                        )
                    members -= 1
                if row is None:
                    continue
                summary = _summary(row)
                if row.label is not None:
                    if type(row.label) is not str or len(row.label) > 256:
                        raise ValueError("Invalid group display label")
                    row.label.encode("utf-8")
                order = _member_order(row, ordinal)
                value = row.value
                # Validate the raw bound before casefold allocation.
                if (
                    isinstance(value, (str, bytes))
                    and self.max_value_bytes is not None
                    and len(value) > self.max_value_bytes
                ):
                    raise ValueError("Group value exceeds byte bound")
                if type(value) not in (str, bytes, int, type(None)):
                    raise ValueError(
                        "Group values must be null, text, blob or signed integer"
                    )
                value = value.casefold() if self.host and type(value) is str else value
                cache_key = (type(value), value)
                identity = cache.get(cache_key)
                if identity is None:
                    identity = group_identity(
                        value, max_value_bytes=self.max_value_bytes
                    )
                    cost = (
                        sys.getsizeof(value) + sys.getsizeof(identity.canonical) + 1024
                    )
                    cache[cache_key] = identity
                    cache_bytes += cost
                    while cache_bytes > GROUP_CACHE_BYTES:
                        evicted_key, evicted = cache.popitem(last=False)
                        cache_bytes -= (
                            sys.getsizeof(evicted_key[1])
                            + sys.getsizeof(evicted.canonical)
                            + 1024
                        )
                else:
                    cache.move_to_end(cache_key)
                if _building and len(identity.canonical) * 8 > self._write_reserve:
                    self._admit_write(len(identity.canonical) * 8 + 32768)
                key = bytes.fromhex(identity.key[3:])
                group = db.execute(
                    "SELECT canonical,sort FROM groups WHERE key=?", (key,)
                ).fetchone()
                if group is None:
                    sort = _group_order(identity)
                    self._write(
                        "INSERT INTO groups VALUES (?,?,?,?,1)",
                        (
                            key,
                            identity.canonical,
                            row.label
                            if row.label is not None
                            else removed_label
                            if previous and previous[0] == key
                            else identity.label,
                            sort,
                        ),
                    )
                    if not _building:
                        self._insert(b"", sort, 1, key)
                    groups += 1
                else:
                    canonical, sort = group
                    if canonical != identity.canonical:
                        raise SnapshotError("Group digest collision")
                    if row.label is None:
                        self._write(
                            "UPDATE groups SET count=count+1 WHERE key=?", (key,)
                        )
                    else:
                        self._write(
                            "UPDATE groups SET count=count+1,label=? WHERE key=?",
                            (row.label, key),
                        )
                    if not _building:
                        self._weight(b"", sort, 1)
                try:
                    self._write(
                        "INSERT INTO members VALUES (?,?,?)", (packed, key, order)
                    )
                except sqlite3.IntegrityError as exc:
                    if _building:
                        raise SnapshotError("Duplicate metadata flow ID") from exc
                    raise
                if _building:
                    self._write(
                        "INSERT INTO items VALUES (?,?,0,1,?,?)",
                        (key, order, packed, summary),
                    )
                else:
                    self._insert(key, order, 1, packed, summary)
                members += 1
                ordinal += 1
                if _building:
                    chunk_rows += 1
                    chunk_bytes += (
                        4096
                        + len(identity.canonical) * 8
                        + (len(summary) * 8 if summary else 0)
                    )
                    if chunk_rows >= 8192 or chunk_bytes >= 64 * 1024**2:
                        db.commit()
                        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                        db.execute("BEGIN IMMEDIATE")
                        chunk_rows = chunk_bytes = 0
            if _building:
                # Group counts are finalized once, not maintained in block
                # headers for every initial member. This ordered scan is initial
                # build only; apply never scans unchanged memberships.
                db.commit()
                db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                last = b""
                pending_rows = pending_bytes = 0
                while True:
                    cursor = db.execute(
                        "SELECT key,sort,count FROM groups WHERE key>? ORDER BY key LIMIT 256",
                        (last,),
                    )
                    batch = []
                    size = 0
                    for key, sort, count in cursor:
                        batch.append((b"", sort, count, key))
                        size += len(sort)
                        if size >= 1024**2:
                            break
                    cursor.close()
                    if not batch:
                        break
                    db.executemany("INSERT INTO items VALUES (?,?,0,?,?,NULL)", batch)
                    last = batch[-1][3]
                    pending_rows += len(batch)
                    pending_bytes += size
                    if pending_rows >= 8192 or pending_bytes >= 1024**2:
                        db.commit()
                        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                        pending_rows = pending_bytes = 0
                db.commit()
                db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                self._finish_build(event)
                db.execute("BEGIN IMMEDIATE")
            self._check(event)
            self._admit_write()
            self._write(
                "UPDATE state SET members=?,groups=?,ordinal=?",
                (members, groups, ordinal),
            )
            db.commit()
            if _building:
                db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except BaseException as exc:
            # Disable cancellation while rolling back, so an interrupted rollback
            # cannot leave a partially open writer transaction.
            db.set_progress_handler(None, 0)
            db.rollback()
            if isinstance(exc, sqlite3.Error):
                if event.is_set():
                    raise SnapshotCancelled("Snapshot cancelled") from exc
                if self._wal_full:
                    raise SnapshotError("Snapshot scratch WAL bound reached") from exc
                raise SnapshotError(
                    "Snapshot scratch database failed or exceeded byte bound"
                ) from exc
            raise
        finally:
            self._cancel = threading.Event()
            self._wal_full = False
            if _building:
                db.execute("PRAGMA synchronous=NORMAL")
            db.set_progress_handler(self._progress, 1000)

    def publish(self, *, cancel: threading.Event | None = None) -> "GroupSnapshot":
        event = cancel if cancel is not None else threading.Event()
        self._check(event)
        self._expire()
        if len(self._versions) >= self.max_versions:
            raise SnapshotError("Snapshot version retention bound reached")
        assert self.db is not None
        if self.db.in_transaction:
            raise SnapshotError("Cannot publish an uncommitted scratch transaction")
        snapshot = GroupSnapshot()
        snapshot._store = self
        snapshot._owns_store = False
        snapshot._closed = False
        snapshot._cancel = event
        snapshot._published = time.monotonic()
        snapshot._db = None
        snapshot._scratch = self._scratch
        try:
            db = snapshot._db = sqlite3.connect(self.path)
            db.execute("PRAGMA cache_size=-2048")
            db.execute("PRAGMA mmap_size=0")
            db.execute("PRAGMA query_only=ON")
            db.set_progress_handler(lambda: int(snapshot._cancel.is_set()), 1000)
            db.execute("BEGIN")
            snapshot.member_count, snapshot.group_count = db.execute(
                "SELECT members,groups FROM state"
            ).fetchone()
            snapshot._check()
            self._versions.append(snapshot)
            return snapshot
        except BaseException as exc:
            snapshot.close()
            if isinstance(exc, sqlite3.Error):
                if event.is_set():
                    raise SnapshotCancelled("Snapshot cancelled") from exc
                raise SnapshotError("Snapshot scratch reader failed") from exc
            raise

    def close(self):
        if not self._closed:
            self._closed = True
            for version in self._versions[:]:
                version.close()
            try:
                if self.db is not None:
                    self.db.close()
            finally:
                self._scratch.cleanup()

    def __enter__(self):
        self._check(threading.Event())
        return self

    def __exit__(self, *exc):
        self.close()


class GroupSnapshot:
    """Immutable scratch read transaction; zero-based pages of at most 500 rows."""

    _store: GroupStore
    _owns_store: bool
    _closed: bool
    _cancel: threading.Event
    _published: float
    _scratch: tempfile.TemporaryDirectory[str]
    _db: sqlite3.Connection | None
    member_count: int
    group_count: int

    @classmethod
    def build(
        cls,
        source: Callable[[threading.Event], Iterable[Metadata]],
        *,
        cancel: threading.Event | None = None,
        **kwargs,
    ) -> "GroupSnapshot":
        store = GroupStore.build(source, cancel=cancel, **kwargs)
        try:
            snapshot = store.publish(cancel=cancel)
            snapshot._owns_store = True
            return snapshot
        except BaseException:
            store.close()
            raise

    @contextmanager
    def query_scope(self, cancel: threading.Event) -> Iterator["GroupSnapshot"]:
        previous = self._cancel
        self._cancel = cancel
        try:
            self._check()
            yield self
        finally:
            self._cancel = previous

    def _check(self):
        if self._closed:
            raise SnapshotError("Snapshot is closed")
        if self._cancel.is_set():
            raise SnapshotCancelled("Snapshot cancelled")

    def _query(self, sql: str, args: tuple) -> list:
        self._check()
        assert self._db is not None
        try:
            return self._db.execute(sql, args).fetchall()
        except sqlite3.Error as exc:
            if self._cancel.is_set():
                raise SnapshotCancelled("Snapshot cancelled") from exc
            raise SnapshotError("Snapshot query failed") from exc

    @staticmethod
    def _page(offset: int, limit: int):
        if type(offset) is not int or not 0 <= offset < 2**63:
            raise ValueError("Invalid snapshot offset")
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError("Invalid snapshot page limit")

    @staticmethod
    def _token(key: str):
        if not isinstance(key, str) or not re.fullmatch(r"g1:[0-9a-f]{64}", key):
            raise ValueError("Invalid group token")

    def _prefix(self, ns: bytes, key: bytes) -> tuple[int, int]:
        block = self._query("SELECT block FROM items WHERE ns=? AND sort=?", (ns, key))[
            0
        ][0]
        first = self._query("SELECT first FROM blocks WHERE id=?", (block,))[0][0]
        count, weight = self._query(
            "SELECT coalesce(sum(count),0),coalesce(sum(weight),0) FROM blocks WHERE ns=? AND first<?",
            (ns, first),
        )[0]
        local_count, local_weight = self._query(
            "SELECT count(*),coalesce(sum(weight),0) FROM items WHERE block=? AND sort<?",
            (block, key),
        )[0]
        return count + local_count, weight + local_weight

    def _items(self, ns: bytes, offset: int, limit: int) -> list:
        # Header-only traversal, followed by an indexed seek in <=512 local
        # items. Never OFFSET across all members or recompute a suffix ordinal.
        remaining = offset
        first = None
        block_id = None
        self._check()
        assert self._db is not None
        try:
            for block_id, first, count in self._db.execute(
                "SELECT id,first,count FROM blocks WHERE ns=? ORDER BY first", (ns,)
            ):
                self._check()
                if remaining < count:
                    break
                remaining -= count
            else:
                return []
        except sqlite3.Error as exc:
            if self._cancel.is_set():
                raise SnapshotCancelled("Snapshot cancelled") from exc
            raise SnapshotError("Snapshot query failed") from exc
        start = self._query(
            "SELECT sort FROM items WHERE block=? ORDER BY sort LIMIT 1 OFFSET ?",
            (block_id, remaining),
        )[0][0]
        return self._query(
            "SELECT payload,summary,weight FROM items WHERE ns=? AND sort>=? ORDER BY sort LIMIT ?",
            (ns, start, limit),
        )

    def _group(self, key: bytes) -> dict | None:
        rows = self._query("SELECT label,sort,count FROM groups WHERE key=?", (key,))
        if not rows:
            return None
        label, sort, count = rows[0]
        ordinal, before = self._prefix(b"", sort)
        return {
            "key": "g1:" + key.hex(),
            "label": label,
            "ordinal": ordinal,
            "count": count,
            "members_before": before,
        }

    def groups(self, *, offset: int = 0, limit: int = 100) -> list[dict]:
        self._page(offset, limit)
        items = self._items(b"", offset, limit)
        if not items:
            return []
        first_sort = self._query("SELECT sort FROM groups WHERE key=?", (items[0][0],))[
            0
        ][0]
        ordinal, before = self._prefix(b"", first_sort)
        result = []
        for key, _, count in items:
            label = self._query("SELECT label FROM groups WHERE key=?", (key,))[0][0]
            result.append(
                {
                    "key": "g1:" + key.hex(),
                    "label": label,
                    "ordinal": ordinal,
                    "count": count,
                    "members_before": before,
                }
            )
            ordinal += 1
            before += count
        return result

    def group(self, key: str) -> dict | None:
        self._token(key)
        return self._group(bytes.fromhex(key[3:]))

    def members(self, key: str, *, offset: int = 0, limit: int = 100) -> list[dict]:
        self._page(offset, limit)
        self._token(key)
        return [
            {
                "flow_id": _flow_id(payload),
                "ordinal": offset + i,
                "summary": json.loads(summary) if summary is not None else {},
            }
            for i, (payload, summary, _) in enumerate(
                self._items(bytes.fromhex(key[3:]), offset, limit)
            )
        ]

    def locate(self, flow_id: str) -> dict | None:
        if type(flow_id) is not str or not 0 < len(flow_id) <= 128:
            raise ValueError("Invalid metadata flow ID")
        rows = self._query(
            "SELECT group_key,sort FROM members WHERE flow_id=?", (_flow_key(flow_id),)
        )
        if not rows:
            return None
        key, sort = rows[0]
        ordinal, _ = self._prefix(key, sort)
        return {"group": self._group(key), "member_ordinal": ordinal}

    def close(self):
        if not self._closed:
            self._closed = True
            try:
                if self._db is not None:
                    self._db.close()
            finally:
                if self in self._store._versions:
                    self._store._versions.remove(self)
                if self._owns_store:
                    self._store.close()

    def __enter__(self):
        self._check()
        return self

    def __exit__(self, *exc):
        self.close()
