"""Transactional provenance for importing historical native flows."""

import hashlib
import os
import sqlite3
import stat
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from mitmproxy import flow
from mitmproxy.io import tnetstring
from mitmproxy.io.sqlite import SQLiteFlowStore
from mitmproxy.io.sqlite import StorageError


@dataclass(frozen=True)
class Source:
    path: str
    timestamp: str
    suffix: int
    size: int
    mtime_ns: int
    ctime_ns: int
    device: int
    inode: int

    @classmethod
    def from_stat(
        cls, path: Path | str, timestamp: str, suffix: int, info: os.stat_result
    ) -> "Source":
        return cls(
            str(Path(path).resolve()),
            timestamp,
            suffix,
            info.st_size,
            info.st_mtime_ns,
            info.st_ctime_ns,
            info.st_dev,
            info.st_ino,
        )

    def verify(self) -> None:
        try:
            info = os.stat(self.path)
            if not stat.S_ISREG(info.st_mode) or (
                info.st_size,
                info.st_mtime_ns,
                info.st_ctime_ns,
                info.st_dev,
                info.st_ino,
            ) != (self.size, self.mtime_ns, self.ctime_ns, self.device, self.inode):
                raise StorageError("Source identity changed")
        except OSError as e:
            raise StorageError("Cannot verify source identity") from e


@dataclass(frozen=True)
class ImportResult:
    status: Literal["inserted", "existing", "updated", "retained"]
    conflicting: bool = False


_MANIFEST = """CREATE TABLE backfill_manifest (
    path TEXT PRIMARY KEY, timestamp TEXT NOT NULL, suffix INTEGER NOT NULL,
    size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, ctime_ns INTEGER NOT NULL,
    device INTEGER NOT NULL, inode INTEGER NOT NULL, revision INTEGER NOT NULL,
    complete INTEGER NOT NULL, flow_count INTEGER
)"""
_PROVENANCE = """CREATE TABLE backfill_provenance (
    flow_id TEXT PRIMARY KEY, timestamp TEXT NOT NULL, suffix INTEGER NOT NULL,
    path TEXT NOT NULL, revision INTEGER NOT NULL, ordinal INTEGER NOT NULL,
    digest BLOB NOT NULL
)"""


class SQLiteBackfillStore(SQLiteFlowStore):
    def __init__(self, path: str | Path):
        super().__init__(path)
        try:
            with self._transaction():
                if not self.check_schema(self.db):
                    self.db.execute(_MANIFEST)
                    self.db.execute(_PROVENANCE)
        except Exception:
            self.close()
            raise

    @staticmethod
    def check_schema(db: sqlite3.Connection) -> bool:
        """Validate optional backfill tables without creating or changing them."""
        rows = db.execute(
            "SELECT name,sql FROM sqlite_master WHERE name IN ('backfill_manifest','backfill_provenance')"
        ).fetchall()
        if rows and dict(rows) != {
            "backfill_manifest": _MANIFEST,
            "backfill_provenance": _PROVENANCE,
        }:
            raise StorageError("Incompatible backfill schema")
        return bool(rows)

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        if self._rollback_failed:
            raise StorageError("Previous rollback failed; close store")
        try:
            self.db.execute("BEGIN IMMEDIATE")
            yield
            self.db.execute("COMMIT")
        except Exception as e:
            if self.db.in_transaction:
                try:
                    self.db.execute("ROLLBACK")
                except sqlite3.Error as rollback_error:
                    self._rollback_failed = True
                    raise StorageError("Backfill rollback failed") from rollback_error
            if isinstance(e, StorageError):
                raise
            detail = str(e) if isinstance(e, sqlite3.Error) else type(e).__name__
            raise StorageError(f"Backfill transaction failed: {detail}") from None

    @staticmethod
    def _fingerprint(source: Source) -> tuple:
        return (
            source.timestamp,
            source.suffix,
            source.size,
            source.mtime_ns,
            source.ctime_ns,
            source.device,
            source.inode,
        )

    def _registered(self, source: Source, revision: int) -> None:
        source.verify()
        row = self.db.execute(
            "SELECT timestamp,suffix,size,mtime_ns,ctime_ns,device,inode,revision FROM backfill_manifest WHERE path=?",
            (source.path,),
        ).fetchone()
        if row != (*self._fingerprint(source), revision):
            raise StorageError("Stale source registration")

    def start_source(self, source: Source) -> int | None:
        with self._transaction():
            source.verify()
            row = self.db.execute(
                "SELECT timestamp,suffix,size,mtime_ns,ctime_ns,device,inode,revision,complete FROM backfill_manifest WHERE path=?",
                (source.path,),
            ).fetchone()
            if row and row[:7] == self._fingerprint(source):
                return None if row[8] else row[7]
            revision = row[7] + 1 if row else 1
            self.db.execute(
                "INSERT INTO backfill_manifest VALUES (?,?,?,?,?,?,?,?,?,0,NULL) ON CONFLICT(path) DO UPDATE SET timestamp=excluded.timestamp,suffix=excluded.suffix,size=excluded.size,mtime_ns=excluded.mtime_ns,ctime_ns=excluded.ctime_ns,device=excluded.device,inode=excluded.inode,revision=excluded.revision,complete=0,flow_count=NULL",
                (source.path, *self._fingerprint(source), revision),
            )
            return revision

    def write_flow(
        self,
        f: flow.Flow,
        source: Source,
        revision: int,
        ordinal: int,
        *,
        check_space: Callable[[int], None] | None = None,
    ) -> ImportResult:
        with self._transaction():
            self._registered(source, revision)
            row = self.db.execute(
                "SELECT flows.sequence,backfill_provenance.timestamp,backfill_provenance.suffix,backfill_provenance.path,backfill_provenance.revision,backfill_provenance.ordinal,backfill_provenance.digest FROM flows LEFT JOIN backfill_provenance ON flows.flow_id=backfill_provenance.flow_id WHERE flows.flow_id=?",
                (f.id,),
            ).fetchone()
            if row and row[1] is None:
                return ImportResult("existing")
            raw = tnetstring.dumps(f.get_state())
            digest = hashlib.sha256(raw).digest()
            order = (source.timestamp, source.suffix, source.path, revision, ordinal)
            if row:
                previous = row[1:6]
                if order == previous and digest != row[6]:
                    raise StorageError("Conflicting state at same source position")
                if order <= previous:
                    return ImportResult(
                        "existing" if digest == row[6] else "retained", digest != row[6]
                    )
                if digest == row[6]:
                    status: Literal["inserted", "existing", "updated", "retained"] = (
                        "existing"
                    )
                else:
                    status = "updated"
            else:
                status = "inserted"
            if status != "existing":
                if check_space:
                    # Freeing an old large BLOB can dirty its pages in the WAL.
                    old_size = (
                        self.db.execute(
                            "SELECT length(payload) FROM flows WHERE sequence=?",
                            (row[0],),
                        ).fetchone()[0]
                        if row
                        else 0
                    )
                    check_space(max(len(raw), old_size))
                self._write_payload(f, raw, row[0] if row else None)
            self.db.execute(
                "INSERT INTO backfill_provenance VALUES (?,?,?,?,?,?,?) ON CONFLICT(flow_id) DO UPDATE SET timestamp=excluded.timestamp,suffix=excluded.suffix,path=excluded.path,revision=excluded.revision,ordinal=excluded.ordinal,digest=excluded.digest",
                (f.id, *order, digest),
            )
            return ImportResult(status, status == "updated")

    def finish_source(self, source: Source, revision: int, flow_count: int) -> None:
        with self._transaction():
            self._registered(source, revision)
            self.db.execute(
                "UPDATE backfill_manifest SET complete=1,flow_count=? WHERE path=?",
                (flow_count, source.path),
            )
