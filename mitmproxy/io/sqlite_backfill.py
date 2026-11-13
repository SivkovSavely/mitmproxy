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
from typing import BinaryIO
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
_NATIVE_IDENTITY = """CREATE TABLE backfill_native_identity (
    flow_id TEXT PRIMARY KEY, digest BLOB NOT NULL
)"""


class SQLiteBackfillStore(SQLiteFlowStore):
    def __init__(self, path: str | Path):
        super().__init__(path)
        try:
            with self._transaction():
                if not self.check_schema(self.db):
                    self.db.execute(_MANIFEST)
                    self.db.execute(_PROVENANCE)
                self.db.execute(
                    _NATIVE_IDENTITY.replace(
                        "CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1
                    )
                )
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
        identity = db.execute(
            "SELECT sql FROM sqlite_master WHERE name='backfill_native_identity'"
        ).fetchone()
        if identity and (not rows or identity[0] != _NATIVE_IDENTITY):
            raise StorageError("Incompatible native provenance schema")
        return bool(rows)

    @contextmanager
    def _transaction(
        self,
        operation: str = "transaction",
        ordinal: int | None = None,
        raw_length: int | None = None,
    ) -> Iterator[None]:
        if self._rollback_failed:
            raise StorageError("Previous rollback failed; close store")
        stage = "begin"
        try:
            self.db.execute("BEGIN IMMEDIATE")
            stage = operation
            yield
            stage = "commit"
            self.db.execute("COMMIT")
        except BaseException as e:
            if self.db.in_transaction:
                try:
                    self.db.execute("ROLLBACK")
                except sqlite3.Error as rollback_error:
                    self._rollback_failed = True
                    raise StorageError("Backfill rollback failed") from rollback_error
            if not isinstance(e, Exception):
                raise
            detail = type(e).__name__
            if isinstance(e, StorageError):
                detail += f": {e}"
            elif isinstance(e, sqlite3.Error):
                detail += f" {getattr(e, 'sqlite_errorname', '')}"
                if str(e) in (
                    "disk full",
                    "database or disk is full",
                    "database is locked",
                ):
                    detail += f" {e}"
            raise StorageError(
                f"Backfill transaction failed stage={stage} ordinal={ordinal} raw_length={raw_length}: {detail}"
            ) from None

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
        native_digest: bytes | None = None,
    ) -> ImportResult:
        with self._transaction("write_flow", ordinal):
            self._registered(source, revision)
            row = self._previous(f.id)
            if row and row[1] is None:
                return ImportResult("existing")
            raw = tnetstring.dumps(f.get_state())
            digest = hashlib.sha256(raw).digest()
            return self._publish(
                f.id,
                digest,
                native_digest,
                source,
                revision,
                ordinal,
                row,
                len(raw),
                lambda sequence: self._write_payload(f, raw, sequence),
                check_space,
            )

    def write_raw(
        self,
        metadata: tuple,
        raw: BinaryIO,
        raw_length: int,
        digest: bytes,
        source: Source,
        revision: int,
        ordinal: int,
        *,
        chunk_size: int = 1024 * 1024,
        check_space: Callable[[int], None] | None = None,
        check: Callable[[], None] | None = None,
        resume_legacy: bool = False,
    ) -> ImportResult:
        """Publish an exact native record and its provenance in one transaction.

        resume_legacy trusts a previously committed v1 source position. Only use
        it after reading that ordinal from the same verified, immutable source:
        older importers hashed normalized state, not the original native bytes.
        """
        with self._transaction("write_raw", ordinal, raw_length):
            self._registered(source, revision)
            row = self._previous(metadata[0])
            if row and row[1] is None:
                return ImportResult("existing")
            return self._publish(
                metadata[0],
                digest,
                digest,
                source,
                revision,
                ordinal,
                row,
                raw_length,
                lambda sequence: self._write_chunked(
                    metadata,
                    raw,
                    raw_length,
                    digest,
                    sequence,
                    chunk_size=chunk_size,
                    check=check,
                ),
                check_space,
                resume_legacy=resume_legacy,
            )

    def _previous(self, flow_id: str) -> tuple | None:
        return self.db.execute(
            "SELECT flows.sequence,p.timestamp,p.suffix,p.path,p.revision,p.ordinal,p.digest,n.digest,flows.payload_version FROM flows LEFT JOIN backfill_provenance p ON flows.flow_id=p.flow_id LEFT JOIN backfill_native_identity n ON flows.flow_id=n.flow_id WHERE flows.flow_id=?",
            (flow_id,),
        ).fetchone()

    def _publish(
        self,
        flow_id: str,
        digest: bytes,
        native_digest: bytes | None,
        source: Source,
        revision: int,
        ordinal: int,
        row: tuple | None,
        raw_length: int,
        write: Callable[[int | None], int],
        check_space: Callable[[int], None] | None,
        *,
        resume_legacy: bool = False,
    ) -> ImportResult:
        order = (source.timestamp, source.suffix, source.path, revision, ordinal)
        status: Literal["inserted", "existing", "updated", "retained"] = "inserted"
        if row:
            same = digest == row[6] or (
                native_digest is not None and native_digest == row[7]
            )
            if resume_legacy and order == row[1:6] and row[7] is None and row[8] == 1:
                same = True
            if order == row[1:6] and not same:
                raise StorageError("Conflicting state at same source position")
            if order <= row[1:6]:
                if same and order == row[1:6] and native_digest is not None:
                    self.db.execute(
                        "INSERT INTO backfill_native_identity VALUES (?,?) ON CONFLICT(flow_id) DO UPDATE SET digest=excluded.digest",
                        (flow_id, native_digest),
                    )
                return ImportResult("existing" if same else "retained", not same)
            status = "existing" if same else "updated"
        if status != "existing":
            if check_space:
                check_space(max(raw_length, self._stored_size(row[0]) if row else 0))
            write(row[0] if row else None)
        self.db.execute(
            "INSERT INTO backfill_provenance VALUES (?,?,?,?,?,?,?) ON CONFLICT(flow_id) DO UPDATE SET timestamp=excluded.timestamp,suffix=excluded.suffix,path=excluded.path,revision=excluded.revision,ordinal=excluded.ordinal,digest=excluded.digest",
            (flow_id, *order, digest),
        )
        if native_digest is None:
            self.db.execute(
                "DELETE FROM backfill_native_identity WHERE flow_id=?", (flow_id,)
            )
        else:
            self.db.execute(
                "INSERT INTO backfill_native_identity VALUES (?,?) ON CONFLICT(flow_id) DO UPDATE SET digest=excluded.digest",
                (flow_id, native_digest),
            )
        return ImportResult(status, status == "updated")

    def finish_source(self, source: Source, revision: int, flow_count: int) -> None:
        with self._transaction():
            self._registered(source, revision)
            self.db.execute(
                "UPDATE backfill_manifest SET complete=1,flow_count=? WHERE path=?",
                (flow_count, source.path),
            )
