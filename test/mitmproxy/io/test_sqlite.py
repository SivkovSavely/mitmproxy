import asyncio
import hashlib
import io
import os
import sqlite3
import threading
from concurrent.futures import Future
from contextlib import closing

import pytest

from mitmproxy.io import sqlite
from mitmproxy.test import tflow


def test_chunked_roundtrip_and_v1_stream(tmp_path):
    path = tmp_path / "db"
    first, second = tflow.tflow(), tflow.tflow()
    raw = sqlite.tnetstring.dumps(second.get_state())
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(first.get_state())
        store.db.execute("BEGIN IMMEDIATE")
        sequence = store._write_chunked(
            sqlite._metadata(second),
            io.BytesIO(raw),
            len(raw),
            hashlib.sha256(raw).digest(),
            chunk_size=100,
        )
        store.db.execute("COMMIT")
        assert store._stored_size(sequence) > 32
        with sqlite.SQLiteFlowReader(path) as reader:
            assert b"".join(
                reader.iter_raw(first.id, chunk_size=7)
            ) == sqlite.tnetstring.dumps(first.get_state())
            assert b"".join(reader.iter_raw(second.id, chunk_size=7)) == raw
            with pytest.raises(sqlite.StorageError, match="iter_raw"):
                reader.get_flow(second.id)


@pytest.mark.parametrize(
    "damage",
    ["missing", "reorder", "digest", "truncated", "trailing", "version", "length"],
)
def test_chunked_corruption(tmp_path, damage):
    path = tmp_path / "db"
    f = tflow.tflow()
    raw = sqlite.tnetstring.dumps(f.get_state())
    with sqlite.SQLiteFlowStore(path) as store:
        store.db.execute("BEGIN IMMEDIATE")
        store._write_chunked(
            sqlite._metadata(f),
            io.BytesIO(raw),
            len(raw),
            hashlib.sha256(raw).digest(),
            chunk_size=100,
        )
        store.db.execute("COMMIT")
        if damage == "missing":
            store.db.execute("DELETE FROM flow_payload_chunks WHERE ordinal=1")
        elif damage == "reorder":
            store.db.execute(
                "UPDATE flow_payload_chunks SET ordinal=1000 WHERE ordinal=0"
            )
        elif damage == "digest":
            store.db.execute("UPDATE flows SET payload=zeroblob(32)")
        elif damage == "version":
            store.db.execute("UPDATE flows SET codec='unknown'")
        elif damage == "length":
            store.db.execute(
                "UPDATE flow_payload_chunks SET raw_length=raw_length+1 WHERE ordinal=0"
            )
        elif damage == "truncated":
            store.db.execute(
                "UPDATE flow_payload_chunks SET payload=substr(payload,1,length(payload)-1) WHERE ordinal=0"
            )
        else:
            store.db.execute(
                "UPDATE flow_payload_chunks SET payload=CAST(payload || x'00' AS BLOB) WHERE ordinal=0"
            )
        with sqlite.SQLiteFlowReader(path) as reader:
            with pytest.raises(sqlite.StorageError):
                b"".join(reader.iter_raw(f.id))


def test_chunked_rollback_and_snapshot(tmp_path):
    path = tmp_path / "db"
    f = tflow.tflow()
    raw = sqlite.tnetstring.dumps(f.get_state())
    digest = hashlib.sha256(raw).digest()
    with sqlite.SQLiteFlowStore(path) as store:
        store.db.execute("BEGIN IMMEDIATE")

        def interrupt():
            raise RuntimeError("interrupted")

        with pytest.raises(RuntimeError):
            store._write_chunked(
                sqlite._metadata(f),
                io.BytesIO(raw),
                len(raw),
                digest,
                chunk_size=100,
                check=interrupt,
            )
        store.db.execute("ROLLBACK")
        assert not store.db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='flow_payload_chunks'"
        ).fetchone()
        store.db.execute("BEGIN IMMEDIATE")
        sequence = store._write_chunked(
            sqlite._metadata(f), io.BytesIO(raw), len(raw), digest, chunk_size=100
        )
        store.db.execute("COMMIT")
        with sqlite.SQLiteFlowReader(path) as reader:
            stream = reader.iter_raw(f.id, chunk_size=10)
            first = next(stream)
            f.comment = "new"
            store.db.execute("BEGIN IMMEDIATE")
            store._write_payload(f, sqlite.tnetstring.dumps(f.get_state()), sequence)
            store.db.execute("COMMIT")
            assert first + b"".join(stream) == raw
            assert (
                store.db.execute("SELECT count(*) FROM flow_payload_chunks").fetchone()[
                    0
                ]
                == 0
            )
            with pytest.raises(sqlite.StorageError):
                list(reader.iter_raw("missing"))


def test_foreign_chunk_schema_rejected(tmp_path):
    path = tmp_path / "db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.db.execute("CREATE TABLE flow_payload_chunks (wrong)")
    with pytest.raises(sqlite.StorageError, match="schema"):
        sqlite.SQLiteFlowReader(path)


def test_chunked_validation_and_update_rollback(tmp_path):
    path = tmp_path / "db"
    f = tflow.tflow()
    raw = sqlite.tnetstring.dumps(f.get_state())
    digest = hashlib.sha256(raw).digest()
    with sqlite.SQLiteFlowStore(path) as store:
        sequence = store.write(f.get_state())
        assert store._stored_size(sequence) > 0
        with pytest.raises(sqlite.StorageError):
            store._stored_size(999)
        with pytest.raises(sqlite.StorageError, match="transaction"):
            store._write_chunked(sqlite._metadata(f), io.BytesIO(raw), len(raw), digest)
        store.db.execute("BEGIN IMMEDIATE")
        for data, length, expected, size in [
            (raw, len(raw), digest, 0),
            (raw, 0, digest, 100),
            (raw[:-1], len(raw), digest, 100),
            (raw + b"x", len(raw), digest, 100),
            (raw, len(raw), bytes(32), 100),
        ]:
            with pytest.raises(sqlite.StorageError):
                store._write_chunked(
                    sqlite._metadata(f),
                    io.BytesIO(data),
                    length,
                    expected,
                    sequence,
                    chunk_size=size,
                )
        store.db.execute("ROLLBACK")
        with sqlite.SQLiteFlowReader(path) as reader:
            assert b"".join(reader.iter_raw(f.id)) == raw
        store.db.execute("BEGIN IMMEDIATE")
        store._write_chunked(
            sqlite._metadata(f),
            io.BytesIO(raw),
            len(raw),
            digest,
            sequence,
            chunk_size=100,
        )
        store.db.execute("COMMIT")
        with pytest.raises(sqlite.StorageError, match="conflict"):
            store.write(f.get_state())
        with sqlite.SQLiteFlowReader(path) as reader:
            stream = reader.iter_raw(f.id, chunk_size=1)
            next(stream)
            with pytest.raises(sqlite.StorageError, match="transaction"):
                next(reader.iter_raw(f.id))
            stream.close()
            assert not reader.db.in_transaction
            with pytest.raises(ValueError):
                next(reader.iter_raw(f.id, chunk_size=0))


def test_compress_diagnostic_hides_error_data(tmp_path, monkeypatch):
    with sqlite.SQLiteFlowStore(tmp_path / "db") as store:

        def fail(*args, **kwargs):
            raise ValueError("sensitive payload")

        monkeypatch.setattr(sqlite.zstd, "compress", fail)
        with pytest.raises(sqlite.StorageError, match="stage=zstd.compress") as exc:
            store.write(tflow.tflow().get_state())
        assert "sensitive payload" not in str(exc.value)


@pytest.mark.parametrize("stage", ["compress", "chunk", "insert", "update"])
def test_chunked_failure_diagnostics(tmp_path, monkeypatch, stage):
    f = tflow.tflow()
    raw = sqlite.tnetstring.dumps(f.get_state())
    with sqlite.SQLiteFlowStore(tmp_path / "db") as store:
        sequence = store.write(f.get_state()) if stage == "update" else None
        store.db.execute("BEGIN IMMEDIATE")
        if stage == "compress":

            def fail(*args, **kwargs):
                raise ValueError("secret raw payload")

            monkeypatch.setattr(sqlite.zstd, "compress", fail)
        elif stage == "chunk":
            store.db.execute(sqlite._CHUNKS)
            store.db.execute(
                "CREATE TRIGGER fail_chunk BEFORE INSERT ON flow_payload_chunks BEGIN SELECT RAISE(ABORT, 'secret chunk payload'); END"
            )
        else:
            operation = "UPDATE" if stage == "update" else "INSERT"
            store.db.execute(
                f"CREATE TRIGGER fail_flow BEFORE {operation} ON flows BEGIN SELECT RAISE(ABORT, 'secret flow payload'); END"
            )
        with pytest.raises(sqlite.StorageError) as exc:
            store._write_chunked(
                sqlite._metadata(f),
                io.BytesIO(raw),
                len(raw),
                hashlib.sha256(raw).digest(),
                sequence,
                chunk_size=100,
            )
        message = str(exc.value)
        expected = {
            "compress": "zstd.compress.chunk",
            "chunk": "sqlite.chunk.insert",
            "insert": "sqlite.flow.insert",
            "update": "sqlite.flow.update",
        }[stage]
        assert f"stage={expected}" in message
        assert f"raw_length={len(raw)}" in message
        assert (
            "chunk_index=" in message
            and "chunk_raw_length=" in message
            and "compressed_length=" in message
        )
        assert ("ValueError" if stage == "compress" else "IntegrityError") in message
        assert "secret" not in message
        store.db.execute("ROLLBACK")


def test_chunk_schema_revalidated_after_open(tmp_path):
    f = tflow.tflow()
    raw = sqlite.tnetstring.dumps(f.get_state())
    with sqlite.SQLiteFlowStore(tmp_path / "db") as store:
        store.db.execute("CREATE TABLE flow_payload_chunks (wrong)")
        store.db.execute("BEGIN IMMEDIATE")
        with pytest.raises(sqlite.StorageError, match="schema"):
            store._write_chunked(
                sqlite._metadata(f),
                io.BytesIO(raw),
                len(raw),
                hashlib.sha256(raw).digest(),
            )
        store.db.execute("ROLLBACK")


@pytest.mark.parametrize("version", [1, 2])
def test_stream_output_is_capped_even_with_large_request(tmp_path, version):
    f = tflow.tflow()
    f.request.raw_content = b"x" * 300_000
    raw = sqlite.tnetstring.dumps(f.get_state())
    path = tmp_path / "db"
    with sqlite.SQLiteFlowStore(path) as store:
        if version == 1:
            store.write(f.get_state())
        else:
            store.db.execute("BEGIN IMMEDIATE")
            store._write_chunked(
                sqlite._metadata(f),
                io.BytesIO(raw),
                len(raw),
                hashlib.sha256(raw).digest(),
            )
            store.db.execute("COMMIT")
        with sqlite.SQLiteFlowReader(path) as reader:
            chunks = list(reader.iter_raw(f.id, chunk_size=10**9))
            assert max(map(len, chunks)) <= 65536
            assert b"".join(chunks) == raw


def test_reader_invalid_metadata_and_short_length(tmp_path):
    path = tmp_path / "db"
    f = tflow.tflow()
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(f.get_state())
        with sqlite.SQLiteFlowReader(path) as reader:
            for mutation in (
                "dictionary_id=1",
                "raw_length=-1",
                "codec='future'",
                "raw_length=1",
            ):
                store.db.execute(f"UPDATE flows SET {mutation}")
                with pytest.raises(sqlite.StorageError):
                    b"".join(reader.iter_raw(f.id))
                store.db.execute(
                    "UPDATE flows SET dictionary_id=NULL, codec='zstd', raw_length=?",
                    (len(sqlite.tnetstring.dumps(f.get_state())),),
                )


def test_missing_update_and_get_flow_race(tmp_path):
    path = tmp_path / "db"
    f = tflow.tflow()
    raw = sqlite.tnetstring.dumps(f.get_state())
    with sqlite.SQLiteFlowStore(path) as store:
        store.db.execute("BEGIN IMMEDIATE")
        with pytest.raises(sqlite.StorageError, match="not found"):
            store._write_chunked(
                sqlite._metadata(f),
                io.BytesIO(raw),
                len(raw),
                hashlib.sha256(raw).digest(),
                999,
            )
        store.db.execute("ROLLBACK")
        store.write(f.get_state())
        with sqlite.SQLiteFlowReader(path) as reader:
            real = reader.db

            class ConcurrentUpdate:
                def execute(self, sql, *args):
                    result = real.execute(sql, *args)
                    if sql.startswith("SELECT payload_version FROM"):
                        store.db.execute("UPDATE flows SET payload_version=2")
                    return result

            reader.db = ConcurrentUpdate()
            try:
                with pytest.raises(sqlite.StorageError, match="changed"):
                    reader.get_flow(f.id)
            finally:
                reader.db = real


def test_chunk_length_limit_and_real_disk_full(tmp_path):
    f = tflow.tflow()
    f.request.raw_content = os.urandom(128 * 1024)
    raw = sqlite.tnetstring.dumps(f.get_state())
    digest = hashlib.sha256(raw).digest()
    with sqlite.SQLiteFlowStore(tmp_path / "db") as store:
        store.db.execute("BEGIN IMMEDIATE")
        old = store.db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 2000)
        try:
            with pytest.raises(sqlite.StorageError, match="limit"):
                store._write_chunked(
                    sqlite._metadata(f), io.BytesIO(raw), len(raw), digest
                )
        finally:
            store.db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, old)
        store.db.execute("ROLLBACK")
        store.db.execute("BEGIN IMMEDIATE")
        store.db.execute(sqlite._CHUNKS)
        store.db.execute("COMMIT")
        pages = store.db.execute("PRAGMA page_count").fetchone()[0]
        store.db.execute(f"PRAGMA max_page_count={pages}")
        try:
            store.db.execute("BEGIN IMMEDIATE")
            with pytest.raises(
                sqlite.StorageError, match=r"stage=sqlite.chunk.insert.*full"
            ):
                store._write_chunked(
                    sqlite._metadata(f),
                    io.BytesIO(raw),
                    len(raw),
                    digest,
                    chunk_size=8192,
                )
        finally:
            if store.db.in_transaction:
                store.db.execute("ROLLBACK")
            store.db.execute("PRAGMA max_page_count=1073741823")


@pytest.mark.parametrize("limit", [4096, None])
def test_chunk_writer_caps_read_size(tmp_path, limit):
    class TrackingInput(io.BytesIO):
        def __init__(self, data):
            super().__init__(data)
            self.requested = []

        def read(self, size=-1):
            self.requested.append(size)
            return super().read(size)

    f = tflow.tflow()
    raw = b"x" * (sqlite._MAX_CHUNK + 100)
    source = TrackingInput(raw)
    with sqlite.SQLiteFlowStore(tmp_path / "db") as store:
        store.db.execute("BEGIN IMMEDIATE")
        if limit is not None:
            previous = store.db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, limit)
        try:
            store._write_chunked(
                sqlite._metadata(f),
                source,
                len(raw),
                hashlib.sha256(raw).digest(),
                chunk_size=10**9,
            )
        finally:
            if limit is not None:
                store.db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, previous)
        store.db.execute("ROLLBACK")
    expected = (limit - 1024) // 2 if limit else sqlite._MAX_CHUNK
    assert max(source.requested) == expected


def test_chunked_flow_binding_error_is_sanitized(tmp_path):
    f = tflow.tflow()
    raw = sqlite.tnetstring.dumps(f.get_state())
    metadata = (object(), *sqlite._metadata(f)[1:])
    with sqlite.SQLiteFlowStore(tmp_path / "db") as store:
        store.db.execute("BEGIN IMMEDIATE")
        with pytest.raises(
            sqlite.StorageError, match=r"stage=sqlite.flow.insert.*ProgrammingError"
        ) as exc:
            store._write_chunked(
                metadata, io.BytesIO(raw), len(raw), hashlib.sha256(raw).digest()
            )
        assert "not supported" not in str(exc.value)
        store.db.execute("ROLLBACK")


def test_roundtrip_and_duplicate(tmp_path):
    path = tmp_path / "flows.db"
    flows = tflow.tflows()
    expected = [f.get_state() for f in flows]
    with sqlite.SQLiteFlowStore(path) as store:
        for i, f in enumerate(flows, 1):
            assert store.write(f.get_state()) == i
        assert store.write(flows[0].get_state()) == 1
        flows[0].comment = "later"
        with pytest.raises(sqlite.StorageError, match="conflict"):
            store.write(flows[0].get_state())
        with sqlite.SQLiteFlowReader(path) as reader:
            assert [x.sequence for x in reader.newest(2)] == list(
                range(len(flows), len(flows) - 2, -1)
            )
            assert reader.get_flow(flows[0].id).get_state() == expected[0]
            assert reader.get_flow("missing") is None
    with sqlite.SQLiteFlowReader(path) as reader:
        assert [reader.get_flow(f.id).get_state() for f in flows] == expected
        assert [x.sequence for x in reader.older(3, 10)] == [2, 1]


def test_payload_helper_updates_sequence(tmp_path):
    path = tmp_path / "db"
    f = tflow.tflow()
    with sqlite.SQLiteFlowStore(path) as store:
        sequence = store.write(f.get_state())
        f.comment = "updated"
        store.db.execute("BEGIN IMMEDIATE")
        assert (
            store._write_payload(f, sqlite.tnetstring.dumps(f.get_state()), sequence)
            == sequence
        )
        store.db.execute("COMMIT")
        with sqlite.SQLiteFlowReader(path) as reader:
            assert reader.get_flow(f.id).comment == "updated"
            assert reader.newest(1)[0].sequence == sequence


def test_read_only_validation_of_empty_database(tmp_path):
    path = tmp_path / "db"
    path.touch()
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        sqlite._validate(db, allow_empty=True)
        with pytest.raises(sqlite.StorageError, match="schema"):
            sqlite._validate(db)
    assert path.stat().st_size == 0
    with closing(sqlite3.connect(path)) as db:
        db.execute("CREATE TABLE unrelated (keep)")
        with pytest.raises(sqlite.StorageError, match="schema"):
            sqlite._validate(db, allow_empty=True)


def test_close_empty_writer_before_start(tmp_path):
    path = tmp_path / "db"
    start: Future[None] = Future()
    writer = sqlite.SQLiteFlowWriter(path, after=start)
    writer.close()
    start.set_result(None)
    writer.closed.result(timeout=10)
    writer.ready.result(timeout=10)
    assert not path.exists()


def test_conflicting_snapshot_after_reopen_and_writer_recovery(tmp_path):
    path = tmp_path / "db"
    f = tflow.tflow()
    with sqlite.SQLiteFlowStore(path) as store:
        assert store.write(f.get_state()) == 1
    writer = sqlite.SQLiteFlowWriter(path)
    try:
        writer.ready.result(timeout=10)
        assert writer.submit(f).result(timeout=10) == 1
        f.comment = "sensitive body content"
        with pytest.raises(sqlite.StorageError, match=f.id) as error:
            writer.submit(f).result(timeout=10)
        assert "conflict" in str(error.value)
        assert "sensitive body content" not in str(error.value)
        assert writer.submit(tflow.tflow()).result(timeout=10) == 2
    finally:
        writer.close().result(timeout=10)
    with sqlite.SQLiteFlowReader(path) as reader:
        assert reader.get_flow(f.id).comment != f.comment


def test_untrusted_writable_directory_rejected(tmp_path):
    directory = tmp_path / "shared"
    directory.mkdir(mode=0o777)
    directory.chmod(0o777)
    path = directory / "db"
    with pytest.raises(sqlite.StorageError, match="directory"):
        sqlite.SQLiteFlowStore(path)
    assert not path.exists()


def test_unsafe_paths_and_sidecars_rejected(tmp_path, monkeypatch):
    with pytest.raises(sqlite.StorageError, match="directory"):
        sqlite.SQLiteFlowStore(tmp_path / ".." / "db")
    path = tmp_path / "db"
    path.touch()
    fstat = os.fstat

    def unowned(fd):
        info = fstat(fd)

        class Stat:
            st_mode = info.st_mode
            st_uid = os.geteuid() + 1

        return Stat()

    with monkeypatch.context() as patch:
        patch.setattr(sqlite.os, "fstat", unowned)
        with pytest.raises(sqlite.StorageError, match="not owned"):
            sqlite.SQLiteFlowStore(path)
    (tmp_path / "db-wal").mkdir()
    with pytest.raises(sqlite.StorageError, match="sidecar"):
        sqlite.SQLiteFlowStore(path)


def test_symlinked_ancestor_and_shared_sticky_parent_rejected(tmp_path):
    directory = tmp_path / "real"
    directory.mkdir()
    (tmp_path / "alias").symlink_to(directory, target_is_directory=True)
    with pytest.raises(sqlite.StorageError, match="directory"):
        sqlite.SQLiteFlowStore(tmp_path / "alias" / "db")
    directory.chmod(0o1777)
    with pytest.raises(sqlite.StorageError, match="directory"):
        sqlite.SQLiteFlowStore(directory / "db")
    assert not (directory / "db").exists()


def test_duplicate_compares_uncompressed_bytes(tmp_path):
    path = tmp_path / "db"
    f = tflow.tflow()
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(f.get_state())
        raw = sqlite.tnetstring.dumps(f.get_state())
        store.db.execute(
            "UPDATE flows SET payload=?", (sqlite.zstd.compress(raw, level=1),)
        )
        assert store.write(f.get_state()) == 1


def test_actual_sqlite_full_recovers(tmp_path):
    path = tmp_path / "db"
    first, failed, last = (tflow.tflow() for _ in range(3))
    failed.request.raw_content = os.urandom(128 * 1024)
    with sqlite.SQLiteFlowStore(path) as store:
        assert store.write(first.get_state()) == 1
        pages = store.db.execute("PRAGMA page_count").fetchone()[0]
        store.db.execute(f"PRAGMA max_page_count={pages}")
        with pytest.raises(sqlite.StorageError, match="full"):
            store.write(failed.get_state())
        store.db.execute("PRAGMA max_page_count=1073741823")
        assert store.write(last.get_state()) == 2
        with sqlite.SQLiteFlowReader(path) as reader:
            assert reader.get_flow(first.id).id == first.id
            assert reader.get_flow(failed.id) is None
            assert reader.get_flow(last.id).id == last.id


def test_corrupt_payload_is_not_empty_flow(tmp_path):
    path = tmp_path / "flows.db"
    f = tflow.tflow()
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(f.get_state())
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("UPDATE flows SET payload = ?", (b"corrupt",))
    with sqlite.SQLiteFlowReader(path) as reader:
        assert reader.newest(1)[0].flow_id == f.id
        with pytest.raises(sqlite.StorageError, match=f.id):
            reader.get_flow(f.id)


def test_deferred_bounded_writer_snapshot_and_close(tmp_path):
    path = tmp_path / "flows.db"
    predecessor = Future()
    writer = sqlite.SQLiteFlowWriter(path, queue_size=1, after=predecessor)
    f = tflow.tflow()
    result = writer.submit(f)
    f.comment = "changed"
    with pytest.raises(sqlite.WriterFull):
        writer.submit(tflow.tflow())
    closed = writer.close()
    assert not closed.done()
    predecessor.set_result(None)
    assert result.result(timeout=10) == 1
    closed.result(timeout=10)
    with sqlite.SQLiteFlowReader(path) as reader:
        assert reader.get_flow(f.id).comment != "changed"


def test_recovery_after_transaction_failure(tmp_path):
    path = tmp_path / "flows.db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.db.execute(
            "CREATE TRIGGER reject_insert BEFORE INSERT ON flows BEGIN SELECT RAISE(ABORT, 'disk full'); END"
        )
        with pytest.raises(sqlite.StorageError, match="disk full"):
            store.write(tflow.tflow().get_state())
        store.db.execute("DROP TRIGGER reject_insert")
        assert store.write(tflow.tflow().get_state()) == 1


def test_invalid_schema_not_modified(tmp_path):
    path = tmp_path / "other.db"
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("CREATE TABLE unrelated (x INTEGER)")
    before = path.read_bytes()
    with pytest.raises(sqlite.StorageError, match="schema"):
        sqlite.SQLiteFlowStore(path)
    assert path.read_bytes() == before
    with pytest.raises(sqlite.StorageError, match="schema"):
        sqlite.SQLiteFlowReader(path)


@pytest.mark.parametrize("interrupted", ["zero", "rolled_back"])
def test_recover_interrupted_empty_initialization(tmp_path, interrupted):
    path = tmp_path / "db"
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    os.close(fd)
    if interrupted == "rolled_back":
        with closing(sqlite3.connect(path, isolation_level=None)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(sqlite._SCHEMA)
            db.execute(f"PRAGMA application_id={sqlite._APP_ID}")
            db.execute("PRAGMA user_version=1")
            db.execute("ROLLBACK")
    f = tflow.tflow()
    with sqlite.SQLiteFlowStore(path) as store:
        assert store.write(f.get_state()) == 1
    with sqlite.SQLiteFlowReader(path) as reader:
        assert reader.get_flow(f.id).id == f.id


def test_schema_initialization_failure_can_be_reopened(tmp_path, monkeypatch):
    path = tmp_path / "db"
    connect = sqlite.sqlite3.connect

    class FailSchemaCommit:
        def __init__(self, *args, **kwargs):
            self.db = connect(*args, **kwargs)

        def execute(self, sql, *args):
            if sql == "COMMIT":
                raise sqlite3.OperationalError("injected init failure")
            return self.db.execute(sql, *args)

        def close(self):
            self.db.close()

    with monkeypatch.context() as patch:
        patch.setattr(sqlite.sqlite3, "connect", FailSchemaCommit)
        with pytest.raises(sqlite.StorageError, match="injected init failure"):
            sqlite.SQLiteFlowStore(path)
    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute("PRAGMA application_id").fetchone()[0] == 0
        assert db.execute("PRAGMA user_version").fetchone()[0] == 0
        assert db.execute("SELECT name FROM sqlite_master").fetchall() == []
    with sqlite.SQLiteFlowStore(path) as store:
        assert store.write(tflow.tflow().get_state()) == 1


@pytest.mark.parametrize("other_schema", [False, True])
def test_initialization_checks_schema_after_acquiring_lock(
    tmp_path, monkeypatch, other_schema
):
    path = tmp_path / "db"
    connect = sqlite3.connect
    blocker = connect(path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    attempting = threading.Event()
    outcome = []

    class SignalBegin:
        def __init__(self, *args, **kwargs):
            self.db = connect(*args, **kwargs)

        def execute(self, sql, *args):
            if sql == "BEGIN IMMEDIATE":
                attempting.set()
            return self.db.execute(sql, *args)

        def close(self):
            self.db.close()

    def initialize():
        try:
            with sqlite.SQLiteFlowStore(path):
                outcome.append("initialized")
        except Exception as e:
            outcome.append(e)

    monkeypatch.setattr(sqlite.sqlite3, "connect", SignalBegin)
    worker = threading.Thread(target=initialize, daemon=True)
    worker.start()
    try:
        assert attempting.wait(5)
        if other_schema:
            blocker.execute("CREATE TABLE unrelated (x INTEGER)")
        else:
            blocker.execute(sqlite._SCHEMA)
            blocker.execute(f"PRAGMA application_id={sqlite._APP_ID}")
            blocker.execute("PRAGMA user_version=1")
        blocker.execute("COMMIT")
    finally:
        blocker.close()
        worker.join(10)
    assert not worker.is_alive()
    if other_schema:
        assert len(outcome) == 1 and isinstance(outcome[0], sqlite.StorageError)
        with closing(connect(path)) as db, db:
            assert db.execute("SELECT name FROM sqlite_master").fetchall() == [
                ("unrelated",)
            ]
    else:
        assert outcome == ["initialized"]


@pytest.mark.parametrize("pragma", ["application_id", "user_version"])
def test_empty_database_with_nonzero_version_is_rejected(tmp_path, pragma):
    path = tmp_path / "db"
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(f"PRAGMA {pragma}=42")
    before = path.read_bytes()
    with pytest.raises(sqlite.StorageError, match="schema"):
        sqlite.SQLiteFlowStore(path)
    assert path.read_bytes() == before


def test_future_schema_rejected_without_modification(tmp_path):
    path = tmp_path / "db"
    with sqlite.SQLiteFlowStore(path):
        pass
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("PRAGMA user_version=999")
    before = path.read_bytes()
    with pytest.raises(sqlite.StorageError, match="schema"):
        sqlite.SQLiteFlowStore(path)
    assert path.read_bytes() == before
    with pytest.raises(sqlite.StorageError, match="schema"):
        sqlite.SQLiteFlowReader(path)


def test_http_metadata_and_full_snapshot(tmp_path):
    f = tflow.tflow(resp=True)
    f.request.method = "POST"
    f.request.host = "example.com"
    f.request.path = "/resource?x=1"
    f.request.raw_content = b"abc"
    f.response.status_code = 201
    f.response.raw_content = b"response"
    f.request.headers["X-Test"] = "request-header"
    f.response.headers["X-Test"] = "response-header"
    f.comment = "comment"
    f.marked = ":star:"
    f.metadata["custom"] = ["value"]
    f.backup()
    f.comment = "changed after backup"
    expected = f.get_state()
    missing = tflow.tflow(resp=True)
    missing.request.raw_content = None
    missing.response.raw_content = None
    path = tmp_path / "db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(f.get_state())
        store.write(missing.get_state())
        with sqlite.SQLiteFlowReader(path) as reader:
            absent, metadata = reader.newest(2)
            assert (
                metadata.method,
                metadata.host,
                metadata.path,
                metadata.status_code,
            ) == ("POST", "example.com", "/resource?x=1", 201)
            assert (metadata.request_size, metadata.response_size) == (3, 8)
            assert (absent.request_size, absent.response_size) == (None, None)
            restored = reader.get_flow(f.id)
            assert restored.get_state() == expected
            assert restored.request.headers["X-Test"] == "request-header"
            assert restored.response.headers["X-Test"] == "response-header"
            assert restored.request.raw_content == b"abc"
            assert restored.response.raw_content == b"response"
            assert restored.comment == "changed after backup"
            assert restored.marked == ":star:"
            assert restored.metadata == {**f.metadata}
            assert restored._backup == f._backup
            assert restored.client_conn.get_state() == f.client_conn.get_state()
            assert restored.server_conn.get_state() == f.server_conn.get_state()


def test_large_flow_and_permissions(tmp_path):
    path = tmp_path / "flows.db"
    f = tflow.tflow()
    f.request.raw_content = b"x" * (17 * 1024 * 1024)
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(f.get_state())
        assert os.stat(path).st_mode & 0o777 == 0o600
        for suffix in ("-wal", "-shm"):
            assert os.stat(str(path) + suffix).st_mode & 0o777 == 0o600
        with sqlite.SQLiteFlowReader(path) as reader:
            assert reader.get_flow(f.id).request.raw_content == f.request.raw_content
            assert reader.newest(1)[0].request_size == len(f.request.raw_content)


def test_cancelled_jobs_and_failed_startup(tmp_path):
    after = Future()
    writer = sqlite.SQLiteFlowWriter(tmp_path / "missing" / "db", after=after)
    first = writer.submit(tflow.tflow())
    cancelled = writer.submit(tflow.tflow())
    try:
        assert not writer.ready.cancel()
        assert not writer.closed.cancel()
        assert not cancelled.cancel()
    finally:
        after.set_result(None)
    with pytest.raises(sqlite.StorageError):
        writer.ready.result(timeout=10)
    with pytest.raises(sqlite.StorageError):
        first.result(timeout=10)
    with pytest.raises(sqlite.StorageError):
        cancelled.result(timeout=10)
    writer.closed.result(timeout=10)
    with pytest.raises(sqlite.StorageError):
        writer.submit(tflow.tflow())


def test_symlink_rejected(tmp_path):
    path = tmp_path / "target"
    path.write_text("untouched")
    link = tmp_path / "link"
    link.symlink_to(path)
    with pytest.raises(sqlite.StorageError):
        sqlite.SQLiteFlowStore(link)
    assert path.read_text() == "untouched"


@pytest.mark.parametrize(
    "change",
    [
        "version",
        "codec",
        "dictionary",
        "length",
        "trailing",
        "checksum",
        "truncated",
        "state",
        "id",
    ],
)
def test_invalid_payload_variants(tmp_path, change):
    path = tmp_path / "flows.db"
    f = tflow.tflow()
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(f.get_state())
        if change == "version":
            store.db.execute("UPDATE flows SET payload_version=2")
        elif change == "codec":
            store.db.execute("UPDATE flows SET codec='future'")
        elif change == "dictionary":
            store.db.execute("UPDATE flows SET dictionary_id=1")
        elif change == "length":
            store.db.execute("UPDATE flows SET raw_length=raw_length-1")
        elif change in ("trailing", "checksum"):
            payload = store.db.execute("SELECT payload FROM flows").fetchone()[0]
            payload = (
                payload + b"trailing"
                if change == "trailing"
                else payload[:-1] + bytes([payload[-1] ^ 1])
            )
            store.db.execute("UPDATE flows SET payload=?", (payload,))
        elif change == "truncated":
            store.db.execute(
                "UPDATE flows SET payload=substr(payload,1,length(payload)-1)"
            )
        elif change == "state":
            raw = sqlite.tnetstring.dumps([1])
            store.db.execute(
                "UPDATE flows SET payload=?, raw_length=?",
                (sqlite.zstd.compress(raw), len(raw)),
            )
        else:
            store.db.execute("UPDATE flows SET flow_id='different'")
    with sqlite.SQLiteFlowReader(path) as reader:
        with pytest.raises(sqlite.StorageError):
            reader.get_flow("different" if change == "id" else f.id)
        with pytest.raises(ValueError):
            reader.newest(-1)


def test_writer_runs_and_recovers(tmp_path, monkeypatch):
    path = tmp_path / "flows.db"
    original = sqlite.SQLiteFlowStore.write

    def write(store, state):
        if state["comment"] == "fail":
            raise sqlite.StorageError("failed")
        return original(store, state)

    monkeypatch.setattr(sqlite.SQLiteFlowStore, "write", write)
    writer = sqlite.SQLiteFlowWriter(path)
    writer.ready.result(timeout=10)
    f = tflow.tflow()
    f.comment = "fail"
    with pytest.raises(sqlite.StorageError):
        writer.submit(f).result(timeout=10)
    f.comment = "ok"
    assert writer.submit(f).result(timeout=10) == 1
    writer.close().result(timeout=10)
    assert writer.close() is writer.closed


def test_cancel_queued_success(tmp_path):
    after = Future()
    path = tmp_path / "db"
    writer = sqlite.SQLiteFlowWriter(path, after=after)
    f = tflow.tflow()
    receipt = writer.submit(f)
    try:
        assert not receipt.cancel()
        assert not writer.ready.cancel()
        assert not writer.closed.cancel()
    finally:
        writer.close()
        after.set_result(None)
        writer.closed.result(timeout=10)
    assert receipt.result(timeout=10) == 1
    with sqlite.SQLiteFlowReader(path) as reader:
        assert reader.get_flow(f.id).id == f.id


async def test_asyncio_wrapper_cancellation_does_not_cancel_receipt(tmp_path):
    predecessor = Future()
    path = tmp_path / "db"
    writer = sqlite.SQLiteFlowWriter(path, after=predecessor)
    f = tflow.tflow()
    receipt = writer.submit(f)
    try:
        wrappers = [
            asyncio.wrap_future(future)
            for future in (writer.ready, writer.closed, receipt)
        ]
        for wrapper in wrappers:
            wrapper.cancel()
        await asyncio.sleep(0)
        assert not writer.ready.cancelled()
        assert not writer.closed.cancelled()
        assert not receipt.cancelled()
    finally:
        writer.close()
        predecessor.set_result(None)
        await asyncio.wait_for(asyncio.shield(asyncio.wrap_future(writer.closed)), 10)
    assert receipt.result() == 1
    with sqlite.SQLiteFlowReader(path) as reader:
        assert reader.get_flow(f.id).id == f.id


def test_other_error_paths(tmp_path):
    with pytest.raises(ValueError):
        sqlite.SQLiteFlowWriter(tmp_path / "db", queue_size=0)
    with pytest.raises(sqlite.StorageError):
        sqlite.SQLiteFlowReader(tmp_path)
    (tmp_path / "db-wal").symlink_to(tmp_path / "nope")
    with pytest.raises(sqlite.StorageError):
        sqlite.SQLiteFlowStore(tmp_path / "db")


def test_metadata_error(tmp_path):
    path = tmp_path / "db"
    with sqlite.SQLiteFlowStore(path):
        pass
    reader = sqlite.SQLiteFlowReader(path)
    reader.close()
    with pytest.raises(sqlite.StorageError):
        reader.newest(1)


def test_serialization_error_does_not_expose_payload(tmp_path):
    class Secret:
        def __repr__(self):
            return "sensitive-payload"

    f = tflow.tflow()
    f.metadata["secret"] = Secret()
    with sqlite.SQLiteFlowStore(tmp_path / "db") as store:
        with pytest.raises(sqlite.StorageError) as exc:
            store.write(f.get_state())
        assert "sensitive-payload" not in str(exc.value)


def test_websocket_response_sizes(tmp_path):
    f = tflow.tflow(resp=True, ws=True)
    with sqlite.SQLiteFlowStore(tmp_path / "db") as store:
        store.write(f.get_state())
        with sqlite.SQLiteFlowReader(tmp_path / "db") as reader:
            metadata = reader.newest(1)[0]
            assert metadata.response_size == len(f.response.raw_content) + sum(
                len(m.content) for m in f.websocket.messages if not m.from_client
            )


def test_existing_sidecars(tmp_path):
    path = tmp_path / "db"
    with sqlite.SQLiteFlowStore(path) as first:
        first.write(tflow.tflow().get_state())
        with sqlite.SQLiteFlowStore(path) as second:
            assert second.write(tflow.tflow().get_state()) == 2


def test_connection_errors(tmp_path, monkeypatch):
    path = tmp_path / "db"
    with sqlite.SQLiteFlowStore(path):
        pass

    def fail(*args, **kwargs):
        raise sqlite3.OperationalError("connection failed")

    monkeypatch.setattr(sqlite.sqlite3, "connect", fail)
    with pytest.raises(sqlite.StorageError, match="connection failed"):
        sqlite.SQLiteFlowStore(path)
    with pytest.raises(sqlite.StorageError, match="connection failed"):
        sqlite.SQLiteFlowReader(path)


def test_worker_close_error(tmp_path, monkeypatch):
    original = sqlite.SQLiteFlowStore.close

    def close(store):
        original(store)
        raise sqlite.StorageError("close failed")

    monkeypatch.setattr(sqlite.SQLiteFlowStore, "close", close)
    writer = sqlite.SQLiteFlowWriter(tmp_path / "db")
    writer.ready.result(timeout=10)
    with pytest.raises(sqlite.StorageError, match="close failed"):
        writer.close().result(timeout=10)


def test_wal_unavailable(tmp_path, monkeypatch):
    connect = sqlite.sqlite3.connect

    class Connection:
        def __init__(self, *args, **kwargs):
            self.db = connect(*args, **kwargs)

        def execute(self, sql, *args):
            if sql == "PRAGMA journal_mode=WAL":
                return self.db.execute("SELECT 'delete'")
            return self.db.execute(sql, *args)

        def close(self):
            self.db.close()

    monkeypatch.setattr(sqlite.sqlite3, "connect", Connection)
    with pytest.raises(sqlite.StorageError, match="WAL"):
        sqlite.SQLiteFlowStore(tmp_path / "db")


def test_commit_failure_rolls_back_and_recovers(tmp_path):
    path = tmp_path / "db"
    first, failed, last = (tflow.tflow() for _ in range(3))
    with sqlite.SQLiteFlowStore(path) as store:
        assert store.write(first.get_state()) == 1
        real = store.db

        class FailCommit:
            def __getattr__(self, name):
                return getattr(real, name)

            def execute(self, sql, *args):
                if sql == "COMMIT":
                    raise sqlite3.OperationalError("injected commit failure")
                return real.execute(sql, *args)

        store.db = FailCommit()
        with pytest.raises(sqlite.StorageError, match="commit failure"):
            store.write(failed.get_state())
        store.db = real
        assert not real.in_transaction
        with sqlite.SQLiteFlowReader(path) as reader:
            assert reader.get_flow(first.id).id == first.id
            assert reader.get_flow(failed.id) is None
        assert store.write(last.get_state()) == 2


def test_rollback_failure_is_reported_as_storage_error(tmp_path):
    with sqlite.SQLiteFlowStore(tmp_path / "db") as store:
        real = store.db

        class FailRollback:
            def __getattr__(self, name):
                return getattr(real, name)

            def execute(self, sql, *args):
                if sql in ("COMMIT", "ROLLBACK"):
                    raise sqlite3.OperationalError(f"{sql} failed")
                return real.execute(sql, *args)

        store.db = FailRollback()
        try:
            with pytest.raises(sqlite.StorageError, match="rollback"):
                store.write(tflow.tflow().get_state())
            with pytest.raises(sqlite.StorageError, match="previous rollback failed"):
                store.write(tflow.tflow().get_state())
        finally:
            store.db = real
            if real.in_transaction:
                real.execute("ROLLBACK")


def test_fifo_path_rejected_without_blocking(tmp_path):
    path = tmp_path / "fifo"
    os.mkfifo(path)
    finished = threading.Event()
    errors = []

    def open_fifo():
        try:
            sqlite.SQLiteFlowReader(path)
        except Exception as e:
            errors.append(e)
        finally:
            finished.set()

    thread = threading.Thread(target=open_fifo, daemon=True)
    thread.start()
    assert finished.wait(5)
    assert isinstance(errors[0], sqlite.StorageError)


def test_sqlite_length_limit_rollback_and_recovery(tmp_path):
    path = tmp_path / "db"
    first, failed, last = (tflow.tflow() for _ in range(3))
    failed.request.raw_content = os.urandom(8192)
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(first.get_state())
        old_limit = store.db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 4096)
        try:
            with pytest.raises(sqlite.StorageError, match="too big"):
                store.write(failed.get_state())
        finally:
            store.db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, old_limit)
        assert store.write(last.get_state()) == 2
        with sqlite.SQLiteFlowReader(path) as reader:
            assert reader.get_flow(first.id).id == first.id
            assert reader.get_flow(failed.id) is None
            assert reader.get_flow(last.id).id == last.id


def test_reopen_keyset_and_query_plans(tmp_path):
    path = tmp_path / "db"
    flows = [tflow.tflow() for _ in range(4)]
    with sqlite.SQLiteFlowStore(path) as store:
        for f in flows[:3]:
            store.write(f.get_state())
        assert store.db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert store.db.execute("PRAGMA synchronous").fetchone()[0] == 2
    with sqlite.SQLiteFlowStore(path) as store:
        statements = []
        store.db.set_trace_callback(statements.append)
        with sqlite.SQLiteFlowReader(path) as reader:
            page = reader.newest(2)
            assert [m.sequence for m in page] == [3, 2]
            assert store.write(flows[3].get_state()) == 4
            assert [m.sequence for m in reader.older(page[-1].sequence, 10)] == [1]
            assert [m.sequence for m in reader.newest(10)] == [4, 3, 2, 1]
            plan_id = reader.db.execute(
                "EXPLAIN QUERY PLAN SELECT payload FROM flows WHERE flow_id=?",
                (flows[0].id,),
            ).fetchone()[3]
            plan_seq = reader.db.execute(
                "EXPLAIN QUERY PLAN SELECT sequence FROM flows WHERE sequence < ? ORDER BY sequence DESC LIMIT ?",
                (3, 2),
            ).fetchone()[3]
            assert "INDEX" in plan_id
            assert "INTEGER PRIMARY KEY" in plan_seq
        store.write(tflow.tflow().get_state())
        assert sum(s == "BEGIN IMMEDIATE" for s in statements) == 2
        assert sum(s == "COMMIT" for s in statements) == 2


@pytest.mark.parametrize("stage", ["compress", "commit"])
async def test_worker_does_not_block_event_loop(tmp_path, monkeypatch, stage):
    path = tmp_path / "db"
    entered = threading.Event()
    release = threading.Event()
    original_compress = sqlite.zstd.compress
    original_write = sqlite.SQLiteFlowStore.write

    def block():
        entered.set()
        if not release.wait(10):
            raise TimeoutError("test barrier timed out")

    def compress(*args, **kwargs):
        if stage == "compress":
            block()
        return original_compress(*args, **kwargs)

    def write(store, state):
        real = store.db

        class Connection:
            def __getattr__(self, name):
                return getattr(real, name)

            def execute(self, sql, *args):
                if sql == "COMMIT" and stage == "commit":
                    block()
                return real.execute(sql, *args)

        store.db = Connection()
        try:
            return original_write(store, state)
        finally:
            store.db = real

    monkeypatch.setattr(sqlite.zstd, "compress", compress)
    monkeypatch.setattr(sqlite.SQLiteFlowStore, "write", write)
    writer = sqlite.SQLiteFlowWriter(path)
    try:
        await asyncio.wait_for(asyncio.shield(asyncio.wrap_future(writer.ready)), 10)
        f = tflow.tflow()
        receipt = writer.submit(f)
        assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 5), 10)
        heartbeat = asyncio.Event()
        asyncio.get_running_loop().call_soon(heartbeat.set)
        await asyncio.wait_for(heartbeat.wait(), 5)
        assert not receipt.done()
        with sqlite.SQLiteFlowReader(path) as reader:
            assert reader.get_flow(f.id) is None
        release.set()
        assert (
            await asyncio.wait_for(asyncio.shield(asyncio.wrap_future(receipt)), 10)
            == 1
        )
    finally:
        release.set()
        await asyncio.wait_for(asyncio.shield(asyncio.wrap_future(writer.close())), 10)
