import os
import sqlite3

import pytest

from mitmproxy.io import sqlite
from mitmproxy.io.sqlite_backfill import Source
from mitmproxy.io.sqlite_backfill import SQLiteBackfillStore
from mitmproxy.test import tflow


def source(tmp_path, name="archive", suffix=0):
    path = tmp_path / name
    path.write_bytes(b"archive")
    return Source.from_stat(path, "2026-01-01_00", suffix, path.stat())


def test_resume_update_and_complete(tmp_path):
    src = source(tmp_path)
    f = tflow.tflow()
    with SQLiteBackfillStore(tmp_path / "db") as store:
        rev = store.start_source(src)
        assert store.write_flow(f, src, rev, 0).status == "inserted"
        assert store.start_source(src) == rev
        assert store.write_flow(f, src, rev, 0).status == "existing"
        f.comment = "updated"
        assert store.write_flow(f, src, rev, 1).status == "updated"
        assert store.write_flow(f, src, rev, 0).status == "existing"
        store.finish_source(src, rev, 2)
        assert store.start_source(src) is None
        with sqlite.SQLiteFlowReader(tmp_path / "db") as reader:
            assert reader.get_flow(f.id).comment == "updated"
            assert reader.newest(1)[0].sequence == 1


def test_non_owned_skip_never_reads_payload_or_serializes(tmp_path, monkeypatch):
    src = source(tmp_path)
    f = tflow.tflow()
    with sqlite.SQLiteFlowStore(tmp_path / "db") as store:
        store.write(f.get_state())
    with SQLiteBackfillStore(tmp_path / "db") as store:
        rev = store.start_source(src)

        def authorize(action, table, column, db, trigger):
            if (
                action == sqlite3.SQLITE_READ
                and table == "flows"
                and column == "payload"
            ):
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        store.db.set_authorizer(authorize)
        monkeypatch.setattr(f, "get_state", lambda: pytest.fail("serialized"))
        assert store.write_flow(f, src, rev, 0).status == "existing"


def test_stale_generation_and_rollback(tmp_path):
    src = source(tmp_path)
    f = tflow.tflow()
    with SQLiteBackfillStore(tmp_path / "db") as store:
        rev = store.start_source(src)

        def fail(size):
            raise RuntimeError("no space")

        with pytest.raises(sqlite.StorageError):
            store.write_flow(f, src, rev, 0, check_space=fail)
        assert store.db.execute("SELECT count(*) FROM flows").fetchone()[0] == 0
        (tmp_path / "archive").write_bytes(b"changed")
        new = Source.from_stat(
            tmp_path / "archive", src.timestamp, 0, (tmp_path / "archive").stat()
        )
        newrev = store.start_source(new)
        assert newrev > rev
        with pytest.raises(sqlite.StorageError):
            store.finish_source(src, rev, 0)
        assert store.write_flow(f, new, newrev, 0).status == "inserted"


def test_ordering_conflict_and_identical_advances(tmp_path):
    low = source(tmp_path, "low", 2)
    high = source(tmp_path, "high", 10)
    f = tflow.tflow()
    with SQLiteBackfillStore(tmp_path / "db") as store:
        a, b = store.start_source(low), store.start_source(high)
        assert store.write_flow(f, low, a, 0).status == "inserted"
        assert store.write_flow(f, high, b, 0).status == "existing"
        f.comment = "older"
        result = store.write_flow(f, low, a, 1)
        assert (result.status, result.conflicting) == ("retained", True)
        with pytest.raises(sqlite.StorageError, match="same source position"):
            store.write_flow(f, high, b, 0)
        result = store.write_flow(f, high, b, 1)
        assert (result.status, result.conflicting) == ("updated", True)


def test_auxiliary_schema_rejected_transactionally(tmp_path):
    path = tmp_path / "db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.db.execute("CREATE TABLE backfill_manifest (wrong TEXT)")
    with pytest.raises(sqlite.StorageError, match="schema"):
        SQLiteBackfillStore(path)
    with sqlite.SQLiteFlowStore(path) as store:
        assert (
            store.db.execute(
                "SELECT name FROM sqlite_master WHERE name='backfill_provenance'"
            ).fetchone()
            is None
        )


def test_payload_and_provenance_atomic_on_failure(tmp_path):
    src = source(tmp_path)
    f = tflow.tflow()
    with SQLiteBackfillStore(tmp_path / "db") as store:
        rev = store.start_source(src)
        store.db.execute(
            "CREATE TRIGGER reject_provenance BEFORE INSERT ON backfill_provenance BEGIN SELECT RAISE(ABORT, 'disk full'); END"
        )
        with pytest.raises(sqlite.StorageError, match="disk full"):
            store.write_flow(f, src, rev, 0)
        assert store.db.execute("SELECT count(*) FROM flows").fetchone()[0] == 0
        store.db.execute("DROP TRIGGER reject_provenance")
        assert store.write_flow(f, src, rev, 0).status == "inserted"


def test_missing_source_and_lost_registration(tmp_path):
    src = source(tmp_path)
    with (
        SQLiteBackfillStore(tmp_path / "db") as first,
        SQLiteBackfillStore(tmp_path / "db") as second,
    ):
        rev = first.start_source(src)
        (tmp_path / "archive").write_bytes(b"different")
        new = Source.from_stat(
            src.path, src.timestamp, src.suffix, (tmp_path / "archive").stat()
        )
        assert second.start_source(new) > rev
        with pytest.raises(sqlite.StorageError):
            first.write_flow(tflow.tflow(), src, rev, 0)
        (tmp_path / "archive").unlink()
        with pytest.raises(sqlite.StorageError):
            second.start_source(new)


def test_roundtrip_protocols_and_large_flow(tmp_path):
    src = source(tmp_path)
    flows = tflow.tflows()
    flows[0].request.raw_content = b"x" * (17 * 1024 * 1024)
    with SQLiteBackfillStore(tmp_path / "db") as store:
        rev = store.start_source(src)
        for i, f in enumerate(flows):
            assert store.write_flow(f, src, rev, i).status == "inserted"
        with sqlite.SQLiteFlowReader(tmp_path / "db") as reader:
            assert [reader.get_flow(f.id).get_state() for f in flows] == [
                f.get_state() for f in flows
            ]


def test_wrong_revision_and_failed_rollback(tmp_path):
    src = source(tmp_path)
    with SQLiteBackfillStore(tmp_path / "db") as store:
        rev = store.start_source(src)
        with pytest.raises(sqlite.StorageError, match="Stale"):
            store.finish_source(src, rev + 1, 0)
        real = store.db

        class FailRollback:
            @property
            def in_transaction(self):
                return real.in_transaction

            def execute(self, sql, *args):
                if sql == "ROLLBACK":
                    raise sqlite3.OperationalError("rollback failed")
                return real.execute(sql, *args)

        store.db = FailRollback()
        with pytest.raises(sqlite.StorageError, match="rollback failed"):
            store.finish_source(src, rev + 1, 0)
        with pytest.raises(sqlite.StorageError, match="Previous rollback failed"):
            store.start_source(src)
        store.db = real
        real.execute("ROLLBACK")


def test_actual_sqlite_full_rolls_back(tmp_path):
    src = source(tmp_path)
    f = tflow.tflow()
    f.request.raw_content = os.urandom(128 * 1024)
    with SQLiteBackfillStore(tmp_path / "db") as store:
        rev = store.start_source(src)
        pages = store.db.execute("PRAGMA page_count").fetchone()[0]
        store.db.execute(f"PRAGMA max_page_count={pages}")
        with pytest.raises(sqlite.StorageError, match="full"):
            store.write_flow(f, src, rev, 0)
        store.db.execute("PRAGMA max_page_count=1073741823")
        assert store.db.execute("SELECT count(*) FROM flows").fetchone()[0] == 0
        assert (
            store.db.execute("SELECT count(*) FROM backfill_provenance").fetchone()[0]
            == 0
        )
        assert store.write_flow(f, src, rev, 0).status == "inserted"


def test_update_reserves_space_for_old_payload_wal_pages(tmp_path):
    src = source(tmp_path)
    f = tflow.tflow()
    f.request.raw_content = os.urandom(128 * 1024)
    with SQLiteBackfillStore(tmp_path / "db") as store:
        rev = store.start_source(src)
        store.write_flow(f, src, rev, 0)
        old_size = store.db.execute("SELECT length(payload) FROM flows").fetchone()[0]
        f.request.raw_content = b"tiny replacement"
        checked = []
        assert (
            store.write_flow(f, src, rev, 1, check_space=checked.append).status
            == "updated"
        )
        assert checked[0] >= old_size
