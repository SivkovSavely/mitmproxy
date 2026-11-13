import hashlib
import io
import os
import sqlite3

import pytest

from mitmproxy.io import FlowReader
from mitmproxy.io import sqlite
from mitmproxy.io import tnetstring
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


def test_chunked_provenance_updates_and_protects_independent_rows(tmp_path):
    src = source(tmp_path)
    f = tflow.tflow(resp=True)
    f.response.raw_content = os.urandom(8192)
    independent = tflow.tflow()
    with sqlite.SQLiteFlowStore(tmp_path / "db") as store:
        store.write(independent.get_state())
    with SQLiteBackfillStore(tmp_path / "db") as store:
        rev = store.start_source(src)

        def put(value, ordinal):
            raw = tnetstring.dumps(value.get_state())
            return store.write_raw(
                sqlite._metadata(value),
                io.BytesIO(raw),
                len(raw),
                hashlib.sha256(raw).digest(),
                src,
                rev,
                ordinal,
                chunk_size=1024,
            )

        assert put(f, 0).status == "inserted"
        assert put(f, 0).status == "existing"
        f.comment = "later"
        assert put(f, 1).status == "updated"
        independent.comment = "cannot overwrite"
        assert put(independent, 2).status == "existing"
        with sqlite.SQLiteFlowReader(tmp_path / "db") as reader:
            restored = next(
                FlowReader(io.BytesIO(b"".join(reader.iter_raw(f.id)))).stream()
            )
            assert restored.get_state() == f.get_state()
            assert reader.get_flow(independent.id).comment == ""
        store.db.execute(
            "CREATE TRIGGER fail_provenance BEFORE INSERT ON backfill_provenance BEGIN SELECT RAISE(ABORT, 'disk full'); END"
        )
        f.comment = "not committed"
        with pytest.raises(sqlite.StorageError, match="ordinal=3"):
            put(f, 3)
        with sqlite.SQLiteFlowReader(tmp_path / "db") as reader:
            assert (
                tnetstring.loads(b"".join(reader.iter_raw(f.id)))["comment"] == "later"
            )


def test_raw_identity_survives_switching_import_representation(tmp_path):
    src = source(tmp_path)
    f = tflow.tflow()
    canonical = tnetstring.dumps(f.get_state())
    raw = b"0" + canonical
    digest = hashlib.sha256(raw).digest()
    with SQLiteBackfillStore(tmp_path / "db") as store:
        rev = store.start_source(src)
        assert (
            store.write_flow(f, src, rev, 0, native_digest=digest).status == "inserted"
        )
        assert (
            store.write_raw(
                sqlite._metadata(f), io.BytesIO(raw), len(raw), digest, src, rev, 0
            ).status
            == "existing"
        )
        assert (
            store.write_raw(
                sqlite._metadata(f), io.BytesIO(raw), len(raw), digest, src, rev, 1
            ).status
            == "existing"
        )
        assert (
            store.write_flow(f, src, rev, 1, native_digest=digest).status == "existing"
        )


def test_resume_legacy_normalized_provenance_without_replacing_payload(tmp_path):
    src = source(tmp_path)
    f = tflow.tflow()
    raw = b"0" + tnetstring.dumps(f.get_state())
    digest = hashlib.sha256(raw).digest()
    with SQLiteBackfillStore(tmp_path / "db") as store:
        rev = store.start_source(src)
        store.write_flow(f, src, rev, 0)
        before = store.db.execute("SELECT sequence,payload FROM flows").fetchone()
        assert (
            store.write_raw(
                sqlite._metadata(f),
                io.BytesIO(raw),
                len(raw),
                digest,
                src,
                rev,
                0,
                resume_legacy=True,
            ).status
            == "existing"
        )
        assert (
            store.db.execute("SELECT sequence,payload FROM flows").fetchone() == before
        )
        assert (
            store.db.execute("SELECT digest FROM backfill_native_identity").fetchone()[
                0
            ]
            == digest
        )
        assert (
            store.write_raw(
                sqlite._metadata(f),
                io.BytesIO(raw),
                len(raw),
                digest,
                src,
                rev,
                0,
            ).status
            == "existing"
        )


def test_incompatible_native_identity_schema_is_rejected(tmp_path):
    path = tmp_path / "db"
    with SQLiteBackfillStore(path) as store:
        store.db.execute("DROP TABLE backfill_native_identity")
        store.db.execute("CREATE TABLE backfill_native_identity (wrong TEXT)")
    with pytest.raises(sqlite.StorageError, match="native provenance schema"):
        SQLiteBackfillStore(path)


def test_later_different_native_encoding_can_replace_v1_with_exact_v2(tmp_path):
    src = source(tmp_path)
    f = tflow.tflow()
    canonical = tnetstring.dumps(f.get_state())
    original = b"0" + canonical
    with SQLiteBackfillStore(tmp_path / "db") as store:
        rev = store.start_source(src)
        store.write_flow(
            f, src, rev, 0, native_digest=hashlib.sha256(canonical).digest()
        )
        result = store.write_raw(
            sqlite._metadata(f),
            io.BytesIO(original),
            len(original),
            hashlib.sha256(original).digest(),
            src,
            rev,
            1,
        )
        assert (result.status, result.conflicting) == ("updated", True)
        with sqlite.SQLiteFlowReader(tmp_path / "db") as reader:
            assert reader.newest(1)[0].sequence == 1
            assert b"".join(reader.iter_raw(f.id)) == original
            with pytest.raises(sqlite.StorageError, match="iter_raw"):
                reader.get_flow(f.id)


def test_chunked_sqlite_full_and_interrupt_preserve_committed_state(tmp_path):
    src = source(tmp_path)
    committed, pending = tflow.tflow(), tflow.tflow(resp=True)
    pending.response.raw_content = os.urandom(128 * 1024)
    raw = tnetstring.dumps(pending.get_state())
    with SQLiteBackfillStore(tmp_path / "db") as store:
        rev = store.start_source(src)
        store.write_flow(committed, src, rev, 0)
        pages = store.db.execute("PRAGMA page_count").fetchone()[0]
        store.db.execute(f"PRAGMA max_page_count={pages}")

        def put(**kwargs):
            return store.write_raw(
                sqlite._metadata(pending),
                io.BytesIO(raw),
                len(raw),
                hashlib.sha256(raw).digest(),
                src,
                rev,
                1,
                chunk_size=1024,
                **kwargs,
            )

        with pytest.raises(sqlite.StorageError, match="full"):
            put()
        store.db.execute("PRAGMA max_page_count=1073741823")

        def interrupt():
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            put(check=interrupt)
        assert store.db.execute("SELECT flow_id FROM flows").fetchall() == [
            (committed.id,)
        ]
        assert store.db.execute(
            "SELECT flow_id FROM backfill_provenance"
        ).fetchall() == [(committed.id,)]
        assert store.db.execute(
            "SELECT complete FROM backfill_manifest"
        ).fetchall() == [(0,)]
        assert (
            store.db.execute(
                "SELECT name FROM sqlite_master WHERE name='flow_payload_chunks'"
            ).fetchone()
            is None
        )
        assert put().status == "inserted"
        with sqlite.SQLiteFlowReader(tmp_path / "db") as reader:
            assert reader.get_flow(committed.id).get_state() == committed.get_state()
            assert b"".join(reader.iter_raw(pending.id)) == raw
