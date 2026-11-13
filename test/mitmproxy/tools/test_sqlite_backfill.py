import hashlib
import io
import os
import runpy
import signal
import sqlite3
import tracemalloc
from contextlib import closing

import pytest

from mitmproxy import http
from mitmproxy.io import FlowReader
from mitmproxy.io import FlowWriter
from mitmproxy.io.sqlite import SQLiteFlowReader
from mitmproxy.io.sqlite import SQLiteFlowStore
from mitmproxy.io.sqlite import zstd
from mitmproxy.test import tflow
from mitmproxy.tools import sqlite_backfill as backfill


def archive(directory, flows=(), timestamp="2026-10-30_10", suffix=0, raw=None):
    name = f"flows-{timestamp}.mitm" + (f"-{suffix}" if suffix else "") + ".zst"
    path = directory / name
    with zstd.ZstdFile(
        path, "wb", options={zstd.CompressionParameter.checksum_flag: 1}
    ) as compressed:
        if raw is not None:
            compressed.write(raw)
        else:
            writer = FlowWriter(compressed)
            for f in flows:
                writer.add(f)
    if path.stat().st_size == 0:
        path.write_bytes(
            zstd.compress(b"", options={zstd.CompressionParameter.checksum_flag: 1})
        )
    return path


def native(f):
    output = io.BytesIO()
    FlowWriter(output).add(f)
    return output.getvalue()


def arguments(directory, database=None, first="2026-10-30_10", through="2026-11-13_08"):
    return [
        "--directory",
        str(directory),
        "--database",
        str(database or directory / "flows.sqlite3"),
        "--from",
        first,
        "--through",
        through,
    ]


def test_inclusive_selection_numeric_suffixes_and_missing_hours(tmp_path):
    names = [
        "flows-2026-10-30_09.mitm.zst",
        "flows-2026-10-30_10.mitm.zst",
        "flows-2026-10-30_10.mitm-10.zst",
        "flows-2026-10-30_10.mitm-2.zst",
        "flows-2026-11-02_03.mitm.zst",
        "flows-2026-11-13_08.mitm-1.zst",
        "flows-2026-11-13_09.mitm.zst",
        "flows-2026-11-13_08.mitm",
        "flows-2026-11-13_08.mitm-0.zst",
        "flows-2026-11-13_08.mitm-01.zst",
        "flows-2026-11-13_08-1.mitm.zst",
        "flows-2026-11-31_08.mitm.zst",
        "unrelated.zst",
    ]
    for name in reversed(names):
        (tmp_path / name).touch()
    selected = backfill.discover(tmp_path, "2026-10-30_10", "2026-11-13_08")
    assert [item.path.name for item in selected] == [
        "flows-2026-10-30_10.mitm.zst",
        "flows-2026-10-30_10.mitm-2.zst",
        "flows-2026-10-30_10.mitm-10.zst",
        "flows-2026-11-02_03.mitm.zst",
        "flows-2026-11-13_08.mitm-1.zst",
    ]


def test_dry_run_creates_no_database_and_does_not_decompress(tmp_path, capsys):
    archive(tmp_path, [tflow.tflow()])
    assert backfill.main(arguments(tmp_path) + ["--dry-run"]) == 0
    assert not (tmp_path / "flows.sqlite3").exists()
    output = capsys.readouterr().out
    assert "flows-2026-10-30_10.mitm.zst" in output
    assert "found=1" in output
    assert "dry-run" in output


def test_complete_native_roundtrips_and_sources_unchanged(tmp_path):
    flows = tflow.tflows()
    for f in flows:
        f.comment = "kept comment"
        f.marked = ":star:"
        f.metadata = {"binary": b"\x00\xff", "nested": [True, 42]}
    flows[0].request.raw_content = b"request\x00body"
    flows[0].response = http.Response.make(201, b"response\xffbody")
    flows[0].request.headers.add("x-duplicate", "one")
    flows[0].request.headers.add("x-duplicate", "two")
    path = archive(tmp_path, flows)
    before = (path.stat(), hashlib.sha256(path.read_bytes()).digest())
    assert backfill.main(arguments(tmp_path)) == 0
    with SQLiteFlowReader(tmp_path / "flows.sqlite3") as reader:
        for f in flows:
            assert reader.get_flow(f.id).get_state() == f.get_state()
    after = path.stat()
    assert (after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_mode) == (
        before[0].st_size,
        before[0].st_mtime_ns,
        before[0].st_ctime_ns,
        before[0].st_mode,
    )
    assert hashlib.sha256(path.read_bytes()).digest() == before[1]
    assert (tmp_path / "flows.sqlite3").stat().st_mode & 0o777 == 0o600


def test_completed_manifest_skips_without_rereading(tmp_path, monkeypatch, capsys):
    f = tflow.tflow()
    archive(tmp_path, [f])
    assert backfill.main(arguments(tmp_path)) == 0
    capsys.readouterr()

    def fail(*args):
        pytest.fail("completed archive was decompressed")

    monkeypatch.setattr(backfill._ZstdStream, "readinto", fail)
    assert backfill.main(arguments(tmp_path)) == 0
    output = capsys.readouterr().out
    assert "skipped=1" in output
    assert "inserted=0" in output
    with SQLiteFlowReader(tmp_path / "flows.sqlite3") as reader:
        assert len(reader.newest(10)) == 1


def test_later_snapshots_update_but_never_replace_independent_rows(tmp_path, capsys):
    earlier = tflow.tflow(resp=False)
    later = tflow.tflow(resp=True)
    later.id = earlier.id
    later.response.raw_content = b"complete response"
    independent = tflow.tflow(resp=True)
    with SQLiteFlowStore(tmp_path / "flows.sqlite3") as store:
        store.write(independent.get_state())
    saved_independent = independent.get_state()
    independent.comment = "must not replace"
    archive(tmp_path, [earlier, independent])
    archive(tmp_path, [later, later], suffix=2)
    assert backfill.main(arguments(tmp_path)) == 0
    with SQLiteFlowReader(tmp_path / "flows.sqlite3") as reader:
        assert reader.get_flow(earlier.id).get_state() == later.get_state()
        assert reader.get_flow(independent.id).get_state() == saved_independent
        assert [m.sequence for m in reader.newest(10)] == [2, 1]
    output = capsys.readouterr().out
    assert "updated=1" in output
    assert "conflicts=1" in output
    assert "existing=2" in output


def test_changed_archive_rescanned_and_argument_changes_never_delete(tmp_path):
    old = tmp_path / "old"
    new = tmp_path / "new"
    old.mkdir()
    new.mkdir()
    db = tmp_path / "flows.sqlite3"
    f = tflow.tflow()
    archive(old, [f])
    assert backfill.main(arguments(old, db)) == 0
    f.comment = "changed archive"
    second = tflow.ttcpflow()
    archive(old, [f, second])
    assert backfill.main(arguments(old, db)) == 0
    third = tflow.tdnsflow()
    archive(new, [third], "2026-10-01_00")
    assert backfill.main(arguments(new, db, "2026-10-01_00", "2026-10-01_00")) == 0
    with SQLiteFlowReader(db) as reader:
        assert len(reader.newest(10)) == 3
        assert reader.get_flow(f.id).comment == "changed archive"
        assert reader.get_flow(second.id)
        assert reader.get_flow(third.id)


def test_valid_empty_checksum_frame_is_complete(tmp_path, capsys):
    path = archive(tmp_path)
    assert path.stat().st_size == 13
    assert backfill.main(arguments(tmp_path)) == 0
    with closing(sqlite3.connect(tmp_path / "flows.sqlite3")) as db:
        assert db.execute(
            "SELECT complete,flow_count FROM backfill_manifest"
        ).fetchall() == [(1, 0)]
    assert "completed=1" in capsys.readouterr().out


@pytest.mark.parametrize(
    "data", [b"", b"not zstd", zstd.compress(b"")[:-1], zstd.compress(b"") + b"junk"]
)
def test_bad_zstd_is_not_marked_complete(tmp_path, data, capsys):
    path = archive(tmp_path)
    path.write_bytes(data)
    assert backfill.main(arguments(tmp_path)) == 1
    output = capsys.readouterr()
    assert path.name in output.err
    assert "failed=1" in output.out
    with closing(sqlite3.connect(tmp_path / "flows.sqlite3")) as db:
        assert db.execute("SELECT complete FROM backfill_manifest").fetchall() == [(0,)]


@pytest.mark.parametrize(
    "data",
    [
        b"garbage secret body",
        b'{"log":{"entries":[]}}',
        b"18:secret-header-body,",
        b"1",
        b"10:short,",
        b"1:1#",
    ],
)
def test_invalid_native_data_and_har_are_rejected_without_logging_payload(
    tmp_path, data, capsys
):
    path = archive(tmp_path, raw=data)
    assert backfill.main(arguments(tmp_path)) == 1
    output = capsys.readouterr()
    assert path.name in output.err
    assert "secret" not in output.err
    with closing(sqlite3.connect(tmp_path / "flows.sqlite3")) as db:
        assert db.execute("SELECT complete FROM backfill_manifest").fetchall() == [(0,)]


def test_partial_corrupt_archive_keeps_committed_flows_and_rescans(tmp_path):
    first, second = tflow.tflow(), tflow.tflow()
    path = archive(tmp_path)
    one, two = zstd.compress(native(first)), zstd.compress(native(second))
    path.write_bytes(one + two[:-2])
    assert backfill.main(arguments(tmp_path)) == 1
    with SQLiteFlowReader(tmp_path / "flows.sqlite3") as reader:
        assert reader.get_flow(first.id).get_state() == first.get_state()
    path.write_bytes(one + two)
    assert backfill.main(arguments(tmp_path)) == 0
    with SQLiteFlowReader(tmp_path / "flows.sqlite3") as reader:
        assert {row.flow_id for row in reader.newest(10)} == {first.id, second.id}


def test_corrupt_declared_length_does_not_allocate_missing_data(tmp_path, capsys):
    archive(tmp_path, raw=b"100000000:x")
    tracemalloc.start()
    try:
        assert backfill.main(arguments(tmp_path)) == 1
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert peak < 2 * 1024 * 1024
    assert "Invalid compressed/native flow data" in capsys.readouterr().err
    with closing(sqlite3.connect(tmp_path / "flows.sqlite3")) as db:
        assert db.execute("SELECT complete FROM backfill_manifest").fetchall() == [(0,)]
        assert db.execute("SELECT count(*) FROM flows").fetchone()[0] == 0


def test_native_stream_disallows_archive_wide_reads():
    with backfill._NativeStream(io.BytesIO(b"abc")) as stream:
        with pytest.raises(backfill.StorageError, match="Unbounded"):
            stream.read()
        assert stream.read(2) == b"ab"


def test_in_archive_progress_and_deferred_ctrl_c(tmp_path, monkeypatch, capsys):
    first, second = tflow.tflow(), tflow.tflow()
    first.request.raw_content = os.urandom(512 * 1024)
    archive(tmp_path, [first, second])
    monkeypatch.setattr(backfill, "_REPORT_SECONDS", 0)
    previous_signal = signal.signal
    handler = None

    def install(signum, callback):
        nonlocal handler
        if callable(callback):
            handler = callback
        return previous_signal(signum, callback)

    monkeypatch.setattr(signal, "signal", install)
    write = SQLiteFlowStore._write_payload

    def interrupt_during_transaction(store, f, raw, sequence=None):
        result = write(store, f, raw, sequence)
        assert store.db.in_transaction
        handler(signal.SIGINT, None)
        return result

    with monkeypatch.context() as patch:
        patch.setattr(SQLiteFlowStore, "_write_payload", interrupt_during_transaction)
        assert backfill.main(arguments(tmp_path)) == 130
    with SQLiteFlowReader(tmp_path / "flows.sqlite3") as reader:
        assert reader.get_flow(first.id).get_state() == first.get_state()
        assert reader.get_flow(second.id) is None
    with closing(sqlite3.connect(tmp_path / "flows.sqlite3")) as db:
        assert db.execute("SELECT complete FROM backfill_manifest").fetchall() == [(0,)]
    output = capsys.readouterr().out
    assert output.count("bytes=") > 3
    assert "inserted=1" in output
    assert backfill.main(arguments(tmp_path)) == 0


def test_module_entrypoint_dry_run(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "sys.argv", [backfill.__file__, *arguments(tmp_path), "--dry-run"]
    )
    with pytest.raises(SystemExit) as result:
        runpy.run_path(backfill.__file__, run_name="__main__")
    assert result.value.code == 0


@pytest.mark.parametrize(
    "first,through",
    [
        ("2026-02-30_10", "2026-11-13_08"),
        ("2026-1-01_10", "2026-11-13_08"),
        ("2026-11-14_00", "2026-11-13_08"),
    ],
)
def test_invalid_range_is_usage_error(tmp_path, first, through):
    with pytest.raises(SystemExit) as result:
        backfill.main(arguments(tmp_path, first=first, through=through))
    assert result.value.code == 2
    assert not (tmp_path / "flows.sqlite3").exists()


@pytest.mark.parametrize("interrupted_setup", [False, True])
def test_empty_uninitialized_database_recovers(tmp_path, interrupted_setup):
    db = tmp_path / "flows.sqlite3"
    if interrupted_setup:
        with closing(sqlite3.connect(db)) as connection:
            connection.execute("BEGIN")
            connection.execute("CREATE TABLE flows (unfinished)")
            connection.rollback()
    else:
        db.touch()
    f = tflow.tflow()
    archive(tmp_path, [f])
    assert backfill.main(arguments(tmp_path) + ["--dry-run"]) == 0
    assert backfill.main(arguments(tmp_path)) == 0
    with SQLiteFlowReader(db) as reader:
        assert reader.get_flow(f.id).get_state() == f.get_state()


@pytest.mark.parametrize("phase", ["preflight", "dry-run", "skip", "finish"])
def test_ctrl_c_on_final_paths_never_reports_success(tmp_path, monkeypatch, phase):
    f = tflow.tflow()
    archive(tmp_path, [f])
    if phase == "skip":
        assert backfill.main(arguments(tmp_path)) == 0
    handler = None
    previous_signal = signal.signal

    def install(signum, callback):
        nonlocal handler
        if callable(callback):
            handler = callback
        return previous_signal(signum, callback)

    monkeypatch.setattr(signal, "signal", install)
    if phase in ("preflight", "dry-run"):
        preflight = backfill._preflight

        def interrupt(*args):
            preflight(*args)
            handler(signal.SIGINT, None)

        monkeypatch.setattr(backfill, "_preflight", interrupt)
    else:
        method_name = "start_source" if phase == "skip" else "finish_source"
        method = getattr(backfill.SQLiteBackfillStore, method_name)

        def interrupt(*args):
            result = method(*args)
            handler(signal.SIGINT, None)
            return result

        monkeypatch.setattr(backfill.SQLiteBackfillStore, method_name, interrupt)
    args = arguments(tmp_path) + (["--dry-run"] if phase == "dry-run" else [])
    assert backfill.main(args) == 130


def test_insufficient_space_before_open_and_during_growth(
    tmp_path, monkeypatch, capsys
):
    f = tflow.tflow()
    f.request.raw_content = b"x" * (128 * 1024)
    archive(tmp_path, [f])
    usage_type = type(backfill.shutil.disk_usage(tmp_path))
    monkeypatch.setattr(
        backfill.shutil, "disk_usage", lambda path: usage_type(10**9, 10**9 - 1, 1)
    )
    assert backfill.main(arguments(tmp_path)) == 1
    assert not (tmp_path / "flows.sqlite3").exists()
    monkeypatch.setattr(
        backfill.shutil,
        "disk_usage",
        lambda path: usage_type(10**9, 0, backfill._RESERVE_BYTES + 4096),
    )
    assert backfill.main(arguments(tmp_path)) == 1
    assert "Insufficient disk space" in capsys.readouterr().err
    with closing(sqlite3.connect(tmp_path / "flows.sqlite3")) as db:
        assert db.execute("SELECT count(*) FROM flows").fetchone()[0] == 0
        assert db.execute("SELECT complete FROM backfill_manifest").fetchall() == [(0,)]


def test_large_flow_and_archive_memory_bounded_by_record(tmp_path, monkeypatch):
    def flows():
        for _ in range(24):
            f = tflow.tflow()
            f.request.raw_content = os.urandom(1024 * 1024)
            yield f

    path = archive(tmp_path, flows())
    original_write = SQLiteFlowStore.write

    def no_reconstruction(*args):
        pytest.fail("import used Store.write and reconstructed/serialized another flow")

    monkeypatch.setattr(SQLiteFlowStore, "write", no_reconstruction)
    tracemalloc.start()
    try:
        assert backfill.main(arguments(tmp_path)) == 0
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert peak < path.stat().st_size // 2
    monkeypatch.setattr(SQLiteFlowStore, "write", original_write)
    large = tflow.tflow()
    large.request.raw_content = b"x" * (17 * 1024 * 1024)
    archive(tmp_path, [large], suffix=1)
    assert backfill.main(arguments(tmp_path)) == 0
    with SQLiteFlowReader(tmp_path / "flows.sqlite3") as reader:
        assert (
            reader.get_flow(large.id).request.raw_content == large.request.raw_content
        )


def test_dry_run_existing_database_has_no_import_side_effects(tmp_path):
    f = tflow.tflow()
    db_path = tmp_path / "flows.sqlite3"
    with SQLiteFlowStore(db_path) as store:
        store.write(f.get_state())
    archive(tmp_path, [tflow.tflow()])
    assert backfill.main(arguments(tmp_path) + ["--dry-run"]) == 0
    with closing(sqlite3.connect(db_path)) as db:
        assert db.execute("SELECT flow_id FROM flows").fetchall() == [(f.id,)]
        assert not db.execute(
            "SELECT name FROM sqlite_master WHERE name LIKE 'backfill_%'"
        ).fetchall()


@pytest.mark.parametrize(
    "kind",
    [
        "missing-directory",
        "unreadable-archive",
        "unwritable-directory",
        "unwritable-database",
        "unsafe-directory",
        "archive-database",
        "archive-alias",
        "incompatible",
        "corrupt",
        "incompatible-manifest",
        "symlink-archive",
    ],
)
def test_preflight_errors_are_safe(tmp_path, kind, capsys):
    directory = tmp_path
    db_path = tmp_path / "flows.sqlite3"
    path = archive(tmp_path, [tflow.tflow()])
    if kind == "missing-directory":
        directory = tmp_path / "does-not-exist"
    elif kind == "unreadable-archive":
        path.chmod(0)
    elif kind == "unwritable-directory":
        tmp_path.chmod(0o500)
    elif kind == "unwritable-database":
        with SQLiteFlowStore(db_path):
            pass
        db_path.chmod(0o400)
    elif kind == "unsafe-directory":
        tmp_path.chmod(0o777)
    elif kind == "archive-database":
        db_path = path
    elif kind == "archive-alias":
        os.link(path, db_path)
    elif kind == "incompatible":
        with closing(sqlite3.connect(db_path)) as db:
            db.execute("CREATE TABLE unrelated (keep)")
    elif kind == "corrupt":
        db_path.write_bytes(b"corrupt SQLite file")
    elif kind == "incompatible-manifest":
        with SQLiteFlowStore(db_path) as store:
            store.db.execute("CREATE TABLE backfill_manifest (wrong)")
    else:
        target = tmp_path / "target"
        path.rename(target)
        path.symlink_to(target)
    before = path.lstat()
    try:
        assert backfill.main(arguments(directory, db_path)) == 1
    finally:
        if kind == "unreadable-archive":
            path.chmod(0o600)
        if kind in ("unsafe-directory", "unwritable-directory"):
            tmp_path.chmod(0o700)
        if kind == "unwritable-database":
            db_path.chmod(0o600)
    after = path.lstat()
    assert after.st_size == before.st_size
    assert after.st_mtime_ns == before.st_mtime_ns
    assert "Backfill failed" in capsys.readouterr().err


def test_sqlite_locked_preserves_existing_flow(tmp_path, monkeypatch, capsys):
    original = tflow.tflow()
    db_path = tmp_path / "flows.sqlite3"
    with SQLiteFlowStore(db_path) as store:
        store.write(original.get_state())
        store.db.execute("BEGIN IMMEDIATE")
        archive(tmp_path, [tflow.tflow()])
        connect = sqlite3.connect

        def short_timeout(*args, **kwargs):
            return connect(*args, **(kwargs | {"timeout": 0.01}))

        monkeypatch.setattr(sqlite3, "connect", short_timeout)
        assert backfill.main(arguments(tmp_path)) == 1
        store.db.execute("ROLLBACK")
    assert "locked" in capsys.readouterr().err
    with SQLiteFlowReader(db_path) as reader:
        assert reader.get_flow(original.id).get_state() == original.get_state()


def test_parser_must_consume_and_verify_full_stream(tmp_path, monkeypatch, capsys):
    archive(tmp_path, [tflow.tflow()])
    monkeypatch.setattr(backfill, "_record_prefix", lambda stream: b"")
    assert backfill.main(arguments(tmp_path)) == 1
    assert "EOF was not verified" in capsys.readouterr().err
    with closing(sqlite3.connect(tmp_path / "flows.sqlite3")) as db:
        assert db.execute("SELECT complete FROM backfill_manifest").fetchall() == [(0,)]
        assert db.execute("SELECT count(*) FROM flows").fetchone()[0] == 0


@pytest.mark.parametrize("count", [0, 2])
def test_inline_parser_requires_exactly_one_flow(tmp_path, monkeypatch, capsys, count):
    f = tflow.tflow()
    archive(tmp_path, [f])
    monkeypatch.setattr(backfill.FlowReader, "stream", lambda reader: iter([f] * count))
    assert backfill.main(arguments(tmp_path)) == 1
    assert "stage=native.decode ordinal=0" in capsys.readouterr().err
    with SQLiteFlowReader(tmp_path / "flows.sqlite3") as reader:
        assert reader.newest(1) == []


def test_native_stream_large_short_read_does_not_preallocate():
    with backfill._NativeStream(io.BytesIO(b"short")) as stream:
        assert stream.read(100000000) == b"short"


def test_unexpected_preflight_error_reports_only_type(tmp_path, monkeypatch, capsys):
    def fail(*args):
        raise ValueError("private-captured-data")

    monkeypatch.setattr(backfill, "_preflight", fail)
    assert backfill.main(arguments(tmp_path)) == 1
    error = capsys.readouterr().err
    assert "ValueError" in error
    assert "private-captured-data" not in error


def test_ctrl_c_between_final_check_and_handler_restoration(
    tmp_path, monkeypatch, capsys
):
    previous_signal = signal.signal
    handler = None

    def install(signum, callback):
        nonlocal handler
        if handler is None:
            handler = callback
        else:
            handler(signum, None)
        return previous_signal(signum, callback)

    monkeypatch.setattr(signal, "signal", install)
    assert backfill.main(arguments(tmp_path)) == 130
    assert "Interrupted" in capsys.readouterr().err


def test_oversized_native_exact_export_resume_and_skip(tmp_path, monkeypatch, capsys):
    first, large, last = tflow.tflow(), tflow.tflow(resp=True), tflow.ttcpflow()
    large.response.raw_content = os.urandom(256 * 1024)
    large.comment = "preserved"
    large.backup()
    path = archive(tmp_path, [first, large, last])
    original = hashlib.sha256(path.read_bytes()).digest()
    monkeypatch.setattr(backfill, "_INLINE_BYTES", 4096, raising=False)
    assert backfill.main(arguments(tmp_path)) == 0
    database = tmp_path / "flows.sqlite3"
    with SQLiteFlowReader(database) as reader:
        assert reader.get_flow(first.id).get_state() == first.get_state()
        with pytest.raises(backfill.StorageError, match="iter_raw"):
            reader.get_flow(large.id)
        chunks = list(reader.iter_raw(large.id, chunk_size=1024))
        assert max(map(len, chunks)) <= 1024
        assert b"".join(chunks) == native(large)
        restored = next(FlowReader(io.BytesIO(b"".join(chunks))).stream())
        assert restored.id == large.id
        assert restored.response.raw_content == large.response.raw_content
        row = next(row for row in reader.newest(10) if row.flow_id == large.id)
        assert row.response_size == len(large.response.raw_content)
    with closing(sqlite3.connect(database)) as db:
        assert db.execute(
            "SELECT complete,flow_count FROM backfill_manifest"
        ).fetchall() == [(1, 3)]
        assert db.execute("SELECT count(*) FROM backfill_provenance").fetchone()[0] == 3
    assert backfill.main(arguments(tmp_path)) == 0
    assert "skipped=1" in capsys.readouterr().out
    assert hashlib.sha256(path.read_bytes()).digest() == original


@pytest.mark.parametrize("phase", ["spool", "chunks"])
def test_oversized_interrupt_rolls_back_and_resumes(tmp_path, monkeypatch, phase):
    first, large = tflow.tflow(), tflow.tflow(resp=True)
    large.response.raw_content = os.urandom(256 * 1024)
    archive(tmp_path, [first, large])
    monkeypatch.setattr(backfill, "_INLINE_BYTES", 4096, raising=False)
    database = tmp_path / "flows.sqlite3"
    with monkeypatch.context() as patch:
        if phase == "spool":
            read = backfill._NativeStream.read

            def interrupted_read(stream, size=-1):
                result = read(stream, size)
                if size == backfill._CHUNK_BYTES:
                    raise backfill._Interrupted
                return result

            patch.setattr(backfill._NativeStream, "read", interrupted_read)
        else:
            write = SQLiteFlowStore._write_chunked

            def interrupted_write(store, *args, **kwargs):
                write(store, *args, **kwargs)
                assert store.db.in_transaction
                raise backfill._Interrupted

            patch.setattr(SQLiteFlowStore, "_write_chunked", interrupted_write)
        assert backfill.main(arguments(tmp_path)) == 130
    with SQLiteFlowReader(database) as reader:
        assert reader.get_flow(first.id)
        assert reader.get_flow(large.id) is None
    with closing(sqlite3.connect(database)) as db:
        assert db.execute("SELECT complete FROM backfill_manifest").fetchall() == [(0,)]
        if db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='flow_payload_chunks'"
        ).fetchone():
            assert (
                db.execute("SELECT count(*) FROM flow_payload_chunks").fetchone()[0]
                == 0
            )
    assert backfill.main(arguments(tmp_path)) == 0
    with SQLiteFlowReader(database) as reader:
        assert b"".join(reader.iter_raw(large.id)) == native(large)
    assert not list(tmp_path.glob("sqlite-backfill-*"))


def test_oversized_corrupt_native_is_not_published(tmp_path, monkeypatch, capsys):
    large = tflow.tflow(resp=True)
    large.response.raw_content = b"private-captured-body" * 4096
    raw = native(large)
    archive(tmp_path, raw=raw[:-1] + b"x")
    monkeypatch.setattr(backfill, "_INLINE_BYTES", 4096, raising=False)
    assert backfill.main(arguments(tmp_path)) == 1
    error = capsys.readouterr().err
    assert "ordinal=0" in error
    assert "raw_length=" in error
    assert "private-captured-body" not in error
    with closing(sqlite3.connect(tmp_path / "flows.sqlite3")) as db:
        assert db.execute("SELECT count(*) FROM flows").fetchone()[0] == 0
        assert db.execute("SELECT complete FROM backfill_manifest").fetchall() == [(0,)]


def test_oversized_import_does_not_materialize_native_body(tmp_path, monkeypatch):
    large = tflow.tflow(resp=True)
    large.response.raw_content = os.urandom(8 * 1024 * 1024)
    archive(tmp_path, [large])
    monkeypatch.setattr(backfill, "_INLINE_BYTES", 4096, raising=False)

    def materialized(*args):
        pytest.fail("oversized record reached FlowReader")

    monkeypatch.setattr(backfill.FlowReader, "stream", materialized)
    tracemalloc.start()
    try:
        assert backfill.main(arguments(tmp_path)) == 0
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert peak < len(large.response.raw_content)


@pytest.mark.parametrize(
    "first_chunked,legacy", [(False, False), (False, True), (True, False)]
)
def test_resume_same_archive_across_payload_formats(
    tmp_path, monkeypatch, first_chunked, legacy
):
    first, last = tflow.tflow(resp=True), tflow.ttcpflow()
    first.response.raw_content = os.urandom(8192)
    original = b"0" + native(first)
    archive(tmp_path, raw=original + native(last))
    database = tmp_path / "flows.sqlite3"
    with monkeypatch.context() as patch:
        patch.setattr(backfill, "_INLINE_BYTES", 4096 if first_chunked else 1024 * 1024)
        method = "write_raw" if first_chunked else "write_flow"
        write = getattr(backfill.SQLiteBackfillStore, method)

        def stop_after_commit(store, *args, **kwargs):
            write(store, *args, **kwargs)
            raise backfill._Interrupted

        patch.setattr(backfill.SQLiteBackfillStore, method, stop_after_commit)
        assert backfill.main(arguments(tmp_path)) == 130
    with closing(sqlite3.connect(database)) as db:
        before = db.execute(
            "SELECT sequence,payload_version,payload FROM flows"
        ).fetchone()
        assert before[1] == (2 if first_chunked else 1)
        if legacy:
            db.execute("DROP TABLE backfill_native_identity")
            db.commit()
    monkeypatch.setattr(
        backfill, "_INLINE_BYTES", 1024 * 1024 if first_chunked else 4096
    )
    assert backfill.main(arguments(tmp_path)) == 0
    with closing(sqlite3.connect(database)) as db:
        assert (
            db.execute(
                "SELECT sequence,payload_version,payload FROM flows WHERE flow_id=?",
                (first.id,),
            ).fetchone()
            == before
        )
        assert db.execute(
            "SELECT complete,flow_count FROM backfill_manifest"
        ).fetchall() == [(1, 2)]
        assert db.execute("SELECT count(*) FROM flows").fetchone()[0] == 2
        assert db.execute(
            "SELECT ordinal FROM backfill_provenance WHERE flow_id=?", (first.id,)
        ).fetchone() == (0,)
    with SQLiteFlowReader(database) as reader:
        expected = original if first_chunked else native(first)
        assert b"".join(reader.iter_raw(first.id)) == expected
        assert reader.get_flow(last.id).get_state() == last.get_state()
