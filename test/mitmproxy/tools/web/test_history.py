import asyncio
import gc
import hashlib
import io
import json
import sqlite3
import tempfile
import threading
import urllib.parse
import weakref
from contextlib import closing
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

import pytest
import tornado.testing
from tornado.tcpclient import TCPClient
from tornado.web import create_signed_value

from mitmproxy import dns
from mitmproxy import flow
from mitmproxy import flowfilter
from mitmproxy import hooks
from mitmproxy import tcp
from mitmproxy import udp
from mitmproxy.addons.view import View
from mitmproxy.io import sqlite
from mitmproxy.io import tnetstring
from mitmproxy.options import Options
from mitmproxy.test import tflow
from mitmproxy.tools.web import app
from mitmproxy.tools.web import history
from mitmproxy.tools.web.master import WebMaster


def make_flow(flow_id, timestamp):
    f = tflow.tflow(resp=True)
    f.id = flow_id
    f.timestamp_created = timestamp
    f.request.timestamp_start = timestamp
    f.request.method = "GET"
    f.request.host = "example.test"
    f.request.path = "/" + flow_id
    f.request.raw_content = b"request"
    f.response.raw_content = b"response"
    f.response.timestamp_end = timestamp + 100
    return f


@pytest.mark.parametrize(
    "arguments",
    [
        {"lookup_groups": "["},
        {"lookup_groups": "x" * 10001},
        {"lookup_groups": "{}"},
        {"lookup_groups": json.dumps(["g1:" + "0" * 64] * 129)},
        {"lookup_groups": json.dumps([1])},
        {"lookup_groups": json.dumps(["g1:invalid"])},
        {"lookup_groups": "[]"},
        {"snapshot": "s1:" + "0" * 32, "lookup_groups": "[]", "lookup_flow": "x"},
        {"refresh": "s1:" + "0" * 32, "lookup_groups": "[]"},
    ],
)
def test_invalid_group_lookup_batches_are_rejected(arguments):
    with pytest.raises(ValueError):
        history.HistoryQuery.parse({"group_by": "status", **arguments}, now=3000)


async def test_live_filter_capacity_prune_and_cancelled_inventory():
    view = View()
    index = history.LiveHistoryIndex(view)
    cancel = threading.Event()
    try:
        for i in range(16):
            await index.register(
                history.HistoryQuery.parse({"search": f"~m M{i}"}),
                cancel,
                verify=False,
            )
        query = history.HistoryQuery.parse({"search": "~m NEW"})
        with pytest.raises(history.SnapshotError, match="capacity"):
            await index.register(query, cancel, verify=False)
        index.prune(set())
        item = make_flow("inventory", 2500)
        view.add([item])

        def cancelled_inventory(_):
            cancel.set()
            return [item]

        with mock.patch.object(view, "resolve", cancelled_inventory):
            with pytest.raises(sqlite.StorageError, match="cancelled"):
                await index.register(query, cancel, verify=False)
        assert not index.filters
        with pytest.raises(sqlite.StorageError, match="cancelled"):
            await index.register(query, cancel)
    finally:
        await index.done()


async def test_shutdown_cancels_preparing_request():
    reader = history.HistoryReader()
    cancel = threading.Event()
    reader._preparing.add(cancel)
    await reader.done()
    assert cancel.is_set()


async def test_cancelled_group_submission_releases_worker_slot(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    reader = history.HistoryReader()
    query = history.HistoryQuery.parse({"group_by": "status"}, now=3000)
    cancel = threading.Event()
    cancel.set()
    try:
        with pytest.raises(sqlite.StorageError, match="cancelled"):
            await reader.grouped_page(
                str(path),
                query,
                [],
                cancel,
                instance_id="test",
                live_generation=lambda: 0,
            )
        result = await reader.grouped_page(
            str(path),
            query,
            [],
            threading.Event(),
            instance_id="test",
            live_generation=lambda: 0,
        )
        assert result["count"] == 0
    finally:
        await reader.done()


async def test_group_build_cancellation_at_index_lock_acquire(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    reader = history.HistoryReader()
    cancel = threading.Event()
    lock = reader._index_lock

    class CancelAtAcquire:
        def acquire(self, **kwargs):
            acquired = lock.acquire(**kwargs)
            cancel.set()
            return acquired

        def release(self):
            lock.release()

    try:
        with mock.patch.object(reader, "_index_lock", CancelAtAcquire()):
            with pytest.raises(sqlite.StorageError, match="cancelled"):
                await reader.grouped_page(
                    str(path),
                    history.HistoryQuery.parse({"group_by": "status"}, now=3000),
                    [],
                    cancel,
                    instance_id="test",
                    live_generation=lambda: 0,
                )
        assert not lock.locked()
        assert not reader._owned_engines
    finally:
        await reader.done()


async def test_cancelled_group_source_is_not_published(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(make_flow("first", 2500).get_state())
    reader = history.HistoryReader()
    cancel = threading.Event()

    def build(source, *, cancel, **kwargs):
        rows = source(cancel)
        cancel.set()
        next(rows)
        raise AssertionError("cancelled source yielded a row")

    try:
        with mock.patch.object(history.GroupStore, "build", build):
            with pytest.raises(sqlite.StorageError, match="cancelled"):
                await reader.grouped_page(
                    str(path),
                    history.HistoryQuery.parse({"group_by": "status"}, now=3000),
                    [],
                    cancel,
                    instance_id="test",
                    live_generation=lambda: 0,
                )
        assert not reader._snapshots
        assert not reader._owned_engines
    finally:
        await reader.done()


async def test_source_batch_rejects_changed_revision_and_database_path(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(make_flow("first", 2500).get_state())
    reader = history.HistoryReader()
    cancel = threading.Event()
    identity = reader._path_identity(path)
    try:
        revision, _ = reader._source_batch(path, None, identity, cancel)
        with sqlite.SQLiteFlowStore(path) as store:
            store.write(make_flow("second", 2500).get_state())
        with pytest.raises(history.HistoryStale, match="database changed"):
            reader._source_batch(path, revision, identity, cancel)
        with pytest.raises(history.HistoryStale, match="path changed"):
            reader._source_batch(path, None, ("another", 0, 0), cancel)
    finally:
        await reader.done()


async def test_journal_rejects_cancellation_and_replaced_database(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    reader = history.HistoryReader()
    engine = reader._build_engine(
        str(path),
        history.HistoryQuery.parse({"group_by": "status"}, now=3000),
        [],
        threading.Event(),
        (),
        None,
    )
    cancel = threading.Event()
    try:
        cancel.set()
        with pytest.raises(sqlite.StorageError, match="cancelled"):
            reader._journal(str(path), engine, 0, 0, cancel)
        cancel.clear()
        with mock.patch.object(
            reader, "_path_identity", return_value=("changed", 0, 0)
        ):
            with pytest.raises(history.HistoryStale, match="path changed"):
                reader._journal(str(path), engine, 0, 0, cancel)
        with mock.patch.object(
            sqlite.SQLiteFlowReader, "revision", return_value=("other", 0)
        ):
            with pytest.raises(history.HistoryStale, match="identity changed"):
                reader._journal(str(path), engine, 0, 0, cancel)
    finally:
        engine.close()
        await reader.done()


@pytest.mark.parametrize("failure", ["path", "revision"])
async def test_refresh_rejects_changed_database(tmp_path, failure):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    reader = history.HistoryReader()
    query = history.HistoryQuery.parse({"group_by": "status"}, now=3000)
    cancel = threading.Event()
    engine = reader._build_engine(str(path), query, [], cancel, (), None)
    original = reader._source_batch

    def changed_revision(
        path, revision, identity, cancel, sql="SELECT 1", args=(), **kwargs
    ):
        observed, rows = original(path, revision, identity, cancel, sql, args, **kwargs)
        return (
            (("different", observed[1]), rows)
            if sql == "SELECT 1"
            else (observed, rows)
        )

    patch = (
        mock.patch.object(reader, "_path_identity", return_value=("different", 0, 0))
        if failure == "path"
        else mock.patch.object(reader, "_source_batch", changed_revision)
    )
    try:
        with patch, pytest.raises(history.HistoryStale, match="changed"):
            reader._refresh_engine(str(path), engine, query, [], cancel)
        assert engine.query == query
    finally:
        engine.close()
        await reader.done()


@pytest.mark.parametrize("failure", ["identity", "journal"])
async def test_initial_build_fails_cleanly_on_inconsistent_durable_cut(
    tmp_path, failure
):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(make_flow("first", 2500).get_state())
    reader = history.HistoryReader()
    query = history.HistoryQuery.parse({"group_by": "status"}, now=3000)
    copy_live = reader._copy_live
    source_batch = reader._source_batch

    def write_before_cut(engine, flows, cancel, *, initial):
        copy_live(engine, flows, cancel, initial=initial)
        with sqlite.SQLiteFlowStore(path) as store:
            store.write(make_flow("second", 2500).get_state())

    def changed_identity(
        path, revision, identity, cancel, sql="SELECT 1", args=(), **kwargs
    ):
        observed, rows = source_batch(
            path, revision, identity, cancel, sql, args, **kwargs
        )
        if failure == "identity" and sql == "SELECT 1":
            return ("different", observed[1]), rows
        return observed, rows

    try:
        with (
            mock.patch.object(reader, "_copy_live", write_before_cut),
            mock.patch.object(reader, "_source_batch", changed_identity),
            mock.patch.object(reader, "_journal", return_value=[]),
        ):
            error = (
                history.HistoryStale if failure == "identity" else sqlite.StorageError
            )
            with pytest.raises(error, match="identity changed|Incomplete history"):
                await reader.grouped_page(
                    str(path),
                    query,
                    [],
                    threading.Event(),
                    instance_id="test",
                    live_generation=lambda: 0,
                )
        assert not reader._owned_engines
        assert not reader._snapshots
    finally:
        await reader.done()


@pytest.mark.parametrize("size", [110000, 200000])
async def test_initial_build_rejects_exhausted_scratch_budget(tmp_path, size):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(make_flow("first", 2500).get_state())
    reader = history.HistoryReader(snapshot_bytes=131072)
    try:
        with mock.patch.object(
            history._Engine,
            "size_bytes",
            new_callable=mock.PropertyMock,
            return_value=size,
        ):
            with pytest.raises(history.SnapshotError, match="scratch"):
                await reader.grouped_page(
                    str(path),
                    history.HistoryQuery.parse({"group_by": "status"}, now=3000),
                    [],
                    threading.Event(),
                    instance_id="test",
                    live_generation=lambda: 0,
                )
        assert not reader._owned_engines
    finally:
        await reader.done()


async def test_initial_live_inventory_spans_commit_batches(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    view = View()
    reader = history.HistoryReader()
    reader.live_index = history.LiveHistoryIndex(view)
    flows = [make_flow(f"live-{i}", 2500) for i in range(1025)]
    try:
        view.add(flows)
        page = await reader.grouped_page(
            str(path),
            history.HistoryQuery.parse({"group_by": "status"}, now=3000),
            [],
            threading.Event(),
            instance_id="test",
            live_generation=lambda: 0,
        )
        assert page["count"] == len(flows)
        view.clear()
        refreshed = await reader.grouped_page(
            str(path),
            history.HistoryQuery.parse(
                {"group_by": "status", "refresh": page["snapshot"]}, now=3000
            ),
            [],
            threading.Event(),
            instance_id="test",
            live_generation=lambda: 0,
        )
        assert refreshed["count"] == 0
    finally:
        await reader.done()


async def test_cancel_while_copying_live_inventory_does_not_publish(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    view = View()
    reader = history.HistoryReader()
    index = history.LiveHistoryIndex(view)
    reader.live_index = index
    view.add([make_flow("live", 2500)])
    cancel = threading.Event()
    rows = index.rows

    def cancel_during_rows(db, cut):
        for row in rows(db, cut):
            cancel.set()
            yield row

    try:
        with mock.patch.object(index, "rows", cancel_during_rows):
            with pytest.raises(sqlite.StorageError, match="cancelled"):
                await reader.grouped_page(
                    str(path),
                    history.HistoryQuery.parse({"group_by": "status"}, now=3000),
                    [],
                    cancel,
                    instance_id="test",
                    live_generation=lambda: 0,
                )
        assert not reader._snapshots
        assert not reader._owned_engines
    finally:
        await reader.done()


async def test_retained_engine_capacity_recovers_after_expiry(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    reader = history.HistoryReader(snapshot_capacity=1)
    loop = asyncio.get_running_loop()

    async def page(selector):
        return await reader.grouped_page(
            str(path),
            history.HistoryQuery.parse({"group_by": selector}, now=3000),
            [],
            threading.Event(),
            instance_id="test",
            live_generation=lambda: 0,
        )

    try:
        first = await page("status")
        await loop.run_in_executor(
            reader._group_worker, reader._discard_snapshot, first["snapshot"]
        )
        with pytest.raises(history.SnapshotError, match="engine capacity"):
            await page("host")

        def expire():
            for engine in reader._owned_engines:
                engine.expires = 0
            reader._expire_snapshots()

        await loop.run_in_executor(reader._group_worker, expire)
        assert (await page("host"))["count"] == 0
    finally:
        await reader.done()


@pytest.mark.parametrize("codec", ["gzip", "deflate", "deflateraw", "br", "zstd"])
def test_bounded_decoder_handles_empty_and_absent_content(codec):
    f = tflow.tflow()
    f.request.headers["content-encoding"] = codec
    f.request.raw_content = b""
    assert history.LiveHistoryIndex._bounded_content(f.request, 100) == b""
    f.request.raw_content = None
    assert history.LiveHistoryIndex._bounded_content(f.request, 100) is None


@pytest.mark.parametrize("codec", ["deflate", "br"])
def test_bounded_decoder_falls_back_to_truncated_input(codec):
    from mitmproxy.net import encoding

    f = tflow.tflow()
    f.request.headers["content-encoding"] = codec
    f.request.raw_content = encoding.encode(b"test contents" * 100, codec)[:-1]
    assert (
        history.LiveHistoryIndex._bounded_content(f.request, 4096)
        == f.request.raw_content
    )


@pytest.mark.parametrize(
    "codec,raw,limit,expected",
    [
        ("unrecognized-codec", b"abc", 3, b"abc"),
        ("unrecognized-codec", b"abc", 2, None),
        ("utf-8", b"abc", 3, b"abc"),
        ("base64", b"YWJj", 4, b"abc"),
        ("base64", b"YWJj", 3, None),
        ("bz2", b"abc", 10, None),
        ("zstd", b"x" * (1024 * 1024 + 1), 2000000, None),
    ],
    ids=[
        "unknown",
        "unknown-limit",
        "text",
        "base64",
        "base64-limit",
        "bz2",
        "zstd-limit",
    ],
)
def test_bounded_decoder_handles_codec_and_input_limits(codec, raw, limit, expected):
    f = tflow.tflow()
    f.request.headers["content-encoding"] = codec
    f.request.raw_content = raw
    assert history.LiveHistoryIndex._bounded_content(f.request, limit) == expected


def test_bounded_decoder_falls_back_to_truncated_zstd():
    from mitmproxy.net import encoding

    f = tflow.tflow()
    f.request.headers["content-encoding"] = "zstd"
    f.request.raw_content = encoding.zstd.compress(b"hello world")[:-1]
    assert (
        history.LiveHistoryIndex._bounded_content(f.request, 100)
        == f.request.raw_content
    )


def test_live_index_rejects_negative_body_cache_size():
    with pytest.raises(ValueError, match="nonnegative"):
        history.LiveHistoryIndex(View(), body_cache_bytes=-1)


async def test_invalid_live_capture_reports_error():
    view = View()
    index = history.LiveHistoryIndex(view)
    f = tflow.tflow()
    f.id = ""
    try:
        view.add([f])
        with pytest.raises(history.LiveIndexError, match="capture failed"):
            await index.flush()
    finally:
        await index.done()


async def test_live_filter_or_and_unavailable_metadata():
    view = View()
    index = history.LiveHistoryIndex(view)
    f = tflow.tflow()
    f.comment = "x" * (300 * 1024)
    try:
        view.add([f])
        query = history.HistoryQuery.parse({"search": "~m POST | ~m GET"})
        token = await index.register(query, threading.Event())
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["search:" + token]
        with pytest.raises(history.LiveIndexError, match="unavailable"):
            await index.register(
                history.HistoryQuery.parse(
                    {"search": "~comment test", "mode": "absolute"}
                ),
                threading.Event(),
            )
    finally:
        await index.done()


@pytest.mark.parametrize("filter_name", ["search", "highlight"])
@pytest.mark.parametrize("source", ["body", "headers"])
@pytest.mark.parametrize(
    "expression,expected",
    [
        ("UNKNOWN | ~all", True),
        ("~all | UNKNOWN", True),
        ("UNKNOWN & ~m POST", False),
        ("~m POST & UNKNOWN", False),
        ("!(UNKNOWN & ~m POST)", True),
        ("!(UNKNOWN | ~m GET)", False),
        ("(UNKNOWN & ~m POST) | ~m GET", True),
        ("UNKNOWN | ~m POST", None),
        ("UNKNOWN & ~all", None),
        ("!UNKNOWN", None),
    ],
)
async def test_unavailable_filter_input_errors_only_if_result_depends_on_it(
    filter_name, source, expression, expected
):
    view = View()
    index = history.LiveHistoryIndex(view)
    f = tflow.tflow()
    if source == "body":
        f.request.raw_content = b"needle" + b"x" * (1024 * 1024)
        unavailable = "~bq needle"
    else:
        f.request.headers["oversized"] = "x" * (1024 * 1024 + 1)
        unavailable = "~hq needle"
    query = history.HistoryQuery.parse(
        {filter_name: expression.replace("UNKNOWN", unavailable), "mode": "absolute"}
    )
    try:
        view.add([f])
        if expected is None:
            with pytest.raises(history.LiveIndexError, match="budget"):
                await index.register(query, threading.Event())
            await index.register(history.HistoryQuery.parse({}), threading.Event())
        else:
            token = await index.register(query, threading.Event())
            with index.read_cut() as (db, cut):
                assert next(index.rows(db, cut))[filter_name + ":" + token] is expected
    finally:
        await index.done()


@pytest.mark.parametrize("cache_bytes", [0, 8 * 1024 * 1024])
@pytest.mark.parametrize("oversized_direction", ["request", "response"])
@pytest.mark.parametrize("either_direction", [False, True])
async def test_body_filter_ignores_unrelated_oversized_direction(
    cache_bytes, oversized_direction, either_direction
):
    view = View()
    index = history.LiveHistoryIndex(view, body_cache_bytes=cache_bytes)
    f = tflow.tflow(resp=True)
    f.request.raw_content = f.response.raw_content = b"needle"
    getattr(f, oversized_direction).raw_content = b"x" * (1024 * 1024 + 1)
    expression = (
        "~b needle"
        if either_direction
        else "~bs needle"
        if oversized_direction == "request"
        else "~bq needle"
    )
    try:
        view.add([f])
        token = await index.register(
            history.HistoryQuery.parse({"search": expression, "mode": "absolute"}),
            threading.Event(),
        )
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["search:" + token] is True
        assert index._body_bytes <= cache_bytes
    finally:
        await index.done()


@pytest.mark.parametrize("changed_direction", ["request", "response"])
async def test_evicted_body_filter_ignores_unnotified_other_direction(
    changed_direction,
):
    view = View()
    index = history.LiveHistoryIndex(view, body_cache_bytes=0)
    f = tflow.tflow(resp=True)
    f.request.raw_content = f.response.raw_content = b"needle"
    try:
        view.add([f])
        await index.flush()
        getattr(f, changed_direction).raw_content = b"unnotified"
        token = await index.register(
            history.HistoryQuery.parse(
                {
                    "search": "~bs needle"
                    if changed_direction == "request"
                    else "~bq needle",
                    "mode": "absolute",
                }
            ),
            threading.Event(),
        )
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["search:" + token] is True
    finally:
        await index.done()


async def test_active_body_filter_reports_eviction_without_poisoning_metadata():
    view = View()
    index = history.LiveHistoryIndex(view, body_cache_bytes=0)
    f = tflow.tflow()
    try:
        await index.register(
            history.HistoryQuery.parse({"search": "~bq needle", "mode": "absolute"}),
            threading.Event(),
        )
        view.add([f])
        with pytest.raises(
            history.LiveIndexError, match="captured event-time.*evicted"
        ):
            with index.read_cut():
                pass
        await index.register(history.HistoryQuery.parse({}), threading.Event())
        with index.read_cut() as (db, cut):
            row = next(index.rows(db, cut))
            assert row["id"] == f.id
            assert "_filter_errors" not in row
            assert all(
                "_filter_errors" not in change[3]
                for change in index.changes(db, 0, cut, limit=100)
            )
    finally:
        await index.done()


@pytest.mark.parametrize(
    "field,expression",
    [
        ("path", "~d address"),
        ("method", "~hs needle"),
        ("headers", "~hs needle"),
        ("headers", "~h needle"),
        ("content-type", "~ts text/plain"),
        ("content-type", "~t text/plain"),
        ("metadata", "~m GET"),
    ],
)
async def test_oversized_field_does_not_invalidate_unrelated_filter(field, expression):
    view = View()
    index = history.LiveHistoryIndex(view)
    f = tflow.tflow(resp=True)
    f.response.headers["test"] = "needle"
    f.response.headers["content-type"] = "text/plain"
    oversized = "x" * (1024 * 1024 + 1)
    if field in ("path", "method"):
        setattr(f.request, field, oversized)
    elif field == "metadata":
        f.metadata["oversized"] = oversized
    else:
        f.request.headers["test" if field == "headers" else field] = oversized
    try:
        view.add([f])
        token = await index.register(
            history.HistoryQuery.parse({"search": expression, "mode": "absolute"}),
            threading.Event(),
        )
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["search:" + token] is True
    finally:
        await index.done()


@pytest.mark.parametrize("field", ["src", "dst"])
async def test_oversized_address_input_does_not_poison_live_index(field):
    view = View()
    index = history.LiveHistoryIndex(view)
    f = tflow.tflow()
    oversized = "x" * (index.queue_bytes + 1)
    if field == "src":
        f.client_conn.peername = (oversized, 123)
    else:
        f.server_conn.address = (oversized, 443)
    try:
        view.add([f])
        token = await index.register(
            history.HistoryQuery.parse({"search": "~m GET", "mode": "absolute"}),
            threading.Event(),
        )
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["search:" + token] is True
            assert db.execute("SELECT length(data) FROM current").fetchone()[0] < 16384
        with pytest.raises(history.LiveIndexError, match="budget"):
            await index.register(
                history.HistoryQuery.parse(
                    {"search": f"~{field} needle", "mode": "absolute"}
                ),
                threading.Event(),
            )
        await index.register(history.HistoryQuery.parse({}), threading.Event())
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["id"] == f.id
    finally:
        await index.done()


async def test_unavailable_metadata_does_not_consume_unrelated_capture_budget():
    view = View()
    index = history.LiveHistoryIndex(view)
    f = tflow.tflow()
    f.metadata = {"first": "x" * 262125, "overflow": "needle" * 100000}
    try:
        view.add([f])
        token = await index.register(
            history.HistoryQuery.parse({"search": "~m GET", "mode": "absolute"}),
            threading.Event(),
        )
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["search:" + token] is True
        with pytest.raises(history.LiveIndexError, match="budget"):
            await index.register(
                history.HistoryQuery.parse(
                    {"search": "~meta needle", "mode": "absolute"}
                ),
                threading.Event(),
            )
    finally:
        await index.done()


@pytest.mark.parametrize(
    "factory", [tflow.ttcpflow, tflow.tudpflow, tflow.twebsocketflow]
)
@pytest.mark.parametrize("expression", ["~bq needle", "~b needle", "~bq absent"])
async def test_stream_filter_keeps_later_decisive_messages(factory, expression):
    view = View()
    index = history.LiveHistoryIndex(view)
    f = factory()
    messages = f.websocket.messages if getattr(f, "websocket", None) else f.messages
    template = messages[0]
    messages[:] = [template.copy() for _ in range(4)]
    for message in messages:
        message.from_client = True
        message.content = b"x" * (1024 * 1024 + 1)
    messages[2].content = b"needle"
    query = history.HistoryQuery.parse({"search": expression, "mode": "absolute"})
    try:
        view.add([f])
        if "absent" in expression:
            with pytest.raises(history.LiveIndexError, match="budget"):
                await index.register(query, threading.Event())
        else:
            token = await index.register(query, threading.Event())
            with index.read_cut() as (db, cut):
                assert next(index.rows(db, cut))["search:" + token] is True
        assert index._body_bytes < 4096
    finally:
        await index.done()


@pytest.mark.parametrize("expression", ["~tq needle", "~t needle", "~tq absent"])
async def test_content_type_filter_keeps_later_decisive_values(expression):
    view = View()
    index = history.LiveHistoryIndex(view)
    f = tflow.tflow()
    f.request.headers.set_all("content-type", ["x" * (1024 * 1024 + 1), "needle"])
    query = history.HistoryQuery.parse({"search": expression, "mode": "absolute"})
    try:
        view.add([f])
        if "absent" in expression:
            with pytest.raises(history.LiveIndexError, match="budget"):
                await index.register(query, threading.Event())
        else:
            token = await index.register(query, threading.Event())
            with index.read_cut() as (db, cut):
                assert next(index.rows(db, cut))["search:" + token] is True
    finally:
        await index.done()


async def test_queued_filter_flags_coalesce_without_losing_bindings():
    view = View()
    index = history.LiveHistoryIndex(view)
    f = tflow.tflow()
    try:
        view.add([f])
        await index.flush()
        with index._condition:
            first = await index.register(
                history.HistoryQuery.parse({"search": "~m GET"}),
                threading.Event(),
                verify=False,
            )
            second = await index.register(
                history.HistoryQuery.parse({"search": "~m POST"}),
                threading.Event(),
                verify=False,
            )
        with index.read_cut() as (db, cut):
            row = next(index.rows(db, cut))
            assert row["search:" + first] is True
            assert row["search:" + second] is False
    finally:
        await index.done()


async def test_live_spool_batches_flags_clear_and_size_bound():
    view = View()
    index = history.LiveHistoryIndex(view, queue_bytes=4096)
    f = tflow.tflow()
    try:
        view.add([f])
        await index.flush()
        with index._condition, closing(sqlite3.connect(index.path)) as db:
            data = db.execute(
                "SELECT data FROM current WHERE id=?", (f.id,)
            ).fetchone()[0]
            seq = db.execute("SELECT seq FROM meta").fetchone()[0]
            flags = json.dumps(
                {
                    "_active_filters": "test",
                    "_queries": {"test": ["~m GET", "", {}, None]},
                }
            )
            index._write_batch(db, [])
            index._write_batch(
                db,
                [
                    (seq + 1, "upsert", f.id, data, 1),
                    (seq + 2, "flags", f.id, flags, 1),
                    (seq + 3, "clear", None, None, 1),
                    (seq + 4, "flags", f.id, flags, 1),
                    (seq + 5, "upsert", f.id, data, 4096),
                ],
            )
            changes = db.execute(
                "SELECT op,data FROM journal WHERE seq>? ORDER BY seq", (seq,)
            ).fetchall()
            assert [row[0] for row in changes] == [
                "upsert",
                "upsert",
                "clear",
                "remove",
                "upsert",
            ]
            assert json.loads(changes[1][1])["search:test"] is True
            assert db.execute("SELECT count(*) FROM current").fetchone()[0] == 1
    finally:
        await index.done()


async def test_live_filter_rejects_oversized_flow_type():
    view = View()
    index = history.LiveHistoryIndex(view)
    f = tflow.tflow()
    f.type = "x" * (1024 * 1024 + 1)
    try:
        view.add([f])
        with pytest.raises(history.LiveIndexError, match="unavailable"):
            await index.register(
                history.HistoryQuery.parse({"search": "~http", "mode": "absolute"}),
                threading.Event(),
            )
    finally:
        await index.done()


async def test_live_filter_rejects_unknown_future_predicate():
    view = View()
    index = history.LiveHistoryIndex(view)
    query = history.HistoryQuery.parse({"search": "~http", "mode": "absolute"})
    try:
        view.add([tflow.tflow()])
        await index.flush()
        with mock.patch.object(flowfilter, "parse", return_value=object()):
            with pytest.raises(history.LiveIndexError, match="unavailable"):
                await index.register(query, threading.Event())
    finally:
        await index.done()


async def test_live_body_cache_handles_other_flow_type_and_large_dns():
    view = View()
    index = history.LiveHistoryIndex(view)
    dummy = tflow.tdummyflow()
    dns_flow = tflow.tdnsflow()
    dns_flow.request.answers.extend(
        dns.ResourceRecord("example.com", 16, 1, 60, b"x" * 50000) for _ in range(3)
    )
    try:
        view.add([dummy, dns_flow])
        with index.read_cut() as (db, cut):
            assert {row["id"] for row in index.rows(db, cut)} == {dummy.id, dns_flow.id}
        with pytest.raises(history.LiveIndexError, match="unavailable"):
            await index.register(
                history.HistoryQuery.parse(
                    {"search": "~bq needle", "mode": "absolute"}
                ),
                threading.Event(),
            )
    finally:
        await index.done()


async def test_live_domain_filter_rejects_oversized_pretty_host():
    view = View()
    index = history.LiveHistoryIndex(view)
    f = tflow.tflow()
    f.request.headers["host"] = "x" * (1024 * 1024 + 1)
    try:
        view.add([f])
        with pytest.raises(history.LiveIndexError, match="unavailable"):
            await index.register(
                history.HistoryQuery.parse(
                    {"search": "~d example", "mode": "absolute"}
                ),
                threading.Event(),
            )
    finally:
        await index.done()


@pytest.mark.parametrize("expression", ["~m GET & ~bq needle", "!~bq missing"])
async def test_evicted_compound_body_filter_uses_captured_candidate(expression):
    view = View()
    index = history.LiveHistoryIndex(view, body_cache_bytes=0)
    f = tflow.tflow()
    f.request.raw_content = b"needle"
    try:
        view.add([f])
        await index.flush()
        token = await index.register(
            history.HistoryQuery.parse({"search": expression}), threading.Event()
        )
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["search:" + token]
    finally:
        await index.done()


async def test_evicted_oversized_body_filter_reports_unavailable():
    view = View()
    index = history.LiveHistoryIndex(view, body_cache_bytes=0)
    f = tflow.tflow()
    f.request.headers["content-encoding"] = "x" * 200
    f.request.raw_content = b"needle"
    try:
        view.add([f])
        with pytest.raises(history.LiveIndexError, match="unavailable"):
            await index.register(
                history.HistoryQuery.parse(
                    {"search": "~bq needle", "mode": "absolute"}
                ),
                threading.Event(),
            )
    finally:
        await index.done()


async def test_live_history_pinned_reader_reports_wal_exhaustion():
    view = View()
    index = history.LiveHistoryIndex(view, wal_bytes=1)
    f = tflow.tflow()
    try:
        view.add([f])
        await index.flush()
        with pytest.raises(history.LiveIndexError, match="WAL"):
            with index.read_cut() as (db, cut):
                assert next(index.rows(db, cut))["method"] == "GET"
                f.request.method = "POST"
                view.update([f])
                with pytest.raises(history.LiveIndexError, match="WAL"):
                    await index.flush()
    finally:
        await index.done()


@pytest.mark.parametrize(
    "field,value,expression",
    [
        ("marked", "x" * (300 * 1024), "~marked"),
        ("host", "x" * (150 * 1024), "~d example"),
        ("method", "x" * (1024 * 1024 + 1), "~m GET"),
        ("metadata", {"x": object()}, "~meta example"),
        ("metadata", {"x": 1 << 1100000}, "~meta example"),
        ("metadata", {"x": "x" * (300 * 1024)}, "~meta example"),
        ("metadata", {"x": b"\xff" * 70000}, "~meta example"),
        ("content-type", "x" * (1024 * 1024 + 1), "~tq text"),
    ],
    ids=[
        "marked",
        "host",
        "method",
        "object",
        "big-int",
        "large-text",
        "escaped-bytes",
        "content-type",
    ],
)
async def test_oversized_live_filter_inputs_fail_explicitly(field, value, expression):
    view = View()
    index = history.LiveHistoryIndex(view)
    f = tflow.tflow()
    if field in ("host", "method"):
        setattr(f.request, field, value)
    elif field == "content-type":
        f.request.headers["content-type"] = value
    else:
        setattr(f, field, value)
    try:
        view.add([f])
        with pytest.raises(history.LiveIndexError, match="unavailable"):
            await index.register(
                history.HistoryQuery.parse({"search": expression, "mode": "absolute"}),
                threading.Event(),
            )
    finally:
        await index.done()


@pytest.mark.parametrize("refresh", [False, True])
def test_group_cut_includes_persisted_live_removal(tmp_path, refresh):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    service = history.HistoryReader()
    query = history.HistoryQuery.parse({"group_by": "status"}, now=3000)
    item = make_flow("handoff", 2500)
    flows = [item]
    cancel = threading.Event()
    engine = None
    try:
        if refresh:
            engine = service._build_engine(str(path), query, flows, cancel, (), None)
        original = service._copy_live

        def handoff(engine, live, cancel, *, initial):
            with sqlite.SQLiteFlowStore(path) as store:
                store.db.executemany(
                    "INSERT INTO flows (flow_id,timestamp,flow_type,status_code,payload_version,codec,raw_length,payload) VALUES (?,2500,'http',200,1,'zstd',1,x'00')",
                    ((f"backlog-{i}",) for i in range(8193)),
                )
                store.write(item.get_state())
            live.clear()
            return original(engine, live, cancel, initial=initial)

        with mock.patch.object(service, "_copy_live", handoff):
            if refresh:
                service._refresh_engine(str(path), engine, query, flows, cancel)
            else:
                engine = service._build_engine(
                    str(path), query, flows, cancel, (), None
                )
        assert engine.metadata("handoff") is not None
        assert engine.metadata("backlog-8192") is not None
    finally:
        if engine is not None:
            engine.close()
        service._group_worker.shutdown()


def test_live_cut_drains_fixed_journal_in_bounded_batches(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    service = history.HistoryReader()
    query = history.HistoryQuery.parse({"group_by": "status"}, now=3000)
    cancel = threading.Event()
    engine = service._build_engine(
        str(path), query, [make_flow("x", 2500)], cancel, (), None
    )
    calls = []

    class Index:
        @contextmanager
        def read_cut(self, cancel, *, bounds):
            assert bounds == (1200, 3000)
            yield None, 8193

        def changes(self, db, after, cut, *, limit):
            calls.append((after, cut, limit))
            return (
                (i, "remove", "x", None)
                for i in range(after + 1, min(cut, after + limit) + 1)
            )

    try:
        service.live_index = Index()
        service._copy_live(engine, [], cancel, initial=False)
        assert calls == [(0, 8193, 8192), (8192, 8193, 8192)]
        assert engine.live_cut == 8193
        assert engine.metadata("x") is None
        engine.live_cut = 0
        with mock.patch.object(Index, "changes", return_value=iter(())):
            service._copy_live(engine, [], cancel, initial=False)
        assert engine.live_cut == 8193

        def cancelled_changes(self, db, after, cut, *, limit):
            cancel.set()
            return iter([(1, "remove", "x", None)])

        engine.live_cut = 0
        with mock.patch.object(Index, "changes", cancelled_changes):
            with pytest.raises(sqlite.StorageError, match="cancelled"):
                service._copy_live(engine, [], cancel, initial=False)
    finally:
        engine.close()
        service._group_worker.shutdown()


async def test_grouped_raw_initialization_failure_removes_all_scratch(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(make_flow("a", 2000).get_state())
    service = history.HistoryReader(snapshot_bytes=32768)
    temporary_directory = tempfile.TemporaryDirectory
    created = []

    def directory(**kwargs):
        result = temporary_directory(dir=tmp_path, **kwargs)
        created.append(Path(result.name))
        return result

    try:
        with mock.patch.object(history.tempfile, "TemporaryDirectory", directory):
            with pytest.raises((sqlite.StorageError, history.SnapshotError)):
                await service.grouped_page(
                    str(path),
                    history.HistoryQuery.parse({"group_by": "host"}, now=3000),
                    [],
                    threading.Event(),
                    instance_id="test",
                    live_generation=lambda: 0,
                )
        assert created
        assert all(not directory.exists() for directory in created)
        assert not service._snapshots
        assert not service._owned_engines
        with closing(sqlite3.connect(path)) as db:
            assert db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0] == 0
    finally:
        await service.done()


class TestHistory(tornado.testing.AsyncHTTPTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "history.db"
        with sqlite.SQLiteFlowStore(self.path) as store:
            for flow_id, timestamp in (("a", 1000), ("b", 1500), ("c", 2000)):
                store.write(make_flow(flow_id, timestamp).get_state())
            f = make_flow("d", 3000)
            raw = tnetstring.dumps(f.get_state())
            store.db.execute("BEGIN IMMEDIATE")
            store._write_chunked(
                sqlite._metadata(f),
                io.BytesIO(raw),
                len(raw),
                hashlib.sha256(raw).digest(),
            )
            store.db.commit()
        super().setUp()

    def tearDown(self):
        try:
            self.io_loop.run_sync(self.master.history.done)
        finally:
            super().tearDown()

    def get_app(self):
        async def make_master():
            return WebMaster(Options(), with_termlog=False)

        self.master = self.io_loop.asyncio_loop.run_until_complete(make_master())
        patcher = mock.patch.object(self.master.addons.get("save"), "_configure_sqlite")
        patcher.start()
        self.addCleanup(patcher.stop)
        self.master.options.update(save_sqlite_file=str(self.path))
        self.live = make_flow("c", 2500)
        self.live.response.status_code = 418
        self.live.response.raw_content = None
        self.master.view.add([self.live, make_flow("e", 2800)])
        self.master.events.done()
        self.master.app.settings["xsrf_cookies"] = False
        return self.master.app

    @property
    def cookie(self):
        name = self._app.settings["auth_cookie_name"]()
        value = create_signed_value(
            self._app.settings["cookie_secret"],
            name,
            app.AuthRequestHandler.AUTH_COOKIE_VALUE,
        ).decode()
        return f"{name}={value}"

    def url(self, path="/flow-history", **query):
        return (
            path
            + "?"
            + urllib.parse.urlencode(
                {"instance_id": getattr(self.master, "instance_id", "missing"), **query}
            )
        )

    def page(self, path="/flow-history", **query):
        with mock.patch("time.time", return_value=3000):
            response = self.fetch(
                self.url(path, **query), headers={"Cookie": self.cookie}
            )
        assert response.code == 200, response.body
        return json.loads(response.body)

    def test_metadata_merge_and_state(self):
        result = self.page()
        assert result["count"] == 4
        assert result["offset"] == 0
        assert [r["id"] for r in result["rows"]] == ["b", "c", "e", "d"]
        assert result["rows"][1] == {
            "id": "c",
            "sequence": "3",
            "timestamp_created": 2500.0,
            "type": "http",
            "method": "GET",
            "host": "example.test",
            "path": "/c",
            "status_code": 418,
            "request_size": 7,
            "response_size": None,
            "payload_version": None,
            "live": True,
            "highlighted": False,
            "cursor": [2500.0, "3", "c"],
        }
        assert result["rows"][2]["sequence"] == "9223372036854775807"
        assert result["rows"][3]["payload_version"] == 2
        assert result["instance_id"] == self.master.instance_id
        assert (result["lower"], result["upper"]) == (1200, 3000)
        assert result["historical_search_unsupported"] is False
        assert result["historical_highlight_unsupported"] is False
        state = self.fetch("/state", headers={"Cookie": self.cookie})
        assert json.loads(state.body)["sqlite_history"] is True

    def test_revision_snapshot_contract_and_frozen_bounds(self):
        first = self.page(group_by="status", offset=0, limit=1)
        assert isinstance(first["snapshot"], str)
        assert first["count"] == 4
        assert first["group_count"] == 2
        group = first["groups"][0]
        assert group["label"] == "200"
        assert group["key"].startswith("g1:")
        with mock.patch("time.time", return_value=5000):
            response = self.fetch(
                self.url(group_by="status", snapshot=first["snapshot"], offset=1),
                headers={"Cookie": self.cookie},
            )
        assert response.code == 200, response.body
        second = json.loads(response.body)
        assert (second["lower"], second["upper"]) == (1200, 3000)
        assert second["groups"][0]["label"] == "418"
        members = self.page(
            group_by="status",
            snapshot=first["snapshot"],
            group_key=group["key"],
            offset=0,
        )
        assert members["count"] == 3
        assert [row["id"] for row in members["rows"]] == ["b", "e", "d"]
        assert [row["sequence"] for row in members["rows"]] == [
            "2",
            "9223372036854775807",
            "4",
        ]
        found = self.page(
            group_by="status", snapshot=first["snapshot"], lookup_flow="e"
        )
        assert found["group"] == group
        assert found["member_ordinal"] == 1
        assert (
            self.page(
                group_by="status", snapshot=first["snapshot"], lookup_group=group["key"]
            )["group"]
            == group
        )
        assert (
            self.page(
                group_by="status",
                snapshot=first["snapshot"],
                lookup_group="g1:" + "0" * 64,
            )["group"]
            is None
        )
        missing = self.page(
            group_by="status", snapshot=first["snapshot"], lookup_flow="missing"
        )
        assert missing["group"] is None
        assert missing["member_ordinal"] is None

    def test_group_lookup_batch_is_ordered_bounded_and_snapshot_scoped(self):
        first = self.page(group_by="status")
        groups = first["groups"]
        keys = [groups[1]["key"], "g1:" + "0" * 64, groups[0]["key"]]
        self.live.response.status_code = 205
        self.master.view.update([self.live])
        result = self.page(
            group_by="status",
            snapshot=first["snapshot"],
            lookup_groups=json.dumps(keys),
        )
        assert result["groups"] == [groups[1], None, groups[0]]
        assert (
            self.page(
                group_by="status", snapshot=first["snapshot"], lookup_groups="[]"
            )["groups"]
            == []
        )
        assert (
            len(
                self.page(
                    group_by="status",
                    snapshot=first["snapshot"],
                    lookup_groups=json.dumps([keys[0]] * 128),
                )["groups"]
            )
            == 128
        )

    def test_grouped_disk_inventory_has_no_legacy_live_row_or_ram_cutoff(self):
        self.master.history.LIVE_ROWS = 1
        self.master.history.LIVE_BYTES = 1
        result = self.page(group_by="status")
        assert result["count"] == 4
        assert [(group["label"], group["count"]) for group in result["groups"]] == [
            ("200", 3),
            ("418", 1),
        ]

    def test_switching_group_selectors_keeps_recent_snapshots_usable(self):
        pages = {
            selector: self.page(group_by=selector)
            for selector in ("host", "method", "status", "type")
        }
        for selector, page in pages.items():
            assert self.page(group_by=selector, snapshot=page["snapshot"]) == page

    def test_grouped_filter_resource_failure_is_explicit_and_does_not_poison_history(
        self,
    ):
        self.live.response.raw_content = b"x" * (1024 * 1024 + 1)
        self.master.view.update([self.live])
        response = self.fetch(
            self.url(group_by="status", search="~bs x", mode="absolute"),
            headers={"Cookie": self.cookie},
        )
        assert response.code == 503
        assert b"filter input unavailable" in response.body
        assert b"1048576" in response.body
        assert b"budget" in response.body
        assert self.page(group_by="status")["count"] == 4

    def test_grouped_filter_eviction_error_preserves_ungrouped_filtering(self):
        self.master.history.live_index.body_cache_bytes = 0
        self.live.response.raw_content = b"captured"
        self.master.view.update([self.live])
        self.live.response.raw_content = b"unnotified"
        response = self.fetch(
            self.url(group_by="status", search="~bs captured", mode="absolute"),
            headers={"Cookie": self.cookie},
        )
        assert response.code == 503
        assert b"captured event-time" in response.body
        assert b"evicted" in response.body
        assert b"No grouping" in response.body
        flat = self.page(search="~bs unnotified", mode="absolute")
        assert [row["id"] for row in flat["rows"]] == [self.live.id]
        assert self.page(group_by="status")["count"] == 4

    def test_grouped_boolean_filter_counts_are_complete_within_live_time_intersection(
        self,
    ):
        self.live.response.raw_content = b"x" * (1024 * 1024 + 1)
        self.master.view.update([self.live])
        query = {"mode": "absolute", "from": "2400", "until": "2700"}
        result = self.page(group_by="status", search="~bs needle | ~all", **query)
        assert result["historical_search_unsupported"] is True
        assert result["count"] == 1
        assert [(g["label"], g["count"]) for g in result["groups"]] == [("418", 1)]
        members = self.page(
            group_by="status",
            search="~bs needle | ~all",
            snapshot=result["snapshot"],
            group_key=result["groups"][0]["key"],
            **query,
        )
        assert [row["id"] for row in members["rows"]] == [self.live.id]
        highlighted = self.page(
            group_by="status", highlight="~bs needle | ~all", mode="absolute"
        )
        assert highlighted["count"] == 5
        assert sum(g["count"] for g in highlighted["groups"]) == 5

    def test_published_members_are_immutable_and_refresh_is_incremental(self):
        first = self.page(group_by="status")
        key = first["groups"][0]["key"]
        before = self.page(group_by="status", snapshot=first["snapshot"], group_key=key)
        with sqlite.SQLiteFlowStore(self.path) as store:
            store.db.execute(
                "UPDATE flows SET timestamp=1600,status_code=201 WHERE flow_id='b'"
            )
            store.db.commit()
            store.write(make_flow("new", 2900).get_state())
        self.live.response.status_code = 202
        self.master.view.update([self.live])
        with mock.patch.object(self.master.view, "resolve", side_effect=AssertionError):
            assert (
                self.page(group_by="status", snapshot=first["snapshot"], group_key=key)
                == before
            )
            fresh = self.page(group_by="status", refresh=first["snapshot"])
        assert fresh["snapshot"] != first["snapshot"]
        assert [(g["label"], g["count"]) for g in fresh["groups"]] == [
            ("200", 3),
            ("201", 1),
            ("202", 1),
        ]
        assert (
            self.page(group_by="status", snapshot=first["snapshot"], group_key=key)
            == before
        )

    def test_refresh_expires_relative_members_without_a_source_write(self):
        with sqlite.SQLiteFlowStore(self.path) as store:
            store.write(make_flow("future", 3500).get_state())
        first = self.page(group_by="status")
        with mock.patch("time.time", return_value=4000):
            response = self.fetch(
                self.url(group_by="status", refresh=first["snapshot"]),
                headers={"Cookie": self.cookie},
            )
        assert response.code == 200, response.body
        fresh = json.loads(response.body)
        assert fresh["count"] == 4
        assert (fresh["lower"], fresh["upper"]) == (2200, 4000)
        assert self.page(group_by="status", snapshot=first["snapshot"])["count"] == 4

    def test_initial_group_cut_finishes_under_continuous_live_and_sqlite_writes(self):
        self.master.history.SOURCE_BATCH = 1
        original = self.master.history._source_batch
        stages = 0

        def batch(path, revision, identity, cancel, sql="SELECT 1", args=(), **kwargs):
            nonlocal stages
            result = original(path, revision, identity, cancel, sql, args, **kwargs)
            if "ORDER BY sequence" in sql:
                stages += 1
                with sqlite.SQLiteFlowStore(self.path) as store:
                    if stages == 1:
                        store.db.execute(
                            "UPDATE flows SET timestamp=2500 WHERE flow_id='a'"
                        )
                        store.db.execute(
                            "UPDATE flows SET sequence=1000,flow_id='renamed',status_code=201 WHERE flow_id='b'"
                        )
                        store.db.execute(
                            "UPDATE flows SET status_code=202 WHERE flow_id='renamed'"
                        )
                        store.db.execute("DELETE FROM flows WHERE flow_id='d'")
                        store.db.commit()
                    store.write(make_flow("stream-" + str(stages), 2900).get_state())
                    assert (
                        store.db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[
                            0
                        ]
                        == 0
                    )
                notified = threading.Event()

                def update():
                    self.live.response.status_code = 210 + stages
                    self.master.view.update([self.live])
                    notified.set()

                self.io_loop.asyncio_loop.call_soon_threadsafe(update)
                assert notified.wait(2)
            elif sql == "SELECT 1":
                with sqlite.SQLiteFlowStore(self.path) as store:
                    store.write(make_flow("after-cut", 2900).get_state())
            return result

        with mock.patch.object(self.master.history, "_source_batch", batch):
            page = self.page(group_by="status")
        assert stages == 3
        assert page["count"] == 4 + stages
        ids = []
        for group in page["groups"]:
            ids.extend(
                row["id"]
                for row in self.page(
                    group_by="status", snapshot=page["snapshot"], group_key=group["key"]
                )["rows"]
            )
        assert set(ids) == {
            "a",
            "renamed",
            "c",
            "e",
            "stream-1",
            "stream-2",
            "stream-3",
        }
        fresh = self.page(group_by="status", refresh=page["snapshot"])
        assert fresh["count"] == page["count"] + 1

    def test_registering_a_filter_does_not_recapture_unnotified_live_metadata(self):
        self.live.response.status_code = 205
        first = self.page(group_by="status", highlight="~c 205")
        found = self.page(
            group_by="status",
            highlight="~c 205",
            snapshot=first["snapshot"],
            lookup_flow="c",
        )
        assert found["group"]["label"] == "418"
        self.master.view.update([self.live])
        fresh = self.page(
            group_by="status", highlight="~c 205", refresh=first["snapshot"]
        )
        found = self.page(
            group_by="status",
            highlight="~c 205",
            snapshot=fresh["snapshot"],
            lookup_flow="c",
        )
        assert found["group"]["label"] == "205"

    def test_grouped_live_capture_waits_for_notification_and_keeps_search_flags(self):
        first = self.page(group_by="status", search="~c 418", highlight="~bq request")
        assert first["count"] == 1
        # This is the async gap between a mutable flow edit and View.update.
        self.live.response.status_code = 200
        with mock.patch.object(
            self.master.view,
            "resolve",
            side_effect=AssertionError("refresh inventoried flows"),
        ):
            gap = self.page(
                group_by="status",
                search="~c 418",
                highlight="~bq request",
                refresh=first["snapshot"],
            )
            assert gap["count"] == 1
            self.master.view.update([self.live])
            notified = self.page(
                group_by="status",
                search="~c 418",
                highlight="~bq request",
                refresh=gap["snapshot"],
            )
        assert notified["count"] == 0
        old = self.page(
            group_by="status",
            search="~c 418",
            highlight="~bq request",
            snapshot=first["snapshot"],
            group_key=first["groups"][0]["key"],
        )
        assert old["rows"][0]["status_code"] == 418
        assert old["rows"][0]["highlighted"] is True

    def test_refresh_only_applies_changed_ids_and_live_removal_reveals_durable(self):
        first = self.page(group_by="status")
        original = history.GroupStore.apply
        calls = []

        def apply(store, changes, **kwargs):
            calls.append(set(changes))
            return original(store, changes, **kwargs)

        with sqlite.SQLiteFlowStore(self.path) as store:
            store.db.execute("UPDATE flows SET status_code=203 WHERE flow_id='c'")
            store.db.commit()
        self.master.view.remove([self.live])
        with (
            mock.patch.object(history.GroupStore, "apply", apply),
            mock.patch.object(
                history.GroupStore,
                "build",
                side_effect=AssertionError("refresh rebuilt groups"),
            ),
        ):
            fresh = self.page(group_by="status", refresh=first["snapshot"])
        assert calls == [{"c"}]
        group = next(group for group in fresh["groups"] if group["label"] == "203")
        members = self.page(
            group_by="status", snapshot=fresh["snapshot"], group_key=group["key"]
        )
        assert members["rows"][0]["live"] is False
        assert members["rows"][0]["sequence"] == "3"

    def test_refresh_validation(self):
        token = "s1:" + "0" * 32
        for query in (
            {"refresh": token},
            {"group_by": "host", "refresh": "bad"},
            {"group_by": "host", "refresh": token, "snapshot": token},
            {"group_by": "host", "refresh": token, "offset": 1},
            {"group_by": "host", "refresh": token, "lookup_flow": "a"},
        ):
            assert (
                self.fetch(self.url(**query), headers={"Cookie": self.cookie}).code
                == 400
            )
        assert (
            self.fetch(
                self.url(group_by="host", refresh=token),
                headers={"Cookie": self.cookie},
            ).code
            == 409
        )

    def test_refresh_can_advance_to_absolute_bounds_without_rebuilding(self):
        first = self.page(group_by="status")
        with sqlite.SQLiteFlowStore(self.path) as store:
            store.db.execute(
                "UPDATE flows SET timestamp=1100,status_code=201 WHERE flow_id='a'"
            )
            store.db.commit()
        with mock.patch.object(
            history.GroupStore, "build", side_effect=AssertionError("full rebuild")
        ):
            fresh = self.page(
                group_by="status",
                refresh=first["snapshot"],
                mode="absolute",
                **{"from": 1000, "until": 4000},
            )
        assert fresh["count"] == 5
        assert (fresh["lower"], fresh["upper"]) == (1000, 4000)
        assert self.page(group_by="status", snapshot=first["snapshot"])["count"] == 4

    def test_grouped_old_pages_do_not_open_or_stat_the_source(self):
        first = self.page(group_by="status")
        key = first["groups"][0]["key"]
        with (
            mock.patch.object(
                sqlite, "SQLiteFlowReader", side_effect=AssertionError("opened source")
            ),
            mock.patch.object(
                self.master.history,
                "_path_identity",
                side_effect=AssertionError("statted source"),
            ),
            mock.patch.object(
                self.master.view, "resolve", side_effect=AssertionError("resolved live")
            ),
        ):
            members = self.page(
                group_by="status", snapshot=first["snapshot"], group_key=key
            )
            assert [row["id"] for row in members["rows"]] == ["b", "e", "d"]
            assert members["rows"][-1]["payload_version"] == 2
            assert (
                self.page(
                    group_by="status", snapshot=first["snapshot"], lookup_flow="d"
                )["member_ordinal"]
                == 2
            )

    def test_incomplete_journal_invalidates_refresh_but_keeps_old_snapshot(self):
        first = self.page(group_by="status")
        with sqlite.SQLiteFlowStore(self.path) as store:
            store.write(make_flow("new-journal-entry", 2900).get_state())
        with mock.patch.object(self.master.history, "_journal", return_value=[]):
            response = self.fetch(
                self.url(group_by="status", refresh=first["snapshot"]),
                headers={"Cookie": self.cookie},
            )
        assert response.code == 503
        assert (
            self.page(group_by="status", snapshot=first["snapshot"])["count"]
            == first["count"]
        )
        response = self.fetch(
            self.url(group_by="status", refresh=first["snapshot"]),
            headers={"Cookie": self.cookie},
        )
        assert response.code == 409

    def test_search_refresh_removal_and_shifted_window(self):
        first = self.page(group_by="status", search="~c 418")
        assert first["count"] == 1
        self.master.view.remove([self.live])
        with mock.patch("time.time", return_value=4000):
            response = self.fetch(
                self.url(group_by="status", search="~c 418", refresh=first["snapshot"]),
                headers={"Cookie": self.cookie},
            )
        assert response.code == 200
        assert json.loads(response.body)["count"] == 0
        assert (
            self.page(group_by="status", search="~c 418", snapshot=first["snapshot"])[
                "count"
            ]
            == 1
        )

    def test_durable_update_expiring_past_lower_bound(self):
        first = self.page(group_by="status")
        with sqlite.SQLiteFlowStore(self.path) as store:
            store.db.execute("UPDATE flows SET timestamp=100 WHERE flow_id='b'")
            store.db.commit()
        fresh = self.page(group_by="status", refresh=first["snapshot"])
        assert fresh["count"] == first["count"] - 1
        assert (
            self.page(group_by="status", snapshot=first["snapshot"])["count"]
            == first["count"]
        )

    def test_expired_change_journal_is_explicit_and_keeps_old_pages(self):
        first = self.page(group_by="status")
        with sqlite.SQLiteFlowStore(self.path) as store:
            store.db.execute("UPDATE flows SET status_code=201 WHERE flow_id='b'")
            store.db.execute(
                "UPDATE flow_change_state SET floor=(SELECT revision FROM flow_revision)"
            )
            store.db.commit()
        response = self.fetch(
            self.url(group_by="status", refresh=first["snapshot"]),
            headers={"Cookie": self.cookie},
        )
        assert response.code == 503
        assert (
            self.page(group_by="status", snapshot=first["snapshot"])["groups"]
            == first["groups"]
        )

    def test_oversized_values_have_per_record_identities_and_bounded_labels(self):
        with sqlite.SQLiteFlowStore(self.path) as store:
            store.db.execute(
                "UPDATE flows SET host=? WHERE flow_id IN ('b','d')", ("h" * 1048577,)
            )
            store.db.commit()
        self.live.request.host = "h" * 1048577
        self.master.view.update([self.live])
        first = self.page(group_by="host")
        exceptional = [
            group
            for group in first["groups"]
            if group["label"] == "value too large to group"
        ]
        assert len(exceptional) == 3
        assert len({group["key"] for group in exceptional}) == 3
        assert all(group["count"] == 1 for group in exceptional)
        for group in exceptional:
            members = self.page(
                group_by="host", snapshot=first["snapshot"], group_key=group["key"]
            )
            assert members["rows"][0]["host"] is None

    def test_grouped_protocol_sizes_are_captured_incrementally(self):
        f = tflow.ttcpflow()
        f.timestamp_created = 2600
        self.master.view.add([f])
        first = self.page(group_by="type")
        group = next(group for group in first["groups"] if group["label"] == "tcp")
        before = self.page(
            group_by="type", snapshot=first["snapshot"], group_key=group["key"]
        )
        assert (
            before["rows"][0]["request_size"],
            before["rows"][0]["response_size"],
        ) == (5, 7)
        f.messages.append(tcp.TCPMessage(True, b"abc"))
        self.master.view.update([f])
        fresh = self.page(group_by="type", refresh=first["snapshot"])
        after = self.page(
            group_by="type", snapshot=fresh["snapshot"], group_key=group["key"]
        )
        assert (
            after["rows"][0]["request_size"],
            after["rows"][0]["response_size"],
        ) == (8, 7)
        assert (
            self.page(
                group_by="type", snapshot=first["snapshot"], group_key=group["key"]
            )
            == before
        )

    def test_relative_refresh_completes_expiration_before_publication(self):
        with sqlite.SQLiteFlowStore(self.path) as store:
            store.db.execute("BEGIN")
            store.db.executemany(
                "INSERT INTO flows (flow_id,timestamp,flow_type,status_code,payload_version,codec,raw_length,payload) VALUES (?,?,'http',200,1,'zstd',1,x'00')",
                (("expiring-" + str(i), 1450 + i / 20) for i in range(8193)),
            )
            store.db.commit()
        first = self.page(group_by="status")
        with mock.patch("time.time", return_value=4000):
            response = self.fetch(
                self.url(group_by="status", refresh=first["snapshot"]),
                headers={"Cookie": self.cookie},
            )
        assert response.code == 200, response.body
        current = json.loads(response.body)
        assert current["count"] == 3
        assert (current["lower"], current["upper"]) == (2200, 4000)
        assert (
            self.page(group_by="status", snapshot=first["snapshot"])["count"]
            == first["count"]
        )

    def test_relative_refresh_expires_large_equal_timestamp_cohort(self):
        with sqlite.SQLiteFlowStore(self.path) as store:
            store.db.executemany(
                "INSERT INTO flows (flow_id,timestamp,flow_type,status_code,payload_version,codec,raw_length,payload) VALUES (?,1450,'http',200,1,'zstd',1,x'00')",
                ((f"cohort-{i}",) for i in range(8193)),
            )
            store.db.commit()
        first = self.page(group_by="status")
        sizes = []
        apply = history.GroupStore.apply

        def bounded(store, changes, **kwargs):
            sizes.append(len(changes))
            return apply(store, changes, **kwargs)

        with (
            mock.patch("time.time", return_value=4000),
            mock.patch.object(history.GroupStore, "apply", bounded),
        ):
            response = self.fetch(
                self.url(group_by="status", refresh=first["snapshot"]),
                headers={"Cookie": self.cookie},
            )
        assert response.code == 200, response.body
        fresh = json.loads(response.body)
        assert (fresh["lower"], fresh["upper"]) == (2200, 4000)
        assert fresh["count"] == 3
        assert sum(sizes) >= 8193
        assert max(sizes) <= 8192
        assert (
            self.page(group_by="status", snapshot=first["snapshot"])["count"]
            == first["count"]
        )

    def test_relative_refresh_enters_large_equal_timestamp_cohort(self):
        with sqlite.SQLiteFlowStore(self.path) as store:
            store.db.executemany(
                "INSERT INTO flows (flow_id,timestamp,flow_type,status_code,payload_version,codec,raw_length,payload) VALUES (?,3500,'http',200,1,'zstd',1,x'00')",
                ((f"entering-{i}",) for i in range(8193)),
            )
            store.db.commit()
        first = self.page(group_by="status")
        batches = []
        original = self.master.history._source_batch

        def source_batch(
            path, revision, identity, cancel, sql="SELECT 1", args=(), **kwargs
        ):
            result = original(path, revision, identity, cancel, sql, args, **kwargs)
            if "(timestamp,sequence,flow_id)>" in sql:
                batches.append(len(result[1]))
                with sqlite.SQLiteFlowStore(self.path) as store:
                    assert (
                        store.db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[
                            0
                        ]
                        == 0
                    )
                    if len(batches) == 1:
                        store.db.execute(
                            "UPDATE flows SET status_code=201 WHERE flow_id='entering-0'"
                        )
                        store.write(make_flow("during-scan", 3500).get_state())
            elif sql == "SELECT 1":
                with sqlite.SQLiteFlowStore(self.path) as store:
                    store.write(make_flow("after-refresh-cut", 3500).get_state())
            return result

        with (
            mock.patch("time.time", return_value=4000),
            mock.patch.object(self.master.history, "_source_batch", source_batch),
        ):
            response = self.fetch(
                self.url(group_by="status", refresh=first["snapshot"]),
                headers={"Cookie": self.cookie},
            )
        assert response.code == 200, response.body
        fresh = json.loads(response.body)
        assert (fresh["lower"], fresh["upper"]) == (2200, 4000)
        assert fresh["count"] == 8197
        assert any(
            group["label"] == "201" and group["count"] == 1 for group in fresh["groups"]
        )
        assert sum(batches) == 8193
        assert max(batches) <= 8192
        assert (
            self.page(group_by="status", snapshot=first["snapshot"])["count"]
            == first["count"]
        )

    def test_cancel_entering_scan_preserves_old_snapshot(self):
        with sqlite.SQLiteFlowStore(self.path) as store:
            store.db.executemany(
                "INSERT INTO flows (flow_id,timestamp,flow_type,status_code,payload_version,codec,raw_length,payload) VALUES (?,3500,'http',200,1,'zstd',1,x'00')",
                ((f"cancel-{i}",) for i in range(8193)),
            )
            store.db.commit()
        first = self.page(group_by="status")
        original = self.master.history._source_batch
        scanned = []

        def source_batch(
            path, revision, identity, cancel, sql="SELECT 1", args=(), **kwargs
        ):
            result = original(path, revision, identity, cancel, sql, args, **kwargs)
            if "(timestamp,sequence,flow_id)>" in sql and result[1]:
                scanned.append(len(result[1]))
                cancel.set()
            return result

        with (
            mock.patch("time.time", return_value=4000),
            mock.patch.object(self.master.history, "_source_batch", source_batch),
        ):
            response = self.fetch(
                self.url(group_by="status", refresh=first["snapshot"]),
                headers={"Cookie": self.cookie},
            )
        assert len(scanned) == 1
        assert response.body == b""
        assert (
            self.page(group_by="status", snapshot=first["snapshot"])["count"]
            == first["count"]
        )

    def test_grouped_refresh_uses_bounded_fixed_journal_batches(self):
        first = self.page(group_by="status")
        with sqlite.SQLiteFlowStore(self.path) as store:
            store.db.execute("BEGIN")
            store.db.executemany(
                "INSERT INTO flows (flow_id,timestamp,flow_type,status_code,payload_version,codec,raw_length,payload) VALUES (?,2900,'http',200,2,'native',1,x'00')",
                (("backlog-" + str(i),) for i in range(8193)),
            )
            store.db.commit()
        apply = history.GroupStore.apply
        sizes = []

        def bounded(store, changes, **kwargs):
            sizes.append(len(changes))
            return apply(store, changes, **kwargs)

        with (
            mock.patch.object(history.GroupStore, "apply", bounded),
            mock.patch.object(
                history.GroupStore, "build", side_effect=AssertionError("full rebuild")
            ),
        ):
            last = self.page(group_by="status", refresh=first["snapshot"])
            assert last["count"] == first["count"] + 8193
        assert sum(sizes) == 8193
        assert max(sizes) <= 512
        assert (
            self.page(group_by="status", snapshot=first["snapshot"])["count"]
            == first["count"]
        )

    def test_revision_snapshot_preserves_sources_but_rejects_changed_query(self):
        first = self.page(group_by="status", offset=0)
        for change in ({"group_by": "host"}, {"search": "~c 200"}, {"duration": "10"}):
            response = self.fetch(
                self.url(
                    **{"group_by": "status", "snapshot": first["snapshot"], **change}
                ),
                headers={"Cookie": self.cookie},
            )
            assert response.code == 409
        with sqlite.SQLiteFlowStore(self.path) as store:
            store.write(make_flow("new", 2900).get_state())
        response = self.fetch(
            self.url(group_by="status", snapshot=first["snapshot"]),
            headers={"Cookie": self.cookie},
        )
        assert response.code == 200
        fresh = self.page(group_by="status", offset=0)
        self.master.view.update([self.live])
        response = self.fetch(
            self.url(group_by="status", snapshot=fresh["snapshot"]),
            headers={"Cookie": self.cookie},
        )
        assert response.code == 200

    def test_grouped_continuations_do_not_resolve_live_store(self):
        first = self.page(group_by="status")
        frozen = {
            "mode": "absolute",
            "from": first["lower"],
            "until": first["upper"],
            "group_by": "status",
            "snapshot": first["snapshot"],
        }
        group = first["groups"][0]
        with mock.patch.object(
            self.master.view,
            "resolve",
            side_effect=AssertionError("Continuation copied the live store"),
        ):
            assert self.page(**frozen)["groups"] == first["groups"]
            members = self.page(**frozen, group_key=group["key"])
            assert [row["id"] for row in members["rows"]] == ["b", "e", "d"]
            assert self.page(**frozen, lookup_group=group["key"])["group"] == group
            assert self.page(**frozen, lookup_flow="e")["member_ordinal"] == 1
            self.master.view.update([self.live])
            assert (
                self.fetch(
                    self.url(**frozen),
                    headers={"Cookie": self.cookie},
                ).code
                == 200
            )

    def test_relative_snapshot_accepts_frozen_absolute_continuations(self):
        first = self.page(group_by="status", highlight="~c 200", limit=1)
        assert (first["lower"], first["upper"]) == (1200, 3000)
        frozen = {
            "mode": "absolute",
            "from": first["lower"],
            "until": first["upper"],
            "group_by": "status",
            "highlight": "~c 200",
            "snapshot": first["snapshot"],
        }
        with mock.patch("time.time", return_value=5000):
            response = self.fetch(
                self.url(**frozen, offset=1, limit=1),
                headers={"Cookie": self.cookie},
            )
        assert response.code == 200, response.body
        second = json.loads(response.body)
        assert second["snapshot"] == first["snapshot"]
        assert (second["lower"], second["upper"]) == (1200, 3000)
        assert second["count"] == 4
        assert second["groups"][0]["label"] == "418"
        group = first["groups"][0]
        members = self.page(**frozen, group_key=group["key"])
        assert [row["id"] for row in members["rows"]] == ["b", "e", "d"]
        assert [row["highlighted"] for row in members["rows"]] == [False, True, False]
        assert self.page(**frozen, lookup_group=group["key"])["group"] == group
        found = self.page(**frozen, lookup_flow="e")
        assert found["group"] == group
        assert found["member_ordinal"] == 1
        for change in (
            {"from": 1201},
            {"until": 3001},
            {"from": None},
            {"until": None},
            {"group_by": "host"},
            {"search": "~c 200"},
            {"highlight": ""},
        ):
            changed = {
                key: value
                for key, value in (frozen | change).items()
                if value is not None
            }
            assert (
                self.fetch(
                    self.url(**changed),
                    headers={"Cookie": self.cookie},
                ).code
                == 409
            )
        with sqlite.SQLiteFlowStore(self.path) as store:
            store.write(make_flow("new", 2900).get_state())
        assert (
            self.fetch(
                self.url(**frozen),
                headers={"Cookie": self.cookie},
            ).code
            == 200
        )

    def test_revision_snapshot_capacity_preserves_the_published_grace_period(self):
        self.master.history.SNAPSHOT_CAPACITY = 1
        first = self.page(group_by="host")
        response = self.fetch(
            self.url(group_by="status"), headers={"Cookie": self.cookie}
        )
        assert response.code == 503
        assert (
            self.fetch(
                self.url(group_by="host", snapshot=first["snapshot"]),
                headers={"Cookie": self.cookie},
            ).code
            == 200
        )
        with mock.patch.object(history, "time", wraps=history.time) as clock:
            clock.monotonic.return_value = float("inf")
            assert (
                self.fetch(
                    self.url(group_by="host", snapshot=first["snapshot"]),
                    headers={"Cookie": self.cookie},
                ).code
                == 409
            )

    def test_revision_snapshot_ineligible_live_suppresses_history(self):
        self.live.timestamp_created = 0
        self.master.view.update([self.live])
        page = self.page(group_by="status", highlight="~c 200")
        assert page["count"] == 3
        assert page["group_count"] == 1
        members = self.page(
            group_by="status",
            highlight="~c 200",
            snapshot=page["snapshot"],
            group_key=page["groups"][0]["key"],
        )
        assert [row["id"] for row in members["rows"]] == ["b", "e", "d"]
        assert [row["highlighted"] for row in members["rows"]] == [False, True, False]

    def test_revision_snapshot_non_http_summary_has_nullable_fields(self):
        live = tflow.ttcpflow()
        live.timestamp_created = 2500
        self.master.view.add([live])
        page = self.page(group_by="type")
        group = next(group for group in page["groups"] if group["label"] == "tcp")
        members = self.page(
            group_by="type", snapshot=page["snapshot"], group_key=group["key"]
        )
        row = members["rows"][0]
        for name in ("host", "method", "path", "status_code", "payload_version"):
            assert row[name] is None
        assert row["id"] == live.id
        assert row["live"] is True

    def test_revision_snapshot_reconciles_write_between_source_batches(self):
        self.master.history.SOURCE_BATCH = 1
        original = self.master.history._source_batch
        changed = False

        def mutate_after_chunk(
            path, revision, identity, cancel, sql="SELECT 1", args=(), **kwargs
        ):
            nonlocal changed
            result = original(path, revision, identity, cancel, sql, args, **kwargs)
            if "ORDER BY sequence" in sql and not changed:
                changed = True
                with sqlite.SQLiteFlowStore(self.path) as store:
                    store.write(make_flow("new", 2900).get_state())
                    assert (
                        store.db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[
                            0
                        ]
                        == 0
                    )
            return result

        with mock.patch.object(
            self.master.history, "_source_batch", mutate_after_chunk
        ):
            response = self.fetch(
                self.url(group_by="status", mode="absolute"),
                headers={"Cookie": self.cookie},
            )
        assert changed
        assert response.code == 200, response.body
        assert json.loads(response.body)["count"] == 6
        assert len(self.master.history._snapshots) == 1

    def test_revision_snapshot_live_change_after_worker_return(self):
        original = self.master.history.grouped_page

        async def change(*args, **kwargs):
            result = await original(*args, **kwargs)
            self.master.view.update([self.live])
            return result

        with mock.patch.object(self.master.history, "grouped_page", change):
            response = self.fetch(
                self.url(group_by="status"), headers={"Cookie": self.cookie}
            )
        assert response.code == 200

    def test_revision_snapshot_path_replacement(self):
        page = self.page(group_by="status")
        replacement = self.path.with_name("replacement.db")
        with sqlite.SQLiteFlowStore(replacement) as store:
            store.write(make_flow("replacement", 2900).get_state())
        replacement.replace(self.path)
        assert (
            self.fetch(
                self.url(group_by="status", snapshot=page["snapshot"]),
                headers={"Cookie": self.cookie},
            ).code
            == 200
        )

    def test_revision_snapshot_reinitializes_replacement_legacy_database(self):
        first = self.page(group_by="type")
        replacement = self.path.with_name("legacy.db")
        with closing(sqlite3.connect(replacement)) as db, db:
            db.execute(sqlite._SCHEMA)
            db.execute(f"PRAGMA application_id={sqlite._APP_ID}")
            db.execute("PRAGMA user_version=1")
            db.execute(
                "INSERT INTO flows (flow_id,timestamp,flow_type,payload_version,codec,raw_length,payload) VALUES ('legacy',2900,'tcp',1,'zstd',1,x'00')"
            )
        replacement.replace(self.path)
        assert (
            self.fetch(
                self.url(group_by="type", snapshot=first["snapshot"]),
                headers={"Cookie": self.cookie},
            ).code
            == 200
        )
        # A source replacement does not invalidate published scratch pages. An
        # explicit new configuration generation selects a new engine.
        self.master.history.generation += 1
        fresh = self.page(group_by="type")
        assert fresh["count"] == 3
        assert [(g["label"], g["count"]) for g in fresh["groups"]] == [
            ("http", 2),
            ("tcp", 1),
        ]
        assert self.page(group_by="type")["groups"] == fresh["groups"]

    def test_revision_snapshot_reads_small_metadata_in_large_chunks(self):
        with sqlite.SQLiteFlowStore(self.path) as store:
            store.db.executemany(
                "INSERT INTO flows (flow_id,timestamp,flow_type,payload_version,codec,raw_length,payload) VALUES (?,2900,'tcp',1,'zstd',1,x'00')",
                ((f"small-{i}",) for i in range(5000)),
            )
        original = self.master.history._source_batch
        chunks = []

        def record(*args, **kwargs):
            result = original(*args, **kwargs)
            if len(args) > 4 and "ORDER BY sequence" in args[4]:
                chunks.append(len(result[1]))
            return result

        with mock.patch.object(self.master.history, "_source_batch", record):
            page = self.page(group_by="type")
        assert page["count"] == 5004
        assert sum(chunks) == 5000 + 3  # a is outside the frozen window.
        assert len(chunks) <= 8

    def test_revision_snapshot_source_chunks_obey_byte_budget(self):
        self.master.history.SOURCE_BYTES = 1024
        with sqlite.SQLiteFlowStore(self.path) as store:
            store.db.executemany(
                "INSERT INTO flows (flow_id,timestamp,flow_type,host,payload_version,codec,raw_length,payload) VALUES (?,2900,'http',?,1,'zstd',1,x'00')",
                ((f"large-{i}", "h" * 600) for i in range(12)),
            )
        original = self.master.history._source_batch
        chunks = []

        def record(*args, **kwargs):
            result = original(*args, **kwargs)
            if len(args) > 4 and "ORDER BY sequence" in args[4] and result[1]:
                rows = result[1]
                chunks.append(rows)
                assert sum(len(row[5] or "") for row in rows) <= 1024
                with closing(sqlite3.connect(self.path)) as db:
                    assert (
                        db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0] == 0
                    )
            return result

        with mock.patch.object(self.master.history, "_source_batch", record):
            page = self.page(group_by="host")
        assert page["count"] == 16
        assert sum(len(rows) for rows in chunks) == 15
        assert len(chunks) >= 12

    def test_revision_snapshot_finishes_despite_continuous_live_generation_changes(
        self,
    ):
        self.master.history.SOURCE_BATCH = 1
        original = self.master.history._source_batch
        chunks = 0

        def change(*args, **kwargs):
            nonlocal chunks
            result = original(*args, **kwargs)
            if len(args) > 4 and "ORDER BY sequence" in args[4]:
                chunks += 1
                self.master.view.generation += 1
            return result

        with mock.patch.object(self.master.history, "_source_batch", change):
            response = self.fetch(
                self.url(group_by="status", mode="absolute"),
                headers={"Cookie": self.cookie},
            )
        assert response.code == 200, response.body
        assert json.loads(response.body)["count"] == 5
        assert chunks == 5
        assert len(self.master.history._snapshots) == 1

    def test_revision_snapshot_parameter_validation(self):
        for query in (
            {"snapshot": "s1:" + "0" * 32},
            {"group_by": "host", "snapshot": "invalid"},
            {"group_by": "host", "lookup_flow": "c"},
            {"group_by": "host", "group_key": "g1:" + "0" * 64},
            {
                "group_by": "host",
                "snapshot": "s1:" + "0" * 32,
                "lookup_flow": "c",
                "lookup_group": "g1:" + "0" * 64,
            },
        ):
            assert (
                self.fetch(self.url(**query), headers={"Cookie": self.cookie}).code
                == 400
            )
        assert (
            self.fetch(
                self.url(group_by="host", snapshot="s1:" + "0" * 32),
                headers={"Cookie": self.cookie},
            ).code
            == 409
        )

    def test_group_pages_and_members(self):
        groups = self.page(group_by="status")
        assert groups["count"] == 4
        assert groups["group_count"] == 2
        assert [
            {k: v for k, v in group.items() if k != "key"} for group in groups["groups"]
        ] == [
            {"label": "200", "count": 3, "ordinal": 0, "members_before": 0},
            {"label": "418", "count": 1, "ordinal": 1, "members_before": 3},
        ]
        members = self.page(
            group_by="status",
            snapshot=groups["snapshot"],
            group_key=groups["groups"][1]["key"],
            offset=0,
        )
        assert [r["id"] for r in members["rows"]] == ["c"]
        response = self.fetch(
            self.url(group_by="invalid"), headers={"Cookie": self.cookie}
        )
        assert response.code == 400

    def test_group_search_and_validation(self):
        groups = self.page(group_by="status", search="~c 418")
        assert groups["count"] == 1
        assert groups["groups"][0]["label"] == "418"
        assert groups["historical_search_unsupported"] is True
        for query in (
            {"group_key": '{"value":null}'},
            {"group_by": "status", "group_key": '{"value":true}'},
            {"group_by": "status", "group_key": '{"value":"200"}'},
            {"group_by": "host", "after": '[1,"1","a"]'},
        ):
            response = self.fetch(self.url(**query), headers={"Cookie": self.cookie})
            assert response.code == 400
        assert self.fetch(self.url(group_by="host")).code in (302, 403)

    def test_group_full_live_and_durable_values(self):
        prefix = "a" * 1024
        self.live.request.host = prefix + "x"
        self.master.view.update([self.live])
        with sqlite.SQLiteFlowStore(self.path) as store:
            store.db.execute(
                "UPDATE flows SET host=? WHERE flow_id='b'", (prefix + "y",)
            )
        groups = self.page(group_by="host")
        keys = []
        for expected in ("c", "b"):
            located = self.page(
                group_by="host", snapshot=groups["snapshot"], lookup_flow=expected
            )
            key = located["group"]["key"]
            keys.append(key)
            page = self.page(
                group_by="host",
                snapshot=groups["snapshot"],
                group_key=key,
                offset=0,
            )
            assert located["member_ordinal"] == 0
            assert [row["id"] for row in page["rows"]] == [expected]
            assert page["rows"][0]["host"] == prefix
        assert keys[0] != keys[1]

    def test_group_key_length_boundary(self):
        groups = self.page(group_by="host")
        key = "g1:" + "0" * 64
        result = self.page(group_by="host", snapshot=groups["snapshot"], group_key=key)
        assert result["count"] == 0
        response = self.fetch(
            self.url(group_by="host", snapshot=groups["snapshot"], group_key=key + "a"),
            headers={"Cookie": self.cookie},
        )
        assert response.code == 400

    def test_ungrouped_live_method_with_surrogate_after_summary_limit(self):
        self.live.request.method = b"G" * 64 + b"\xff"
        result = self.page()
        row = next(row for row in result["rows"] if row["id"] == "c")
        assert row["method"] == "G" * 64

    def test_large_and_non_utf8_live_method_groups_are_expandable(self):
        self.live.request.method = b"G" * 5000
        other = self.master.view.get_by_id("e")
        other.request.method = b"G" * 64 + b"\xff"
        self.master.view.update([self.live, other])
        groups = self.page(group_by="method")
        special = [group for group in groups["groups"] if group["label"] != "GET"]
        assert len(special) == 2
        assert all(len(group["label"]) <= 256 for group in special)
        for group in special:
            members = self.page(
                group_by="method",
                snapshot=groups["snapshot"],
                group_key=group["key"],
                offset=0,
            )
            assert members["count"] == 1
            assert members["rows"][0]["id"] in ("c", "e")

    def test_protocol_sizes_survive_live_and_durable_merge(self):
        flows = [
            tflow.ttcpflow(),
            tflow.tudpflow(),
            tflow.twebsocketflow(),
            tflow.tdnsflow(resp=True),
        ]
        flows[2].request.raw_content = b"rq"
        flows[2].response.raw_content = b"res"
        with sqlite.SQLiteFlowStore(self.path) as store:
            for i, f in enumerate(flows):
                f.id = str(100 + i)
                f.timestamp_created = 2600
                store.write(f.get_state())
        self.master.view.add(flows)

        def sizes():
            rows = {row["id"]: row for row in self.page()["rows"]}
            assert len(rows) == 8
            return [
                (
                    rows[f.id]["request_size"],
                    rows[f.id]["response_size"],
                    rows[f.id]["live"],
                )
                for f in flows
            ]

        assert sizes() == [(5, 7, True), (5, 7, True), (24, 10, True), (28, 80, True)]
        flows[0].messages.append(tcp.TCPMessage(True, b"abc"))
        flows[1].messages.append(udp.UDPMessage(False, b"more"))
        client = flows[2].websocket.messages[0].copy()
        client.content = b"x"
        server = flows[2].websocket.messages[-1].copy()
        server.content = b"yz"
        flows[2].websocket.messages.extend((client, server))
        flows[3].response.answers.append(flows[3].response.answers[0])
        self.master.view.update(flows)
        assert sizes() == [(8, 7, True), (5, 11, True), (25, 12, True), (28, 106, True)]
        flows[2].request.raw_content = None
        self.master.view.update([flows[2]])
        assert sizes()[2] == (None, 12, True)
        flows[2].response.raw_content = None
        self.master.view.update([flows[2]])
        assert sizes()[2] == (None, None, True)
        self.master.view.remove(flows)
        assert sizes() == [
            (5, 7, False),
            (5, 7, False),
            (24, 10, False),
            (28, 80, False),
        ]

    def test_size_cache_edits_revert_hidden_updates_and_removal(self):
        f = tflow.ttcpflow()
        f.timestamp_created = 2500
        self.master.view.add([f])

        def sizes():
            row = next(row for row in self.page()["rows"] if row["id"] == f.id)
            return row["request_size"], row["response_size"]

        assert sizes() == (5, 7)
        f.backup()
        f.messages[0].content = b"modified"
        self.master.view.update([f])
        assert sizes() == (8, 7)
        f.revert()
        self.master.view.update([f])
        assert sizes() == (5, 7)
        self.master.view.set_filter(flowfilter.parse("~http"))
        f.messages[1].content = b"new"
        self.master.addons.trigger(hooks.UpdateHook([f]))
        assert sizes() == (5, 3)
        f.messages[0].content = b"edited"
        f.messages.append(tcp.TCPMessage(False, b"more"))
        self.master.addons.trigger(hooks.UpdateHook([f]))
        assert sizes() == (6, 7)
        f.messages.pop()
        f.messages[0].content = b"hello"
        self.master.addons.trigger(hooks.UpdateHook([f]))
        assert sizes() == (5, 3)
        self.master.view.remove([f])
        f.messages[0].content = b"readded"
        self.master.view.add([f])
        assert sizes() == (7, 3)
        self.master.view.clear()
        f.messages[0].content = b"clear"
        self.master.view.add([f])
        assert sizes() == (5, 3)

    def test_dns_sizes_are_cached_without_packing_payloads(self):
        f = tflow.tdnsflow()
        f.timestamp_created = 2500
        self.master.view.add([f])

        def sizes():
            row = next(row for row in self.page()["rows"] if row["id"] == f.id)
            return row["request_size"], row["response_size"]

        with mock.patch.object(
            dns.DNSMessage,
            "packed",
            new_callable=mock.PropertyMock,
            side_effect=AssertionError("packet copy"),
        ):
            assert sizes() == (28, None)
            with mock.patch.object(
                history.domain_names,
                "pack",
                side_effect=AssertionError("repeat DNS scan"),
            ):
                assert sizes() == (28, None)
            request = f.request
            f.request = None
            assert sizes() == (None, None)
            f.request = request
            f.response = tflow.tdnsflow(resp=True).response
            assert sizes() == (28, 80)
            f.response.answers[0].data = b"changed"
            self.master.view.update([f])
            assert sizes() == (28, 83)

    def test_inclusive_open_and_rolling_bounds(self):
        assert [r["id"] for r in self.page(mode="absolute")["rows"]] == list("abced")
        assert [
            r["id"] for r in self.page(mode="absolute", until=1500)["rows"]
        ] == list("ab")
        assert [
            r["id"] for r in self.page(mode="absolute", **{"from": 2800})["rows"]
        ] == list("ed")
        assert [
            r["id"]
            for r in self.page(mode="absolute", **{"from": 2500, "until": 2800})["rows"]
        ] == list("ce")
        assert self.page(mode="absolute", **{"from": 1500, "until": 1500})["count"] == 1
        assert self.page(duration=500)["count"] == 3
        with mock.patch("time.time", return_value=3301):
            response = self.fetch(self.url(), headers={"Cookie": self.cookie})
        result = json.loads(response.body)
        assert (result["lower"], result["upper"]) == (1501, 3301)
        assert [r["id"] for r in result["rows"]] == list("ced")

    def test_paging_count_and_cursor_precision(self):
        first = self.page(offset=0, limit=2, anchor="e", selected="a")
        assert (
            first["count"],
            first["anchor_position"],
            first["selected_eligible"],
        ) == (4, 2, False)
        assert [r["id"] for r in first["rows"]] == list("bc")
        newest = self.page(limit=2, selected="b")
        assert (newest["offset"], newest["selected_eligible"]) == (2, True)
        assert [r["id"] for r in newest["rows"]] == list("ed")
        after = self.page(after=json.dumps(first["rows"][-1]["cursor"]), limit=1)
        assert (after["offset"], [r["id"] for r in after["rows"]]) == (2, ["e"])
        last = self.page(after=json.dumps(after["rows"][-1]["cursor"]))
        assert [r["id"] for r in last["rows"]] == ["d"]
        before = self.page(before=json.dumps(newest["rows"][0]["cursor"]), limit=1)
        assert (before["offset"], [r["id"] for r in before["rows"]]) == (1, ["c"])
        assert self.page(offset=999)["rows"] == []
        assert self.page("/flow-history/count", anchor="e", selected="c") == {
            "count": 4,
            "offset": 0,
            "rows": [],
            "anchor_position": 2,
            "selected_eligible": True,
            "instance_id": self.master.instance_id,
            "lower": 1200,
            "upper": 3000,
            "historical_search_unsupported": False,
            "historical_highlight_unsupported": False,
        }

    def test_live_exact_filters_and_hidden_view(self):
        self.master.view.set_filter(flowfilter.parse("~m POST"))
        expr = "~m GET & ~dtse < 1970-01-01T00:45:00Z"
        result = self.page(search=expr, highlight="~c 418")
        assert [r["id"] for r in result["rows"]] == ["c"]
        assert result["rows"][0]["highlighted"] is True
        assert result["historical_search_unsupported"] is True
        assert result["historical_highlight_unsupported"] is True
        assert (
            self.page(search=expr, **{"mode": "absolute", "from": 2600})["count"] == 0
        )
        assert self.page(search="~m POST", selected="c")["selected_eligible"] is False
        assert self.page(search="~m POST")["count"] == 0
        highlighted = self.page(highlight="~c 418")
        assert [r["highlighted"] for r in highlighted["rows"]] == [
            False,
            True,
            False,
            False,
        ]
        assert (
            self.page(search=" ", highlight=" ")["historical_search_unsupported"]
            is False
        )
        # The changed live timestamp must suppress c's older durable row.
        assert [
            r["id"] for r in self.page(mode="absolute", until=2100)["rows"]
        ] == list("ab")

    def test_live_body_filters_and_metadata_run_off_loop(self):
        loop_thread = threading.get_ident()
        get_content = type(self.live.request).get_content
        snapshot = history.snapshot
        reads = []

        def read_body(request, *args, **kwargs):
            assert threading.get_ident() != loop_thread
            reads.append(request)
            return get_content(request, *args, **kwargs)

        def summarize(*args, **kwargs):
            assert threading.get_ident() != loop_thread
            return snapshot(*args, **kwargs)

        with (
            mock.patch.object(type(self.live.request), "get_content", read_body),
            mock.patch.object(history, "snapshot", summarize),
        ):
            result = self.page(
                mode="absolute",
                **{"from": 2600, "until": 2800},
                search="~bq request",
                highlight="~bq request",
            )
        assert [row["id"] for row in result["rows"]] == ["e"]
        assert result["rows"][0]["highlighted"] is True
        assert reads == [self.master.view.get_by_id("e").request] * 2

    @tornado.testing.gen_test
    async def test_live_filter_workers_are_bounded_and_cancellable(self):
        loop_thread = threading.get_ident()
        entered = threading.Barrier(3)
        release = threading.Event()
        get_content = type(self.live.request).get_content
        reads = []
        cancelled = threading.Event()
        cancellations = []
        original_close = app.FlowHistory.on_connection_close

        def read_body(request, *args, **kwargs):
            assert threading.get_ident() != loop_thread
            reads.append(request)
            entered.wait(3)
            assert release.wait(5)
            return get_content(request, *args, **kwargs)

        def on_close(handler):
            original_close(handler)
            cancellations.append(handler.cancel)
            if len(cancellations) == 2:
                cancelled.set()

        streams = []
        with (
            mock.patch.object(type(self.live.request), "get_content", read_body),
            mock.patch.object(app.FlowHistory, "on_connection_close", on_close),
        ):
            try:
                for _ in range(2):
                    stream = await TCPClient().connect(
                        "127.0.0.1", self.get_http_port()
                    )
                    streams.append(stream)
                    await stream.write(
                        f"GET {self.url(mode='absolute', search='~bq request', highlight='~bq request')} HTTP/1.1\r\nHost: localhost\r\nCookie: {self.cookie}\r\n\r\n".encode()
                    )
                await asyncio.wait_for(asyncio.to_thread(entered.wait, 3), 4)
                state = await self.http_client.fetch(
                    self.get_url("/state"), headers={"Cookie": self.cookie}
                )
                assert state.code == 200
                busy = await self.http_client.fetch(
                    self.get_url(self.url(mode="absolute", search="~bq request")),
                    headers={"Cookie": self.cookie},
                    raise_error=False,
                )
                assert busy.code == 503
                for stream in streams:
                    stream.close()
                assert await asyncio.wait_for(asyncio.to_thread(cancelled.wait, 2), 3)
                assert all(cancel.is_set() for cancel in cancellations)
            finally:
                for stream in streams:
                    stream.close()
                release.set()
            # A cancellation after Search must skip Highlight and subsequent flows.
            await asyncio.wait_for(
                asyncio.gather(
                    *(self.master.history._workers.acquire() for _ in range(2))
                ),
                2,
            )
            for _ in range(2):
                self.master.history._workers.release()
            assert len(reads) == 2

    def test_live_summary_pins_body_reference(self):
        with mock.patch.object(
            type(self.live.request),
            "raw_content",
            new_callable=mock.PropertyMock,
            side_effect=[b"request", None, None, None],
        ):
            result = self.page(mode="absolute", **{"from": 2500, "until": 2500})
        assert result["rows"][0]["request_size"] == 7

    def test_live_filter_mutation_is_retryable(self):
        class ChangingMetadata(dict):
            def items(self):
                for pair in super().items():
                    self["concurrent"] = "update"
                    yield pair

        self.live.metadata = ChangingMetadata(original="value")
        response = self.fetch(
            self.url(mode="absolute", search="~meta value"),
            headers={"Cookie": self.cookie},
        )
        assert response.code == 503
        self.live.metadata = {"original": "value"}
        assert self.page(mode="absolute", search="~meta value")["count"] == 1

    def test_validation_and_instance_fence(self):
        bad_queries = [
            {"mode": "other"},
            {"duration": "nan"},
            {"duration": "inf"},
            {"duration": 0},
            {"duration": -1},
            {"duration": 8640000000001},
            {"mode": "absolute", "from": "nan"},
            {"mode": "absolute", "until": "inf"},
            {"mode": "absolute", "from": 8640000000001},
            {"mode": "absolute", "from": "1" * 65},
            {"mode": "absolute", "from": 10, "until": 9},
            {"offset": -1},
            {"offset": "1.1"},
            {"offset": 9007199254740992},
            {"limit": 0},
            {"limit": 501},
            {"limit": "abc"},
            {"search": "x" * 4097},
            {"highlight": "x" * 4097},
            {"search": " " * 4097},
            {"highlight": " " * 4097},
            {"search": "~invalid"},
            {"highlight": "~invalid"},
            {"anchor": "x" * 129},
            {"selected": "x" * 129},
            {"anchor": ""},
            {"selected": ""},
            {"after": "no"},
            {"after": "{}"},
            {"after": "[]"},
            {"after": " " * 1025},
            {"after": '[true,"1","c"]'},
            {"after": '[1,1,"c"]'},
            {"after": '[1,"-1","c"]'},
            {"after": '[1,"9223372036854775808","c"]'},
            {"after": '[1,"1",""]'},
            {"after": '[1,"1","\\ud800"]'},
            {"before": '[1,"1","\\udfff"]'},
            {"after": '[1,"1","' + "x" * 129 + '"]'},
            {"after": '[NaN,"1","c"]'},
            {"after": '[8640000000001,"1","c"]'},
            {"after": '[1,"1","c"]', "before": '[2,"1","c"]'},
            {"before": '[1,"1","c"]', "offset": 0},
        ]
        for query in bad_queries:
            with self.subTest(query=query):
                response = self.fetch(
                    self.url(**query), headers={"Cookie": self.cookie}
                )
                assert response.code == 400, response.body
        for path in ("/flow-history", "/flow-history/count"):
            assert self.fetch(path, headers={"Cookie": self.cookie}).code == 409
            assert (
                self.fetch(
                    self.url(path, instance_id="old"), headers={"Cookie": self.cookie}
                ).code
                == 409
            )
            assert self.fetch(self.url(path)).code == 403

    def test_no_payload_read_or_full_live_serialization(self):
        connect = sqlite3.connect
        sha256 = hashlib.sha256

        def no_body_hash(data=b"", *args, **kwargs):
            assert data not in (b"request", b"response")
            return sha256(data, *args, **kwargs)

        def guarded_connect(*args, **kwargs):
            db = connect(*args, **kwargs)

            def authorize(action, table, column, *_):
                if action == sqlite3.SQLITE_READ and (
                    (
                        table == "flows"
                        and column
                        in {"payload", "codec", "dictionary_id", "raw_length"}
                    )
                    or table == "flow_payload_chunks"
                ):
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            db.set_authorizer(authorize)
            return db

        with (
            mock.patch.object(sqlite3, "connect", guarded_connect),
            mock.patch.object(
                sqlite.SQLiteFlowReader,
                "get_flow",
                side_effect=AssertionError("payload"),
            ),
            mock.patch.object(
                flow.Flow, "from_state", side_effect=AssertionError("flow")
            ),
            mock.patch.object(
                app, "flow_to_json", side_effect=AssertionError("full serialization")
            ),
            mock.patch.object(hashlib, "sha256", no_body_hash),
        ):
            assert self.page()["count"] == 4
            assert self.page("/flow-history/count")["count"] == 4
            groups = self.page(group_by="status")
            assert (
                self.page(
                    group_by="status",
                    snapshot=groups["snapshot"],
                    group_key=groups["groups"][0]["key"],
                )["count"]
                == 3
            )
            for suffix, method in (
                ("", "GET"),
                ("", "DELETE"),
                ("/resume", "POST"),
                ("/response/content.data", "GET"),
            ):
                response = self.fetch(
                    "/flows/d" + suffix,
                    method=method,
                    body="" if method == "POST" else None,
                    headers={"Cookie": self.cookie, "X-XSRFToken": "unused"},
                )
                assert response.code == 404

    def test_storage_error_and_missing_file_do_not_create_database(self):
        self.master.options.update(
            save_sqlite_file=str(self.path.with_name("missing.db"))
        )
        response = self.fetch(self.url(), headers={"Cookie": self.cookie})
        assert response.code == 503
        assert not self.path.with_name("missing.db").exists()
        self.master.options.update(save_sqlite_file=str(self.path))
        with mock.patch.object(
            sqlite.SQLiteFlowReader,
            "history_page",
            side_effect=sqlite.StorageError("failed"),
        ):
            assert self.fetch(self.url(), headers={"Cookie": self.cookie}).code == 503

    @tornado.testing.gen_test
    async def test_workers_nonblocking_bounded_and_path_change(self):
        entered = threading.Event()
        release = threading.Event()
        original = sqlite.ensure_history_index
        threads = []

        def slow_index(path, **kwargs):
            threads.append(threading.get_ident())
            entered.set()
            assert release.wait(5)
            original(path, **kwargs)

        async def request():
            return await self.http_client.fetch(
                self.get_url(self.url()),
                headers={"Cookie": self.cookie},
                raise_error=False,
            )

        with mock.patch.object(sqlite, "ensure_history_index", slow_index):
            first = asyncio.create_task(request())
            second = asyncio.create_task(request())
            try:
                assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), 3)
                state = await self.http_client.fetch(
                    self.get_url("/state"), headers={"Cookie": self.cookie}
                )
                assert state.code == 200
                third = await request()
                assert third.code == 503
                self.master.options.update(save_sqlite_file=None)
            finally:
                release.set()
            responses = await asyncio.gather(first, second)
        assert [response.code for response in responses] == [409, 409]
        assert len(threads) == 1
        assert threads[0] != threading.get_ident()
        self.master.options.update(save_sqlite_file=str(self.path))
        with mock.patch.object(
            sqlite, "ensure_history_index", side_effect=AssertionError("repeated index")
        ):
            assert (await request()).code == 200

    @tornado.testing.gen_test
    async def test_disconnect_cancels_sqlite_and_releases_reader(self):
        entered = threading.Event()
        closed = threading.Event()
        cancelled = threading.Event()
        original = sqlite.SQLiteFlowReader.history_page

        def slow_page(reader, **kwargs):
            entered.set()
            try:
                reader.db.execute(
                    "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) FROM n"
                ).fetchone()
            except sqlite3.OperationalError as error:
                assert error.sqlite_errorcode == sqlite3.SQLITE_INTERRUPT
                cancelled.set()
                raise sqlite.StorageError("cancelled") from error
            finally:
                closed.set()
            return original(reader, **kwargs)

        with mock.patch.object(sqlite.SQLiteFlowReader, "history_page", slow_page):
            stream = await TCPClient().connect("127.0.0.1", self.get_http_port())
            await stream.write(
                f"GET {self.url()} HTTP/1.1\r\nHost: localhost\r\nCookie: {self.cookie}\r\n\r\n".encode()
            )
            try:
                assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), 3)
            finally:
                stream.close()
            assert await asyncio.wait_for(asyncio.to_thread(closed.wait, 2), 3)
        assert cancelled.is_set()
        response = await self.http_client.fetch(
            self.get_url(self.url()), headers={"Cookie": self.cookie}
        )
        assert response.code == 200


@pytest.mark.parametrize(
    "limits",
    [
        {"live_rows": 0},
        {"live_bytes": -1},
        {"snapshot_bytes": 32767},
        {"snapshot_capacity": True},
        {"snapshot_ttl": float("inf")},
        {"source_batch": 0},
        {"source_bytes": 0},
    ],
)
def test_grouped_resource_limits_are_validated(limits):
    with pytest.raises(ValueError):
        history.HistoryReader(**limits)


@pytest.mark.parametrize(
    "field,value,message",
    [("id", "x" * 129, "flow ID"), ("host", "x" * (1048576 + 1), "Group value")],
)
def test_live_metadata_rejects_unbounded_identity(field, value, message):
    f = make_flow("a", 2000)
    if field == "id":
        f.id = value
    else:
        f.request.host = value
    query = history.HistoryQuery.parse({"group_by": "host"}, now=3000)
    with pytest.raises(history.SnapshotError, match=message):
        history.snapshot(
            [f],
            query,
            threading.Event(),
            history.SizeCache(),
            max_bytes=8 * 1024 * 1024,
        )


@pytest.mark.parametrize(
    "failure", ["cancel", "replace", "missing", "sql", "byte_limit"]
)
async def test_source_batch_releases_reader_on_failure_and_chunk_boundary(
    tmp_path, failure
):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(make_flow("a", 2000).get_state())
    service = history.HistoryReader()
    cancel = threading.Event()
    identity = service._path_identity(path)
    try:
        if failure == "cancel":
            cancel.set()
            with pytest.raises(sqlite.StorageError, match="cancelled"):
                service._source_batch(str(path), None, identity, cancel)
        elif failure == "replace":
            with mock.patch.object(
                service, "_path_identity", side_effect=[identity, ("replacement", 0, 0)]
            ):
                with pytest.raises(history.HistoryStale, match="path changed"):
                    service._source_batch(str(path), None, identity, cancel)
        elif failure == "missing":
            path.unlink()
            with pytest.raises(history.HistoryStale, match="path changed") as caught:
                service._source_batch(str(path), None, identity, cancel)
            assert isinstance(caught.value.__cause__, FileNotFoundError)
        elif failure == "sql":
            with pytest.raises(
                sqlite.StorageError, match="Cannot read grouped"
            ) as caught:
                service._source_batch(
                    str(path),
                    None,
                    identity,
                    cancel,
                    "SELECT missing_column FROM flows",
                )
            assert isinstance(caught.value.__cause__, sqlite3.OperationalError)
        else:
            _, rows = service._source_batch(
                str(path),
                None,
                identity,
                cancel,
                "SELECT flow_id FROM flows UNION ALL SELECT flow_id FROM flows",
                byte_limit=1,
            )
            assert rows == [("a",)]
        if path.exists():
            with closing(sqlite3.connect(path)) as db:
                assert db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0] == 0
    finally:
        await service.done()


@pytest.mark.parametrize("grouped", [False, True])
async def test_history_submission_failure_releases_admission(tmp_path, grouped):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    service = history.HistoryReader()
    query = history.HistoryQuery.parse(
        {"group_by": "host"} if grouped else {}, now=3000
    )

    async def read():
        if grouped:
            return await service.grouped_page(
                str(path),
                query,
                [],
                threading.Event(),
                instance_id="test",
                live_generation=lambda: 0,
            )
        return await service.page(str(path), query, [], threading.Event())

    try:
        with mock.patch.object(
            asyncio.get_running_loop(),
            "run_in_executor",
            side_effect=RuntimeError("executor unavailable"),
        ):
            with pytest.raises(RuntimeError, match="executor unavailable"):
                await read()
        assert service._workers._value == 2
        assert not service._group_admission.locked()
        assert (await read())["count"] == 0
    finally:
        await service.done()


@pytest.mark.parametrize("grouped", [False, True])
async def test_index_replacement_during_initialization_is_rejected(tmp_path, grouped):
    path, replacement = tmp_path / "history.db", tmp_path / "replacement.db"
    for p in (path, replacement):
        with sqlite.SQLiteFlowStore(p):
            pass
    service = history.HistoryReader()
    original = sqlite.ensure_history_index

    def replace(path, **kwargs):
        original(path, **kwargs)
        replacement.replace(path)

    query = history.HistoryQuery.parse(
        {"group_by": "host"} if grouped else {}, now=3000
    )
    try:
        with mock.patch.object(sqlite, "ensure_history_index", replace):
            with pytest.raises(
                history.HistoryStale if grouped else sqlite.StorageError,
                match="path changed",
            ):
                if grouped:
                    await service.grouped_page(
                        str(path),
                        query,
                        [],
                        threading.Event(),
                        instance_id="test",
                        live_generation=lambda: 0,
                    )
                else:
                    await service.page(str(path), query, [], threading.Event())
        assert not service._index_lock.locked()
        assert not service._indexed_paths
    finally:
        await service.done()


@pytest.mark.parametrize(
    "field,value,message",
    [("flow_id", "x" * 129, "flow ID"), ("host", "x" * (1048576 + 1), "Group value")],
    ids=["invalid-id", "oversized-value"],
)
async def test_grouped_durable_metadata_limits_remove_failed_scratch(
    tmp_path, field, value, message
):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(make_flow("a", 2000).get_state())
        store.db.execute(f"UPDATE flows SET {field}=?", (value,))
    service = history.HistoryReader()
    build = history.GroupStore.build
    try:
        with mock.patch.object(
            history.GroupStore,
            "build",
            side_effect=lambda *args, **kwargs: build(
                *args, directory=tmp_path, **kwargs
            ),
        ):

            async def page():
                return await service.grouped_page(
                    str(path),
                    history.HistoryQuery.parse({"group_by": "host"}, now=3000),
                    [],
                    threading.Event(),
                    instance_id="test",
                    live_generation=lambda: 0,
                )

            if field == "flow_id":
                with pytest.raises(history.SnapshotError, match=message):
                    await page()
                assert not list(tmp_path.glob("group-snapshot-*"))
                assert not service._snapshots
            else:
                result = await page()
                assert result["count"] == 1
                assert result["groups"][0]["label"] == "value too large to group"
    finally:
        await service.done()


@pytest.mark.parametrize(
    "stage", ["before_result", "after_result", "cancel_after_result"]
)
async def test_grouped_publication_ignores_traffic_but_honors_cancellation(
    tmp_path, stage
):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(make_flow("a", 2000).get_state())
    service = history.HistoryReader()
    original, build = service._grouped_result, history.GroupStore.build
    cancel = threading.Event()
    generation = 0

    def batch(*args, **kwargs):
        nonlocal generation
        if stage == "before_result":
            generation += 1
        result = original(*args, **kwargs)
        if stage == "cancel_after_result":
            cancel.set()
        elif stage == "after_result":
            generation += 1
        return result

    try:
        with (
            mock.patch.object(service, "_grouped_result", batch),
            mock.patch.object(
                history.GroupStore,
                "build",
                side_effect=lambda *args, **kwargs: build(
                    *args, directory=tmp_path, **kwargs
                ),
            ),
        ):

            async def page():
                return await service.grouped_page(
                    str(path),
                    history.HistoryQuery.parse({"group_by": "host"}, now=3000),
                    [],
                    cancel,
                    instance_id="test",
                    live_generation=lambda: generation,
                )

            if stage == "cancel_after_result":
                with pytest.raises(sqlite.StorageError, match="cancelled"):
                    await page()
                assert not service._snapshots
            else:
                result = await page()
                assert generation == 1
                assert result["count"] == 1
                assert result["snapshot"] in service._snapshots
    finally:
        await service.done()


@pytest.mark.parametrize("change", ["cancel", "generation"])
async def test_live_only_group_build_honors_cancellation_but_not_generation(
    tmp_path, change
):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    service = history.HistoryReader()
    cancel = threading.Event()
    generation = 0
    build = history.GroupStore.build

    def changed(source, **kwargs):
        def begin(event):
            nonlocal generation
            if change == "cancel":
                cancel.set()
            else:
                generation += 1
            return source(event)

        return build(begin, directory=tmp_path, **kwargs)

    try:
        with mock.patch.object(history.GroupStore, "build", changed):

            async def page():
                return await service.grouped_page(
                    str(path),
                    history.HistoryQuery.parse(
                        {"group_by": "host", "search": "~all"}, now=3000
                    ),
                    [make_flow("a", 2000)],
                    cancel,
                    instance_id="test",
                    live_generation=lambda: generation,
                )

            if change == "cancel":
                with pytest.raises(sqlite.StorageError, match="cancelled"):
                    await page()
                assert not service._snapshots
                assert not list(tmp_path.glob("group-snapshot-*"))
            else:
                result = await page()
                assert generation == 1
                assert result["count"] == 1
    finally:
        await service.done()


async def test_grouped_missing_source_is_storage_error(tmp_path):
    service = history.HistoryReader()
    try:
        with pytest.raises(sqlite.StorageError, match="Cannot build grouped") as caught:
            await service.grouped_page(
                str(tmp_path / "missing.db"),
                history.HistoryQuery.parse({"group_by": "host"}, now=3000),
                [],
                threading.Event(),
                instance_id="test",
                live_generation=lambda: 0,
            )
        assert isinstance(caught.value.__cause__, FileNotFoundError)
        assert not service._snapshots
    finally:
        await service.done()


@pytest.mark.parametrize(
    "stage", ["generation", "cancel", "closed_grouped", "closed_regular"]
)
async def test_worker_return_boundary_never_publishes_unusable_result(tmp_path, stage):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(make_flow("a", 2000).get_state())
    service = history.HistoryReader()
    loop = asyncio.get_running_loop()
    generation = 0
    grouped = stage != "closed_regular"
    query = history.HistoryQuery.parse(
        {"group_by": "host"} if grouped else {}, now=3000
    )
    original = service._read_grouped if grouped else service._read
    task = None
    invalidated = threading.Event()

    def invalidate():
        nonlocal generation
        if stage == "generation":
            generation += 1
        elif stage == "cancel":
            task.cancel()
        else:
            service._closed = True
        invalidated.set()

    def returned(*args):
        result = original(*args)
        # Finish event-loop invalidation before the worker can publish its result.
        loop.call_soon_threadsafe(invalidate)
        assert invalidated.wait(2)
        return result

    async def read():
        if grouped:
            return await service.grouped_page(
                str(path),
                query,
                [],
                threading.Event(),
                instance_id="test",
                live_generation=lambda: generation,
            )
        return await service.page(str(path), query, [], threading.Event())

    try:
        with mock.patch.object(
            service, "_read_grouped" if grouped else "_read", returned
        ):
            task = asyncio.create_task(read())
            expected = (
                history.HistoryStale
                if stage == "generation"
                else asyncio.CancelledError
                if stage == "cancel"
                else sqlite.StorageError
            )
            if stage == "generation":
                result = await task
                assert result["count"] == 1
                assert result["snapshot"] in service._snapshots
            else:
                with pytest.raises(expected):
                    await task
        # A cancelled HTTP waiter can finish before its worker's done callbacks.
        await asyncio.gather(*service._requests, return_exceptions=True)
        await asyncio.sleep(0)
        await loop.run_in_executor(service._group_worker, lambda: None)
        if stage == "cancel":
            assert not service._snapshots
    finally:
        await service.done()
    assert not service._snapshots
    assert all(not thread.is_alive() for thread in service._group_worker._threads)


async def test_expiry_timer_removes_idle_snapshot_without_another_request(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    service = history.HistoryReader(snapshot_ttl=1)
    closed = threading.Event()
    close = history.GroupSnapshot.close

    def observed_close(scratch):
        close(scratch)
        closed.set()

    try:
        with mock.patch.object(history.GroupSnapshot, "close", observed_close):
            first = await service.grouped_page(
                str(path),
                history.HistoryQuery.parse({"group_by": "host"}, now=3000),
                [],
                threading.Event(),
                instance_id="test",
                live_generation=lambda: 0,
            )
            assert first["snapshot"] in service._snapshots
            assert await asyncio.wait_for(asyncio.to_thread(closed.wait, 3), 4)
            await asyncio.get_running_loop().run_in_executor(
                service._group_worker, lambda: None
            )
            assert not service._snapshots
            assert service._expiry_timer is None
            assert not service._expiry_pending
    finally:
        await service.done()
    # A timer callback already submitted during shutdown must be harmless.
    service._expire_snapshots(timer_fired=True)
    assert service._expiry_timer is None


async def test_shutdown_finishes_executor_cleanup_if_cleanup_future_is_cancelled():
    service = history.HistoryReader()
    loop = asyncio.get_running_loop()
    submit = loop.run_in_executor
    cleaned = threading.Event()
    entered, release = threading.Event(), threading.Event()

    def interrupted(executor, fn, *args):
        if executor is service._group_worker:

            def cleanup():
                try:
                    entered.set()
                    assert release.wait(2)
                    return fn(*args)
                finally:
                    cleaned.set()

            future = submit(executor, cleanup)
            # Cancellation of an asyncio wrapper cannot stop its running thread.
            assert entered.wait(2)
            future.cancel()
            release.set()
            return future
        return submit(executor, fn, *args)

    with mock.patch.object(loop, "run_in_executor", interrupted):
        with pytest.raises(asyncio.CancelledError):
            await service.done()
    assert cleaned.is_set()
    assert all(not thread.is_alive() for thread in service._group_worker._threads)


async def test_grouped_members_decode_non_utf8_durable_method(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(make_flow("a", 2000).get_state())
        store.db.execute("UPDATE flows SET method=?", (b"GET\xff",))
    service = history.HistoryReader()
    query = {"group_by": "method"}

    async def read(params):
        return await service.grouped_page(
            str(path),
            history.HistoryQuery.parse(params, now=3000),
            [],
            threading.Event(),
            instance_id="test",
            live_generation=lambda: 0,
        )

    try:
        first = await read(query)
        page = await read(
            query
            | {"snapshot": first["snapshot"], "group_key": first["groups"][0]["key"]}
        )
        assert len(page["rows"]) == 1
        assert page["rows"][0]["id"] == "a"
        assert page["rows"][0]["method"] == "GET\ufffd"
        assert page["rows"][0]["live"] is False
    finally:
        await service.done()


async def test_grouped_large_live_view_and_configurable_metadata_limits(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    service = history.HistoryReader()
    query = history.HistoryQuery.parse({"group_by": "method"}, now=3000)
    flows = [make_flow(f"live-{i}", 2500) for i in range(10001)]

    async def read():
        return await service.grouped_page(
            str(path),
            query,
            flows,
            threading.Event(),
            instance_id="test",
            live_generation=lambda: 0,
        )

    try:
        page = await read()
        assert page["count"] == 10001
        assert [(g["label"], g["count"]) for g in page["groups"]] == [("GET", 10001)]
        service.LIVE_ROWS = 10000
        with pytest.raises(history.SnapshotError, match="Too many live"):
            await read()
        service.LIVE_ROWS = 20000
        service.LIVE_BYTES = 1024
        with pytest.raises(history.SnapshotError, match="Live metadata"):
            await read()
    finally:
        await service.done()


@pytest.mark.parametrize(
    "operation", ["groups", "members", "lookup_group", "lookup_flow"]
)
async def test_grouped_continuation_cancels_sqlite_without_poisoning_snapshot(
    tmp_path, operation
):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(make_flow("a", 2000).get_state())
    service = history.HistoryReader()
    initial_cancel, cancel = threading.Event(), threading.Event()
    initial = history.HistoryQuery.parse({"group_by": "host"}, now=3000)

    async def read(query, event):
        return await service.grouped_page(
            str(path),
            query,
            [],
            event,
            instance_id="test",
            live_generation=lambda: 0,
        )

    try:
        first = await read(initial, initial_cancel)
        frozen = {
            "group_by": "host",
            "mode": "absolute",
            "from": str(first["lower"]),
            "until": str(first["upper"]),
            "snapshot": first["snapshot"],
        }
        key = first["groups"][0]["key"]
        extra = {
            "groups": {},
            "members": {"group_key": key},
            "lookup_group": {"lookup_group": key},
            "lookup_flow": {"lookup_flow": "a"},
        }[operation]
        continuation = history.HistoryQuery.parse(frozen | extra)
        original = history.GroupSnapshot._query

        def cancel_from_sql():
            cancel.set()
            return 0

        def query(scratch, sql, args):
            scratch._db.create_function("cancel_current_request", 0, cancel_from_sql)
            # Cancel inside SQLite after _check(), using the real progress handler.
            original(
                scratch,
                "WITH RECURSIVE n(x) AS (VALUES(cancel_current_request()) "
                "UNION ALL SELECT x+1 FROM n WHERE x<10000) SELECT sum(x) FROM n",
                (),
            )
            return original(scratch, sql, args)

        with mock.patch.object(history.GroupSnapshot, "_query", query):
            with pytest.raises(history.SnapshotError, match="cancelled") as caught:
                await read(continuation, cancel)
        assert cancel.is_set()
        assert isinstance(caught.value.__cause__, sqlite3.OperationalError)
        assert caught.value.__cause__.sqlite_errorcode == sqlite3.SQLITE_INTERRUPT
        # A completed creation request's event must not control future queries either.
        initial_cancel.set()
        recovered = await read(history.HistoryQuery.parse(frozen), threading.Event())
        assert recovered["snapshot"] == first["snapshot"]
        assert recovered["groups"] == first["groups"]
        assert recovered["count"] == 1
    finally:
        await service.done()


async def test_history_done_closes_cached_snapshots_and_worker(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(make_flow("a", 2000).get_state())
    service = history.HistoryReader()
    query = history.HistoryQuery.parse({"group_by": "host"}, now=3000)
    build = history.GroupStore.build

    with mock.patch.object(
        history.GroupStore,
        "build",
        side_effect=lambda *args, **kwargs: build(*args, directory=tmp_path, **kwargs),
    ):
        await service.grouped_page(
            str(path),
            query,
            [],
            threading.Event(),
            instance_id="test",
            live_generation=lambda: 0,
        )
    assert list(tmp_path.glob("group-snapshot-*"))
    threads = list(service._group_worker._threads)
    await service.done()
    assert not list(tmp_path.glob("group-snapshot-*"))
    assert all(not thread.is_alive() for thread in threads)
    await service.done()
    with pytest.raises(sqlite.StorageError, match="closed"):
        await service.grouped_page(
            str(path),
            query,
            [],
            threading.Event(),
            instance_id="test",
            live_generation=lambda: 0,
        )
    with pytest.raises(sqlite.StorageError, match="closed"):
        await service.page(str(path), query, [], threading.Event())


async def test_history_done_cancels_active_build_before_closing_worker(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(make_flow("a", 2000).get_state())
    service = history.HistoryReader()
    query = history.HistoryQuery.parse({"group_by": "host"}, now=3000)
    entered, cancel = threading.Event(), threading.Event()
    original, build = service._source_batch, history.GroupStore.build

    def batch(*args, **kwargs):
        result = original(*args, **kwargs)
        if len(args) > 4 and "ORDER BY sequence" in args[4]:
            entered.set()
            assert cancel.wait(3)
        return result

    with (
        mock.patch.object(service, "_source_batch", batch),
        mock.patch.object(
            history.GroupStore,
            "build",
            side_effect=lambda *args, **kwargs: build(
                *args, directory=tmp_path, **kwargs
            ),
        ),
    ):
        task = asyncio.create_task(
            service.grouped_page(
                str(path),
                query,
                [],
                cancel,
                instance_id="test",
                live_generation=lambda: 0,
            )
        )
        try:
            assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), 3)
            await asyncio.wait_for(service.done(), 3)
            assert cancel.is_set()
            with pytest.raises(sqlite.StorageError, match="cancelled"):
                await task
        finally:
            cancel.set()
            await asyncio.gather(task, return_exceptions=True)
    assert not list(tmp_path.glob("group-snapshot-*"))
    assert all(not thread.is_alive() for thread in service._group_worker._threads)


async def test_direct_shutdown_task_cancellation_still_releases_resources(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(make_flow("a", 2000).get_state())
    service = history.HistoryReader()
    grouped = history.HistoryQuery.parse({"group_by": "host"}, now=3000)
    regular = history.HistoryQuery.parse({}, now=3000)
    build = history.GroupStore.build
    with mock.patch.object(
        history.GroupStore,
        "build",
        side_effect=lambda *args, **kwargs: build(*args, directory=tmp_path, **kwargs),
    ):
        await service.grouped_page(
            str(path),
            grouped,
            [],
            threading.Event(),
            instance_id="test",
            live_generation=lambda: 0,
        )
    entered, release, closed = (threading.Event() for _ in range(3))
    cancel = threading.Event()
    close = sqlite.SQLiteFlowReader.close

    def blocked(reader, **kwargs):
        entered.set()
        assert release.wait(3)
        raise sqlite.StorageError("cancelled")

    def checked_close(reader):
        close(reader)
        closed.set()

    with (
        mock.patch.object(sqlite.SQLiteFlowReader, "history_page", blocked),
        mock.patch.object(sqlite.SQLiteFlowReader, "close", checked_close),
    ):
        request = asyncio.create_task(service.page(str(path), regular, [], cancel))
        shutdown = None
        try:
            assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), 3)
            source_future = next(iter(service._requests))
            shutdown = asyncio.create_task(service.done())
            assert await asyncio.wait_for(asyncio.to_thread(cancel.wait, 2), 3)
            # Loop teardown cancels every task, including this shielded child.
            service._shutdown_task.cancel()
            await asyncio.sleep(0)
            assert not source_future.cancelled()
            service._shutdown_task.cancel()
            await asyncio.sleep(0)
            assert not closed.is_set()
        finally:
            release.set()
            await asyncio.gather(request, return_exceptions=True)
            if shutdown is not None:
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(shutdown, 3)
    assert closed.is_set()
    assert not list(tmp_path.glob("group-snapshot-*"))
    assert service._expiry_timer is None
    assert all(not thread.is_alive() for thread in service._group_worker._threads)


async def test_history_done_waits_for_regular_source_reader_to_close(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    service = history.HistoryReader()
    query = history.HistoryQuery.parse({}, now=3000)
    entered, cancel = threading.Event(), threading.Event()
    closed = threading.Event()
    close = sqlite.SQLiteFlowReader.close

    def blocked(reader, **kwargs):
        entered.set()
        assert cancel.wait(3)
        raise sqlite.StorageError("cancelled")

    def checked_close(reader):
        close(reader)
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            reader.db.execute("SELECT 1")
        closed.set()

    with (
        mock.patch.object(sqlite.SQLiteFlowReader, "history_page", blocked),
        mock.patch.object(sqlite.SQLiteFlowReader, "close", checked_close),
    ):
        task = asyncio.create_task(service.page(str(path), query, [], cancel))
        try:
            assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), 3)
            await asyncio.wait_for(service.done(), 3)
            with pytest.raises(sqlite.StorageError, match="cancelled"):
                await task
            assert closed.is_set()
        finally:
            cancel.set()
            await asyncio.gather(task, return_exceptions=True)


async def test_grouped_admission_reserves_capacity_for_regular_history(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(make_flow("a", 2000).get_state())
    service = history.HistoryReader()
    grouped = history.HistoryQuery.parse({"group_by": "host"}, now=3000)
    regular = history.HistoryQuery.parse({}, now=3000)
    entered, release = threading.Event(), threading.Event()
    original = service._read_grouped
    cancels = [threading.Event(), threading.Event()]

    def blocked(*args):
        entered.set()
        assert release.wait(5)
        return original(*args)

    async def read_group(cancel):
        return await service.grouped_page(
            str(path),
            grouped,
            [],
            cancel,
            instance_id="test",
            live_generation=lambda: 0,
        )

    tasks = []
    with mock.patch.object(service, "_read_grouped", blocked):
        try:
            tasks.append(asyncio.create_task(read_group(cancels[0])))
            assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), 3)
            tasks.append(asyncio.create_task(read_group(cancels[1])))
            await asyncio.sleep(0)
            page = await service.page(str(path), regular, [], threading.Event())
            assert [row["id"] for row in page["rows"]] == ["a"]
            with pytest.raises(history.HistoryBusy):
                await asyncio.wait_for(asyncio.shield(tasks[1]), 1)
            tasks[0].cancel()
            with pytest.raises(asyncio.CancelledError):
                await tasks[0]
            assert cancels[0].is_set()
            # HTTP cancellation must not admit another serialized job yet.
            with pytest.raises(history.HistoryBusy):
                await read_group(threading.Event())
            assert (await service.page(str(path), regular, [], threading.Event()))[
                "count"
            ] == 1
        finally:
            for task in tasks:
                task.cancel()
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.get_running_loop().run_in_executor(
                service._group_worker, lambda: None
            )
    assert service._snapshots == {}
    page = await read_group(threading.Event())
    assert page["count"] == 1
    await service.done()


async def test_grouped_cancellation_removes_scratch_and_keeps_worker_slot(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path) as store:
        store.write(make_flow("a", 2000).get_state())
    service = history.HistoryReader()
    query = history.HistoryQuery.parse({"group_by": "host", "offset": "0"}, now=3000)
    entered = threading.Event()
    release = threading.Event()
    cancelled = threading.Event()
    original_batch = service._source_batch
    original_build = history.GroupStore.build

    def build(*args, **kwargs):
        return original_build(*args, directory=tmp_path, **kwargs)

    def batch(*args, **kwargs):
        result = original_batch(*args, **kwargs)
        if len(args) > 4 and "ORDER BY sequence" in args[4]:
            entered.set()
            assert release.wait(5)
        return result

    with (
        mock.patch.object(service, "_source_batch", batch),
        mock.patch.object(history.GroupStore, "build", build),
    ):
        task = asyncio.create_task(
            service.grouped_page(
                str(path),
                query,
                [],
                cancelled,
                instance_id="test",
                live_generation=lambda: 0,
            )
        )
        try:
            assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), 3)
            assert (
                not service._snapshots
            )  # Still copying the unpublished raw candidate.
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert cancelled.is_set()
            # The cancelled worker still owns its slot until source cleanup finishes.
            assert service._workers._value == 1
        finally:
            release.set()
        await asyncio.get_running_loop().run_in_executor(
            service._group_worker, lambda: None
        )
    assert not list(tmp_path.glob("group-snapshot-*"))
    assert service._snapshots == {}
    await service.done()


async def test_task_cancellation_keeps_worker_slot_until_finished(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    service = history.HistoryReader()
    query = history.HistoryQuery.parse({}, now=3000)
    entered = threading.Barrier(3)
    release = threading.Event()
    cancels = [threading.Event(), threading.Event()]
    original = service._read

    def blocked(*args, **kwargs):
        entered.wait(5)
        assert release.wait(5)
        return original(*args, **kwargs)

    with mock.patch.object(service, "_read", blocked):
        tasks = [
            asyncio.create_task(service.page(str(path), query, [], cancel))
            for cancel in cancels
        ]
        try:
            await asyncio.wait_for(asyncio.to_thread(entered.wait), 2)
            tasks[0].cancel()
            with pytest.raises(asyncio.CancelledError):
                await tasks[0]
            assert cancels[0].is_set()
            with pytest.raises(history.HistoryBusy):
                await service.page(str(path), query, [], threading.Event())
        finally:
            release.set()
        assert (await tasks[1])["count"] == 0
    assert (await service.page(str(path), query, [], threading.Event()))["count"] == 0


async def test_cancellation_during_initialization_does_not_open_reader(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    service = history.HistoryReader()
    event = threading.Event()
    original = sqlite.ensure_history_index

    def cancelled_index(path, *, cancel):
        assert not cancel()
        original(path, cancel=cancel)
        event.set()
        assert cancel()

    with mock.patch.object(sqlite, "ensure_history_index", cancelled_index):
        with pytest.raises(sqlite.StorageError, match="cancelled"):
            await service.page(
                str(path), history.HistoryQuery.parse({}, now=3000), [], event
            )
    with mock.patch.object(
        sqlite, "ensure_history_index", side_effect=sqlite.StorageError("index retry")
    ):
        with pytest.raises(sqlite.StorageError, match="index retry"):
            await service.page(
                str(path),
                history.HistoryQuery.parse({}, now=3000),
                [],
                threading.Event(),
            )
    assert (
        await service.page(
            str(path), history.HistoryQuery.parse({}, now=3000), [], threading.Event()
        )
    )["count"] == 0


@pytest.mark.parametrize("grouped", [False, True])
async def test_cancelled_index_waiter_releases_worker_before_lock_owner(
    tmp_path, grouped
):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    service = history.HistoryReader()
    cancel = threading.Event()
    entered = threading.Event()
    original = service._build_engine if grouped else history.snapshot

    def summarize(*args, **kwargs):
        entered.set()
        return original(*args, **kwargs)

    async def read():
        query = history.HistoryQuery.parse(
            {"group_by": "host"} if grouped else {}, now=3000
        )
        if grouped:
            return await service.grouped_page(
                str(path),
                query,
                [],
                cancel,
                instance_id="test",
                live_generation=lambda: 0,
            )
        return await service.page(str(path), query, [], cancel)

    service._index_lock.acquire()
    with mock.patch.object(
        service if grouped else history,
        "_build_engine" if grouped else "snapshot",
        summarize,
    ):
        task = asyncio.create_task(read())
        try:
            assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 1), 2)
            cancel.set()
            with pytest.raises(sqlite.StorageError, match="cancelled"):
                await asyncio.wait_for(asyncio.shield(task), 0.5)
            # Index ownership remains held, but the cancelled request's slot is free.
            assert service._index_lock.locked()
            assert not service._workers.locked()
        finally:
            service._index_lock.release()
            await asyncio.gather(task, return_exceptions=True)
            await service.done()


async def test_cancellation_after_highlight_skips_remaining_flows(tmp_path):
    flows = [make_flow("first", 2500), make_flow("second", 2600)]
    cancel = threading.Event()
    get_content = type(flows[0].request).get_content
    reads = []

    def read_body(request, *args, **kwargs):
        reads.append(request)
        cancel.set()
        return get_content(request, *args, **kwargs)

    with mock.patch.object(type(flows[0].request), "get_content", read_body):
        with pytest.raises(sqlite.StorageError, match="cancelled"):
            await history.HistoryReader().page(
                str(tmp_path / "never-opened.db"),
                history.HistoryQuery.parse({"highlight": "~bq request"}, now=3000),
                flows,
                cancel,
            )
    assert reads == [flows[0].request]
    assert not (tmp_path / "never-opened.db").exists()


@pytest.mark.parametrize("stage", ["acquire", "release"])
async def test_cancellation_at_index_lock_boundary_does_not_open_reader(
    tmp_path, stage
):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    service = history.HistoryReader()
    query = history.HistoryQuery.parse({}, now=3000)
    assert (await service.page(str(path), query, [], threading.Event()))["count"] == 0
    cancel = threading.Event()
    lock = service._index_lock

    class CancelAtLockBoundary:
        def acquire(self, **kwargs):
            acquired = lock.acquire(**kwargs)
            if stage == "acquire":
                cancel.set()
            return acquired

        def release(self):
            lock.release()
            if stage == "release":
                cancel.set()

    with (
        mock.patch.object(service, "_index_lock", CancelAtLockBoundary()),
        mock.patch.object(
            sqlite, "SQLiteFlowReader", side_effect=AssertionError("cancelled reader")
        ),
    ):
        with pytest.raises(sqlite.StorageError, match="cancelled"):
            await service.page(str(path), query, [], cancel)
    assert not lock.locked()


async def test_incremental_sizes_do_not_rescan_or_copy_message_lists(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    reads = []

    class Messages(list):
        def __iter__(self):
            raise AssertionError("Do not copy or scan the entire message list")

        def __getitem__(self, index):
            assert isinstance(index, int)
            reads.append(index)
            return super().__getitem__(index)

    f = tflow.ttcpflow()
    messages = Messages(f.messages)
    f.messages = messages
    f.timestamp_created = 2500
    service = history.HistoryReader()
    query = history.HistoryQuery.parse({}, now=3000)

    async def sizes():
        result = await service.page(str(path), query, [f], threading.Event())
        row = result["rows"][0]
        return row["request_size"], row["response_size"]

    assert await sizes() == (5, 7)
    assert reads == [0, 1]
    reads.clear()
    assert await sizes() == (5, 7)
    assert reads == []
    messages.append(tcp.TCPMessage(False, b"new"))
    service.sizes.changed(f)
    assert await sizes() == (5, 10)
    assert reads == [2]
    reads.clear()
    messages.pop()
    assert await sizes() == (5, 7)
    assert reads == [0, 1]
    f.messages = Messages([tcp.TCPMessage(True, b"replacement")])
    reads.clear()
    assert await sizes() == (11, 0)
    assert reads == [0]


@pytest.mark.parametrize("count", [1, 1000])
async def test_message_size_scan_cancellation_is_bounded_and_retryable(tmp_path, count):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    cancel = threading.Event()
    reads = []

    class Content(bytes):
        def __len__(self):
            reads.append(1)
            if len(reads) == 1:
                cancel.set()
            return super().__len__()

    f = tflow.ttcpflow(
        messages=[tcp.TCPMessage(True, Content(b"x")) for _ in range(count)]
    )
    f.timestamp_created = 2500
    service = history.HistoryReader()
    query = history.HistoryQuery.parse({}, now=3000)
    with pytest.raises(sqlite.StorageError, match="cancelled"):
        await service.page(str(path), query, [f], cancel)
    assert 0 < len(reads) <= 128
    cancel.clear()
    row = (await service.page(str(path), query, [f], cancel))["rows"][0]
    assert (row["request_size"], row["response_size"]) == (count, 0)


async def test_dns_size_scan_cancellation_is_bounded_and_retryable(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    cancel = threading.Event()
    f = tflow.tdnsflow()
    f.request.questions *= 1000
    f.timestamp_created = 2500
    pack = history.domain_names.pack
    reads = []

    def name_size(name):
        reads.append(name)
        cancel.set()
        return pack(name)

    service = history.HistoryReader()
    query = history.HistoryQuery.parse({}, now=3000)
    with mock.patch.object(history.domain_names, "pack", name_size):
        with pytest.raises(sqlite.StorageError, match="cancelled"):
            await service.page(str(path), query, [f], cancel)
    assert 0 < len(reads) <= 128
    cancel.clear()
    row = (await service.page(str(path), query, [f], cancel))["rows"][0]
    assert (row["request_size"], row["response_size"]) == (16012, None)


async def test_message_size_workers_do_not_lock_scans_or_cache_invalidated_results(
    tmp_path,
):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    entered = threading.Barrier(3)
    release = threading.Event()

    class Content(bytes):
        def __len__(self):
            entered.wait(3)
            assert release.wait(5)
            return super().__len__()

    f = tflow.ttcpflow(messages=[tcp.TCPMessage(True, Content(b"x"))])
    f.timestamp_created = 2500
    service = history.HistoryReader()
    query = history.HistoryQuery.parse({}, now=3000)
    tasks = [
        asyncio.create_task(service.page(str(path), query, [f], threading.Event()))
        for _ in range(2)
    ]
    try:
        await asyncio.wait_for(asyncio.to_thread(entered.wait, 3), 4)
        service.update([f])
    finally:
        release.set()
        results = await asyncio.gather(*tasks, return_exceptions=True)
    assert all(isinstance(result, sqlite.StorageError) for result in results)
    f.messages[0].content = b"ok"
    row = (await service.page(str(path), query, [f], threading.Event()))["rows"][0]
    assert (row["request_size"], row["response_size"]) == (2, 0)


async def test_message_size_cache_is_bounded_and_does_not_retain_flows(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    flows = [tflow.ttcpflow(messages=[]) for _ in range(1001)]
    for f in flows:
        f.timestamp_created = 2500
    reference = weakref.ref(f)
    del f
    service = history.HistoryReader()
    result = await service.page(
        str(path),
        history.HistoryQuery.parse({"limit": "1"}, now=3000),
        flows,
        threading.Event(),
    )
    assert result["count"] == 1001
    assert len(service.sizes._entries) == 1000
    del flows
    await asyncio.sleep(0)
    gc.collect()
    assert reference() is None
    assert len(service.sizes._entries) == 0


async def test_older_size_worker_does_not_replace_newer_cached_prefix(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    entered = [threading.Event(), threading.Event()]
    releases = [threading.Event(), threading.Event()]
    reads = []

    class Content(bytes):
        def __len__(self):
            index = len(reads)
            reads.append(1)
            assert index < 2, "A completed prefix must not be scanned again"
            entered[index].set()
            assert releases[index].wait(5)
            return super().__len__()

    f = tflow.ttcpflow(messages=[tcp.TCPMessage(True, Content(b"x"))])
    f.timestamp_created = 2500
    service = history.HistoryReader()
    query = history.HistoryQuery.parse({}, now=3000)
    tasks = [
        asyncio.create_task(service.page(str(path), query, [f], threading.Event()))
    ]
    try:
        assert await asyncio.wait_for(asyncio.to_thread(entered[0].wait, 2), 3)
        f.messages.append(tcp.TCPMessage(True, b"yz"))
        service.sizes.changed(f)
        tasks.append(
            asyncio.create_task(service.page(str(path), query, [f], threading.Event()))
        )
        assert await asyncio.wait_for(asyncio.to_thread(entered[1].wait, 2), 3)
        releases[1].set()
        assert (await tasks[1])["rows"][0]["request_size"] == 3
        releases[0].set()
        assert (await tasks[0])["rows"][0]["request_size"] == 1
    finally:
        for release in releases:
            release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
    row = (await service.page(str(path), query, [f], threading.Event()))["rows"][0]
    assert row["request_size"] == 3
    assert len(reads) == 2


async def test_message_list_replacement_during_scan_is_retryable(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass

    class Content(bytes):
        def __len__(self):
            f.messages = []
            return super().__len__()

    f = tflow.ttcpflow(messages=[tcp.TCPMessage(True, Content(b"x"))])
    f.timestamp_created = 2500
    service = history.HistoryReader()
    query = history.HistoryQuery.parse({}, now=3000)
    with pytest.raises(sqlite.StorageError, match="Live flow changed"):
        await service.page(str(path), query, [f], threading.Event())
    row = (await service.page(str(path), query, [f], threading.Event()))["rows"][0]
    assert (row["request_size"], row["response_size"]) == (0, 0)


async def test_new_filter_uses_captured_status_not_unnotified_edit():
    from mitmproxy.tools.web.history import HistoryQuery
    from mitmproxy.tools.web.history import LiveHistoryIndex

    view = View()
    index = LiveHistoryIndex(view)
    flow = tflow.tflow(resp=True)
    flow.response.status_code = 418
    try:
        view.add([flow])
        await index.flush()
        flow.response.status_code = 205
        query = HistoryQuery.parse({"search": "~c 205", "highlight": "~c 418"})
        token = await index.register(query, threading.Event())
        with index.read_cut() as (db, cut):
            row = next(index.rows(db, cut))
            assert row["status_code"] == 418
            assert row["search:" + token] is False
            assert row["highlight:" + token] is True
        view.update([flow])
        with index.read_cut() as (db, cut):
            row = next(index.rows(db, cut))
            assert row["status_code"] == 205
            assert row["search:" + token] is True
            assert row["highlight:" + token] is False
    finally:
        await index.done()


async def test_composed_method_filter_matches_captured_scalars():
    from mitmproxy.tools.web.history import HistoryQuery
    from mitmproxy.tools.web.history import LiveHistoryIndex

    view = View()
    index = LiveHistoryIndex(view)
    flow = tflow.tflow(resp=True)
    flow.response.status_code = 418
    try:
        view.add([flow])
        flow.request.method = "POST"
        token = await index.register(
            HistoryQuery.parse({"search": "~http & ~m GET & !~c 205"}),
            threading.Event(),
        )
        with index.read_cut() as (db, cut):
            row = next(index.rows(db, cut))
            assert row["method"] == "GET"
            assert row["search:" + token] is True
        view.update([flow])
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["search:" + token] is False
    finally:
        await index.done()


async def test_late_body_and_url_filters_use_captured_inputs():
    from mitmproxy.tools.web.history import HistoryQuery
    from mitmproxy.tools.web.history import LiveHistoryIndex

    view = View()
    index = LiveHistoryIndex(view)
    try:
        flow = tflow.tflow()
        flow.request.raw_content = b"first body"
        view.add([flow])
        first = await index.flush()
        url = flow.request.pretty_url
        flow.request.raw_content = b"second body"
        flow.request.host = "changed.test"
        token = await index.register(
            HistoryQuery.parse({"search": "~bq first", "highlight": url}),
            threading.Event(),
        )
        with index.read_cut() as (db, cut):
            row = next(index.rows(db, cut))
            assert row["search:" + token] is True
            assert row["highlight:" + token] is True
            assert not any("body" in key or "footprint" in key for key in row)
            changes = list(index.changes(db, first, cut, limit=100))
            assert all(
                "_filter_inputs" not in event[3] for event in changes if event[3]
            )
            assert all("_body_stamp" not in event[3] for event in changes if event[3])
        view.update([flow])
        with index.read_cut() as (db, cut):
            row = next(index.rows(db, cut))
            assert row["search:" + token] is False
            assert row["highlight:" + token] is False
    finally:
        await index.done()


async def test_oversized_body_does_not_poison_live_metadata_or_url_filters():
    from mitmproxy.tools.web.history import HistoryQuery
    from mitmproxy.tools.web.history import LiveHistoryIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    view = View()
    index = LiveHistoryIndex(view)
    f = tflow.tflow()
    f.request.raw_content = b"x" * (index.queue_bytes // 2 + 1)
    try:
        view.add([f])
        await index.flush()
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["id"] == f.id
        token = await index.register(
            HistoryQuery.parse(
                {"search": f.request.host, "highlight": "~d " + f.request.host}
            ),
            threading.Event(),
            verify=False,
        )
        with index.read_cut() as (db, cut):
            row = next(index.rows(db, cut))
            assert row["id"] == f.id
            assert row["search:" + token] is True
            assert row["highlight:" + token] is True
        with pytest.raises(LiveIndexError, match="unavailable"):
            await index.register(
                HistoryQuery.parse({"search": "~bq needle", "mode": "absolute"}),
                threading.Event(),
            )
        await index.register(HistoryQuery.parse({}), threading.Event())
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["id"] == f.id
    finally:
        await index.done()


async def test_gzip_bomb_is_bounded_and_does_not_touch_decode_cache():
    from mitmproxy.net import encoding
    from mitmproxy.tools.web.history import HistoryQuery
    from mitmproxy.tools.web.history import LiveHistoryIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    view = View()
    index = LiveHistoryIndex(view)
    f = tflow.tflow()
    f.request.headers["Content-Encoding"] = "gzip"
    compressor = __import__("zlib").compressobj(wbits=31)
    chunk = b"x" * 65536
    f.request.raw_content = (
        b"".join(compressor.compress(chunk) for _ in range(1600)) + compressor.flush()
    )
    before = encoding._cache
    try:
        view.add([f])
        await index.flush()
        assert encoding._cache is before
        with pytest.raises(LiveIndexError, match="unavailable"):
            await index.register(
                HistoryQuery.parse({"search": "~bq x", "mode": "absolute"}),
                threading.Event(),
            )
        await index.register(HistoryQuery.parse({}), threading.Event())
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["id"] == f.id
    finally:
        await index.done()


async def test_evicted_bodies_recover_only_if_notified_state_matches():
    from mitmproxy.tools.web.history import HistoryQuery
    from mitmproxy.tools.web.history import LiveHistoryIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    view = View()
    index = LiveHistoryIndex(view)
    flows = [tflow.tflow() for _ in range(14)]
    try:
        for f in flows:
            f.request.raw_content = b"needle" + b"x" * 900000
            view.add([f])
            await index.flush()
        assert flows[0].id not in index._body_cache
        query = HistoryQuery.parse({"search": "~bq needle", "mode": "absolute"})
        flows[0].request.raw_content = b"changed"
        with pytest.raises(LiveIndexError, match="unavailable"):
            await index.register(query, threading.Event())
        flows[0].request.raw_content = b"needle" + b"x" * 900000
        token = await index.register(query, threading.Event())
        with index.read_cut() as (db, cut):
            assert all(row["search:" + token] for row in index.rows(db, cut))
        flows[0].request.raw_content = b"different"
        view.update([flows[0]])
        with index.read_cut() as (db, cut):
            rows = {row["id"]: row for row in index.rows(db, cut)}
            assert rows[flows[0].id]["search:" + token] is False
            assert all(rows[f.id]["search:" + token] for f in flows[1:])
    finally:
        await index.done()


async def test_body_fingerprint_includes_encoding_and_stream_direction():
    from mitmproxy.tools.web.history import LiveHistoryIndex

    index = LiveHistoryIndex(View())
    try:
        f = tflow.tflow()
        f.request.raw_content = b"same"
        first = index._body_fingerprint(index._body_record(f)[0])
        f.request.headers["content-encoding"] = "gzip"
        assert index._body_fingerprint(index._body_record(f)[0]) != first
        tcp_flow = tflow.ttcpflow()
        first = index._body_fingerprint(index._body_record(tcp_flow)[0])
        tcp_flow.messages[0].from_client = not tcp_flow.messages[0].from_client
        assert index._body_fingerprint(index._body_record(tcp_flow)[0]) != first
    finally:
        await index.done()


async def test_unfiltered_capture_never_decodes_body():
    from mitmproxy.tools.web.history import LiveHistoryIndex

    view = View()
    index = LiveHistoryIndex(view)
    f = tflow.tflow()
    f.request.raw_content = b"body"
    try:
        with mock.patch.object(
            index,
            "_bounded_content",
            side_effect=AssertionError("decoded during metadata capture"),
        ):
            view.add([f])
            with index.read_cut() as (db, cut):
                assert next(index.rows(db, cut))["id"] == f.id
    finally:
        await index.done()


async def test_unavailable_body_outside_window_does_not_fail_query():
    from mitmproxy.tools.web.history import HistoryQuery
    from mitmproxy.tools.web.history import LiveHistoryIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    view = View()
    index = LiveHistoryIndex(view)
    old, current = tflow.tflow(), tflow.tflow()
    old.timestamp_created, current.timestamp_created = 10, 100
    old.request.raw_content = b"x" * (2 * 1024 * 1024)
    current.request.raw_content = b"needle"
    try:
        view.add([old, current])
        token = await index.register(
            HistoryQuery.parse(
                {
                    "search": "~bq needle",
                    "mode": "absolute",
                    "from": "0",
                    "until": "150",
                }
            ),
            threading.Event(),
            verify=False,
        )
        with index.read_cut(bounds=(50, 150)) as (db, cut):
            rows = {row["id"]: row for row in index.rows(db, cut)}
            assert rows[current.id]["search:" + token] is True
        with pytest.raises(LiveIndexError, match="unavailable"):
            with index.read_cut(bounds=(0, 20)):
                pass
        await index.register(
            HistoryQuery.parse(
                {
                    "search": "~bq needle",
                    "mode": "absolute",
                    "from": "50",
                    "until": "150",
                }
            ),
            threading.Event(),
        )
        with index.read_cut() as (db, cut):
            assert any(row["id"] == current.id for row in index.rows(db, cut))
    finally:
        await index.done()


async def test_unavailable_highlight_on_search_excluded_flow_is_ignored():
    from mitmproxy.tools.web.history import HistoryQuery
    from mitmproxy.tools.web.history import LiveHistoryIndex

    view = View()
    index = LiveHistoryIndex(view)
    excluded = tflow.tflow(resp=True)
    excluded.response.status_code = 404
    excluded.request.raw_content = b"x" * (2 * 1024 * 1024)
    try:
        view.add([excluded])
        token = await index.register(
            HistoryQuery.parse(
                {"search": "~c 200", "highlight": "~bq x", "mode": "absolute"}
            ),
            threading.Event(),
        )
        with index.read_cut() as (db, cut):
            row = next(index.rows(db, cut))
            assert row["search:" + token] is False
    finally:
        await index.done()


async def test_body_cache_eviction_does_not_break_metadata_spool():
    from mitmproxy.tools.web.history import HistoryQuery
    from mitmproxy.tools.web.history import LiveHistoryIndex

    view = View()
    index = LiveHistoryIndex(view, body_cache_bytes=200000, disk_bytes=128 * 1024)
    flows = [tflow.tflow() for _ in range(12)]
    try:
        for f in flows:
            f.request.raw_content = b"x" * 100000
            view.add([f])
            await index.flush()
        with index.read_cut() as (db, cut):
            assert len(list(index.rows(db, cut))) == len(flows)
            assert (
                db.execute("SELECT sum(length(data)) FROM current").fetchone()[0]
                < 50000
            )
        token = await index.register(
            HistoryQuery.parse({"search": "~bq x", "mode": "absolute"}),
            threading.Event(),
        )
        with index.read_cut() as (db, cut):
            assert all(row["search:" + token] for row in index.rows(db, cut))
        await index.register(HistoryQuery.parse({}), threading.Event())
        with index.read_cut() as (db, cut):
            assert len(list(index.rows(db, cut))) == len(flows)
    finally:
        await index.done()


async def test_invalid_content_encoding_uses_raw_body_without_global_cache():
    from mitmproxy.net import encoding
    from mitmproxy.tools.web.history import HistoryQuery
    from mitmproxy.tools.web.history import LiveHistoryIndex

    view = View()
    index = LiveHistoryIndex(view)
    f = tflow.tflow()
    f.request.headers["Content-Encoding"] = "gzip"
    f.request.raw_content = b"not gzip but contains needle"
    before = encoding._cache
    try:
        view.add([f])
        token = await index.register(
            HistoryQuery.parse({"search": "~bq needle"}), threading.Event()
        )
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["search:" + token] is True
        assert encoding._cache is before
    finally:
        await index.done()


@pytest.mark.parametrize(
    "codec", ["gzip", "deflate", "deflateraw", "br", "zstd", "identity"]
)
def test_bounded_content_decoder_matches_project(codec):
    from mitmproxy.net import encoding
    from mitmproxy.tools.web.history import LiveHistoryIndex

    f = tflow.tflow()
    body = b"message contents " * 1000
    raw = encoding.encode(body, codec)
    f.request.headers["Content-Encoding"] = codec
    f.request.raw_content = raw
    before = encoding._cache
    assert LiveHistoryIndex._bounded_content(f.request, len(body)) == body
    assert encoding._cache is before
    assert LiveHistoryIndex._bounded_content(f.request, 32) is None


def test_bounded_zstd_decoder_handles_concatenated_frames():
    from mitmproxy.net import encoding
    from mitmproxy.tools.web.history import LiveHistoryIndex

    f = tflow.tflow()
    f.request.headers["Content-Encoding"] = "zstd"
    f.request.raw_content = encoding.zstd.compress(b"first") + encoding.zstd.compress(
        b"second"
    )
    assert LiveHistoryIndex._bounded_content(f.request, 11) == b"firstsecond"


async def test_later_oversized_body_fails_only_active_body_query():
    from mitmproxy.tools.web.history import HistoryQuery
    from mitmproxy.tools.web.history import LiveHistoryIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    view = View()
    index = LiveHistoryIndex(view)
    f = tflow.tflow()
    try:
        await index.register(
            HistoryQuery.parse({"search": "~bq needle", "mode": "absolute"}),
            threading.Event(),
        )
        view.add([f])
        f.request.raw_content = b"x" * (2 * 1024 * 1024)
        view.update([f])
        with pytest.raises(LiveIndexError, match="unavailable"):
            with index.read_cut():
                pass
        await index.register(HistoryQuery.parse({}), threading.Event())
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["id"] == f.id
    finally:
        await index.done()


async def test_status_grouping_remains_available_for_oversized_live_body(tmp_path):
    from mitmproxy.io import sqlite
    from mitmproxy.tools.web.history import HistoryQuery
    from mitmproxy.tools.web.history import HistoryReader
    from mitmproxy.tools.web.history import LiveHistoryIndex

    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    view = View()
    reader = HistoryReader()
    reader.live_index = LiveHistoryIndex(view)
    f = tflow.tflow(resp=True)
    f.request.raw_content = b"x" * (2 * 1024 * 1024)
    try:
        view.add([f])
        result = await reader.grouped_page(
            str(path),
            HistoryQuery.parse({"group_by": "status", "mode": "absolute"}),
            [],
            threading.Event(),
            instance_id="test",
            live_generation=lambda: 0,
        )
        assert result["count"] == 1
        assert result["groups"][0]["label"] == str(f.response.status_code)
    finally:
        await reader.done()


async def test_filter_uses_captured_decoded_body_and_headers():
    import gzip

    from mitmproxy.tools.web.history import HistoryQuery
    from mitmproxy.tools.web.history import LiveHistoryIndex

    view = View()
    index = LiveHistoryIndex(view)
    f = tflow.tflow(resp=True)
    f.request.headers["Content-Encoding"] = "gzip"
    f.request.headers["X-Token"] = "first"
    f.request.raw_content = gzip.compress(b"secret needle")
    try:
        view.add([f])
        await index.flush()
        f.request.headers["X-Token"] = "second"
        f.request.raw_content = gzip.compress(b"other")
        token = await index.register(
            HistoryQuery.parse({"search": "~bq needle & ~hq first"}), threading.Event()
        )
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["search:" + token] is True
        view.update([f])
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["search:" + token] is False
    finally:
        await index.done()


async def test_body_filter_registered_before_flow_is_evaluated_on_notifications():
    from mitmproxy.tools.web.history import HistoryQuery
    from mitmproxy.tools.web.history import LiveHistoryIndex

    view = View()
    index = LiveHistoryIndex(view)
    try:
        token = await index.register(
            HistoryQuery.parse({"search": "~bq needle"}), threading.Event()
        )
        flow = tflow.tflow()
        flow.request.raw_content = b"needle"
        view.add([flow])
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["search:" + token] is True
        flow.request.raw_content = b"different"
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["search:" + token] is True
        view.update([flow])
        with index.read_cut() as (db, cut):
            assert next(index.rows(db, cut))["search:" + token] is False
    finally:
        await index.done()


@pytest.mark.parametrize(
    "factory",
    [tflow.tflow, tflow.ttcpflow, tflow.tudpflow, tflow.tdnsflow, tflow.twebsocketflow],
)
async def test_late_predicates_match_event_time_flowfilter(factory):
    from mitmproxy import flowfilter
    from mitmproxy.tools.web.history import HistoryQuery
    from mitmproxy.tools.web.history import LiveHistoryIndex

    view = View()
    index = LiveHistoryIndex(view)
    f = factory()
    f.marked = ":star:"
    f.comment = "important comment"
    f.metadata["alpha"] = "beta"
    expressions = [
        "~all",
        "~http",
        "~tcp",
        "~udp",
        "~dns",
        "~websocket",
        "~q",
        "~s",
        "~e",
        "~marked",
        "~marker star",
        "~comment important",
        "~meta beta",
        "~replay",
        "~replayq",
        "~replays",
        "~src .",
        "~dst .",
        "~c 200",
        "~m GET",
        "~d example",
        "example",
        "~b message",
        "~bq message",
        "~bs message",
        "~h host",
        "~hq host",
        "~hs host",
        "~t text",
        "~tq text",
        "~ts text",
        "~a",
        "~dt >= 1970-01-01",
        "~dtq >= 1970-01-01",
        "~dts >= 1970-01-01",
        "~dtqs >= 1970-01-01",
        "~dtqe >= 1970-01-01",
        "~dtss >= 1970-01-01",
        "~dtse >= 1970-01-01",
    ]
    try:
        view.add([f])
        await index.flush()
        expected = {expr: bool(flowfilter.parse(expr)(f)) for expr in expressions}
        f.comment = "changed after capture"
        for expr in expressions:
            token = await index.register(
                HistoryQuery.parse({"search": expr}), threading.Event()
            )
            with index.read_cut() as (db, cut):
                row = next(index.rows(db, cut))
                assert row["search:" + token] == expected[expr], (
                    factory.__name__,
                    expr,
                )
            index.prune(set())
    finally:
        await index.done()
