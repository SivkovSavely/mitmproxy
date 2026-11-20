import sqlite3
import threading
import time
from unittest import mock

import pytest

from mitmproxy.addons.view import View
from mitmproxy.test import tflow


def test_value_and_constructor_bounds():
    from mitmproxy.tools.web.live_history import _value
    from mitmproxy.tools.web.live_history import LiveIndex

    assert _value(None) == (None, False)
    assert _value("x" * (1024 * 1024 + 1)) == (None, True)
    assert _value("é" * (1024 * 512 + 1)) == (None, True)
    view = View()
    view.add([tflow.tflow()])
    with pytest.raises(ValueError, match="empty View"):
        LiveIndex(view)
    with pytest.raises(ValueError, match="positive"):
        LiveIndex(View(), disk_bytes=0)


async def test_invalid_change_ranges_and_closed_index():
    from mitmproxy.tools.web.live_history import LiveIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    index = LiveIndex(View())
    with index.read_cut() as (db, cut):
        for limit in (0, 10001, None):
            with pytest.raises(ValueError, match="limit"):
                list(index.changes(db, 0, cut, limit=limit))
        with pytest.raises(LiveIndexError, match="range"):
            list(index.changes(db, 1, cut, limit=1))
        with pytest.raises(LiveIndexError, match="range"):
            list(index.changes(db, 0, cut + 1, limit=1))
    await index.done()
    index.capture(tflow.tflow())
    with pytest.raises(LiveIndexError, match="closed"):
        index.cut()


async def test_capture_error_and_cancelled_reader():
    from mitmproxy.tools.web.live_history import LiveIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    index = LiveIndex(View())
    cancel = threading.Event()
    try:
        with pytest.raises(LiveIndexError, match="cancelled"):
            with index.read_cut(cancel):
                cancel.set()
        with mock.patch(
            "mitmproxy.tools.web.live_history._metadata",
            side_effect=ValueError("invalid"),
        ):
            index.capture(tflow.tflow())
        with pytest.raises(LiveIndexError, match="capture failed"):
            index.cut()
    finally:
        await index.done()


async def test_reader_wraps_sqlite_errors_and_fence_mismatch():
    from mitmproxy.tools.web.live_history import LiveIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    index = LiveIndex(View())
    try:
        with pytest.raises(LiveIndexError, match="read failed"):
            with index.read_cut():
                raise sqlite3.OperationalError("test")
        original = index._fence

        def bad_fence(*args, **kwargs):
            fence = original(*args, **kwargs)
            fence.sequence += 1
            return fence

        with mock.patch.object(index, "_fence", bad_fence):
            with pytest.raises(LiveIndexError, match="fence mismatch"):
                with index.read_cut():
                    pass
    finally:
        await index.done()


async def test_waiting_fence_cancellation_and_worker_error():
    from mitmproxy.tools.web.live_history import LiveIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    entered, release = threading.Event(), threading.Event()
    original = LiveIndex._write_batch

    def blocked(index, db, batch):
        entered.set()
        assert release.wait(5)
        raise sqlite3.OperationalError("spool failure")

    view = View()
    with mock.patch.object(LiveIndex, "_write_batch", blocked):
        index = LiveIndex(view)
        try:
            view.add([tflow.tflow()])
            assert entered.wait(5)
            cancel = threading.Event()
            cancel.set()
            with pytest.raises(LiveIndexError, match="cancelled"):
                await __import__("asyncio").to_thread(index._fence, cancel, True)
            release.set()
            with pytest.raises(LiveIndexError, match="spool failure"):
                await index.flush()
        finally:
            release.set()
            await index.done()
    assert original is LiveIndex._write_batch


async def test_queued_fence_is_released_on_spool_error():
    import asyncio

    from mitmproxy.tools.web.live_history import LiveIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    entered, release = threading.Event(), threading.Event()

    def blocked(index, db, batch):
        entered.set()
        assert release.wait(5)
        raise ValueError("failed spool")

    view = View()
    with mock.patch.object(LiveIndex, "_write_batch", blocked):
        index = LiveIndex(view)
        try:
            view.add([tflow.tflow()])
            assert entered.wait(5)
            task = asyncio.create_task(asyncio.to_thread(index._fence, None, True))
            while True:
                with index._condition:
                    if any(hasattr(item, "ready") for item in index._queue):
                        break
                await asyncio.sleep(0)
            release.set()
            with pytest.raises(LiveIndexError, match="failed spool"):
                await task
        finally:
            release.set()
            await index.done()


async def test_fence_queue_capacity():
    from mitmproxy.tools.web.live_history import LiveIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    index = LiveIndex(View(), queue_bytes=255)
    try:
        with pytest.raises(LiveIndexError, match="capacity exceeded by fence"):
            index.cut()
    finally:
        await index.done()


async def test_fence_detects_unexpected_worker_stop():
    from mitmproxy.tools.web.live_history import LiveIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    index = LiveIndex(View())
    try:
        with index._condition:
            with mock.patch.object(index._worker, "is_alive", return_value=False):
                # Keep the real worker from dequeuing the fence until the check.
                with pytest.raises(LiveIndexError, match="worker stopped"):
                    index._fence(pin=True)
    finally:
        await index.done()


async def test_shutdown_releases_waiting_pinned_fence():
    import asyncio

    from mitmproxy.tools.web.live_history import LiveIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    index = LiveIndex(View())
    fence = await asyncio.to_thread(index._fence, None, True)
    try:
        assert fence.release is not None
        # The spool is waiting on this pin; shutdown must release it without a client.
        await asyncio.wait_for(index.done(), 2)
        with pytest.raises(LiveIndexError, match="closed"):
            index.cut()
    finally:
        fence.release.set()


async def test_close_race_while_fence_waits():
    import asyncio

    from mitmproxy.tools.web.live_history import LiveIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    entered, release = threading.Event(), threading.Event()

    def blocked(index, db, batch):
        entered.set()
        assert release.wait(5)

    view = View()
    with mock.patch.object(LiveIndex, "_write_batch", blocked):
        index = LiveIndex(view)
        try:
            view.add([tflow.tflow()])
            assert entered.wait(5)
            waiter = asyncio.create_task(asyncio.to_thread(index._fence, None, True))
            while True:
                with index._condition:
                    if index._queue:
                        break
                await asyncio.sleep(0)
            shutdown = asyncio.create_task(index.done())
            await asyncio.sleep(0)
            release.set()
            with pytest.raises(LiveIndexError, match="closed"):
                await waiter
            await shutdown
        finally:
            release.set()


async def test_spool_failure_while_pin_is_active_wakes_reader():
    import asyncio

    from mitmproxy.tools.web.live_history import _Fence
    from mitmproxy.tools.web.live_history import LiveIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    original = _Fence.__init__

    class BrokenRelease:
        def wait(self, timeout):
            raise RuntimeError("pin failure")

        def set(self):
            pass

    def broken(fence, sequence, pin=False):
        original(fence, sequence, pin)
        if pin:
            fence.release = BrokenRelease()

    index = LiveIndex(View())
    try:
        with mock.patch.object(_Fence, "__init__", broken):
            with pytest.raises(LiveIndexError, match="pin failure"):
                await asyncio.to_thread(index._fence, None, True)
    finally:
        await index.done()


async def test_capture_cut_and_hidden_removal():
    from mitmproxy.tools.web.live_history import LiveIndex

    view = View()
    view.set_filter_cmd("~m POST")
    index = LiveIndex(view)
    flow = tflow.tflow()
    try:
        view.add([flow])
        first = await index.flush()
        flow.request.method = "POST"
        with index.read_cut() as (db, _):
            assert list(index.rows(db, first))[0]["method"] == "GET"
        view.update([flow])
        second = await index.flush()
        with index.read_cut() as (db, _):
            assert list(index.rows(db, second))[0]["method"] == "POST"
        view.remove([flow])
        third = await index.flush()
        with index.read_cut() as (db, _):
            assert list(index.rows(db, third)) == []
    finally:
        await index.done()
    assert not index.path.exists()


async def test_overflow_is_reported():
    from mitmproxy.tools.web.live_history import LiveIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    view = View()
    index = LiveIndex(view, queue_bytes=1)
    try:
        view.add([tflow.tflow()])
        with pytest.raises(LiveIndexError, match="queue"):
            await index.flush()
    finally:
        await index.done()


async def test_lifecycle_clear_and_oversized():
    from mitmproxy import http
    from mitmproxy.tools.web.live_history import LiveIndex

    view = View()
    index = LiveIndex(view)
    flow = tflow.tflow()
    try:
        view.requestheaders(flow)
        flow.request.raw_content = b"body"
        view.request(flow)
        flow.response = http.Response.make(204)
        view.responseheaders(flow)
        flow.request.method = "x" * (1024 * 1024 + 1)
        view.update([flow])
        cut = await index.flush()
        with index.read_cut() as (db, _):
            row = next(index.rows(db, cut))
            assert row["method"] is None and row["method_oversized"]
            assert row["request_size"] == 4
            assert row["status_code"] == 204
        view.clear()
        cleared = await index.flush()
        with index.read_cut() as (db, _):
            assert list(index.rows(db, cleared)) == []
    finally:
        await index.done()


async def test_journal_changes_and_disk_backed_inventory():
    from mitmproxy.tools.web.live_history import LiveIndex

    view = View()
    index = LiveIndex(view)
    try:
        flows = [tflow.tflow() for _ in range(300)]
        view.add(flows)
        cut = await index.flush()
        with index.read_cut() as (db, baseline):
            assert baseline == cut
            assert sum(1 for _ in index.rows(db, baseline)) == len(flows)
        view.clear_not_marked()
        end = await index.flush()
        with index.read_cut() as (db, _):
            changes = list(index.changes(db, cut, end, limit=500))
            assert len(changes) == len(flows)
            assert all(change[1] == "remove" for change in changes)
            assert list(index.rows(db, end)) == []
    finally:
        await index.done()


async def test_journal_retention_expires_old_cuts():
    from mitmproxy.tools.web.live_history import LiveIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    view = View()
    index = LiveIndex(view, journal_bytes=700)
    try:
        flow = tflow.tflow()
        unchanged = tflow.tflow()
        view.add([flow, unchanged])
        first = await index.flush()
        for i in range(10):
            flow.request.method = f"X{i}"
            view.update([flow])
            await index.flush()
        with index.read_cut() as (db, cut):
            rows = {row["id"]: row for row in index.rows(db, cut)}
            assert rows[flow.id]["method"] == "X9"
            assert rows[unchanged.id]["method"] == "GET"
            with pytest.raises(LiveIndexError, match="expired"):
                list(index.changes(db, first, cut, limit=100))
    finally:
        await index.done()


async def test_fence_preserves_coalesced_events(monkeypatch):
    from mitmproxy.tools.web.live_history import LiveIndex

    entered, release = threading.Event(), threading.Event()
    original = LiveIndex._write_batch

    def paused(self, db, batch):
        entered.set()
        assert release.wait(5)
        return original(self, db, batch)

    monkeypatch.setattr(LiveIndex, "_write_batch", paused)
    view = View()
    index = LiveIndex(view, queue_bytes=3000)
    flow = tflow.tflow()
    try:
        view.add([flow])
        assert entered.wait(5)
        for i in range(100):
            flow.request.method = f"X{i}"
            view.update([flow])
        result = []
        fence_thread = threading.Thread(target=lambda: result.append(index.cut()))
        fence_thread.start()
        with index._condition:
            assert index._condition.wait_for(
                lambda: any(hasattr(item, "ready") for item in index._queue), 5
            )
        flow.request.method = "AFTER"
        view.update([flow])
        release.set()
        fence_thread.join(5)
        assert not fence_thread.is_alive()
        cut = result[0]
        end = await index.flush()
        with index.read_cut() as (db, seq):
            assert seq == end
            assert next(index.rows(db, seq))["method"] == "AFTER"
            changes = list(index.changes(db, 0, seq, limit=1000))
            assert len(changes) < 10
            assert [item[3]["method"] for item in changes if item[0] <= cut][
                -1
            ] == "X99"
    finally:
        release.set()
        await index.done()


async def test_pinned_reader_reports_wal_exhaustion():
    from mitmproxy.tools.web.live_history import LiveIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    view = View()
    index = LiveIndex(view, wal_bytes=1)
    flow = tflow.tflow()
    try:
        view.add([flow])
        await index.flush()
        with pytest.raises(LiveIndexError, match="WAL"):
            with index.read_cut() as (db, cut):
                assert next(index.rows(db, cut))["method"] == "GET"
                flow.request.method = "POST"
                view.update([flow])
                with pytest.raises(LiveIndexError, match="WAL"):
                    await index.flush()
    finally:
        await index.done()


async def test_pinned_current_and_bounded_change_pages():
    from mitmproxy.tools.web.live_history import LiveIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    view = View()
    index = LiveIndex(view)
    flow = tflow.tflow()
    try:
        view.add([flow])
        await index.flush()
        with index.read_cut() as (db, baseline):
            flow.request.method = "POST"
            view.update([flow])
            await index.flush()
            assert next(index.rows(db, baseline))["method"] == "GET"
        view.remove([flow])
        end = await index.flush()
        with index.read_cut() as (db, cut):
            assert cut == end
            first = list(index.changes(db, baseline, cut, limit=1))
            second = list(index.changes(db, first[-1][0], cut, limit=1))
            assert [first[0][1], second[0][1]] == ["upsert", "remove"]
            with pytest.raises(LiveIndexError, match="does not match"):
                index.rows(db, baseline)
    finally:
        await index.done()


async def test_reader_deadline():
    from mitmproxy.tools.web.live_history import LiveIndex
    from mitmproxy.tools.web.live_history import LiveIndexError

    index = LiveIndex(View(), read_timeout=0.01)
    try:
        with pytest.raises(LiveIndexError, match="deadline"):
            with index.read_cut():
                time.sleep(0.02)
    finally:
        await index.done()
