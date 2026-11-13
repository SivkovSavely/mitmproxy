import asyncio
import logging
from concurrent.futures import Future

import pytest

from mitmproxy import exceptions
from mitmproxy import io
from mitmproxy.addons import errorcheck
from mitmproxy.addons import save
from mitmproxy.addons import view
from mitmproxy.io import sqlite
from mitmproxy.test import taddons
from mitmproxy.test import tflow


def test_configure(tmp_path):
    sa = save.Save()
    with taddons.context(sa) as tctx:
        with pytest.raises(exceptions.OptionsError):
            tctx.configure(sa, save_stream_file=str(tmp_path))
        with pytest.raises(Exception, match="Invalid filter"):
            tctx.configure(
                sa, save_stream_file=str(tmp_path / "foo"), save_stream_filter="~~"
            )
        tctx.configure(sa, save_stream_filter="foo")
        assert sa.filt
        tctx.configure(sa, save_stream_filter=None)
        assert not sa.filt


def rd(p):
    with open(p, "rb") as f:
        x = io.FlowReader(f)
        return list(x.stream())


def test_tcp(tmp_path):
    sa = save.Save()
    with taddons.context(sa) as tctx:
        p = str(tmp_path / "foo")
        tctx.configure(sa, save_stream_file=p)

        tt = tflow.ttcpflow()
        sa.tcp_start(tt)
        sa.tcp_end(tt)

        tt = tflow.ttcpflow()
        sa.tcp_start(tt)
        sa.tcp_error(tt)

        tctx.configure(sa, save_stream_file=None)
        assert len(rd(p)) == 2


def test_udp(tmp_path):
    sa = save.Save()
    with taddons.context(sa) as tctx:
        p = str(tmp_path / "foo")
        tctx.configure(sa, save_stream_file=p)

        tt = tflow.tudpflow()
        sa.udp_start(tt)
        sa.udp_end(tt)

        tt = tflow.tudpflow()
        sa.udp_start(tt)
        sa.udp_error(tt)

        tctx.configure(sa, save_stream_file=None)
        assert len(rd(p)) == 2


def test_dns(tmp_path):
    sa = save.Save()
    with taddons.context(sa) as tctx:
        p = str(tmp_path / "foo")
        tctx.configure(sa, save_stream_file=p)

        f = tflow.tdnsflow(resp=True)
        sa.dns_request(f)
        sa.dns_response(f)
        tctx.configure(sa, save_stream_file=None)
        assert rd(p)[0].response

        tctx.configure(sa, save_stream_file="+" + p)
        f = tflow.tdnsflow(err=True)
        sa.dns_request(f)
        sa.dns_error(f)
        tctx.configure(sa, save_stream_file=None)
        assert rd(p)[1].error

        tctx.configure(sa, save_stream_file="+" + p)
        f = tflow.tdnsflow()
        sa.dns_request(f)
        tctx.configure(sa, save_stream_file=None)
        assert not rd(p)[2].response

        f = tflow.tdnsflow()
        sa.dns_response(f)
        assert len(rd(p)) == 3


def test_websocket(tmp_path):
    sa = save.Save()
    with taddons.context(sa) as tctx:
        p = str(tmp_path / "foo")
        tctx.configure(sa, save_stream_file=p)

        f = tflow.twebsocketflow()
        sa.request(f)
        sa.websocket_end(f)

        f = tflow.twebsocketflow()
        sa.request(f)
        sa.websocket_end(f)

        tctx.configure(sa, save_stream_file=None)
        assert len(rd(p)) == 2


def test_save_command(tmp_path):
    sa = save.Save()
    with taddons.context() as tctx:
        p = str(tmp_path / "foo")
        sa.save([tflow.tflow(resp=True)], p)
        assert len(rd(p)) == 1
        sa.save([tflow.tflow(resp=True)], p)
        assert len(rd(p)) == 1
        sa.save([tflow.tflow(resp=True)], "+" + p)
        assert len(rd(p)) == 2

        with pytest.raises(exceptions.CommandError):
            sa.save([tflow.tflow(resp=True)], str(tmp_path))

        v = view.View()
        tctx.master.addons.add(v)
        tctx.master.addons.add(sa)
        tctx.master.commands.execute("save.file @shown %s" % p)


def test_simple(tmp_path):
    sa = save.Save()
    with taddons.context(sa) as tctx:
        p = str(tmp_path / "foo")

        tctx.configure(sa, save_stream_file=p)

        f = tflow.tflow(resp=True)
        sa.request(f)
        sa.response(f)
        tctx.configure(sa, save_stream_file=None)
        assert rd(p)[0].response

        tctx.configure(sa, save_stream_file="+" + p)
        f = tflow.tflow(err=True)
        sa.request(f)
        sa.error(f)
        tctx.configure(sa, save_stream_file=None)
        assert rd(p)[1].error

        tctx.configure(sa, save_stream_file="+" + p)
        f = tflow.tflow()
        sa.request(f)
        tctx.configure(sa, save_stream_file=None)
        assert not rd(p)[2].response

        f = tflow.tflow()
        sa.response(f)
        assert len(rd(p)) == 3


def test_rotate_stream(tmp_path):
    sa = save.Save()
    with taddons.context(sa) as tctx:
        tctx.configure(sa, save_stream_file=str(tmp_path / "a.txt"))
        f1 = tflow.tflow(resp=True)
        f2 = tflow.tflow(resp=True)
        sa.request(f1)
        sa.response(f1)
        sa.request(f2)  # second request already started.
        tctx.configure(sa, save_stream_file=str(tmp_path / "b.txt"))
        sa.response(f2)
        sa.done()

        assert len(rd(tmp_path / "a.txt")) == 1
        assert len(rd(tmp_path / "b.txt")) == 1


def test_disk_full(tmp_path, monkeypatch, capsys):
    sa = save.Save()
    with taddons.context(sa) as tctx:
        tctx.configure(sa, save_stream_file=str(tmp_path / "foo.txt"))

        def _raise(*_):
            raise OSError("wat")

        monkeypatch.setattr(sa, "maybe_rotate_to_new_file", _raise)

        f = tflow.tflow(resp=True)
        sa.request(f)
        try:
            with pytest.raises(SystemExit):
                sa.response(f)
            assert "Error while writing" in capsys.readouterr().err
        finally:
            sa.stream.fo.close()


def test_sqlite_disabled(tmp_path, monkeypatch):
    def unexpected_writer(*args, **kwargs):
        pytest.fail("SQLite must not be activated by save_stream_file")

    monkeypatch.setattr(sqlite, "SQLiteFlowWriter", unexpected_writer)
    sa = save.Save()
    with taddons.context(sa) as tctx:
        assert tctx.options.save_sqlite_file is None
        tctx.configure(sa, save_stream_file=str(tmp_path / "flows.mitm"))
        f = tflow.tflow(resp=True)
        sa.request(f)
        sa.response(f)
        sa.done()
        assert rd(tmp_path / "flows.mitm")[0].get_state() == f.get_state()


@pytest.mark.parametrize(
    "factory,start,end",
    [
        (lambda: tflow.tflow(resp=True), "request", "response"),
        (lambda: tflow.tflow(err=True), "request", "error"),
        (lambda: tflow.tflow(resp=True, ws=True), "request", "websocket_end"),
        (tflow.ttcpflow, "tcp_start", "tcp_end"),
        (lambda: tflow.ttcpflow(err=True), "tcp_start", "tcp_error"),
        (tflow.tudpflow, "udp_start", "udp_end"),
        (lambda: tflow.tudpflow(err=True), "udp_start", "udp_error"),
        (lambda: tflow.tdnsflow(resp=True), "dns_request", "dns_response"),
        (lambda: tflow.tdnsflow(err=True), "dns_request", "dns_error"),
    ],
)
async def test_sqlite_terminal_hooks(tmp_path, factory, start, end):
    path = tmp_path / "flows.db"
    sa = save.Save()
    with taddons.context(sa) as tctx:
        tctx.configure(sa, save_sqlite_file=str(path))
        assert not path.exists()  # No worker before the running hook.
        await sa.running()
        f = factory()
        getattr(sa, start)(f)
        getattr(sa, end)(f)
        getattr(sa, end)(f)  # Repeated terminal events are idempotent in SQLite.
        await sa.done()
        with sqlite.SQLiteFlowReader(path) as reader:
            assert len(reader.newest(10)) == 1
            assert reader.get_flow(f.id).get_state() == f.get_state()


@pytest.mark.parametrize(
    "factory,start",
    [
        (tflow.tflow, "request"),
        (tflow.ttcpflow, "tcp_start"),
        (tflow.tudpflow, "udp_start"),
        (tflow.tdnsflow, "dns_request"),
    ],
)
async def test_sqlite_unfinished_on_shutdown(tmp_path, factory, start):
    path = tmp_path / "flows.db"
    sa = save.Save()
    with taddons.context(sa) as tctx:
        tctx.configure(sa, save_sqlite_file=str(path))
        await sa.running()
        f = factory()
        getattr(sa, start)(f)
        await sa.done()
        with sqlite.SQLiteFlowReader(path) as reader:
            assert reader.get_flow(f.id).get_state() == f.get_state()


async def test_sqlite_websocket_waits_for_end(tmp_path):
    path = tmp_path / "flows.db"
    sa = save.Save()
    with taddons.context(sa) as tctx:
        tctx.configure(sa, save_sqlite_file=str(path))
        await sa.running()
        f = tflow.tflow(resp=True, ws=True)
        sa.request(f)
        sa.response(f)
        with sqlite.SQLiteFlowReader(path) as reader:
            assert reader.newest(10) == []
        f.websocket.messages[-1].content = b"last websocket message"
        sa.websocket_end(f)
        await sa.done()
        with sqlite.SQLiteFlowReader(path) as reader:
            assert (
                reader.get_flow(f.id).websocket.messages[-1].content
                == b"last websocket message"
            )


async def test_sqlite_filter_and_independent_mitm(tmp_path):
    path = tmp_path / "flows.db"
    native = tmp_path / "flows.mitm"
    sa = save.Save()
    with taddons.context(sa) as tctx:
        tctx.configure(
            sa,
            save_sqlite_file=str(path),
            save_stream_file=str(native),
            save_stream_filter="~c 201",
        )
        await sa.running()
        accepted = tflow.tflow(resp=True)
        rejected = tflow.tflow(resp=True)
        accepted.response.status_code = 201
        for f in (accepted, rejected):
            sa.request(f)
            sa.response(f)
        tctx.configure(sa, save_stream_file=None)
        assert sa.sqlite is not None
        # The current filter is also applied to unfinished flows on disable.
        unfinished = tflow.tflow()
        sa.request(unfinished)
        tctx.configure(sa, save_stream_filter=None)
        tctx.configure(sa, save_sqlite_file=None)
        await sa.done()
        assert [f.id for f in rd(native)] == [accepted.id]
        with sqlite.SQLiteFlowReader(path) as reader:
            assert {r.flow_id for r in reader.newest(10)} == {
                accepted.id,
                unfinished.id,
            }


async def test_disable_sqlite_does_not_close_mitm(tmp_path):
    path = tmp_path / "flows.db"
    native = tmp_path / "flows.mitm"
    sa = save.Save()
    with taddons.context(sa) as tctx:
        tctx.configure(sa, save_sqlite_file=str(path), save_stream_file=str(native))
        await sa.running()
        pending = tflow.tflow()
        sa.request(pending)
        tctx.configure(sa, save_sqlite_file=None)
        assert sa.stream is not None
        pending.response = tflow.tresp()
        sa.response(pending)
        await sa.done()
        assert rd(native)[0].response is not None
        with sqlite.SQLiteFlowReader(path) as reader:
            assert reader.get_flow(pending.id).response is None


async def test_sqlite_configuration_switch_drains_in_order(tmp_path, caplog):
    path = tmp_path / "flows.db"
    other = tmp_path / "other.db"
    sa = save.Save()
    with taddons.context(sa) as tctx:
        tctx.configure(sa, save_sqlite_file=str(path))
        await sa.running()
        original = tflow.tflow(resp=True)
        sa.response(original)
        tctx.configure(sa, save_sqlite_file=str(other))
        middle = tflow.tflow(resp=True)
        sa.response(middle)
        tctx.configure(sa, save_sqlite_file=str(path))
        original.comment = "later duplicate"
        sa.response(original)
        newest = tflow.tflow(resp=True)
        sa.response(newest)
        await sa.done()
        await asyncio.sleep(0)
        assert original.id in caplog.text
        assert "conflicting state" in caplog.text
        with sqlite.SQLiteFlowReader(path) as reader:
            assert reader.get_flow(original.id).comment == ""
            assert [r.flow_id for r in reader.newest(10)] == [newest.id, original.id]
        with sqlite.SQLiteFlowReader(other) as reader:
            assert [r.flow_id for r in reader.newest(10)] == [middle.id]


async def test_sqlite_errors_are_visible_and_mitm_continues(
    tmp_path, monkeypatch, caplog
):
    path = tmp_path / "flows.db"
    native = tmp_path / "flows.mitm"
    sa = save.Save()
    with taddons.context(sa) as tctx:
        tctx.configure(sa, save_sqlite_file=str(path), save_stream_file=str(native))
        await sa.running()

        def full(*args):
            raise sqlite.WriterFull("queue is full")

        monkeypatch.setattr(sa.sqlite, "submit", full)
        rejected = [tflow.tflow(resp=True) for _ in range(3)]
        sa.response(rejected[0])
        for f in rejected[1:]:
            sa.request(f)
        await sa.done()
        await asyncio.sleep(0)  # Deliver worker error callbacks.
        assert len(rd(native)) == 3
        for f in rejected:
            assert f.id in caplog.text
        assert "queue is full" in caplog.text
        with sqlite.SQLiteFlowReader(path) as reader:
            assert reader.newest(10) == []


async def test_sqlite_startup_error(tmp_path, caplog):
    sa = save.Save()
    with taddons.context(sa) as tctx:
        tctx.configure(sa, save_sqlite_file=str(tmp_path))
        await sa.running()
        await sa.done()
        await asyncio.sleep(0)
        assert "SQLite" in caplog.text
        assert str(tmp_path) in caplog.text


async def test_sqlite_write_failure_is_reported(tmp_path, monkeypatch, caplog):
    def fail(*args):
        raise sqlite.StorageError("disk full")

    monkeypatch.setattr(sqlite.SQLiteFlowStore, "write", fail)
    sa = save.Save()
    with taddons.context(sa) as tctx:
        tctx.configure(sa, save_sqlite_file=str(tmp_path / "flows.db"))
        await sa.running()
        f = tflow.tflow(resp=True)
        sa.response(f)
        await sa.done()
        await asyncio.sleep(0)
        assert f.id in caplog.text
        assert "disk full" in caplog.text


def test_sqlite_error_after_loop_closed_is_visible(tmp_path, capsys):
    sa = save.Save()
    with taddons.context(sa) as tctx:
        sa._event_loop = tctx.master.event_loop
    result = Future()
    result.set_exception(sqlite.StorageError("late failure"))
    sa._sqlite_result(str(tmp_path / "flows.db"), "flow-id", result)
    assert "late failure" in capsys.readouterr().err


async def test_sqlite_closes_even_if_native_shutdown_fails(tmp_path, monkeypatch):
    sa = save.Save()
    with taddons.context(sa) as tctx:
        tctx.configure(
            sa,
            save_sqlite_file=str(tmp_path / "flows.db"),
            save_stream_file=str(tmp_path / "flows.mitm"),
        )
        await sa.running()
        writer = sa.sqlite
        f = tflow.tflow()
        sa.request(f)

        def fail(*args):
            raise OSError("native stream is full")

        monkeypatch.setattr(sa.stream, "add", fail)
        try:
            with pytest.raises(OSError, match="native stream is full"):
                sa.done()
            with pytest.raises(sqlite.StorageError, match="closed"):
                writer.submit(tflow.tflow())
        finally:
            await asyncio.wrap_future(writer.close())
            sa.stream.fo.close()
        with sqlite.SQLiteFlowReader(tmp_path / "flows.db") as reader:
            assert reader.get_flow(f.id).id == f.id


async def test_sqlite_early_response_waits_for_streamed_request(tmp_path):
    path = tmp_path / "flows.db"
    native = tmp_path / "flows.mitm"
    sa = save.Save()
    with taddons.context(sa) as tctx:
        tctx.configure(sa, save_sqlite_file=str(path), save_stream_file=str(native))
        await sa.running()
        try:
            f = tflow.tflow(resp=True)
            f.request.timestamp_end = None
            f.request.raw_content = None
            f.response.status_code = 413
            sa.response(f)
            f.request.raw_content = b"complete streamed upload"
            f.request.timestamp_end = f.response.timestamp_end + 1
            sa.request(f)
        finally:
            await sa.done()
        with sqlite.SQLiteFlowReader(path) as reader:
            assert (
                reader.get_flow(f.id).request.raw_content == b"complete streamed upload"
            )
            assert len(reader.newest(10)) == 1
        # Preserve Save's original native-stream behavior even in this edge case.
        assert [f.request.raw_content for f in rd(native)] == [
            None,
            b"complete streamed upload",
        ]


async def test_sqlite_upload_incomplete_on_disable(tmp_path):
    path = tmp_path / "flows.db"
    sa = save.Save()
    with taddons.context(sa) as tctx:
        tctx.configure(sa, save_sqlite_file=str(path))
        await sa.running()
        f = tflow.tflow()
        f.request.timestamp_end = None
        f.request.raw_content = None
        try:
            sa.requestheaders(f)
            tctx.configure(sa, save_sqlite_file=None)
        finally:
            await sa.done()
        with sqlite.SQLiteFlowReader(path) as reader:
            assert reader.get_flow(f.id).get_state() == f.get_state()


async def test_sqlite_early_response_upload_aborted_without_error_hook(tmp_path):
    path = tmp_path / "flows.db"
    sa = save.Save()
    with taddons.context(sa) as tctx:
        tctx.configure(sa, save_sqlite_file=str(path))
        await sa.running()
        f = tflow.tflow(resp=True)
        f.request.timestamp_end = None
        f.request.raw_content = None
        f.response.status_code = 413
        try:
            sa.response(f)
            # HTTP suppresses error hooks after a response, including stream resets.
            f.live = False

            async def wait_for_recording():
                with sqlite.SQLiteFlowReader(path) as reader:
                    while reader.get_flow(f.id) is None:
                        await asyncio.sleep(0.01)
                    return reader.get_flow(f.id)

            restored = await asyncio.wait_for(wait_for_recording(), 2)
            assert restored.get_state() == f.get_state()
            assert f not in sa.sqlite_active_flows
            assert f not in sa._sqlite_waiting_for_request
        finally:
            await sa.done()


@pytest.mark.parametrize("start_proxy", [False, True])
async def test_sqlite_buffers_flows_before_running(tmp_path, start_proxy):
    path = tmp_path / "flows.db"
    sa = save.Save()
    with taddons.context(sa) as tctx:
        tctx.configure(sa, save_sqlite_file=str(path))
        f = tflow.tflow(resp=True)
        sa.request(f)
        sa.response(f)
        assert not path.exists()
        if start_proxy:
            await sa.running()
        drain = sa.done()
        if drain is not None:
            await drain
        with sqlite.SQLiteFlowReader(path) as reader:
            assert reader.get_flow(f.id).get_state() == f.get_state()


async def test_sqlite_disabled_before_running(tmp_path, caplog):
    path = tmp_path / "flows.db"
    sa = save.Save()
    with taddons.context(sa) as tctx:
        tctx.configure(sa, save_sqlite_file=str(path))
        tctx.configure(sa, save_sqlite_file=None)
        await sa.running()
        await sa.done()
        assert not path.exists()
        assert "SQLite recording failed" not in caplog.text


@pytest.mark.parametrize("early_exit", ["shutdown", "setup_error"])
async def test_sqlite_master_exits_before_running(tmp_path, early_exit):
    path = tmp_path / "flows.db"
    sa = save.Save()
    ec = errorcheck.ErrorCheck()
    completed, unfinished = tflow.tflow(resp=True), tflow.tflow()

    class Proxyserver:
        async def setup_servers(self):
            sa.response(completed)
            sa.request(unfinished)
            if early_exit == "shutdown":
                tctx.master.shutdown()
            else:
                logging.error("injected setup error")

    with taddons.context(sa, ec, Proxyserver()) as tctx:
        tctx.configure(
            sa,
            save_sqlite_file=str(path),
            save_stream_file=str(tmp_path / "flows.mitm"),
        )
        try:
            if early_exit == "setup_error":
                with pytest.raises(SystemExit):
                    await tctx.master.run()
            else:
                await tctx.master.run()
            with sqlite.SQLiteFlowReader(path) as reader:
                assert (
                    reader.get_flow(completed.id).get_state() == completed.get_state()
                )
                assert (
                    reader.get_flow(unfinished.id).get_state() == unfinished.get_state()
                )
            # Early SQLite cleanup must not change native-stream shutdown behavior.
            assert sa.stream is not None and unfinished in sa.active_flows
            assert [f.id for f in rd(tmp_path / "flows.mitm")] == [completed.id]
        finally:
            await tctx.master.done()
            ec.finish()


async def test_sqlite_startup_overload_does_not_abort_proxy(tmp_path, caplog):
    path = tmp_path / "flows.db"
    sa = save.Save()
    ec = errorcheck.ErrorCheck()
    flows = [tflow.tflow(resp=True) for _ in range(65)]

    class Proxyserver:
        async def setup_servers(self):
            for f in flows:
                sa.response(f)

        def running(self):
            tctx.master.shutdown()

    with taddons.context(sa, ec, Proxyserver()) as tctx:
        tctx.configure(
            sa,
            save_sqlite_file=str(path),
            save_stream_file=str(tmp_path / "flows.mitm"),
        )
        try:
            await tctx.master.run()
            assert not ec.logger.has_errored
            assert flows[-1].id in caplog.text
            assert "queue is full" in caplog.text
            with sqlite.SQLiteFlowReader(path) as reader:
                assert {row.flow_id for row in reader.newest(100)} == {
                    f.id for f in flows[:-1]
                }
                assert reader.get_flow(flows[-1].id) is None
            assert [f.id for f in rd(tmp_path / "flows.mitm")] == [f.id for f in flows]
        finally:
            await tctx.master.done()
            ec.finish()
