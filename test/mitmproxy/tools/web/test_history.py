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


async def test_cancelled_index_waiter_releases_worker_before_lock_owner(tmp_path):
    path = tmp_path / "history.db"
    with sqlite.SQLiteFlowStore(path):
        pass
    service = history.HistoryReader()
    cancel = threading.Event()
    entered = threading.Event()
    original = history.snapshot

    def summarize(*args):
        result = original(*args)
        entered.set()
        return result

    service._index_lock.acquire()
    with mock.patch.object(history, "snapshot", summarize):
        task = asyncio.create_task(
            service.page(
                str(path), history.HistoryQuery.parse({}, now=3000), [], cancel
            )
        )
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
