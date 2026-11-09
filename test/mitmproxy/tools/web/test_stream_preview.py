import asyncio
import gc
import weakref

import pytest

from mitmproxy import flow as flow_module
from mitmproxy import http
from mitmproxy import options
from mitmproxy.addons import view
from mitmproxy.test import tflow
from mitmproxy.tools.web import stream_preview


class Sink:
    def __init__(self):
        self.pending = None
        self.queued = 0

    def queue_preview(self, factory):
        self.pending = factory
        self.queued += 1

    def clear_preview(self):
        self.pending = None

    def drain(self):
        factory, self.pending = self.pending, None
        return factory() if factory else None


@pytest.fixture
def setup():
    v = view.View()
    opts = options.Options()
    opts.add_option("web_stream_preview_max_bytes", int, 8, "preview limit")
    manager = stream_preview.StreamPreviewManager(v, opts)
    flow = tflow.tflow(resp=True)
    flow.response.headers["content-type"] = "text/plain; charset=utf-8"
    v.add([flow])
    manager.responseheaders(flow)
    yield manager, v, opts, flow
    manager.done()


@pytest.mark.asyncio
async def test_subscription_delta_and_gap(setup):
    manager, _, _, flow = setup
    sink = Sink()
    flow.response._observe_stream_chunk(b"old")
    manager.subscribe(sink, flow.id, 1)
    first = sink.drain()
    assert first["reset"] and first["missing"] and first["received"] == 3
    flow.response._observe_stream_chunk(b"hello")
    await asyncio.sleep(0.22)
    assert sink.drain()["text"] == "hello"
    flow.response._observe_stream_chunk(b"abcdefghijkl")
    await asyncio.sleep(0.22)
    gap = sink.drain()
    assert gap["text"] == "efghijkl"
    assert (gap["start"], gap["end"], gap["dropped"], gap["reset"]) == (
        12,
        20,
        12,
        True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("content_type", ["text/plain; charset=utf-8", None])
async def test_utf8_join_coalescing_and_disconnect(setup, content_type):
    manager, _, _, flow = setup
    if content_type is None:
        del flow.response.headers["content-type"]
    manager.responseheaders(flow)
    a, b = Sink(), Sink()
    original = flow.response.raw_content
    stream = flow.response.stream
    manager.subscribe(a, flow.id, 2)
    assert a.drain()["status"] == "waiting"
    flow.response._observe_stream_chunk(b"A\xe2")
    flow.response._observe_stream_chunk(b"\x82")
    await asyncio.sleep(0.22)
    assert a.queued == 2
    assert a.drain()["text"] == "A"
    manager.subscribe(b, flow.id, 3)
    assert b.drain()["missing"]
    flow.response._observe_stream_chunk(b"\xacZ")
    await asyncio.sleep(0.22)
    assert a.drain()["text"] == "\u20acZ"
    assert b.drain()["text"] == "Z"
    manager.unsubscribe(a)
    manager.unsubscribe(b)
    assert flow.response.raw_content == original and flow.response.stream == stream
    assert not manager.active[flow.id].blocks


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "header,value,body,reason",
    [
        ("content-type", "application/octet-stream", b"\x00secret", "binary"),
        ("content-type", "text/plain; charset=latin-1", b"secret", "charset"),
        (
            "content-type",
            "application/octet-stream; charset=utf-16",
            b"secret",
            "charset",
        ),
        ("content-encoding", "gzip", b"secret", "encoding"),
        ("content-encoding", "br", b"secret", "encoding"),
    ],
)
async def test_unsupported(setup, header, value, body, reason):
    manager, _, _, flow = setup
    flow.response.headers[header] = value
    manager.responseheaders(flow)
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    sink.drain()
    flow.response._observe_stream_chunk(body)
    await asyncio.sleep(0.22)
    payload = sink.drain()
    assert payload["received"] == len(body) and payload["text"] == ""
    assert reason in payload["reason"].lower()
    assert payload["start"] == 0 and payload["end"] == len(body)
    assert payload["dropped"] == 0 and not payload["missing"]
    assert not manager.active[flow.id].blocks


@pytest.mark.asyncio
async def test_invalid_utf8_and_terminal(setup):
    manager, _, _, flow = setup
    sink = Sink()
    manager.subscribe(sink, flow.id, 7)
    sink.drain()
    flow.response._observe_stream_chunk(b"\xff")
    await asyncio.sleep(0.22)
    assert "UTF-8" in sink.drain()["reason"]
    flow.response.raw_content = None
    manager.response(flow)
    result = sink.drain()
    assert result["status"] == "finished" and not result["stored"]
    assert flow.response._stream_observer is None
    assert not manager.active and not manager.clients


@pytest.mark.asyncio
async def test_removal_replacement_and_options(setup):
    manager, v, opts, flow = setup
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    sink.drain()
    flow.response._observe_stream_chunk(b"abcdefgh")
    opts.web_stream_preview_max_bytes = 3
    manager.configure({"web_stream_preview_max_bytes"})
    result = sink.drain()
    assert result["text"] == "fgh" and result["dropped"] == 5
    opts.web_stream_preview_max_bytes = 0
    manager.configure({"web_stream_preview_max_bytes"})
    result = sink.drain()
    assert result["status"] == "aborted"
    assert result["reason"] == "Live previews disabled" and result["text"] == ""
    assert not manager.active and not manager.clients
    assert flow.response._stream_observer is None
    manager.responseheaders(flow)
    manager.subscribe(sink, flow.id, 2)
    assert sink.drain()["reason"] == "Live previews disabled"
    assert flow.response._stream_observer is None
    opts.web_stream_preview_max_bytes = 3
    manager.configure({"web_stream_preview_max_bytes"})
    manager.responseheaders(flow)
    manager.subscribe(sink, flow.id, 3)
    v.set_filter(lambda f: False)
    assert flow.response._stream_observer is not None
    v.clear()
    assert flow.response._stream_observer is None and sink.drain() is None


def test_validation_and_error(setup):
    manager, v, _, flow = setup
    sink = Sink()
    for flow_id, token in [(flow.id, True), (3, 1)]:
        with pytest.raises(ValueError):
            manager.subscribe(sink, flow_id, token)
    manager.subscribe(sink, "missing", 1)
    assert sink.drain()["reason"] == "Unknown HTTP response"
    other = tflow.tflow()
    v.add([other])
    manager.subscribe(sink, other.id, 1)
    assert sink.drain()["reason"] == "Unknown HTTP response"
    manager.subscribe(sink, flow.id, 1)
    manager.error(flow)
    assert sink.drain()["status"] == "aborted"
    manager.subscribe(sink, None, 2)


@pytest.mark.asyncio
async def test_pending_terminal_replacement_and_removal(setup):
    manager, v, _, flow = setup
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    flow.response._observe_stream_chunk(b"")
    flow.response._observe_stream_chunk(b"abc")
    manager.response(flow)
    final = sink.pending
    assert flow.response._stream_observer is None
    assert not manager.active and not manager.clients
    manager.responseheaders(flow)
    assert final() is None
    manager.subscribe(sink, flow.id, 2)
    flow.response._observe_stream_chunk(b"next")
    manager.unsubscribe(sink)
    assert not manager.active[flow.id].blocks
    manager.subscribe(sink, flow.id, 3)
    v.remove([flow])
    assert flow.response._stream_observer is None
    assert sink.drain() is None
    manager.response(flow)


@pytest.mark.asyncio
async def test_final_stored_and_done_invalidate(setup):
    manager, _, _, flow = setup
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    flow.response._observe_stream_chunk(b"abc")
    manager.response(flow)
    result = sink.drain()
    assert (
        result["stored"] and result["status"] == "finished" and result["text"] == "abc"
    )
    manager.subscribe(sink, flow.id, 2)
    pending = sink.pending
    manager.done()
    assert pending() is None


@pytest.mark.asyncio
async def test_ascii_gap_and_large_tail(setup):
    manager, _, opts, flow = setup
    opts.web_stream_preview_max_bytes = 20000
    flow.response.headers["content-type"] = "application/problem+json; charset=ascii"
    manager.responseheaders(flow)
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    sink.drain()
    flow.response._observe_stream_chunk(b"a" * 50000)
    assert sum(map(len, manager.active[flow.id].blocks)) == 20000
    await asyncio.sleep(0.22)
    result = sink.drain()
    assert result["start"] == 30000 and len(result["text"]) == 20000
    flow.response._observe_stream_chunk(b"\xc3\xa9")
    await asyncio.sleep(0.22)
    assert "ASCII" in sink.drain()["reason"]


def test_no_response_headers(setup):
    manager, _, _, flow = setup
    flow.response = None
    manager.responseheaders(flow)
    assert not manager.active


def test_abandoned_final_does_not_retain_flow():
    v = view.View()
    opts = options.Options()
    opts.add_option("web_stream_preview_max_bytes", int, 8, "preview limit")
    manager = stream_preview.StreamPreviewManager(v, opts)
    flow = tflow.tflow(resp=True)
    v.add([flow])
    manager.responseheaders(flow)
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    manager.response(flow)
    ref = weakref.ref(flow)
    v.remove([flow])
    del flow
    gc.collect()
    assert ref() is None
    assert sink.drain() is None
    manager.done()


@pytest.mark.asyncio
async def test_disabled_headers_and_runtime_disable(setup):
    manager, v, opts, flow = setup
    updates = []

    def on_update(flow):
        updates.append(flow)

    v.sig_view_update.connect(on_update)
    a, b = Sink(), Sink()
    manager.subscribe(a, flow.id, 1)
    manager.subscribe(b, flow.id, 2)
    a.drain()
    b.drain()
    flow.response._observe_stream_chunk(b"private")
    old_observer = flow.response._stream_observer
    opts.web_stream_preview_max_bytes = 0
    manager.configure({"web_stream_preview_max_bytes"})
    assert not manager.active and not manager.clients
    assert flow.response._stream_observer is None and updates == [flow]
    for sink in (a, b):
        payload = sink.drain()
        assert payload["status"] == "aborted"
        assert payload["reason"] == "Live previews disabled"
        assert payload["text"] == "" and payload["max_bytes"] == 0
    old_observer(b"later")
    await asyncio.sleep(0.22)
    assert a.drain() is None and b.drain() is None
    manager.responseheaders(flow)
    assert flow.response._stream_observer is None and not manager.active
    assert updates == [flow, flow]
    manager.subscribe(a, flow.id, 9)
    assert a.drain()["reason"] == "Live previews disabled"
    assert not manager.active and not manager.clients


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body,text,reason",
    [
        (b"data: ok\n\n", "data: ok\n\n", None),
        (b"\x00binary", "", "Binary content: NUL byte"),
    ],
)
async def test_missing_content_type_is_detected_from_body(setup, body, text, reason):
    manager, _, opts, flow = setup
    opts.web_stream_preview_max_bytes = 64
    del flow.response.headers["content-type"]
    manager.responseheaders(flow)
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    sink.drain()
    flow.response._observe_stream_chunk(body)
    await asyncio.sleep(0.22)
    result = sink.drain()
    assert result["text"] == text and result["reason"] == reason
    assert result["received"] == len(body)
    assert result["dropped"] == 0 and not result["missing"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "content_type",
    [
        "text/event-stream",
        "application/json",
        "application/octet-stream",
        "application/x-ndjson",
        "image/png",
        "nonstandard",
    ],
)
async def test_text_is_visible_before_completion_regardless_of_mime(
    setup, content_type
):
    manager, _, opts, flow = setup
    opts.web_stream_preview_max_bytes = 64
    flow.response.headers["content-type"] = content_type
    manager.responseheaders(flow)
    original = flow.get_state()
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    assert sink.drain()["reason"] is None
    flow.response._observe_stream_chunk(b"d")
    await asyncio.sleep(0.22)
    first = sink.drain()
    assert first["text"] == "d" and first["status"] == "streaming"
    assert first["received"] == 1 and first["reason"] is None
    flow.response._observe_stream_chunk(b"ata: \xc3\xa9\r\n\r\n")
    await asyncio.sleep(0.22)
    second = sink.drain()
    assert second["text"] == "ata: \u00e9\r\n\r\n" and second["reason"] is None
    assert (second["start"], second["end"], second["dropped"]) == (1, 12, 0)
    assert flow.get_state() == original


@pytest.mark.asyncio
@pytest.mark.parametrize("content_type", ["application/octet-stream", "nonstandard"])
@pytest.mark.parametrize(
    "body,reason",
    [
        (b"a\x00b", "NUL byte"),
        (b"\xff", "Invalid UTF-8"),
        (b"\xc3(", "Invalid UTF-8"),
        (b"\x01abc", "control characters"),
        (b"\x7f", "control characters"),
        (b"\xc2\x85", "control characters"),
        (b"\x1b[31mred", "control characters"),
        (b"\x00" + b"a" * 65536, "NUL byte"),
    ],
)
async def test_unknown_type_rejects_binary_without_false_overflow(
    setup, content_type, body, reason
):
    manager, _, _, flow = setup
    flow.response.headers["content-type"] = content_type
    manager.responseheaders(flow)
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    sink.drain()
    flow.response._observe_stream_chunk(body)
    await asyncio.sleep(0.22)
    result = sink.drain()
    assert reason in result["reason"] and result["text"] == ""
    assert result["received"] == len(body)
    assert result["dropped"] == 0 and not result["missing"]
    assert not manager.active[flow.id].blocks


@pytest.mark.asyncio
async def test_unknown_type_stops_if_utf8_becomes_invalid(setup):
    manager, _, _, flow = setup
    del flow.response.headers["content-type"]
    manager.responseheaders(flow)
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    sink.drain()
    flow.response._observe_stream_chunk(b"text\r\n")
    await asyncio.sleep(0.22)
    assert sink.drain()["text"] == "text\r\n"
    flow.response._observe_stream_chunk(b"\xff")
    await asyncio.sleep(0.22)
    result = sink.drain()
    assert result["reason"] == "Invalid UTF-8" and result["text"] == ""
    assert (result["start"], result["end"], result["dropped"]) == (6, 7, 0)
    assert not result["reset"] and not result["missing"]
    flow.response._observe_stream_chunk(b"later")
    manager.response(flow)
    result = sink.drain()
    assert result["received"] == 12 and result["text"] == ""
    assert result["reason"] == "Invalid UTF-8" and result["dropped"] == 0


@pytest.mark.asyncio
async def test_unknown_type_detection_is_bounded_and_resumes_after_unwatched_gap(setup):
    manager, _, _, flow = setup
    del flow.response.headers["content-type"]
    manager.responseheaders(flow)
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    sink.drain()
    flow.response._observe_stream_chunk(b"a" * 100000 + b"\xe2")
    state = manager.active[flow.id]
    assert state.size == 8 and sum(map(len, state.blocks)) == 8
    await asyncio.sleep(0.22)
    result = sink.drain()
    assert result["text"] == "aaaaaaa" and result["reason"] is None
    assert result["dropped"] == 99993 and result["missing"]
    manager.unsubscribe(sink)
    flow.response._observe_stream_chunk(b"\x82\xac skipped\xe2")
    assert not state.blocks
    manager.subscribe(sink, flow.id, 2)
    assert sink.drain()["missing"]
    flow.response._observe_stream_chunk(b"\x82\xacnew")
    await asyncio.sleep(0.22)
    result = sink.drain()
    assert result["text"] == "new" and result["reason"] is None


@pytest.mark.asyncio
async def test_unknown_type_split_utf8_and_incomplete_final_sequence(setup):
    manager, _, _, flow = setup
    del flow.response.headers["content-type"]
    manager.responseheaders(flow)
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    sink.drain()
    for chunk, text in [(b"A\xe2", "A"), (b"\x82", ""), (b"\xac\r\n", "\u20ac\r\n")]:
        flow.response._observe_stream_chunk(chunk)
        await asyncio.sleep(0.22)
        result = sink.drain()
        assert result["text"] == text and result["reason"] is None
    assert result["received"] == 6 and not result["missing"]
    flow.response._observe_stream_chunk(b"\xe2")
    await asyncio.sleep(0.22)
    assert sink.drain()["reason"] is None
    manager.response(flow)
    result = sink.drain()
    assert "UTF-8" in result["reason"] and result["received"] == 7


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "charset,chunks,reason,text",
    [
        ("ascii", [b"\x80hello"], "Invalid ASCII", ""),
        ("utf-8", [b"\x80\x80\x80\x80hello"], "Invalid UTF-8", ""),
        ("utf-8", [b"\x80", b"\x80", b"\x80", b"\x80hello"], "Invalid UTF-8", ""),
        ("utf-8", [b"\x80", b"\x80", b"\x80hello"], None, "hello"),
    ],
)
async def test_unknown_type_late_join_only_skips_a_plausible_utf8_boundary(
    setup, charset, chunks, reason, text
):
    manager, _, _, flow = setup
    flow.response.headers["content-type"] = (
        f"application/octet-stream; charset={charset}"
    )
    manager.responseheaders(flow)
    flow.response._observe_stream_chunk(b"\xf0")
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    assert sink.drain()["missing"]
    for chunk in chunks:
        flow.response._observe_stream_chunk(chunk)
    await asyncio.sleep(0.22)
    result = sink.drain()
    assert result["reason"] == reason and result["text"] == text


@pytest.mark.asyncio
@pytest.mark.parametrize("hook", ["response", "error"])
async def test_unknown_type_finalizes_utf8_even_if_cap_hides_incomplete_sequence(
    setup, hook
):
    manager, _, opts, flow = setup
    opts.web_stream_preview_max_bytes = 1
    del flow.response.headers["content-type"]
    manager.responseheaders(flow)
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    sink.drain()
    flow.response._observe_stream_chunk(b"\xe2\x82")
    getattr(manager, hook)(flow)
    result = sink.drain()
    assert "Incomplete UTF-8" in result["reason"]
    assert result["received"] == 2 and result["text"] == ""


@pytest.mark.asyncio
async def test_unknown_type_retains_real_overflow_count_after_binary_detection(setup):
    manager, _, _, flow = setup
    del flow.response.headers["content-type"]
    manager.responseheaders(flow)
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    sink.drain()
    flow.response._observe_stream_chunk(b"abcdefghij")
    flow.response._observe_stream_chunk(b"\x00")
    await asyncio.sleep(0.22)
    result = sink.drain()
    assert result["reason"] == "Binary content: NUL byte"
    assert result["received"] == 11 and result["text"] == ""
    assert result["dropped"] == 2 and result["missing"]
    flow.response._observe_stream_chunk(b"more")
    manager.response(flow)
    result = sink.drain()
    assert result["received"] == 15 and result["dropped"] == 2


@pytest.mark.asyncio
async def test_dropped_offset_advances_for_fast_reader(setup):
    manager, _, _, flow = setup
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    sink.drain()
    flow.response._observe_stream_chunk(b"abcdefgh")
    await asyncio.sleep(0.22)
    assert sink.drain()["text"] == "abcdefgh"
    flow.response._observe_stream_chunk(b"ij")
    await asyncio.sleep(0.22)
    result = sink.drain()
    assert (result["start"], result["end"], result["dropped"]) == (8, 10, 2)
    assert result["missing"] and not result["reset"] and result["text"] == "ij"


def test_aborted_reason_includes_flow_error(setup):
    manager, _, _, flow = setup
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    flow.error = flow_module.Error("upstream disconnected")
    manager.error(flow)
    payload = sink.drain()
    assert payload["status"] == "aborted"
    assert "upstream disconnected" in payload["reason"]


def test_disable_cleans_replaced_response_and_pending_terminal(setup):
    manager, _, opts, flow = setup
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    old_response = flow.response
    flow.response = http.Response.make(200, b"new", {"content-type": "text/plain"})
    opts.web_stream_preview_max_bytes = 0
    manager.configure({"web_stream_preview_max_bytes"})
    assert not manager.active and not manager.clients
    assert old_response._stream_observer is None
    assert sink.drain() is None


@pytest.mark.asyncio
async def test_disable_invalidates_undrained_finished_text(setup):
    manager, _, opts, flow = setup
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    flow.response._observe_stream_chunk(b"private")
    manager.response(flow)
    opts.web_stream_preview_max_bytes = 0
    manager.configure({"web_stream_preview_max_bytes"})
    assert sink.drain() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("hook", ["response", "error"])
async def test_terminal_hook_discards_replaced_response(hook):
    v = view.View()
    opts = options.Options()
    opts.add_option("web_stream_preview_max_bytes", int, 8, "preview limit")
    manager = stream_preview.StreamPreviewManager(v, opts)
    flow = tflow.tflow(resp=True)
    flow.response.headers["content-type"] = "text/plain"
    v.add([flow])
    manager.responseheaders(flow)
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    sink.drain()
    old = flow.response
    old._observe_stream_chunk(b"secret")
    state = manager.active[flow.id]
    flow.response = http.Response.make(200, b"replacement")
    getattr(manager, hook)(flow)
    assert not manager.active and not manager.clients
    assert old._stream_observer is None and not state.blocks
    assert state.timer is None and sink.drain() is None
    await asyncio.sleep(0.22)
    assert sink.drain() is None
    flow_ref, old_ref = weakref.ref(flow), weakref.ref(old)
    v.remove([flow])
    del state, flow, old
    gc.collect()
    assert flow_ref() is None and old_ref() is None
    manager.done()


@pytest.mark.asyncio
async def test_shrink_finished_undrained_snapshot(setup):
    manager, _, opts, flow = setup
    sink = Sink()
    manager.subscribe(sink, flow.id, 1)
    sink.drain()
    flow.response._observe_stream_chunk(b"abcdefgh")
    manager.response(flow)
    state = next(iter(manager.finished))
    opts.web_stream_preview_max_bytes = 3
    manager.configure({"web_stream_preview_max_bytes"})
    assert state.size == 3
    payload = sink.drain()
    assert payload["status"] == "finished" and payload["text"] == "fgh"
    assert (payload["start"], payload["end"], payload["dropped"]) == (5, 8, 5)
    assert payload["reset"] and payload["missing"] and payload["max_bytes"] == 3
