"""Bounded, opt-in previews of response bytes observed on the wire."""

import asyncio
import codecs
import weakref
from collections import deque
from dataclasses import dataclass
from dataclasses import field
from email.message import Message
from functools import partial
from typing import Callable
from typing import Protocol

from mitmproxy import http
from mitmproxy.addons.view import View
from mitmproxy.options import Options


class Sink(Protocol):
    def queue_preview(self, factory: Callable[[], dict | None]) -> None: ...

    def clear_preview(self) -> None: ...


@dataclass(eq=False)
class Subscription:
    client: Sink
    token: int
    cursor: int
    dropped: int
    missing: bool
    seq: int = 0
    reset: bool = True
    aligning: bool = False
    decoder: codecs.IncrementalDecoder = field(
        default_factory=lambda: codecs.getincrementaldecoder("utf-8")("strict")
    )
    reason: str | None = None


@dataclass(eq=False)
class Preview:
    flow: http.HTTPFlow
    response: http.Response
    reason: str | None
    encoding: str = "utf-8"
    received: int = 0
    status: str = "waiting"
    terminal_reason: str | None = None
    stored: bool = False
    blocks: deque[bytearray] = field(default_factory=deque)
    size: int = 0
    subscribers: set[Subscription] = field(default_factory=set)
    timer: asyncio.TimerHandle | None = None
    valid: bool = True
    flow_id: str = field(init=False)
    flow_ref: weakref.ReferenceType = field(init=False)
    response_ref: weakref.ReferenceType = field(init=False)

    def __post_init__(self) -> None:
        self.flow_id = self.flow.id
        self.flow_ref = weakref.ref(self.flow)
        self.response_ref = weakref.ref(self.response)


class StreamPreviewManager:
    def __init__(self, view: View, options: Options):
        self.view = view
        self.options = options
        self.active: dict[str, Preview] = {}
        self.clients: dict[Sink, tuple[Preview, Subscription]] = {}
        self.finished: weakref.WeakSet[Preview] = weakref.WeakSet()
        view.sig_store_remove.connect(self._remove)
        view.sig_store_refresh.connect(self._refresh)

    def responseheaders(self, flow: http.HTTPFlow) -> None:
        for previous in tuple(self.finished):
            if previous.flow_id == flow.id:
                previous.valid = False
        old = self.active.get(flow.id)
        if old:
            self._discard(old)
        if flow.response is None:
            return
        if not self.options.web_stream_preview_max_bytes:
            self.view.update([flow])
            return
        response = flow.response
        reason = None
        content_type = Message()
        content_type["content-type"] = response.headers.get("content-type", "")
        media = content_type.get_content_type().lower()
        charset = content_type.get_content_charset() or "utf-8"
        if response.headers.get("content-encoding", "identity").lower().strip() not in (
            "",
            "identity",
        ):
            reason = "Unsupported content encoding"
        elif not response.headers.get("content-type", "").strip() or not (
            media.startswith("text/")
            or media == "application/json"
            or (media.startswith("application/") and media.endswith("+json"))
        ):
            reason = "Unsupported content type"
        elif charset.lower() not in ("utf-8", "utf8", "ascii", "us-ascii"):
            reason = "Unsupported charset"
        state = Preview(
            flow,
            response,
            reason,
            encoding="ascii" if charset.lower() in ("ascii", "us-ascii") else "utf-8",
        )
        self.active[flow.id] = state
        response._stream_observer = lambda chunk: self._observe(state, chunk)
        self.view.update([flow])

    def _observe(self, state: Preview, chunk: bytes) -> None:
        if not chunk or self.active.get(state.flow_id) is not state:
            return
        state.received += len(chunk)
        state.status = "streaming"
        cap = self.options.web_stream_preview_max_bytes
        if state.subscribers and not state.reason and cap:
            chunk = chunk[-cap:]
            pos = 0
            while pos < len(chunk):
                if not state.blocks or len(state.blocks[-1]) == 16384:
                    state.blocks.append(bytearray())
                n = min(16384 - len(state.blocks[-1]), len(chunk) - pos)
                state.blocks[-1].extend(chunk[pos : pos + n])
                state.size += n
                pos += n
            self._trim(state, cap)
        if state.subscribers and state.timer is None:
            state.timer = asyncio.get_running_loop().call_later(
                0.2, self._notify, state
            )

    def _trim(self, state: Preview, cap: int) -> None:
        excess = max(0, state.size - cap)
        while excess:
            block = state.blocks[0]
            n = min(excess, len(block))
            if n == len(block):
                state.blocks.popleft()
            else:
                del block[:n]
            state.size -= n
            excess -= n

    def _notify(self, state: Preview) -> None:
        state.timer = None
        for sub in tuple(state.subscribers):
            sub.client.queue_preview(partial(self._payload, state, sub))

    def _payload(self, state: Preview, sub: Subscription) -> dict | None:
        flow = self.view.get_by_id(state.flow_id)
        if (
            not state.valid
            or flow is not state.flow_ref()
            or flow is None
            or flow.response is not state.response_ref()
        ):
            return None
        start = max(sub.cursor, state.received - state.size)
        gap = start > sub.cursor
        sub.dropped = max(sub.dropped, state.received - state.size)
        sub.missing |= sub.dropped > 0
        skip = start - (state.received - state.size)
        parts = []
        for block in state.blocks:
            if skip >= len(block):
                skip -= len(block)
            else:
                parts.append(bytes(block[skip:]))
                skip = 0
        data = b"".join(parts)
        if gap:
            sub.decoder = codecs.getincrementaldecoder(state.encoding)("strict")
            sub.aligning = True
        if sub.aligning:
            while data and data[0] & 0xC0 == 0x80:
                data = data[1:]
                start += 1
            sub.dropped = max(sub.dropped, start)
            if data:
                sub.aligning = False
        reason = state.reason or sub.reason
        text = ""
        if not reason:
            try:
                text = sub.decoder.decode(
                    data, final=state.status in ("finished", "aborted")
                )
            except UnicodeDecodeError:
                sub.reason = reason = f"Invalid {state.encoding.upper()}"
        if state.terminal_reason:
            reason = (
                f"{reason}: {state.terminal_reason}"
                if reason
                else state.terminal_reason
            )
        sub.seq += 1
        payload = dict(
            flow_id=state.flow_id,
            subscription=sub.token,
            seq=sub.seq,
            start=start,
            end=state.received,
            received=state.received,
            dropped=sub.dropped,
            missing=sub.missing,
            status=state.status,
            reason=reason,
            text=text,
            reset=sub.reset or gap,
            stored=state.stored,
            max_bytes=self.options.web_stream_preview_max_bytes,
        )
        sub.cursor = state.received
        sub.reset = False
        return payload

    def subscribe(self, client: Sink, flow_id: str | None, subscription: int) -> None:
        if not isinstance(subscription, int) or isinstance(subscription, bool):
            raise ValueError("Invalid subscription")
        if flow_id is None:
            self.unsubscribe(client)
            return
        if not isinstance(flow_id, str):
            raise ValueError("Invalid flow id")
        self.unsubscribe(client)
        flow = self.view.get_by_id(flow_id)
        if not isinstance(flow, http.HTTPFlow) or flow.response is None:
            client.queue_preview(
                lambda: dict(
                    flow_id=flow_id,
                    subscription=subscription,
                    seq=1,
                    start=0,
                    end=0,
                    received=0,
                    dropped=0,
                    missing=True,
                    status="aborted",
                    reason="Unknown HTTP response",
                    text="",
                    reset=True,
                    stored=False,
                    max_bytes=self.options.web_stream_preview_max_bytes,
                )
            )
            return
        state = self.active.get(flow_id)
        if state is None or state.response is not flow.response:
            disabled = not self.options.web_stream_preview_max_bytes
            state = Preview(
                flow,
                flow.response,
                "Live previews disabled" if disabled else None,
                status="aborted" if disabled else "finished",
                stored=flow.response.raw_content is not None,
            )
            self.finished.add(state)
            state.flow = weakref.proxy(flow)
            state.response = weakref.proxy(flow.response)
        sub = Subscription(
            client,
            subscription,
            state.received,
            state.received,
            state.received > 0,
            aligning=state.received > 0,
        )
        sub.decoder = codecs.getincrementaldecoder(state.encoding)("strict")
        if self.active.get(flow_id) is state:
            state.subscribers.add(sub)
            self.clients[client] = state, sub
        client.queue_preview(lambda: self._payload(state, sub))

    def unsubscribe(self, client: Sink) -> None:
        client.clear_preview()
        entry = self.clients.pop(client, None)
        if entry:
            state, sub = entry
            state.subscribers.discard(sub)
            if not state.subscribers:
                if state.timer:
                    state.timer.cancel()
                    state.timer = None
                self._trim(state, 0)

    def _finish(self, flow: http.HTTPFlow, status: str) -> None:
        state = self.active.get(flow.id)
        if not state:
            return
        if state.response is not flow.response:
            self._discard(state)
            return
        self.active.pop(flow.id)
        state.response._stream_observer = None
        state.status = status
        if status == "aborted" and flow.error:
            state.terminal_reason = flow.error.msg
        self.finished.add(state)
        state.stored = state.response.raw_content is not None
        if state.timer:
            state.timer.cancel()
            state.timer = None
        self._notify(state)
        for sub in state.subscribers:
            self.clients.pop(sub.client, None)
        state.subscribers.clear()
        state.flow = weakref.proxy(flow)
        state.response = weakref.proxy(flow.response)

    def response(self, flow: http.HTTPFlow) -> None:
        self._finish(flow, "finished")

    def error(self, flow: http.HTTPFlow) -> None:
        self._finish(flow, "aborted")

    def _discard(self, state: Preview) -> None:
        self.active.pop(state.flow.id, None)
        state.response._stream_observer = None
        for sub in tuple(state.subscribers):
            self.unsubscribe(sub.client)
        self._trim(state, 0)

    def _remove(self, flow) -> None:
        state = self.active.get(flow.id)
        if state:
            self._discard(state)

    def _refresh(self) -> None:
        for state in tuple(self.active.values()):
            if self.view.get_by_id(state.flow.id) is not state.flow:
                self._discard(state)

    def configure(self, updated: set[str]) -> None:
        if "web_stream_preview_max_bytes" in updated:
            for state in self.finished:
                if not self.options.web_stream_preview_max_bytes:
                    state.valid = False
                self._trim(state, self.options.web_stream_preview_max_bytes)
            for state in tuple(self.active.values()):
                if not self.options.web_stream_preview_max_bytes:
                    flow = state.flow
                    if flow.response is not state.response:
                        self._discard(state)
                    else:
                        state.reason = "Live previews disabled"
                        self._trim(state, 0)
                        self._finish(flow, "aborted")
                    self.view.update([flow])
                    continue
                self._trim(state, self.options.web_stream_preview_max_bytes)
                if state.subscribers:
                    if state.timer:
                        state.timer.cancel()
                    self._notify(state)

    def done(self) -> None:
        for state in self.finished:
            state.valid = False
        for state in tuple(self.active.values()):
            self._discard(state)
