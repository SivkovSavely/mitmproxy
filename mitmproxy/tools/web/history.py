"""Metadata-only history queries and request-lifetime background readers."""

import asyncio
import json
import re
import threading
import time
import weakref
from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass

from mitmproxy import dns
from mitmproxy import flow
from mitmproxy import flowfilter
from mitmproxy import http
from mitmproxy import tcp
from mitmproxy import udp
from mitmproxy.io import sqlite
from mitmproxy.net.dns import domain_names

MAX_TIMESTAMP = 8.64e12  # JavaScript Date's range, in seconds.


def _timestamp(value: object) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not -MAX_TIMESTAMP <= value <= MAX_TIMESTAMP
    ):
        raise ValueError("Invalid history timestamp")
    return float(value)


def _number(value: str) -> float:
    if len(value) > 64:
        raise ValueError("Invalid history timestamp")
    return _timestamp(float(value))


def _integer(value: str, maximum: int) -> int:
    if not re.fullmatch(r"[0-9]{1,19}", value) or int(value) > maximum:
        raise ValueError("Invalid history integer")
    return int(value)


def _id(value: object) -> str:
    if not isinstance(value, str) or not 0 < len(value) <= 128:
        raise ValueError("Invalid history flow ID")
    value.encode("utf-8")
    return value


def _cursor(value: str) -> tuple[float, int, str]:
    if len(value) > 1024:
        raise ValueError("Invalid history cursor")
    cursor = json.loads(value)
    if (
        not isinstance(cursor, list)
        or len(cursor) != 3
        or not isinstance(cursor[1], str)
    ):
        raise ValueError("Invalid history cursor")
    return (
        _timestamp(cursor[0]),
        _integer(cursor[1], 2**63 - 1),
        _id(cursor[2]),
    )


@dataclass(frozen=True)
class HistoryQuery:
    lower: float | None
    upper: float | None
    search: flowfilter.TFilter | None
    highlight: flowfilter.TFilter | None
    page_options: dict

    @classmethod
    def parse(cls, arguments: Mapping[str, str], *, now: float | None = None):
        lower: float | None
        upper: float | None
        mode = arguments.get("mode", "relative")
        if mode == "relative":
            duration = _number(arguments.get("duration", "1800"))
            if duration <= 0:
                raise ValueError("History duration must be positive")
            upper = _timestamp(time.time() if now is None else now)
            lower = _timestamp(upper - duration)
        elif mode == "absolute":
            lower = _number(arguments["from"]) if "from" in arguments else None
            upper = _number(arguments["until"]) if "until" in arguments else None
        else:
            raise ValueError("Invalid history mode")
        if lower is not None and upper is not None and lower > upper:
            raise ValueError("History from must not exceed until")

        page_options: dict = {"lower": lower, "upper": upper}
        if "offset" in arguments:
            page_options["offset"] = _integer(arguments["offset"], 2**53 - 1)
        limit = _integer(arguments.get("limit", "500"), 500)
        if limit == 0:
            raise ValueError("History limit must be between 1 and 500")
        page_options["limit"] = limit
        for name in ("after", "before"):
            if name in arguments:
                page_options[name] = _cursor(arguments[name])
        if ("after" in page_options and "before" in page_options) or (
            "offset" in page_options
            and ("after" in page_options or "before" in page_options)
        ):
            raise ValueError("Use one history cursor or an absolute offset")
        for name in ("anchor", "selected"):
            if name in arguments:
                page_options[name] = _id(arguments[name])
        filters = []
        for name in ("search", "highlight"):
            expression = arguments.get(name, "")
            if len(expression) > 4096:
                raise ValueError(f"History {name} exceeds 4096 characters")
            filters.append(flowfilter.parse(expression) if expression.strip() else None)
        return cls(lower, upper, filters[0], filters[1], page_options)


def _messages(f: flow.Flow):
    if isinstance(f, (tcp.TCPFlow, udp.UDPFlow)):
        return f.messages
    if isinstance(f, http.HTTPFlow) and f.websocket is not None:
        return f.websocket.messages
    return None


def _dns_shape(message: dns.DNSMessage | None):
    if message is None:
        return None
    return (
        id(message),
        *(
            (id(section), len(section))
            for section in (
                message.questions,
                message.answers,
                message.authorities,
                message.additionals,
            )
        ),
    )


def _dns_size(message: dns.DNSMessage | None, cancel: threading.Event) -> int | None:
    if message is None:
        return None
    # DNSMessage.packed uses uncompressed names. Count its wire format without
    # constructing a packet or copying resource-record payloads.
    size = dns.DNSMessage.HEADER.size
    for section in (
        message.questions,
        message.answers,
        message.authorities,
        message.additionals,
    ):
        for i in range(len(section)):
            if i % 128 == 0 and cancel.is_set():
                raise sqlite.StorageError("History request cancelled")
            record = section[i]
            size += len(domain_names.pack(record.name))
            if isinstance(record, dns.Question):
                size += dns.Question.HEADER.size
            else:
                size += dns.ResourceRecord.HEADER.size + len(record.data)
    return size


@dataclass
class _Sizes:
    source: object
    observed: int
    count: int = 0
    request_size: int | None = 0
    response_size: int | None = 0
    valid: bool = True


class SizeCache:
    """Bounded aggregates only: never retain flows, message lists, or bodies.

    Stream notifications append messages; same-count View changes invalidate
    prior contents. Explicit UpdateHook edits invalidate even when also appending.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._entries: weakref.WeakKeyDictionary[flow.Flow, _Sizes] = (
            weakref.WeakKeyDictionary()
        )

    def remove(self, flow: flow.Flow):
        with self._lock:
            entry = self._entries.pop(flow, None)
            if entry is not None:
                entry.valid = False

    def clear(self):
        with self._lock:
            for entry in self._entries.values():
                entry.valid = False
            self._entries.clear()

    def changed(self, flow: flow.Flow, index: int | None = None):
        with self._lock:
            entry = self._entries.get(flow)
            if entry is None:
                return
            messages = _messages(flow)
            if (
                messages is not None
                and id(messages) == entry.source
                and len(messages) > entry.observed
            ):
                entry.observed = len(messages)
            else:
                entry.valid = False
                del self._entries[flow]

    def get(
        self, f: flow.Flow, cancel: threading.Event
    ) -> tuple[int | None, int | None]:
        messages = _messages(f)
        if messages is None:
            assert isinstance(f, dns.DNSFlow)
            request, response = f.request, f.response
            source: object = (_dns_shape(request), _dns_shape(response))
            count = 1
        else:
            source, count = id(messages), len(messages)
        with self._lock:
            entry = self._entries.get(f)
            if entry is None or entry.source != source or count < entry.observed:
                if entry is not None:
                    entry.valid = False
                entry = _Sizes(source, count)
                self._entries[f] = entry
                if len(self._entries) > 1000:
                    self._entries.pop(next(iter(self._entries)))
            entry.observed = max(entry.observed, count)
            start, request_size, response_size = (
                entry.count,
                entry.request_size,
                entry.response_size,
            )
        if messages is None:
            if start == 0:
                request_size, response_size = (
                    _dns_size(request, cancel),
                    _dns_size(response, cancel),
                )
            assert isinstance(f, dns.DNSFlow)
            changed = source != (_dns_shape(f.request), _dns_shape(f.response))
        else:
            assert request_size is not None and response_size is not None
            for i in range(start, count):
                if i % 128 == 0 and cancel.is_set():
                    raise sqlite.StorageError("History request cancelled")
                message = messages[i]
                if message.from_client:
                    request_size += len(message.content)
                else:
                    response_size += len(message.content)
            changed = _messages(f) is not messages or len(messages) < count
        if cancel.is_set():
            raise sqlite.StorageError("History request cancelled")
        with self._lock:
            if changed or not entry.valid:
                raise sqlite.StorageError(
                    "Live flow changed during history query; retry"
                )
            if self._entries.get(f) is entry and entry.count <= count:
                entry.count, entry.request_size, entry.response_size = (
                    count,
                    request_size,
                    response_size,
                )
        return request_size, response_size


def snapshot(
    flows: Iterable[flow.Flow],
    query: HistoryQuery,
    cancel: threading.Event,
    sizes: SizeCache,
) -> list[dict]:
    """Read live references in a bounded worker without serializing their bodies.

    Even ineligible IDs suppress durable copies. Size scans are incremental and
    cooperatively cancellable; no payload or message list is copied.
    Live state is eventually consistent, not frozen. Pin message/body references
    for summaries; filter read races fail the request so the client can retry.
    Cancellation is cooperative: a single synchronous regex cannot be preempted.
    """
    rows = []
    for f in flows:
        if cancel.is_set():
            raise sqlite.StorageError("History request cancelled")
        timestamp = f.timestamp_created
        matches = (query.lower is None or timestamp >= query.lower) and (
            query.upper is None or timestamp <= query.upper
        )
        row: dict = {
            "id": f.id,
            "timestamp_created": timestamp,
            "type": f.type,
            "matches": matches,
        }
        try:
            if matches and query.search is not None:
                matches = bool(query.search(f))
                row["matches"] = matches
            if cancel.is_set():
                raise sqlite.StorageError("History request cancelled")
            if matches:
                row["highlighted"] = bool(query.highlight and query.highlight(f))
                if isinstance(f, http.HTTPFlow):
                    request, response = f.request, f.response
                    if request:
                        body = request.raw_content
                        row.update(
                            method=request.method[:64],
                            host=request.host[:1024],
                            path=request.path[:4096],
                            request_size=None if body is None else len(body),
                        )
                    if response:
                        body = response.raw_content
                        row.update(
                            status_code=response.status_code,
                            response_size=None if body is None else len(body),
                        )
                    if f.websocket is not None:
                        request_extra, response_extra = sizes.get(f, cancel)
                        for key, extra in (
                            ("request_size", request_extra),
                            ("response_size", response_extra),
                        ):
                            if row.get(key) is not None:
                                row[key] += extra
                elif isinstance(f, (tcp.TCPFlow, udp.UDPFlow, dns.DNSFlow)):
                    row["request_size"], row["response_size"] = sizes.get(f, cancel)
        except (AttributeError, IndexError, RuntimeError, TypeError) as error:
            raise sqlite.StorageError(
                "Live flow changed during history query; retry"
            ) from error
        rows.append(row)
    if cancel.is_set():
        raise sqlite.StorageError("History request cancelled")
    return rows


class HistoryBusy(Exception):
    """The bounded history worker slots are already occupied."""


class HistoryReader:
    def __init__(self):
        self.generation = 0
        self.sizes = SizeCache()
        self._workers = asyncio.Semaphore(2)
        self._index_lock = threading.Lock()
        self._indexed_paths: set[str] = set()

    def update(self, flows: Iterable[flow.Flow]):
        for f in flows:
            self.sizes.remove(f)

    async def page(
        self,
        path: str,
        query: HistoryQuery,
        live: Iterable[flow.Flow],
        cancel: threading.Event,
        *,
        count_only: bool = False,
    ) -> dict:
        if self._workers.locked():
            raise HistoryBusy()
        await self._workers.acquire()
        future = asyncio.get_running_loop().run_in_executor(
            None, self._read, path, query, live, cancel, count_only
        )
        # A cancelled HTTP task must not free a slot while SQLite still runs.
        future.add_done_callback(lambda _: self._workers.release())
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            cancel.set()
            future.add_done_callback(lambda f: f.exception())
            raise

    def _read(self, path, query, live, cancel, count_only):
        live = snapshot(live, query, cancel, self.sizes)
        while not self._index_lock.acquire(timeout=0.05):
            if cancel.is_set():
                raise sqlite.StorageError("History request cancelled")
        try:
            if cancel.is_set():
                raise sqlite.StorageError("History request cancelled")
            if path not in self._indexed_paths:
                sqlite.ensure_history_index(path, cancel=cancel.is_set)
                if cancel.is_set():
                    raise sqlite.StorageError("History request cancelled")
                self._indexed_paths.add(path)
        finally:
            self._index_lock.release()
        if cancel.is_set():
            raise sqlite.StorageError("History request cancelled")
        with sqlite.SQLiteFlowReader(path) as reader:
            reader.db.set_progress_handler(lambda: int(cancel.is_set()), 1000)
            try:
                page = reader.history_page(
                    **query.page_options,
                    live=live,
                    include_history=query.search is None,
                    count_only=count_only,
                )
            finally:
                reader.db.set_progress_handler(None, 0)
        for row in page["rows"]:
            row["sequence"] = str(row["sequence"])
            row["cursor"] = [row["timestamp_created"], row["sequence"], row["id"]]
        return page
