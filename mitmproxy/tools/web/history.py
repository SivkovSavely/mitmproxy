"""Metadata-only history queries and request-lifetime background readers."""

import asyncio
import json
import math
import os
import re
import sqlite3
import tempfile
import threading
import time
import uuid
import weakref
from collections import OrderedDict
from collections.abc import Iterable
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from dataclasses import field

from mitmproxy import dns
from mitmproxy import flow
from mitmproxy import flowfilter
from mitmproxy import http
from mitmproxy import tcp
from mitmproxy import udp
from mitmproxy.io import sqlite
from mitmproxy.net.dns import domain_names
from mitmproxy.tools.web.group_snapshot import GroupSnapshot
from mitmproxy.tools.web.group_snapshot import GroupStore
from mitmproxy.tools.web.group_snapshot import Metadata
from mitmproxy.tools.web.group_snapshot import SnapshotError
from mitmproxy.tools.web.live_history import _metadata
from mitmproxy.tools.web.live_history import LiveIndex
from mitmproxy.tools.web.live_history import LiveIndexError as LiveIndexError
from mitmproxy.utils import asyncio_utils

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
    grouped: dict = field(default_factory=dict)
    binding: tuple = ()

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
        if "group_by" in arguments:
            group_by = arguments["group_by"]
            if group_by not in ("host", "method", "status", "type"):
                raise ValueError("Invalid history group selector")
            page_options["group_by"] = group_by
        grouped: dict = {}
        if "group_by" in arguments or any(
            name in arguments
            for name in (
                "snapshot",
                "refresh",
                "lookup_group",
                "lookup_groups",
                "lookup_flow",
            )
        ):
            if "group_by" not in arguments:
                raise ValueError("Snapshot requests require group_by")
            if any(
                name in arguments for name in ("after", "before", "anchor", "selected")
            ):
                raise ValueError("Group pages require an ordinal offset")
            for name in (
                "snapshot",
                "refresh",
                "group_key",
                "lookup_group",
                "lookup_flow",
            ):
                if name in arguments:
                    value = arguments[name]
                    if name in ("snapshot", "refresh"):
                        if not re.fullmatch(r"s1:[0-9a-f]{32}", value):
                            raise ValueError("Invalid snapshot token")
                    elif name == "lookup_flow":
                        _id(value)
                    elif not re.fullmatch(r"g1:[0-9a-f]{64}", value):
                        raise ValueError("Invalid group token")
                    grouped[name] = value
            if "lookup_groups" in arguments:
                value = arguments["lookup_groups"]
                if len(value) > 10000:
                    raise ValueError("Group lookup exceeds 128 keys")
                keys = json.loads(value)
                if (
                    not isinstance(keys, list)
                    or len(keys) > 128
                    or any(
                        not isinstance(key, str)
                        or not re.fullmatch(r"g1:[0-9a-f]{64}", key)
                        for key in keys
                    )
                ):
                    raise ValueError("Invalid group lookup keys")
                grouped["lookup_groups"] = keys
            if "refresh" in grouped and (
                "snapshot" in grouped
                or any(
                    name in grouped
                    for name in (
                        "group_key",
                        "lookup_group",
                        "lookup_groups",
                        "lookup_flow",
                    )
                )
                or page_options.get("offset", 0) != 0
            ):
                raise ValueError(
                    "Refresh requires an initial group page without snapshot"
                )
            if (
                sum(
                    name in grouped
                    for name in (
                        "group_key",
                        "lookup_group",
                        "lookup_groups",
                        "lookup_flow",
                    )
                )
                > 1
            ):
                raise ValueError("Use one snapshot operation")
            if (
                any(
                    name in grouped
                    for name in (
                        "group_key",
                        "lookup_group",
                        "lookup_groups",
                        "lookup_flow",
                    )
                )
                and "snapshot" not in grouped
            ):
                raise ValueError("Group members and lookups require a snapshot")
        elif "group_key" in arguments:
            raise ValueError("Group members require group_by and a snapshot")
        filters = []
        for name in ("search", "highlight"):
            expression = arguments.get(name, "")
            if len(expression) > 4096:
                raise ValueError(f"History {name} exceeds 4096 characters")
            filters.append(flowfilter.parse(expression) if expression.strip() else None)
        binding = (
            mode,
            arguments.get("duration", "1800") if mode == "relative" else (lower, upper),
            arguments.get("search", ""),
            arguments.get("highlight", ""),
            arguments.get("group_by"),
        )
        return cls(lower, upper, filters[0], filters[1], page_options, grouped, binding)


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
    *,
    max_rows: int | None = None,
    max_bytes: int | None = None,
) -> list[dict]:
    """Read live references in a bounded worker without serializing their bodies.

    Even ineligible IDs suppress durable copies. Size scans are incremental and
    cooperatively cancellable; no payload or message list is copied.
    Live state is eventually consistent, not frozen. Pin message/body references
    for summaries; filter read races fail the request so the client can retry.
    Cancellation is cooperative: a single synchronous regex cannot be preempted.
    """
    rows: list[dict] = []
    retained_bytes = 0
    for f in flows:
        if cancel.is_set():
            raise sqlite.StorageError("History request cancelled")
        if max_rows is not None and len(rows) >= max_rows:
            raise SnapshotError("Too many live flows for a grouped snapshot")
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
                            method=(
                                request.method
                                if query.page_options.get("group_by") == "method"
                                else request.method[:64]
                            ),
                            host=(
                                request.host
                                if query.page_options.get("group_by") == "host"
                                else request.host[:1024]
                            ),
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
        if max_bytes is not None:
            if not 0 < len(row["id"]) <= 128:
                raise SnapshotError("Invalid live flow ID")
            for name in ("host", "method"):
                if isinstance(row.get(name), str) and len(row[name]) > 1024 * 1024:
                    raise SnapshotError("Group value exceeds snapshot byte bound")
            # Include per-row container overhead as well as encoded scalar values.
            retained_bytes += 512 + len(json.dumps(row, ensure_ascii=True))
            if retained_bytes > max_bytes:
                raise SnapshotError("Live metadata exceeds snapshot byte bound")
        rows.append(row)
    if cancel.is_set():
        raise sqlite.StorageError("History request cancelled")
    return rows


class HistoryBusy(Exception):
    """The bounded history worker slots are already occupied."""


class HistoryStale(Exception):
    """A token expired or no longer matches its instance/configuration/query."""


_FILTER_BUDGET = "input exceeds the 1048576-byte capture or decoding budget"
_FILTER_EVICTED = "captured event-time body value was evicted"


class LiveHistoryIndex(LiveIndex):
    """Evaluate active filters only on the View's owning thread."""

    def __init__(self, view, *, body_cache_bytes=8 * 1024 * 1024, **limits):
        self.filters: dict[tuple, tuple[str, HistoryQuery]] = {}
        self._read_filter_token = None
        self._read_bounds = (None, None)
        if body_cache_bytes < 0:
            raise ValueError("Live body cache bound must be nonnegative")
        self.body_cache_bytes = body_cache_bytes
        self._body_cache: OrderedDict[str, tuple[dict, int]] = OrderedDict()
        self._body_bytes = 0
        self.sizes = SizeCache()
        super().__init__(view, **limits)

    def capture(self, flow):
        try:
            _id(flow.id)
            stamp = self._capture_body(flow)
            record = _metadata(flow)
            record["_body_stamp"] = stamp
            record["_filter_inputs"] = self._filter_inputs(flow)
            if isinstance(flow, (tcp.TCPFlow, udp.UDPFlow, dns.DNSFlow)):
                self.sizes.changed(flow)
                record["request_size"], record["response_size"] = self.sizes.get(
                    flow, threading.Event()
                )
            elif isinstance(flow, http.HTTPFlow) and flow.websocket is not None:
                self.sizes.changed(flow)
                request_size, response_size = self.sizes.get(flow, threading.Event())
                if record["request_size"] is not None:
                    record["request_size"] += request_size
                if record["response_size"] is not None:
                    record["response_size"] += response_size
            for token, query in self.filters.values():
                self._merge_flags(
                    record,
                    {
                        "_active_filters": ",".join(
                            t for t, _ in self.filters.values()
                        ),
                        "_queries": {token: self._query_spec(query, flow.id)},
                    },
                )
            self._enqueue("upsert", flow.id, json.dumps(record, ensure_ascii=True))
        except (
            ValueError,
            UnicodeError,
            TypeError,
            AttributeError,
            RuntimeError,
        ) as error:
            with self._condition:
                self._error = f"live filter capture failed: {error}"
                self._condition.notify_all()

    def _enqueue(self, op, flow_id=None, data=None):
        with self._condition:
            if (
                op == "flags"
                and self._queue
                and isinstance(self._queue[-1], OrderedDict)
            ):
                queued = self._queue[-1].get(flow_id)
                if queued is not None and queued[1] in ("upsert", "flags"):
                    record = json.loads(queued[3])
                    flags = json.loads(data)
                    if queued[1] == "flags":
                        record["_queries"].update(flags["_queries"])
                        record["_active_filters"] = flags["_active_filters"]
                    else:
                        self._merge_flags(record, flags)
                    op, data = queued[1], json.dumps(record, ensure_ascii=True)
            super()._enqueue(op, flow_id, data)

    @staticmethod
    def _merge_flags(record, flags):
        active = flags.pop("_active_filters").split(",")
        queries = flags.pop("_queries")
        record.pop("_active_filters", None)
        errors = record.setdefault("_filter_errors", {})
        for name in list(record):
            if (
                name.startswith(("search:", "highlight:"))
                and name.split(":", 1)[1] not in active
            ):
                del record[name]
        for name in list(errors):
            if name.split(":", 1)[1] not in active:
                del errors[name]
        for token, (
            search,
            highlight,
            body_results,
            candidate_stamp,
        ) in queries.items():
            if candidate_stamp is not None:
                captured = record["_body_stamp"]
                body_results = {
                    key: {
                        direction: _FILTER_BUDGET
                        if captured[direction] is None
                        else _FILTER_EVICTED
                        if candidate_stamp[direction] != captured[direction]
                        else result
                        for direction, result in directions.items()
                    }
                    for key, directions in body_results.items()
                }
            for name, expression, default in (
                ("search:", search, True),
                ("highlight:", highlight, False),
            ):
                try:
                    record[name + token] = LiveHistoryIndex._match_metadata(
                        expression, record, default, body_results
                    )
                    errors.pop(name + token, None)
                except LiveIndexError as error:
                    record[name + token] = None
                    errors[name + token] = str(error)

    @staticmethod
    def _match_metadata(expression, record, default, body_results):
        import base64

        data = record["_filter_inputs"]

        def binary_match(regex, value):
            if value is None or value == "!":
                return _FILTER_BUDGET
            return bool(regex.search(base64.b64decode(value)))

        def combine(values, decisive):
            unavailable = None
            for value in values:
                if value is decisive:
                    return decisive
                if isinstance(value, str):
                    unavailable = value
            return unavailable if unavailable is not None else not decisive

        def match(predicate):
            if isinstance(predicate, flowfilter.FAnd):
                return combine((match(item) for item in predicate.lst), False)
            if isinstance(predicate, flowfilter.FOr):
                return combine((match(item) for item in predicate.lst), True)
            if isinstance(predicate, flowfilter.FNot):
                result = match(predicate.itm)
                return result if isinstance(result, str) else not result
            if isinstance(predicate, flowfilter.FAll):
                return True
            if isinstance(predicate, flowfilter.FErr):
                return data["error"]
            if isinstance(predicate, flowfilter.FMarked):
                if data.get("marked_unavailable"):
                    return _FILTER_BUDGET
                return bool(data["marked"])
            if isinstance(predicate, flowfilter.FWebSocket):
                return data["kind"] == "http" and data["websocket"]
            if isinstance(predicate, flowfilter.FReq):
                return data["kind"] in ("http", "dns") and not data["response"]
            if isinstance(predicate, flowfilter.FResp):
                return data["kind"] in ("http", "dns") and data["response"]
            if isinstance(predicate, flowfilter.FReplay):
                return data["replay"] is not None
            if isinstance(predicate, flowfilter.FReplayClient):
                return data["replay"] == "request"
            if isinstance(predicate, flowfilter.FReplayServer):
                return data["replay"] == "response"
            strings = {
                flowfilter.FSrc: "src",
                flowfilter.FDst: "dst",
                flowfilter.FMeta: "meta",
                flowfilter.FMarker: "marked",
                flowfilter.FComment: "comment",
                flowfilter.FUrl: "url",
            }
            for cls, key in strings.items():
                if isinstance(predicate, cls):
                    if data.get(key + "_unavailable"):
                        return _FILTER_BUDGET
                    value = data[key]
                    return bool(value is not None and predicate.re.search(value))
            if isinstance(predicate, flowfilter.FDomain):
                return data["kind"] == "http" and combine(
                    (
                        _FILTER_BUDGET
                        if value is None
                        else bool(predicate.re.search(value))
                        for value in data["domains"]
                    ),
                    True,
                )
            if isinstance(predicate, flowfilter.FDateTime):
                if data["kind"] != "http":
                    return False
                times = data["times"]
                fields = {
                    "dt": (0, 1, 2, 3),
                    "dtq": (0, 1),
                    "dts": (2, 3),
                    "dtqs": (0,),
                    "dtqe": (1,),
                    "dtss": (2,),
                    "dtse": (3,),
                }[predicate.code]
                compare = flowfilter._timestamp_comparators[predicate.comparator]
                return any(
                    times[i] is not None and compare(times[i], predicate.timestamp)
                    for i in fields
                )
            if isinstance(
                predicate,
                (flowfilter.FBod, flowfilter.FBodRequest, flowfilter.FBodResponse),
            ):
                result = body_results[predicate.code + "\0" + predicate.expr]
                return combine(result.values(), True)
            if isinstance(
                predicate,
                (flowfilter.FHead, flowfilter.FHeadRequest, flowfilter.FHeadResponse),
            ):
                if data["kind"] != "http":
                    return False
                directions = (
                    ("request_headers", "response_headers")
                    if isinstance(predicate, flowfilter.FHead)
                    else ("request_headers",)
                    if isinstance(predicate, flowfilter.FHeadRequest)
                    else ("response_headers",)
                )
                return combine(
                    (
                        binary_match(predicate.re, data[key])
                        for key in directions
                        if data[key] is not None
                    ),
                    True,
                )
            if isinstance(predicate, flowfilter.FAsset):
                return data["kind"] == "http" and combine(
                    (
                        binary_match(rex, value)
                        for value in data["response_content_types"]
                        for rex in flowfilter.FAsset.ASSET_TYPES
                    ),
                    True,
                )
            if isinstance(
                predicate,
                (
                    flowfilter.FContentType,
                    flowfilter.FContentTypeRequest,
                    flowfilter.FContentTypeResponse,
                ),
            ):
                if data["kind"] != "http":
                    return False
                directions = (
                    ("request_content_types", "response_content_types")
                    if isinstance(predicate, flowfilter.FContentType)
                    else ("request_content_types",)
                    if isinstance(predicate, flowfilter.FContentTypeRequest)
                    else ("response_content_types",)
                )
                return combine(
                    (
                        binary_match(predicate.re, value)
                        for key in directions
                        for value in data[key]
                    ),
                    True,
                )
            if isinstance(predicate, flowfilter.FCode):
                return (
                    record["type"] == "http" and record["status_code"] == predicate.num
                )
            if isinstance(predicate, flowfilter.FMethod):
                if record["type"] != "http":
                    return False
                method = data["method"]
                return method is not None and binary_match(predicate.re, method)
            types = {
                flowfilter.FHTTP: "http",
                flowfilter.FTCP: "tcp",
                flowfilter.FUDP: "udp",
                flowfilter.FDNS: "dns",
            }
            if type(predicate) in types:
                if record.get("type_oversized"):
                    return _FILTER_BUDGET
                return record["type"] == types[type(predicate)]
            return "predicate requires unavailable event-boundary metadata"

        result = match(flowfilter.parse(expression)) if expression else default
        if isinstance(result, str):
            raise LiveIndexError("live filter input unavailable: " + result)
        return result

    def _body_record(self, f):
        data: dict[str, list] = {"request_body": [], "response_body": []}
        size = 512
        unavailable: set[str] = set()

        def missing(direction):
            if direction not in unavailable:
                data[direction].append(None)
                unavailable.add(direction)

        def add(direction, raw, encoding=None):
            nonlocal size
            if raw is None:
                return
            if size + len(raw) + 128 > 1024 * 1024:
                missing(direction)
            else:
                data[direction].append((raw, encoding))
                size += len(raw) + 128

        if isinstance(f, http.HTTPFlow):
            for direction, message in (
                ("request_body", f.request),
                ("response_body", f.response),
            ):
                if message is not None:
                    encoding_size = sum(
                        len(value) + 2
                        for key, value in message.headers.fields
                        if key.lower() == b"content-encoding"
                    )
                    if encoding_size > 128 and message.raw_content is not None:
                        missing(direction)
                    else:
                        add(
                            direction,
                            message.raw_content,
                            message.headers.get("content-encoding"),
                        )
            messages: Iterable = f.websocket.messages if f.websocket else ()
        elif isinstance(f, (tcp.TCPFlow, udp.UDPFlow)):
            messages = f.messages
        elif isinstance(f, dns.DNSFlow):
            for direction, dns_message in (
                ("request_body", f.request),
                ("response_body", f.response),
            ):
                if dns_message:
                    length = _dns_size(dns_message, threading.Event())
                    if length is not None and length * 8 <= 1024 * 1024:
                        add(direction, str(dns_message).encode())
                    else:
                        missing(direction)
            messages = ()
        else:
            messages = ()
        for stream_message in messages:
            direction = (
                "request_body" if stream_message.from_client else "response_body"
            )
            add(direction, stream_message.content)
        return data, size

    @staticmethod
    def _body_fingerprint(data):
        import hashlib

        stamps: dict[str, str | None] = {}
        for direction in ("request_body", "response_body"):
            if any(item is None for item in data[direction]):
                stamps[direction] = None
                continue
            digest = hashlib.sha256()
            digest.update(direction.encode("ascii"))
            for item in data[direction]:
                raw, encoding = item
                name = (encoding or "").encode("utf-8", "surrogateescape")
                digest.update(len(name).to_bytes(8, "big"))
                digest.update(name)
                digest.update(len(raw).to_bytes(8, "big"))
                digest.update(raw)
            stamps[direction] = digest.hexdigest()
        return stamps

    def _capture_body(self, f):
        old = self._body_cache.pop(f.id, None)
        if old:
            self._body_bytes -= old[1]
        data, size = self._body_record(f)
        stamp = self._body_fingerprint(data)
        while self._body_cache and self._body_bytes + size > self.body_cache_bytes:
            _, (_, removed) = self._body_cache.popitem(last=False)
            self._body_bytes -= removed
        if size <= self.body_cache_bytes:
            self._body_cache[f.id] = (data, size)
            self._body_bytes += size
        return stamp

    def _query_spec(self, query, flow_id, candidate=None):
        from types import SimpleNamespace

        cache = self._body_cache.get(flow_id)
        bodies = cache[0] if cache else None
        candidate_stamp = None

        def needs_body(predicate):
            if isinstance(predicate, (flowfilter.FAnd, flowfilter.FOr)):
                return any(needs_body(item) for item in predicate.lst)
            if isinstance(predicate, flowfilter.FNot):
                return needs_body(predicate.itm)
            return isinstance(
                predicate,
                (flowfilter.FBod, flowfilter.FBodRequest, flowfilter.FBodResponse),
            )

        if (
            bodies is None
            and candidate is not None
            and (needs_body(query.search) or needs_body(query.highlight))
        ):
            bodies, _ = self._body_record(candidate)
            candidate_stamp = self._body_fingerprint(bodies)
        results: dict[str, dict[str, bool | str]] = {}

        def visit(predicate):
            if predicate is None:
                return
            if isinstance(predicate, (flowfilter.FAnd, flowfilter.FOr)):
                for item in predicate.lst:
                    visit(item)
            elif isinstance(predicate, flowfilter.FNot):
                visit(predicate.itm)
            elif isinstance(
                predicate,
                (flowfilter.FBod, flowfilter.FBodRequest, flowfilter.FBodResponse),
            ):
                key = predicate.code + "\0" + predicate.expr
                directions = (
                    ("request_body", "response_body")
                    if isinstance(predicate, flowfilter.FBod)
                    else ("request_body",)
                    if isinstance(predicate, flowfilter.FBodRequest)
                    else ("response_body",)
                )
                results[key] = {}
                for direction in directions:
                    result: bool | str = False
                    if bodies is None:
                        results[key][direction] = _FILTER_EVICTED
                        continue
                    for item in bodies[direction]:
                        if item is None:
                            result = _FILTER_BUDGET
                            continue
                        raw, encoding = item
                        content = self._bounded_content(
                            SimpleNamespace(
                                raw_content=raw, headers={"content-encoding": encoding}
                            ),
                            1024 * 1024,
                        )
                        if content is None:
                            result = _FILTER_BUDGET
                            continue
                        if predicate.re.search(content):
                            result = True
                            break
                    results[key][direction] = result

        visit(query.search)
        visit(query.highlight)
        return (query.binding[2], query.binding[3], results, candidate_stamp)

    def _filter_inputs(self, f):
        import base64

        remaining = min(1024 * 1024, self.queue_bytes // 4)

        def encoded(value):
            nonlocal remaining
            if len(value) + 32 > remaining:
                return None
            remaining -= len(value) + 32
            return base64.b64encode(value).decode("ascii")

        def address(key, value):
            if value:
                if len(value[0]) * 4 > remaining:
                    data[key + "_unavailable"] = True
                else:
                    text(key, f"{value[0]}:{value[1]}")

        def text(key, value):
            nonlocal remaining
            if len(value) * 4 > remaining:
                data[key] = None
                data[key + "_unavailable"] = True
            else:
                remaining -= len(value) * 4
                data[key] = value

        data = {
            "kind": f.type,
            "error": bool(f.error),
            "marked": "",
            "comment": "",
            "meta": "",
            "replay": f.is_replay,
            "src": None,
            "dst": None,
            "websocket": False,
            "response": False,
            "url": None,
            "domains": [],
            "method": None,
            "times": [None] * 4,
            "request_body": [],
            "response_body": [],
            "request_headers": None,
            "response_headers": None,
            "request_content_types": [],
            "response_content_types": [],
        }
        address("src", f.client_conn.peername if f.client_conn else None)
        address("dst", f.server_conn.address if f.server_conn else None)
        text("marked", f.marked)
        text("comment", f.comment)
        parts = []
        metadata_budget = remaining
        for key, value in f.metadata.items():
            if not isinstance(key, (str, int, float, bool)) or not isinstance(
                value, (str, bytes, int, float, bool, type(None))
            ):
                data["meta_unavailable"] = True
                break
            if (isinstance(key, int) and key.bit_length() > remaining) or (
                isinstance(value, int) and value.bit_length() > remaining
            ):
                data["meta_unavailable"] = True
                break
            if (isinstance(key, str) and len(key) * 4 > remaining) or (
                isinstance(value, (str, bytes)) and len(value) * 4 > remaining
            ):
                data["meta_unavailable"] = True
                break
            part = f"{key}: {str(value)}"
            if len(part) * 4 > remaining:
                data["meta_unavailable"] = True
                break
            remaining -= len(part) * 4
            parts.append(part)
        if data.get("meta_unavailable"):
            remaining = metadata_budget
        else:
            data["meta"] = "\n".join(parts)
        if isinstance(f, http.HTTPFlow):
            data["websocket"] = f.websocket is not None
            data["response"] = f.response is not None
            if f.request:
                host_size = sum(
                    len(value)
                    for key, value in f.request.headers.fields
                    if key.lower() == b"host"
                )
                if (
                    len(f.request.data.path) + len(f.request.data.host) + host_size
                    > remaining // 8
                ):
                    data["url_unavailable"] = True
                else:
                    text("url", f.request.pretty_url)
                for value in (
                    f.request.host,
                    f.request.pretty_host if host_size <= remaining // 4 else None,
                ):
                    if value is None or len(value) * 4 > remaining:
                        data["domains"].append(None)
                    else:
                        remaining -= len(value) * 4
                        data["domains"].append(value)
                method = encoded(f.request.data.method)
                data["method"] = method if method is not None else "!"
            for index, (name, message) in enumerate(
                (("request", f.request), ("response", f.response))
            ):
                if message is None:
                    continue
                data["times"][2 * index : 2 * index + 2] = [
                    message.timestamp_start,
                    message.timestamp_end,
                ]
                header_size = sum(
                    len(key) + len(value) + 4 for key, value in message.headers.fields
                )
                data[name + "_headers"] = (
                    encoded(bytes(message.headers)) if header_size <= remaining else "!"
                )
                content_type_unavailable = False
                for key, value in message.headers.fields:
                    if key.lower() == b"content-type":
                        item = encoded(value)
                        if item is not None or not content_type_unavailable:
                            data[name + "_content_types"].append(item)
                        if item is None:
                            content_type_unavailable = True
        elif isinstance(f, dns.DNSFlow):
            data["response"] = f.response is not None
            data["url"] = (
                f.request.questions[0].name
                if f.request and f.request.questions
                else None
            )
        return data

    @staticmethod
    def _bounded_content(message, limit):
        import codecs
        import zlib

        import brotli

        from mitmproxy.net import encoding

        raw = message.raw_content
        if raw is None:
            return None
        ce = (message.headers.get("content-encoding") or "identity").lower()
        if ce in ("identity", "none"):
            return raw if len(raw) <= limit else None
        try:
            if ce in ("gzip", "deflate", "deflateraw"):
                if not raw:
                    return b""
                for window in (47,) if ce == "gzip" else (15, -15):
                    try:
                        decoder = zlib.decompressobj(window)
                        output = decoder.decompress(raw, limit + 1)
                        if len(output) > limit or decoder.unconsumed_tail:
                            return None
                        output += decoder.flush(limit + 1 - len(output))
                        if ce != "gzip" and not decoder.eof:
                            raise zlib.error("incomplete deflate stream")
                        return output if len(output) <= limit else None
                    except zlib.error:
                        if window == (47 if ce == "gzip" else -15):
                            raise
            elif ce == "br":
                if not raw:
                    return b""
                decoder = brotli.Decompressor()
                output = decoder.process(raw, output_buffer_limit=limit + 1)
                if len(output) > limit or not decoder.can_accept_more_data():
                    return None
                if not decoder.is_finished():
                    raise ValueError("incomplete brotli stream")
                return output
            elif ce == "zstd":
                if not raw:
                    return b""
                if len(raw) > 1024 * 1024:
                    return None
                frames = []
                size = 0
                remaining_raw = raw
                while remaining_raw:
                    zstd_decoder = encoding.zstd.ZstdDecompressor()
                    frame = zstd_decoder.decompress(
                        remaining_raw, max_length=limit + 1 - size
                    )
                    size += len(frame)
                    if size > limit or (
                        not zstd_decoder.eof and not zstd_decoder.needs_input
                    ):
                        return None
                    if not zstd_decoder.eof:
                        raise ValueError("incomplete zstd stream")
                    frames.append(frame)
                    remaining_raw = zstd_decoder.unused_data
                return b"".join(frames)
            else:
                try:
                    codec = codecs.lookup(ce)
                except LookupError:
                    return raw if len(raw) <= limit else None
                if codec._is_text_encoding:
                    return raw if len(raw) <= limit else None
                if codec.name in ("base64", "hex", "quopri", "uu"):
                    if len(raw) > limit:
                        return None
                    decoded = codecs.decode(raw, ce)
                    return (
                        decoded
                        if isinstance(decoded, bytes) and len(decoded) <= limit
                        else raw
                    )
                # Unknown codecs may expand without a bounded streaming API.
                return None
        except (
            ValueError,
            zlib.error,
            brotli.error,
            EOFError,
            encoding.zstd.ZstdError,
        ):
            return raw if len(raw) <= limit else None

    def read_cut(self, cancel=None, *, bounds=None):
        from contextlib import contextmanager

        lower, upper = self._read_bounds if bounds is None else bounds

        @contextmanager
        def checked():
            with super(LiveHistoryIndex, self).read_cut(cancel) as (db, cut):
                if self._read_filter_token:
                    unavailable = db.execute(
                        "SELECT data FROM unavailable JOIN current USING(id) WHERE token=? AND coalesce(json_extract(data, '$.\"search:' || token || '\"'), 1) != 0 AND (? IS NULL OR json_extract(data,'$.timestamp_created')>=?) AND (? IS NULL OR json_extract(data,'$.timestamp_created')<=?) LIMIT 1",
                        (self._read_filter_token, lower, lower, upper, upper),
                    ).fetchone()
                    if unavailable:
                        record = json.loads(unavailable[0])
                        name = (
                            "search:"
                            if record["search:" + self._read_filter_token] is None
                            else "highlight:"
                        ) + self._read_filter_token
                        raise LiveIndexError(record["_filter_errors"][name])
                yield db, cut

        return checked()

    def _write_batch(self, db, batch):
        pending: list[tuple[int, str, str | None, str | None, int]] = []
        records: dict[str, str | None] = {}
        pending_bytes = 0
        cleared = False

        def flush():
            nonlocal pending_bytes, cleared
            if not pending:
                return
            LiveIndex._write_batch(self, db, pending)
            for _, op, flow_id, data, _ in pending:
                if op == "clear":
                    db.execute("DELETE FROM unavailable")
                else:
                    db.execute("DELETE FROM unavailable WHERE id=?", (flow_id,))
                    if op == "upsert":
                        assert data is not None
                        row = json.loads(data)
                        tokens = {
                            name.split(":", 1)[1]
                            for name, value in row.items()
                            if name.startswith(("search:", "highlight:"))
                            and value is None
                        }
                        db.executemany(
                            "INSERT OR IGNORE INTO unavailable VALUES (?,?)",
                            ((token, flow_id) for token in tokens),
                        )
            db.commit()
            wal = self.path.with_name(self.path.name + "-wal")
            if wal.exists() and wal.stat().st_size > self.wal_bytes:
                busy = db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0]
                if busy or wal.stat().st_size > self.wal_bytes:
                    raise LiveIndexError(
                        "live metadata WAL capacity exceeded by reader"
                    )
            pending.clear()
            records.clear()
            pending_bytes = 0
            cleared = False

        for seq, op, flow_id, data, size in batch:
            if op == "flags":
                if flow_id in records:
                    captured = records[flow_id]
                else:
                    found = (
                        None
                        if cleared
                        else db.execute(
                            "SELECT data FROM current WHERE id=?", (flow_id,)
                        ).fetchone()
                    )
                    captured = found[0] if found else None
                if captured is None:
                    op, data = "remove", None
                else:
                    record = json.loads(captured)
                    self._merge_flags(record, json.loads(data))
                    op, data = "upsert", json.dumps(record, ensure_ascii=True)
                size = 256 + len(flow_id) + (len(data) if data else 0)
            if pending and pending_bytes + size > self.queue_bytes:
                flush()
            if op == "clear":
                records.clear()
                cleared = True
            elif op == "remove":
                records[flow_id] = None
            else:
                records[flow_id] = data
            pending.append((seq, op, flow_id, data, size))
            pending_bytes += size
        flush()

    def remove(self, flow):
        self.sizes.remove(flow)
        old = self._body_cache.pop(flow.id, None)
        if old:
            self._body_bytes -= old[1]
        super().remove(flow)

    def clear(self):
        self.sizes.clear()
        self._body_cache.clear()
        self._body_bytes = 0
        super().clear()

    async def done(self):
        await super().done()
        self.sizes.clear()
        self.filters.clear()
        self._body_cache.clear()
        self._body_bytes = 0

    def prune(self, tokens):
        for binding, (token, _) in list(self.filters.items()):
            if token not in tokens:
                del self.filters[binding]

    async def register(self, query, cancel, *, verify=True):
        if cancel.is_set():
            raise sqlite.StorageError("History request cancelled")
        self._read_bounds = (query.lower, query.upper)
        if query.search is None and query.highlight is None:
            self._read_filter_token = None
            return None
        binding = query.binding[2:4]
        if binding in self.filters:
            self._read_filter_token = self.filters[binding][0]
            return self._read_filter_token
        if len(self.filters) >= 16:
            raise SnapshotError("Live filter binding capacity reached")
        token = uuid.uuid4().hex
        self.filters[binding] = (token, query)
        # A new filter needs one inventory. Ordinary refreshes consume only journal IDs.
        try:
            inventory = self.view.resolve("@all")
            for i, f in enumerate(inventory):
                if cancel.is_set():
                    raise sqlite.StorageError("History request cancelled")
                if self.view.get_by_id(f.id) is f:
                    flags = {
                        "_queries": {token: self._query_spec(query, f.id, f)},
                        "_active_filters": ",".join(
                            token for token, _ in self.filters.values()
                        ),
                    }
                    self._enqueue("flags", f.id, json.dumps(flags))
                if i % 128 == 0:
                    await asyncio.sleep(0)
        except BaseException:
            del self.filters[binding]
            raise
        self._read_filter_token = token
        if not verify:
            return token
        try:

            def verify():
                with self.read_cut(cancel):
                    pass

            await asyncio.to_thread(verify)
        except BaseException:
            self.filters.pop(binding, None)
            self._read_filter_token = None
            raise
        return token


_COLUMNS = "sequence flow_id timestamp flow_type method host path status_code request_size response_size payload_version oversized".split()
_PROJECTION = """sequence,CASE WHEN length(flow_id)<=128 THEN flow_id ELSE NULL END,timestamp,
CASE WHEN length(CAST(flow_type AS BLOB))<=1048576 THEN flow_type END,
CASE WHEN length(CAST(method AS BLOB))<=1048576 THEN method END,
CASE WHEN length(CAST(host AS BLOB))<=1048576 THEN host END,
substr(path,1,4096),status_code,request_size,response_size,payload_version,
coalesce(length(CAST(flow_type AS BLOB))>1048576,0)+2*coalesce(length(CAST(method AS BLOB))>1048576,0)+4*coalesce(length(CAST(host AS BLOB))>1048576,0)"""


def _durable(record):
    record = dict(record)
    try:
        flow_id = _id(record["flow_id"])
    except ValueError as error:
        raise SnapshotError("Invalid durable flow ID") from error
    row = {
        "id": flow_id,
        "timestamp_created": _timestamp(record["timestamp"]),
        "type": record["flow_type"],
        "sequence": record["sequence"],
        "live": False,
        "highlighted": False,
    }
    for name in (
        "method",
        "host",
        "path",
        "status_code",
        "request_size",
        "response_size",
        "payload_version",
    ):
        row[name] = record[name]
    row["type_oversized"] = bool(record["oversized"] & 1)
    row["method_oversized"] = bool(record["oversized"] & 2)
    row["host_oversized"] = bool(record["oversized"] & 4)
    row["_blobs"] = [key for key, value in row.items() if isinstance(value, bytes)]
    for key in row["_blobs"]:
        row[key] = row[key].decode("utf-8", "surrogateescape")
    return row


class _Engine:
    def __init__(self, query, context, identity, max_bytes):
        self.query = query
        self.context = context
        self.identity = identity
        self.revision = ("", 0)
        self.live_cut = 0
        self.filter_token = None
        self.usable = True
        self.live_count = 0
        self.live_bytes = 0
        self.store: GroupStore | None = None
        self.expires = time.monotonic()
        self.directory = tempfile.TemporaryDirectory(prefix="history-raw-")
        try:
            self.db = sqlite3.connect(self.directory.name + "/raw.sqlite")
            self.db.execute(f"PRAGMA max_page_count={max_bytes // 4096}")
            self.db.execute("PRAGMA cache_size=-2048")
            self.db.execute("PRAGMA temp_store=FILE")
            self.db.execute("PRAGMA mmap_size=0")
            self.db.executescript("""
                CREATE TABLE durable (id TEXT PRIMARY KEY, timestamp REAL, data TEXT);
                CREATE INDEX durable_time ON durable(timestamp,id);
                CREATE TABLE live (id TEXT PRIMARY KEY, timestamp REAL, data TEXT);
                CREATE INDEX live_time ON live(timestamp,id);
                CREATE TABLE affected (id TEXT PRIMARY KEY);
            """)
        except BaseException:
            self.close()
            raise

    def put(self, table, row):
        data = json.dumps(row, ensure_ascii=True)
        if table == "live":
            old = self.db.execute(
                "SELECT length(data) FROM live WHERE id=?", (row["id"],)
            ).fetchone()
            if old is None:
                self.live_count += 1
                self.live_bytes += 512
            self.live_bytes += len(data) - (old[0] if old else 0)
        self.db.execute(
            f"INSERT OR REPLACE INTO {table} VALUES (?,?,?)",
            (
                _id(row["id"]),
                _timestamp(row["timestamp_created"]),
                data,
            ),
        )

    def remove_live(self, flow_id):
        old = self.db.execute(
            "SELECT length(data) FROM live WHERE id=?", (flow_id,)
        ).fetchone()
        if old is not None:
            self.live_count -= 1
            self.live_bytes -= old[0] + 512
            self.db.execute("DELETE FROM live WHERE id=?", (flow_id,))

    def clear_live(self):
        self.db.execute("INSERT OR IGNORE INTO affected SELECT id FROM live")
        self.db.execute("DELETE FROM live")
        self.live_count = self.live_bytes = 0

    def mark(self, flow_id):
        self.db.execute("INSERT OR IGNORE INTO affected VALUES (?)", (_id(flow_id),))

    def metadata(self, flow_id):
        found = self.db.execute(
            "SELECT data FROM live WHERE id=?", (flow_id,)
        ).fetchone()
        live = found is not None
        if not live:
            if self.query.search is not None:
                return None
            found = self.db.execute(
                "SELECT data FROM durable WHERE id=?", (flow_id,)
            ).fetchone()
        if found is None:
            return None
        row = json.loads(found[0])
        timestamp = row["timestamp_created"]
        if (self.query.lower is not None and timestamp < self.query.lower) or (
            self.query.upper is not None and timestamp > self.query.upper
        ):
            return None
        if live:
            if self.filter_token and not row.get("search:" + self.filter_token, False):
                return None
            row["highlighted"] = bool(
                self.filter_token and row.get("highlight:" + self.filter_token)
            )
            sequence = self.db.execute(
                "SELECT data FROM durable WHERE id=?", (flow_id,)
            ).fetchone()
            row["sequence"] = (
                json.loads(sequence[0])["sequence"] if sequence else 2**63 - 1
            )
        selector = {
            "status": "status_code",
            "type": "type",
            "method": "method",
            "host": "host",
        }[self.query.page_options["group_by"]]
        value = row.get(selector)
        label = None
        if row.get(selector + "_oversized"):
            value = b"value too large to group\0" + flow_id.encode("utf-8")
            label = "value too large to group"
        elif selector in row.get("_blobs", ()):
            # Reserve a separate byte namespace for exceptional per-flow identities.
            value = b"ordinary\0" + value.encode("utf-8", "surrogateescape")
        elif isinstance(value, str):
            try:
                value.encode("utf-8")
            except UnicodeEncodeError:
                value = b"ordinary\0" + value.encode("utf-8", "surrogateescape")
        summary = {}
        for key in "timestamp_created type sequence method host path status_code request_size response_size payload_version live highlighted".split():
            item = row.get(key)
            if isinstance(item, str):
                item = item[
                    : 4096 if key == "path" else 64 if key == "method" else 1024
                ]
                item = item.encode("utf-8", "surrogateescape").decode(
                    "utf-8", "replace"
                )
            summary[key] = item
        return Metadata(flow_id, value, summary, label=label)

    @property
    def size_bytes(self):
        return sum(
            entry.stat().st_size for entry in os.scandir(self.directory.name)
        ) + (self.store.size_bytes if self.store else 0)

    def close(self):
        if self.store is not None:
            self.store.close()
        if db := getattr(self, "db", None):
            db.close()
        self.directory.cleanup()


@dataclass
class _GroupedSnapshot:
    snapshot: GroupSnapshot
    query: HistoryQuery
    context: tuple
    revision: tuple
    identity: tuple
    engine: _Engine
    expires: float


class HistoryReader:
    def __init__(
        self,
        *,
        snapshot_capacity: int = 16,
        snapshot_bytes: int = 2 * 1024 * 1024 * 1024,
        snapshot_ttl: int = 15,
        live_rows: int = 250000,
        live_bytes: int = 128 * 1024 * 1024,
        source_batch: int = 8192,
        source_bytes: int = 4 * 1024 * 1024,
    ):
        # Programmatic limits; HTTP clients cannot increase these resource bounds.
        limits = (
            snapshot_capacity,
            snapshot_bytes,
            snapshot_ttl,
            live_rows,
            live_bytes,
            source_batch,
            source_bytes,
        )
        if any(type(value) is not int or value < 1 for value in limits):
            raise ValueError("History limits must be positive finite integers")
        if snapshot_bytes < 32768:
            raise ValueError("Snapshot byte bound must be at least 32768")
        self.SNAPSHOT_CAPACITY = snapshot_capacity
        self.SNAPSHOT_BYTES = snapshot_bytes
        self.SNAPSHOT_TTL = snapshot_ttl
        self.LIVE_ROWS = live_rows
        self.LIVE_BYTES = live_bytes
        self.SOURCE_BATCH = source_batch
        self.SOURCE_BYTES = source_bytes
        self.generation = 0
        self.sizes = SizeCache()
        self._workers = asyncio.Semaphore(2)
        self._group_admission = asyncio.Semaphore(1)
        self._index_lock = threading.Lock()
        self._indexed_paths: dict[str, tuple] = {}
        # Scratch SQLite connections, queries, and eviction all have one owner.
        self._group_worker = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="history-groups"
        )
        self._snapshots: OrderedDict[str, _GroupedSnapshot] = OrderedDict()
        self._engines: OrderedDict[tuple, _Engine] = OrderedDict()
        self._owned_engines: set[_Engine] = set()
        self.live_index: LiveHistoryIndex | None = None
        self._filter_tokens: frozenset[str] = frozenset()
        self._expiry_pending = False
        self._expiry_timer: threading.Timer | None = None
        self._requests: dict[asyncio.Future, threading.Event] = {}
        self._preparing: set[threading.Event] = set()
        self._closed = False
        self._shutdown_task: asyncio.Task | None = None

    async def done(self):
        if self._shutdown_task is None:
            self._closed = True
            for cancel in self._requests.values():
                cancel.set()
            for cancel in self._preparing:
                cancel.set()

            def close_snapshots():
                if self._expiry_timer is not None:
                    self._expiry_timer.cancel()
                    # A firing timer must finish submitting before executor shutdown.
                    self._expiry_timer.join()
                    self._expiry_timer = None
                self._expiry_pending = False
                for token in list(self._snapshots):
                    self._discard_snapshot(token)
                for engine in self._owned_engines:
                    engine.close()
                self._engines.clear()
                self._owned_engines.clear()
                self.sizes.clear()

            async def shutdown():
                cancelled = False

                async def complete(future):
                    nonlocal cancelled
                    # Loop teardown cancels this task directly, not just done().
                    while True:
                        try:
                            return await asyncio.shield(future)
                        except asyncio.CancelledError:
                            if future.cancelled():
                                raise
                            cancelled = True

                try:
                    await complete(
                        asyncio.gather(*self._requests, return_exceptions=True)
                    )
                finally:
                    try:
                        await complete(
                            asyncio.get_running_loop().run_in_executor(
                                self._group_worker, close_snapshots
                            )
                        )
                    finally:
                        await complete(
                            asyncio.get_running_loop().run_in_executor(
                                None, self._group_worker.shutdown
                            )
                        )
                        if self.live_index is not None:
                            await complete(
                                asyncio_utils.create_task(
                                    self.live_index.done(),
                                    name="live history shutdown",
                                    keep_ref=True,
                                )
                            )
                if cancelled:
                    raise asyncio.CancelledError

            self._shutdown_task = asyncio_utils.create_task(
                shutdown(), name="history shutdown", keep_ref=True
            )
        await asyncio.shield(self._shutdown_task)

    def _expire_snapshots(self, timer_fired=False):
        if self._closed:
            return
        if timer_fired:
            self._expiry_pending = False
            self._expiry_timer = None
        for token, entry in list(self._snapshots.items()):
            if entry.expires <= time.monotonic():
                self._snapshots.pop(token).snapshot.close()
        for engine in list(self._owned_engines):
            if engine.expires <= time.monotonic() and not any(
                entry.engine is engine for entry in self._snapshots.values()
            ):
                for key, cached in list(self._engines.items()):
                    if cached is engine:
                        del self._engines[key]
                self._owned_engines.remove(engine)
                engine.close()
        if timer_fired:
            self._schedule_expiry()

    def _schedule_expiry(self):
        if (
            not self._closed
            and not self._expiry_pending
            and (self._snapshots or self._engines)
        ):
            self._expiry_pending = True
            deadline = min(
                [entry.expires for entry in self._snapshots.values()]
                + [engine.expires for engine in self._engines.values()]
            )
            timer = threading.Timer(
                max(0, deadline - time.monotonic()),
                self._group_worker.submit,
                args=(self._expire_snapshots, True),
            )
            timer.daemon = True
            self._expiry_timer = timer
            timer.start()

    def _discard_snapshot(self, token):
        entry = self._snapshots.pop(token, None)
        if entry is not None:
            entry.snapshot.close()

    async def grouped_page(
        self, path, query, live, cancel, *, instance_id, live_generation
    ):
        if self._closed:
            raise sqlite.StorageError("History reader is closed")
        # Never queue serialized grouped jobs while reserving regular capacity.
        if self._group_admission.locked() or self._workers.locked():
            raise HistoryBusy()
        await self._group_admission.acquire()
        await self._workers.acquire()
        self._preparing.add(cancel)
        try:
            filter_token = None
            if "snapshot" not in query.grouped and self.live_index is not None:
                self.live_index.prune(self._filter_tokens)
                filter_token = await self.live_index.register(
                    query, cancel, verify=False
                )
            if self._closed or cancel.is_set():
                raise sqlite.StorageError(
                    "History reader is closed or request cancelled"
                )
            context = (instance_id, self.generation, path)
            future = asyncio.get_running_loop().run_in_executor(
                self._group_worker,
                self._read_grouped,
                path,
                query,
                live,
                cancel,
                context,
                filter_token,
            )
        except BaseException:
            self._group_admission.release()
            self._workers.release()
            raise
        finally:
            self._preparing.discard(cancel)
        self._requests[future] = cancel
        future.add_done_callback(lambda f: self._requests.pop(f))
        future.add_done_callback(lambda _: self._group_admission.release())
        future.add_done_callback(lambda _: self._workers.release())
        try:
            page = await asyncio.shield(future)
            if self._closed:
                raise sqlite.StorageError("History reader is closed")
            return page
        except asyncio.CancelledError:
            cancel.set()

            def finished(f):
                if (
                    f.exception() is None
                    and "snapshot" not in query.grouped
                    and not self._closed
                ):
                    self._group_worker.submit(
                        self._discard_snapshot, f.result()["snapshot"]
                    )

            future.add_done_callback(finished)
            raise

    @staticmethod
    def _path_identity(path):
        stat = os.stat(path)
        return (os.path.realpath(path), stat.st_dev, stat.st_ino)

    def _source_batch(
        self,
        path,
        revision,
        identity,
        cancel,
        sql="SELECT 1",
        args=(),
        *,
        byte_limit=None,
    ):
        """Copy one bounded chunk and release its WAL reader before yielding it."""
        if cancel.is_set():
            raise sqlite.StorageError("History request cancelled")
        try:
            if self._path_identity(path) != identity:
                raise HistoryStale("History database path changed")
            with sqlite.SQLiteFlowReader(path) as reader:
                reader.db.set_progress_handler(lambda: int(cancel.is_set()), 1000)
                reader.db.execute("BEGIN")
                observed = reader.revision()
                if revision is not None and observed != revision:
                    raise HistoryStale("History database changed")
                cursor = reader.db.execute(sql, args)
                if byte_limit is None:
                    rows = cursor.fetchall()
                else:
                    rows = []
                    size = 0
                    deadline = time.monotonic() + 0.05
                    for row in cursor:
                        row_bytes = 128 + sum(
                            len(value.encode("utf-8"))
                            if isinstance(value, str)
                            else len(value)
                            if isinstance(value, bytes)
                            else 8
                            for value in row
                        )
                        # Permit one independently bounded value larger than the target.
                        if rows and size + row_bytes > byte_limit:
                            break
                        rows.append(row)
                        size += row_bytes
                        if size >= byte_limit or time.monotonic() >= deadline:
                            break
                cursor.close()
            if self._path_identity(path) != identity:
                raise HistoryStale("History database path changed")
            return observed, rows
        except OSError as error:
            raise HistoryStale("History database path changed") from error
        except sqlite3.Error as error:
            raise sqlite.StorageError("Cannot read grouped history metadata") from error

    def _journal(self, path, engine, after, target, cancel):
        if cancel.is_set():
            raise sqlite.StorageError("History request cancelled")
        if self._path_identity(path) != engine.identity:
            raise HistoryStale("History database path changed")
        with sqlite.SQLiteFlowReader(path) as reader:
            reader.db.execute("BEGIN")
            revision = reader.revision()
            if revision[0] != engine.revision[0]:
                raise HistoryStale("History database identity changed")
            changes = reader.changes(after, target, min(self.SOURCE_BATCH, 8192))
        return changes

    def _check_resources(self, candidate):
        engines = self._owned_engines | {candidate}
        if sum(engine.size_bytes for engine in engines) > self.SNAPSHOT_BYTES:
            raise SnapshotError("History scratch exceeds total byte bound")

    @staticmethod
    def _apply_journal(engine, changes):
        for change in changes:
            old = change["old_flow_id"]
            if old is not None:
                engine.mark(old)
                engine.db.execute("DELETE FROM durable WHERE id=?", (old,))
            if change["flow_id"] is not None:
                row = _durable(change)
                if (
                    engine.query.lower is None
                    or row["timestamp_created"] >= engine.query.lower
                ):
                    engine.put("durable", row)
                else:
                    engine.db.execute("DELETE FROM durable WHERE id=?", (row["id"],))
                engine.mark(row["id"])

    def _copy_live(self, engine, flows, cancel, *, initial):
        index = self.live_index
        if index is None:
            # Standalone callers without a View retain the flat-path adapter.
            rows = snapshot(
                flows,
                engine.query,
                cancel,
                self.sizes,
                max_rows=self.LIVE_ROWS,
                max_bytes=self.LIVE_BYTES,
            )
            engine.clear_live()
            for row in rows:
                row.update(live=True, payload_version=None)
                if row.get("matches"):
                    row["search:standalone"] = True
                row["highlight:standalone"] = row.get("highlighted", False)
                engine.put("live", row)
                engine.mark(row["id"])
            if engine.query.search is not None or engine.query.highlight is not None:
                engine.filter_token = "standalone"
            return
        with index.read_cut(
            cancel, bounds=(engine.query.lower, engine.query.upper)
        ) as (db, cut):
            if initial:
                for count, row in enumerate(index.rows(db, cut), 1):
                    if cancel.is_set():
                        raise sqlite.StorageError("History request cancelled")
                    engine.put("live", row)
                    if count % 1024 == 0:
                        engine.db.commit()
                        self._check_resources(engine)
                engine.live_cut = cut
            else:
                while engine.live_cut < cut:
                    changes = list(index.changes(db, engine.live_cut, cut, limit=8192))
                    if not changes:
                        engine.live_cut = cut
                        break
                    for sequence, op, flow_id, row in changes:
                        if cancel.is_set():
                            raise sqlite.StorageError("History request cancelled")
                        if op == "clear":
                            engine.clear_live()
                        else:
                            engine.mark(flow_id)
                            if op == "remove":
                                engine.remove_live(flow_id)
                            else:
                                engine.put("live", row)
                        engine.live_cut = sequence
                    engine.db.commit()
                    self._check_resources(engine)
        engine.db.commit()

    def _build_engine(self, path, query, flows, cancel, context, filter_token):
        if cancel.is_set():
            raise sqlite.StorageError("History request cancelled")
        identity = self._path_identity(path)
        while not self._index_lock.acquire(timeout=0.05):
            if cancel.is_set():
                raise sqlite.StorageError("History request cancelled")
        try:
            if cancel.is_set():
                raise sqlite.StorageError("History request cancelled")
            if self._indexed_paths.get(path) != identity:
                sqlite.ensure_history_index(path, cancel=cancel.is_set)
                if self._path_identity(path) != identity:
                    raise HistoryStale("History database path changed")
                self._indexed_paths[path] = identity
        finally:
            self._index_lock.release()
        engine = _Engine(query, context, identity, self.SNAPSHOT_BYTES)
        engine.filter_token = filter_token
        try:
            revision, maximum = self._source_batch(
                path,
                None,
                identity,
                cancel,
                "SELECT coalesce(max(sequence),0) FROM flows",
            )
            engine.revision = revision
            sequence = 0
            lower = query.lower if query.lower is not None else -MAX_TIMESTAMP
            upper = query.upper if query.upper is not None else MAX_TIMESTAMP
            if query.search is None:
                while True:
                    _, batch = self._source_batch(
                        path,
                        None,
                        identity,
                        cancel,
                        f"SELECT {_PROJECTION} FROM flows NOT INDEXED WHERE sequence>? AND sequence<=? AND timestamp>=? AND timestamp<=? ORDER BY sequence LIMIT ?",
                        (
                            sequence,
                            maximum[0][0],
                            lower,
                            upper,
                            min(self.SOURCE_BATCH, 8192),
                        ),
                        byte_limit=self.SOURCE_BYTES,
                    )
                    if not batch:
                        break
                    for row in batch:
                        engine.put("durable", _durable(dict(zip(_COLUMNS, row))))
                    sequence = batch[-1][0]
                    engine.db.commit()
                    self._check_resources(engine)
            self._copy_live(engine, flows, cancel, initial=True)
            # Fix R1 after the live cut. Writes after R1 belong to the next refresh.
            target, _ = self._source_batch(path, None, identity, cancel)
            if target[0] != revision[0]:
                raise HistoryStale("History database identity changed")
            after = revision[1]
            while query.search is None and after < target[1]:
                changes = self._journal(path, engine, after, target[1], cancel)
                if not changes:
                    raise sqlite.StorageError("Incomplete history change journal")
                self._apply_journal(engine, changes)
                after = changes[-1]["revision"]
                engine.db.commit()
                self._check_resources(engine)
            engine.revision = target
            engine.db.commit()

            def source(event):
                for (flow_id,) in engine.db.execute(
                    "SELECT id FROM live UNION ALL SELECT id FROM durable WHERE NOT EXISTS (SELECT 1 FROM live WHERE live.id=durable.id)"
                ):
                    if event.is_set():
                        raise sqlite.StorageError("History request cancelled")
                    metadata = engine.metadata(flow_id)
                    if metadata is not None:
                        yield metadata

            available = (
                self.SNAPSHOT_BYTES
                - engine.size_bytes
                - sum(cached.size_bytes for cached in self._owned_engines)
            )
            if available < 32768:
                raise SnapshotError("History scratch exceeds total byte bound")
            engine.store = GroupStore.build(
                source,
                cancel=cancel,
                max_bytes=available,
                max_value_bytes=None,
                host=query.page_options["group_by"] == "host",
                max_versions=self.SNAPSHOT_CAPACITY,
                grace_seconds=self.SNAPSHOT_TTL,
            )
            self._check_resources(engine)
            engine.db.execute("DELETE FROM affected")
            engine.db.commit()
            return engine
        except BaseException:
            engine.close()
            raise

    def _refresh_engine(self, path, engine, query, flows, cancel):
        if self._path_identity(path) != engine.identity:
            raise HistoryStale("History database path changed")
        old_query = engine.query
        old_cut = engine.live_cut
        old_revision = engine.revision
        try:
            engine.query = query
            self._copy_live(engine, flows, cancel, initial=False)
            old_lower = (
                old_query.lower if old_query.lower is not None else -MAX_TIMESTAMP
            )
            old_upper = (
                old_query.upper if old_query.upper is not None else MAX_TIMESTAMP
            )
            lower = query.lower if query.lower is not None else -MAX_TIMESTAMP
            upper = query.upper if query.upper is not None else MAX_TIMESTAMP
            if (lower, upper) != (old_lower, old_upper):
                if query.search is None:
                    _, maximum = self._source_batch(
                        path,
                        None,
                        engine.identity,
                        cancel,
                        "SELECT coalesce(max(sequence),0) FROM flows",
                    )
                    intervals = []
                    if lower < old_lower:
                        intervals.append(("<", old_lower))
                    if upper > old_upper:
                        intervals.append((">", old_upper))
                    for operator, boundary in intervals:
                        cursor = (-math.inf, 0, "")
                        while True:
                            _, batch = self._source_batch(
                                path,
                                None,
                                engine.identity,
                                cancel,
                                f"SELECT {_PROJECTION} FROM flows WHERE timestamp>=? AND timestamp<=? AND timestamp{operator}? AND sequence<=? AND (timestamp,sequence,flow_id)>(?,?,?) ORDER BY timestamp,sequence,flow_id LIMIT ?",
                                (
                                    lower,
                                    upper,
                                    boundary,
                                    maximum[0][0],
                                    *cursor,
                                    min(self.SOURCE_BATCH, 8192),
                                ),
                                byte_limit=self.SOURCE_BYTES,
                            )
                            if not batch:
                                break
                            for record in batch:
                                row = _durable(dict(zip(_COLUMNS, record)))
                                engine.put("durable", row)
                                engine.mark(row["id"])
                            cursor = (batch[-1][2], batch[-1][0], batch[-1][1])
                            engine.db.commit()
                            self._check_resources(engine)
                for table in ("durable", "live"):
                    if table == "durable" and query.search is not None:
                        continue
                    engine.db.execute(
                        f"INSERT OR IGNORE INTO affected SELECT id FROM {table} WHERE timestamp>=? AND timestamp<=? AND (timestamp<? OR timestamp>?) UNION SELECT id FROM {table} WHERE timestamp>=? AND timestamp<=? AND (timestamp<? OR timestamp>?)",
                        (
                            old_lower,
                            old_upper,
                            lower,
                            upper,
                            lower,
                            upper,
                            old_lower,
                            old_upper,
                        ),
                    )
            current, _ = self._source_batch(path, None, engine.identity, cancel)
            if current[0] != old_revision[0]:
                raise HistoryStale("History database identity changed")
            after = old_revision[1]
            while query.search is None and after < current[1]:
                changes = self._journal(path, engine, after, current[1], cancel)
                if not changes:
                    raise sqlite.StorageError("Incomplete history change journal")
                self._apply_journal(engine, changes)
                after = changes[-1]["revision"]
                engine.db.commit()
                self._check_resources(engine)
            engine.db.commit()
            self._check_resources(engine)
            assert engine.store is not None
            while True:
                ids = engine.db.execute("SELECT id FROM affected LIMIT 512").fetchall()
                if not ids:
                    break
                updates = {flow_id: engine.metadata(flow_id) for (flow_id,) in ids}
                engine.store.apply(updates, cancel=cancel)
                engine.db.executemany("DELETE FROM affected WHERE id=?", ids)
                engine.db.commit()
                self._check_resources(engine)
            if query.lower is not None:
                engine.db.execute(
                    "DELETE FROM durable WHERE timestamp<?", (query.lower,)
                )
                engine.db.commit()
            engine.revision = current
        except BaseException:
            engine.db.rollback()
            engine.query = old_query
            engine.live_cut = old_cut
            engine.revision = old_revision
            # Partly applied refreshes are never reusable. Published versions survive.
            raise

    def _read_grouped(self, path, query, flows, cancel, context, filter_token):
        try:
            return self._grouped(path, query, flows, cancel, context, filter_token)
        except SnapshotError as error:
            if cancel.is_set() and "snapshot" not in query.grouped:
                raise sqlite.StorageError("History request cancelled") from error
            raise
        except (OSError, ValueError, sqlite3.Error) as error:
            raise sqlite.StorageError(
                "Cannot build grouped history snapshot"
            ) from error

        finally:
            self._filter_tokens = frozenset(
                engine.filter_token
                for engine in self._owned_engines
                if engine.filter_token is not None
            )

    def _grouped(self, path, query, flows, cancel, context, filter_token):
        self._expire_snapshots()
        token = query.grouped.get("snapshot") or query.grouped.get("refresh")
        entry = self._snapshots.get(token) if token else None
        if token:
            continuation = "snapshot" in query.grouped
            if entry is None or entry.context != context:
                raise HistoryStale("Snapshot expired or query changed")
            if continuation:
                valid = query.binding in (
                    entry.query.binding,
                    (
                        "absolute",
                        (entry.query.lower, entry.query.upper),
                        *entry.query.binding[2:],
                    ),
                )
            else:
                valid = query.binding[2:] == entry.query.binding[2:] and (
                    query.binding[0] == entry.query.binding[0]
                    or query.binding[0] == "absolute"
                )
            if not valid:
                raise HistoryStale("Snapshot expired or query changed")
            if continuation:
                with entry.snapshot.query_scope(cancel):
                    result = self._grouped_result(path, entry, query, cancel)
                return result | {
                    "snapshot": token,
                    "lower": entry.query.lower,
                    "upper": entry.query.upper,
                }
        if len(self._snapshots) >= self.SNAPSHOT_CAPACITY:
            raise SnapshotError("History snapshot retention capacity reached")
        key = (context, query.binding)
        engine = entry.engine if entry else self._engines.get(key)
        if engine is not None and not engine.usable:
            raise HistoryStale("History engine expired; request a new snapshot")
        if engine is None:
            if len(self._owned_engines) >= self.SNAPSHOT_CAPACITY:
                raise SnapshotError("History engine capacity reached")
            engine = self._build_engine(
                path, query, flows, cancel, context, filter_token
            )
            self._engines[key] = engine
            self._owned_engines.add(engine)
        else:
            try:
                self._refresh_engine(path, engine, query, flows, cancel)
            except BaseException:
                engine.usable = False
                # Retain old pages, but never reuse a potentially partial coordinator.
                for engine_key, cached in list(self._engines.items()):
                    if cached is engine:
                        del self._engines[engine_key]
                raise
        assert engine.store is not None
        built = engine.store.publish(cancel=cancel)
        entry = _GroupedSnapshot(
            built,
            engine.query,
            context,
            (engine.revision, engine.live_cut),
            engine.identity,
            engine,
            time.monotonic() + self.SNAPSHOT_TTL,
        )
        token = "s1:" + uuid.uuid4().hex
        try:
            with built.query_scope(cancel):
                result = self._grouped_result(path, entry, query, cancel)
            if cancel.is_set():
                raise sqlite.StorageError("History request cancelled")
            self._snapshots[token] = entry
            engine.expires = entry.expires
            self._schedule_expiry()
            return result | {
                "snapshot": token,
                "lower": entry.query.lower,
                "upper": entry.query.upper,
            }
        except BaseException:
            built.close()
            self._expire_snapshots()
            raise

    def _grouped_result(self, path, entry, query, cancel):
        scratch = entry.snapshot
        offset = query.page_options.get("offset", 0)
        limit = query.page_options["limit"]
        result = {
            "count": scratch.member_count,
            "group_count": scratch.group_count,
            "offset": offset,
        }

        if "lookup_group" in query.grouped:
            return result | {"group": scratch.group(query.grouped["lookup_group"])}
        if "lookup_groups" in query.grouped:
            return result | {
                "groups": [scratch.group(key) for key in query.grouped["lookup_groups"]]
            }
        if "lookup_flow" in query.grouped:
            found = scratch.locate(query.grouped["lookup_flow"])
            return result | (
                found if found else {"group": None, "member_ordinal": None}
            )
        if "group_key" not in query.grouped:
            return result | {"groups": scratch.groups(offset=offset, limit=limit)}
        key = query.grouped["group_key"]
        group = scratch.group(key)
        summaries = []
        for member in scratch.members(key, offset=offset, limit=limit):
            row = dict(member["summary"])
            row["id"] = member["flow_id"]
            row["sequence"] = str(row["sequence"])
            row["cursor"] = [
                row["timestamp_created"],
                row["sequence"],
                member["flow_id"],
            ]
            summaries.append(row)
        return result | {
            "count": group["count"] if group else 0,
            "rows": summaries,
            "anchor_position": None,
            "selected_eligible": None,
        }

    def update(self, flows: Iterable[flow.Flow]):
        for f in flows:
            self.sizes.remove(f)
            if self.live_index is not None:
                self.live_index.sizes.remove(f)

    async def page(
        self,
        path: str,
        query: HistoryQuery,
        live: Iterable[flow.Flow],
        cancel: threading.Event,
        *,
        count_only: bool = False,
    ) -> dict:
        if self._closed:
            raise sqlite.StorageError("History reader is closed")
        if self._workers.locked():
            raise HistoryBusy()
        await self._workers.acquire()
        try:
            future = asyncio.get_running_loop().run_in_executor(
                None, self._read, path, query, live, cancel, count_only
            )
        except BaseException:
            self._workers.release()
            raise
        self._requests[future] = cancel
        future.add_done_callback(lambda f: self._requests.pop(f))
        # A cancelled HTTP task must not free a slot while SQLite still runs.
        future.add_done_callback(lambda _: self._workers.release())
        try:
            result = await asyncio.shield(future)
            if self._closed:
                raise sqlite.StorageError("History reader is closed")
            return result
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
            identity = self._path_identity(path)
            if self._indexed_paths.get(path) != identity:
                sqlite.ensure_history_index(path, cancel=cancel.is_set)
                if cancel.is_set():
                    raise sqlite.StorageError("History request cancelled")
                if self._path_identity(path) != identity:
                    raise sqlite.StorageError("History database path changed")
                self._indexed_paths[path] = identity
        except OSError as error:
            raise sqlite.StorageError("Cannot access history database") from error
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
        for row in page.get("rows", []):
            row["sequence"] = str(row["sequence"])
            row["cursor"] = [row["timestamp_created"], row["sequence"], row["id"]]
        return page
