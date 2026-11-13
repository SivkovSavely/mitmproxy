"""Bounded metadata extraction from a single current-version native flow.

Unknown byte payloads are skipped; Unicode is validated incrementally.
This is not a replacement for full Flow.from_state semantic validation.
"""

import codecs
import math
from dataclasses import dataclass
from typing import BinaryIO
from typing import Iterator

from mitmproxy.io.sqlite import StorageError
from mitmproxy.net.dns import domain_names


class UnsupportedVersion(Exception):
    pass


@dataclass(frozen=True)
class Node:
    start: int
    end: int
    tag: bytes


class Scanner:
    def __init__(self, stream: BinaryIO):
        self.stream = stream

    def read(self, position: int, size: int) -> bytes:
        self.stream.seek(position)
        chunks = []
        while size:
            chunk = self.stream.read(min(size, 65536))
            if not chunk:
                raise ValueError("truncated native record")
            chunks.append(chunk)
            size -= len(chunk)
        return b"".join(chunks)

    def node(self, position: int, limit: int) -> Node:
        digits = bytearray()
        while position < limit:
            c = self.read(position, 1)
            position += 1
            if c == b":":
                break
            if not b"0" <= c <= b"9" or len(digits) >= 12:
                raise ValueError("invalid length prefix")
            digits.extend(c)
        else:
            raise ValueError("truncated length prefix")
        if not digits:
            raise ValueError("empty length prefix")
        end = position + int(digits)
        if end >= limit:
            raise ValueError("native value exceeds its boundary")
        tag = self.read(end, 1)
        if tag not in (b",", b";", b"#", b"^", b"!", b"~", b"]", b"}"):
            raise ValueError("invalid type tag")
        return Node(position, end, tag)

    def value(self, node: Node, tag: bytes, cap: int = 1048576):
        if node.tag != tag or node.end - node.start > cap:
            raise ValueError("invalid or oversized scalar")
        data = self.read(node.start, node.end - node.start)
        if tag == b";":
            return data.decode("utf8")
        if tag == b"#":
            return int(data)
        if tag == b"^":
            return float(data)
        if tag == b"!":
            if data not in (b"true", b"false"):
                raise ValueError("invalid boolean")
            return data == b"true"
        return data

    def children(self, node: Node) -> Iterator[Node]:
        if node.tag not in (b"]", b"}"):
            raise ValueError("expected container")
        pos = node.start
        while pos < node.end:
            child = self.node(pos, node.end)
            yield child
            pos = child.end + 1

    def validate(self, node: Node, depth: int = 0) -> None:
        if depth > 128:
            raise ValueError("native nesting exceeds limit")
        if node.tag in (b"]", b"}"):
            children = iter(self.children(node))
            for child in children:
                if node.tag == b"}":
                    if child.tag in (b"]", b"}"):
                        raise ValueError("invalid dictionary key")
                    self.validate(child, depth + 1)
                    try:
                        child = next(children)
                    except StopIteration:
                        raise ValueError("dictionary key without value") from None
                self.validate(child, depth + 1)
        elif node.tag in (b"#", b"^", b"!"):
            self.value(node, node.tag)
        elif node.tag == b";":
            self.validate_utf8(node)
        elif node.tag == b"~" and node.start != node.end:
            raise ValueError("invalid null")

    def validate_utf8(self, node: Node) -> None:
        decoder = codecs.getincrementaldecoder("utf8")()
        position = node.start
        while position < node.end:
            size = min(node.end - position, 65536)
            decoder.decode(self.read(position, size), final=False)
            position += size
        decoder.decode(b"", final=True)

    def fields(self, node: Node, names: tuple[str, ...]) -> dict[str, Node]:
        if node.tag != b"}":
            raise ValueError("expected dictionary")
        result = {}
        children = iter(self.children(node))
        max_key_length = max(map(len, names), default=0)
        for key in children:
            keystr = None
            if key.tag == b";" and key.end - key.start <= max_key_length:
                raw = self.read(key.start, key.end - key.start)
                keystr = next(
                    (name for name in names if raw == name.encode("ascii")), None
                )
            value = next(children)
            if keystr in names:
                if keystr in result:
                    raise ValueError("duplicate metadata key")
                result[keystr] = value
        return result

    def length(self, node: Node) -> int | None:
        if node.tag == b"~":
            return None
        if node.tag != b",":
            raise ValueError("expected bytes or null")
        return node.end - node.start

    def schema(self, node: Node, fields: dict[str, bytes]) -> dict[str, Node]:
        result = self.fields(node, tuple(fields))
        for name, tags in fields.items():
            if result[name].tag not in tuple(bytes([tag]) for tag in tags):
                raise ValueError("invalid field type")
        return result

    def connection(self, node: Node, client: bool) -> None:
        schema = {
            "peername": b"]~",
            "sockname": b"]~",
            "id": b";",
            "transport_protocol": b";",
            "error": b";~",
            "tls": b"!",
            "certificate_list": b"]",
            "alpn": b",~",
            "alpn_offers": b"]",
            "cipher": b";~",
            "cipher_list": b"]",
            "tls_version": b";~",
            "sni": b";~",
            "timestamp_start": b"#^~",
            "timestamp_end": b"#^~",
            "timestamp_tls_setup": b"#^~",
        }
        if client:
            schema.update(mitmcert=b",~", proxy_mode=b";")
        else:
            schema.update(address=b"]~", timestamp_tcp_setup=b"#^~", via=b"]~")
        fields = self.schema(node, schema)
        for name, tag in (
            ("certificate_list", b","),
            ("alpn_offers", b","),
            ("cipher_list", b";"),
        ):
            for entry in self.children(fields[name]):
                if entry.tag != tag:
                    raise ValueError("invalid connection list")
        for name in (
            ("peername", "sockname") if client else ("peername", "sockname", "address")
        ):
            address = fields[name]
            if address.tag == b"]":
                parts = iter(self.children(address))
                if next(parts).tag != b";" or next(parts).tag != b"#":
                    raise ValueError("invalid address")
                for extra in parts:
                    if extra.tag != b"#":
                        raise ValueError("invalid address")

    def http_message(self, node: Node, request: bool) -> dict[str, Node]:
        schema = {
            "http_version": b",",
            "headers": b"]",
            "content": b",~",
            "trailers": b"]~",
            "timestamp_start": b"#^",
            "timestamp_end": b"#^~",
        }
        if request:
            schema.update(
                host=b";",
                port=b"#",
                method=b",",
                scheme=b",",
                authority=b",",
                path=b",",
            )
        else:
            schema.update(status_code=b"#", reason=b",")
        fields = self.schema(node, schema)
        for name in ("headers", "trailers"):
            headers = fields[name]
            if headers.tag == b"~":
                continue
            for pair in self.children(headers):
                if pair.tag != b"]":
                    raise ValueError("invalid header pair")
                items = iter(self.children(pair))
                if (
                    next(items).tag != b","
                    or next(items).tag != b","
                    or next(items, None) is not None
                ):
                    raise ValueError("invalid header pair")
        return fields

    def messages(self, node: Node, websocket: bool) -> tuple[int, int]:
        if node.tag != b"]":
            raise ValueError("expected messages array")
        sizes = [0, 0]
        for message in self.children(node):
            if message.tag != b"]":
                raise ValueError("expected message tuple")
            items = iter(self.children(message))
            if websocket:
                self.value(next(items), b"#")
            direction = self.value(next(items), b"!")
            length = self.length(next(items))
            if length is None:
                raise ValueError("missing message content")
            timestamp = next(items)
            if timestamp.tag not in (b"#", b"^"):
                raise ValueError("invalid message timestamp")
            if websocket:
                self.value(next(items), b"!")
                self.value(next(items), b"!")
            if next(items, None) is not None:
                raise ValueError("invalid message length")
            sizes[0 if direction else 1] += length
        return sizes[0], sizes[1]

    def dns_size(self, node: Node) -> int | None:
        if node.tag == b"~":
            return None
        fields = self.fields(
            node,
            (
                "id",
                "op_code",
                "reserved",
                "response_code",
                "query",
                "authoritative_answer",
                "truncation",
                "recursion_desired",
                "recursion_available",
                "timestamp",
                "questions",
                "answers",
                "authorities",
                "additionals",
            ),
        )
        for key in (
            "query",
            "authoritative_answer",
            "truncation",
            "recursion_desired",
            "recursion_available",
        ):
            if fields[key].tag != b"!":
                raise ValueError("invalid DNS flag")
        if fields["timestamp"].tag not in (b"#", b"^", b"~"):
            raise ValueError("invalid DNS timestamp")
        for key, maximum in (
            ("id", 65535),
            ("op_code", 15),
            ("reserved", 7),
            ("response_code", 15),
        ):
            if not 0 <= self.value(fields[key], b"#") <= maximum:
                raise ValueError("invalid DNS header")
        size = 12
        for section in ("questions", "answers", "authorities", "additionals"):
            array = fields[section]
            if array.tag != b"]":
                raise ValueError("invalid DNS section")
            count = 0
            for entry in self.children(array):
                count += 1
                if count > 65535:
                    raise ValueError("too many DNS records")
                rr = self.fields(entry, ("name", "type", "class_", "ttl", "data"))
                size += len(domain_names.pack(self.value(rr["name"], b";")))
                for key in ("type", "class_"):
                    if not 0 <= self.value(rr[key], b"#") <= 65535:
                        raise ValueError("invalid DNS record")
                size += 4 if section == "questions" else 10
                if section != "questions":
                    if not 0 <= self.value(rr["ttl"], b"#") <= 0xFFFFFFFF:
                        raise ValueError("invalid DNS TTL")
                    length = self.length(rr["data"])
                    if length is None or length > 65535:
                        raise ValueError("invalid DNS data")
                    size += length
        return size


def metadata(stream: BinaryIO, raw_length: int) -> tuple:
    stage = "framing"
    try:
        scanner = Scanner(stream)
        stream.seek(0, 2)
        if raw_length < 0 or stream.tell() != raw_length:
            raise ValueError("invalid native record length")
        root = scanner.node(0, raw_length)
        if root.end + 1 != raw_length or root.tag != b"}":
            raise ValueError("invalid native record framing")
        stage = "structure"
        scanner.validate(root)
        stage = "flow state"
        state = scanner.fields(
            root,
            (
                "id",
                "version",
                "type",
                "timestamp_created",
                "request",
                "response",
                "websocket",
                "messages",
                "client_conn",
                "server_conn",
                "error",
                "intercepted",
                "is_replay",
                "marked",
                "metadata",
                "comment",
                "backup",
            ),
        )
        if scanner.value(state["version"], b"#") != 21:
            raise UnsupportedVersion
        for name, tags in (
            ("intercepted", (b"!",)),
            ("is_replay", (b";", b"~")),
            ("marked", (b";",)),
            ("metadata", (b"}",)),
            ("comment", (b";",)),
        ):
            if state[name].tag not in tags:
                raise ValueError("invalid common field")
        if "backup" in state and state["backup"].tag not in (b"}", b"~"):
            raise ValueError("invalid backup")
        if state["error"].tag != b"~":
            scanner.schema(state["error"], {"msg": b";", "timestamp": b"#^"})
        scanner.connection(state["client_conn"], True)
        scanner.connection(state["server_conn"], False)
        ident = scanner.value(state["id"], b";")
        kind = scanner.value(state["type"], b";")
        ts = state["timestamp_created"]
        if ts.tag not in (b"#", b"^"):
            raise ValueError("invalid timestamp")
        timestamp = scanner.value(ts, ts.tag)
        if not math.isfinite(timestamp):
            raise ValueError("invalid timestamp")
        method = host = path = status = request_size = response_size = None
        stage = "protocol state"
        if kind == "http":
            request = state["request"]
            response = state["response"]
            if request.tag == b"~":
                raise ValueError("missing request")
            if request.tag != b"~":
                req = scanner.http_message(request, True)
                method = (
                    scanner.value(req["method"], b",")
                    .decode("utf8", "surrogateescape")
                    .upper()
                )
                host = scanner.value(req["host"], b";")
                path = scanner.value(req["path"], b",").decode(
                    "utf8", "surrogateescape"
                )
                request_size = scanner.length(req["content"])
            if response.tag != b"~":
                resp = scanner.http_message(response, False)
                status = scanner.value(resp["status_code"], b"#")
                response_size = scanner.length(resp["content"])
            ws = state["websocket"]
            if ws.tag != b"~":
                ws_fields = scanner.schema(
                    ws,
                    {
                        "messages": b"]",
                        "closed_by_client": b"!~",
                        "close_code": b"#~",
                        "close_reason": b";~",
                        "timestamp_end": b"#^~",
                    },
                )
                sizes = scanner.messages(ws_fields["messages"], True)
                if request_size is not None:
                    request_size += sizes[0]
                if response_size is not None:
                    response_size += sizes[1]
        elif kind in ("tcp", "udp"):
            request_size, response_size = scanner.messages(state["messages"], False)
        elif kind == "dns":
            if state["request"].tag == b"~":
                raise ValueError("missing request")
            request_size = scanner.dns_size(state["request"])
            response_size = scanner.dns_size(state["response"])
        else:
            raise ValueError("unsupported flow type")
        return (
            ident,
            timestamp,
            kind,
            method,
            host,
            path,
            status,
            request_size,
            response_size,
        )
    except UnsupportedVersion:
        raise StorageError(
            "bounded migration is not available for this flow version"
        ) from None
    except (
        ValueError,
        TypeError,
        KeyError,
        StopIteration,
        OSError,
        UnicodeError,
        OverflowError,
        StorageError,
    ) as e:
        raise StorageError(
            f"Invalid native flow during {stage}: {type(e).__name__}"
        ) from None
