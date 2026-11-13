import io

import pytest

from mitmproxy.io import sqlite
from mitmproxy.io import sqlite_native
from mitmproxy.io import tnetstring
from mitmproxy.io.io import FlowReader
from mitmproxy.test import tflow


def test_flow_metadata():
    for flow in tflow.tflows():
        raw = tnetstring.dumps(flow.get_state())
        assert sqlite_native.metadata(io.BytesIO(raw), len(raw)) == sqlite._metadata(
            flow
        )


def test_unsupported_version():
    state = tflow.tflow().get_state()
    state["version"] = 20
    raw = tnetstring.dumps(state)
    with pytest.raises(sqlite.StorageError, match="migration"):
        sqlite_native.metadata(io.BytesIO(raw), len(raw))


def scan(state):
    raw = tnetstring.dumps(state)
    return sqlite_native.metadata(io.BytesIO(raw), len(raw))


@pytest.mark.parametrize("content", [None, b""])
def test_http_content_and_path(content):
    flow = tflow.tflow(resp=True, ws=True)
    flow.request.raw_content = content
    flow.response.raw_content = content
    flow.request.data.path = b"/bad\xff"
    assert scan(flow.get_state()) == sqlite._metadata(flow)


def test_bounded_reads_and_backup():
    class Bounded(io.BytesIO):
        def read(self, size=-1):
            assert 0 <= size <= 65536
            return super().read(size)

    flow = tflow.tflow(resp=True)
    flow.request.raw_content = b"x" * (4 * 1024 * 1024)
    state = flow.get_state()
    state["backup"] = flow.get_state()
    raw = tnetstring.dumps(state)
    assert sqlite_native.metadata(Bounded(raw), len(raw)) == sqlite._metadata(flow)


@pytest.mark.parametrize(
    "raw", [b"", b"1:x?", b"2:x,", b"1:x,extra", b"3:1:a}", b"2:0:}", b"1:x}"]
)
def test_invalid_framing(raw):
    with pytest.raises(sqlite.StorageError):
        sqlite_native.metadata(io.BytesIO(raw), len(raw))


def test_invalid_state():
    state = tflow.tflow().get_state()
    state["type"] = "other"
    with pytest.raises(sqlite.StorageError, match="protocol state"):
        scan(state)
    state["type"] = "http"
    state["id"] = "x" * (1024 * 1024 + 1)
    with pytest.raises(sqlite.StorageError, match="flow state"):
        scan(state)


def test_depth_limit():
    state = tflow.tflow().get_state()
    nested = None
    for _ in range(130):
        nested = [nested]
    state["metadata"] = nested
    with pytest.raises(sqlite.StorageError, match="structure"):
        scan(state)


def test_duplicate_metadata_key():
    state = tflow.tflow().get_state()
    body = tnetstring.dumps(state).split(b":", 1)[1][:-1]
    body += tnetstring.dumps("id") + tnetstring.dumps("other")
    raw = str(len(body)).encode() + b":" + body + b"}"
    with pytest.raises(sqlite.StorageError, match="flow state"):
        sqlite_native.metadata(io.BytesIO(raw), len(raw))


@pytest.mark.parametrize(
    "messages",
    [None, [None], [(True, None, 1)], [(True, b"x", None)], [(True, b"x", 1, 2)]],
)
def test_invalid_messages(messages):
    state = tflow.ttcpflow().get_state()
    state["messages"] = messages
    with pytest.raises(sqlite.StorageError):
        scan(state)


@pytest.mark.parametrize(
    "field,value",
    [
        ("timestamp_created", None),
        ("request", []),
        ("response", {"status_code": 200, "content": 1}),
    ],
)
def test_invalid_http_fields(field, value):
    state = tflow.tflow().get_state()
    state[field] = value
    with pytest.raises(sqlite.StorageError):
        scan(state)


@pytest.mark.parametrize(
    "field,value",
    [
        ("id", 65536),
        ("questions", None),
        ("questions", [{"name": "x", "type": -1, "class_": 1}]),
        ("answers", [{"name": "x", "type": 1, "class_": 1, "ttl": -1, "data": b""}]),
        ("answers", [{"name": "x", "type": 1, "class_": 1, "ttl": 1, "data": None}]),
    ],
)
def test_invalid_dns(field, value):
    state = tflow.tdnsflow().get_state()
    state["request"][field] = value
    with pytest.raises(sqlite.StorageError):
        scan(state)


@pytest.mark.parametrize("inner", [b"1:1#", b"1:a;", b"1:x~", b"1:x!"])
def test_invalid_nested_tnetstring(inner):
    raw = str(len(inner)).encode() + b":" + inner + b"}"
    with pytest.raises(sqlite.StorageError):
        sqlite_native.metadata(io.BytesIO(raw), len(raw))


def test_mismatched_stream_length():
    with pytest.raises(sqlite.StorageError):
        sqlite_native.metadata(io.BytesIO(b"0:}"), 4)


def test_truncated_stream():
    class Truncated(io.BytesIO):
        def read(self, size=-1):
            return b""

    with pytest.raises(sqlite.StorageError, match="framing"):
        sqlite_native.metadata(Truncated(b"0:}"), 3)


@pytest.mark.parametrize("raw", [b":}", b"8:1:k;1:x~}", b"8:1:k;1:x!}"])
def test_invalid_scalars(raw):
    with pytest.raises(sqlite.StorageError):
        sqlite_native.metadata(io.BytesIO(raw), len(raw))


def test_children_rejects_scalar():
    scanner = sqlite_native.Scanner(io.BytesIO(b"0:,"))
    with pytest.raises(ValueError, match="container"):
        list(scanner.children(scanner.node(0, 3)))


def test_dns_section_count():
    state = tflow.tdnsflow().get_state()
    question = state["request"]["questions"][0]
    state["request"]["questions"] = [question] * 65536
    with pytest.raises(sqlite.StorageError, match="protocol state"):
        scan(state)


@pytest.mark.parametrize(
    "key",
    [
        "client_conn",
        "server_conn",
        "error",
        "intercepted",
        "is_replay",
        "marked",
        "metadata",
        "comment",
    ],
)
def test_missing_common_field(key):
    state = tflow.tflow().get_state()
    del state[key]
    with pytest.raises(KeyError):
        list(FlowReader(io.BytesIO(tnetstring.dumps(state))).stream())
    with pytest.raises(sqlite.StorageError):
        scan(state)


@pytest.mark.parametrize(
    "connection,key",
    [
        ("client_conn", "id"),
        ("server_conn", "address"),
        ("client_conn", "proxy_mode"),
        ("server_conn", "certificate_list"),
    ],
)
def test_missing_connection_field(connection, key):
    state = tflow.tflow().get_state()
    del state[connection][key]
    with pytest.raises(sqlite.StorageError):
        scan(state)


@pytest.mark.parametrize(
    "field",
    [
        "http_version",
        "headers",
        "trailers",
        "timestamp_start",
        "timestamp_end",
        "port",
        "scheme",
        "authority",
    ],
)
def test_missing_http_message_field(field):
    state = tflow.tflow().get_state()
    del state["request"][field]
    with pytest.raises(sqlite.StorageError):
        scan(state)


@pytest.mark.parametrize(
    "headers", [[(b"name",)], [(b"name", "value")], [b"invalid"], None]
)
def test_invalid_headers(headers):
    state = tflow.tflow().get_state()
    state["request"]["headers"] = headers
    with pytest.raises(sqlite.StorageError):
        scan(state)


def test_missing_websocket_field():
    state = tflow.tflow(ws=True).get_state()
    del state["websocket"]["close_code"]
    with pytest.raises(sqlite.StorageError):
        scan(state)


@pytest.mark.parametrize("timestamp", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_created_timestamp(timestamp):
    state = tflow.tflow().get_state()
    state["timestamp_created"] = timestamp
    with pytest.raises(sqlite.StorageError):
        scan(state)


@pytest.mark.parametrize("bad", [b"12:SECRETnumber#", b"11:SECRETfloat^"])
def test_error_does_not_leak_payload(bad):
    state = tflow.tflow().get_state()
    body = tnetstring.dumps(state).split(b":", 1)[1][:-1]
    body += tnetstring.dumps("extra") + bad
    raw = str(len(body)).encode() + b":" + body + b"}"
    with pytest.raises(sqlite.StorageError) as exc:
        sqlite_native.metadata(io.BytesIO(raw), len(raw))
    assert "SECRET" not in str(exc.value)


def test_error_does_not_leak_selected_text():
    state = tflow.tflow().get_state()
    state["id"] = "SECRETplaceholder"
    raw = tnetstring.dumps(state).replace(b"17:SECRETplaceholder;", b"7:SECRET\xff;")
    # Reframe after replacing the selected value with invalid UTF-8.
    body = raw.split(b":", 1)[1][:-1]
    raw = str(len(body)).encode() + b":" + body + b"}"
    with pytest.raises(sqlite.StorageError) as exc:
        sqlite_native.metadata(io.BytesIO(raw), len(raw))
    assert "SECRET" not in str(exc.value)


def test_error_does_not_leak_key():
    state = tflow.tflow().get_state()
    body = tnetstring.dumps(state).split(b":", 1)[1][:-1]
    body += b"7:SECRET\xff;0:~"
    raw = str(len(body)).encode() + b":" + body + b"}"
    with pytest.raises(sqlite.StorageError) as exc:
        sqlite_native.metadata(io.BytesIO(raw), len(raw))
    assert "SECRET" not in str(exc.value)


@pytest.mark.parametrize("factory", [tflow.tflow, tflow.tdnsflow])
def test_missing_request(factory):
    state = factory().get_state()
    state["request"] = None
    with pytest.raises(sqlite.StorageError):
        scan(state)


def test_no_leak_from_stream_storage_error():
    class BadStream(io.BytesIO):
        def read(self, size=-1):
            raise sqlite.StorageError("SECRET")

    with pytest.raises(sqlite.StorageError) as exc:
        sqlite_native.metadata(BadStream(b"0:}"), 3)
    assert "SECRET" not in str(exc.value)


def test_optional_backup_and_http_trailers():
    flow = tflow.tflow(resp=True)
    state = flow.get_state()
    del state["backup"]
    state["request"]["trailers"] = [(b"name", b"value")]
    assert scan(state) == sqlite._metadata(flow)


def test_dns_unicode_names_and_sections():
    flow = tflow.tdnsflow(resp=True)
    flow.request.questions[0].name = "bücher.example"
    flow.response.answers[0].name = "bücher.example"
    flow.response.authorities = flow.response.answers[:1]
    flow.response.additionals = flow.response.answers[1:]
    assert scan(flow.get_state()) == sqlite._metadata(flow)


@pytest.mark.parametrize(
    "field,value",
    [("intercepted", None), ("metadata", []), ("backup", 3)],
)
def test_invalid_common_field(field, value):
    state = tflow.tflow().get_state()
    state[field] = value
    with pytest.raises(sqlite.StorageError):
        scan(state)


@pytest.mark.parametrize(
    "field,value",
    [("certificate_list", [1]), ("peername", (1, 2)), ("sockname", ("host", 1, "bad"))],
)
def test_invalid_connection_field(field, value):
    state = tflow.tflow().get_state()
    state["client_conn"][field] = value
    with pytest.raises(sqlite.StorageError):
        scan(state)


@pytest.mark.parametrize("field,value", [("query", None), ("timestamp", "bad")])
def test_invalid_dns_field_types(field, value):
    state = tflow.tdnsflow().get_state()
    state["request"][field] = value
    with pytest.raises(sqlite.StorageError):
        scan(state)


def test_invalid_message_content_type():
    state = tflow.ttcpflow().get_state()
    state["messages"] = [(True, 123, 1)]
    with pytest.raises(sqlite.StorageError):
        scan(state)


def test_dns_name_error_does_not_leak():
    state = tflow.tdnsflow().get_state()
    state["request"]["questions"][0]["name"] = "SECRET..example"
    with pytest.raises(sqlite.StorageError) as exc:
        scan(state)
    assert "SECRET" not in str(exc.value)


def test_arbitrary_long_metadata_key_is_valid():
    state = tflow.tflow().get_state()
    state["metadata"] = {"k" * 5000: "value"}
    raw = tnetstring.dumps(state)
    assert list(FlowReader(io.BytesIO(raw)).stream())
    parsed = next(FlowReader(io.BytesIO(raw)).stream())
    assert sqlite_native.metadata(io.BytesIO(raw), len(raw)) == sqlite._metadata(parsed)


@pytest.mark.parametrize("field", ["comment", "metadata", "key"])
def test_invalid_utf8_unicode_scalar_rejected_safely(field):
    state = tflow.tflow().get_state()
    raw = tnetstring.dumps(state)
    if field == "comment":
        raw = raw.replace(b"7:comment;0:;", b"7:comment;1:\xff;", 1)
    elif field == "metadata":
        state["metadata"] = {"note": "foo"}
        raw = tnetstring.dumps(state).replace(b"4:note;3:foo;", b"4:note;1:\xff;", 1)
    else:
        raw = raw.replace(b"7:comment;", b"7:commen\xff;", 1)
    # Replacing values can alter the outer declared length.
    _, body = raw.split(b":", 1)
    body = body[:-1]
    raw = str(len(body)).encode() + b":" + body + b"}"
    with pytest.raises(sqlite.StorageError) as exc:
        sqlite_native.metadata(io.BytesIO(raw), len(raw))
    assert "utf" not in str(exc.value).lower()
    assert "\\xff" not in str(exc.value).lower()


def test_utf8_character_crossing_read_boundary():
    state = tflow.tflow().get_state()
    state["comment"] = "a" * 65535 + "é"
    raw = tnetstring.dumps(state)

    class Bounded(io.BytesIO):
        def read(self, size=-1):
            assert 0 <= size <= 65536
            return super().read(size)

    assert sqlite_native.metadata(Bounded(raw), len(raw))[0] == state["id"]


def test_scalar_metadata_keys_match_flow_reader():
    state = tflow.tflow().get_state()
    state["metadata"] = {1: "hello", None: 42, False: "boolean", b"bytes": "value"}
    raw = tnetstring.dumps(state)
    parsed = next(FlowReader(io.BytesIO(raw)).stream())
    assert sqlite_native.metadata(io.BytesIO(raw), len(raw)) == sqlite._metadata(parsed)


def test_unhashable_metadata_key_is_rejected():
    state = tflow.tflow().get_state()
    # Construct a dictionary containing a list key directly in native encoding.
    state["metadata"] = {}
    raw = tnetstring.dumps(state)
    empty = tnetstring.dumps({})
    invalid = tnetstring.dumps(["unhashable"]) + tnetstring.dumps("value")
    raw = raw.replace(empty, str(len(invalid)).encode() + b":" + invalid + b"}", 1)
    outer_body = raw.split(b":", 1)[1][:-1]
    raw = str(len(outer_body)).encode() + b":" + outer_body + b"}"
    with pytest.raises(sqlite.StorageError):
        sqlite_native.metadata(io.BytesIO(raw), len(raw))
