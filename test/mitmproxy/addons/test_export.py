import gzip
import os
import shlex
from unittest import mock

import pyperclip
import pytest

from mitmproxy import exceptions
from mitmproxy.addons import export  # heh
from mitmproxy.test import taddons
from mitmproxy.test import tflow
from mitmproxy.test import tutils


@pytest.fixture
def get_request():
    return tflow.tflow(
        req=tutils.treq(
            method=b"GET",
            content=b"",
            path=b"/path?a=foo&a=bar&b=baz",
            headers=((b"header", b"qvalue"), (b"content-length", b"0")),
        )
    )


@pytest.fixture
def get_response():
    return tflow.tflow(
        resp=tutils.tresp(status_code=404, content=b"Test Response Body")
    )


@pytest.fixture
def get_flow():
    return tflow.tflow(
        req=tutils.treq(method=b"GET", content=b"", path=b"/path?a=foo&a=bar&b=baz"),
        resp=tutils.tresp(status_code=404, content=b"Test Response Body"),
    )


@pytest.fixture
def post_request():
    return tflow.tflow(
        req=tutils.treq(method=b"POST", headers=(), content=bytes(range(256)))
    )


@pytest.fixture
def patch_request():
    return tflow.tflow(
        req=tutils.treq(method=b"PATCH", content=b"content", path=b"/path?query=param")
    )


@pytest.fixture
def tcp_flow():
    return tflow.ttcpflow()


@pytest.fixture
def udp_flow():
    return tflow.tudpflow()


@pytest.fixture
def websocket_flow():
    return tflow.twebsocketflow()


@pytest.fixture(scope="module")
def export_curl():
    e = export.Export()
    with taddons.context() as tctx:
        tctx.configure(e)
        yield export.curl_command


class TestExportCurlCommand:
    def test_get(self, export_curl, get_request):
        result = (
            """curl -H 'header: qvalue' 'http://address:22/path?a=foo&a=bar&b=baz'"""
        )
        assert export_curl(get_request) == result

    def test_post(self, export_curl, post_request):
        post_request.request.content = b"nobinarysupport"
        result = "curl -X POST http://address:22/path -d nobinarysupport"
        assert export_curl(post_request) == result

    def test_post_with_no_content_has_explicit_content_length_header(
        self, export_curl, post_request
    ):
        post_request.request.content = None
        result = "curl -H 'content-length: 0' -X POST http://address:22/path"
        assert export_curl(post_request) == result

    def test_fails_with_binary_data(self, export_curl, post_request):
        # shlex.quote doesn't support a bytes object
        # see https://github.com/python/cpython/pull/10871
        post_request.request.headers["Content-Type"] = "application/json; charset=utf-8"
        with pytest.raises(exceptions.CommandError):
            export_curl(post_request)

    def test_patch(self, export_curl, patch_request):
        result = """curl -H 'header: qvalue' -X PATCH 'http://address:22/path?query=param' -d content"""
        assert export_curl(patch_request) == result

    def test_tcp(self, export_curl, tcp_flow):
        with pytest.raises(exceptions.CommandError):
            export_curl(tcp_flow)

    def test_udp(self, export_curl, udp_flow):
        with pytest.raises(exceptions.CommandError):
            export_curl(udp_flow)

    def test_escape_single_quotes_in_body(self, export_curl):
        request = tflow.tflow(
            req=tutils.treq(method=b"POST", headers=(), content=b"'&#")
        )
        command = export_curl(request)
        assert shlex.split(command)[-2] == "-d"
        assert shlex.split(command)[-1] == "'&#"

    def test_expand_escaped(self, export_curl, post_request):
        post_request.request.content = b"foo\nbar"
        result = "curl -X POST http://address:22/path -d \"$(printf 'foo\\x0abar')\""
        assert export_curl(post_request) == result

    def test_no_expand_when_no_escaped(self, export_curl, post_request):
        post_request.request.content = b"foobar"
        result = "curl -X POST http://address:22/path -d foobar"
        assert export_curl(post_request) == result

    def test_strip_unnecessary(self, export_curl, get_request):
        get_request.request.headers.clear()
        get_request.request.headers["host"] = "address"
        get_request.request.headers[":authority"] = "address"
        get_request.request.headers["accept-encoding"] = "br"
        result = """curl --compressed 'http://address:22/path?a=foo&a=bar&b=baz'"""
        assert export_curl(get_request) == result

    # This tests that we always specify the original host in the URL, which is
    # important for SNI. If option `export_preserve_original_ip` is true, we
    # ensure that we still connect to the same IP by using curl's `--resolve`
    # option.
    def test_correct_host_used(self, get_request):
        e = export.Export()
        with taddons.context() as tctx:
            tctx.configure(e)

            get_request.request.headers["host"] = "domain:22"

            result = """curl -H 'header: qvalue' -H 'host: domain:22' 'http://domain:22/path?a=foo&a=bar&b=baz'"""
            assert export.curl_command(get_request) == result

            tctx.options.export_preserve_original_ip = True
            result = (
                """curl --resolve 'domain:22:[192.168.0.1]' -H 'header: qvalue' -H 'host: domain:22' """
                """'http://domain:22/path?a=foo&a=bar&b=baz'"""
            )
            assert export.curl_command(get_request) == result


class TestExportHttpieCommand:
    def test_get(self, get_request):
        result = (
            """http GET 'http://address:22/path?a=foo&a=bar&b=baz' 'header: qvalue'"""
        )
        assert export.httpie_command(get_request) == result

    def test_post(self, post_request):
        post_request.request.content = b"nobinarysupport"
        result = "http POST http://address:22/path <<< nobinarysupport"
        assert export.httpie_command(post_request) == result

    def test_fails_with_binary_data(self, post_request):
        # shlex.quote doesn't support a bytes object
        # see https://github.com/python/cpython/pull/10871
        post_request.request.headers["Content-Type"] = "application/json; charset=utf-8"
        with pytest.raises(exceptions.CommandError):
            export.httpie_command(post_request)

    def test_patch(self, patch_request):
        result = """http PATCH 'http://address:22/path?query=param' 'header: qvalue' <<< content"""
        assert export.httpie_command(patch_request) == result

    def test_tcp(self, tcp_flow):
        with pytest.raises(exceptions.CommandError):
            export.httpie_command(tcp_flow)

    def test_udp(self, udp_flow):
        with pytest.raises(exceptions.CommandError):
            export.httpie_command(udp_flow)

    def test_escape_single_quotes_in_body(self):
        request = tflow.tflow(
            req=tutils.treq(method=b"POST", headers=(), content=b"'&#")
        )
        command = export.httpie_command(request)
        assert shlex.split(command)[-2] == "<<<"
        assert shlex.split(command)[-1] == "'&#"

    # See comment in `TestExportCurlCommand.test_correct_host_used`. httpie
    # currently doesn't have a way of forcing connection to a particular IP, so
    # the command-line may not always reproduce the original request, in case
    # the host is resolved to a different IP address.
    #
    # httpie tracking issue: https://github.com/httpie/httpie/issues/414
    def test_correct_host_used(self, get_request):
        get_request.request.headers["host"] = "domain:22"

        result = (
            """http GET 'http://domain:22/path?a=foo&a=bar&b=baz' """
            """'header: qvalue' 'host: domain:22'"""
        )
        assert export.httpie_command(get_request) == result


class TestRaw:
    def test_req_and_resp_present(self, get_flow):
        assert b"header: qvalue" in export.raw(get_flow)
        assert b"header-response: svalue" in export.raw(get_flow)

    def test_get_request_present(self, get_request):
        assert b"header: qvalue" in export.raw(get_request)
        assert b"content-length: 0" in export.raw_request(get_request)

    def test_get_response_present(self, get_response):
        get_response.request.content = None
        assert b"header-response: svalue" in export.raw(get_response)

    def test_tcp(self, tcp_flow):
        with pytest.raises(
            exceptions.CommandError,
            match="Can't export flow with no request or response",
        ):
            export.raw(tcp_flow)

    def test_udp(self, udp_flow):
        with pytest.raises(
            exceptions.CommandError,
            match="Can't export flow with no request or response",
        ):
            export.raw(udp_flow)

    def test_websocket(self, websocket_flow):
        assert b"hello binary" in export.raw(websocket_flow)
        assert b"hello text" in export.raw(websocket_flow)
        assert b"it's me" in export.raw(websocket_flow)


class TestRawRequest:
    def test_get(self, get_request):
        assert b"header: qvalue" in export.raw_request(get_request)
        assert b"content-length: 0" in export.raw_request(get_request)

    def test_no_content(self, get_request):
        get_request.request.content = None
        with pytest.raises(exceptions.CommandError):
            export.raw_request(get_request)

    def test_tcp(self, tcp_flow):
        with pytest.raises(exceptions.CommandError):
            export.raw_request(tcp_flow)

    def test_udp(self, udp_flow):
        with pytest.raises(exceptions.CommandError):
            export.raw_request(udp_flow)


class TestRawResponse:
    def test_get(self, get_response):
        assert b"header-response: svalue" in export.raw_response(get_response)

    def test_no_content(self, get_response):
        get_response.response.content = None
        with pytest.raises(exceptions.CommandError):
            export.raw_response(get_response)

    def test_tcp(self, tcp_flow):
        with pytest.raises(exceptions.CommandError):
            export.raw_response(tcp_flow)

    def test_udp(self, udp_flow):
        with pytest.raises(exceptions.CommandError):
            export.raw_response(udp_flow)

    def test_head_non_zero_content_length(self):
        request = tflow.tflow(
            req=tutils.treq(method=b"HEAD"),
            resp=tutils.tresp(headers=((b"content-length", b"7"),), content=b""),
        )
        assert b"content-length: 7" in export.raw_response(request)


class TestRawBodies:
    def test_request_and_response(self, get_flow):
        get_flow.request.content = b"request body"
        get_flow.response.content = b"response body"
        assert export.raw_request_body(get_flow) == b"request body"
        assert export.raw_response_body(get_flow) == b"response body"
        assert export.raw_bodies(get_flow) == b"request bodyresponse body"

    def test_one_side_only(self, get_request, get_response):
        get_request.request.content = b"request body"
        get_response.request.content = None
        get_response.response.content = b"response body"
        assert export.raw_bodies(get_request) == b"request body"
        assert export.raw_bodies(get_response) == b"response body"

    def test_missing_body(self, get_request, get_response):
        get_request.request.content = None
        get_response.response.content = None
        with pytest.raises(exceptions.CommandError):
            export.raw_request_body(get_request)
        with pytest.raises(exceptions.CommandError):
            export.raw_response_body(get_response)
        with pytest.raises(exceptions.CommandError):
            export.raw_bodies(get_request)

    def test_empty_body(self, get_request, get_response):
        get_request.request.content = b""
        get_response.response.content = b""
        assert export.raw_request_body(get_request) == b""
        assert export.raw_response_body(get_response) == b""
        assert export.raw_bodies(get_request) == b""

    def test_decodes_content_encoding(self):
        body = b"decoded body"
        request = tflow.tflow(req=tutils.treq(content=gzip.compress(body)))
        request.request.headers["content-encoding"] = "gzip"
        response = tflow.tflow(resp=tutils.tresp(content=gzip.compress(body)))
        response.response.headers["content-encoding"] = "gzip"

        assert export.raw_request_body(request) == body
        assert body in export.raw_request(request)
        assert export.raw_response_body(response) == body
        assert body in export.raw_response(response)

    def test_tcp(self, tcp_flow):
        with pytest.raises(exceptions.CommandError):
            export.raw_request_body(tcp_flow)


class TestRedacted:
    @pytest.fixture
    def configured_export(self):
        e = export.Export()
        with taddons.context() as tctx:
            tctx.configure(e)
            yield e, tctx

    @staticmethod
    def flow():
        return tflow.tflow(
            req=tutils.treq(
                method=b"POST",
                content=b"request body",
                headers=(
                    (b"Authorization", b"Bearer request"),
                    (b"X-Test", b"request value"),
                    (b"authorization", b"another request"),
                ),
            ),
            resp=tutils.tresp(
                content=b"response body",
                headers=(
                    (b"X-Api-Key", b"response key"),
                    (b"X-Other", b"response value"),
                ),
            ),
        )

    def test_default_redaction(self, configured_export):
        _, tctx = configured_export
        f = self.flow()
        request_fields = f.request.headers.fields
        response_fields = f.response.headers.fields

        assert b"Authorization: [redacted]" in export.redacted_request(f)
        assert b"authorization: [redacted]" in export.redacted_request(f)
        assert b"X-Api-Key: [redacted]" in export.redacted_response(f)
        assert b"X-Test: request value" in export.redacted_request(f)
        assert b"request body" in export.redacted_request(f)
        assert b"response body" in export.redacted_response(f)
        assert request_fields == f.request.headers.fields
        assert response_fields == f.response.headers.fields
        combined = export.redacted(f)
        assert b"\r\n\r\n" in combined
        assert b"[redacted]" in combined
        assert b"Bearer request" not in combined
        assert tctx.options.redacted_headers

    def test_default_header_patterns(self, configured_export):
        headers = [
            "api-key",
            "api_key",
            "apikey",
            "API-Key",
            "X-Auth-Token",
            "RefreshToken",
        ]
        f = tflow.tflow(
            req=tutils.treq(
                headers=tuple((name.encode(), b"secret") for name in headers),
                content=b"body",
            )
        )
        output = export.redacted_request(f)
        for name in headers:
            assert f"{name}: secret".encode() not in output
            assert f"{name}: [redacted]".encode() in output

    @pytest.mark.parametrize(
        "header_name, pattern, should_match",
        [
            ("X-Foo[0]", "X-Foo[0]", True),
            ("X-Foo0", "X-Foo[0]", False),
            ("authorization", "Authorization", False),
            ("X-Auth-Token-Extra", "/token/", False),
            ("X-Auth-token-Extra", "/.*token.*/", True),
            ("Authorization", "/authorization/", False),
            ("Authorization", "/authorization/i", True),
            ("/", r"/\//", True),
        ],
    )
    def test_pattern_matching(
        self, configured_export, header_name, pattern, should_match
    ):
        e, tctx = configured_export
        tctx.configure(e, redacted_headers=[pattern])
        f = tflow.tflow(
            req=tutils.treq(
                headers=((header_name.encode(), b"secret"),), content=b"body"
            )
        )
        assert (b"secret" not in export.redacted_request(f)) is should_match

    def test_replacement_and_whitespace(self, configured_export):
        _, tctx = configured_export
        tctx.options.update(
            redacted_headers=["  Authorization  ", "", "   "],
            redacted_headers_replacement="",
        )
        f = self.flow()
        output = export.redacted_request(f)
        assert b"Authorization: \r\n" in output
        assert b"authorization: another request" in output

    @pytest.mark.parametrize(
        "pattern",
        ["/foo", "/foo/z", "/foo/bar/i", "/[/"],
    )
    def test_invalid_pattern_is_rejected_during_configure(
        self, configured_export, pattern
    ):
        e, tctx = configured_export
        tctx.options._options["redacted_headers"].value = [pattern]
        with pytest.raises(exceptions.OptionsError):
            e.configure({"redacted_headers"})

    def test_supported_flags(self, configured_export):
        e, tctx = configured_export
        tctx.configure(e, redacted_headers=["/foo/ims"])

    def test_binary_export_str(self, configured_export):
        e, _ = configured_export
        f = tflow.tflow(req=tutils.treq(content=b"\xff\xfe"))
        assert e.export_str("raw_request_body", f) == r"\xff\xfe"


def qr(f):
    with open(f, "rb") as fp:
        return fp.read()


def test_export(tmp_path) -> None:
    f = tmp_path / "outfile"
    e = export.Export()
    with taddons.context() as tctx:
        tctx.configure(e)

        assert e.formats() == [
            "curl",
            "httpie",
            "raw",
            "raw_bodies",
            "raw_request",
            "raw_request_body",
            "raw_response",
            "raw_response_body",
            "redacted",
            "redacted_request",
            "redacted_response",
        ]
        with pytest.raises(exceptions.CommandError):
            e.file("nonexistent", tflow.tflow(resp=True), f)

        e.file("raw_request", tflow.tflow(resp=True), f)
        assert qr(f)
        os.unlink(f)

        e.file("raw_response", tflow.tflow(resp=True), f)
        assert qr(f)
        os.unlink(f)

        e.file("curl", tflow.tflow(resp=True), f)
        assert qr(f)
        os.unlink(f)

        e.file("httpie", tflow.tflow(resp=True), f)
        assert qr(f)
        os.unlink(f)

        e.file("raw", tflow.twebsocketflow(), f)
        assert qr(f)
        os.unlink(f)


@pytest.mark.parametrize(
    "exception, log_message",
    [
        (PermissionError, "Permission denied"),
        (IsADirectoryError, "Is a directory"),
        (FileNotFoundError, "No such file or directory"),
    ],
)
def test_export_open(exception, log_message, tmpdir, caplog):
    f = str(tmpdir.join("path"))
    e = export.Export()
    with mock.patch("mitmproxy.addons.export.open") as m:
        m.side_effect = exception(log_message)
        e.file("raw_request", tflow.tflow(resp=True), f)
        assert log_message in caplog.text


def test_export_str(tmpdir, caplog):
    """Test that string export return a str without any UTF-8 surrogates"""
    e = export.Export()
    with taddons.context(e):
        f = tflow.tflow()
        f.request.headers.fields = (
            (b"utf8-header", "é".encode("utf-8")),
            (b"latin1-header", "é".encode("latin1")),
        )
        # ensure that we have no surrogates in the return value
        assert e.export_str("curl", f).encode("utf8", errors="strict")
        assert e.export_str("raw", f).encode("utf8", errors="strict")


def test_clip(tmpdir, caplog):
    e = export.Export()
    with taddons.context() as tctx:
        tctx.configure(e)

        with pytest.raises(exceptions.CommandError):
            e.clip("nonexistent", tflow.tflow(resp=True))

        with mock.patch("pyperclip.copy") as pc:
            e.clip("raw_request", tflow.tflow(resp=True))
            assert pc.called

        with mock.patch("pyperclip.copy") as pc:
            e.clip("raw_response", tflow.tflow(resp=True))
            assert pc.called

        with mock.patch("pyperclip.copy") as pc:
            e.clip("curl", tflow.tflow(resp=True))
            assert pc.called

        with mock.patch("pyperclip.copy") as pc:
            e.clip("httpie", tflow.tflow(resp=True))
            assert pc.called

        with mock.patch("pyperclip.copy") as pc:
            log_message = (
                "Pyperclip could not find a copy/paste mechanism for your system."
            )
            pc.side_effect = pyperclip.PyperclipException(log_message)
            e.clip("raw_request", tflow.tflow(resp=True))
            assert log_message in caplog.text
