import io
import json
import os
import re
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime
from datetime import timezone as dt_timezone
from unittest.mock import patch

import pytest

from mitmproxy import flowfilter
from mitmproxy import http
from mitmproxy.flowfilter import FAnd
from mitmproxy.flowfilter import TFilter
from mitmproxy.test import tflow


@contextmanager
def _ignore_case(flag: bool):
    """Temporarily control the global case-insensitivity flag."""
    old = flowfilter.maybe_ignore_case
    flowfilter.maybe_ignore_case = re.IGNORECASE if flag else re.NOFLAG
    try:
        yield
    finally:
        flowfilter.maybe_ignore_case = old


class TestParsing:
    def _dump(self, x: TFilter):
        c = io.StringIO()
        x.dump(fp=c)
        assert c.getvalue()

    def test_parse_err(self):
        with pytest.raises(ValueError, match="Empty filter"):
            flowfilter.parse("")
        with pytest.raises(ValueError, match="Invalid filter"):
            flowfilter.parse("~b")
        with pytest.raises(ValueError, match="Invalid filter"):
            flowfilter.parse("~h [")

    def test_simple(self):
        assert flowfilter.parse("~q")
        assert flowfilter.parse("~c 10")
        assert flowfilter.parse("~m foobar")
        assert flowfilter.parse("~u foobar")
        assert flowfilter.parse("~q ~c 10")
        assert flowfilter.parse("~replay")
        assert flowfilter.parse("~replayq")
        assert flowfilter.parse("~replays")
        assert flowfilter.parse("~comment .")
        p = flowfilter.parse("~q ~c 10")
        self._dump(p)
        assert isinstance(p, FAnd)
        assert len(p.lst) == 2

    def test_non_ascii(self):
        assert flowfilter.parse("~s шгн")

    def test_naked_url(self):
        a = flowfilter.parse("foobar ~h rex")
        assert a.lst[0].expr == "foobar"
        assert a.lst[1].expr == "rex"
        self._dump(a)

    def test_quoting(self):
        a = flowfilter.parse("~u 'foo ~u bar' ~u voing")
        assert a.lst[0].expr == "foo ~u bar"
        assert a.lst[1].expr == "voing"
        self._dump(a)

        a = flowfilter.parse("~u foobar")
        assert a.expr == "foobar"

        a = flowfilter.parse(r"~u 'foobar\"\''")
        assert a.expr == "foobar\"'"

        a = flowfilter.parse(r'~u "foo \'bar"')
        assert a.expr == "foo 'bar"

    def test_nesting(self):
        a = flowfilter.parse("(~u foobar & ~h voing)")
        assert a.lst[0].expr == "foobar"
        self._dump(a)

    def test_not(self):
        a = flowfilter.parse("!~h test")
        assert a.itm.expr == "test"
        a = flowfilter.parse("!(~u test & ~h bar)")
        assert a.itm.lst[0].expr == "test"
        self._dump(a)

    def test_binaryops(self):
        a = flowfilter.parse("~u foobar | ~h voing")
        isinstance(a, flowfilter.FOr)
        self._dump(a)

        a = flowfilter.parse("~u foobar & ~h voing")
        isinstance(a, flowfilter.FAnd)
        self._dump(a)

    def test_wideops(self):
        a = flowfilter.parse("~hq 'header: qvalue'")
        assert isinstance(a, flowfilter.FHeadRequest)
        self._dump(a)

    @pytest.mark.parametrize(
        ("expr", "expected"),
        [
            ("~a", "is asset"),
            ("~marked", "is marked"),
            ("~http", "is an HTTP Flow"),
            ("~websocket", "is a Websocket Flow"),
            ("~tcp", "is a TCP Flow"),
            ("~udp", "is a UDP Flow"),
            ("~dns", "is a DNS Flow"),
            ("~all", "all flows"),
            ("~q", "has no response"),
            ("~s", "has response"),
            ("~e", "has error"),
            ("~t content", "content type matches /content/i"),
            ("~tq content", "req. content type matches /content/i"),
            ("~ts content", "resp. content type matches /content/i"),
            ("~h rex", "header matches /rex/im"),
            ("~hq rex", "req. header matches /rex/im"),
            ("~hs rex", "resp. header matches /rex/im"),
            ("~h header", "header matches /header/im"),
            ("~hq header", "req. header matches /header/im"),
            ("~hs header", "resp. header matches /header/im"),
            ("~b rex", "body matches /rex/is"),
            ("~bq rex", "body request matches /rex/is"),
            ("~bs rex", "body response matches /rex/is"),
            ("~b content", "body matches /content/is"),
            ("~bq content", "body request matches /content/is"),
            ("~bs content", "body response matches /content/is"),
            ("~m get", "method matches /get/i"),
            ("~d example.com", "domain matches /example.com/i"),
            ("~u foo", "url matches /foo/i"),
            ("~src 127.0.0.1", "source address matches /127.0.0.1/i"),
            ("~dst example.com:443", "destination address matches /example.com:443/i"),
            ("~replay", "flow has been replayed"),
            ("~replayq", "request has been replayed"),
            ("~replays", "response has been replayed"),
            ("~meta foo", "flow metadata matches /foo/im"),
            ("~marker red", "marker matches /red/i"),
            ("~comment note", "comment matches /note/im"),
            # case-sensitive variants keep their other flags and lose "i".
            ("~bc rex", "body matches /rex/s"),
            ("~bqc rex", "body request matches /rex/s"),
            ("~bsc rex", "body response matches /rex/s"),
            ("~tc content", "content type matches /content/"),
            ("~tqc content", "req. content type matches /content/"),
            ("~tsc content", "resp. content type matches /content/"),
            ("~hc rex", "header matches /rex/m"),
            ("~hqc rex", "req. header matches /rex/m"),
            ("~hsc rex", "resp. header matches /rex/m"),
            ("~mc get", "method matches /get/"),
            ("~dc example.com", "domain matches /example.com/"),
            ("~uc foo", "url matches /foo/"),
            ("~srcc 127.0.0.1", "source address matches /127.0.0.1/"),
            ("~dstc example.com:443", "destination address matches /example.com:443/"),
            ("~metac foo", "flow metadata matches /foo/m"),
            ("~markerc red", "marker matches /red/"),
            ("~commentc note", "comment matches /note/m"),
            # HTTP timestamp comparisons
            ("~dt >= 2026-05-20", "http timestamp >= 2026-05-20"),
            ("~dtq < 2026-05-20 12:00:00", "request timestamp < 2026-05-20 12:00:00"),
            ("~dts != 2026-05-20", "response timestamp != 2026-05-20"),
            (
                "~dtqs <= 2026-05-20T12:30:45Z",
                "request start time <= 2026-05-20T12:30:45Z",
            ),
            ("~dtqe = 2026-05-20", "request end time = 2026-05-20"),
            ("~dtss > 2026-05-20", "response start time > 2026-05-20"),
            (
                '~dtse <= "2026-05-20 12:30:45.5+02:00"',
                "response end time <= 2026-05-20 12:30:45.5+02:00",
            ),
            ("~c 404", "response code is 404"),
            (
                "(~u foobar & ~h voing)",
                "url matches /foobar/i and header matches /voing/im",
            ),
            ("!~h test", "not header matches /test/im"),
            ("~u foo & ~c 200", "url matches /foo/i and response code is 200"),
            ("~u foo | ~c 200", "url matches /foo/i or response code is 200"),
            ("!(~u foo | ~c 200)", "not (url matches /foo/i or response code is 200)"),
        ],
    )
    def test_str_implementations(self, expr: str, expected: str):
        assert str(flowfilter.parse(expr)) == expected


class TestCaseSensitiveRegexVariants:
    def req(self):
        return tflow.tflow()

    def q(self, q, o):
        return flowfilter.parse(q)(o)

    @pytest.mark.parametrize(
        "code",
        [
            "b",
            "bq",
            "bs",
            "t",
            "tq",
            "ts",
            "h",
            "hq",
            "hs",
            "m",
            "d",
            "u",
            "src",
            "dst",
            "meta",
            "marker",
            "comment",
        ],
    )
    def test_variant_exists_and_parses(self, code):
        variants = {cls.code: cls for cls in flowfilter.filter_rex}
        assert f"{code}c" in variants
        assert code in variants
        base = flowfilter.parse(f"~{code} foo")
        variant = flowfilter.parse(f"~{code}c foo")
        assert isinstance(variant, type(base))

    def test_no_variants_for_non_regex_filters(self):
        codes = {cls.code for cls in flowfilter.filter_rex}
        assert "cc" not in codes  # ~c is a response code filter
        assert not any(
            code.endswith("qc") and len(code) > 3 and code[:-1] not in codes
            for code in codes
        )

    def test_variant_flags(self):
        """The c variant only differs by the absence of re.IGNORECASE."""
        variants = {cls.code: cls for cls in flowfilter.filter_rex}
        for code in ["b", "bq", "bs", "t", "tq", "ts", "h", "hq", "hs", "m", "d", "u", "src", "dst", "meta", "marker", "comment"]:
            base = flowfilter.parse(f"~{code} foo")
            variant = flowfilter.parse(f"~{code}c foo")
            assert not variant.re.flags & re.IGNORECASE
            assert base.re.flags & ~re.IGNORECASE == variant.re.flags & ~re.IGNORECASE
            assert variants[code].flags == variants[f"{code}c"].flags

    def test_default_is_case_insensitive(self):
        with _ignore_case(True):
            q = self.req()
            q.request.path = "/FooBar"
            assert self.q("~u foobar", q)
            assert self.q("~u FooBar", q)
            # The c variant only matches the exact case.
            assert self.q("~uc FooBar", q)
            assert not self.q("~uc FOOBAR", q)

    def test_variant_forces_case_sensitivity(self):
        """~uc stays case-sensitive even if global insensitive matching is on."""
        with _ignore_case(True):
            q = self.req()
            q.request.path = "/FooBar"
            assert self.q("~u foobar", q)
            assert not self.q("~uc foobar", q)
            assert self.q("~uc FooBar", q)

    def test_legacy_global_flag_still_applies_to_plain_operators(self):
        with _ignore_case(False):
            q = self.req()
            q.request.path = "/FooBar"
            # Legacy MITMPROXY_CASE_SENSITIVE_FILTERS=1 behaviour: plain
            # operators become case-sensitive.
            assert self.q("~u FooBar", q)
            assert not self.q("~u foobar", q)
            # The c variants are unaffected.
            assert self.q("~uc FooBar", q)
            assert not self.q("~uc foobar", q)

    def test_inherited_flags_are_kept(self, monkeypatch):
        monkeypatch.setenv("MITMPROXY_CASE_SENSITIVE_FILTERS", "0")
        s = tflow.tflow(resp=True)
        s.request.headers["X-Sent"] = "Yes"
        s.response.headers["X-Header"] = "QValue"
        # Headers remain multiline ...
        assert self.q("~hc 'X-Sent'", s)
        assert self.q(r"~hqc '^X-Sent: Yes\r?$'", s)
        assert not self.q("~hqc 'x-sent'", s)
        assert self.q(r"~hsc '^X-Header: QValue\r?$'", s)
        # ... bodies keep DOTALL.
        s.request.content = b"a\nb"
        assert self.q("~bq 'a.b'", s)
        assert self.q("~bqc 'a.b'", s)
        assert not self.q("~bqc 'A.B'", s)

    @pytest.mark.parametrize(
        ("env_value", "expected"),
        [
            (
                None,
                {"plain_lower": True, "cs_lower": False, "cs_upper": True},
            ),
            (
                "1",
                {"plain_lower": False, "cs_lower": False, "cs_upper": True},
            ),
            (
                "0",
                {"plain_lower": True, "cs_lower": False, "cs_upper": True},
            ),
        ],
    )
    def test_env_var_end_to_end(self, env_value, expected):
        """MITMPROXY_CASE_SENSITIVE_FILTERS keeps steering plain operators.

        The flag is read at import time, so this runs a fresh interpreter.
        """
        code = f"""
import json
from mitmproxy import flowfilter
from mitmproxy.test import tflow

f = tflow.tflow()
f.request.path = "/FooBar"
print(json.dumps({{
    "plain_lower": bool(flowfilter.parse("~u foobar")(f)),
    "cs_lower": bool(flowfilter.parse("~uc foobar")(f)),
    "cs_upper": bool(flowfilter.parse("~uc FooBar")(f)),
}}))
"""
        env = dict(os.environ)
        if env_value is None:
            env.pop("MITMPROXY_CASE_SENSITIVE_FILTERS", None)
        else:
            env["MITMPROXY_CASE_SENSITIVE_FILTERS"] = env_value
        proc = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            env=env,
            check=True,
        )
        assert json.loads(proc.stdout.strip().splitlines()[-1]) == expected


class TestHTTPTimestampFilters:
    ts_req_start = datetime(2026, 5, 20, 12, 30, tzinfo=dt_timezone.utc).timestamp()

    @staticmethod
    def iso(ts: float) -> str:
        return (
            datetime.fromtimestamp(ts, dt_timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )

    def resp_flow(self) -> http.HTTPFlow:
        f = tflow.tflow(resp=True)
        f.request.timestamp_start = self.ts_req_start
        f.request.timestamp_end = None  # incomplete request
        f.response.timestamp_start = datetime(2026, 5, 21, tzinfo=dt_timezone.utc).timestamp()
        f.response.timestamp_end = datetime(2026, 6, 11, tzinfo=dt_timezone.utc).timestamp()
        return f

    def q(self, expr, f):
        return bool(flowfilter.parse(expr)(f))

    @pytest.mark.parametrize(
        "expr",
        [
            "~dt >= 2026-05-20",
            "~dtq >= 2026-05-20",
            "~dts >= 2026-05-20",
            "~dtqs >= 2026-05-20",
            "~dtqe >= 2026-05-20",
            "~dtss >= 2026-05-20",
            "~dtse >= 2026-05-20",
            '~dtqs >= "2026-05-20"',
        ],
    )
    def test_parse_all_operators(self, expr):
        assert flowfilter.parse(expr)

    @pytest.mark.parametrize("comparator", ["=", "!=", ">", ">=", "<", "<="])
    def test_parse_all_comparators(self, comparator):
        assert flowfilter.parse(f"~dtqs {comparator} 2026-05-20")

    def test_request_vs_response_fields(self):
        f = self.resp_flow()
        assert self.q(f"~dtqs = {self.iso(self.ts_req_start)}", f)
        assert not self.q(f"~dtss = {self.iso(self.ts_req_start)}", f)
        assert not self.q("~dtqe != 1970-01-01", f)  # request end missing
        assert self.q("~dtse > 2026-06-10", f)
        assert not self.q("~dtqs > 2026-06-10", f)

    def test_aggregates_match_any_timestamp(self):
        f = self.resp_flow()
        # Only the response timestamps satisfy these.
        assert self.q("~dt >= 2026-05-21", f)
        assert self.q("~dts >= 2026-05-21", f)
        assert not self.q("~dtqs >= 2026-05-21", f)
        assert self.q("~dtq < 2026-05-21", f)
        # Aggregates match if ANY of their timestamps compares successfully.
        assert self.q("~dtq != 1970-01-01", f)
        assert not self.q("~dtqe != 1970-01-01", f)

    def test_comparator_boundaries(self):
        f = self.resp_flow()
        start = f.response.timestamp_start
        exact = f"~dtss = {self.iso(start)}"
        assert self.q(exact, f)
        assert self.q(f"~dtss <= {self.iso(start)}", f)
        assert self.q(f"~dtss >= {self.iso(start)}", f)
        assert not self.q(f"~dtss < {self.iso(start)}", f)
        assert not self.q(f"~dtss > {self.iso(start)}", f)
        assert self.q(f"~dtss <= {self.iso(start + 1)}", f)
        assert not self.q(f"~dtss < {self.iso(start - 1)}", f)
        assert self.q(f"~dtss > {self.iso(start - 1)}", f)

    def test_date_only_is_local_midnight(self):
        midnight_local = datetime(2026, 5, 21).astimezone().timestamp()
        f = tflow.tflow()
        f.request.timestamp_start = midnight_local
        assert self.q("~dtqs = 2026-05-21", f)
        assert self.q("~dtqs >= 2026-05-21 00:00:00", f)
        assert not self.q("~dtqs > 2026-05-21 00:00:00", f)
        assert not self.q("~dtqs < 2026-05-20T23:59:59Z", f) or (
            datetime.fromtimestamp(midnight_local, dt_timezone.utc).date().isoformat()
            == "2026-05-20"
        )

    def test_separator_forms_and_fractional_seconds(self):
        f = tflow.tflow()
        f.request.timestamp_start = datetime(
            2026, 5, 20, 12, 30, 45, 123456, dt_timezone.utc
        ).timestamp()
        assert self.q("~dtqs = 2026-05-20T12:30:45.123456Z", f)
        assert self.q('~dtqs = "2026-05-20 12:30:45.123456+00:00"', f)
        assert self.q("~dtqs = 2026-05-20 14:30:45.123456+02:00", f)
        assert not self.q("~dtqs = 2026-05-20T12:30:45.000001Z", f)

    def test_timezones_are_normalized(self):
        f = tflow.tflow()
        f.request.timestamp_start = self.ts_req_start
        assert self.q("~dtqs = 2026-05-20T12:30:00Z", f)
        assert self.q("~dtqs = 2026-05-20T14:30:00+02:00", f)
        assert self.q("~dtqs = 2026-05-20T09:30:00-03:00", f)
        # Timezone-less input is interpreted as local time.
        local_equiv = datetime.fromtimestamp(self.ts_req_start).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        assert self.q(f"~dtqs = {local_equiv}", f)

    def test_missing_timestamps_never_match(self):
        f = tflow.tflow()  # no response at all
        for op in ["=", "!=", ">", ">=", "<", "<="]:
            assert not self.q(f"~dts {op} 1970-01-01", f)
            assert not self.q(f"~dtss {op} 1970-01-01", f)
            assert not self.q(f"~dtse {op} 1970-01-01", f)

    def test_missing_end_skipped_by_aggregates(self):
        f = self.resp_flow()
        f.request.timestamp_end = None
        assert not self.q("~dtqe != 1970-01-01", f)
        # ... but other request timestamps still count.
        assert self.q("~dtq != 1970-01-01", f)

    def test_non_http_flows_do_not_match(self):
        flows = [
            tflow.ttcpflow(),
            tflow.tudpflow(),
            tflow.tdnsflow(),
            tflow.tdummyflow(),
        ]
        for f in flows:
            for op in ["~dt", "~dtq", "~dts", "~dtqs", "~dtqe", "~dtss", "~dtse"]:
                assert not self.q(f"{op} >= 1970-01-01", f), op

    @pytest.mark.parametrize(
        "expr",
        [
            "~dtqs",
            "~dtqs 2026-05-20",
            "~dtqs == 2026-05-20",
            "~dtqs => 2026-05-20",
            "~dtqs > yesterday",
            "~dtqs > 2026-13-40",
            "~dtqs > 2026-5-20",
            "~dtqs > 2026-05-20T99:00:00",
        ],
    )
    def test_malformed_expressions_fail_cleanly(self, expr):
        with pytest.raises(ValueError, match="Invalid filter expression"):
            flowfilter.parse(expr)

    def test_help_contains_entries(self):
        commands = [cmd for cmd, _ in flowfilter.help]
        for code in ["~dt", "~dtq", "~dts", "~dtqs", "~dtqe", "~dtss", "~dtse"]:
            assert any(cmd.split(" ")[0] == code for cmd in commands), code


class TestMatchingHTTPFlow:
    def req(self):
        return tflow.tflow()

    def resp(self):
        return tflow.tflow(resp=True)

    def err(self):
        return tflow.tflow(err=True)

    def q(self, q, o):
        return flowfilter.parse(q)(o)

    def test_http(self):
        s = self.req()
        assert self.q("~http", s)
        assert not self.q("~tcp", s)

    def test_asset(self):
        s = self.resp()
        assert not self.q("~a", s)
        s.response.headers["content-type"] = "text/javascript"
        assert self.q("~a", s)

    def test_fcontenttype(self):
        q = self.req()
        s = self.resp()
        assert not self.q("~t content", q)
        assert not self.q("~t content", s)

        q.request.headers["content-type"] = "text/json"
        assert self.q("~t json", q)
        assert self.q("~tq json", q)
        assert not self.q("~ts json", q)

        s.response.headers["content-type"] = "text/json"
        assert self.q("~t json", s)

        del s.response.headers["content-type"]
        s.request.headers["content-type"] = "text/json"
        assert self.q("~t json", s)
        assert self.q("~tq json", s)
        assert not self.q("~ts json", s)

    def test_freq_fresp(self):
        q = self.req()
        s = self.resp()

        assert self.q("~q", q)
        assert not self.q("~q", s)

        assert not self.q("~s", q)
        assert self.q("~s", s)

    def test_ferr(self):
        e = self.err()
        assert self.q("~e", e)

    def test_fmarked(self):
        q = self.req()
        assert not self.q("~marked", q)
        q.marked = ":default:"
        assert self.q("~marked", q)

    def test_fmarker_char(self):
        t = tflow.tflow()
        t.marked = ":default:"
        assert not self.q("~marker X", t)
        t.marked = "X"
        assert self.q("~marker X", t)

    def test_head(self):
        q = self.req()
        s = self.resp()
        assert not self.q("~h nonexistent", q)
        assert self.q("~h qvalue", q)
        assert self.q("~h header", q)
        assert self.q("~h 'header: qvalue'", q)

        assert self.q("~h 'header: qvalue'", s)
        assert self.q("~h 'header-response: svalue'", s)

        assert self.q("~hq 'header: qvalue'", s)
        assert not self.q("~hq 'header-response: svalue'", s)

        assert self.q("~hq 'header: qvalue'", q)
        assert not self.q("~hq 'header-request: svalue'", q)

        assert not self.q("~hs 'header: qvalue'", s)
        assert self.q("~hs 'header-response: svalue'", s)
        assert not self.q("~hs 'header: qvalue'", q)

    def match_body(self, q, s):
        assert not self.q("~b nonexistent", q)
        assert self.q("~b content", q)
        assert self.q("~b message", s)

        assert not self.q("~bq nomatch", s)
        assert self.q("~bq content", q)
        assert self.q("~bq content", s)
        assert not self.q("~bq message", q)
        assert not self.q("~bq message", s)

        s.response.text = "яч"  # Cyrillic
        assert self.q("~bs яч", s)
        s.response.text = "测试"  # Chinese
        assert self.q("~bs 测试", s)
        s.response.text = "ॐ"  # Hindi
        assert self.q("~bs ॐ", s)
        s.response.text = "لله"  # Arabic
        assert self.q("~bs لله", s)
        s.response.text = "θεός"  # Greek
        assert self.q("~bs θεός", s)
        s.response.text = "לוהים"  # Hebrew
        assert self.q("~bs לוהים", s)
        s.response.text = "神"  # Japanese
        assert self.q("~bs 神", s)
        s.response.text = "하나님"  # Korean
        assert self.q("~bs 하나님", s)
        s.response.text = "Äÿ"  # Latin
        assert self.q("~bs Äÿ", s)

        assert not self.q("~bs nomatch", s)
        assert not self.q("~bs content", q)
        assert not self.q("~bs content", s)
        assert not self.q("~bs message", q)
        s.response.text = "message"
        assert self.q("~bs message", s)

    def test_body(self):
        q = self.req()
        s = self.resp()
        self.match_body(q, s)

        q.request.encode("gzip")
        s.request.encode("gzip")
        s.response.encode("gzip")
        self.match_body(q, s)

    def test_case_sensitive(self, monkeypatch):
        q = self.req()

        monkeypatch.setenv("MITMPROXY_CASE_SENSITIVE_FILTERS", "0")
        assert self.q("~m get", q)
        assert self.q("~m GET", q)
        assert not self.q("~m post", q)

        q.request.method = "oink"
        assert not self.q("~m get", q)

        monkeypatch.setenv("MITMPROXY_CASE_SENSITIVE_FILTERS", "1")
        assert not self.q("~m get", q)
        assert not self.q("~m GET", q)
        assert not self.q("~m post", q)

        q.request.method = "oink"
        assert not self.q("~m get", q)

    def test_method(self):
        q = self.req()
        assert self.q("~m get", q)
        assert not self.q("~m post", q)

        q.request.method = "oink"
        assert not self.q("~m get", q)

    def test_domain(self):
        q = self.req()
        assert self.q("~d address", q)
        assert not self.q("~d none", q)

    def test_url(self):
        q = self.req()
        s = self.resp()
        assert self.q("~u address", q)
        assert self.q("~u address:22/path", q)
        assert not self.q("~u moo/path", q)

        q.request = None
        assert not self.q("~u address", q)

        assert self.q("~u address", s)
        assert self.q("~u address:22/path", s)
        assert not self.q("~u moo/path", s)

    def test_code(self):
        q = self.req()
        s = self.resp()
        assert not self.q("~c 200", q)
        assert self.q("~c 200", s)
        assert not self.q("~c 201", s)

    def test_src(self):
        q = self.req()
        assert self.q("~src 127.0.0.1", q)
        assert not self.q("~src foobar", q)
        assert self.q("~src :22", q)
        assert not self.q("~src :99", q)
        assert self.q("~src 127.0.0.1:22", q)

        q.client_conn.peername = None
        assert not self.q("~src address:22", q)
        q.client_conn = None
        assert not self.q("~src address:22", q)

    def test_dst(self):
        q = self.req()
        q.server_conn = tflow.tserver_conn()
        assert self.q("~dst address", q)
        assert not self.q("~dst foobar", q)
        assert self.q("~dst :22", q)
        assert not self.q("~dst :99", q)
        assert self.q("~dst address:22", q)

        q.server_conn.address = None
        assert not self.q("~dst address:22", q)
        q.server_conn = None
        assert not self.q("~dst address:22", q)

    def test_exact_header_clause_adversarial(self):
        """The clause the web UI emits for a header row matches exactly.

        Mirrors web/src/js/filt/exactHeader.ts: regex-escape the serialized
        line, then filter-quote-escape the result. The value deliberately
        contains regex metacharacters, a quote and a backslash.
        """
        value = 'a"b\\c.d(e)[f]'
        # JS: escapeRegex(value) -> 'a"b\\\\c\\.d\\(e\\)\\[f\\]'
        # then quoteFilterArgument escapes every backslash and quote.
        escaped = r"a\"b\\\\c\\.d\\(e\\)\\[f\\]"
        clause = f'~hqc "^X-W: {escaped}\\r?$"'
        flt = flowfilter.parse(clause)
        assert "^X-W: " in str(flt)

        s = tflow.tflow()
        s.request.headers["X-W"] = value
        assert bool(flt(s))
        s.request.headers["X-W"] = value.replace("\\", "")
        assert not bool(flt(s))

    def test_and(self):
        s = self.resp()
        assert self.q("~c 200 & ~h head", s)
        assert self.q("~c 200 & ~h head", s)
        assert not self.q("~c 200 & ~h nohead", s)
        assert self.q("(~c 200 & ~h head) & ~b content", s)
        assert not self.q("(~c 200 & ~h head) & ~b nonexistent", s)
        assert not self.q("(~c 200 & ~h nohead) & ~b content", s)

    def test_or(self):
        s = self.resp()
        assert self.q("~c 200 | ~h nohead", s)
        assert self.q("~c 201 | ~h head", s)
        assert not self.q("~c 201 | ~h nohead", s)
        assert self.q("(~c 201 | ~h nohead) | ~s", s)

    def test_not(self):
        s = self.resp()
        assert not self.q("! ~c 200", s)
        assert self.q("! ~c 201", s)
        assert self.q("!~c 201 !~c 202", s)
        assert not self.q("!~c 201 !~c 200", s)

    def test_replay(self):
        f = tflow.tflow()
        assert not self.q("~replay", f)
        f.is_replay = "request"
        assert self.q("~replay", f)
        assert self.q("~replayq", f)
        assert not self.q("~replays", f)
        f.is_replay = "response"
        assert self.q("~replay", f)
        assert not self.q("~replayq", f)
        assert self.q("~replays", f)

    def test_metadata(self):
        f = tflow.tflow()
        f.metadata["a"] = 1
        f.metadata["b"] = "string"
        f.metadata["c"] = {"key": "value"}
        assert self.q("~meta a", f)
        assert not self.q("~meta no", f)
        assert self.q("~meta string", f)
        assert self.q("~meta key", f)
        assert self.q("~meta value", f)
        assert self.q('~meta "b: string"', f)
        assert self.q("~meta \"'key': 'value'\"", f)


class TestMatchingDNSFlow:
    def req(self):
        return tflow.tdnsflow()

    def resp(self):
        return tflow.tdnsflow(resp=True)

    def err(self):
        return tflow.tdnsflow(err=True)

    def q(self, q, o):
        return flowfilter.parse(q)(o)

    def test_dns(self):
        s = self.req()
        assert self.q("~dns", s)
        assert not self.q("~http", s)
        assert not self.q("~tcp", s)

    def test_freq_fresp(self):
        q = self.req()
        s = self.resp()

        assert self.q("~q", q)
        assert not self.q("~q", s)

        assert not self.q("~s", q)
        assert self.q("~s", s)

    def test_ferr(self):
        e = self.err()
        assert self.q("~e", e)

    def test_body(self):
        q = self.req()
        s = self.resp()
        assert not self.q("~b nonexistent", q)
        assert self.q("~b dns.google", q)
        assert self.q("~b 8.8.8.8", s)

        assert not self.q("~bq 8.8.8.8", s)
        assert self.q("~bq dns.google", q)
        assert self.q("~bq dns.google", s)

        assert not self.q("~bs dns.google", q)
        assert self.q("~bs dns.google", s)
        assert self.q("~bs 8.8.8.8", s)

    def test_url(self):
        f = self.req()
        assert not self.q("~u whatever", f)
        assert self.q("~u dns.google", f)


class TestMatchingTCPFlow:
    def flow(self):
        return tflow.ttcpflow()

    def err(self):
        return tflow.ttcpflow(err=True)

    def q(self, q, o):
        return flowfilter.parse(q)(o)

    def test_tcp(self):
        f = self.flow()
        assert self.q("~tcp", f)
        assert not self.q("~udp", f)
        assert not self.q("~http", f)
        assert not self.q("~websocket", f)

    def test_ferr(self):
        e = self.err()
        assert self.q("~e", e)

    def test_body(self):
        f = self.flow()

        # Messages sent by client or server
        assert self.q("~b hello", f)
        assert self.q("~b me", f)
        assert not self.q("~b nonexistent", f)

        # Messages sent by client
        assert self.q("~bq hello", f)
        assert not self.q("~bq me", f)
        assert not self.q("~bq nonexistent", f)

        # Messages sent by server
        assert self.q("~bs me", f)
        assert not self.q("~bs hello", f)
        assert not self.q("~bs nonexistent", f)

    def test_src(self):
        f = self.flow()
        assert self.q("~src 127.0.0.1", f)
        assert not self.q("~src foobar", f)
        assert self.q("~src :22", f)
        assert not self.q("~src :99", f)
        assert self.q("~src 127.0.0.1:22", f)

    def test_dst(self):
        f = self.flow()
        f.server_conn = tflow.tserver_conn()
        assert self.q("~dst address", f)
        assert not self.q("~dst foobar", f)
        assert self.q("~dst :22", f)
        assert not self.q("~dst :99", f)
        assert self.q("~dst address:22", f)

    def test_and(self):
        f = self.flow()
        f.server_conn = tflow.tserver_conn()
        assert self.q("~b hello & ~b me", f)
        assert not self.q("~src wrongaddress & ~b hello", f)
        assert self.q("(~src :22 & ~dst :22) & ~b hello", f)
        assert not self.q("(~src address:22 & ~dst :22) & ~b nonexistent", f)
        assert not self.q("(~src address:22 & ~dst :99) & ~b hello", f)

    def test_or(self):
        f = self.flow()
        f.server_conn = tflow.tserver_conn()
        assert self.q("~b hello | ~b me", f)
        assert self.q("~src :22 | ~b me", f)
        assert not self.q("~src :99 | ~dst :99", f)
        assert self.q("(~src :22 | ~dst :22) | ~b me", f)

    def test_not(self):
        f = self.flow()
        assert not self.q("! ~src :22", f)
        assert self.q("! ~src :99", f)
        assert self.q("!~src :99 !~src :99", f)
        assert not self.q("!~src :99 !~src :22", f)

    def test_request(self):
        f = self.flow()
        assert not self.q("~q", f)

    def test_response(self):
        f = self.flow()
        assert not self.q("~s", f)

    def test_headers(self):
        f = self.flow()
        assert not self.q("~h whatever", f)

        # Request headers
        assert not self.q("~hq whatever", f)

        # Response headers
        assert not self.q("~hs whatever", f)

    def test_content_type(self):
        f = self.flow()
        assert not self.q("~t whatever", f)

        # Request content-type
        assert not self.q("~tq whatever", f)

        # Response content-type
        assert not self.q("~ts whatever", f)

    def test_code(self):
        f = self.flow()
        assert not self.q("~c 200", f)

    def test_domain(self):
        f = self.flow()
        assert not self.q("~d whatever", f)

    def test_method(self):
        f = self.flow()
        assert not self.q("~m whatever", f)

    def test_url(self):
        f = self.flow()
        assert not self.q("~u whatever", f)


class TestMatchingUDPFlow:
    def flow(self):
        return tflow.tudpflow()

    def err(self):
        return tflow.tudpflow(err=True)

    def q(self, q, o):
        return flowfilter.parse(q)(o)

    def test_udp(self):
        f = self.flow()
        assert self.q("~udp", f)
        assert not self.q("~tcp", f)
        assert not self.q("~http", f)
        assert not self.q("~websocket", f)

    def test_ferr(self):
        e = self.err()
        assert self.q("~e", e)

    def test_body(self):
        f = self.flow()

        # Messages sent by client or server
        assert self.q("~b hello", f)
        assert self.q("~b me", f)
        assert not self.q("~b nonexistent", f)

        # Messages sent by client
        assert self.q("~bq hello", f)
        assert not self.q("~bq me", f)
        assert not self.q("~bq nonexistent", f)

        # Messages sent by server
        assert self.q("~bs me", f)
        assert not self.q("~bs hello", f)
        assert not self.q("~bs nonexistent", f)

    def test_src(self):
        f = self.flow()
        assert self.q("~src 127.0.0.1", f)
        assert not self.q("~src foobar", f)
        assert self.q("~src :22", f)
        assert not self.q("~src :99", f)
        assert self.q("~src 127.0.0.1:22", f)

    def test_dst(self):
        f = self.flow()
        f.server_conn = tflow.tserver_conn()
        assert self.q("~dst address", f)
        assert not self.q("~dst foobar", f)
        assert self.q("~dst :22", f)
        assert not self.q("~dst :99", f)
        assert self.q("~dst address:22", f)

    def test_and(self):
        f = self.flow()
        f.server_conn = tflow.tserver_conn()
        assert self.q("~b hello & ~b me", f)
        assert not self.q("~src wrongaddress & ~b hello", f)
        assert self.q("(~src :22 & ~dst :22) & ~b hello", f)
        assert not self.q("(~src address:22 & ~dst :22) & ~b nonexistent", f)
        assert not self.q("(~src address:22 & ~dst :99) & ~b hello", f)

    def test_or(self):
        f = self.flow()
        f.server_conn = tflow.tserver_conn()
        assert self.q("~b hello | ~b me", f)
        assert self.q("~src :22 | ~b me", f)
        assert not self.q("~src :99 | ~dst :99", f)
        assert self.q("(~src :22 | ~dst :22) | ~b me", f)

    def test_not(self):
        f = self.flow()
        assert not self.q("! ~src :22", f)
        assert self.q("! ~src :99", f)
        assert self.q("!~src :99 !~src :99", f)
        assert not self.q("!~src :99 !~src :22", f)

    def test_request(self):
        f = self.flow()
        assert not self.q("~q", f)

    def test_response(self):
        f = self.flow()
        assert not self.q("~s", f)

    def test_headers(self):
        f = self.flow()
        assert not self.q("~h whatever", f)

        # Request headers
        assert not self.q("~hq whatever", f)

        # Response headers
        assert not self.q("~hs whatever", f)

    def test_content_type(self):
        f = self.flow()
        assert not self.q("~t whatever", f)

        # Request content-type
        assert not self.q("~tq whatever", f)

        # Response content-type
        assert not self.q("~ts whatever", f)

    def test_code(self):
        f = self.flow()
        assert not self.q("~c 200", f)

    def test_domain(self):
        f = self.flow()
        assert not self.q("~d whatever", f)

    def test_method(self):
        f = self.flow()
        assert not self.q("~m whatever", f)

    def test_url(self):
        f = self.flow()
        assert not self.q("~u whatever", f)


class TestMatchingWebSocketFlow:
    def flow(self) -> http.HTTPFlow:
        return tflow.twebsocketflow()

    def q(self, q, o):
        return flowfilter.parse(q)(o)

    def test_websocket(self):
        f = self.flow()
        assert self.q("~websocket", f)
        assert not self.q("~tcp", f)
        assert self.q("~http", f)

    def test_handshake(self):
        f = self.flow()
        assert self.q("~websocket", f)
        assert not self.q("~tcp", f)
        assert self.q("~http", f)

        f = tflow.tflow()
        assert not self.q("~websocket", f)
        f = tflow.tflow(resp=True)
        assert not self.q("~websocket", f)

    def test_domain(self):
        q = self.flow()
        assert self.q("~d example.com", q)
        assert not self.q("~d none", q)

    def test_url(self):
        q = self.flow()
        assert self.q("~u example.com", q)
        assert self.q("~u example.com/ws", q)
        assert not self.q("~u moo/path", q)

    def test_body(self):
        f = self.flow()

        # Messages sent by client or server
        assert self.q("~b hello", f)
        assert self.q("~b me", f)
        assert not self.q("~b nonexistent", f)

        # Messages sent by client
        assert self.q("~bq hello", f)
        assert not self.q("~bq me", f)
        assert not self.q("~bq nonexistent", f)

        # Messages sent by server
        assert self.q("~bs me", f)
        assert not self.q("~bs hello", f)
        assert not self.q("~bs nonexistent", f)

    def test_src(self):
        f = self.flow()
        assert self.q("~src 127.0.0.1", f)
        assert not self.q("~src foobar", f)
        assert self.q("~src :22", f)
        assert not self.q("~src :99", f)
        assert self.q("~src 127.0.0.1:22", f)

    def test_dst(self):
        f = self.flow()
        f.server_conn = tflow.tserver_conn()
        assert self.q("~dst address", f)
        assert not self.q("~dst foobar", f)
        assert self.q("~dst :22", f)
        assert not self.q("~dst :99", f)
        assert self.q("~dst address:22", f)

    def test_and(self):
        f = self.flow()
        f.server_conn = tflow.tserver_conn()
        assert self.q("~b hello & ~b me", f)
        assert not self.q("~src wrongaddress & ~b hello", f)
        assert self.q("(~src :22 & ~dst :22) & ~b hello", f)
        assert not self.q("(~src address:22 & ~dst :22) & ~b nonexistent", f)
        assert not self.q("(~src address:22 & ~dst :99) & ~b hello", f)

    def test_or(self):
        f = self.flow()
        f.server_conn = tflow.tserver_conn()
        assert self.q("~b hello | ~b me", f)
        assert self.q("~src :22 | ~b me", f)
        assert not self.q("~src :99 | ~dst :99", f)
        assert self.q("(~src :22 | ~dst :22) | ~b me", f)

    def test_not(self):
        f = self.flow()
        assert not self.q("! ~src :22", f)
        assert self.q("! ~src :99", f)
        assert self.q("!~src :99 !~src :99", f)
        assert not self.q("!~src :99 !~src :22", f)


class TestMatchingDummyFlow:
    def flow(self):
        return tflow.tdummyflow()

    def err(self):
        return tflow.tdummyflow(err=True)

    def q(self, q, o):
        return flowfilter.parse(q)(o)

    def test_filters(self):
        e = self.err()
        f = self.flow()
        f.server_conn = tflow.tserver_conn()

        assert self.q("~all", f)

        assert not self.q("~a", f)

        assert not self.q("~b whatever", f)
        assert not self.q("~bq whatever", f)
        assert not self.q("~bs whatever", f)

        assert not self.q("~c 0", f)

        assert not self.q("~d whatever", f)

        assert self.q("~dst address", f)
        assert not self.q("~dst nonexistent", f)

        assert self.q("~e", e)
        assert not self.q("~e", f)

        assert not self.q("~http", f)
        assert not self.q("~tcp", f)
        assert not self.q("~websocket", f)

        assert not self.q("~h whatever", f)
        assert not self.q("~hq whatever", f)
        assert not self.q("~hs whatever", f)

        assert not self.q("~m whatever", f)

        assert not self.q("~s", f)

        assert self.q("~src 127.0.0.1", f)
        assert not self.q("~src nonexistent", f)

        assert not self.q("~tcp", f)

        assert not self.q("~t whatever", f)
        assert not self.q("~tq whatever", f)
        assert not self.q("~ts whatever", f)

        assert not self.q("~u whatever", f)

        assert not self.q("~q", f)

        assert not self.q("~comment .", f)
        f.comment = "comment"
        assert self.q("~comment .", f)


@patch("traceback.extract_tb")
def test_pyparsing_bug(extract_tb):
    """https://github.com/mitmproxy/mitmproxy/issues/1087"""
    # The text is a string with leading and trailing whitespace stripped; if the source is not available it is None.
    extract_tb.return_value = [("", 1, "test", None)]
    assert flowfilter.parse("test")


def test_match():
    with pytest.raises(ValueError):
        flowfilter.match("[foobar", tflow.tflow())

    assert flowfilter.match(None, tflow.tflow())
    assert not flowfilter.match("foobar", tflow.tflow())
