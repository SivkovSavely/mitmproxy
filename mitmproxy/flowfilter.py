"""
The following operators are understood:

    ~q          Request
    ~s          Response

Headers:

    Patterns are matched against "name: value" strings. Field names are
    all-lowercase.

    ~a          Asset content-type in response. Asset content types are:
                    text/javascript
                    application/x-javascript
                    application/javascript
                    text/css
                    image/*
                    font/*
                    application/font-*
    ~h rex      Header line in either request or response
    ~hq rex     Header in request
    ~hs rex     Header in response

    ~b rex      Expression in the body of either request or response
    ~bq rex     Expression in the body of request
    ~bs rex     Expression in the body of response
    ~t rex      Shortcut for content-type header.

    ~d rex      Request domain
    ~m rex      Method
    ~u rex      URL
    ~c CODE     Response code.
    rex         Equivalent to ~u rex

    Every regex filter has a case-sensitive variant with a "c" suffix
    (~bc, ~bqc, ..., ~uc), which always matches case-sensitively. The plain
    variants are case-insensitive unless MITMPROXY_CASE_SENSITIVE_FILTERS=1.

HTTP timestamps:

    Timestamps are compared against datetimes in the form YYYY-MM-DD or
    YYYY-MM-DD[ T]HH:MM:SS[.ffffff], optionally followed by Z or ±HH:MM.
    Supported comparisons: =, !=, >, >=, <, <=. Missing timestamps never
    match.

    ~dt cmp d   Any request/response timestamp
    ~dtq cmp d  Any request timestamp
    ~dts cmp d  Any response timestamp
    ~dtqs cmp d Request start timestamp
    ~dtqe cmp d Request end timestamp
    ~dtss cmp d Response start timestamp
    ~dtse cmp d Response end timestamp
"""

import functools
import operator
import os
import re
import sys
from abc import ABC
from abc import abstractmethod
from collections.abc import Iterator
from collections.abc import Sequence
from datetime import datetime
from typing import AnyStr
from typing import cast
from typing import ClassVar
from typing import Generic
from typing import Protocol

import pyparsing as pp

from mitmproxy import dns
from mitmproxy import flow
from mitmproxy import http
from mitmproxy import tcp
from mitmproxy import udp

maybe_ignore_case: re.RegexFlag = (
    cast(re.RegexFlag, re.IGNORECASE)
    if os.environ.get("MITMPROXY_CASE_SENSITIVE_FILTERS") != "1"
    else re.NOFLAG
)


def only(*types):
    def decorator(fn):
        @functools.wraps(fn)
        def filter_types(self, flow):
            if isinstance(flow, types):
                return fn(self, flow)
            return False

        return filter_types

    return decorator


class _Token(ABC):
    def dump(self, indent=0, fp=sys.stdout) -> None:
        print(
            "{spacing}{name}{expr}".format(
                spacing="\t" * indent,
                name=self.__class__.__name__,
                expr=getattr(self, "expr", ""),
            ),
            file=fp,
        )

    @abstractmethod
    def __str__(self) -> str: ...


class _Action(_Token, ABC):
    code: ClassVar[str]
    help: ClassVar[str]

    @classmethod
    def make(cls, s, loc, toks):
        return cls(*toks[1:])


class FErr(_Action):
    code = "e"
    help = "Match error"

    def __call__(self, f) -> bool:
        return bool(f.error)

    def __str__(self) -> str:
        return "has error"


class FMarked(_Action):
    code = "marked"
    help = "Match marked flows"

    def __call__(self, f) -> bool:
        return bool(f.marked)

    def __str__(self) -> str:
        return "is marked"


class FHTTP(_Action):
    code = "http"
    help = "Match HTTP flows"

    @only(http.HTTPFlow)
    def __call__(self, f) -> bool:
        return True

    def __str__(self) -> str:
        return "is an HTTP Flow"


class FWebSocket(_Action):
    code = "websocket"
    help = "Match WebSocket flows"

    @only(http.HTTPFlow)
    def __call__(self, f: http.HTTPFlow) -> bool:
        return f.websocket is not None

    def __str__(self) -> str:
        return "is a Websocket Flow"


class FTCP(_Action):
    code = "tcp"
    help = "Match TCP flows"

    @only(tcp.TCPFlow)
    def __call__(self, f) -> bool:
        return True

    def __str__(self) -> str:
        return "is a TCP Flow"


class FUDP(_Action):
    code = "udp"
    help = "Match UDP flows"

    @only(udp.UDPFlow)
    def __call__(self, f) -> bool:
        return True

    def __str__(self) -> str:
        return "is a UDP Flow"


class FDNS(_Action):
    code = "dns"
    help = "Match DNS flows"

    @only(dns.DNSFlow)
    def __call__(self, f) -> bool:
        return True

    def __str__(self) -> str:
        return "is a DNS Flow"


class FReq(_Action):
    code = "q"
    help = "Match request with no response"

    @only(http.HTTPFlow, dns.DNSFlow)
    def __call__(self, f) -> bool:
        return not f.response

    def __str__(self) -> str:
        return "has no response"


class FResp(_Action):
    code = "s"
    help = "Match response"

    @only(http.HTTPFlow, dns.DNSFlow)
    def __call__(self, f) -> bool:
        return bool(f.response)

    def __str__(self) -> str:
        return "has response"


class FAll(_Action):
    code = "all"
    help = "Match all flows"

    def __call__(self, f: flow.Flow) -> bool:
        return True

    def __str__(self) -> str:
        return "all flows"


class _Rex(Generic[AnyStr], _Action, ABC):
    flags: ClassVar[re.RegexFlag] = re.RegexFlag.NOFLAG
    # Case-sensitive variants ignore MITMPROXY_CASE_SENSITIVE_FILTERS and
    # never add re.IGNORECASE.
    case_sensitive: ClassVar[bool] = False

    expr: str
    re: re.Pattern[AnyStr]

    def __init__(self, expr_str: str, expr: AnyStr):
        self.expr = expr_str
        flags = self.flags
        if not self.case_sensitive:
            flags = flags | maybe_ignore_case
        try:
            self.re = re.compile(expr, flags)
        except Exception:
            raise ValueError("Cannot compile expression.")

    @property
    def regex_str(self) -> str:
        flags = ""
        if self.re.flags & re.IGNORECASE:
            flags += "i"
        if self.re.flags & re.MULTILINE:
            flags += "m"
        if self.re.flags & re.DOTALL:
            flags += "s"
        return f"/{self.expr}/{flags}"


class _StrRex(_Rex[str], ABC):
    def __init__(self, expr: str):
        super().__init__(expr, expr)


class _BinRex(_Rex[bytes], ABC):
    def __init__(self, expr: str):
        super().__init__(expr, expr.encode())


def _check_content_type(rex: re.Pattern[bytes], message: http.Message) -> bool:
    return any(
        name.lower() == b"content-type" and rex.search(value)
        for name, value in message.headers.fields
    )


class FAsset(_Action):
    code = "a"
    help = "Match asset in response: CSS, JavaScript, images, fonts."
    ASSET_TYPES = [
        re.compile(x)
        for x in [
            b"text/javascript",
            b"application/x-javascript",
            b"application/javascript",
            b"text/css",
            b"image/.*",
            b"font/.*",
            b"application/font.*",
        ]
    ]

    @only(http.HTTPFlow)
    def __call__(self, f) -> bool:
        if f.response:
            for i in self.ASSET_TYPES:
                if _check_content_type(i, f.response):
                    return True
        return False

    def __str__(self) -> str:
        return "is asset"


class FContentType(_BinRex):
    code = "t"
    help = "Content-type header"

    @only(http.HTTPFlow)
    def __call__(self, f) -> bool:
        if _check_content_type(self.re, f.request):
            return True
        elif f.response and _check_content_type(self.re, f.response):
            return True
        return False

    def __str__(self) -> str:
        return f"content type matches {self.regex_str}"


class FContentTypeRequest(_BinRex):
    code = "tq"
    help = "Request Content-Type header"

    @only(http.HTTPFlow)
    def __call__(self, f) -> bool:
        return _check_content_type(self.re, f.request)

    def __str__(self) -> str:
        return f"req. content type matches {self.regex_str}"


class FContentTypeResponse(_BinRex):
    code = "ts"
    help = "Response Content-Type header"

    @only(http.HTTPFlow)
    def __call__(self, f) -> bool:
        if f.response:
            return _check_content_type(self.re, f.response)
        return False

    def __str__(self) -> str:
        return f"resp. content type matches {self.regex_str}"


class FHead(_BinRex):
    code = "h"
    help = "Header"
    flags = re.MULTILINE

    @only(http.HTTPFlow)
    def __call__(self, f) -> bool:
        if f.request and self.re.search(bytes(f.request.headers)):
            return True
        if f.response and self.re.search(bytes(f.response.headers)):
            return True
        return False

    def __str__(self) -> str:
        return f"header matches {self.regex_str}"


class FHeadRequest(_BinRex):
    code = "hq"
    help = "Request header"
    flags = re.MULTILINE

    @only(http.HTTPFlow)
    def __call__(self, f) -> bool:
        if f.request and self.re.search(bytes(f.request.headers)):
            return True
        return False

    def __str__(self) -> str:
        return f"req. header matches {self.regex_str}"


class FHeadResponse(_BinRex):
    code = "hs"
    help = "Response header"
    flags = re.MULTILINE

    @only(http.HTTPFlow)
    def __call__(self, f) -> bool:
        if f.response and self.re.search(bytes(f.response.headers)):
            return True
        return False

    def __str__(self) -> str:
        return f"resp. header matches {self.regex_str}"


class FBod(_BinRex):
    code = "b"
    help = "Body"
    flags = re.DOTALL

    @only(http.HTTPFlow, tcp.TCPFlow, udp.UDPFlow, dns.DNSFlow)
    def __call__(self, f) -> bool:
        if isinstance(f, http.HTTPFlow):
            if (
                f.request
                and (content := f.request.get_content(strict=False)) is not None
            ):
                if self.re.search(content):
                    return True
            if (
                f.response
                and (content := f.response.get_content(strict=False)) is not None
            ):
                if self.re.search(content):
                    return True
            if f.websocket:
                for wmsg in f.websocket.messages:
                    if wmsg.content is not None and self.re.search(wmsg.content):
                        return True
        elif isinstance(f, (tcp.TCPFlow, udp.UDPFlow)):
            for msg in f.messages:
                if msg.content is not None and self.re.search(msg.content):
                    return True
        elif isinstance(f, dns.DNSFlow):
            if f.request and self.re.search(str(f.request).encode()):
                return True
            if f.response and self.re.search(str(f.response).encode()):
                return True
        return False

    def __str__(self) -> str:
        return f"body matches {self.regex_str}"


class FBodRequest(_BinRex):
    code = "bq"
    help = "Request body"
    flags = re.DOTALL

    @only(http.HTTPFlow, tcp.TCPFlow, udp.UDPFlow, dns.DNSFlow)
    def __call__(self, f) -> bool:
        if isinstance(f, http.HTTPFlow):
            if (
                f.request
                and (content := f.request.get_content(strict=False)) is not None
            ):
                if self.re.search(content):
                    return True
            if f.websocket:
                for wmsg in f.websocket.messages:
                    if wmsg.from_client and self.re.search(wmsg.content):
                        return True
        elif isinstance(f, (tcp.TCPFlow, udp.UDPFlow)):
            for msg in f.messages:
                if msg.from_client and self.re.search(msg.content):
                    return True
        elif isinstance(f, dns.DNSFlow):
            if f.request and self.re.search(str(f.request).encode()):
                return True
        return False

    def __str__(self) -> str:
        return f"body request matches {self.regex_str}"


class FBodResponse(_BinRex):
    code = "bs"
    help = "Response body"
    flags = re.DOTALL

    @only(http.HTTPFlow, tcp.TCPFlow, udp.UDPFlow, dns.DNSFlow)
    def __call__(self, f) -> bool:
        if isinstance(f, http.HTTPFlow):
            if (
                f.response
                and (content := f.response.get_content(strict=False)) is not None
            ):
                if self.re.search(content):
                    return True
            if f.websocket:
                for wmsg in f.websocket.messages:
                    if not wmsg.from_client and self.re.search(wmsg.content):
                        return True
        elif isinstance(f, (tcp.TCPFlow, udp.UDPFlow)):
            for msg in f.messages:
                if not msg.from_client and self.re.search(msg.content):
                    return True
        elif isinstance(f, dns.DNSFlow):
            if f.response and self.re.search(str(f.response).encode()):
                return True
        return False

    def __str__(self) -> str:
        return f"body response matches {self.regex_str}"


class FMethod(_BinRex):
    code = "m"
    help = "Method"

    @only(http.HTTPFlow)
    def __call__(self, f) -> bool:
        return bool(self.re.search(f.request.data.method))

    def __str__(self) -> str:
        return f"method matches {self.regex_str}"


class FDomain(_StrRex):
    code = "d"
    help = "Domain"

    @only(http.HTTPFlow)
    def __call__(self, f) -> bool:
        return bool(
            self.re.search(f.request.host) or self.re.search(f.request.pretty_host)
        )

    def __str__(self) -> str:
        return f"domain matches {self.regex_str}"


class FUrl(_StrRex):
    code = "u"
    help = "URL"

    # FUrl is special, because it can be "naked".

    @classmethod
    def make(cls, s, loc, toks):
        if len(toks) > 1:
            toks = toks[1:]
        return cls(*toks)

    @only(http.HTTPFlow, dns.DNSFlow)
    def __call__(self, f) -> bool:
        if not f or not f.request:
            return False
        if isinstance(f, http.HTTPFlow):
            return bool(self.re.search(f.request.pretty_url))
        assert isinstance(f, dns.DNSFlow)
        return bool(f.request.questions and self.re.search(f.request.questions[0].name))

    def __str__(self) -> str:
        return f"url matches {self.regex_str}"


class FSrc(_StrRex):
    code = "src"
    help = "Match source address"

    def __call__(self, f) -> bool:
        if not f.client_conn or not f.client_conn.peername:
            return False
        r = f"{f.client_conn.peername[0]}:{f.client_conn.peername[1]}"
        return bool(self.re.search(r))

    def __str__(self) -> str:
        return f"source address matches {self.regex_str}"


class FDst(_StrRex):
    code = "dst"
    help = "Match destination address"

    def __call__(self, f) -> bool:
        if not f.server_conn or not f.server_conn.address:
            return False
        r = f"{f.server_conn.address[0]}:{f.server_conn.address[1]}"
        return bool(self.re.search(r))

    def __str__(self) -> str:
        return f"destination address matches {self.regex_str}"


class FReplay(_Action):
    code = "replay"
    help = "Match replayed flows"

    def __call__(self, f):
        return f.is_replay is not None

    def __str__(self) -> str:
        return "flow has been replayed"


class FReplayClient(_Action):
    code = "replayq"
    help = "Match replayed client request"

    def __call__(self, f):
        return f.is_replay == "request"

    def __str__(self) -> str:
        return "request has been replayed"


class FReplayServer(_Action):
    code = "replays"
    help = "Match replayed server response"

    def __call__(self, f):
        return f.is_replay == "response"

    def __str__(self) -> str:
        return "response has been replayed"


class FMeta(_StrRex):
    code = "meta"
    help = "Flow metadata"
    flags = re.MULTILINE

    def __call__(self, f) -> bool:
        m = "\n".join([f"{key}: {value}" for key, value in f.metadata.items()])
        return bool(self.re.search(m))

    def __str__(self) -> str:
        return f"flow metadata matches {self.regex_str}"


class FMarker(_StrRex):
    code = "marker"
    help = "Match marked flows with specified marker"

    def __call__(self, f) -> bool:
        return bool(self.re.search(f.marked))

    def __str__(self) -> str:
        return f"marker matches {self.regex_str}"


class FComment(_StrRex):
    code = "comment"
    help = "Flow comment"
    flags = re.MULTILINE

    def __call__(self, f) -> bool:
        return bool(self.re.search(f.comment))

    def __str__(self) -> str:
        return f"comment matches {self.regex_str}"


_timestamp_comparators = {
    "=": operator.eq,
    "!=": operator.ne,
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
}

# A narrow grammar for HTTP timestamps: a date, optionally followed by a time
# (space or "T" separated) with an optional timezone. The internal space of
# "YYYY-MM-DD HH:MM:SS" is consumed by this single token so that it cannot be
# mistaken for the implicit-and separator between two filters.
_datetime_grammar = pp.Regex(
    r'["\']?\d{4}-\d{2}-\d{2}'
    r"(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)?"
    r'["\']?'
)
_comparator_grammar = pp.Regex(r"!=|>=|<=|=|>|<")


def parse_http_time(value: str) -> float:
    """Parse a filter datetime into a Unix timestamp.

    Date-only values denote midnight at the start of that date, timezone-less
    values are interpreted in the local timezone of the mitmproxy process.
    """
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        raise ValueError(f"Invalid datetime: {value!r}") from None
    if dt.tzinfo is None:
        dt = dt.astimezone()
    return dt.timestamp()


def _message_times(*messages: http.Message | None) -> Iterator[float]:
    for message in messages:
        if message is None:
            continue
        yield message.timestamp_start
        if message.timestamp_end is not None:
            yield message.timestamp_end


class FDateTime(_Action, ABC):
    """Compare HTTP timestamps against a datetime.

    A missing timestamp never matches, not even for `!=`. Aggregates match if
    any existing timestamp in their field set satisfies the comparison.
    """

    code: ClassVar[str]
    help: ClassVar[str]
    field: ClassVar[str]

    comparator: str
    value: str
    timestamp: float

    def __init__(self, comparator: str, value: str):
        if comparator not in _timestamp_comparators:
            raise ValueError(f"Invalid comparator: {comparator!r}")
        self.comparator = comparator
        self.value = value.strip("\"'")
        self.timestamp = parse_http_time(self.value)

    def timestamps(self, f: http.HTTPFlow) -> Iterator[float]:
        raise NotImplementedError  # pragma: no cover

    @only(http.HTTPFlow)
    def __call__(self, f) -> bool:
        compare = _timestamp_comparators[self.comparator]
        return any(compare(ts, self.timestamp) for ts in self.timestamps(f))

    def __str__(self) -> str:
        return f"{self.field} {self.comparator} {self.value}"


class FDateTimeAny(FDateTime):
    code = "dt"
    help = "Any HTTP request/response start/end timestamp"
    field = "http timestamp"

    def timestamps(self, f: http.HTTPFlow) -> Iterator[float]:
        yield from _message_times(f.request, f.response)


class FDateTimeRequest(FDateTime):
    code = "dtq"
    help = "Any request start/end timestamp"
    field = "request timestamp"

    def timestamps(self, f: http.HTTPFlow) -> Iterator[float]:
        yield from _message_times(f.request)


class FDateTimeResponse(FDateTime):
    code = "dts"
    help = "Any response start/end timestamp"
    field = "response timestamp"

    def timestamps(self, f: http.HTTPFlow) -> Iterator[float]:
        yield from _message_times(f.response)


class FDateTimeRequestStart(FDateTime):
    code = "dtqs"
    help = "Request start timestamp"
    field = "request start time"

    def timestamps(self, f: http.HTTPFlow) -> Iterator[float]:
        if f.request is not None:
            yield f.request.timestamp_start


class FDateTimeRequestEnd(FDateTime):
    code = "dtqe"
    help = "Request end timestamp"
    field = "request end time"

    def timestamps(self, f: http.HTTPFlow) -> Iterator[float]:
        if f.request is not None and f.request.timestamp_end is not None:
            yield f.request.timestamp_end


class FDateTimeResponseStart(FDateTime):
    code = "dtss"
    help = "Response start timestamp"
    field = "response start time"

    def timestamps(self, f: http.HTTPFlow) -> Iterator[float]:
        if f.response is not None:
            yield f.response.timestamp_start


class FDateTimeResponseEnd(FDateTime):
    code = "dtse"
    help = "Response end timestamp"
    field = "response end time"

    def timestamps(self, f: http.HTTPFlow) -> Iterator[float]:
        if f.response is not None and f.response.timestamp_end is not None:
            yield f.response.timestamp_end


class _Int(_Action, ABC):
    def __init__(self, num):
        self.num = int(num)


class FCode(_Int):
    code = "c"
    help = "HTTP response code"

    @only(http.HTTPFlow)
    def __call__(self, f) -> bool:
        if f.response and f.response.status_code == self.num:
            return True
        return False

    def __str__(self) -> str:
        return f"response code is {self.num}"


def _parenthesize(t: _Token) -> str:
    if isinstance(t, (FAnd, FOr)):
        return f"({t})"
    else:
        return str(t)


class FAnd(_Token):
    def __init__(self, lst):
        self.lst = lst

    def dump(self, indent=0, fp=sys.stdout):
        super().dump(indent, fp)
        for i in self.lst:
            i.dump(indent + 1, fp)

    def __call__(self, f) -> bool:
        return all(i(f) for i in self.lst)

    def __str__(self) -> str:
        return " and ".join(_parenthesize(x) for x in self.lst)


class FOr(_Token):
    def __init__(self, lst):
        self.lst = lst

    def dump(self, indent=0, fp=sys.stdout):
        super().dump(indent, fp)
        for i in self.lst:
            i.dump(indent + 1, fp)

    def __call__(self, f) -> bool:
        return any(i(f) for i in self.lst)

    def __str__(self) -> str:
        return " or ".join(_parenthesize(x) for x in self.lst)


class FNot(_Token):
    def __init__(self, itm):
        self.itm = itm[0]

    def dump(self, indent=0, fp=sys.stdout):
        super().dump(indent, fp)
        self.itm.dump(indent + 1, fp)

    def __call__(self, f) -> bool:
        return not self.itm(f)

    def __str__(self) -> str:
        return f"not {_parenthesize(self.itm)}"


filter_unary: Sequence[type[_Action]] = [
    FAsset,
    FErr,
    FHTTP,
    FMarked,
    FReplay,
    FReplayClient,
    FReplayServer,
    FReq,
    FResp,
    FTCP,
    FUDP,
    FDNS,
    FWebSocket,
    FAll,
]
_regex_filters: list[type[_Rex]] = [
    FBod,
    FBodRequest,
    FBodResponse,
    FContentType,
    FContentTypeRequest,
    FContentTypeResponse,
    FDomain,
    FDst,
    FHead,
    FHeadRequest,
    FHeadResponse,
    FMethod,
    FSrc,
    FUrl,
    FMeta,
    FMarker,
    FComment,
]
filter_int = [FCode]
filter_datetime: Sequence[type[FDateTime]] = [
    FDateTimeAny,
    FDateTimeRequest,
    FDateTimeResponse,
    FDateTimeRequestStart,
    FDateTimeRequestEnd,
    FDateTimeResponseStart,
    FDateTimeResponseEnd,
]


def _make_case_sensitive_variant(cls: type[_Rex]) -> type[_Rex]:
    """Create the forced case-sensitive `~<code>c` variant of a regex filter."""
    return cast(
        type[_Rex],
        type(
            f"{cls.__name__}CaseSensitive",
            (cls,),
            {
                "code": f"{cls.code}c",
                "help": f"{cls.help} (case-sensitive)",
                "case_sensitive": True,
            },
        ),
    )


# Every regex filter gets an explicit case-sensitive variant (~u -> ~uc, ...).
filter_rex: Sequence[type[_Rex]] = [
    *_regex_filters,
    *(_make_case_sensitive_variant(cls) for cls in _regex_filters),
]


def _make():
    # Order is important - multi-char expressions need to come before narrow
    # ones.
    parts = []
    for cls in filter_unary:
        f = pp.Literal(f"~{cls.code}") + pp.WordEnd()
        f.set_parse_action(cls.make)
        parts.append(f)

    # This is a bit of a hack to simulate Word(pyparsing_unicode.printables),
    # which has a horrible performance with len(pyparsing.pyparsing_unicode.printables) == 1114060
    unicode_words = pp.CharsNotIn("()~'\"" + pp.ParserElement.DEFAULT_WHITE_CHARS)
    unicode_words.skipWhitespace = True
    regex = (
        unicode_words
        | pp.QuotedString('"', esc_char="\\")
        | pp.QuotedString("'", esc_char="\\")
    )
    for cls in filter_rex:
        f = pp.Literal(f"~{cls.code}") + pp.WordEnd() + regex.copy()
        f.set_parse_action(cls.make)
        parts.append(f)

    for cls in filter_int:
        f = pp.Literal(f"~{cls.code}") + pp.WordEnd() + pp.Word(pp.nums)
        f.set_parse_action(cls.make)
        parts.append(f)

    for cls in filter_datetime:
        f = (
            pp.Literal(f"~{cls.code}")
            + pp.WordEnd()
            + _comparator_grammar
            + _datetime_grammar
        )
        f.set_parse_action(cls.make)
        parts.append(f)

    # A naked rex is a URL rex:
    f = regex.copy()
    f.set_parse_action(FUrl.make)
    parts.append(f)

    atom = pp.MatchFirst(parts)
    expr = pp.OneOrMore(
        pp.infix_notation(
            atom,
            [
                (pp.Literal("!").suppress(), 1, pp.opAssoc.RIGHT, lambda x: FNot(*x)),
                (pp.Literal("&").suppress(), 2, pp.opAssoc.LEFT, lambda x: FAnd(*x)),
                (pp.Literal("|").suppress(), 2, pp.opAssoc.LEFT, lambda x: FOr(*x)),
            ],
        )
    )
    return expr.set_parse_action(lambda x: FAnd(x) if len(x) != 1 else x)


bnf = _make()


class TFilter(Protocol):
    pattern: str

    def __call__(self, f: flow.Flow) -> bool: ...  # pragma: no cover

    def __str__(self) -> str: ...  # pragma: no cover

    def dump(self, indent=0, fp=sys.stdout): ...  # pragma: no cover


def parse(s: str) -> TFilter:
    """
    Parse a filter expression and return the compiled filter function.
    If the filter syntax is invalid, `ValueError` is raised.
    """
    if not s:
        raise ValueError("Empty filter expression")
    try:
        flt = bnf.parse_string(s, parse_all=True)[0]
        flt.pattern = s
        return flt
    except (pp.ParseException, ValueError) as e:
        raise ValueError(f"Invalid filter expression: {s!r}") from e


def match(flt: str | TFilter | None, flow: flow.Flow) -> bool:
    """
    Matches a flow against a compiled filter expression.
    Returns True if matched, False if not.

    If flt is a string, it will be compiled as a filter expression.
    If the expression is invalid, ValueError is raised.
    """
    if isinstance(flt, str):
        flt = parse(flt)
    if flt:
        return flt(flow)
    return True


match_all: TFilter = parse("~all")
"""A filter function that matches all flows"""


help = []
for a in filter_unary:
    help.append((f"~{a.code}", a.help))
for b in filter_rex:
    help.append((f"~{b.code} regex", b.help))
for c in filter_int:
    help.append((f"~{c.code} int", c.help))
for d in filter_datetime:
    help.append((f"~{d.code} datetime", f"{d.help} (=, !=, >, >=, <, <=)"))
help.sort()
help.extend(
    [
        ("!", "unary not"),
        ("&", "and"),
        ("|", "or"),
        ("(...)", "grouping"),
    ]
)
