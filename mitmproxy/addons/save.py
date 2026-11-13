import asyncio
import logging
import os.path
import sys
from collections.abc import Sequence
from concurrent.futures import Future
from datetime import datetime
from functools import lru_cache
from functools import partial
from pathlib import Path
from typing import Literal
from typing import Optional

import mitmproxy.types
from mitmproxy import command
from mitmproxy import ctx
from mitmproxy import dns
from mitmproxy import exceptions
from mitmproxy import flow
from mitmproxy import flowfilter
from mitmproxy import http
from mitmproxy import io
from mitmproxy import tcp
from mitmproxy import udp
from mitmproxy.io import sqlite
from mitmproxy.log import ALERT


@lru_cache
def _path(path: str) -> str:
    """Extract the path from a path spec (which may have an extra "+" at the front)"""
    if path.startswith("+"):
        path = path[1:]
    return os.path.expanduser(path)


@lru_cache
def _mode(path: str) -> Literal["ab", "wb"]:
    """Extract the writing mode (overwrite or append) from a path spec"""
    if path.startswith("+"):
        return "ab"
    else:
        return "wb"


class Save:
    def __init__(self) -> None:
        self.stream: io.FilteredFlowWriter | None = None
        self.filt: flowfilter.TFilter | None = None
        self.active_flows: set[flow.Flow] = set()
        self.current_path: str | None = None
        self.sqlite: sqlite.SQLiteFlowWriter | None = None
        self.sqlite_active_flows: set[flow.Flow] = set()
        self._sqlite_waiting_for_request: set[flow.Flow] = set()
        self._sqlite_deferred_check: asyncio.TimerHandle | None = None
        self._sqlite_start: Future[None] = Future()
        self._sqlite_closed: Future[None] | None = None
        self._sqlite_path: str | None = None
        self._event_loop: asyncio.AbstractEventLoop | None = None

    def load(self, loader):
        loader.add_option(
            "save_stream_file",
            Optional[str],
            None,
            """
            Stream flows to file as they arrive. Prefix path with + to append.
            The full path can use python strftime() formating, missing
            directories are created as needed. A new file is opened every time
            the formatted string changes.
            """,
        )
        loader.add_option(
            "save_stream_filter",
            Optional[str],
            None,
            "Filter which flows are written to file.",
        )
        loader.add_option(
            "save_sqlite_file",
            Optional[str],
            None,
            "Additionally record flows in an append-only SQLite database using "
            "independent zstd compression. Uses save_stream_filter. The parent "
            "directory must exist and not be shared-writable. Initialization "
            "errors abort startup. Later "
            "storage errors and queue overload are reported without stopping "
            "traffic; affected flows are not confirmed persisted.",
        )

    def configure(self, updated):
        if "save_stream_filter" in updated:
            if ctx.options.save_stream_filter:
                try:
                    self.filt = flowfilter.parse(ctx.options.save_stream_filter)
                except ValueError as e:
                    raise exceptions.OptionsError(str(e)) from e
            else:
                self.filt = None
        if "save_stream_file" in updated or "save_stream_filter" in updated:
            if ctx.options.save_stream_file:
                try:
                    self.maybe_rotate_to_new_file()
                except OSError as e:
                    raise exceptions.OptionsError(str(e)) from e
                assert self.stream
                self.stream.flt = self.filt
            else:
                self._finish_stream()
        if "save_sqlite_file" in updated:
            self._configure_sqlite()

    async def running(self) -> None:
        self._event_loop = ctx.master.event_loop
        self._configure_sqlite()
        if not self._sqlite_start.done():
            self._sqlite_start.set_result(None)
        if self.sqlite:
            try:
                await asyncio.wrap_future(self.sqlite.ready)
            except sqlite.StorageError:
                # The readiness callback reports initialization errors.
                return

    def _configure_sqlite(self) -> None:
        path = ctx.options.save_sqlite_file
        if path:
            path = os.path.abspath(os.path.expanduser(path))
        if path == self._sqlite_path:
            return
        self._finish_sqlite()
        if path:
            self._event_loop = ctx.master.event_loop
            self._sqlite_path = path
            self.sqlite = sqlite.SQLiteFlowWriter(
                path, after=self._sqlite_closed or self._sqlite_start
            )
            self.sqlite.ready.add_done_callback(
                partial(self._sqlite_result, path, "initialization")
            )

    def _sqlite_error(self, path: str, operation: str, error: BaseException) -> None:
        message = f"SQLite recording failed ({operation}) at {path}: {error}"
        report = logging.error if operation == "initialization" else logging.warning
        if self._event_loop and self._event_loop.is_running():
            try:
                self._event_loop.call_soon_threadsafe(report, message)
                return
            except RuntimeError:
                pass  # The loop closed while the worker was reporting its error.
        sys.stderr.write(message + "\n")

    def _sqlite_result(self, path: str, operation: str, result: Future) -> None:
        if error := result.exception():
            self._sqlite_error(path, operation, error)

    def _save_sqlite(self, f: flow.Flow) -> None:
        assert self.sqlite is not None and self._sqlite_path is not None
        try:
            if self.filt and not flowfilter.match(self.filt, f):
                return
            receipt = self.sqlite.submit(f)
        except Exception as e:
            self._sqlite_error(self._sqlite_path, f"flow {f.id}", e)
        else:
            receipt.add_done_callback(
                partial(self._sqlite_result, self._sqlite_path, f"flow {f.id}")
            )

    def _finish_sqlite(self) -> None:
        if self.sqlite:
            assert self._sqlite_path is not None
            if self._sqlite_deferred_check is not None:
                self._sqlite_deferred_check.cancel()
                self._sqlite_deferred_check = None
            for f in self.sqlite_active_flows:
                self._save_sqlite(f)
            self.sqlite_active_flows.clear()
            self._sqlite_waiting_for_request.clear()
            self._sqlite_closed = self.sqlite.close()
            self._sqlite_closed.add_done_callback(
                partial(self._sqlite_result, self._sqlite_path, "closing")
            )
            self.sqlite = None
            self._sqlite_path = None

    def _check_sqlite_deferred(self) -> None:
        self._sqlite_deferred_check = None
        for f in tuple(self._sqlite_waiting_for_request):
            if not f.live:
                self._save_sqlite(f)
                self.sqlite_active_flows.discard(f)
                self._sqlite_waiting_for_request.discard(f)
        if self._sqlite_waiting_for_request:
            assert self._event_loop is not None
            # A reset after an early response has no further HTTP lifecycle hook.
            self._sqlite_deferred_check = self._event_loop.call_later(
                0.1, self._check_sqlite_deferred
            )

    def maybe_rotate_to_new_file(self) -> None:
        path = datetime.today().strftime(_path(ctx.options.save_stream_file))
        if self.current_path == path:
            return

        if self.stream:
            self.stream.fo.close()
            self.stream = None

        new_log_file = Path(path)
        new_log_file.parent.mkdir(parents=True, exist_ok=True)

        f = new_log_file.open(_mode(ctx.options.save_stream_file))
        self.stream = io.FilteredFlowWriter(f, self.filt)
        self.current_path = path

    def save_flow(self, flow: flow.Flow) -> None:
        """
        Write the flow to the stream, but first check if we need to rotate to a new file.
        """
        if self.sqlite:
            if (
                isinstance(flow, http.HTTPFlow)
                and flow.live
                and flow.request.timestamp_end is None
                and flow.error is None
            ):
                # An early response can precede the end of a streamed upload.
                self._sqlite_waiting_for_request.add(flow)
                self.sqlite_active_flows.add(flow)
                if self._sqlite_deferred_check is None:
                    self._check_sqlite_deferred()
            else:
                self._save_sqlite(flow)
                self.sqlite_active_flows.discard(flow)
                self._sqlite_waiting_for_request.discard(flow)
        if not self.stream:
            return
        try:
            self.maybe_rotate_to_new_file()
            self.stream.add(flow)
        except OSError as e:
            # If we somehow fail to write flows to a logfile, we really want to crash visibly
            # instead of letting traffic through unrecorded.
            # No normal logging here, that would not be triggered anymore.
            sys.stderr.write(f"Error while writing to {self.current_path}: {e}")
            sys.exit(1)
        else:
            self.active_flows.discard(flow)

    def done(self) -> asyncio.Future[None] | None:
        try:
            self._finish_stream()
        finally:
            drain = self.close_sqlite()
        return drain

    def close_sqlite(self) -> asyncio.Future[None] | None:
        """Drain SQLite without changing the native recorder on early startup exit."""
        self._finish_sqlite()
        if not self._sqlite_start.done():
            self._sqlite_start.set_result(None)
        if self._sqlite_closed is not None:
            # Async shutdown awaits the drain; sync unload still initiates it.
            return asyncio.wrap_future(self._sqlite_closed, loop=self._event_loop)
        return None

    def _finish_stream(self) -> None:
        if self.stream:
            for f in self.active_flows:
                self.stream.add(f)
            self.active_flows.clear()

            self.current_path = None
            self.stream.fo.close()
            self.stream = None

    @command.command("save.file")
    def save(self, flows: Sequence[flow.Flow], path: mitmproxy.types.Path) -> None:
        """
        Save flows to a file. If the path starts with a +, flows are
        appended to the file, otherwise it is over-written.
        """
        try:
            with open(_path(path), _mode(path)) as f:
                stream = io.FlowWriter(f)
                for i in flows:
                    stream.add(i)
        except OSError as e:
            raise exceptions.CommandError(e) from e
        if path.endswith(".har") or path.endswith(".zhar"):  # pragma: no cover
            logging.log(
                ALERT,
                f"Saved as mitmproxy dump file. To save HAR files, use the `save.har` command.",
            )
        else:
            logging.log(ALERT, f"Saved {len(flows)} flows.")

    def tcp_start(self, flow: tcp.TCPFlow):
        if self.stream:
            self.active_flows.add(flow)
        if self.sqlite:
            self.sqlite_active_flows.add(flow)

    def tcp_end(self, flow: tcp.TCPFlow):
        self.save_flow(flow)

    def tcp_error(self, flow: tcp.TCPFlow):
        self.tcp_end(flow)

    def udp_start(self, flow: udp.UDPFlow):
        if self.stream:
            self.active_flows.add(flow)
        if self.sqlite:
            self.sqlite_active_flows.add(flow)

    def udp_end(self, flow: udp.UDPFlow):
        self.save_flow(flow)

    def udp_error(self, flow: udp.UDPFlow):
        self.udp_end(flow)

    def websocket_end(self, flow: http.HTTPFlow):
        self.save_flow(flow)

    def requestheaders(self, flow: http.HTTPFlow):
        if self.sqlite:
            self.sqlite_active_flows.add(flow)

    def request(self, flow: http.HTTPFlow):
        if self.stream:
            self.active_flows.add(flow)
        if self.sqlite:
            self.sqlite_active_flows.add(flow)
            if flow in self._sqlite_waiting_for_request:
                self._save_sqlite(flow)
                self.sqlite_active_flows.discard(flow)
                self._sqlite_waiting_for_request.discard(flow)

    def response(self, flow: http.HTTPFlow):
        # websocket flows will receive a websocket_end,
        # we don't want to persist them here already
        if flow.websocket is None:
            self.save_flow(flow)

    def error(self, flow: http.HTTPFlow):
        self.response(flow)

    def dns_request(self, flow: dns.DNSFlow):
        if self.stream:
            self.active_flows.add(flow)
        if self.sqlite:
            self.sqlite_active_flows.add(flow)

    def dns_response(self, flow: dns.DNSFlow):
        self.save_flow(flow)

    def dns_error(self, flow: dns.DNSFlow):
        self.save_flow(flow)
