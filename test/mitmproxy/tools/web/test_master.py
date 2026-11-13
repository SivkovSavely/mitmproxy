import asyncio

import pytest

from mitmproxy.options import Options
from mitmproxy.tools.web.app import State
from mitmproxy.tools.web.master import WebMaster


async def test_instance_identity():
    first = WebMaster(Options(), with_termlog=False)
    second = WebMaster(Options(), with_termlog=False)
    first.options.update(web_password="shared password")
    second.options.update(web_password="shared password")
    state = State.get_json(first)
    assert len(bytes.fromhex(state["instance_id"])) == 32
    assert state["instance_id"] == State.get_json(first)["instance_id"]
    assert state["instance_id"] != State.get_json(second)["instance_id"]
    assert state["sqlite_history"] is False


async def test_reuse():
    async def handler(r, w):
        pass

    server = await asyncio.start_server(
        handler, host="127.0.0.1", port=0, reuse_address=False
    )
    port = server.sockets[0].getsockname()[1]
    master = WebMaster(Options(), with_termlog=False)
    master.options.web_host = "127.0.0.1"
    master.options.web_port = port
    with pytest.raises(OSError, match=f"--set web_port={port + 2}"):
        await master.running()
    server.close()
    # tornado registers some callbacks,
    # we want to run them to avoid fatal warnings.
    await asyncio.sleep(0)
