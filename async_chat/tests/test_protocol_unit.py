import asyncio
import json
import pytest

from client_lib import read_json_line


@pytest.mark.asyncio
async def test_read_json_line_ok():
    r = asyncio.StreamReader()
    r.feed_data((json.dumps({"type": "info", "text": "hi"}) + "\n").encode("utf-8"))
    r.feed_eof()
    ev = await read_json_line(r)
    assert ev["type"] == "info"
    assert ev["text"] == "hi"