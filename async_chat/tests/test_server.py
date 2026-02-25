import asyncio
import json
import pytest
import pytest_asyncio

from server import ChatServer


def j(obj: dict) -> bytes:
    return (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")


async def read_until(reader: asyncio.StreamReader, pred, timeout=3.0):
    async def _inner():
        while True:
            line = await reader.readline()
            if not line:
                return None
            ev = json.loads(line.decode("utf-8"))
            if pred(ev):
                return ev
    return await asyncio.wait_for(_inner(), timeout=timeout)


async def wait_info_contains(reader: asyncio.StreamReader, needle: str, timeout=3.0):
    return await read_until(
        reader,
        lambda e: e.get("type") == "info" and needle in (e.get("text") or ""),
        timeout=timeout,
    )


@pytest_asyncio.fixture
async def server_port():
    srv = ChatServer(host="127.0.0.1", port=0)
    port = await srv.start()
    try:
        yield port
    finally:
        await srv.stop()


@pytest.mark.asyncio
async def test_join_and_broadcast(server_port):
    port = server_port

    r1, w1 = await asyncio.open_connection("127.0.0.1", port)
    r2, w2 = await asyncio.open_connection("127.0.0.1", port)

    try:
        # alice hello + join room1
        w1.write(j({"type": "hello", "name": "alice"}))
        w1.write(j({"type": "join", "room": "room1"}))
        await w1.drain()
        await wait_info_contains(r1, "You joined room room1")

        # bob hello + join room1
        w2.write(j({"type": "hello", "name": "bob"}))
        w2.write(j({"type": "join", "room": "room1"}))
        await w2.drain()
        await wait_info_contains(r2, "You joined room room1")

        # alice sends a message
        w1.write(j({"type": "msg", "text": "hi"}))
        await w1.drain()

        ev = await read_until(r2, lambda e: e.get("type") == "msg" and e.get("from") == "alice")
        assert ev["text"] == "hi"
        assert ev["room"] == "room1"

    finally:
        w1.close(); w2.close()
        await w1.wait_closed(); await w2.wait_closed()


@pytest.mark.asyncio
async def test_private_message(server_port):
    port = server_port

    r1, w1 = await asyncio.open_connection("127.0.0.1", port)
    r2, w2 = await asyncio.open_connection("127.0.0.1", port)

    try:
        w1.write(j({"type": "hello", "name": "alice"})); await w1.drain()
        await wait_info_contains(r1, "You are logged in as alice")

        w2.write(j({"type": "hello", "name": "bob"})); await w2.drain()
        await wait_info_contains(r2, "You are logged in as bob")

        w1.write(j({"type": "pm", "to": "bob", "text": "secret"}))
        await w1.drain()

        ev = await read_until(r2, lambda e: e.get("type") == "pm" and e.get("from") == "alice")
        assert ev["text"] == "secret"

    finally:
        w1.close(); w2.close()
        await w1.wait_closed(); await w2.wait_closed()


@pytest.mark.asyncio
async def test_bad_json_does_not_crash(server_port):
    port = server_port
    r, w = await asyncio.open_connection("127.0.0.1", port)
    try:
        w.write(b"{not_json}\n")
        await w.drain()

        ev = await read_until(r, lambda e: e.get("type") == "error")
        assert "Invalid" in (ev.get("text") or "")

        w.write(j({"type": "hello", "name": "alice"}))
        await w.drain()
        ok = await read_until(r, lambda e: e.get("type") in ("info", "error"))
        assert ok is not None
    finally:
        w.close()
        await w.wait_closed()