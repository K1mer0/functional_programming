#!/usr/bin/env python3
import asyncio
import base64
import contextlib
import json
import logging
import signal
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional, Set, Any

MAX_LINE_BYTES = 256 * 1024
STREAM_LIMIT   = 256 * 1024   # StreamReader limit must be >= MAX_LINE_BYTES
DEFAULT_ROOM = "lobby"


def utc_ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def dumps(obj: dict) -> bytes:
    return (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")


async def safe_send(writer: asyncio.StreamWriter, obj: dict) -> None:
    writer.write(dumps(obj))
    await writer.drain()


@dataclass(eq=False)
class Client:
    writer: asyncio.StreamWriter
    reader: asyncio.StreamReader
    addr: str
    name: Optional[str] = None
    room: Optional[str] = None
    out_q: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=200))

    def __hash__(self) -> int:
        return id(self)


@dataclass
class Event:
    client: Client
    msg: dict


class ChatServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 7777) -> None:
        self.host = host
        self.port = port

        self._server: Optional[asyncio.AbstractServer] = None
        self._dispatcher_task: Optional[asyncio.Task] = None

        self._events: asyncio.Queue[Event] = asyncio.Queue(maxsize=5000)
        self._rooms: Dict[str, Set[Client]] = {}
        self._clients_by_name: Dict[str, Client] = {}
        self._client_tasks: Set[asyncio.Task] = set()

        # Active file transfers: id -> metadata (sender, room, filename, size)
        self._files: Dict[str, dict] = {}

    # lifecycle

    async def start(self) -> int:
        self._server = await asyncio.start_server(self._on_connect, self.host, self.port, limit=STREAM_LIMIT)
        sock = self._server.sockets[0]
        real_port = sock.getsockname()[1]
        logging.info("Server listening on %s:%s", self.host, real_port)

        self._dispatcher_task = asyncio.create_task(self._dispatcher(), name="dispatcher")
        return real_port

    async def stop(self) -> None:
        logging.info("Stopping server...")

        if self._dispatcher_task:
            self._dispatcher_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._dispatcher_task

        for t in list(self._client_tasks):
            t.cancel()
        await asyncio.gather(*self._client_tasks, return_exceptions=True)
        self._client_tasks.clear()

        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        self._rooms.clear()
        self._clients_by_name.clear()
        self._files.clear()

    # connection handling

    async def _on_connect(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        addr = writer.get_extra_info("peername")
        addr_s = f"{addr[0]}:{addr[1]}" if addr else "unknown"
        client = Client(writer=writer, reader=reader, addr=addr_s)

        logging.info("Client connected: %s", addr_s)

        read_task = asyncio.create_task(self._read_loop(client), name=f"read:{addr_s}")
        write_task = asyncio.create_task(self._write_loop(client), name=f"write:{addr_s}")

        self._client_tasks.update({read_task, write_task})

        done, pending = await asyncio.wait(
            {read_task, write_task}, return_when=asyncio.FIRST_COMPLETED
        )

        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

        self._client_tasks.difference_update({read_task, write_task})
        await self._cleanup_client(client)
        logging.info("Client disconnected: %s", addr_s)

    async def _read_loop(self, client: Client) -> None:
        try:
            await safe_send(client.writer, {
                "type": "info",
                "text": 'Welcome! First send: {"type":"hello","name":"..."}',
                "ts": utc_ts(),
            })
            while True:
                try:
                    line = await client.reader.readline()
                except ValueError:
                    # Raised when a single line exceeds StreamReader's internal limit
                    with contextlib.suppress(Exception):
                        await safe_send(client.writer, {
                            "type": "error",
                            "text": "Incoming line exceeds server read limit",
                            "ts": utc_ts(),
                        })
                    return
                if len(line) > MAX_LINE_BYTES:
                    await client.out_q.put({"type": "error", "text": "Message is too long", "ts": utc_ts()})
                    return

                try:
                    msg = json.loads(line.decode("utf-8"))
                    if not isinstance(msg, dict) or "type" not in msg:
                        raise ValueError("bad schema")
                except Exception:
                    # Important: do not crash, return an error to the client
                    await client.out_q.put({"type": "error", "text": "Invalid JSON or schema", "ts": utc_ts()})
                    continue

                # Push the event to the central queue
                try:
                    self._events.put_nowait(Event(client=client, msg=msg))
                except asyncio.QueueFull:
                    await client.out_q.put({"type": "error", "text": "Server overloaded (events queue)", "ts": utc_ts()})
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.exception("read_loop error")
        finally:
            pass

    async def _write_loop(self, client: Client) -> None:
        try:
            while True:
                obj = await client.out_q.get()
                await safe_send(client.writer, obj)
        except asyncio.CancelledError:
            raise
        except Exception:
            # If writer is broken, just exit
            return

    async def _cleanup_client(self, client: Client) -> None:
        # Remove from rooms and indexes
        if client.room:
            self._rooms.get(client.room, set()).discard(client)
            if client.room in self._rooms and not self._rooms[client.room]:
                del self._rooms[client.room]

        if client.name and self._clients_by_name.get(client.name) is client:
            del self._clients_by_name[client.name]

        try:
            client.writer.close()
            await client.writer.wait_closed()
        except Exception:
            pass

    # core dispatcher

    async def _dispatcher(self) -> None:
        while True:
            ev = await self._events.get()
            c = ev.client
            m = ev.msg
            mtype = m.get("type")

            try:
                if mtype == "hello":
                    await self._handle_hello(c, m)
                elif mtype == "join":
                    await self._handle_join(c, m)
                elif mtype == "msg":
                    await self._handle_msg(c, m)
                elif mtype == "pm":
                    await self._handle_pm(c, m)
                elif mtype == "list_rooms":
                    await self._handle_list_rooms(c)
                elif mtype == "list_users":
                    await self._handle_list_users(c)
                elif mtype in ("file_start", "file_chunk", "file_end"):
                    await self._handle_file(c, m)
                else:
                    await c.out_q.put({"type": "error", "text": f"Unknown type={mtype}", "ts": utc_ts()})
            except Exception:
                logging.exception("dispatcher handler error")
                with contextlib.suppress(Exception):
                    await c.out_q.put({"type": "error", "text": "Server-side processing error", "ts": utc_ts()})

    # handlers

    async def _handle_hello(self, c: Client, m: dict) -> None:
        name = str(m.get("name", "")).strip()
        if not name or len(name) > 32 or any(ch.isspace() for ch in name):
            await c.out_q.put({"type": "error", "text": "Invalid name (max 32 chars, no spaces)", "ts": utc_ts()})
            return
        if name in self._clients_by_name and self._clients_by_name[name] is not c:
            await c.out_q.put({"type": "error", "text": "Name already taken", "ts": utc_ts()})
            return

        # If renamed, update the index
        if c.name and c.name in self._clients_by_name and self._clients_by_name[c.name] is c:
            del self._clients_by_name[c.name]

        c.name = name
        self._clients_by_name[name] = c

        await c.out_q.put({"type": "info", "text": f"You are logged in as {name}. Use join to pick a room.", "ts": utc_ts()})

        # Convenience: auto-join lobby
        if not c.room:
            await self._join_room(c, DEFAULT_ROOM)

    async def _handle_join(self, c: Client, m: dict) -> None:
        if not c.name:
            await c.out_q.put({"type": "error", "text": "Send hello first", "ts": utc_ts()})
            return
        room = str(m.get("room", "")).strip()
        if not room or len(room) > 40:
            await c.out_q.put({"type": "error", "text": "Invalid room name", "ts": utc_ts()})
            return
        await self._join_room(c, room)

    async def _join_room(self, c: Client, room: str) -> None:
        old = c.room
        if old == room:
            await c.out_q.put({"type": "info", "text": f"You are already in room {room}", "ts": utc_ts()})
            return

        # Leave old room
        if old:
            self._rooms.get(old, set()).discard(c)
            await self._broadcast(old, {"type": "info", "text": f"{c.name} left room {old}", "ts": utc_ts()},
                                  exclude=c)
            if old in self._rooms and not self._rooms[old]:
                del self._rooms[old]

        # Join new room
        c.room = room
        self._rooms.setdefault(room, set()).add(c)
        await c.out_q.put({"type": "info", "text": f"You joined room {room}", "ts": utc_ts()})
        await self._broadcast(room, {"type": "info", "text": f"{c.name} joined room {room}", "ts": utc_ts()},
                              exclude=c)

    async def _handle_msg(self, c: Client, m: dict) -> None:
        if not c.name:
            await c.out_q.put({"type": "error", "text": "Send hello first", "ts": utc_ts()})
            return
        if not c.room:
            await c.out_q.put({"type": "error", "text": "Join a room first", "ts": utc_ts()})
            return
        text = str(m.get("text", "")).rstrip("\n")
        if not text:
            return
        if len(text) > 2000:
            await c.out_q.put({"type": "error", "text": "Message too long", "ts": utc_ts()})
            return

        payload = {"type": "msg", "room": c.room, "from": c.name, "text": text, "ts": utc_ts()}
        await self._broadcast(c.room, payload, exclude=None)

    async def _handle_pm(self, c: Client, m: dict) -> None:
        if not c.name:
            await c.out_q.put({"type": "error", "text": "Send hello first", "ts": utc_ts()})
            return
        to = str(m.get("to", "")).strip()
        text = str(m.get("text", "")).rstrip("\n")
        if not to or not text:
            await c.out_q.put({"type": "error", "text": "PM format: to + text", "ts": utc_ts()})
            return
        dst = self._clients_by_name.get(to)
        if not dst:
            await c.out_q.put({"type": "error", "text": f"User {to} not found", "ts": utc_ts()})
            return

        payload = {"type": "pm", "from": c.name, "text": text, "ts": utc_ts()}
        await dst.out_q.put(payload)
        await c.out_q.put({"type": "info", "text": f"PM sent -> {to}", "ts": utc_ts()})

    async def _handle_list_rooms(self, c: Client) -> None:
        rooms = sorted(self._rooms.keys())
        await c.out_q.put({"type": "room_list", "rooms": rooms, "ts": utc_ts()})

    async def _handle_list_users(self, c: Client) -> None:
        if not c.room:
            await c.out_q.put({"type": "user_list", "users": [], "ts": utc_ts()})
            return
        users = sorted([x.name for x in self._rooms.get(c.room, set()) if x.name])
        await c.out_q.put({"type": "user_list", "room": c.room, "users": users, "ts": utc_ts()})

    async def _handle_file(self, c: Client, m: dict) -> None:
        if not c.name or not c.room:
            await c.out_q.put({"type": "error", "text": "File transfer requires hello + join", "ts": utc_ts()})
            return

        t = m["type"]
        if t == "file_start":
            filename = str(m.get("filename", "")).strip()[:200]
            size = int(m.get("size", 0) or 0)
            if not filename or size < 0 or size > 200 * 1024 * 1024:
                await c.out_q.put({"type": "error", "text": "Invalid file (name/size)", "ts": utc_ts()})
                return

            fid = uuid.uuid4().hex
            meta = {"from": c.name, "room": c.room, "filename": filename, "size": size, "ts": utc_ts()}
            self._files[fid] = meta

            await c.out_q.put({"type": "file_ack", "id": fid, "ts": utc_ts()})
            await self._broadcast(c.room, {"type": "file_start", "id": fid, **meta}, exclude=c)

        elif t == "file_chunk":
            fid = str(m.get("id", "")).strip()
            if fid not in self._files:
                await c.out_q.put({"type": "error", "text": "Unknown file id", "ts": utc_ts()})
                return
            meta = self._files[fid]
            if meta["from"] != c.name or meta["room"] != c.room:
                await c.out_q.put({"type": "error", "text": "Forbidden file_chunk", "ts": utc_ts()})
                return

            seq = int(m.get("seq", 0) or 0)
            data = m.get("data", "")
            if not isinstance(data, str) or len(data) > 200_000:
                await c.out_q.put({"type": "error", "text": "Chunk too large", "ts": utc_ts()})
                return
            # Validate base64 to avoid relaying garbage
            try:
                base64.b64decode(data.encode("ascii"), validate=True)
            except Exception:
                await c.out_q.put({"type": "error", "text": "Invalid base64", "ts": utc_ts()})
                return

            await self._broadcast(c.room, {
                "type": "file_chunk", "id": fid, "seq": seq, "data": data, "from": c.name, "ts": utc_ts()
            }, exclude=c)

        elif t == "file_end":
            fid = str(m.get("id", "")).strip()
            meta = self._files.get(fid)
            if not meta:
                await c.out_q.put({"type": "error", "text": "Unknown file id", "ts": utc_ts()})
                return
            if meta["from"] != c.name or meta["room"] != c.room:
                await c.out_q.put({"type": "error", "text": "Forbidden file_end", "ts": utc_ts()})
                return

            await self._broadcast(c.room, {
                "type": "file_end", "id": fid, "from": c.name, "ts": utc_ts()
            }, exclude=c)
            del self._files[fid]

    # helpers

    async def _broadcast(self, room: str, payload: dict, exclude: Optional[Client]) -> None:
        targets = list(self._rooms.get(room, set()))
        for cl in targets:
            if exclude is not None and cl is exclude:
                continue
            try:
                cl.out_q.put_nowait(payload)
            except asyncio.QueueFull:
                # If a client is too slow, do not crash the server
                pass


async def amain() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    srv = ChatServer(host="0.0.0.0", port=7777)
    await srv.start()

    stop_ev = asyncio.Event()

    def _stop(*_: Any) -> None:
        stop_ev.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    await stop_ev.wait()
    await srv.stop()


if __name__ == "__main__":
    asyncio.run(amain())