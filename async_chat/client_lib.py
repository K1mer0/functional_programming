import asyncio
import base64
import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Callable, Awaitable, Dict, List, Tuple

MAX_LINE_BYTES = 32 * 1024


def dumps(obj: dict) -> bytes:
    return (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")


async def read_json_line(reader: asyncio.StreamReader) -> Optional[dict]:
    line = await reader.readline()
    if not line:
        return None
    if len(line) > MAX_LINE_BYTES:
        return {"type": "error", "text": "Server sent an overly long line"}
    try:
        return json.loads(line.decode("utf-8"))
    except Exception:
        return {"type": "error", "text": "Server sent invalid JSON"}


@dataclass
class AsyncChatClient:
    host: str
    port: int
    name: str
    room: str = "lobby"

    reader: Optional[asyncio.StreamReader] = None
    writer: Optional[asyncio.StreamWriter] = None

    on_event: Optional[Callable[[dict], Awaitable[None]]] = None
    _rx_task: Optional[asyncio.Task] = None

    _in_files: Dict[str, tuple] = field(default_factory=dict, init=False)

    _waiters: List[Tuple[Callable[[dict], bool], asyncio.Future]] = field(default_factory=list, init=False)

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)

        await self.send({"type": "hello", "name": self.name})
        await self.send({"type": "join", "room": self.room})

        self._rx_task = asyncio.create_task(self._rx_loop(), name="client-rx")

    async def close(self) -> None:
        # cancel rx
        if self._rx_task:
            self._rx_task.cancel()
            await asyncio.gather(self._rx_task, return_exceptions=True)

        # fail all pending waiters
        for _, fut in list(self._waiters):
            if not fut.done():
                fut.set_exception(ConnectionError("Client closed"))
        self._waiters.clear()

        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    async def send(self, obj: dict) -> None:
        if not self.writer:
            return
        self.writer.write(dumps(obj))
        await self.writer.drain()

    async def send_msg(self, text: str) -> None:
        await self.send({"type": "msg", "text": text})

    async def join(self, room: str) -> None:
        await self.send({"type": "join", "room": room})

    async def pm(self, to: str, text: str) -> None:
        await self.send({"type": "pm", "to": to, "text": text})

    async def list_rooms(self) -> None:
        await self.send({"type": "list_rooms"})

    async def list_users(self) -> None:
        await self.send({"type": "list_users"})

    def _add_waiter(self, predicate: Callable[[dict], bool]) -> asyncio.Future:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._waiters.append((predicate, fut))
        return fut

    def _notify_waiters(self, ev: dict) -> None:
        if not self._waiters:
            return
        for pred, fut in list(self._waiters):
            if fut.done():
                self._waiters.remove((pred, fut))
                continue
            try:
                if pred(ev):
                    fut.set_result(ev)
                    self._waiters.remove((pred, fut))
            except Exception:
                pass

    async def send_file(self, path: str, chunk_size: int = 48 * 1024, ack_timeout: float = 5.0) -> None:
        if not self.reader or not self.writer:
            raise RuntimeError("Not connected")

        filename = os.path.basename(path)
        size = os.path.getsize(path)

        ack_fut = self._add_waiter(lambda ev: ev.get("type") == "file_ack")

        await self.send({"type": "file_start", "filename": filename, "size": size})

        try:
            ack_ev = await asyncio.wait_for(ack_fut, timeout=ack_timeout)
        except asyncio.TimeoutError as e:
            raise TimeoutError("Timed out waiting for file_ack from server") from e

        fid = ack_ev.get("id")
        if not fid:
            raise RuntimeError("Server returned file_ack without id")

        seq = 0
        with open(path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                data = base64.b64encode(chunk).decode("ascii")
                await self.send({"type": "file_chunk", "id": fid, "seq": seq, "data": data})
                seq += 1

        await self.send({"type": "file_end", "id": fid})

    async def _rx_loop(self) -> None:
        assert self.reader is not None
        while True:
            ev = await read_json_line(self.reader)
            if ev is None:
                # connection closed
                # fail all pending waiters
                for _, fut in list(self._waiters):
                    if not fut.done():
                        fut.set_exception(ConnectionError("Connection closed by server"))
                self._waiters.clear()

                if self.on_event:
                    await self.on_event({"type": "info", "text": "Connection closed by server"})
                return

            self._notify_waiters(ev)

            await self._handle_incoming_file(ev)

            if self.on_event:
                await self.on_event(ev)

    async def _handle_incoming_file(self, ev: dict) -> None:
        t = ev.get("type")
        if t == "file_start":
            fid = ev.get("id")
            filename = ev.get("filename", "file.bin")
            os.makedirs("downloads", exist_ok=True)
            outpath = os.path.join("downloads", f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}")
            fh = open(outpath, "wb")
            self._in_files[fid] = (outpath, 0, fh)

        elif t == "file_chunk":
            fid = ev.get("id")
            if fid not in self._in_files:
                return
            outpath, written, fh = self._in_files[fid]
            try:
                data = base64.b64decode(ev.get("data", "").encode("ascii"), validate=True)
            except Exception:
                return
            fh.write(data)
            self._in_files[fid] = (outpath, written + len(data), fh)

        elif t == "file_end":
            fid = ev.get("id")
            if fid not in self._in_files:
                return
            outpath, written, fh = self._in_files.pop(fid)
            fh.close()
