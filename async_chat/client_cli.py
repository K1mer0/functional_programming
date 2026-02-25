#!/usr/bin/env python3
import asyncio
import sys
from client_lib import AsyncChatClient


def fmt(ev: dict) -> str:
    t = ev.get("type")
    if t == "msg":
        return f"[{ev.get('room')}] {ev.get('from')}: {ev.get('text')}"
    if t == "pm":
        return f"[PM] {ev.get('from')}: {ev.get('text')}"
    if t == "room_list":
        return f"Rooms: {', '.join(ev.get('rooms', []))}"
    if t == "user_list":
        return f"Users({ev.get('room','')}): {', '.join(ev.get('users', []))}"
    if t == "file_start":
        return f"[FILE] {ev.get('from')} is sending {ev.get('filename')} ({ev.get('size')} bytes)"
    if t == "file_end":
        return f"[FILE] done from {ev.get('from')}"
    if t in ("error", "info"):
        return f"[{t.upper()}] {ev.get('text', '')}"
    return str(ev)


async def ainput(prompt: str = "") -> str:
    return (await asyncio.to_thread(lambda: input(prompt))).strip()


async def main() -> None:
    if len(sys.argv) < 4:
        print("Usage: client_cli.py HOST PORT NAME [ROOM]")
        sys.exit(1)
    host = sys.argv[1]
    port = int(sys.argv[2])
    name = sys.argv[3]
    room = sys.argv[4] if len(sys.argv) >= 5 else "lobby"

    client = AsyncChatClient(host=host, port=port, name=name, room=room)

    async def on_event(ev: dict) -> None:
        print(fmt(ev))

    client.on_event = on_event
    await client.connect()

    print("Commands: /join, /rooms, /users, /pm, /file, /quit")

    try:
        while True:
            s = await ainput("> ")
            if not s:
                continue
            if s == "/quit":
                break
            if s.startswith("/join "):
                await client.join(s.split(" ", 1)[1].strip())
                continue
            if s == "/rooms":
                await client.list_rooms()
                continue
            if s == "/users":
                await client.list_users()
                continue
            if s.startswith("/pm "):
                _, rest = s.split(" ", 1)
                to, text = rest.split(" ", 1)
                await client.pm(to.strip(), text.strip())
                continue
            if s.startswith("/file "):
                path = s.split(" ", 1)[1].strip()
                try:
                    await client.send_file(path)
                except Exception as e:
                    print(f"[ERROR] File send failed: {e}")
                continue

            await client.send_msg(s)
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())