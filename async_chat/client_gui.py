#!/usr/bin/env python3
import asyncio
import threading
import queue
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from client_lib import AsyncChatClient


class ChatGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        root.title("Async Chat")

        self.net_thread = None
        self.loop = None
        self.client = None
        self.ui_q = queue.Queue()

        # Top panel
        top = ttk.Frame(root, padding=8)
        top.pack(fill="x")

        ttk.Label(top, text="Host").grid(row=0, column=0, sticky="w")
        self.e_host = ttk.Entry(top, width=18)
        self.e_host.insert(0, "127.0.0.1")
        self.e_host.grid(row=0, column=1, padx=4)

        ttk.Label(top, text="Port").grid(row=0, column=2, sticky="w")
        self.e_port = ttk.Entry(top, width=6)
        self.e_port.insert(0, "7777")
        self.e_port.grid(row=0, column=3, padx=4)

        ttk.Label(top, text="Name").grid(row=0, column=4, sticky="w")
        self.e_name = ttk.Entry(top, width=12)
        self.e_name.insert(0, "alice")
        self.e_name.grid(row=0, column=5, padx=4)

        ttk.Label(top, text="Room").grid(row=0, column=6, sticky="w")
        self.e_room = ttk.Entry(top, width=12)
        self.e_room.insert(0, "lobby")
        self.e_room.grid(row=0, column=7, padx=4)

        self.btn_connect = ttk.Button(top, text="Connect", command=self.connect)
        self.btn_connect.grid(row=0, column=8, padx=6)

        self.btn_rooms = ttk.Button(top, text="Rooms", command=self.list_rooms, state="disabled")
        self.btn_rooms.grid(row=0, column=9, padx=2)

        self.btn_users = ttk.Button(top, text="Users", command=self.list_users, state="disabled")
        self.btn_users.grid(row=0, column=10, padx=2)

        # Chat log
        mid = ttk.Frame(root, padding=8)
        mid.pack(fill="both", expand=True)

        self.txt = tk.Text(mid, height=18, wrap="word")
        self.txt.pack(fill="both", expand=True)

        # Bottom panel
        bot = ttk.Frame(root, padding=8)
        bot.pack(fill="x")

        self.e_msg = ttk.Entry(bot)
        self.e_msg.pack(side="left", fill="x", expand=True, padx=4)
        self.e_msg.bind("<Return>", lambda e: self.send_msg())

        self.btn_send = ttk.Button(bot, text="Send", command=self.send_msg, state="disabled")
        self.btn_send.pack(side="left", padx=4)

        self.btn_join = ttk.Button(bot, text="Join", command=self.join_room, state="disabled")
        self.btn_join.pack(side="left", padx=4)

        self.btn_file = ttk.Button(bot, text="Send file", command=self.send_file, state="disabled")
        self.btn_file.pack(side="left", padx=4)

        self.btn_pm = ttk.Button(bot, text="PM...", command=self.pm_dialog, state="disabled")
        self.btn_pm.pack(side="left", padx=4)

        self.root.after(100, self.poll_ui_queue)

    def log(self, s: str):
        self.txt.insert("end", s + "\n")
        self.txt.see("end")

    def connect(self):
        if self.net_thread:
            messagebox.showinfo("Info", "Already connected")
            return

        host = self.e_host.get().strip()
        port = int(self.e_port.get().strip())
        name = self.e_name.get().strip()
        room = self.e_room.get().strip()

        self.net_thread = threading.Thread(
            target=self.net_worker,
            args=(host, port, name, room),
            daemon=True
        )
        self.net_thread.start()

        self.btn_send.config(state="normal")
        self.btn_join.config(state="normal")
        self.btn_file.config(state="normal")
        self.btn_pm.config(state="normal")
        self.btn_rooms.config(state="normal")
        self.btn_users.config(state="normal")
        self.btn_connect.config(state="disabled")

    def net_worker(self, host, port, name, room):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        async def runner():
            self.client = AsyncChatClient(host=host, port=port, name=name, room=room)

            async def on_event(ev: dict):
                self.ui_q.put(ev)

            self.client.on_event = on_event
            await self.client.connect()

        try:
            self.loop.run_until_complete(runner())
            self.loop.run_forever()
        except Exception as e:
            self.ui_q.put({"type": "error", "text": f"NET error: {e}"})

    def poll_ui_queue(self):
        try:
            while True:
                ev = self.ui_q.get_nowait()
                self.log(self.format_event(ev))
        except queue.Empty:
            pass
        self.root.after(100, self.poll_ui_queue)

    def format_event(self, ev: dict) -> str:
        t = ev.get("type")
        if t == "msg":
            return f"[{ev.get('room')}] {ev.get('from')}: {ev.get('text')}"
        if t == "pm":
            return f"[PM] {ev.get('from')}: {ev.get('text')}"
        if t == "room_list":
            return "Rooms: " + ", ".join(ev.get("rooms", []))
        if t == "user_list":
            return f"Users({ev.get('room','')}): " + ", ".join(ev.get("users", []))
        if t == "file_start":
            return f"[FILE] {ev.get('from')} -> {ev.get('filename')} ({ev.get('size')} bytes)"
        if t == "file_end":
            return f"[FILE] done from {ev.get('from')}"
        if t in ("error", "info"):
            return f"[{t.upper()}] {ev.get('text')}"
        return str(ev)

    def run_coro(self, coro):
        if not self.loop:
            return
        asyncio.run_coroutine_threadsafe(coro, self.loop)

    def send_msg(self):
        if not self.client:
            return
        text = self.e_msg.get().strip()
        if not text:
            return
        self.e_msg.delete(0, "end")
        self.run_coro(self.client.send_msg(text))

    def join_room(self):
        if not self.client:
            return
        room = self.e_room.get().strip()
        self.run_coro(self.client.join(room))

    def list_rooms(self):
        if self.client:
            self.run_coro(self.client.list_rooms())

    def list_users(self):
        if self.client:
            self.run_coro(self.client.list_users())

    def pm_dialog(self):
        if not self.client:
            return
        win = tk.Toplevel(self.root)
        win.title("Private message")
        ttk.Label(win, text="To:").grid(row=0, column=0, padx=6, pady=6, sticky="w")
        e_to = ttk.Entry(win, width=20)
        e_to.grid(row=0, column=1, padx=6, pady=6)

        ttk.Label(win, text="Text:").grid(row=1, column=0, padx=6, pady=6, sticky="w")
        e_text = ttk.Entry(win, width=40)
        e_text.grid(row=1, column=1, padx=6, pady=6)

        def send():
            to = e_to.get().strip()
            text = e_text.get().strip()
            if to and text:
                self.run_coro(self.client.pm(to, text))
            win.destroy()

        ttk.Button(win, text="Send", command=send).grid(row=2, column=0, columnspan=2, pady=8)

    def send_file(self):
        if not self.client:
            return
        path = filedialog.askopenfilename()
        if not path:
            return
        self.run_coro(self.client.send_file(path))


def main():
    root = tk.Tk()
    ChatGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()