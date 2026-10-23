#!/usr/bin/env python3
"""Mesh monitor for the Raspberry Pi touch screen (800x480).

Pages (bottom navigation): Overview (map, metrics, activity), Chat (LongFast),
Private (one conversation per node), Nodes (nodes, events, waypoints), Network (hop-ring route map, like the
Meshpi web panel). A node page has history charts (SNR, battery, voltage, airtime) and, for our own node, the radio
power read from meshtasticd (read only). Sent messages show their delivery status.
Talks to meshtasticd through mesh-proxy (TCP 4403), so it shares the node with the
phone app and the Telegram bridge. History and node-count stats come from the SQLite
file written by mesh-logger. Run with DISPLAY=:0 inside the desktop session.
"""
import json
import math
import os
import queue
import random
import sqlite3
import struct
import sys
import subprocess
import threading
import time
import tkinter as tk
import wave
from collections import defaultdict, deque
from tkinter import font as tkfont
from tkinter import ttk

from pubsub import pub
from meshtastic.tcp_interface import TCPInterface

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mesh_env  # noqa: E402
import mesh_extra  # noqa: E402

HOST = os.environ.get("MESH_HOST", "127.0.0.1")
PORT = int(os.environ.get("MESH_PORT", "4403"))
DB = os.environ.get("MESH_DB", os.path.expanduser("~/.local/share/mesh/messages.db"))
SOUND_DIR = os.path.expanduser("~/.cache/mesh-screen")
SETTINGS = os.path.expanduser("~/.config/mesh-screen/settings.json")
RADIO_STATE = os.path.expanduser("~/.config/mesh-screen/radio.json")
MAX_MSGS = 300
FAV_OFFLINE_S = 30 * 60      # a favourite is "offline" if not heard for this long
LINK_LOST_S = 90             # node link considered lost after this long offline

BG, CARD, PANEL, LINE = "#0b0f14", "#131a22", "#0f151c", "#222c38"
KEYBG, FG, DIM = "#1c2530", "#e6edf3", "#8b98a5"
ACCENT, OWN, WARN, ERR = "#3fb950", "#58a6ff", "#e3b341", "#f85149"

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  pkt_id INTEGER UNIQUE,
  t REAL NOT NULL,
  from_id TEXT, from_name TEXT,
  to_id TEXT, to_name TEXT,
  text TEXT NOT NULL,
  dm INTEGER NOT NULL DEFAULT 0,
  own INTEGER NOT NULL DEFAULT 0,
  snr REAL, rssi REAL, hops INTEGER
);
CREATE TABLE IF NOT EXISTS stats (
  t REAL PRIMARY KEY,
  online1h INTEGER NOT NULL,
  online24h INTEGER NOT NULL,
  total INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS heard (
  t REAL NOT NULL,
  node_id TEXT NOT NULL,
  snr REAL, rssi REAL, hops INTEGER
);
CREATE INDEX IF NOT EXISTS heard_node_t ON heard(node_id, t);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  t REAL NOT NULL,
  kind TEXT NOT NULL,
  node_id TEXT,
  text TEXT NOT NULL
);
"""

KEYS = {
    "ua": ["1234567890", "йцукенгшщзхї", "фівапролджє", "ячсмитьбюґ"],
    "en": ["1234567890", "qwertyuiop", "asdfghjkl", "zxcvbnm"],
}
QUICK = ["Ок", "Так", "Ні", "Дякую", "На зв'язку", "Чую вас"]
FILTERS = [("1 год", 3600), ("24 год", 86400), ("усі", None)]
NICE_KM = [1, 2, 5, 10, 20, 50, 100, 200, 500]
NAV = [("home", "◉", "Огляд"), ("chat", "▤", "Чат"), ("dm", "✉", "Приватні"), ("nodes", "☷", "Вузли"),
       ("net", "⌖", "Мережа")]
MAX_QUICK = 8
TRACE_GAP = 45               # s between our traceroutes: each one is a transmission on the shared channel
HOP_COLORS = {"up": "#f85149", "down": "#3fb950"}


def clean(text: str) -> str:
    """Tk can crash on characters outside the BMP (emoji); replace them."""
    return "".join(ch if ord(ch) <= 0xFFFF else "□" for ch in text or "")


def ago(age):
    if age is None:
        return "—"
    if age < 90:
        return "зараз"
    if age < 3600:
        return f"{int(age // 60)} хв"
    if age < 86400:
        return f"{int(age // 3600)} год"
    return f"{int(age // 86400)} дн"


def km_between(lat1, lon1, lat2, lon2):
    dx = (lon2 - lon1) * 111.32 * math.cos(math.radians((lat1 + lat2) / 2))
    dy = (lat2 - lat1) * 110.57
    return dx, dy


def make_sounds():
    os.makedirs(SOUND_DIR, exist_ok=True)

    def tone(path, freqs, dur=0.12, rate=22050):
        if os.path.exists(path):
            return
        frames = bytearray()
        for f in freqs:
            for i in range(int(rate * dur)):
                env = min(1, i / 300, (rate * dur - i) / 300)
                frames += struct.pack("<h", int(9000 * env * math.sin(2 * math.pi * f * i / rate)))
            frames += b"\x00\x00" * int(rate * 0.03)
        with wave.open(path, "wb") as w:
            w.setnchannels(1)
            w.setsampwidth(2)
            w.setframerate(rate)
            w.writeframes(bytes(frames))

    tone(os.path.join(SOUND_DIR, "msg.wav"), [880])
    tone(os.path.join(SOUND_DIR, "dm.wav"), [880, 1175, 1568])


def play(kind):
    path = os.path.join(SOUND_DIR, f"{kind}.wav")
    for cmd in (["pw-play", path], ["aplay", "-q", "-D", "plughw:2,0", path], ["aplay", "-q", path]):
        try:
            subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return
        except OSError:
            continue


def load_settings():
    try:
        with open(SETTINGS, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        data = {}
    return {"favorites": list(data.get("favorites", [])), "known": list(data.get("known", [])),
            "init": bool(data.get("init", False)), "hide_mqtt": bool(data.get("hide_mqtt", True)),
            "quick": [str(q)[:40] for q in data.get("quick", QUICK) if str(q).strip()][:MAX_QUICK] or list(QUICK)}


def save_settings(s):
    """Atomic + fsync, so a power cut cannot leave a half-written (zeroed) file."""
    try:
        os.makedirs(os.path.dirname(SETTINGS), exist_ok=True)
        tmp = SETTINGS + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(s, fh, ensure_ascii=False)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, SETTINGS)
        fd = os.open(os.path.dirname(SETTINGS), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def rrect(c, x1, y1, x2, y2, r=12, **kw):
    pts = [x1 + r, y1, x2 - r, y1, x2, y1, x2, y1 + r, x2, y2 - r, x2, y2, x2 - r, y2,
           x1 + r, y2, x1, y2, x1, y2 - r, x1, y1 + r, x1, y1]
    return c.create_polygon(pts, smooth=True, **kw)


def touch_scroll(widget, on_tap=None):
    """Drag-to-scroll for a Text/Treeview; a press without movement counts as a tap."""
    st = {"y": 0, "start": 0, "moved": False}
    step = 22

    def press(e):
        st.update(y=e.y_root, start=e.y_root, moved=False)
        return "break"

    def motion(e):
        if abs(e.y_root - st["start"]) > 10:
            st["moved"] = True
        dy = st["y"] - e.y_root
        if abs(dy) >= step:
            widget.yview_scroll(int(dy / step), "units")
            st["y"] = e.y_root
        return "break"

    def release(e):
        if not st["moved"] and on_tap:
            on_tap(e)
        return "break"

    widget.bind("<ButtonPress-1>", press)
    widget.bind("<B1-Motion>", motion)
    widget.bind("<ButtonRelease-1>", release)


class Link(threading.Thread):
    """Owns the TCP connection, reconnects forever, feeds node/status events to the UI."""

    def __init__(self, events: queue.Queue):
        super().__init__(daemon=True)
        self.events = events
        self.iface = None
        self.lost = threading.Event()
        pub.subscribe(self._on_lost, "meshtastic.connection.lost")
        pub.subscribe(self._on_packet, "meshtastic.receive")

    def _on_lost(self, interface=None):
        self.lost.set()

    def _on_packet(self, packet, interface=None):
        self.events.put(("pkt", packet))      # the UI thread sorts out environment, routes, telemetry, waypoints

    def run(self):
        while True:
            self.lost.clear()
            self.events.put(("status", "connecting"))
            try:
                self.iface = TCPInterface(hostname=HOST, portNumber=PORT)
                self.events.put(("status", "online"))
                self.events.put(("nodes", self._snapshot()))
                while not self.lost.wait(5):
                    self.events.put(("nodes", self._snapshot()))
            except Exception as exc:
                self.events.put(("status", f"offline: {type(exc).__name__}"))
            finally:
                try:
                    if self.iface:
                        self.iface.close()
                except Exception:
                    pass
                self.iface = None
            time.sleep(3)

    def _snapshot(self):
        iface = self.iface
        me = iface.getMyNodeInfo() if iface else {}
        nodes = []
        for n in (iface.nodes or {}).values():
            u = n.get("user", {})
            pos = n.get("position") or {}
            nodes.append({
                "id": u.get("id"),
                "name": u.get("shortName") or u.get("longName") or "?",
                "long": u.get("longName", ""),
                "snr": n.get("snr"),
                "hops": n.get("hopsAway"),
                "heard": n.get("lastHeard") or 0,
                "me": n.get("num") == me.get("num"),
                "num": n.get("num"),
                "lat": pos.get("latitude"),
                "lon": pos.get("longitude"),
                "hw": u.get("hwModel"),
                "role": u.get("role"),
                "dev": n.get("deviceMetrics") or {},
                "env": n.get("environmentMetrics") or {},
                "mqtt": bool(n.get("viaMqtt")),
            })
        chan, tx = "", None
        try:
            chan = iface.localNode.channels[0].settings.name or "LongFast"
            chan += f" · слот {iface.localNode.localConfig.lora.channel_num}"
            tx = iface.localNode.localConfig.lora.tx_power or None      # 0 = "default" in the firmware
        except Exception:
            pass
        return {"me": (me.get("user") or {}).get("longName", ""), "nodes": nodes, "chan": chan, "tx": tx,
                "num": me.get("num")}

    def send(self, text: str, dest: str | None = None):
        """Returns the packet id (0 if unknown) or None when not sent. The answer of the network (delivered / relayed /
        failed) comes back to the UI as an "ack" event."""
        if not self.iface:
            return None
        dest_num = int(dest[1:], 16) if dest and dest.startswith("!") else None
        pid = {"v": 0}

        def on_resp(resp):
            self.events.put(("ack", pid["v"], resp, dest_num))
        try:
            pkt = self.iface.sendText(text, destinationId=dest or "^all", wantAck=True, onResponse=on_resp,
                                      channelIndex=0)
            pid["v"] = getattr(pkt, "id", 0) or 0
            return pid["v"]
        except Exception:
            return None

    def trace(self, dest: str):
        """Traceroute to a node. The library waits for the answer, so this runs in its own thread; the answer also
        arrives as an ordinary packet and is recorded by the UI."""
        iface = self.iface
        if not iface:
            return False

        def work():
            try:
                hops = iface.localNode.localConfig.lora.hop_limit or 3
                iface.sendTraceRoute(dest, hops)
                self.events.put(("trace_done", dest, True))
            except Exception:
                self.events.put(("trace_done", dest, False))
        threading.Thread(target=work, daemon=True).start()
        return True

    def waypoint(self, name, lat, lon, ttl, wid=None, delete=False):
        iface = self.iface
        if not iface:
            return False
        try:
            if delete:
                iface.deleteWaypoint(wid)
            else:
                iface.sendWaypoint(name=name, description="", icon=0x1F4CD, expire=int(time.time()) + ttl,
                                   waypoint_id=wid, latitude=lat, longitude=lon, channelIndex=0)
            return True
        except Exception:
            return False


class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.events: queue.Queue = queue.Queue()
        self.layout = "ua"
        self.caps = False
        self.kb_visible = False
        self.active_entry = None
        self.page = "home"
        self.unread_chat = 0
        self.unread_peer = defaultdict(int)
        self.filter_i = 1
        self.snap = None
        self.has_chat = False
        self.threads = defaultdict(lambda: deque(maxlen=200))   # peer id -> messages
        self.peers = {}                                          # peer id -> {"name","t","text"}
        self.cur_peer = None
        self.map_zoom = 0
        self.sel = None           # selected node id on the overview map
        self.hits = []
        self.flash_job = None
        self.status_ok = False
        self.link_ok = False      # real connection to the node (not affected by send errors)
        self.settings = load_settings()
        self.fav_state = {}
        self.alerts = []          # [(node_id, text)] favourites currently offline
        self.link_down_since = None
        self.link_alert = False
        self.prev_page = "home"
        self.detail_id = None
        self.detail_hits = []
        self.nodes_view = "nodes"
        self.hide_mqtt = self.settings.get("hide_mqtt", True)
        self.env_hits = []
        self.links = mesh_extra.Links()
        self.traces = {}              # node id -> last traceroute answer
        self.relay_nodes = []         # [{num, hops, mqtt}] for resolving the relay byte of a packet
        self.my_num = None
        self.net_day = None           # None = today
        self.net_hits = []
        self.net_zoom = 0
        self.net_sel = None
        self.net_flows = {"up": True, "down": True}
        self.last_trace = 0.0
        self.tracing = None
        self.by_pkt = {}              # packet id -> message dict, to paint the delivery status
        self.detail_metric = "snr"
        self.radio_info = None
        self.quick_edit = False
        self.quick_bars = []          # (frame, on_send, entry) of each input bar
        self.wp_sel = None

        root.configure(bg=BG)
        root.option_add("*Button.highlightThickness", 0)
        root.geometry(f"{root.winfo_screenwidth()}x{root.winfo_screenheight()}+0+0")
        root.attributes("-fullscreen", True)
        root.bind("<Escape>", lambda e: root.destroy())
        mk = lambda size, w="normal": tkfont.Font(family="DejaVu Sans", size=size, weight=w)
        self.f_small, self.f_norm, self.f_bold = mk(10), mk(13), mk(13, "bold")
        self.f_key, self.f_tab, self.f_cap = mk(15, "bold"), mk(12, "bold"), mk(9, "bold")
        self.f_big, self.f_mid = mk(30, "bold"), mk(15, "bold")
        self.f_val = mk(18, "bold")

        try:
            make_sounds()
        except Exception:
            pass
        self._style()
        self._build()
        os.makedirs(os.path.dirname(DB), exist_ok=True)
        self.db = sqlite3.connect(DB, timeout=10)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA)
        mesh_env.ensure(self.db)
        mesh_extra.ensure(self.db)
        self.last_id = 0
        self._poll_db(initial=True)
        self._render_peers()
        self._load_events()
        self._log_power_permission()
        self._show_page("home")
        self.link = Link(self.events)
        self.link.start()
        threading.Thread(target=self._radio_loop, daemon=True).start()
        self._tick()
        self._pump()

    # ---- layout ---------------------------------------------------------
    def _style(self):
        s = ttk.Style()
        s.theme_use("clam")
        s.configure("Treeview", background=BG, fieldbackground=BG, foreground=FG, rowheight=44,
                    font=self.f_norm, borderwidth=0)
        s.configure("Treeview", bordercolor=BG, lightcolor=BG, darkcolor=BG)
        s.map("Treeview", background=[("selected", "#1d2a3a")], foreground=[("selected", FG)])
        s.configure("Treeview.Heading", background=PANEL, foreground=DIM, font=self.f_small,
                    relief="flat", borderwidth=0)

    def _build(self):
        r = self.root
        r.grid_columnconfigure(0, weight=1)
        r.grid_rowconfigure(2, weight=1)

        head = tk.Frame(r, bg=PANEL, height=46)
        head.grid(row=0, column=0, sticky="ew")
        head.pack_propagate(False)
        self.dot = tk.Label(head, text="●", bg=PANEL, fg=DIM, font=self.f_norm)
        self.dot.pack(side="left", padx=(14, 4))
        self.lbl_title = tk.Label(head, text="Mesh", bg=PANEL, fg=FG, font=self.f_mid)
        self.lbl_title.pack(side="left")
        self.lbl_status = tk.Label(head, text="", bg=PANEL, fg=DIM, font=self.f_small)
        self.lbl_status.pack(side="left", padx=12)
        tk.Button(head, text="⏻", font=self.f_tab, bg=PANEL, fg=DIM, relief="flat", bd=0,
                  activebackground=PANEL, activeforeground=FG, command=self._ask_exit
                  ).pack(side="right", ipadx=14, fill="y")
        self.lbl_clock = tk.Label(head, text="", bg=PANEL, fg=FG, font=self.f_mid)
        self.lbl_clock.pack(side="right", padx=10)
        strip = tk.Frame(r, bg=PANEL)
        strip.grid(row=1, column=0, sticky="ew")
        self.flash_bar = tk.Frame(strip, bg=PANEL, height=3)
        self.flash_bar.pack(fill="x")
        self.banner = tk.Label(strip, text="", bg="#3a1418", fg="#ffb4b0", font=self.f_small, anchor="w")
        self.banner.bind("<Button-1>", lambda e: self._tap_banner())

        stack = tk.Frame(r, bg=BG)
        stack.grid(row=2, column=0, sticky="nsew")
        stack.grid_rowconfigure(0, weight=1)
        stack.grid_columnconfigure(0, weight=1)
        self.pages = {}
        for key, _, _ in NAV:
            f = tk.Frame(stack, bg=BG)
            f.grid(row=0, column=0, sticky="nsew")
            self.pages[key] = f
        f = tk.Frame(stack, bg=BG)          # node details (not in the bottom nav)
        f.grid(row=0, column=0, sticky="nsew")
        self.pages["node"] = f
        f = tk.Frame(stack, bg=BG)          # environment details
        f.grid(row=0, column=0, sticky="nsew")
        self.pages["env"] = f

        self.kb = tk.Frame(r, bg=PANEL)
        self.kb.grid(row=3, column=0, sticky="ew")
        self._render_kb()
        self.kb.grid_remove()

        nav = tk.Frame(r, bg=PANEL, height=58)
        nav.grid(row=4, column=0, sticky="ew")
        nav.grid_propagate(False)
        nav.grid_rowconfigure(0, weight=1)
        self.nav = {}
        for i, (key, icon, label) in enumerate(NAV):
            nav.grid_columnconfigure(i, weight=1, uniform="nav")
            cell = tk.Frame(nav, bg=PANEL)
            cell.grid(row=0, column=i, sticky="nsew")
            bar = tk.Frame(cell, bg=PANEL, height=3)
            bar.pack(fill="x")
            lbl = tk.Label(cell, text="", bg=PANEL, fg=DIM, font=self.f_tab)
            lbl.pack(expand=True, fill="both")
            for w in (cell, bar, lbl):
                w.bind("<Button-1>", lambda e, k=key: self._show_page(k))
            self.nav[key] = (cell, bar, lbl)

        self._build_home(self.pages["home"])
        self._build_chat(self.pages["chat"])
        self._build_dm(self.pages["dm"])
        self._build_nodes(self.pages["nodes"])
        self._build_net(self.pages["net"])
        self._build_detail(self.pages["node"])
        self._build_env(self.pages["env"])

    def _input_bar(self, page, on_send):
        """Quick replies + entry + send button. Returns the entry."""
        inp = tk.Frame(page, bg=PANEL)
        inp.pack(side="bottom", fill="x")
        quick = tk.Frame(page, bg=BG)
        quick.pack(side="bottom", fill="x")
        inp.grid_columnconfigure(0, weight=1)
        var = tk.StringVar()
        entry = tk.Entry(inp, textvariable=var, font=self.f_norm, bg=BG, fg=FG, insertbackground=FG,
                         relief="flat", highlightthickness=1, highlightbackground=LINE, highlightcolor=ACCENT)
        entry.grid(row=0, column=0, sticky="ew", padx=8, pady=6, ipady=8)
        entry.bind("<Button-1>", lambda e, en=entry: self._focus_entry(en))
        entry.bind("<Return>", lambda e: on_send(None))
        tk.Button(inp, text="Надіслати", font=self.f_bold, bg=ACCENT, fg="#06210d", relief="flat", bd=0,
                  activebackground="#2ea043", command=lambda: on_send(None)
                  ).grid(row=0, column=1, padx=(0, 8), ipadx=12, ipady=6)
        entry.var = var
        self.quick_bars.append((quick, on_send, entry))
        self._render_quick()
        return entry

    def _render_quick(self):
        """Quick replies are editable like in the Android app: ＋ saves the typed text, ✎ switches delete mode."""
        for frame, on_send, entry in self.quick_bars:
            for w in frame.winfo_children():
                w.destroy()
            for text in self.settings["quick"]:
                if self.quick_edit:
                    tk.Button(frame, text="✕ " + text, font=self.f_small, bg="#3a1418", fg="#ffb4b0", relief="flat", bd=0,
                              activebackground="#4a1c20", command=lambda t=text: self._quick_del(t)
                              ).pack(side="left", padx=3, pady=5, ipadx=6, ipady=4)
                else:
                    tk.Button(frame, text=text, font=self.f_small, bg=KEYBG, fg=FG, relief="flat", bd=0,
                              activebackground="#2a3644", command=lambda t=text, f=on_send: f(t)
                              ).pack(side="left", padx=3, pady=5, ipadx=8, ipady=4)
            tk.Button(frame, text="✎", font=self.f_small, bg=WARN if self.quick_edit else PANEL, fg="#241a00" if self.quick_edit else DIM,
                      relief="flat", bd=0, command=self._quick_toggle).pack(side="right", padx=3, pady=5, ipadx=8, ipady=4)
            if not self.quick_edit:
                tk.Button(frame, text="＋", font=self.f_small, bg=PANEL, fg=DIM, relief="flat", bd=0,
                          command=lambda en=entry: self._quick_add(en)).pack(side="right", padx=0, pady=5, ipadx=8, ipady=4)

    def _quick_toggle(self):
        self.quick_edit = not self.quick_edit
        self._render_quick()

    def _quick_add(self, entry):
        text = entry.var.get().strip()[:40]
        q = self.settings["quick"]
        if text and text not in q and len(q) < MAX_QUICK:
            q.append(text)
            save_settings(self.settings)
            entry.var.set("")
            self._render_quick()
        else:
            self._hint_bar("щоб додати швидку відповідь, наберіть її в полі (не більше %d)" % MAX_QUICK)

    def _quick_del(self, text):
        q = self.settings["quick"]
        if text in q and len(q) > 1:
            q.remove(text)
            save_settings(self.settings)
        self._render_quick()

    def _text(self, parent):
        t = tk.Text(parent, bg=BG, fg=FG, font=self.f_norm, wrap="word", state="disabled", width=1, height=1,
                    borderwidth=0, highlightthickness=0, padx=14, pady=6, cursor="arrow", spacing3=6)
        touch_scroll(t)
        t.tag_configure("time", foreground=DIM, font=self.f_small)
        t.tag_configure("name", foreground=ACCENT, font=self.f_bold)
        t.tag_configure("own", foreground=OWN, font=self.f_bold)
        t.tag_configure("snr", foreground=DIM, font=self.f_small)
        t.tag_configure("hint", foreground=DIM, font=self.f_small)
        t.tag_configure("mine", justify="right", lmargin1=120, lmargin2=120, foreground=FG)
        t.tag_configure("mine_head", justify="right", foreground=OWN, font=self.f_small)
        t.tag_configure("theirs", foreground=FG)
        t.tag_configure("ack_pending", foreground=DIM, font=self.f_small)
        t.tag_configure("ack_relayed", foreground=WARN, font=self.f_small)
        t.tag_configure("ack_delivered", foreground=ACCENT, font=self.f_small)
        t.tag_configure("ack_failed", foreground=ERR, font=self.f_small)
        return t

    def _build_home(self, page):
        self.canvas = tk.Canvas(page, bg=BG, highlightthickness=0, width=1, height=1)
        self.canvas.pack(fill="both", expand=True)
        self.canvas.bind("<ButtonRelease-1>", self._tap_home)
        self.canvas.bind("<Configure>", lambda e: self._draw_home())

    def _build_chat(self, page):
        self.chat_entry = self._input_bar(page, lambda t: self._send(t, None))
        self.chat = self._text(page)
        self.chat.pack(side="top", fill="both", expand=True)
        self._hint(self.chat, "Чат каналу LongFast. Поки що повідомлень немає, нові з'являться тут.\n"
                              "Історія зберігається в базі й не зникає після перезапуску.\n")

    def _build_dm(self, page):
        left = tk.Frame(page, bg=PANEL, width=250)
        left.pack(side="left", fill="y")
        left.pack_propagate(False)
        tk.Label(left, text="РОЗМОВИ", bg=PANEL, fg=DIM, font=self.f_cap, anchor="w"
                 ).pack(fill="x", padx=14, pady=(10, 6))
        self.peer_tree = ttk.Treeview(left, columns=("n",), show="", selectmode="none")
        self.peer_tree.column("n", width=240)
        self.peer_tree.pack(fill="both", expand=True)
        self.peer_tree.tag_configure("unread", foreground=WARN)
        touch_scroll(self.peer_tree, on_tap=self._tap_peer)

        right = tk.Frame(page, bg=BG)
        right.pack(side="left", fill="both", expand=True)
        self.dm_head = tk.Label(right, text="", bg=PANEL, fg=WARN, font=self.f_bold, anchor="w")
        self.dm_head.pack(side="top", fill="x", ipady=8)
        self.dm_entry = self._input_bar(right, lambda t: self._send(t, self.cur_peer))
        self.dm_text = self._text(right)
        self.dm_text.pack(side="top", fill="both", expand=True)
        self._open_peer(None)

    def _build_nodes(self, page):
        bar = tk.Frame(page, bg=PANEL)
        bar.pack(side="top", fill="x")
        self.seg = {}
        for key, label in (("nodes", "Вузли"), ("events", "Події"), ("points", "Точки")):
            b = tk.Button(bar, text=label, font=self.f_small, relief="flat", bd=0,
                          command=lambda k=key: self._set_nodes_view(k))
            b.pack(side="left", padx=(8, 0), pady=5, ipadx=12, ipady=3)
            self.seg[key] = b
        self.lbl_count = tk.Label(bar, text="", bg=PANEL, fg=DIM, font=self.f_small)
        self.lbl_count.pack(side="left", padx=14)
        self.btn_filter = tk.Button(bar, text="", font=self.f_small, bg=KEYBG, fg=FG, relief="flat", bd=0,
                                    activebackground="#2a3644", command=self._cycle_filter)
        self.btn_filter.pack(side="right", padx=8, pady=5, ipadx=10, ipady=3)
        self.btn_mqtt = tk.Button(bar, text="", font=self.f_small, bg=KEYBG, fg=FG, relief="flat", bd=0,
                                  activebackground="#2a3644", command=self._toggle_mqtt)
        self.btn_mqtt.pack(side="right", padx=0, pady=5, ipadx=10, ipady=3)
        self.lbl_hint = tk.Label(bar, text="", bg=PANEL, fg=DIM, font=self.f_small)
        self.lbl_hint.pack(side="right", padx=8)

        self.ev_tree = ttk.Treeview(page, columns=("t", "text"), show="headings", selectmode="none")
        self.ev_tree.heading("t", text="Час", anchor="w")
        self.ev_tree.heading("text", text="Подія", anchor="w")
        self.ev_tree.column("t", width=130, anchor="w", stretch=False)
        self.ev_tree.column("text", width=600, anchor="w", stretch=True)
        self.ev_tree.tag_configure("fav_off", foreground=ERR)
        self.ev_tree.tag_configure("link_lost", foreground=ERR)
        self.ev_tree.tag_configure("fav_on", foreground=ACCENT)
        self.ev_tree.tag_configure("link_ok", foreground=ACCENT)
        self.ev_tree.tag_configure("new", foreground=WARN)
        touch_scroll(self.ev_tree)

        # waypoints: a list plus a line to create one at our position (name typed on the on-screen keyboard)
        self.wp_bar = tk.Frame(page, bg=BG)
        self.wp_var = tk.StringVar()
        self.wp_ttl_i = 0
        self.wp_entry = tk.Entry(self.wp_bar, textvariable=self.wp_var, font=self.f_norm, bg=BG, fg=FG, insertbackground=FG,
                                 relief="flat", highlightthickness=1, highlightbackground=LINE, highlightcolor=ACCENT)
        self.wp_entry.var = self.wp_var
        self.wp_entry.bind("<Button-1>", lambda e: self._focus_entry(self.wp_entry))
        self.wp_entry.pack(side="left", fill="x", expand=True, padx=(8, 6), pady=6, ipady=6)
        self.btn_wp_ttl = tk.Button(self.wp_bar, text="", font=self.f_small, bg=KEYBG, fg=FG, relief="flat", bd=0,
                                    activebackground="#2a3644", command=self._wp_cycle_ttl)
        self.btn_wp_ttl.pack(side="left", padx=2, ipadx=8, ipady=6)
        tk.Button(self.wp_bar, text="＋ Точка тут", font=self.f_small, bg=ACCENT, fg="#06210d", relief="flat", bd=0,
                  activebackground="#2ea043", command=self._wp_add).pack(side="left", padx=2, ipadx=8, ipady=6)
        tk.Button(self.wp_bar, text="Видалити", font=self.f_small, bg="#3a1418", fg="#ffb4b0", relief="flat", bd=0,
                  activebackground="#4a1c20", command=self._wp_del).pack(side="left", padx=(2, 8), ipadx=8, ipady=6)
        self.wp_tree = ttk.Treeview(page, columns=("name", "dist", "from", "left"), show="headings", selectmode="none")
        for c, text, w, anchor in (("name", "Точка", 250, "w"), ("dist", "км", 70, "e"), ("from", "Від", 170, "w"),
                                   ("left", "Лишилось", 110, "e")):
            self.wp_tree.heading(c, text=text, anchor=anchor)
            self.wp_tree.column(c, width=w, anchor=anchor, stretch=(c == "name"))
        self.wp_tree.tag_configure("mine", foreground=OWN)
        self.wp_tree.tag_configure("sel", background="#1d2a3a")
        touch_scroll(self.wp_tree, on_tap=self._tap_wp)
        self._wp_cycle_ttl(first=True)

        cols = ("name", "env", "dist", "snr", "hops", "ago")
        self.tree = ttk.Treeview(page, columns=cols, show="headings", selectmode="none")
        for c, text, w, anchor in (("name", "Вузол", 190, "w"), ("env", "Довкілля", 250, "w"),
                                   ("dist", "км", 60, "e"), ("snr", "SNR", 55, "e"), ("hops", "Хопи", 55, "e"),
                                   ("ago", "Чули", 90, "e")):
            self.tree.heading(c, text=text, anchor=anchor)
            self.tree.column(c, width=w, anchor=anchor, stretch=(c == "name"))
        self.tree.pack(side="top", fill="both", expand=True)
        self.tree.tag_configure("me", foreground=OWN)
        self.tree.tag_configure("old", foreground=DIM)
        touch_scroll(self.tree, on_tap=self._tap_node)
        self._update_filter_label()
        self._update_mqtt_label()
        self._set_nodes_view("nodes")

    def _set_nodes_view(self, view):
        self.nodes_view = view
        for key, b in self.seg.items():
            on = key == view
            b.config(bg=ACCENT if on else KEYBG, fg="#06210d" if on else FG,
                     activebackground=ACCENT if on else "#2a3644")
        for w in (self.tree, self.ev_tree, self.wp_tree, self.wp_bar):
            w.pack_forget()
        self.btn_filter.pack_forget()
        self.btn_mqtt.pack_forget()
        if view != "points":
            self._show_kb(False)
        if view == "nodes":
            self.tree.pack(side="top", fill="both", expand=True)
            self.btn_filter.pack(side="right", padx=8, pady=5, ipadx=10, ipady=3)
            self.btn_mqtt.pack(side="right", padx=0, pady=5, ipadx=10, ipady=3)
            self.lbl_hint.config(text="")
        elif view == "events":
            self.ev_tree.pack(side="top", fill="both", expand=True)
            self.lbl_hint.config(text="журнал подій (нові вгорі)")
            self.lbl_count.config(text="")
        else:
            self.wp_bar.pack(side="top", fill="x")
            self.wp_tree.pack(side="top", fill="both", expand=True)
            self.lbl_hint.config(text="точки з ефіру та ваші")
            self.lbl_count.config(text="")
            self._render_wps()

    # ---- waypoints ------------------------------------------------------
    WP_TTLS = [("24 год", 86400), ("7 днів", 7 * 86400), ("без строку", None)]

    def _wp_cycle_ttl(self, first=False):
        if not first:
            self.wp_ttl_i = (self.wp_ttl_i + 1) % len(self.WP_TTLS)
        self.btn_wp_ttl.config(text="строк: " + self.WP_TTLS[self.wp_ttl_i][0])

    def _wp_changed(self):
        if self.page == "nodes" and self.nodes_view == "points":
            self._render_wps()
        elif self.page == "home":
            self._draw_home()

    def _render_wps(self):
        me = self._me()
        first = self.wp_tree.yview()[0]
        self.wp_tree.delete(*self.wp_tree.get_children())
        now = time.time()
        names = {n["id"]: n["name"] for n in (self.snap["nodes"] if self.snap else [])}
        for w in mesh_extra.list_waypoints(self.db, now):
            d = None
            if me and me["lat"] is not None:
                d = math.hypot(*km_between(me["lat"], me["lon"], w["lat"], w["lon"]))
            left = "∞" if w["expire"] is None else ago(max(w["expire"] - now, 0))
            mine = bool(me) and w["sender"] == me["id"]
            tags = (("mine",) if mine else ()) + (("sel",) if w["id"] == self.wp_sel else ())
            self.wp_tree.insert("", "end", iid=str(w["id"]), tags=tags, values=(
                clean(mesh_extra.wp_icon(w["icon"]) + " " + (w["name"] or "без назви")),
                f"{d:.1f}" if d is not None else "·", clean(names.get(w["sender"], w["sender"] or "?")) + (" (я)" if mine else ""),
                left))
        self.wp_tree.yview_moveto(first)

    def _tap_wp(self, e):
        iid = self.wp_tree.identify_row(e.y)
        self.wp_sel = int(iid) if iid else None
        self._render_wps()

    def _my_waypoint_packet(self, wid, lat, lon, expire, name):
        me = self._me()
        return {"from": self.my_num, "fromId": me["id"], "decoded": {"portnum": "WAYPOINT_APP", "waypoint": {
            "id": wid, "latitudeI": int(lat * 1e7), "longitudeI": int(lon * 1e7), "expire": expire, "name": name,
            "icon": 0x1F4CD}}}

    def _wp_add(self):
        me = self._me()
        if not me or me["lat"] is None or self.my_num is None:
            self._hint_bar("у ноди немає позиції: точку нема де поставити")
            return
        name = (self.wp_var.get().strip() or "Точка")[:30]
        ttl = self.WP_TTLS[self.wp_ttl_i][1]
        expire = mesh_extra.WP_NEVER if ttl is None else int(time.time()) + ttl
        wid = random.randint(1, 10 ** 9 - 1)
        if not self.link.waypoint(name, me["lat"], me["lon"], ttl or 10 * 365 * 86400, wid=wid):
            self._hint_bar("не надіслано: немає зв'язку з нодою")
            return
        mesh_extra.record_waypoint(self.db, self._my_waypoint_packet(wid, me["lat"], me["lon"], expire, name))
        self.wp_var.set("")
        self.wp_sel = wid
        self._hint_bar("точку надіслано в ефір")
        self._render_wps()

    def _wp_del(self):
        me = self._me()
        w = next((x for x in mesh_extra.list_waypoints(self.db) if x["id"] == self.wp_sel), None)
        if not w:
            self._hint_bar("торкніться точки в списку")
        elif not me or w["sender"] != me["id"]:
            self._hint_bar("видалити можна лише свою точку")
        elif self.link.waypoint(None, 0, 0, 0, wid=w["id"], delete=True):
            mesh_extra.record_waypoint(self.db, self._my_waypoint_packet(w["id"], w["lat"], w["lon"], 0, w["name"]))
            self.wp_sel = None
            self._render_wps()
        else:
            self._hint_bar("не надіслано: немає зв'язку з нодою")

    def _build_detail(self, page):
        self.dcanvas = tk.Canvas(page, bg=BG, highlightthickness=0, width=1, height=1)
        self.dcanvas.pack(fill="both", expand=True)
        self.dcanvas.bind("<ButtonRelease-1>", self._tap_detail)
        self.dcanvas.bind("<Configure>", lambda e: self._draw_detail())

    def _render_kb(self):
        for w in self.kb.winfo_children():
            w.destroy()
        for ri, row in enumerate(KEYS[self.layout]):
            f = tk.Frame(self.kb, bg=PANEL)
            f.pack(fill="x", padx=4, pady=1)
            for i, ch in enumerate(row):
                label = ch.upper() if self.caps else ch
                f.grid_columnconfigure(i, weight=1, uniform=f"r{ri}")
                tk.Button(f, text=label, font=self.f_key, bg=KEYBG, fg=FG, relief="flat", bd=0,
                          activebackground="#2a3644", command=lambda c=label: self._type(c)
                          ).grid(row=0, column=i, sticky="ew", padx=1, ipady=6)
        f = tk.Frame(self.kb, bg=PANEL)
        f.pack(fill="x", padx=4, pady=(1, 4))

        def btn(text, cmd, w):
            return tk.Button(f, text=text, font=self.f_key, bg="#2a3644", fg=FG, relief="flat", bd=0,
                             activebackground="#364555", command=cmd, width=w)
        btn("⇧", self._toggle_caps, 3).pack(side="left", padx=1, ipady=6)
        btn("УКР" if self.layout == "en" else "EN", self._toggle_layout, 4).pack(side="left", padx=1, ipady=6)
        tk.Button(f, text="пробіл", font=self.f_key, bg=KEYBG, fg=FG, relief="flat", bd=0,
                  command=lambda: self._type(" ")).pack(side="left", fill="x", expand=True, padx=1, ipady=6)
        for ch in ".,?!":
            btn(ch, lambda c=ch: self._type(c), 2).pack(side="left", padx=1, ipady=6)
        btn("⌫", self._backspace, 4).pack(side="left", padx=1, ipady=6)
        btn("▾", lambda: self._show_kb(False), 3).pack(side="left", padx=1, ipady=6)

    # ---- pages ----------------------------------------------------------
    def _show_page(self, name):
        self.page = name
        self.pages[name].tkraise()
        if name not in ("chat", "dm") and not (name == "nodes" and self.nodes_view == "points"):
            self._show_kb(False)
        if name == "chat":
            self.unread_chat = 0
            self.chat.see("end")
        if name == "dm" and self.cur_peer:
            self.unread_peer[self.cur_peer] = 0
            self._render_peers()
        if name == "home":
            self._draw_home()
        if name == "node":
            self._draw_detail()
        if name == "net":
            self._draw_net()
        if name == "env":
            self._draw_env()
        self._update_tabs()

    def _update_tabs(self):
        unread_dm = sum(self.unread_peer.values())
        badges = {"chat": self.unread_chat if self.page != "chat" else 0, "dm": unread_dm}
        for key, icon, label in NAV:
            cell, bar, lbl = self.nav[key]
            active = key == self.page
            n = badges.get(key, 0)
            lbl.config(text=f"{icon}  {label}" + (f"  ●{n}" if n else ""),
                       fg=(WARN if n else (FG if active else DIM)),
                       bg=CARD if active else PANEL)
            cell.config(bg=CARD if active else PANEL)
            bar.config(bg=ACCENT if active else PANEL)

    # ---- power menu -----------------------------------------------------
    def _log_power_permission(self):
        try:
            out = subprocess.run(["busctl", "call", "org.freedesktop.login1", "/org/freedesktop/login1",
                                  "org.freedesktop.login1.Manager", "CanPowerOff"],
                                 capture_output=True, text=True, timeout=5).stdout.strip()
        except Exception as exc:
            out = f"error {exc}"
        print("CanPowerOff:", out, flush=True)

    def _ask_exit(self):
        box = tk.Frame(self.root, bg=CARD, highlightthickness=2, highlightbackground=LINE)
        box.place(relx=0.5, rely=0.5, anchor="center")
        tk.Label(box, text="Живлення", bg=CARD, fg=FG, font=self.f_bold).pack(padx=40, pady=(20, 6))
        self.power_msg = tk.Label(box, text="Pi вимикається безпечно: дані зберігаються на карту.",
                                  bg=CARD, fg=DIM, font=self.f_small, wraplength=360)
        self.power_msg.pack(padx=20, pady=(0, 10))
        for text, color, cmd in (
                ("Вимкнути Pi", ERR, lambda: self._power("poweroff")),
                ("Перезавантажити Pi", WARN, lambda: self._power("reboot")),
                ("Закрити застосунок", KEYBG, self.root.destroy),
                ("Скасувати", KEYBG, box.destroy)):
            tk.Button(box, text=text, font=self.f_bold, bg=color, fg="white" if color != KEYBG else FG,
                      relief="flat", bd=0, command=cmd).pack(fill="x", padx=24, pady=4, ipady=9)
        tk.Frame(box, height=14, bg=CARD).pack()

    def _power(self, action):
        self.power_msg.config(text="Зберігаю дані та виконую…", fg=WARN)
        self.root.update_idletasks()
        subprocess.run(["sync"])
        last = ""
        for cmd in (["systemctl", "--no-ask-password", action], ["sudo", "-n", "systemctl", action]):
            try:
                r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
            except Exception as exc:
                last = str(exc)
                continue
            if r.returncode == 0:
                return
            last = ((r.stderr or r.stdout).strip().splitlines() or ["невідома помилка"])[-1]
        self.power_msg.config(
            text=f"Немає дозволу ({last[:60]}).\nОдноразово виконай через SSH: sudo bash ~/mesh-power-install.sh",
            fg=ERR)

    # ---- overview -------------------------------------------------------
    def _me(self):
        return next((n for n in self.snap["nodes"] if n["me"]), None) if self.snap else None

    def _dist(self, n, me):
        if not me or me["lat"] is None or n["lat"] is None or n is me:
            return None
        dx, dy = km_between(me["lat"], me["lon"], n["lat"], n["lon"])
        return math.hypot(dx, dy)

    def _metrics(self):
        now = time.time()
        me = self._me()
        others = [n for n in self._pool() if not n["me"]]
        on1 = [n for n in others if n["heard"] and now - n["heard"] < 3600]
        on24 = [n for n in others if n["heard"] and now - n["heard"] < 86400]
        far = max(((self._dist(n, me), n) for n in on24 if self._dist(n, me) is not None),
                  key=lambda x: x[0], default=(None, None))
        direct = [n["snr"] for n in on1 if n["hops"] == 0 and n["snr"] is not None]
        try:
            msgs = self.db.execute("SELECT COUNT(*), MAX(t) FROM messages WHERE t>? AND own=0",
                                   (now - 86400,)).fetchone()
        except sqlite3.Error:
            msgs = (0, None)
        try:
            series = self.db.execute("SELECT t,online1h FROM stats WHERE t>? ORDER BY t",
                                     (now - 86400,)).fetchall()
        except sqlite3.Error:
            series = []
        recent = sorted(on24, key=lambda n: -n["heard"])[:3]
        return {"now": now, "me": me, "on1": len(on1), "on24": len(on24), "total": len(others),
                "far": far, "snr": (sum(direct) / len(direct)) if direct else None, "direct": len(direct),
                "msgs": msgs[0] or 0, "last_msg": msgs[1], "series": series, "recent": recent}

    def _card(self, c, x1, y1, x2, y2, title):
        rrect(c, x1, y1, x2, y2, 14, fill=CARD, outline=LINE)
        c.create_text(x1 + 14, y1 + 15, anchor="w", fill=DIM, font=self.f_cap, text=title)

    def _draw_home(self):
        c = self.canvas
        c.delete("all")
        self.hits = []
        if self.page != "home":
            return
        W, H = c.winfo_width(), c.winfo_height()
        if W < 100 or H < 100:
            return
        if not self.snap:
            c.create_text(W // 2, H // 2, fill=DIM, font=self.f_norm, text="Підключення до ноди…")
            return
        m = self._metrics()
        pad = 10
        mw = int((W - 3 * pad) * 0.58)
        self._draw_map(c, pad, pad, pad + mw, H - pad, m)

        rx1, rx2 = pad + mw + pad, W - pad
        cw = (rx2 - rx1 - pad) // 2
        ch = 62
        spots = [(rx1, pad), (rx1 + cw + pad, pad), (rx1, pad + ch + pad), (rx1 + cw + pad, pad + ch + pad)]
        far_d, far_n = m["far"]
        cards = [
            ("ОНЛАЙН · 1 ГОД", str(m["on1"]), ACCENT, f"{m['on24']} за добу"),
            ("ЧАТ · 24 ГОД", str(m["msgs"]), OWN,
             ("останнє " + ago(m["now"] - m["last_msg"])) if m["last_msg"] else "поки немає"),
            ("НАЙДАЛЬШИЙ", f"{far_d:.1f} км" if far_d is not None else "—", WARN,
             clean(far_n["name"]) if far_n else "немає позицій"),
            ("ЯКІСТЬ ЗВ'ЯЗКУ", f"{m['snr']:.0f} dB" if m["snr"] is not None else "—", FG,
             f"прямих вузлів: {m['direct']}"),
        ]
        for (x, y), (title, val, col, sub) in zip(spots, cards):
            self._card(c, x, y, x + cw, y + ch, title)
            c.create_text(x + 14, y + 33, anchor="w", fill=col, font=self.f_val if len(val) < 9 else self.f_norm,
                          text=val)
            c.create_text(x + 14, y + ch - 9, anchor="w", fill=DIM, font=self.f_small, text=sub)

        ey1 = pad + 2 * (ch + pad)
        eh = 76
        self._draw_env_card(c, rx1, ey1, rx2, ey1 + eh, self._env())

        ay1 = ey1 + eh + pad
        ay2 = H - pad
        self._card(c, rx1, ay1, rx2, ay2, "ВУЗЛІВ ОНЛАЙН · 24 ГОД")
        self._spark(c, rx1 + 14, ay1 + 26, rx2 - 14, ay1 + 50, m["series"])
        row_y = ay1 + 58
        rows = max(1, min(len(m["recent"]), (ay2 - row_y) // 28))
        avail = (ay2 - row_y) // rows
        for n in m["recent"][:rows]:
            age = m["now"] - n["heard"]
            col = ACCENT if age < 3600 else WARN
            cy = row_y + avail // 2
            c.create_oval(rx1 + 14, cy - 4, rx1 + 22, cy + 4, fill=col, outline="")
            c.create_text(rx1 + 32, cy, anchor="w", fill=FG, font=self.f_bold, text=clean(n["name"])[:9])
            right = (f"{n['snr']:.0f} dB · " if n["snr"] is not None else "") + \
                    (f"{n['hops']} хоп · " if n["hops"] is not None else "") + ago(age)
            c.create_text(rx2 - 14, cy, anchor="e", fill=DIM, font=self.f_small, text=right)
            self.hits.append(("rect", rx1, row_y, rx2, row_y + avail, lambda i=n["id"]: self._select(i)))
            row_y += avail
        if not m["recent"]:
            c.create_text((rx1 + rx2) // 2, row_y + avail // 2, fill=DIM, font=self.f_small,
                          text="поки нікого не чути")

    SHORT_LABEL = {"temperature": "темп.", "humidity": "волог.", "pressure": "тиск", "iaq": "IAQ", "gas": "газ",
                   "lux": "світло", "wind": "вітер", "pm25": "PM2.5", "co2": "CO₂"}

    @staticmethod
    def _metric_text(mt, short=False):
        if short and mt["id"] == "pressure":      # the unit goes to the caption: three values must fit one row
            return f"{mt['avg']:.0f}"
        f = mesh_env.SHORT.get(mt["id"])
        return f(mt["avg"]) if f else f"{mt['avg']}{mt['unit']}"

    def _draw_env_card(self, c, x1, y1, x2, y2, env):
        n_src = len(env["sources"])
        self._card(c, x1, y1, x2, y2,
                   f"ДОВКІЛЛЯ · ДО {env['max_hops']} ХОПІВ" + (f" · {n_src} ВУЗЛ." if n_src else ""))
        self.hits.append(("rect", x1, y1, x2, y2, self._open_env))
        ms = env["metrics"][:3]
        if not ms:
            c.create_text(x1 + 14, y1 + 46, anchor="w", fill=DIM, font=self.f_small,
                          text="поки немає даних від вузлів із датчиками")
            return
        w = (x2 - x1 - 20) / len(ms)
        for i, mt in enumerate(ms):
            x = x1 + 14 + i * w
            c.create_text(x, y1 + 40, anchor="w", fill=FG, font=self.f_val,
                          text=self._metric_text(mt, short=True))
            label = "тиск, гПа" if mt["id"] == "pressure" else self.SHORT_LABEL.get(mt["id"], mt["label"])
            c.create_text(x, y1 + 62, anchor="w", fill=DIM, font=self.f_small, text=f"{label} · {mt['n']}")

    # ---- environment page --------------------------------------------------------------------------
    def _open_env(self):
        if self.page != "env":
            self.prev_page = self.page
        self._show_page("env")

    def _build_env(self, page):
        self.ecanvas = tk.Canvas(page, bg=BG, highlightthickness=0, width=1, height=1)
        self.ecanvas.pack(fill="both", expand=True)
        self.ecanvas.bind("<ButtonRelease-1>", self._tap_env)
        self.ecanvas.bind("<Configure>", lambda e: self._draw_env())

    def _tap_env(self, e):
        for h in reversed(self.env_hits):
            if h[0] <= e.x <= h[2] and h[1] <= e.y <= h[3]:
                h[4]()
                return

    @staticmethod
    def _line(c, x1, y1, x2, y2, series):
        if len(series) < 2:
            return
        t0, t1 = series[0][0], series[-1][0]
        lo, hi = min(v for _, v in series), max(v for _, v in series)
        span = (hi - lo) or 1
        pts = []
        for t, v in series:
            pts += [x1 + (x2 - x1) * ((t - t0) / max(t1 - t0, 1)), y2 - (y2 - y1) * ((v - lo) / span)]
        c.create_line(pts, fill=ACCENT, width=2, smooth=True)

    def _draw_env(self):
        c = self.ecanvas
        c.delete("all")
        self.env_hits = []
        if self.page != "env" or not self.snap:
            return
        W, H = c.winfo_width(), c.winfo_height()
        if W < 100:
            return
        pad = 10
        env = self._env()
        rrect(c, pad, pad, pad + 84, pad + 38, 10, fill=KEYBG)
        c.create_text(pad + 42, pad + 19, fill=FG, font=self.f_small, text="← Назад")
        self.env_hits.append((pad, pad, pad + 84, pad + 38, lambda: self._show_page(self.prev_page)))
        c.create_text(pad + 100, pad + 10, anchor="w", fill=FG, font=self.f_mid,
                      text=f"Довкілля · вузли до {env['max_hops']} хопів")
        c.create_text(pad + 100, pad + 30, anchor="w", fill=DIM, font=self.f_small,
                      text=f"середнє за вузлами, що передали показники за {env['window'] // 3600} год"
                           + (" · без MQTT" if self.hide_mqtt else ""))
        ms = env["metrics"][:9]
        if not ms:
            c.create_text(W // 2, H // 2, fill=DIM, font=self.f_norm,
                          text="Поки немає даних: жоден вузол у межах 3 хопів не передавав\n"
                               "температуру, вологість чи тиск (вузол із датчиком шле їх раз на ~30 хв).",
                          justify="center")
            return
        cols = 3
        rows = (len(ms) + cols - 1) // cols
        y0, foot = 62, 24
        tw = (W - 2 * pad - 2 * 8) // cols
        th = min(170, (H - y0 - foot - pad - (rows - 1) * 8) // rows)
        for i, mt in enumerate(ms):
            x, y = pad + (i % cols) * (tw + 8), y0 + (i // cols) * (th + 8)
            rrect(c, x, y, x + tw, y + th, 12, fill=CARD, outline=LINE)
            c.create_text(x + 12, y + 14, anchor="w", fill=DIM, font=self.f_cap, text=mt["label"].upper())
            c.create_text(x + 12, y + 40, anchor="w", fill=FG, font=self.f_val, text=self._metric_text(mt))
            rng = f"{mt['min']}…{mt['max']}" if mt["n"] > 1 else "1 вузол"
            c.create_text(x + 12, y + th - 12, anchor="w", fill=DIM, font=self.f_small, text=f"{rng} · {mt['n']} вузл.")
            self._line(c, x + tw * 0.48, y + 26, x + tw - 12, y + th - 24, mt["series"])
        srcs = ", ".join(f"{clean(s['name'])[:10]} ({s['hops']})" for s in env["sources"][:8])
        c.create_text(pad, H - 12, anchor="w", fill=DIM, font=self.f_small, text=f"джерела: {srcs}"[:110])

    def _spark(self, c, x1, y1, x2, y2, series):
        if len(series) < 2:
            c.create_text(x1, (y1 + y2) // 2, anchor="w", fill=DIM, font=self.f_small,
                          text="збираю дані…")
            return
        t0, t1 = series[0][0], series[-1][0]
        top = max(max(v for _, v in series), 1)
        pts = []
        for t, v in series:
            x = x1 + (x2 - x1) * ((t - t0) / max(t1 - t0, 1))
            y = y2 - (y2 - y1) * (v / top)
            pts += [x, y]
        c.create_line(x1, y2, x2, y2, fill=LINE)
        c.create_polygon([x1, y2] + pts + [x2, y2], fill="#16331f", outline="")
        c.create_line(pts, fill=ACCENT, width=2, smooth=True)
        c.create_text(x2, y1 - 2, anchor="e", fill=DIM, font=self.f_small, text=f"макс {top}")

    def _draw_map(self, c, x1, y1, x2, y2, m):
        self._card(c, x1, y1, x2, y2, "КАРТА")
        me = m["me"]
        cx, cy = (x1 + x2) / 2, (y1 + y2) / 2 + 6
        if not me or me["lat"] is None:
            c.create_text(cx, cy, fill=DIM, font=self.f_norm, text="У ноди немає позиції")
            return
        pts = []
        for n in self._pool():
            if n["me"] or n["lat"] is None or not n["heard"] or m["now"] - n["heard"] > 86400:
                continue
            dx, dy = km_between(me["lat"], me["lon"], n["lat"], n["lon"])
            pts.append((n, dx, dy, math.hypot(dx, dy)))
        dists = sorted(p[3] for p in pts)
        base = dists[max(int(len(dists) * 0.9) - 1, 0)] if dists else 5
        idx = next((i for i, v in enumerate(NICE_KM) if v >= max(base, 1)), len(NICE_KM) - 1)
        idx = max(0, min(len(NICE_KM) - 1, idx + self.map_zoom))
        rng = NICE_KM[idx]
        radius = min(x2 - x1, y2 - y1) / 2 - 22
        scale = radius / rng
        for frac in (1 / 3, 2 / 3, 1):
            r = radius * frac
            c.create_oval(cx - r, cy - r, cx + r, cy + r, outline=LINE)
        c.create_text(cx + radius * 0.707 + 4, cy - radius * 0.707, anchor="w", fill=DIM, font=self.f_small,
                      text=f"{rng} км")
        c.create_text(x1 + 14, y2 - 14, anchor="w", fill=DIM, font=self.f_small, text="↑ північ")
        shown = 0
        for n, dx, dy, dist in pts:
            x, y = cx + dx * scale, cy - dy * scale
            if (x - cx) ** 2 + (y - cy) ** 2 > (radius + 8) ** 2:
                continue
            age = m["now"] - n["heard"]
            col = ACCENT if age < 3600 else WARN
            sel = n["id"] == self.sel
            r = 7 if sel else 5
            c.create_oval(x - r, y - r, x + r, y + r, fill=col, outline="white" if sel else "")
            if age < 3600 or sel:
                c.create_text(x + 9, y, anchor="w", fill=FG, font=self.f_small, text=clean(n["name"]))
            self.hits.append(("circle", x, y, 26, n["id"]))
            shown += 1
        for w in mesh_extra.list_waypoints(self.db, m["now"]):
            dx, dy = km_between(me["lat"], me["lon"], w["lat"], w["lon"])
            x, y = cx + dx * scale, cy - dy * scale
            if (x - cx) ** 2 + (y - cy) ** 2 > (radius + 8) ** 2:
                continue
            c.create_polygon(x, y - 7, x + 6, y, x, y + 7, x - 6, y, fill="#e3b341", outline="")
            c.create_text(x + 9, y, anchor="w", fill=WARN, font=self.f_small, text=clean(w["name"] or "точка")[:14])
        c.create_oval(cx - 7, cy - 7, cx + 7, cy + 7, fill=OWN, outline="white", width=2)
        no_pos = sum(1 for n in self._pool()
                     if not n["me"] and n["lat"] is None and n["heard"] and m["now"] - n["heard"] < 86400)
        c.create_text(x1 + 70, y1 + 15, anchor="w", fill=DIM, font=self.f_small,
                      text=f"на карті {shown}" + (f" · без позиції {no_pos}" if no_pos else "")
                      + (" · без MQTT" if self.hide_mqtt else ""))
        for i, (label, d) in enumerate((("－", 1), ("＋", -1))):
            bx2 = x2 - 10 - i * 46
            rrect(c, bx2 - 38, y1 + 8, bx2, y1 + 40, 8, fill=KEYBG)
            c.create_text(bx2 - 19, y1 + 24, fill=FG, font=self.f_bold, text=label)
            self.hits.append(("rect", bx2 - 38, y1 + 8, bx2, y1 + 40, lambda d=d: self._zoom(d)))
        node = next((n for n in self.snap["nodes"] if n["id"] == self.sel), None)
        if node and not node["me"]:
            age = (m["now"] - node["heard"]) if node["heard"] else None
            d = self._dist(node, me)
            by1, by2 = y2 - 70, y2 - 12
            rrect(c, x1 + 12, by1, x2 - 12, by2, 12, fill="#1b2633", outline=LINE)
            c.create_text(x1 + 26, by1 + 18, anchor="w", fill=FG, font=self.f_bold,
                          text=("★ " if node["id"] in self.settings["favorites"] else "") + clean(node["name"]))
            bx = x2 - 22
            for label, fill, fg, cb, w in (
                    ("Написати", ACCENT, "#06210d", lambda i=node["id"]: self._message_node(i), 96),
                    ("Деталі", KEYBG, FG, lambda i=node["id"]: self._open_detail(i), 74)):
                rrect(c, bx - w, by1 + 10, bx, by2 - 10, 10, fill=fill)
                c.create_text(bx - w / 2, (by1 + by2) / 2, fill=fg, font=self.f_small, text=label)
                self.hits.append(("rect", bx - w, by1 + 10, bx, by2 - 10, cb))
                bx -= w + 8
            c.create_text(x1 + 26, by1 + 42, anchor="w", fill=DIM, font=self.f_small,
                          text=" · ".join(self._node_info(node, d, age)))

    @staticmethod
    def _node_info(node, dist, age):
        info = []
        if dist is not None:
            info.append(f"{dist:.1f} км")
        if node["snr"] is not None:
            info.append(f"{node['snr']:.0f} dB")
        if node["hops"] is not None:
            info.append(f"{node['hops']} хоп")
        info.append(ago(age))
        return info

    def _select(self, node_id):
        self.sel = node_id
        self._draw_home()

    def _zoom(self, d):
        self.map_zoom = max(-3, min(4, self.map_zoom + d))
        self._draw_home()

    def _tap_home(self, e):
        for h in reversed(self.hits):
            if h[0] == "rect" and h[1] <= e.x <= h[3] and h[2] <= e.y <= h[4]:
                h[5]()
                return
        best = min((h for h in self.hits if h[0] == "circle"),
                   key=lambda h: (h[1] - e.x) ** 2 + (h[2] - e.y) ** 2, default=None)
        if best and (best[1] - e.x) ** 2 + (best[2] - e.y) ** 2 < best[3] ** 2:
            self._select(best[4])
        elif self.sel:
            self._select(None)

    # ---- network page: hop rings, links, roles ---------------------------------------------------------
    def _build_net(self, page):
        self.ncanvas = tk.Canvas(page, bg=BG, highlightthickness=0, width=1, height=1)
        self.ncanvas.pack(fill="both", expand=True)
        self.ncanvas.bind("<ButtonRelease-1>", self._tap_net)
        self.ncanvas.bind("<Configure>", lambda e: self._draw_net())

    def _tap_net(self, e):
        for h in reversed(self.net_hits):
            if h[0] == "rect" and h[1] <= e.x <= h[3] and h[2] <= e.y <= h[4]:
                h[5]()
                return
        best = min((h for h in self.net_hits if h[0] == "circle"),
                   key=lambda h: (h[1] - e.x) ** 2 + (h[2] - e.y) ** 2, default=None)
        if best and (best[1] - e.x) ** 2 + (best[2] - e.y) ** 2 < best[3] ** 2:
            self.net_sel = best[4]
            self._draw_net()
        elif self.net_sel:
            self.net_sel = None
            self._draw_net()

    def _net_step_day(self, d):
        self.links.flush(self.db, force=True)
        days = sorted(set(mesh_extra.link_days(self.db, 8)) | {mesh_extra.day_of(time.time())})
        cur = self.net_day or days[-1]
        i = days.index(cur) if cur in days else len(days) - 1
        i = max(0, min(len(days) - 1, i + d))
        self.net_day = None if i == len(days) - 1 else days[i]
        self.net_sel = None
        self._draw_net()

    def _net_toggle(self, flow):
        self.net_flows[flow] = not self.net_flows[flow]
        self._draw_net()

    def _net_zoom_by(self, d):
        self.net_zoom = max(-2, min(4, self.net_zoom + d))
        self._draw_net()

    @staticmethod
    def _shape(c, shape, x, y, r, fill, outline="", width=1):
        """Marker by node role: circle client, hollow circle muted client, square router, triangle repeater,
        diamond tracker, hexagon sensor, cross lost-and-found."""
        if shape in ("circle", "circle_open", "circle_dot"):
            if shape == "circle_open":
                c.create_oval(x - r, y - r, x + r, y + r, fill=BG, outline=fill, width=2)
            else:
                c.create_oval(x - r, y - r, x + r, y + r, fill=fill, outline=outline, width=width)
                if shape == "circle_dot":
                    c.create_oval(x - 2, y - 2, x + 2, y + 2, fill=BG, outline="")
        elif shape in ("square", "square_open"):
            if shape == "square_open":
                c.create_rectangle(x - r, y - r, x + r, y + r, fill=BG, outline=fill, width=2)
            else:
                c.create_rectangle(x - r, y - r, x + r, y + r, fill=fill, outline=outline, width=width)
        elif shape == "triangle":
            c.create_polygon(x, y - r - 1, x + r + 1, y + r, x - r - 1, y + r, fill=fill, outline=outline, width=width)
        elif shape == "diamond":
            c.create_polygon(x, y - r - 2, x + r + 2, y, x, y + r + 2, x - r - 2, y, fill=fill, outline=outline, width=width)
        elif shape == "hex":
            pts = []
            for k in range(6):
                a = math.pi / 3 * k
                pts += [x + (r + 1) * math.cos(a), y + (r + 1) * math.sin(a)]
            c.create_polygon(pts, fill=fill, outline=outline, width=width)
        else:
            c.create_line(x - r, y, x + r, y, fill=fill, width=4)
            c.create_line(x, y - r, x, y + r, fill=fill, width=4)

    def _node_names(self):
        return {n["id"]: n["name"] for n in self.snap["nodes"] if n["id"]}

    def _trace_text(self, dest):
        tr, names = self.traces.get(dest), self._node_names()
        if not tr:
            return None
        nm = lambda i: clean(names.get(i, i[-4:]))  # noqa: E731
        fwd = " → ".join(nm(i) for i in tr["towards"])
        back = " → ".join(nm(i) for i in tr["back"])
        return f"туди: {fwd}" + ("" if tr["towards"][::-1] == tr["back"] else f"  ·  назад: {back}")

    def _draw_net(self):
        c = self.ncanvas
        c.delete("all")
        self.net_hits = []
        if self.page != "net":
            return
        W, H = c.winfo_width(), c.winfo_height()
        if W < 100 or H < 100:
            return
        if not self.snap or not self._me():
            c.create_text(W // 2, H // 2, fill=DIM, font=self.f_norm, text="Підключення до ноди…")
            return
        me, now = self._me(), time.time()
        self.links.flush(self.db, force=True)
        day = self.net_day or mesh_extra.day_of(now)
        today = self.net_day is None
        links = mesh_extra.load_links(self.db, day)
        in_links = {x for l in links for x in (l["src"], l["dst"])}
        shown = [n for n in self._pool() if not n["me"] and n["id"]
                 and ((n["heard"] and now - n["heard"] < 86400) if today else n["id"] in in_links)]

        # top strip: day, directions, MQTT, zoom
        def chip(x1, y1, x2, y2, label, fill, fg, cb):
            rrect(c, x1, y1, x2, y2, 8, fill=fill)
            c.create_text((x1 + x2) / 2, (y1 + y2) / 2, fill=fg, font=self.f_small, text=label)
            self.net_hits.append(("rect", x1, y1, x2, y2, cb))

        pad, top = 10, 40
        chip(pad, 6, pad + 34, 34, "◀", KEYBG, FG, lambda: self._net_step_day(-1))
        c.create_text(pad + 76, 20, fill=FG, font=self.f_bold, text="сьогодні" if today else day)
        chip(pad + 118, 6, pad + 152, 34, "▶", KEYBG if not today else PANEL, FG if not today else DIM,
             lambda: self._net_step_day(1))
        x = pad + 168
        for flow, label in (("up", "↑ вгору"), ("down", "↓ вниз")):
            on = self.net_flows[flow]
            chip(x, 6, x + 78, 34, label, HOP_COLORS[flow] if on else KEYBG, "#101010" if on else DIM,
                 lambda f=flow: self._net_toggle(f))
            x += 84
        chip(x, 6, x + 118, 34, "MQTT: сховано" if self.hide_mqtt else "MQTT: показано", ACCENT if self.hide_mqtt else KEYBG,
             "#06210d" if self.hide_mqtt else FG, self._toggle_mqtt)
        for i, (label, d) in enumerate((("－", -1), ("＋", 1))):
            chip(W - pad - 34 - i * 40, 6, W - pad - i * 40, 34, label, KEYBG, FG, lambda d=d: self._net_zoom_by(d))

        # rings by hops
        known = [n["hops"] for n in shown if n["hops"] is not None]
        kk = max(1, min(max(known, default=1), 5))
        extra = any(n["hops"] is None or n["hops"] > 5 for n in shown)
        kr = kk + 1 if extra else kk
        cx, cy = W / 2, top + (H - top) / 2
        ry = (H - top) / 2 - 16
        ry *= 1.0 + 0.2 * self.net_zoom
        ax = min(1.9, (W / 2 - 60) / max(ry / (1.0 + 0.2 * self.net_zoom), 1))
        radii = [0] + [ry * ((0.34 + 0.66 * (k - 1) / (kr - 1)) if kr > 1 else 0.62) for k in range(1, kr + 1)]
        for k in range(1, kr + 1):
            r = radii[k]
            c.create_oval(cx - r * ax, cy - r, cx + r * ax, cy + r, outline=LINE)
            c.create_text(cx + r * ax * 0.707 + 3, cy - r * 0.707 - 1, anchor="w", fill=DIM, font=self.f_small,
                          text=("?" if extra and k == kr else f"{k} хоп"))
        lay = mesh_extra.ring_layout(me, [{"id": n["id"], "hops": n["hops"], "lat": n["lat"], "lon": n["lon"]} for n in shown],
                                     radii, min_px=34, max_ring=kr)
        pos = {me["id"]: (cx, cy)}
        for nid, (_k, ang, r) in lay.items():
            pos[nid] = (cx + r * ax * math.cos(ang), cy - r * math.sin(ang))

        # links: one thin arrow per direction; with a node selected only its links stay bright
        sel = self.net_sel if self.net_sel in pos else None
        for l in links:
            if not self.net_flows.get(l["flow"], True) or l["src"] not in pos or l["dst"] not in pos:
                continue
            (x1, y1), (x2, y2) = pos[l["src"]], pos[l["dst"]]
            dx, dy = x2 - x1, y2 - y1
            ln = math.hypot(dx, dy)
            if ln < 22:
                continue
            ux, uy = dx / ln, dy / ln
            off = 2.5 if l["flow"] == "up" else -2.5
            sx, sy = x1 + ux * 10 - uy * off, y1 + uy * 10 + ux * off
            ex, ey = x2 - ux * 10 - uy * off, y2 - uy * 10 + ux * off
            hot = sel is None or sel in (l["src"], l["dst"])
            col = HOP_COLORS[l["flow"]] if hot else ("#3a2326" if l["flow"] == "up" else "#1d3324")
            c.create_line(sx, sy, ex, ey, fill=col, width=(2 if hot and l["n"] > 2 else 1), arrow="last",
                          arrowshape=(8, 9, 3), dash=(4, 3) if l["kind"] in ("via", "relay") else ())

        # nodes
        roles = {}
        many = len(shown) > 24
        for n in shown:
            if n["id"] not in pos:
                continue
            x, y = pos[n["id"]]
            age = now - n["heard"] if n["heard"] else 1e9
            col = ACCENT if age < 3600 else (WARN if age < 86400 else DIM)
            shape, label = mesh_extra.role_info(n.get("role"))
            roles[shape] = label
            on = n["id"] == sel
            self._shape(c, shape, x, y, 7 if on else 5, col, "white" if on else "", 2 if on else 1)
            if on or not many or age < 3600:
                c.create_text(x + 9, y, anchor="w", fill=FG if on or age < 3600 else DIM, font=self.f_small,
                              text=clean(n["name"])[:9])
            self.net_hits.append(("circle", x, y, 24, n["id"]))
        c.create_oval(cx - 8, cy - 8, cx + 8, cy + 8, fill=OWN, outline="white", width=2)
        c.create_text(cx, cy + 20, fill=FG, font=self.f_small, text=clean(me["name"]))

        # legend: only the role shapes present on the map
        ly = H - 12
        lx = 12
        for shape, label in sorted(roles.items(), key=lambda kv: kv[1]):
            if ly < top + 60:
                break
            self._shape(c, shape, lx + 5, ly, 4, DIM)
            c.create_text(lx + 16, ly, anchor="w", fill=DIM, font=self.f_small, text=label)
            ly -= 16
        if not sel:
            c.create_text(W - 12, H - 10, anchor="e", fill=DIM, font=self.f_small,
                          text=f"вузлів {len(shown)} · зв'язків {len(links)}" + (" · пунктир: через ретранслятор" if links else ""))
        if self.tracing:
            c.create_text(W // 2, top + 8, fill=WARN, font=self.f_small, text=f"шукаю маршрут до {clean(self._node_names().get(self.tracing, ''))}…")

        node = next((n for n in shown if n["id"] == sel), None)
        if node:
            by2 = H - 8
            by1 = by2 - 62
            rrect(c, 196, by1, W - 12, by2, 12, fill="#1b2633", outline=LINE)
            age = (now - node["heard"]) if node["heard"] else None
            c.create_text(210, by1 + 16, anchor="w", fill=FG, font=self.f_bold,
                          text=("★ " if node["id"] in self.settings["favorites"] else "") + clean(node["name"])
                          + "   " + " · ".join(self._node_info(node, self._dist(node, me), age)))
            tt = self._trace_text(node["id"])
            c.create_text(210, by1 + 42, anchor="w", fill=DIM if tt else "#6b7785", font=self.f_small,
                          text=clean(tt)[:78] if tt else "маршрут ще не перевірявся: натисніть «Маршрут»")
            bx = W - 22
            for label, fill, fg, cb, w in (
                    ("Написати", ACCENT, "#06210d", lambda i=node["id"]: self._message_node(i), 90),
                    ("Деталі", KEYBG, FG, lambda i=node["id"]: self._open_detail(i), 70),
                    ("Маршрут", KEYBG, WARN, lambda i=node["id"]: self._trace_route(i), 84)):
                rrect(c, bx - w, by1 + 8, bx, by1 + 34, 9, fill=fill)
                c.create_text(bx - w / 2, by1 + 21, fill=fg, font=self.f_small, text=label)
                self.net_hits.append(("rect", bx - w, by1 + 8, bx, by1 + 34, cb))
                bx -= w + 6

    # ---- favourites / alerts / events -----------------------------------
    def _toggle_fav(self, node_id):
        favs = self.settings["favorites"]
        if node_id in favs:
            favs.remove(node_id)
        else:
            favs.append(node_id)
        self.fav_state.pop(node_id, None)
        save_settings(self.settings)
        self._check_alerts()
        self._redraw_current()

    def _redraw_current(self):
        if self.page == "home":
            self._draw_home()
        elif self.page == "node":
            self._draw_detail()
        elif self.page == "net":
            self._draw_net()
        elif self.page == "nodes" and self.snap:
            self._render_nodes()

    def _event(self, kind, node_id, text):
        now = time.time()
        try:
            self.db.execute("INSERT INTO events(t,kind,node_id,text) VALUES (?,?,?,?)", (now, kind, node_id, text))
            self.db.commit()
        except sqlite3.Error:
            pass
        self._insert_event_row(now, kind, text)
        self._notify("dm" if kind in ("fav_off", "link_lost") else "msg")

    def _insert_event_row(self, t, kind, text):
        icon = {"fav_off": "⚠", "link_lost": "⚠", "fav_on": "●", "link_ok": "●", "new": "✚"}.get(kind, "•")
        self.ev_tree.insert("", 0, values=(time.strftime("%d.%m %H:%M", time.localtime(t)),
                                           f"{icon}  {clean(text)}"), tags=(kind,))

    def _load_events(self):
        try:
            rows = self.db.execute("SELECT t,kind,text FROM events ORDER BY id DESC LIMIT 200").fetchall()
        except sqlite3.Error:
            return
        for t, kind, text in rows[::-1]:
            self._insert_event_row(t, kind, text)

    def _check_alerts(self):
        if not self.snap:
            return
        now = time.time()
        by_id = {n["id"]: n for n in self.snap["nodes"] if n["id"]}
        known = set(self.settings["known"])
        if not self.settings["init"]:
            self.settings["known"] = sorted(by_id)
            self.settings["init"] = True
            save_settings(self.settings)
        else:
            fresh = [n for i, n in by_id.items() if i not in known and not n["me"]]
            for n in fresh:
                known.add(n["id"])
                if self.hide_mqtt and n.get("mqtt"):
                    continue          # known only through the MQTT bridge: remember, do not alert
                self._event("new", n["id"], f"Новий вузол {n['name']}" +
                            (f" ({n['long']})" if n["long"] and n["long"] != n["name"] else ""))
            if fresh:
                self.settings["known"] = sorted(known)
                save_settings(self.settings)
        self.alerts = []
        for fid in self.settings["favorites"]:
            n = by_id.get(fid)
            if not n:
                continue
            age = (now - n["heard"]) if n["heard"] else None
            offline = age is None or age > FAV_OFFLINE_S
            prev = self.fav_state.get(fid)
            if prev is None:
                self.fav_state[fid] = "off" if offline else "on"
            elif prev == "on" and offline:
                self.fav_state[fid] = "off"
                self._event("fav_off", fid, f"★ {n['name']} не на зв'язку ({ago(age)})")
            elif prev == "off" and not offline:
                self.fav_state[fid] = "on"
                self._event("fav_on", fid, f"★ {n['name']} знову на зв'язку")
            if offline:
                self.alerts.append((fid, f"{n['name']} ({ago(age)})"))
        self._update_banner()

    def _update_banner(self):
        text = ""
        if self.link_alert:
            text = "⚠  Немає зв'язку з нодою"
        elif self.alerts:
            text = "⚠  Не на зв'язку: " + ", ".join(clean(t) for _, t in self.alerts[:3]) + \
                   (f" і ще {len(self.alerts) - 3}" if len(self.alerts) > 3 else "")
        if text:
            self.banner.config(text="  " + text)
            if not self.banner.winfo_ismapped():
                self.banner.pack(fill="x", ipady=5)
        elif self.banner.winfo_ismapped():
            self.banner.pack_forget()

    def _tap_banner(self):
        if self.alerts and not self.link_alert:
            self._open_detail(self.alerts[0][0])

    def _check_link(self):
        """Raise/clear the 'no link to node' alert based on how long we have been offline."""
        if self.link_ok:
            self.link_down_since = None
            if self.link_alert:
                self.link_alert = False
                self._event("link_ok", None, "Зв'язок із нодою відновлено")
        else:
            if self.link_down_since is None:
                self.link_down_since = time.time()
            elif not self.link_alert and time.time() - self.link_down_since > LINK_LOST_S:
                self.link_alert = True
                self._event("link_lost", None, "Втрачено зв'язок із нодою")
        self._update_banner()

    # ---- node details ---------------------------------------------------
    def _open_detail(self, node_id):
        if self.page != "node":
            self.prev_page = self.page
        self.detail_id = node_id
        mine = next((n for n in (self.snap or {"nodes": []})["nodes"] if n["id"] == node_id), None)
        self.detail_metric = "radio" if mine and mine["me"] else "snr"
        self._show_page("node")

    def _tap_detail(self, e):
        for h in reversed(self.detail_hits):
            if h[0] <= e.x <= h[2] and h[1] <= e.y <= h[3]:
                h[4]()
                return

    def _fmt_uptime(self, s):
        if s is None:
            return "—"
        d, rem = divmod(int(s), 86400)
        h, m = divmod(rem, 3600)
        return f"{d} д {h} г" if d else f"{h} г {m // 60} хв"

    def _draw_detail(self):
        c = self.dcanvas
        c.delete("all")
        self.detail_hits = []
        if self.page != "node" or not self.snap:
            return
        W, H = c.winfo_width(), c.winfo_height()
        node = next((n for n in self.snap["nodes"] if n["id"] == self.detail_id), None)
        if W < 100 or not node:
            c.create_text(W // 2, H // 2, fill=DIM, font=self.f_norm, text="Вузол не знайдено")
            return
        now = time.time()
        me = self._me()
        age = (now - node["heard"]) if node["heard"] else None
        pad = 10

        def button(x1, y1, x2, y2, label, fill, fg, cb):
            rrect(c, x1, y1, x2, y2, 10, fill=fill)
            c.create_text((x1 + x2) / 2, (y1 + y2) / 2, fill=fg, font=self.f_small, text=label)
            self.detail_hits.append((x1, y1, x2, y2, cb))

        button(pad, pad, pad + 84, pad + 38, "← Назад", KEYBG, FG, lambda: self._show_page(self.prev_page))
        fav = node["id"] in self.settings["favorites"]
        if not node["me"]:
            button(W - pad - 100, pad, W - pad, pad + 38, "Написати", ACCENT, "#06210d",
                   lambda: self._message_node(node["id"]))
            button(W - pad - 100 - 8 - 120, pad, W - pad - 108, pad + 38,
                   "★ в улюблених" if fav else "☆ в улюблені", WARN if fav else KEYBG,
                   "#241a00" if fav else FG, lambda: self._toggle_fav(node["id"]))
        c.create_text(pad + 100, pad + 10, anchor="w", fill=FG, font=self.f_mid, text=clean(node["name"]))
        sub = clean(node["long"]) + ("  ·  " if node["long"] else "") + str(node["id"] or "") + \
              (f"  ·  {node['hw']}" if node["hw"] else "") + ("  ·  через MQTT" if node.get("mqtt") else "")
        c.create_text(pad + 100, pad + 30, anchor="w", fill=DIM, font=self.f_small, text=sub[:70])
        info = "  ·  ".join(self._node_info(node, self._dist(node, me), age))
        c.create_text(pad, 62, anchor="w", fill=FG, font=self.f_norm, text=info)

        dev = node["dev"]
        batt = dev.get("batteryLevel")
        tiles = [
            ("БАТАРЕЯ", ("живлення" if batt and batt > 100 else f"{batt}%") if batt is not None else "—"),
            ("НАПРУГА", f"{dev['voltage']:.2f} В" if dev.get("voltage") else "—"),
            ("ЕФІР · КАНАЛ", f"{dev['channelUtilization']:.0f}%" if dev.get("channelUtilization") is not None else "—"),
            ("ЕФІР · TX", f"{dev['airUtilTx']:.1f}%" if dev.get("airUtilTx") is not None else "—"),
            ("АПТАЙМ", self._fmt_uptime(dev.get("uptimeSeconds"))),
            ("ДОВКІЛЛЯ", mesh_env.fmt(self._node_env(node), limit=3) or "—"),
        ]
        cols, tw, th = 3, (W - 2 * pad - 2 * 8) // 3, 54
        for i, (title, val) in enumerate(tiles):
            x = pad + (i % cols) * (tw + 8)
            y = 80 + (i // cols) * (th + 8)
            rrect(c, x, y, x + tw, y + th, 12, fill=CARD, outline=LINE)
            c.create_text(x + 12, y + 14, anchor="w", fill=DIM, font=self.f_cap, text=title)
            c.create_text(x + 12, y + 37, anchor="w", fill=FG, font=self.f_bold, text=val)

        cy1 = 80 + 2 * (th + 8)
        rrect(c, pad, cy1, W - pad, H - pad, 14, fill=CARD, outline=LINE)
        chips = ([("radio", "Радіо")] if node["me"] else []) + [("snr", "SNR")] + \
                [(k, mesh_extra.TELE_KEYS[key][1].replace("Ефір · ", "Ефір ")) for key in mesh_extra.TELE_KEYS
                 for k in [mesh_extra.TELE_KEYS[key][0]]]
        metric = self.detail_metric if self.detail_metric in dict(chips) else chips[0][0]
        x = pad + 12
        for key, label in chips:
            w = 14 + 8 * len(label)
            rrect(c, x, cy1 + 8, x + w, cy1 + 34, 8, fill=ACCENT if key == metric else KEYBG)
            c.create_text(x + w / 2, cy1 + 21, fill="#06210d" if key == metric else FG, font=self.f_small, text=label)
            self.detail_hits.append((x, cy1 + 8, x + w, cy1 + 34, lambda k=key: self._set_metric(k)))
            x += w + 6
        if metric == "radio":
            self._draw_radio(c, pad + 14, cy1 + 42, W - pad - 14, H - pad - 8)
            return
        unit = "dB" if metric == "snr" else next(v[2] for v in mesh_extra.TELE_KEYS.values() if v[0] == metric)
        if metric == "snr":
            try:
                rows = self.db.execute("SELECT t,snr FROM heard WHERE node_id=? AND t>? AND snr IS NOT NULL ORDER BY t",
                                       (node["id"], now - 86400)).fetchall()
            except sqlite3.Error:
                rows = []
        else:
            try:
                rows = mesh_extra.tele_series(self.db, node["id"], metric, now - 86400)
            except sqlite3.Error:
                rows = []
        x1, x2, y1, y2 = pad + 54, W - pad - 16, cy1 + 48, H - pad - 22
        if len(rows) < 2:
            c.create_text((x1 + x2) / 2, (y1 + y2) / 2, fill=DIM, font=self.f_small, justify="center",
                          text="поки мало даних: історія збирається з моменту запуску екрана,\n"
                               "а вузли надсилають телеметрію раз на пів години")
            return
        lo, hi = min(v for _, v in rows), max(v for _, v in rows)
        margin = max((hi - lo) * 0.1, 1.0 if metric == "snr" else 0.02 if metric == "voltage" else 1.0)
        lo, hi = lo - margin, hi + margin
        if metric in ("battery", "chutil", "airtx"):
            lo = max(lo, 0)
        t0 = rows[0][0]
        span = max(now - t0, 1)
        pts = []
        for t, v in rows:
            pts += [x1 + (x2 - x1) * ((t - t0) / span), y2 - (y2 - y1) * ((v - lo) / (hi - lo))]
        fmt = (lambda v: f"{v:.2f}") if metric == "voltage" else (lambda v: f"{v:.0f}")
        c.create_line(x1, y2, x2, y2, fill=LINE)
        c.create_line(pts, fill=ACCENT, width=2)
        for i in range(0, len(pts), 2):
            c.create_oval(pts[i] - 2, pts[i + 1] - 2, pts[i] + 2, pts[i + 1] + 2, fill=ACCENT, outline="")
        c.create_text(x1 - 6, y1, anchor="e", fill=DIM, font=self.f_small, text=fmt(hi))
        c.create_text(x1 - 6, y2, anchor="e", fill=DIM, font=self.f_small, text=fmt(lo))
        avg = sum(v for _, v in rows) / len(rows)
        c.create_text(x2, cy1 + 21, anchor="e", fill=DIM, font=self.f_small,
                      text=f"24 год · середнє {fmt(avg)} {unit} · зразків {len(rows)}")
        c.create_text(x1, y2 + 12, anchor="w", fill=DIM, font=self.f_small, text=ago(now - t0) + " тому")
        c.create_text(x2, y2 + 12, anchor="e", fill=DIM, font=self.f_small, text="зараз")

    def _set_metric(self, key):
        self.detail_metric = key
        self._draw_detail()

    def _draw_radio(self, c, x1, y1, x2, y2):
        """Power at the chip output, as the firmware really set it (read only: nothing here changes a setting)."""
        info = self.radio_info
        if info is None:
            c.create_text((x1 + x2) / 2, (y1 + y2) / 2, fill=DIM, font=self.f_small, text="читаю журнал meshtasticd…")
            return
        setting = (self.snap or {}).get("tx")
        eff, estimate = mesh_extra.effective_power(info, setting)
        w = (x2 - x1 - 16) / 3
        cells = [
            ("ФАКТИЧНО НА ВИХОДІ ЧИПА", ("≈ " if estimate else "") + f"{eff} дБм" if eff is not None else "—", OWN),
            ("ПІСЛЯ ЛІМІТУ РЕГІОНУ", f"{info['logged']} дБм" if info.get("logged") is not None else "—", FG),
            ("СТЕЛЯ ЧИПА", f"{info['cap']} дБм" if info.get("cap") is not None else "—", FG),
        ]
        for i, (title, val, col) in enumerate(cells):
            x = x1 + i * (w + 8)
            rrect(c, x, y1, x + w, y1 + 62, 10, fill=PANEL, outline=LINE)
            c.create_text(x + 10, y1 + 13, anchor="w", fill=DIM, font=self.f_cap, text=title)
            c.create_text(x + 10, y1 + 41, anchor="w", fill=col, font=self.f_val, text=val)
        notes = [f"Налаштування вузла (tx_power): {setting} дБм" if setting else "У налаштуваннях вузла потужність за замовчуванням (0)"]
        if info.get("radio"):
            notes.append(f"стеля задана в {info['radio']}")
        if not info.get("access"):
            notes.append("немає доступу до журналу meshtasticd: додайте користувача до групи systemd-journal")
        elif estimate:
            notes.append("у журналі вже немає рядка запуску радіо, тому показано налаштування вузла (оцінка)")
        elif info.get("cached"):
            notes.append("значення з пам'яті екрана: журнал уже стер рядок запуску, чинне до перезапуску meshtasticd")
        for i, text in enumerate(notes[:4]):
            c.create_text(x1 + 4, y1 + 78 + i * 16, anchor="w", fill=DIM, font=self.f_small, text=clean(text)[:100])

    # ---- keyboard / input -----------------------------------------------
    def _focus_entry(self, entry):
        self.active_entry = entry
        entry.focus_set()
        self._show_kb(True)

    def _show_kb(self, show: bool):
        if show and not self.kb_visible:
            self.kb.grid()
        elif not show and self.kb_visible:
            self.kb.grid_remove()
        self.kb_visible = show
        if show:
            for t in (self.chat, self.dm_text):
                t.see("end")

    def _type(self, ch):
        if not self.active_entry:
            return
        self.active_entry.insert("insert", ch)
        if self.caps:
            self.caps = False
            self._render_kb()

    def _backspace(self):
        e = self.active_entry
        if e:
            pos = int(e.index("insert"))
            if pos:
                e.delete(pos - 1, pos)

    def _toggle_caps(self):
        self.caps = not self.caps
        self._render_kb()

    def _toggle_layout(self):
        self.layout = "en" if self.layout == "ua" else "ua"
        self._render_kb()

    def _send(self, text, peer):
        """peer: node id for a private message, None for the channel."""
        entry = self.dm_entry if peer else self.chat_entry
        quick = text is not None
        text = (text if quick else entry.var.get()).strip()
        if not text:
            return
        if len(text.encode("utf-8")) > 200:
            self._status("повідомлення задовге (макс. 200 байт)", False)
            return
        name = self.peers.get(peer, {}).get("name") if peer else None
        pid = self.link.send(text, peer)
        if pid is not None:
            if not quick:
                entry.var.set("")
            self.db.execute(
                "INSERT INTO messages(pkt_id,t,from_name,to_id,to_name,text,dm,own,ack) VALUES (?,?,?,?,?,?,?,1,?)",
                (pid or None, time.time(), "я", peer or "^all", name, text, int(bool(peer)), "pending" if pid else None))
            self.db.commit()
            self._poll_db()
            self._status("", True)
        else:
            self._status("не надіслано: немає зв'язку з нодою", False)

    # ---- delivery status ------------------------------------------------
    ACK_NOTE = {"NO_ROUTE": "немає маршруту", "MAX_RETRANSMIT": "ніхто не підтвердив", "TIMEOUT": "час вийшов",
                "NO_CHANNEL": "немає каналу", "PKI_UNKNOWN_PUBKEY": "невідомий ключ вузла", "NO_RESPONSE": "немає відповіді"}

    def _ack_mark(self, t, m):
        """Delivery mark after the text of our own message; painted again when the answer comes."""
        if m.get("own") and m.get("pkt") and m.get("ack"):
            t.insert("end", mesh_extra.ACK_MARK.get(m["ack"], ""), (f"ack{m['pkt']}", f"ack_{m['ack']}"))

    def _on_ack(self, pid, resp, dest_num):
        state, why = mesh_extra.delivery(resp, dest_num)
        m = self.by_pkt.get(pid)
        if not pid or not m:
            return
        if m.get("ack") == "delivered":
            return
        m["ack"] = state
        try:
            self.db.execute("UPDATE messages SET ack=? WHERE pkt_id=?", (state, pid))
            self.db.commit()
        except sqlite3.Error:
            pass
        for widget in (self.chat, self.dm_text):
            tag = f"ack{pid}"
            r = widget.tag_ranges(tag)
            if r:
                widget.config(state="normal")
                widget.delete(r[0], r[1])
                widget.insert(r[0], mesh_extra.ACK_MARK[state], (tag, f"ack_{state}"))
                widget.config(state="disabled")
        if state == "failed":
            self._hint_bar("не доставлено: " + self.ACK_NOTE.get(why, why.lower()))

    def _hint_bar(self, text):
        """A short note in the header that does not turn the connection dot red."""
        self.lbl_status.config(text=text, fg=WARN)

    def _status(self, text, ok):
        self.status_ok = ok
        self.dot.config(fg=ACCENT if ok else ERR)
        self.lbl_status.config(text=text, fg=DIM if ok else ERR)

    # ---- private conversations ------------------------------------------
    def _open_peer(self, peer_id, name=None):
        self.cur_peer = peer_id
        if peer_id and peer_id not in self.peers:
            self.peers[peer_id] = {"name": name or peer_id, "t": 0, "text": ""}
        if peer_id:
            self.unread_peer[peer_id] = 0
            self.dm_head.config(text="  ✉  " + clean(self.peers[peer_id]["name"]))
        else:
            self.dm_head.config(text="  Оберіть розмову або торкніться вузла на вкладці «Огляд» чи «Вузли»")
        self._render_thread()
        self._render_peers()
        self._update_tabs()

    def _render_thread(self):
        t = self.dm_text
        t.config(state="normal")
        t.delete("1.0", "end")
        if self.cur_peer and not self.threads[self.cur_peer]:
            t.insert("end", "Повідомлень поки немає.\n", "hint")
        for m in (self.threads[self.cur_peer] if self.cur_peer else []):
            self._insert_dm(t, m)
        t.config(state="disabled")
        t.see("end")

    def _insert_dm(self, t, m):
        hm = time.strftime("%H:%M", time.localtime(m["t"]))
        if m["own"]:
            t.insert("end", f"{hm}\n", "mine_head")
            t.insert("end", m["text"], "mine")
            self._ack_mark(t, m)
            t.insert("end", "\n", "mine")
        else:
            t.insert("end", f"{hm} ", "time")
            t.insert("end", m["from"] + "\n", "name")
            t.insert("end", m["text"] + "\n", "theirs")

    def _render_peers(self):
        tree = self.peer_tree
        first = tree.yview()[0]
        tree.delete(*tree.get_children())
        for pid, p in sorted(self.peers.items(), key=lambda kv: -kv[1]["t"]):
            n = self.unread_peer.get(pid, 0)
            label = clean(p["name"]) + (f"   ●{n}" if n else "")
            tree.insert("", "end", iid=pid, values=(label,), tags=("unread",) if n else ())
        tree.yview_moveto(first)

    def _tap_peer(self, e):
        iid = self.peer_tree.identify_row(e.y)
        if iid:
            self._open_peer(iid)

    # ---- nodes list -----------------------------------------------------
    def _cycle_filter(self):
        self.filter_i = (self.filter_i + 1) % len(FILTERS)
        self._update_filter_label()
        if self.snap:
            self._render_nodes()

    def _update_filter_label(self):
        label, limit = FILTERS[self.filter_i]
        self.btn_filter.config(text=f"за {label}" if limit else "усі вузли")

    def _update_mqtt_label(self):
        self.btn_mqtt.config(text="MQTT: сховано" if self.hide_mqtt else "MQTT: показано",
                             bg=ACCENT if self.hide_mqtt else KEYBG, fg="#06210d" if self.hide_mqtt else FG,
                             activebackground=ACCENT if self.hide_mqtt else "#2a3644")

    def _toggle_mqtt(self):
        self.hide_mqtt = not self.hide_mqtt
        self.settings["hide_mqtt"] = self.hide_mqtt
        save_settings(self.settings)
        self._update_mqtt_label()
        if self.snap:
            self._render_nodes()

    def _merge_heard(self, snap):
        """A fresh connection gets stale lastHeard values from the node (it only learns live ones from new packets).
        The logger records every heard packet, so take the later of the two: counts stay right after a restart."""
        try:
            last = dict(self.db.execute("SELECT node_id, MAX(t) FROM heard WHERE t > ? GROUP BY node_id",
                                        (time.time() - 86400,)).fetchall())
        except sqlite3.Error:
            return
        for n in snap["nodes"]:
            t = last.get(n["id"])
            if t and t > (n["heard"] or 0):
                n["heard"] = int(t)

    def _pool(self):
        """Nodes after the MQTT filter (nodes known only through the MQTT bridge); own node and favourites stay."""
        nodes = self.snap["nodes"]
        if not self.hide_mqtt:
            return nodes
        favs = self.settings["favorites"]
        return [n for n in nodes if not n.get("mqtt") or n["me"] or n["id"] in favs]

    def _node_env(self, n, latest=None):
        """{metric id: value} for a node: the readings stored in the DB, else what the library remembers."""
        latest = latest if latest is not None else mesh_env.latest_per_node(self.db)
        if n["id"] in latest:
            return latest[n["id"]][0]
        vals = {}
        for block, table in ((n.get("env"), mesh_env.METRICS), ):
            for key, (mid, _l, _u, (lo, hi)) in table.items():
                v = (block or {}).get(key)
                if isinstance(v, (int, float)) and not isinstance(v, bool) and lo <= v <= hi:
                    vals[mid] = round(float(v), mesh_env.DIGITS.get(mid, 1))
        return vals

    def _env(self):
        """Average environment readings of nodes within 3 hops, honouring the MQTT filter."""
        nodes = self.snap["nodes"]
        names = {n["id"]: n["name"] for n in nodes if n["id"]}
        hops = {n["id"]: (0 if n["me"] else n["hops"]) for n in nodes if n["id"] and (n["me"] or n["hops"] is not None)}
        keep = {n["id"] for n in self._pool()}
        excl = {n["id"] for n in nodes if n["id"] and n["id"] not in keep}
        try:
            return mesh_env.summary(self.db, hops, names, exclude=excl)
        except sqlite3.Error:
            return {"max_hops": mesh_env.MAX_HOPS, "window": mesh_env.WINDOW, "metrics": [], "sources": [],
                    "now": int(time.time())}

    def _visible_nodes(self):
        limit = FILTERS[self.filter_i][1]
        now = time.time()
        nodes = [n for n in self._pool()
                 if n["me"] or limit is None or (n["heard"] and now - n["heard"] < limit)]
        return sorted(nodes, key=lambda n: (not n["me"], -n["heard"]))

    def _render_nodes(self):
        snap = self.snap
        self.lbl_title.config(text=clean(snap["me"] or "Mesh"))
        self.lbl_status.config(text=clean(snap["chan"]) if self.status_ok else self.lbl_status.cget("text"))
        nodes = self._visible_nodes()
        me = self._me()
        if self.nodes_view == "nodes":
            self.lbl_count.config(text=f"показано {len(nodes)} з {len(snap['nodes'])}")
        first = self.tree.yview()[0]
        self.tree.delete(*self.tree.get_children())
        now = time.time()
        favs = self.settings["favorites"]
        latest = mesh_env.latest_per_node(self.db)
        for i, n in enumerate(nodes):
            age = now - n["heard"] if n["heard"] else None
            name = ("★ " if n["id"] in favs else "") + clean(n["name"]) + \
                   (f"   {clean(n['long'])}" if n["long"] and n["long"] != n["name"] else "")
            tags = ("me",) if n["me"] else (("old",) if age is None or age > 3600 else ())
            d = self._dist(n, me)
            self.tree.insert("", "end", iid=n["id"] or f"x{i}", tags=tags, values=(
                name + ("  (я)" if n["me"] else ""),
                mesh_env.fmt(self._node_env(n, latest), limit=3) or "",
                f"{d:.1f}" if d is not None else "·",
                f"{n['snr']:.0f}" if n["snr"] is not None else "·",
                str(n["hops"]) if n["hops"] is not None else "·",
                "це я" if n["me"] else ago(age)))
        self.tree.yview_moveto(first)
        if self.page == "home":
            self._draw_home()
        elif self.page == "node":
            self._draw_detail()
        elif self.page == "env":
            self._draw_env()
        elif self.page == "net":
            self._draw_net()

    def _tap_node(self, e):
        iid = self.tree.identify_row(e.y)
        if iid and iid.startswith("!"):
            self._open_detail(iid)

    def _message_node(self, iid):
        if not iid or not iid.startswith("!") or not self.snap:
            return
        node = next((n for n in self.snap["nodes"] if n["id"] == iid), None)
        if not node or node["me"]:
            return
        self._open_peer(iid, node["name"])
        self.peers[iid]["name"] = node["name"]
        self._show_page("dm")
        self._focus_entry(self.dm_entry)

    # ---- messages -------------------------------------------------------
    def _hint(self, t, text):
        t.config(state="normal")
        t.insert("end", text, "hint")
        t.config(state="disabled")

    def _poll_db(self, initial=False):
        """History lives in SQLite (written by mesh-logger); show rows we have not shown yet."""
        cols = "id,t,from_id,from_name,to_id,to_name,text,dm,own,snr,pkt_id,ack"
        try:
            if initial:
                rows = self.db.execute(f"SELECT {cols} FROM messages ORDER BY id DESC LIMIT 1500").fetchall()[::-1]
            else:
                rows = self.db.execute(f"SELECT {cols} FROM messages WHERE id>? ORDER BY id",
                                       (self.last_id,)).fetchall()
        except sqlite3.Error:
            return
        for rid, t, fid, fname, tid, tname, text, dm, own, snr, pkt, ack in rows:
            self.last_id = max(self.last_id, rid)
            if ack == "pending" and time.time() - t > 600:      # the answer never reached us (e.g. the screen restarted)
                ack = None
            m = {"t": t, "from": clean(fname or "?"), "text": clean(text), "own": bool(own), "snr": snr,
                 "pkt": pkt if own else None, "ack": ack if own else None}
            if own and pkt:
                self.by_pkt[pkt] = m
                if len(self.by_pkt) > 400:
                    self.by_pkt.pop(next(iter(self.by_pkt)))
            if dm:
                peer = tid if own else fid
                if not peer:
                    continue
                self.threads[peer].append(m)
                p = self.peers.setdefault(peer, {"name": clean((tname if own else fname) or peer), "t": 0, "text": ""})
                if not own and fname:
                    p["name"] = clean(fname)
                p["t"], p["text"] = t, m["text"]
                if not initial:
                    if peer == self.cur_peer and self.page == "dm":
                        self._append_dm(m)
                    elif not own:
                        self.unread_peer[peer] += 1
                    if not own:
                        self._notify("dm")
                    self._render_peers()
            else:
                self._add_chat(m)
                if not initial and not own:
                    if self.page != "chat":
                        self.unread_chat += 1
                    self._notify("msg")
        if initial and self.peers:
            self._render_peers()
        if rows:
            self._update_tabs()

    def _append_dm(self, m):
        t = self.dm_text
        t.config(state="normal")
        self._insert_dm(t, m)
        t.config(state="disabled")
        t.see("end")

    def _add_chat(self, m):
        c = self.chat
        c.config(state="normal")
        if not self.has_chat:
            c.delete("1.0", "end")
            self.has_chat = True
        c.insert("end", time.strftime("%H:%M ", time.localtime(m["t"])), "time")
        c.insert("end", m["from"] + ": ", "own" if m["own"] else "name")
        c.insert("end", m["text"])
        if m.get("snr") is not None:
            c.insert("end", f"  {m['snr']:.0f} dB", "snr")
        self._ack_mark(c, m)
        c.insert("end", "\n")
        c.config(state="disabled")
        c.see("end")

    def _notify(self, kind):
        self.flash_bar.config(bg=WARN if kind == "dm" else ACCENT)
        if self.flash_job:
            self.root.after_cancel(self.flash_job)
        self.flash_job = self.root.after(2500, lambda: self.flash_bar.config(bg=PANEL))
        play(kind)

    # ---- loops ----------------------------------------------------------
    def _pump(self):
        try:
            while True:
                kind, data, *rest = self.events.get_nowait()
                if kind == "nodes":
                    first = self.snap is None
                    self._merge_heard(data)
                    self.snap = data
                    self.my_num = data.get("num")
                    self.relay_nodes = [{"num": n["num"], "hops": n["hops"], "mqtt": n["mqtt"]} for n in data["nodes"]]
                    self._check_alerts()
                    self._render_nodes()
                    if first and os.environ.get("MESH_DEMO"):
                        self._demo(os.environ["MESH_DEMO"])
                elif kind == "pkt":
                    self._on_pkt(data)
                elif kind == "ack":
                    self._on_ack(data, *rest)
                elif kind == "trace_done":
                    self._trace_done(data, *rest)
                elif kind == "radio":
                    self.radio_info = data
                    if self.page == "node":
                        self._draw_detail()
                elif kind == "status":
                    ok = data == "online"
                    self.link_ok = ok
                    self._status("" if ok else data, ok)
        except queue.Empty:
            pass
        self._poll_db()
        self.root.after(1000, self._pump)

    def _on_pkt(self, p):
        """Every packet from the node: environment, device telemetry, waypoints, traceroute answers and the links
        that show who relays whom (for the route map)."""
        try:
            if mesh_env.interesting(p) and mesh_env.record(self.db, p) and self.page in ("home", "env"):
                self._redraw_current()
            me = self._me()
            if me and me["id"]:
                self.links.learn(p, me["id"], self.relay_nodes)
            tr = mesh_extra.parse_trace(p, self.my_num)
            if tr:
                self.traces[tr["dest"]] = tr
                self.links.learn_trace(tr)
                self.links.flush(self.db, force=True)
                if self.tracing == tr["dest"]:
                    self.tracing = None
                if self.page == "net":
                    self._draw_net()
            if mesh_extra.record_tele(self.db, p) and self.page == "node":
                self._draw_detail()
            if mesh_extra.record_waypoint(self.db, p):
                self._wp_changed()
        except sqlite3.Error:
            pass

    def _radio_loop(self):
        """Radio power from the journal and config of meshtasticd (read only); the journal is read in its own thread."""
        rp = mesh_extra.RadioPower(RADIO_STATE)
        while True:
            try:
                self.events.put(("radio", rp.refresh()))
            except Exception as exc:  # noqa: BLE001 — never let the thread die
                print(f"radio: {exc}", file=sys.stderr, flush=True)
            time.sleep(60)

    def _trace_route(self, node_id):
        """Traceroute to a node on request. Each one is a transmission, so they are spaced out and one at a time."""
        wait = TRACE_GAP - (time.time() - self.last_trace)
        if self.tracing:
            self._hint_bar("маршрут уже шукається…")
        elif wait > 0:
            self._hint_bar(f"маршрут можна запитувати раз на {TRACE_GAP} с: ще {int(wait) + 1} с")
        elif self.link.trace(node_id):
            self.last_trace, self.tracing = time.time(), node_id
            self._hint_bar("шукаю маршрут… відповідь може йти до хвилини")
            if self.page == "net":
                self._draw_net()
        else:
            self._hint_bar("немає зв'язку з нодою")

    def _trace_done(self, dest, ok):
        if self.tracing == dest:
            self.tracing = None
        if not ok or dest not in self.traces:
            self._hint_bar("маршрут не знайдено: вузол не відповів")
        else:
            self._hint_bar("")
        if self.page == "net":
            self._draw_net()

    def _demo(self, what):
        """Developer aid: MESH_DEMO=node|events|menu|fav opens that view for screenshots."""
        recent = sorted((n for n in self.snap["nodes"] if not n["me"] and n["id"]),
                        key=lambda n: -n["heard"])
        if what == "node" and recent:
            self._open_detail(recent[0]["id"])
        elif what == "events":
            self._insert_event_row(time.time() - 600, "new", "Демо: новий вузол Test")
            self._insert_event_row(time.time(), "fav_off", "★ Демо не на зв'язку (2 год)")
            self._set_nodes_view("events")
            self._show_page("nodes")
        elif what == "env":
            self._open_env()
        elif what == "nodes":
            self._show_page("nodes")
        elif what == "menu":
            self._ask_exit()
        elif what == "fav" and recent:
            self.sel = recent[0]["id"]
            self._show_page("home")

    def _tick(self):
        self.lbl_clock.config(text=time.strftime("%H:%M"))
        self._check_link()
        try:
            self.links.flush(self.db)
        except sqlite3.Error:
            pass
        self.root.after(5000, self._tick)


def main():
    root = tk.Tk()
    root.title("Mesh")
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
