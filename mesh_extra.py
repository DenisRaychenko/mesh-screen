"""Features of the Meshpi web panel for the touch screen: learned links between nodes (route map), traceroute results,
device telemetry history, waypoints, delivery status of sent messages and the radio power read from meshtasticd.

Standard library only; no Tk and no meshtastic import, so everything here is testable on any machine
(scripts/tests/test_mesh_extra.py). Used by mesh-screen.py, which owns the SQLite connection and the UI.

Everything here only reads: the radio power comes from the journal and the config files of meshtasticd, and nothing
in this module changes a setting of any node.
"""
import glob
import json
import math
import os
import re
import sqlite3
import subprocess
import time

BROADCAST_ID = "!ffffffff"
LINK_KEEP = 8 * 86400
TELE_KEEP = 8 * 86400
TELE_GAP = 300                      # s between stored values of one quantity of one node
TELE_KEYS = {"batteryLevel": ("battery", "Батарея", "%"), "voltage": ("voltage", "Напруга", "В"),
             "channelUtilization": ("chutil", "Ефір · канал", "%"), "airUtilTx": ("airtx", "Ефір · TX", "%")}
TELE_ORDER = ["battery", "voltage", "chutil", "airtx"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS links (
  day TEXT NOT NULL, src TEXT NOT NULL, dst TEXT NOT NULL, flow TEXT NOT NULL, kind TEXT NOT NULL,
  n INTEGER NOT NULL, snr_sum REAL NOT NULL, snr_n INTEGER NOT NULL, t REAL NOT NULL,
  PRIMARY KEY (day, src, dst, flow, kind)
);
CREATE TABLE IF NOT EXISTS tele (t REAL NOT NULL, node TEXT NOT NULL, k TEXT NOT NULL, v REAL NOT NULL);
CREATE INDEX IF NOT EXISTS tele_node ON tele(node, k, t);
CREATE TABLE IF NOT EXISTS waypoint (
  id INTEGER PRIMARY KEY, lat REAL, lon REAL, name TEXT, descr TEXT, icon INTEGER, expire INTEGER,
  locked INTEGER, sender TEXT, t REAL
);
"""


def ensure(db):
    db.executescript(SCHEMA)
    try:                              # delivery status of our own messages (the logger ignores this column)
        db.execute("ALTER TABLE messages ADD COLUMN ack TEXT")
    except sqlite3.OperationalError:
        pass


def node_id(num):
    return f"!{num & 0xFFFFFFFF:08x}"


def day_of(t):
    return time.strftime("%Y-%m-%d", time.localtime(t))


def hops_used(packet):
    hs = packet.get("hopStart")
    return hs - (packet.get("hopLimit") or 0) if hs else None


# ---- routes: links learned from the air (same rules as the web panel) ----------------------------------------------------
def resolve_relay(nodes, byte, exclude):
    """Node by the last byte of its id. A relay is our direct neighbour: among several candidates take the one with the
    smallest hopsAway, and only if it is unique; otherwise None (better no line than a wrong one)."""
    cands = [n for n in nodes if isinstance(n.get("num"), int) and n["num"] & 0xFF == byte
             and node_id(n["num"]) not in exclude and not n.get("mqtt")]
    if not cands:
        return None
    key = lambda n: n["hops"] if n.get("hops") is not None else 99  # noqa: E731
    best = min(key(n) for n in cands)
    top = [n for n in cands if key(n) == best]
    return node_id(top[0]["num"]) if len(top) == 1 else None


class Links:
    """Collects links in memory and writes them to the DB in batches (a busy mesh produces many packets)."""

    def __init__(self):
        self.buf = {}
        self.flushed = 0.0

    def add(self, src, dst, flow, kind, snr=None, t=None):
        if not src or not dst or src == dst or BROADCAST_ID in (src, dst):
            return
        t = float(t or time.time())
        e = self.buf.setdefault((day_of(t), src, dst, flow, kind), [0, 0.0, 0, t])
        e[0] += 1
        e[3] = t
        if isinstance(snr, (int, float)) and not isinstance(snr, bool) and math.isfinite(snr):
            e[1] += snr
            e[2] += 1

    def learn(self, packet, me, nodes):
        """me: our node id; nodes: [{num, hops, mqtt}]. A packet heard on the air shows who relayed it."""
        if packet.get("viaMqtt") or not me:
            return
        sender = packet.get("fromId") or (node_id(packet["from"]) if isinstance(packet.get("from"), int) else None)
        if not sender:
            return
        t, snr = packet.get("rxTime") or time.time(), packet.get("rxSnr")
        used, byte = hops_used(packet), packet.get("relayNode")
        if sender == me:      # our packet relayed by a neighbour and heard back: the way "down" without a traceroute
            relay = resolve_relay(nodes, byte, {me}) if isinstance(byte, int) else None
            if relay and used:
                self.add(me, relay, "down", "direct" if used == 1 else "via", snr, t)
            return
        dec = packet.get("decoded") or {}
        if dec.get("portnum") == "NEIGHBORINFO_APP":
            info = dec.get("neighborinfo") or dec.get("neighborInfo") or {}
            for nb in info.get("neighbors") or []:
                if isinstance(nb.get("nodeId"), int):
                    self.add(node_id(nb["nodeId"]), sender, "up", "direct", nb.get("snr"), t)
        if used == 0:
            self.add(sender, me, "up", "direct", snr, t)
            return
        if not isinstance(byte, int):
            return
        relay = resolve_relay(nodes, byte, {me, sender})
        if relay is None:
            return
        self.add(relay, me, "up", "relay", snr, t)
        if used == 1:
            self.add(sender, relay, "up", "direct", None, t)
        elif used and used > 1:
            self.add(sender, relay, "up", "via", None, t)

    def learn_trace(self, tr):
        """tr: the result of parse_trace(); every step is a direct hearing, forward is "down", back is "up"."""
        for key, flow in (("towards", "down"), ("back", "up")):
            path, snrs = tr[key], tr.get(key + "_snr") or []
            for i, (a, b) in enumerate(zip(path, path[1:])):
                self.add(a, b, flow, "direct", snrs[i] if i < len(snrs) else None, tr["t"])

    def flush(self, db, force=False, now=None):
        now = now or time.time()
        if not force and now - self.flushed < 20:
            return
        self.flushed = now
        items, self.buf = list(self.buf.items()), {}
        if not items:
            return
        try:
            for (day, a, b, flow, kind), (n, ssum, scnt, t) in items:
                db.execute("INSERT INTO links (day,src,dst,flow,kind,n,snr_sum,snr_n,t) VALUES (?,?,?,?,?,?,?,?,?)"
                           " ON CONFLICT (day,src,dst,flow,kind) DO UPDATE SET n=n+excluded.n,"
                           " snr_sum=snr_sum+excluded.snr_sum, snr_n=snr_n+excluded.snr_n, t=MAX(t,excluded.t)",
                           (day, a, b, flow, kind, n, ssum, scnt, t))
            db.execute("DELETE FROM links WHERE t < ?", (now - LINK_KEEP,))
            db.commit()
        except sqlite3.Error:
            pass


def parse_trace(packet, me_num):
    """Answer to our traceroute -> {dest, towards, back, towards_snr, back_snr, t}; None for anything else."""
    dec = packet.get("decoded") or {}
    tr = dec.get("traceroute")
    if dec.get("portnum") != "TRACEROUTE_APP" or not isinstance(tr, dict) or packet.get("to") != me_num:
        return None
    if not dec.get("requestId"):          # a request from someone else's traceroute passing by is not our answer
        return None
    dest = packet.get("from")
    if not isinstance(dest, int):
        return None
    toward = [me_num, *[x for x in tr.get("route") or [] if isinstance(x, int)], dest]
    back = [dest, *[x for x in tr.get("routeBack") or [] if isinstance(x, int)], me_num]
    snr = lambda arr: [(x / 4 if isinstance(x, (int, float)) and x != -128 else None) for x in arr or []]  # noqa: E731
    return {"dest": node_id(dest), "towards": [node_id(x) for x in toward], "back": [node_id(x) for x in back],
            "towards_snr": snr(tr.get("snrTowards")), "back_snr": snr(tr.get("snrBack")), "t": time.time()}


def load_links(db, day):
    """Links of one day (aggregated)."""
    rows = db.execute("SELECT src,dst,flow,kind,n,snr_sum,snr_n,t FROM links WHERE day=?", (day,)).fetchall()
    return [{"src": s, "dst": d, "flow": f, "kind": k, "n": n, "snr": (ss / sn) if sn else None, "t": t}
            for s, d, f, k, n, ss, sn, t in rows]


def link_days(db, limit=8):
    return [r[0] for r in db.execute("SELECT DISTINCT day FROM links ORDER BY day DESC LIMIT ?", (limit,))]


# ---- map layout: hop rings -------------------------------------------------------------------------------------------------
def bearing(me, n):
    """Angle (radians, 0 = east, counter-clockwise like the maths) from me to n by their positions; None without both."""
    if None in (me.get("lat"), me.get("lon"), n.get("lat"), n.get("lon")):
        return None
    dx = (n["lon"] - me["lon"]) * math.cos(math.radians((me["lat"] + n["lat"]) / 2))
    dy = n["lat"] - me["lat"]
    return math.atan2(dy, dx) if dx or dy else None


def spread(angles, gap):
    """angles: {id: angle}; move the angles as little as possible so that neighbours on the circle are at least `gap`
    radians apart (isotonic regression on the circle cut at its widest gap; evenly spaced if that cannot fit)."""
    if not angles:
        return {}
    tau = 2 * math.pi
    n = len(angles)
    gap = min(gap, tau / n)
    ids = sorted(angles, key=lambda k: angles[k] % tau)
    pos = [angles[k] % tau for k in ids]
    if n == 1:
        return {ids[0]: pos[0]}
    cut = max(range(n), key=lambda i: (pos[(i + 1) % n] - pos[i]) % tau or tau)     # widest gap follows ids[cut]
    order = [(cut + 1 + j) % n for j in range(n)]
    base = pos[order[0]]
    seq = [(pos[i] - base) % tau for i in order]                                    # ascending, 0 <= seq < tau
    q = [seq[j] - j * gap for j in range(n)]
    blocks = []                                                                     # pool adjacent violators
    for v in q:
        blocks.append([v, 1])
        while len(blocks) > 1 and blocks[-2][0] / blocks[-2][1] > blocks[-1][0] / blocks[-1][1]:
            s2, c2 = blocks.pop()
            blocks[-1][0] += s2
            blocks[-1][1] += c2
    fixed = [b[0] / b[1] for b in blocks for _ in range(b[1])]
    new = [fixed[j] + j * gap for j in range(n)]
    if new[-1] + gap > tau + 1e-9:                                                  # does not fit around the circle
        new = [j * tau / n for j in range(n)]
    return {ids[order[j]]: (base + new[j]) % tau for j in range(n)}


def ring_layout(me, nodes, radii, min_px, max_ring=None):
    """Place nodes on rings by hop count. radii[k] = pixel radius of ring k (k>=1); nodes with unknown hops go to the
    outer ring. Returns {id: (ring, angle, radius)}. Positions use the real bearing where known, and nodes are pushed
    apart along the ring so that none overlaps (min_px = needed distance between two markers)."""
    outer = max_ring or (len(radii) - 1)
    rings = {}
    for n in nodes:
        h = n.get("hops")
        k = outer if h is None else max(1, min(int(h), outer))
        if h == 0:
            k = 1
        rings.setdefault(k, []).append(n)
    out = {}
    for k, ns in rings.items():
        r = radii[k] if k < len(radii) else radii[-1]
        with_pos = {n["id"]: bearing(me, n) for n in ns}
        free = [n["id"] for n in ns if with_pos[n["id"]] is None]
        angles = {i: a for i, a in with_pos.items() if a is not None}
        for idx, i in enumerate(free):                 # without a position: spread over the whole ring
            angles[i] = 2 * math.pi * (idx + 0.5) / max(len(free), 1) + math.pi / 7
        for i, a in spread(angles, min_px / max(r, 1)).items():
            out[i] = (k, a, r)
    return out


# ---- roles ---------------------------------------------------------------------------------------------------------------
# role -> (shape, Ukrainian label); the shapes are drawn by the screen
ROLES = {
    "CLIENT": ("circle", "Клієнт"), "CLIENT_MUTE": ("circle_open", "Клієнт (мовчазний)"),
    "CLIENT_HIDDEN": ("circle_open", "Клієнт (прихований)"), "CLIENT_BASE": ("circle_dot", "Базовий клієнт"),
    "ROUTER": ("square", "Роутер"), "ROUTER_CLIENT": ("square", "Роутер-клієнт"), "ROUTER_LATE": ("square_open", "Роутер (пізній)"),
    "REPEATER": ("triangle", "Репітер"), "TRACKER": ("diamond", "Трекер"), "TAK_TRACKER": ("diamond", "TAK-трекер"),
    "SENSOR": ("hex", "Датчик"), "TAK": ("hex", "TAK"), "LOST_AND_FOUND": ("cross", "Знайти загублене"),
}
DEFAULT_ROLE = "CLIENT"


def role_info(role):
    if isinstance(role, int):
        role = None
    return ROLES.get(role or DEFAULT_ROLE, ROLES[DEFAULT_ROLE])


# ---- telemetry history ---------------------------------------------------------------------------------------------------
_last_tele = {}


def record_tele(db, packet, now=None):
    """Device metrics of a node (battery, voltage, airtime) for the charts; at most one value per TELE_GAP."""
    dec = packet.get("decoded") or {}
    if dec.get("portnum") != "TELEMETRY_APP" or packet.get("viaMqtt"):
        return False
    dm = (dec.get("telemetry") or {}).get("deviceMetrics") or {}
    node = packet.get("fromId") or (node_id(packet["from"]) if isinstance(packet.get("from"), int) else None)
    if not dm or not node:
        return False
    now = now or time.time()
    wrote = False
    for key, (k, _label, _unit) in TELE_KEYS.items():
        v = dm.get(key)
        if not isinstance(v, (int, float)) or isinstance(v, bool) or not math.isfinite(v):
            continue
        if k == "battery" and v > 100:        # 101 means "powered from the mains"
            continue
        if now - _last_tele.get((node, k), 0) < TELE_GAP:
            continue
        _last_tele[(node, k)] = now
        db.execute("INSERT INTO tele (t,node,k,v) VALUES (?,?,?,?)", (now, node, k, float(v)))
        wrote = True
    if wrote:
        db.execute("DELETE FROM tele WHERE t < ?", (now - TELE_KEEP,))
        db.commit()
    return wrote


def tele_series(db, node, k, since):
    return db.execute("SELECT t,v FROM tele WHERE node=? AND k=? AND t>? ORDER BY t", (node, k, since)).fetchall()


# ---- waypoints -----------------------------------------------------------------------------------------------------------
WP_NEVER = 0x7FFFFFFF
WP_DEFAULT_ICON = 0x1F4CD


def record_waypoint(db, packet, now=None):
    """A waypoint from the air: new or updated; expire == 0 (or in the past) deletes it; locked ones obey their owner."""
    dec = packet.get("decoded") or {}
    if dec.get("portnum") != "WAYPOINT_APP" or packet.get("viaMqtt"):
        return False
    w = dec.get("waypoint") or {}
    wid, snum = w.get("id"), packet.get("from")
    sender = packet.get("fromId") or (node_id(snum) if isinstance(snum, int) else None)
    if not isinstance(wid, int) or not sender:
        return False
    now = now or time.time()
    expire = int(w.get("expire") or 0)
    row = db.execute("SELECT locked FROM waypoint WHERE id=?", (wid,)).fetchone()
    if row and row[0] and row[0] != snum:
        return False
    if expire == 0 or expire <= now:
        db.execute("DELETE FROM waypoint WHERE id=?", (wid,))
        db.commit()
        return True
    lat, lon = w.get("latitudeI"), w.get("longitudeI")
    if not isinstance(lat, int) or not isinstance(lon, int) or not (-900000000 <= lat <= 900000000
                                                                    and -1800000000 <= lon <= 1800000000):
        return False
    db.execute("INSERT OR REPLACE INTO waypoint (id,lat,lon,name,descr,icon,expire,locked,sender,t) VALUES (?,?,?,?,?,?,?,?,?,?)",
               (wid, lat / 1e7, lon / 1e7, str(w.get("name") or "")[:30], str(w.get("description") or "")[:100],
                int(w.get("icon") or 0), expire, int(w.get("lockedTo") or 0), sender, now))
    db.execute("DELETE FROM waypoint WHERE expire <= ?", (now,))
    db.commit()
    return True


def list_waypoints(db, now=None):
    now = now or time.time()
    rows = db.execute("SELECT id,lat,lon,name,descr,icon,expire,sender,t FROM waypoint WHERE expire > ? ORDER BY t DESC",
                      (now,)).fetchall()
    return [{"id": i, "lat": la, "lon": lo, "name": nm, "descr": ds, "icon": ic, "expire": None if ex >= WP_NEVER else ex,
             "sender": s, "t": t} for i, la, lo, nm, ds, ic, ex, s, t in rows]


def wp_icon(icon):
    """The symbol is a Unicode code point; Tk cannot draw anything outside the BMP, so those become a plain marker."""
    if isinstance(icon, int) and 0x20 < icon <= 0xFFFF and not 0xD800 <= icon < 0xE000:
        return chr(icon)
    return "◆"


# ---- delivery status -----------------------------------------------------------------------------------------------------
def delivery(resp, dest_num):
    """Answer of the library to a message sent with wantAck -> (state, text).
    delivered = the addressee itself confirmed; relayed = a neighbour took it (or, for a channel message, heard it
    being repeated); failed = a routing error."""
    resp = resp or {}
    dec = resp.get("decoded") or {}
    reason = (dec.get("routing") or {}).get("errorReason", "NONE")
    if reason not in (None, "NONE"):
        return "failed", reason
    if dest_num is not None and resp.get("from") == dest_num:
        return "delivered", ""
    return "relayed", ""


ACK_MARK = {"pending": " ⏳", "delivered": " ✓✓", "relayed": " ✓", "failed": " ✗", None: ""}


# ---- radio power (read only) ---------------------------------------------------------------------------------------------
UNIT = "meshtasticd"
POWER_RE = re.compile(r"Set radio:.*power=(-?\d+)")
FINAL_RE = re.compile(r"Final Tx power: (-?\d+)")
CAP_RE = re.compile(r"^\s*(?:SX126X|RF95)_MAX_POWER:\s*(\d+)", re.M)
CFG_GLOB = os.pathsep.join(["/etc/meshtasticd/config.yaml", "/etc/meshtasticd/config.d/*.yaml"])


def parse_journal(text):
    """Newest first (journalctl -r) -> (power after the region limit, power really given to the chip)."""
    logged, final = POWER_RE.findall(text or ""), FINAL_RE.findall(text or "")
    return (int(logged[0]) if logged else None), (int(final[0]) if final else None)


def chip_cap(cfg_glob=CFG_GLOB):
    """(ceiling of the chip, name of the config that sets it); the usual 22 if configs exist without the key."""
    cap, radio, seen = None, None, False
    for f in [p for pat in cfg_glob.split(os.pathsep) if pat for p in sorted(glob.glob(pat))]:
        try:
            text = open(f, encoding="utf-8", errors="replace").read()
        except OSError:
            continue
        seen = True
        found = CAP_RE.findall(text)
        if found:
            cap, radio = int(found[-1]), os.path.basename(f)
    return (cap if cap is not None else (22 if seen else None)), radio


def journal_text(unit=UNIT):
    try:
        r = subprocess.run(["journalctl", "-u", unit, "-b", "--no-pager", "-o", "cat", "-g", "Set radio|Final Tx power",
                            "-n", "10", "-r"], capture_output=True, text=True, errors="replace", timeout=8)
        return r.stdout
    except (OSError, subprocess.SubprocessError):
        return None                         # None = no journalctl / no access, "" = the journal has no such lines


def start_key(unit=UNIT):
    """Identifier of this start of meshtasticd (boot + moment the service became active)."""
    try:
        boot = open("/proc/sys/kernel/random/boot_id").read().strip()
        mono = subprocess.run(["systemctl", "show", unit, "-p", "ActiveEnterTimestampMonotonic", "--value"],
                              capture_output=True, text=True, timeout=5).stdout.strip()
        return f"{boot}:{mono}" if mono else None
    except (OSError, subprocess.SubprocessError):
        return None


class RadioPower:
    """The journal keeps "Set radio" only for some hours, so the last seen values are remembered in a file and stay
    valid until meshtasticd starts again."""

    def __init__(self, state_path, journal=journal_text, key=start_key, cap=chip_cap):
        self.path, self.journal, self.key, self.cap = state_path, journal, key, cap
        self.state = {}
        try:
            with open(state_path, encoding="utf-8") as fh:
                s = json.load(fh)
            self.state = {k: s.get(k) for k in ("key", "logged", "final", "seen")}
        except (OSError, ValueError):
            pass

    def refresh(self):
        """-> {logged, final, cap, radio, cached, seen, access}; `access` False when the journal cannot be read."""
        text = self.journal()
        logged, final = parse_journal(text)
        key = self.key()
        if key and self.state.get("key") not in (None, key):          # meshtasticd restarted: old values are void
            self.state.update(logged=None, final=None, seen=None)
        if key:
            self.state["key"] = key
        if logged is not None:
            self.state["logged"] = logged
        if final is not None:
            self.state["final"] = final
        fresh = logged is not None or final is not None
        if fresh:
            self.state["seen"] = int(time.time())
        self._save()
        cap, radio = self.cap()
        return {"logged": self.state.get("logged"), "final": self.state.get("final"), "cap": cap, "radio": radio,
                "cached": bool(self.state.get("seen")) and not fresh, "seen": self.state.get("seen"),
                "access": text is not None}

    def _save(self):
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(self.state, fh)
            os.replace(tmp, self.path)
        except OSError:
            pass


def effective_power(info, setting):
    """What the chip really puts out: Final Tx if known, else the logged value capped by the chip. When the journal has
    nothing, the node's own setting is shown as an estimate (flag True)."""
    final, logged, cap = info.get("final"), info.get("logged"), info.get("cap")
    if final is not None:
        return final, False
    if logged is not None:
        return (min(logged, cap) if cap is not None else logged), False
    if setting:
        return setting, True
    return None, False
