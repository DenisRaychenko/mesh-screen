"""Environment readings (temperature, humidity, pressure, ...) from node telemetry.

Shared by mesh-logger.py (records every telemetry packet) and mesh-screen.py (records too, so the screen has
data even before the logger is restarted, and draws the summary). Standard library only.

record()  stores readings in the `env` table (plausibility-checked, rate-limited per node and quantity).
summary() averages the latest reading of each node within MAX_HOPS hops, plus a 24 h history for a sparkline.
"""
import math
import time

MAX_HOPS = 3           # only nodes this many hops away (or closer) are averaged
WINDOW = 3 * 3600      # s: a reading counts as "current" for this long
KEEP = 7 * 86400
MIN_GAP = 300          # s between stored readings of one quantity of one node
BUCKET = 1800          # s: sparkline step

# telemetry key -> (id, label, unit, plausible range); values outside the range are sensor garbage and dropped
METRICS = {
    "temperature": ("temperature", "Температура", "°C", (-60, 80)),
    "relativeHumidity": ("humidity", "Вологість", "%", (0, 100)),
    "barometricPressure": ("pressure", "Тиск", "гПа", (300, 1100)),
    "iaq": ("iaq", "Якість повітря (IAQ)", "", (0, 500)),
    "gasResistance": ("gas", "Опір газу", "МОм", (0, 100000)),
    "lux": ("lux", "Освітленість", "лк", (0, 200000)),
    "windSpeed": ("wind", "Вітер", "м/с", (0, 120)),
}
AIR = {
    "pm25Standard": ("pm25", "PM2.5", "мкг/м³", (0, 1000)),
    "co2": ("co2", "CO₂", "ppm", (0, 10000)),
}
ORDER = ["temperature", "humidity", "pressure", "iaq", "gas", "lux", "wind", "pm25", "co2"]
INFO = {m[0]: m for m in [*METRICS.values(), *AIR.values()]}
DIGITS = {"temperature": 1, "humidity": 0, "pressure": 1, "iaq": 0, "gas": 1, "lux": 0, "wind": 1,
          "pm25": 0, "co2": 0}
SHORT = {  # compact one-line form used in tables
    "temperature": lambda v: f"{v:.1f}°C", "humidity": lambda v: f"{v:.0f}%", "pressure": lambda v: f"{v:.0f} гПа",
    "iaq": lambda v: f"IAQ {v:.0f}", "lux": lambda v: f"{v:.0f} лк", "wind": lambda v: f"{v:.1f} м/с",
    "pm25": lambda v: f"PM2.5 {v:.0f}", "co2": lambda v: f"{v:.0f} ppm",
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS env (t INT, node TEXT, hops INT, k TEXT, v REAL);
CREATE INDEX IF NOT EXISTS env_t ON env (t);
"""


def ensure(db):
    db.executescript(SCHEMA)


def interesting(packet):
    """Cheap pre-check so callers can skip the DB for the vast majority of packets."""
    dec = packet.get("decoded") or {}
    if dec.get("portnum") != "TELEMETRY_APP":
        return False
    tel = dec.get("telemetry") or {}
    return bool(tel.get("environmentMetrics") or tel.get("airQualityMetrics"))


def extract(packet):
    """(sender id, hops or None, [(metric id, value)]) from a telemetry packet; values are range-checked."""
    if not interesting(packet):
        return None, None, []
    if packet.get("viaMqtt"):          # relayed through an MQTT bridge, not heard on the radio
        return None, None, []
    tel = packet["decoded"]["telemetry"]
    sender = packet.get("fromId")
    if not sender and isinstance(packet.get("from"), int):
        sender = f"!{packet['from']:08x}"
    if not sender:
        return None, None, []
    hs = packet.get("hopStart")
    hops = hs - (packet.get("hopLimit") or 0) if hs else None
    vals = []
    for block, table in ((tel.get("environmentMetrics"), METRICS), (tel.get("airQualityMetrics"), AIR)):
        for key, (mid, _label, _unit, (lo, hi)) in table.items():
            v = (block or {}).get(key)
            if isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) and lo <= v <= hi:
                vals.append((mid, float(v)))
    return sender, hops, vals


def record(db, packet, now=None):
    """Store the readings of one packet. Returns how many rows were written. Caller serialises DB access."""
    sender, hops, vals = extract(packet)
    if not vals:
        return 0
    now = int(now if now is not None else time.time())
    written = 0
    for mid, v in vals:
        last = db.execute("SELECT MAX(t) FROM env WHERE node = ? AND k = ?", (sender, mid)).fetchone()[0]
        if last and now - last < MIN_GAP:
            continue
        db.execute("INSERT INTO env VALUES (?,?,?,?,?)", (now, sender, hops, mid, v))
        written += 1
    db.execute("DELETE FROM env WHERE t < ?", (now - KEEP,))
    db.commit()
    return written


def latest_per_node(db, since=86400, now=None):
    """{node id: ({metric id: value}, time of the newest reading)} for the last `since` seconds."""
    now = int(now if now is not None else time.time())
    out = {}
    for node, k, v, t in db.execute("SELECT node, k, v, t FROM env WHERE t >= ? ORDER BY t", (now - since,)):
        vals, last = out.get(node, ({}, 0))
        vals[k] = round(v, DIGITS.get(k, 1))
        out[node] = (vals, max(last, t))
    return out


def fmt(vals, limit=4):
    """'20.1°C · 75% · 1023 гПа' from {metric id: value}; None if nothing to show."""
    parts = [SHORT[k](vals[k]) for k in ORDER if k in vals and k in SHORT][:limit]
    return " · ".join(parts) if parts else None


def summary(db, hops_now, names, exclude=(), max_hops=MAX_HOPS, now=None):
    """Average over nodes within max_hops hops.

    hops_now: {node id: hopsAway from the node list} (used when a reading carries no hop count)
    names:    {node id: display name}
    exclude:  node ids to ignore (e.g. MQTT-only nodes)
    """
    now = int(now if now is not None else time.time())
    excl = set(exclude)

    def hops_of(node, h):
        h = h if h is not None else hops_now.get(node)
        return h if h is not None and h <= max_hops else None

    recent = db.execute("SELECT node, k, v, t, hops FROM env WHERE t >= ? ORDER BY t", (now - WINDOW,)).fetchall()
    day = db.execute("SELECT node, k, v, t, hops FROM env WHERE t >= ? ORDER BY t", (now - 86400,)).fetchall()

    latest = {}
    for node, k, v, t, h in recent:
        latest[(node, k)] = (v, t, h)
    values, sources = {}, {}
    for (node, k), (v, t, h) in latest.items():
        hh = hops_of(node, h)
        if node in excl or hh is None:
            continue
        values.setdefault(k, []).append(v)
        src = sources.setdefault(node, {"id": node, "name": names.get(node, node), "hops": hh, "t": t, "values": {}})
        src["values"][k] = round(v, DIGITS.get(k, 1))
        src["t"] = max(src["t"], t)

    # history: average inside each node first (a chatty node must not outweigh the rest), then across nodes
    buckets = {}
    for node, k, v, t, h in day:
        if node in excl or hops_of(node, h) is None:
            continue
        buckets.setdefault((k, t // BUCKET * BUCKET), {}).setdefault(node, []).append(v)
    series = {}
    for (k, b), per_node in sorted(buckets.items(), key=lambda kv: kv[0][1]):
        means = [sum(x) / len(x) for x in per_node.values()]
        series.setdefault(k, []).append([b + BUCKET // 2, round(sum(means) / len(means), DIGITS.get(k, 1))])

    metrics = []
    for k in ORDER:
        vs = values.get(k)
        if not vs:
            continue
        d = DIGITS.get(k, 1)
        mid, label, unit, _ = INFO[k]
        metrics.append({"id": mid, "label": label, "unit": unit, "avg": round(sum(vs) / len(vs), d),
                        "min": round(min(vs), d), "max": round(max(vs), d), "n": len(vs),
                        "series": series.get(k, [])})
    srcs = sorted(sources.values(), key=lambda x: (x["hops"], -x["t"]))
    for x in srcs:
        x["age"] = now - x.pop("t")
    return {"max_hops": max_hops, "window": WINDOW, "metrics": metrics, "sources": srcs, "now": now}
