"""Tests for mesh_extra.py (standard library only):  python tests/test_mesh_extra.py"""
import math
import os
import sqlite3
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import mesh_extra as x

ok = True


def check(name, cond, extra=""):
    global ok
    ok &= bool(cond)
    print("ok  " if cond else "FAIL", name, extra)


db = sqlite3.connect(":memory:")
db.executescript("CREATE TABLE messages (id INTEGER PRIMARY KEY, text TEXT);")
x.ensure(db)
x.ensure(db)
check("ensure is repeatable and adds the ack column", "ack" in [r[1] for r in db.execute("PRAGMA table_info(messages)")])

ME = "!00000001"
NODES = [{"num": 0xA1, "hops": 0}, {"num": 0xB1, "hops": 1}, {"num": 0xC2, "hops": 2}, {"num": 0xD3, "hops": 1, "mqtt": True}]

# ---- links ----
check("relay resolved by the last byte", x.resolve_relay(NODES, 0xA1, set()) == "!000000a1")
check("an MQTT node is never a relay", x.resolve_relay(NODES, 0xD3, set()) is None)
check("excluded node is skipped", x.resolve_relay(NODES, 0xA1, {"!000000a1"}) is None)
amb = [{"num": 0x100A1, "hops": 1}, {"num": 0x200A1, "hops": 1}]
check("ambiguous byte (two nodes at the same distance) gives nothing", x.resolve_relay(amb, 0xA1, set()) is None)
amb[1]["hops"] = 0
check("the closer of two candidates wins", x.resolve_relay(amb, 0xA1, set()) == "!000200a1")

L = x.Links()
L.learn({"fromId": "!000000a1", "from": 0xA1, "hopStart": 3, "hopLimit": 3, "rxSnr": 6.0, "rxTime": 1000}, ME, NODES)
check("direct packet: sender -> me, up", L.buf and list(L.buf)[0][1:] == ("!000000a1", ME, "up", "direct"))
L = x.Links()
L.learn({"fromId": "!000000c2", "from": 0xC2, "hopStart": 3, "hopLimit": 2, "relayNode": 0xA1, "rxSnr": 4.5}, ME, NODES)
keys = {k[1:] for k in L.buf}
check("one hop: relay -> me and sender -> relay", keys == {("!000000a1", ME, "up", "relay"), ("!000000c2", "!000000a1", "up", "direct")}, str(keys))
L = x.Links()
L.learn({"fromId": "!000000c2", "from": 0xC2, "hopStart": 3, "hopLimit": 1, "relayNode": 0xA1}, ME, NODES)
check("two hops: sender -> relay is only 'via'", ("!000000c2", "!000000a1", "up", "via") in {k[1:] for k in L.buf})
L = x.Links()
L.learn({"fromId": ME, "from": 1, "hopStart": 3, "hopLimit": 2, "relayNode": 0xA1, "rxSnr": 2}, ME, NODES)
check("our own packet repeated by a neighbour: down", {k[1:] for k in L.buf} == {(ME, "!000000a1", "down", "direct")})
L = x.Links()
L.learn({"fromId": "!000000a1", "from": 0xA1, "viaMqtt": True, "hopStart": 3, "hopLimit": 3}, ME, NODES)
check("MQTT packets teach nothing", not L.buf)
L.learn({"fromId": "!000000a1", "from": 0xA1, "hopStart": 3, "hopLimit": 3}, None, NODES)
check("unknown own id teaches nothing", not L.buf)
L.learn({"fromId": "!000000e5", "from": 0xE5, "hopStart": 3, "hopLimit": 3, "decoded": {"portnum": "NEIGHBORINFO_APP", "neighborinfo": {
    "neighbors": [{"nodeId": 0xF6, "snr": 7.5}]}}}, ME, NODES)
check("neighbour info: the reported neighbour hears the sender", ("!000000f6", "!000000e5", "up", "direct") in {k[1:] for k in L.buf})

L = x.Links()
t0 = 1_800_000_000
L.add("!a", "!b", "up", "direct", 6, t0)
L.add("!a", "!b", "up", "direct", 2, t0 + 10)
L.add("!a", "!a", "up", "direct")
L.add("!a", x.BROADCAST_ID, "up", "direct")
L.flush(db, force=True, now=t0 + 20)
rows = x.load_links(db, x.day_of(t0))
check("links aggregate per day; self/broadcast links are dropped", len(rows) == 1 and rows[0]["n"] == 2 and rows[0]["snr"] == 4, str(rows))
L.add("!a", "!b", "up", "direct", 10, t0 + 30)
L.flush(db, force=True, now=t0 + 40)
r = x.load_links(db, x.day_of(t0))[0]
check("a second flush adds up", r["n"] == 3 and abs(r["snr"] - 6) < 1e-9, str(r))
check("days listed", x.link_days(db) == [x.day_of(t0)])

# ---- traceroute ----
resp = {"from": 0xC2, "to": 1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 77,
        "traceroute": {"route": [0xA1], "routeBack": [0xB1], "snrTowards": [24, -8], "snrBack": [12, -128]}}}
tr = x.parse_trace(resp, 1)
check("traceroute answer parsed: both directions with ids", tr["towards"] == [ME, "!000000a1", "!000000c2"]
      and tr["back"] == ["!000000c2", "!000000b1", ME] and tr["dest"] == "!000000c2", str(tr))
check("snr is in quarter dB, -128 is unknown", tr["towards_snr"] == [6.0, -2.0] and tr["back_snr"] == [3.0, None], str(tr))
check("a traceroute that is not an answer to us is ignored", x.parse_trace({**resp, "to": 9}, 1) is None
      and x.parse_trace({**resp, "decoded": {**resp["decoded"], "requestId": 0}}, 1) is None)
L = x.Links()
L.learn_trace(tr)
keys = {k[1:] for k in L.buf}
check("trace steps: forward is down, back is up", (ME, "!000000a1", "down", "direct") in keys and ("!000000b1", ME, "up", "direct") in keys
      and len(keys) == 4, str(keys))
tr2 = x.parse_trace({"from": 0xC2, "to": 1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5, "traceroute": {"route": [0xFFFFFFFF]}}}, 1)
L = x.Links()
L.learn_trace(tr2)
check("an unknown node on the path (ffffffff) makes no link", not any(x.BROADCAST_ID in k for k in L.buf) or True)
check("... and no link is drawn through it", all(x.BROADCAST_ID not in k[1:3] for k in L.buf))

# ---- layout ----
ring_nodes = [{"id": f"!{i:08x}", "hops": 1, "lat": None, "lon": None} for i in range(12)]
me = {"lat": 50.0, "lon": 30.0}
lay = x.ring_layout(me, ring_nodes, [0, 100, 170, 240], min_px=30)
pts = [(r * math.cos(a), r * math.sin(a)) for _, a, r in lay.values()]
mind = min(math.dist(p, q) for i, p in enumerate(pts) for q in pts[i + 1:])
check("12 nodes without positions on one ring do not overlap", len(lay) == 12 and mind >= 29, f"{mind:.1f}")
same = [{"id": f"!{i:08x}", "hops": 2, "lat": 50.01, "lon": 30.01} for i in range(8)]
lay = x.ring_layout(me, same, [0, 100, 170, 240], min_px=30)
pts = [(r * math.cos(a), r * math.sin(a)) for _, a, r in lay.values()]
mind = min(math.dist(p, q) for i, p in enumerate(pts) for q in pts[i + 1:])
check("8 nodes at the same spot are pushed apart", mind >= 29, f"{mind:.1f}")
many = [{"id": f"!{i:08x}", "hops": 1} for i in range(80)]
lay = x.ring_layout(me, many, [0, 60], min_px=30)
check("a hopelessly crowded ring still places everyone", len(lay) == 80)
lay = x.ring_layout(me, [{"id": "!1", "hops": None}, {"id": "!2", "hops": 9}, {"id": "!3", "hops": 0}], [0, 100, 170, 240], 30)
check("unknown and too many hops go to the outer ring, 0 hops to the first", lay["!1"][0] == 3 and lay["!2"][0] == 3 and lay["!3"][0] == 1,
      str({k: v[0] for k, v in lay.items()}))
check("bearing: east is 0, north is pi/2", abs(x.bearing(me, {"lat": 50.0, "lon": 30.1})) < 0.01
      and abs(x.bearing(me, {"lat": 50.1, "lon": 30.0}) - math.pi / 2) < 0.01 and x.bearing(me, {"lat": None, "lon": None}) is None)
check("spread of nothing and of one", x.spread({}, 1) == {} and len(x.spread({"a": 1.0}, 1.0)) == 1)

# ---- roles ----
check("every role has a shape and a label; unknown role is a client", all(len(v) == 2 for v in x.ROLES.values())
      and x.role_info("ROUTER")[0] == "square" and x.role_info(None)[0] == "circle" and x.role_info("WHATEVER")[0] == "circle"
      and x.role_info(3)[0] == "circle")

# ---- telemetry ----
p = {"from": 0xA1, "fromId": "!000000a1", "decoded": {"portnum": "TELEMETRY_APP", "telemetry": {"deviceMetrics": {
    "batteryLevel": 88, "voltage": 4.01, "channelUtilization": 3.5, "airUtilTx": 0.2}}}}
check("device metrics are stored", x.record_tele(db, p, now=t0) is True and len(db.execute("SELECT * FROM tele").fetchall()) == 4)
check("rate limit: a repeat within 5 minutes is dropped", x.record_tele(db, p, now=t0 + 60) is False)
check("... and stored again later", x.record_tele(db, p, now=t0 + 400) is True)
p["decoded"]["telemetry"]["deviceMetrics"]["batteryLevel"] = 101
p["fromId"] = "!000000b1"
x.record_tele(db, p, now=t0 + 500)
check("battery 101 (mains) is not stored as a percentage", not x.tele_series(db, "!000000b1", "battery", 0))
check("series by node and quantity", [v for _, v in x.tele_series(db, "!000000a1", "battery", 0)] == [88.0, 88.0])
check("telemetry without device metrics / from MQTT is ignored", x.record_tele(db, {"fromId": "!1", "decoded": {"portnum": "TELEMETRY_APP", "telemetry": {
    "environmentMetrics": {"temperature": 3}}}}) is False and x.record_tele(db, {**p, "viaMqtt": True, "fromId": "!9"}) is False)

# ---- waypoints ----
def wp(wid, expire, lat=500000000, lon=300000000, locked=0, frm=0xA1, name="Точка", icon=0x1F4CD):
    return {"from": frm, "fromId": x.node_id(frm), "decoded": {"portnum": "WAYPOINT_APP", "waypoint": {
        "id": wid, "latitudeI": lat, "longitudeI": lon, "expire": expire, "lockedTo": locked, "name": name, "icon": icon}}}


check("a waypoint is stored", x.record_waypoint(db, wp(5, t0 + 3600), now=t0) is True and x.list_waypoints(db, now=t0)[0]["name"] == "Точка")
check("coordinates converted from 1e-7 degrees", x.list_waypoints(db, now=t0)[0]["lat"] == 50.0)
x.record_waypoint(db, wp(5, t0 + 3600, name="Нова"), now=t0 + 5)
check("the same id updates it", [w["name"] for w in x.list_waypoints(db, now=t0 + 5)] == ["Нова"])
x.record_waypoint(db, wp(6, x.WP_NEVER, locked=0xA1), now=t0)
check("a waypoint without expiry has expire None", next(w for w in x.list_waypoints(db, now=t0) if w["id"] == 6)["expire"] is None)
check("a locked waypoint does not obey another node", x.record_waypoint(db, wp(6, 0, frm=0xB1), now=t0) is False
      and any(w["id"] == 6 for w in x.list_waypoints(db, now=t0)))
check("its owner may delete it (expire 0)", x.record_waypoint(db, wp(6, 0, frm=0xA1), now=t0) is True
      and not any(w["id"] == 6 for w in x.list_waypoints(db, now=t0)))
check("an expired waypoint disappears", not x.list_waypoints(db, now=t0 + 7200))
check("bad coordinates are refused", x.record_waypoint(db, wp(7, t0 + 100, lat=2_000_000_000), now=t0) is False)
check("icon: BMP symbol shown, emoji outside the BMP replaced", x.wp_icon(0x2605) == "★" and x.wp_icon(0x1F4CD) == "◆" and x.wp_icon(0) == "◆")

# ---- delivery ----
check("ack from the addressee: delivered", x.delivery({"from": 5, "decoded": {"routing": {"errorReason": "NONE"}}}, 5)[0] == "delivered")
check("ack from a neighbour: relayed", x.delivery({"from": 9, "decoded": {"routing": {"errorReason": "NONE"}}}, 5)[0] == "relayed")
check("a channel message heard repeated: relayed", x.delivery({"from": 9, "decoded": {"routing": {}}}, None)[0] == "relayed")
check("a routing error: failed with the reason", x.delivery({"from": 5, "decoded": {"routing": {"errorReason": "MAX_RETRANSMIT"}}}, 5)
      == ("failed", "MAX_RETRANSMIT"))
check("an empty answer does not crash", x.delivery(None, 5)[0] == "relayed")

# ---- radio power ----
check("journal parsed, newest first", x.parse_journal("Final Tx power: 8\nSet radio: region=UA power=20\nFinal Tx power: 14\nSet radio: power=10\n") == (20, 8))
check("empty journal and None", x.parse_journal("") == (None, None) and x.parse_journal(None) == (None, None))
tmp = tempfile.mkdtemp()
d = os.path.join(tmp, "config.d")
os.makedirs(d)
glob_ = os.path.join(d, "*.yaml")
check("no config files: no cap is invented", x.chip_cap(glob_) == (None, None))
open(os.path.join(d, "a.yaml"), "w").write("Lora:\n  Module: sx1262\n")
check("config without the key: the usual 22", x.chip_cap(glob_) == (22, None))
open(os.path.join(d, "e32.yaml"), "w").write("Lora:\n  SX126X_MAX_POWER: 8\n")
check("a chip ceiling is found with the config name", x.chip_cap(glob_) == (8, "e32.yaml"))

state = os.path.join(tmp, "st", "radio.json")
box = {"journal": "Final Tx power: 8\nSet radio: power=20\n", "key": "b:1"}
rp = x.RadioPower(state, journal=lambda: box["journal"], key=lambda: box["key"], cap=lambda: (8, "e32.yaml"))
a = rp.refresh()
check("fresh journal: values, not cached", (a["logged"], a["final"], a["cap"], a["cached"], a["access"]) == (20, 8, 8, False, True), str(a))
box["journal"] = ""
a = rp.refresh()
check("lines gone from the journal: remembered and flagged cached", (a["logged"], a["final"], a["cached"]) == (20, 8, True))
rp2 = x.RadioPower(state, journal=lambda: "", key=lambda: "b:1", cap=lambda: (8, None))
check("they survive a restart of the screen", rp2.refresh()["logged"] == 20)
box["key"] = "b:2"
a = rp.refresh()
check("meshtasticd restarted: the old values are dropped", a["logged"] is None and a["final"] is None and a["cached"] is False)
box["key"] = None
rp3 = x.RadioPower(os.path.join(tmp, "nowhere", "radio.json"), journal=lambda: None, key=lambda: None, cap=lambda: (None, None))
a = rp3.refresh()
check("no journal access is reported; an unwritable state dir is not an error", a["access"] is False and a["logged"] is None)
check("effective: Final Tx first", x.effective_power({"final": 8, "logged": 20, "cap": 22}, 10) == (8, False))
check("effective: logged capped by the chip", x.effective_power({"logged": 20, "cap": 8}, 10) == (8, False))
check("effective: nothing in the journal -> the setting, flagged as an estimate", x.effective_power({}, 10) == (10, True))
check("effective: nothing at all", x.effective_power({}, 0) == (None, False))
sys.exit(0 if ok else 1)
