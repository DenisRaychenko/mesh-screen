"""Tests for mesh_env.py (standard library only):  python tests/test_mesh_env.py"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import mesh_env as e

NOW = 1_800_000_000
db = sqlite3.connect(":memory:")
e.ensure(db)


def pkt(i, env=None, air=None, hops=None):
    tel = {}
    if env is not None:
        tel["environmentMetrics"] = env
    if air is not None:
        tel["airQualityMetrics"] = air
    p = {"from": i, "fromId": f"!{i:08x}", "decoded": {"portnum": "TELEMETRY_APP", "telemetry": tel}}
    if hops is not None:
        p["hopStart"], p["hopLimit"] = 3 + hops, 3
    return p


ok = True


def check(name, cond, extra=""):
    global ok
    ok &= bool(cond)
    print("ok  " if cond else "FAIL", name, extra)


check("non-telemetry packet is ignored", e.record(db, {"decoded": {"portnum": "TEXT_MESSAGE_APP"}}, NOW) == 0)
check("telemetry without env block is ignored", not e.interesting(pkt(1, None)))
e.record(db, pkt(1, {"temperature": 20.0, "relativeHumidity": 50, "barometricPressure": 1010.0}, hops=1), NOW)
e.record(db, pkt(2, {"temperature": 24.0, "relativeHumidity": 60}, hops=2), NOW)
e.record(db, pkt(3, {"temperature": 40.0}, hops=5), NOW)                       # too far
e.record(db, pkt(4, {"temperature": 22.0, "barometricPressure": 1014.0}), NOW)  # no hop info
e.record(db, pkt(5, {"temperature": 200.0, "relativeHumidity": 130, "barometricPressure": 5}, hops=1), NOW)
n = e.record(db, pkt(1, {"temperature": 99.0}, hops=1), NOW + 60)               # inside MIN_GAP
check("garbage values rejected", db.execute("SELECT COUNT(*) FROM env WHERE node='!00000005'").fetchone()[0] == 0)
check("rate limit: second reading within MIN_GAP is skipped", n == 0)
e.record(db, pkt(2, air={"pm25Standard": 12, "co2": 450}, hops=2), NOW + 10)
check("packets relayed via MQTT are ignored",
      e.record(db, dict(pkt(7, {"temperature": 30.0}, hops=1), viaMqtt=True), NOW) == 0
      and db.execute("SELECT COUNT(*) FROM env WHERE node='!00000007'").fetchone()[0] == 0)

s = e.summary(db, {"!00000004": 3}, {"!00000001": "N1"}, now=NOW + 120)
by = {m["id"]: m for m in s["metrics"]}
check("temperature is the mean of nodes within 3 hops", by["temperature"]["avg"] == 22.0 and by["temperature"]["n"] == 3,
      str(by["temperature"]["avg"]))
check("min/max", (by["temperature"]["min"], by["temperature"]["max"]) == (20.0, 24.0))
check("humidity mean(50, 60)", by["humidity"]["avg"] == 55 and by["humidity"]["n"] == 2)
check("pressure mean(1010, 1014)", by["pressure"]["avg"] == 1012.0)
check("air quality included", by["pm25"]["avg"] == 12 and by["co2"]["avg"] == 450)
check("order is stable", [m["id"] for m in s["metrics"]][:3] == ["temperature", "humidity", "pressure"])
check("far node excluded, sources sorted by hops",
      [x["hops"] for x in s["sources"]] == [1, 2, 3], str([(x["name"], x["hops"]) for x in s["sources"]]))
check("names mapped", s["sources"][0]["name"] == "N1")
check("series for the sparkline", len(by["temperature"]["series"]) >= 1)

x = e.summary(db, {"!00000004": 3}, {}, exclude={"!00000001"}, now=NOW + 120)
bx = {m["id"]: m for m in x["metrics"]}
check("exclude (MQTT filter) drops a node from the average", bx["temperature"]["avg"] == 23.0 and bx["temperature"]["n"] == 2)
check("old readings fall out of the window", e.summary(db, {}, {}, now=NOW + e.WINDOW + 600)["metrics"] == [])
lat = e.latest_per_node(db, now=NOW + 120)
check("latest_per_node", lat["!00000001"][0]["pressure"] == 1010.0 and lat["!00000001"][1] == NOW)
check("fmt compact string", e.fmt(lat["!00000001"][0]) == "20.0°C · 50% · 1010 гПа", e.fmt(lat["!00000001"][0]))
check("fmt of nothing", e.fmt({}) is None)
sys.exit(0 if ok else 1)
