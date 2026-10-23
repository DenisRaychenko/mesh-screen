"""Smoke test of the touch screen UI with fake data (needs Tk and a display; no node, no network):

    python tests/test_mesh_screen_ui.py            # draws every page, checks for exceptions
    SHOT_DIR=out python tests/test_mesh_screen_ui.py   # also saves PNG screenshots (needs Pillow)

Window size is forced to the 800x480 of the Pi screen.
"""
import importlib.util
import os
import sys
import tempfile
import time
import tkinter as tk

here = os.path.dirname(os.path.abspath(__file__))
tmp = tempfile.mkdtemp()
os.environ["MESH_DB"] = os.path.join(tmp, "messages.db")
os.environ["MESH_PORT"] = "1"                       # nothing listens there: the link just stays offline
spec = importlib.util.spec_from_file_location("meshscreen", os.path.join(here, "..", "mesh-screen.py"))
ms = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ms)
ms.SETTINGS = os.path.join(tmp, "settings.json")
ms.RADIO_STATE = os.path.join(tmp, "radio.json")
import mesh_extra as x  # noqa: E402

ok = True


def check(name, cond, extra=""):
    global ok
    ok &= bool(cond)
    print("ok  " if cond else "FAIL", name, extra)


root = tk.Tk()
root.winfo_screenwidth = lambda: 800
root.winfo_screenheight = lambda: 480
root.attributes = lambda *a, **k: None
app = ms.App(root)
root.geometry("800x480+0+0")
root.wm_attributes("-topmost", True)
root.update()
shots = os.environ.get("SHOT_DIR")


def shot(name):
    root.lift()
    root.update()
    time.sleep(0.15)
    root.update()
    if not shots:
        return
    from PIL import ImageGrab
    os.makedirs(shots, exist_ok=True)
    x0, y0 = root.winfo_rootx(), root.winfo_rooty()
    full = ImageGrab.grab()               # physical pixels; Tk coordinates are logical when Windows scales the display
    k = full.width / int(root.tk.call("winfo", "screenwidth", "."))
    full.crop((round(x0 * k), round(y0 * k), round((x0 + 800) * k), round((y0 + 480) * k))).resize((800, 480)).save(
        os.path.join(shots, name + ".png"))


NOW = time.time()


def node(i, name, hops, role="CLIENT", lat=None, lon=None, snr=5.0, age=60, me=False):
    return {"id": f"!{i:08x}", "num": i, "name": name, "long": name + " long", "snr": snr, "hops": hops, "heard": NOW - age,
            "me": me, "lat": lat, "lon": lon, "hw": "TBEAM", "role": role, "dev": {"batteryLevel": 80, "voltage": 4.0},
            "env": {}, "mqtt": False}


nodes = [node(1, "Чечен", None, lat=50.45, lon=30.52, me=True, snr=None)]
roles = ["CLIENT", "ROUTER", "REPEATER", "CLIENT_MUTE", "TRACKER", "SENSOR"]
for i in range(2, 30):
    h = 0 if i < 5 else (1 if i < 12 else (2 if i < 20 else (3 if i < 27 else None)))
    nodes.append(node(i, f"Вузол{i}", h, roles[i % len(roles)], 50.45 + (i % 7 - 3) * 0.01, 30.52 + (i % 5 - 2) * 0.02, age=i * 500))
snap = {"me": "Чечен", "nodes": nodes, "chan": "LongFast · слот 20", "tx": 10, "num": 1}
app.snap = snap
app.my_num = 1
app.relay_nodes = [{"num": n["num"], "hops": n["hops"], "mqtt": False} for n in nodes]
app._status("", True)
app._render_nodes()

# links learned from packets
for i in range(2, 12):
    app.links.learn({"fromId": f"!{i:08x}", "from": i, "hopStart": 3, "hopLimit": 3, "rxSnr": 4.0}, "!00000001", app.relay_nodes)
app.links.learn({"fromId": "!00000010", "from": 16, "hopStart": 3, "hopLimit": 2, "relayNode": 6, "rxSnr": 3.0}, "!00000001", app.relay_nodes)
app.links.add("!00000001", "!00000006", "down", "direct", 5)
app._on_pkt({"from": 6, "fromId": "!00000006", "to": 1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 9,
             "traceroute": {"route": [3], "routeBack": [4], "snrTowards": [20, 16], "snrBack": [12, 8]}}})
check("a traceroute answer is kept and teaches links", "!00000006" in app.traces
      and len(x.load_links(app.db, x.day_of(time.time()))) > 5)
app._on_pkt({"from": 3, "fromId": "!00000003", "to": 0xFFFFFFFF, "decoded": {"portnum": "TELEMETRY_APP", "telemetry": {
    "deviceMetrics": {"batteryLevel": 70, "voltage": 3.9}}}})
app._on_pkt({"from": 3, "fromId": "!00000003", "decoded": {"portnum": "WAYPOINT_APP", "waypoint": {
    "id": 77, "latitudeI": 504600000, "longitudeI": 305300000, "expire": int(NOW) + 3600, "name": "Склад"}}})
for i in range(30):
    app.db.execute("INSERT INTO tele(t,node,k,v) VALUES (?,?,?,?)", (NOW - 3600 * (30 - i) / 2, "!00000001", "battery", 70 + i % 9))
    app.db.execute("INSERT INTO tele(t,node,k,v) VALUES (?,?,?,?)", (NOW - 3600 * (30 - i) / 2, "!00000001", "voltage", 3.8 + (i % 9) / 20))
    app.db.execute("INSERT INTO heard(t,node_id,snr,rssi,hops) VALUES (?,?,?,?,?)", (NOW - 3000 * (30 - i) / 2, "!00000001", -3 + i % 8, -90, 0))
app.db.commit()

for page in ("home", "chat", "dm", "nodes", "net"):
    app._show_page(page)
    root.update()
    shot("page-" + page)
check("every page draws", True)

app._show_page("nodes")
app._set_nodes_view("points")
root.update()
shot("nodes-points")
check("the received waypoint is listed", len(app.wp_tree.get_children()) == 1)
app._set_nodes_view("nodes")

app._show_page("net")
app.net_sel = "!00000006"
app._draw_net()
shot("net-selected")
check("the network canvas drew nodes and links", len(app.ncanvas.find_all()) > 60)
app._net_zoom_by(1)
app._net_toggle("up")
app._net_step_day(-1)
app._net_step_day(1)
check("zoom, direction chips and day switch do not fail", app.net_day is None)
app._draw_net()

app.radio_info = {"logged": 20, "final": None, "cap": 8, "radio": "e32.yaml", "cached": True, "seen": 1, "access": True}
app._open_detail("!00000001")
root.update()
shot("detail-own-radio")
check("own node opens on the radio block", app.detail_metric == "radio")
for key in ("snr", "battery", "voltage", "chutil"):
    app._set_metric(key)
    root.update()
    shot("detail-" + key)
app.radio_info = {"logged": None, "final": None, "cap": 22, "radio": None, "cached": False, "seen": None, "access": False}
app._set_metric("radio")
root.update()
shot("detail-radio-estimate")
app._open_detail("!00000003")
root.update()
shot("detail-other")
check("another node opens on SNR", app.detail_metric == "snr")

# delivery status
ids = iter(range(4242, 5000))
app.link.send = lambda text, dest=None: next(ids)
app._show_page("chat")
app.chat_entry.var.set("перевірка")
app._send(None, None)
check("a sent message is stored as pending", app.db.execute("SELECT ack FROM messages WHERE pkt_id=4242").fetchone()[0] == "pending")
app._poll_db()
check("... and shown with the pending mark", "⏳" in app.chat.get("1.0", "end"))
app._on_ack(4242, {"from": 9, "decoded": {"routing": {"errorReason": "NONE"}}}, None)
check("a neighbour's ack turns it into ✓", "✓" in app.chat.get("1.0", "end") and "⏳" not in app.chat.get("1.0", "end"))
shot("chat-ack")
app._send("Ок", "!00000003")
app._poll_db()
app._on_ack(4243, {"from": 3, "decoded": {"routing": {"errorReason": "NONE"}}}, 3)
check("the addressee's ack gives ✓✓", app.by_pkt[4243]["ack"] == "delivered")
app._open_peer("!00000003", "Вузол3")
root.update()
check("a private message is stored with its destination", app.db.execute("SELECT COUNT(*) FROM messages WHERE dm=1 AND own=1").fetchone()[0] == 1)

# quick messages
before = len(app.settings["quick"])
app.chat_entry.var.set("Їду")
app._quick_add(app.chat_entry)
check("a quick message can be added", len(app.settings["quick"]) == before + 1 and "Їду" in ms.load_settings()["quick"])
app._quick_toggle()
app._quick_del("Їду")
app._quick_toggle()
check("... and deleted", "Їду" not in app.settings["quick"])

# waypoints from the screen
sent = []
app.link.waypoint = lambda *a, **k: sent.append((a, k)) or True
app.wp_var.set("Привал")
app._wp_add()
check("a waypoint is created at our position", sent and len(mesh_extra_list := x.list_waypoints(app.db)) == 2, str(sent[:1]))
mine = next(w for w in x.list_waypoints(app.db) if w["name"] == "Привал")
app.wp_sel = mine["id"]
app._wp_del()
check("our own waypoint can be deleted, a foreign one cannot", len(x.list_waypoints(app.db)) == 1)
app.wp_sel = 77
sent.clear()
app._wp_del()
check("deleting a foreign waypoint sends nothing", not sent)

# traceroute spacing
calls = []
app.link.trace = lambda d: calls.append(d) or True
app.last_trace, app.tracing = 0.0, None
app._trace_route("!00000005")
app._trace_route("!00000006")
check("traceroutes are one at a time and spaced out", calls == ["!00000005"])
root.destroy()
sys.exit(0 if ok else 1)
