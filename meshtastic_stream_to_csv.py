#!/usr/bin/env python3
"""
meshtastic_stream_to_csv.py

Subscribe to Meshtastic public MQTT, optionally filter by node(s), and log to JSONL + CSV (+ optional RAW JSONL).
- Live view with env colors and trend arrows (↑ green / ↓ red / → yellow) for env + RSSI/SNR.
- Derived fields:
    relative_humidity  (numeric %, logged)
    barometric_pressure (numeric hPa, logged)
    relative_humidity_pretty  (live-only pretty)
    barometric_pressure_pretty (live-only pretty)
- Daily rotation for all outputs.
- Per-node split (--split-by-node), optional combined disable (--no-combined).
- NEW: Raw JSON capture (--raw-jsonl) writing the exact broker payload lines (with same rotation & split flags, and --raw-no-combined).

Default live keys now show **ALL CSV fields plus pretty fields**.
"""

import argparse
import csv
import json
import os
import re
import signal
import sys
import time
import shutil
import textwrap
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import paho.mqtt.client as mqtt

# ---------- Helpers ----------

def to_epoch_seconds(ts) -> Optional[int]:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return int(ts)
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except Exception:
            try:
                return int(float(ts))
            except Exception:
                return None
    return None

def to_hex_id_from_decimal(n: int) -> str:
    return f"!{n:08x}"

def normalize_node_id(s: Any) -> Optional[str]:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    if s.startswith("!"):
        return s.lower()
    try:
        n = int(s)
        return to_hex_id_from_decimal(n)
    except ValueError:
        return s.lower()

def c_to_f(c: Optional[float]) -> Optional[float]:
    if c is None:
        return None
    try:
        return round((float(c) * 9/5) + 32, 2)
    except Exception:
        return None

def hpa_to_inhg(hpa: float) -> float:
    return hpa * 0.0295299830714

def ensure_keys(d: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    return {k: d.get(k, None) for k in keys}

def safe_part(s: str) -> str:
    # keep alnum, dash, underscore, exclamation; replace others with '_'
    return re.sub(r'[^0-9A-Za-z_\-!]+', '_', s)[:64]

# ---------- CSV fields (include derived numeric) ----------
CSV_FIELDS = [
    "epoch",
    "local_time_iso",
    "utc_time_iso",
    "topic",
    "type",
    "from",
    "sender",
    "to",
    "id",
    "channel",
    "hop_start",
    "hops_away",
    "rssi",
    "snr",
    "text",
    "battery_level",
    "voltage",
    "temperature_c",
    "temperature_f",
    "humidity",
    "relative_humidity",     # numeric %
    "pressure_hpa",
    "barometric_pressure",   # numeric hPa
    "gas_resistance_ohm",
    "iaq",
    "lux",
    "white_lux",
    "radiation_cpm",
    "wind_speed",
    "wind_gust",
    "wind_lull",
    "wind_direction_deg",
    "latitude",
    "longitude",
    "altitude",
    "uptime_seconds",
    "channel_utilization",
    "air_util_tx",
]

# Live-only pretty field names
DERIVED_KEYS_PRETTY = ["relative_humidity_pretty", "barometric_pressure_pretty"]

# ---------- Env/RF + colors ----------
ENV_FIELDS = {
    "temperature_c","temperature_f","humidity","pressure_hpa","iaq","lux","white_lux",
    "wind_speed","wind_gust","wind_lull","wind_direction_deg","radiation_cpm",
    "relative_humidity","barometric_pressure",
}
RF_FIELDS = {"rssi","snr"}

GREEN = "\033[32m"; RED = "\033[31m"; YELL = "\033[33m"; RESET = "\033[0m"
FIELD_COLOR = {
    "temperature_c":"\033[33m", "temperature_f":"\033[33m",
    "humidity":"\033[36m", "relative_humidity":"\033[36m",
    "pressure_hpa":"\033[34m", "barometric_pressure":"\033[34m",
    "iaq":"\033[35m",
    "lux":"\033[97m", "white_lux":"\033[37m",
    "wind_speed":"\033[32m","wind_gust":"\033[32m","wind_lull":"\033[32m","wind_direction_deg":"\033[32m",
    "radiation_cpm":"\033[31m",
}
DERIVED_TREND_SOURCE = {  # pretty field -> numeric source for delta
    "relative_humidity_pretty": "relative_humidity",
    "barometric_pressure_pretty": "pressure_hpa",
}

# ---------- Derived (numeric for logs; pretty for live) ----------
def compute_derived_numeric(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    rh = row.get("humidity")
    if rh is not None:
        try: out["relative_humidity"] = float(rh)
        except Exception: out["relative_humidity"] = None
    else:
        out["relative_humidity"] = None
    p = row.get("pressure_hpa")
    try: out["barometric_pressure"] = float(p) if p is not None else None
    except Exception: out["barometric_pressure"] = None
    return out

def compute_derived_pretty(row: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    if row.get("relative_humidity") is not None:
        try: out["relative_humidity_pretty"] = f"{float(row['relative_humidity']):.0f} %"
        except Exception: pass
    if row.get("pressure_hpa") is not None:
        try:
            p = float(row["pressure_hpa"])
            out["barometric_pressure_pretty"] = f"{p:.1f} hPa ({hpa_to_inhg(p):.2f} inHg)"
        except Exception:
            pass
    return out

# ---------- Flattening ----------
def flatten_message(msg_json: Dict[str, Any], topic: str) -> Dict[str, Any]:
    _type = msg_json.get("type")
    _from = msg_json.get("from")
    _sender = msg_json.get("sender")
    _to = msg_json.get("to")
    _id = msg_json.get("id")
    channel = msg_json.get("channel")
    hop_start = msg_json.get("hop_start")
    hops_away = msg_json.get("hops_away")
    rssi = msg_json.get("rssi")
    snr = msg_json.get("snr")
    payload = msg_json.get("payload", {}) or {}

    epoch = to_epoch_seconds(msg_json.get("timestamp"))
    if epoch is None:
        epoch = int(time.time())

    local_time_iso = datetime.fromtimestamp(epoch).astimezone().isoformat(timespec="seconds")
    utc_time_iso   = datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(timespec="seconds")

    battery_level = payload.get("battery_level")
    voltage = payload.get("voltage")
    temperature_c = payload.get("temperature") or payload.get("temperature_c")
    humidity = payload.get("humidity")
    pressure_hpa = payload.get("pressure_hpa")
    gas_resistance_ohm = payload.get("gas_resistance_ohm")
    iaq = payload.get("iaq")
    lux = payload.get("lux")
    white_lux = payload.get("white_lux")
    radiation_cpm = payload.get("radiation_cpm")

    wind_speed = payload.get("wind_speed")
    wind_gust = payload.get("wind_gust")
    wind_lull = payload.get("wind_lull")
    wind_direction_deg = payload.get("wind_direction_deg")

    latitude = payload.get("latitude") or msg_json.get("latitude") or payload.get("decoded", {}).get("latitude")
    longitude = payload.get("longitude") or msg_json.get("longitude") or payload.get("decoded", {}).get("longitude")
    altitude = payload.get("altitude") or msg_json.get("altitude") or payload.get("decoded", {}).get("altitude")

    uptime_seconds = payload.get("uptime_seconds")
    channel_utilization = payload.get("channel_utilization")
    air_util_tx = payload.get("air_util_tx")

    text = msg_json.get("text") or payload.get("text")

    base = {
        "epoch": epoch,
        "local_time_iso": local_time_iso,
        "utc_time_iso": utc_time_iso,
        "topic": topic,
        "type": _type,
        "from": _from,
        "sender": _sender,
        "to": _to,
        "id": _id,
        "channel": channel,
        "hop_start": hop_start,
        "hops_away": hops_away,
        "rssi": rssi,
        "snr": snr,
        "text": text,
        "battery_level": battery_level,
        "voltage": voltage,
        "temperature_c": temperature_c,
        "temperature_f": c_to_f(temperature_c),
        "humidity": humidity,
        "pressure_hpa": pressure_hpa,
        "gas_resistance_ohm": gas_resistance_ohm,
        "iaq": iaq,
        "lux": lux,
        "white_lux": white_lux,
        "radiation_cpm": radiation_cpm,
        "wind_speed": wind_speed,
        "wind_gust": wind_gust,
        "wind_lull": wind_lull,
        "wind_direction_deg": wind_direction_deg,
        "latitude": latitude,
        "longitude": longitude,
        "altitude": altitude,
        "uptime_seconds": uptime_seconds,
        "channel_utilization": channel_utilization,
        "air_util_tx": air_util_tx,
    }
    base = ensure_keys(base, CSV_FIELDS)
    base = compute_derived_numeric(base)
    return base

# ---------- Live formatting ----------
def detect_width(override: Optional[int]) -> int:
    if override:
        return max(40, int(override))
    try:
        return max(40, shutil.get_terminal_size(fallback=(120, 24)).columns)
    except Exception:
        return 120

def _kv_string(k: str, v: Any) -> str:
    return f"{k} = {v}"

def layout_grid(kv_pairs, width: int, columns: Optional[int], colwidth: Optional[int], trunc: bool, colors: Optional[List[Optional[str]]] = None) -> str:
    keyw = max((len(k) for k, _ in kv_pairs), default=0)
    natural = max((len(_kv_string(k.rjust(keyw), v)) for k, v in kv_pairs), default=0)
    if columns and columns > 0:
        cols = columns
    else:
        min_cell = min(natural, 40)
        cols = max(1, width // (min_cell + 2))
    if colwidth and colwidth > 10:
        cell_w = colwidth
    else:
        cell_w = max(20, (width - 2*(cols-1)) // cols)

    out_lines = []
    for idx, (k, v) in enumerate(kv_pairs):
        k_al = k.rjust(keyw)
        s = _kv_string(k_al, v)
        if trunc:
            wrapped = [s[:cell_w].rstrip()]
        else:
            wrapped = textwrap.wrap(s, width=cell_w, replace_whitespace=False, drop_whitespace=False) or [""]
        if colors and colors[idx]:
            wrapped = [colors[idx] + line + RESET for line in wrapped]
        out_lines.append(wrapped)

    grid_lines = []
    for i in range(0, len(out_lines), cols):
        block = out_lines[i:i+cols]
        height = max(len(x) for x in block)
        for r in range(height):
            row_cells = []
            for cell in block:
                row_cells.append(cell[r] if r < len(cell) else "")
            grid_lines.append(("  ").join(x.ljust(cell_w) for x in row_cells))
    return "\n".join(grid_lines)

def format_live(row: Dict[str, Any], fmt: str, keys: Optional[List[str]], width: int, columns: Optional[int], colwidth: Optional[int], trunc: bool, color_mode: str, colors_for: Dict[str, Optional[str]]) -> str:
    # Default: ALL CSV fields plus pretty derived
    if keys is None:
        keys = CSV_FIELDS + DERIVED_KEYS_PRETTY
    data = [(k, row.get(k)) for k in keys if k in row and row.get(k) is not None]
    kv_pairs = [(k, str(v)) for k, v in data]

    colors = None
    if color_mode == "env":
        colors = []
        for k, _ in kv_pairs:
            color_key = k
            if k == "relative_humidity_pretty": color_key = "relative_humidity"
            if k == "barometric_pressure_pretty": color_key = "barometric_pressure"
            colors.append(FIELD_COLOR.get(color_key))

    if fmt == "json":
        d = {k: v for k, v in data}
        try:
            return json.dumps(d, ensure_ascii=False, indent=2)
        except Exception:
            return json.dumps(d, ensure_ascii=False)

    if fmt == "kv":
        if not kv_pairs:
            return "(no fields)"
        keyw = max(len(k) for k, _ in kv_pairs)
        parts = []
        for i, (k, v) in enumerate(kv_pairs):
            s = f"{k.rjust(keyw)} = {v}"
            if colors and colors[i]:
                s = colors[i] + s + RESET
            parts.append(s)
        return "  ".join(parts)

    return layout_grid(kv_pairs, width=width, columns=columns, colwidth=colwidth, trunc=trunc, colors=colors)

# ---------- Rotators ----------
class DailyRotator:
    def __init__(self, base: str, fieldnames: Optional[List[str]] = None, suffix: Optional[str] = None):
        self.base = base
        self.fieldnames = fieldnames  # None for raw lines
        self.suffix = suffix
        self.cur_date = None
        self.fp = None
        self.writer = None

    def _with_date(self, base: str, day: str) -> str:
        root, ext = os.path.splitext(base)
        if self.suffix:
            return f"{root}_{self.suffix}_{day}{ext or ''}"
        return f"{root}_{day}{ext or ''}"

    def _open(self):
        day = datetime.now().strftime("%Y-%m-%d")
        if self.cur_date == day and self.fp:
            return
        self.close()
        self.cur_date = day
        path = self._with_date(self.base, day)
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        mode = "a"
        newline = ""
        enc = "utf-8"
        self.fp = open(path, mode, newline=newline, encoding=enc)
        if self.fieldnames:
            self.writer = csv.DictWriter(self.fp, fieldnames=self.fieldnames)
            if os.stat(path).st_size == 0:
                self.writer.writeheader()

    def write_csv_row(self, row: Dict[str, Any]):
        self._open()
        self.writer.writerow(row)
        self.fp.flush()

    def write_jsonl_obj(self, obj: Dict[str, Any]):
        self._open()
        self.fp.write(json.dumps(obj, ensure_ascii=False) + "\n")
        self.fp.flush()

    def write_raw_line(self, raw_line: str):
        self._open()
        self.fp.write(raw_line + ("\n" if not raw_line.endswith("\n") else ""))
        self.fp.flush()

    def close(self):
        if self.fp:
            self.fp.close()
            self.fp = None
            self.writer = None

class PerNodeRotator:
    def __init__(self, base: str, fieldnames: Optional[List[str]] = None, include_type: bool = False):
        self.base = base
        self.fieldnames = fieldnames
        self.include_type = include_type
        self.map: Dict[str, DailyRotator] = {}

    def _suffix_from_row(self, row: Dict[str, Any]) -> str:
        node = row.get("sender") or normalize_node_id(row.get("from")) or "unknown"
        node = safe_part(node)
        if self.include_type:
            t = safe_part(str(row.get("type") or "unknown"))
            return f"{node}_{t}"
        return node

    def get(self, row: Dict[str, Any]) -> DailyRotator:
        key = self._suffix_from_row(row)
        if key not in self.map:
            self.map[key] = DailyRotator(self.base, fieldnames=self.fieldnames, suffix=key)
        return self.map[key]

    def close(self):
        for rot in self.map.values():
            rot.close()

# ---------- MQTT Logger ----------
class MeshtasticLogger:
    def __init__(self, args):
        self.args = args
                # Make sure targets exists before callbacks can ever fire
        self.targets = set(normalize_node_id(x) for x in (getattr(args, "nodes", []) or []))
        # If you also have split_for, normalize once here (safe even if not used)
        self.split_for = (
            set(normalize_node_id(x) for x in (getattr(args, "split_for", []) or []))
            or None
        )

        self.client = mqtt.Client(client_id=args.client_id or f"meshtastic-logger-{int(time.time())}", clean_session=True)
        if args.user:
            self.client.username_pw_set(args.user, args.password or None)

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

        
        # Nodes we want extra per-node files for (does NOT filter stream)
        #self.split_for = (set(normalize_node_id(x) for x in (getattr(args, "split_for", []) or [])) or None)
        self.split_for = (
            set(normalize_node_id(x) for x in (getattr(args, "split_for", []) or []))
            or None
        )

        # Rotators: combined
        self.csv_rot = DailyRotator(args.csv, fieldnames=CSV_FIELDS)
        self.json_rot = DailyRotator(args.jsonl, fieldnames=None)  # jsonl uses raw write_jsonl_obj
        # Per-node rotators
        self.csv_pernode = PerNodeRotator(args.csv, fieldnames=CSV_FIELDS) if args.split_by_node or args.split_for else None
        self.json_pernode = PerNodeRotator(args.jsonl, fieldnames=None) if args.split_by_node or args.split_for else None

        # RAW rotators (if enabled)
        self.raw_rot = DailyRotator(args.raw_jsonl, fieldnames=None) if args.raw_jsonl else None
        self.raw_pernode = PerNodeRotator(args.raw_jsonl, fieldnames=None) if (args.raw_jsonl and (args.split_by_node or args.split_for)) else None

        self.stopping = False
        self.last_values: Dict[str, Dict[str, float]] = {}

    def start(self):
        self._connect()
        self.client.loop_start()
        try:
            while not self.stopping:
                time.sleep(0.25)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def stop(self):
        self.stopping = True
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass
        # Close all rotators
        if self.csv_pernode: self.csv_pernode.close()
        if self.json_pernode: self.json_pernode.close()
        if self.raw_pernode: self.raw_pernode.close()
        self.csv_rot.close()
        self.json_rot.close()
        if self.raw_rot: self.raw_rot.close()

    def _connect(self):
        while not self.stopping:
            try:
                self.client.connect(self.args.host, self.args.port, keepalive=60)
                return
            except Exception as e:
                print(f"[WARN] MQTT connect failed: {e}. Retrying in 5s...", file=sys.stderr)
                time.sleep(5)

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"[INFO] Connected to {self.args.host}:{self.args.port}")
            client.subscribe(self.args.topic, qos=0)
            print(f"[INFO] Subscribed: {self.args.topic}")
        else:
            print(f"[ERROR] Connect failed with code {rc}", file=sys.stderr)

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            print("[WARN] Unexpected disconnect. Reconnecting...", file=sys.stderr)

    def _arrow_for_delta(self, delta: float, eps: float) -> str:
        if delta > eps:   return f" {GREEN}↑{RESET}"
        if delta < -eps:  return f" {RED}↓{RESET}"
        return f" {YELL}→{RESET}"

    def _decorate_trend(self, row: Dict[str, Any], eps: float) -> Dict[str, Any]:
        sender = row.get("sender") or row.get("from") or "?"
        prev_map = self.last_values.setdefault(sender, {})

        decorated = dict(row)

        trend_fields = (ENV_FIELDS | RF_FIELDS) - {"relative_humidity_pretty","barometric_pressure_pretty"}
        for k in trend_fields:
            v = row.get(k)
            if v is None:
                continue
            try:
                cur = float(v)
            except Exception:
                continue
            prev = prev_map.get(k)
            arrow = ""
            if prev is not None:
                arrow = self._arrow_for_delta(cur - prev, eps)
            decorated[k] = f"{v}{arrow}"
            prev_map[k] = cur

        for dk, src in DERIVED_TREND_SOURCE.items():
            if dk not in row:
                continue
            src_val = row.get(src)
            if src_val is None:
                continue
            try:
                cur = float(src_val)
            except Exception:
                continue
            prev = prev_map.get(src)
            arrow = ""
            if prev is not None:
                arrow = self._arrow_for_delta(cur - prev, eps)
            decorated[dk] = f"{row[dk]}{arrow}"
            prev_map[src] = cur

        return decorated

    def _write_logs(self, row: Dict[str, Any], enriched: Dict[str, Any], raw_line: Optional[str]):
        wrote_any = False

        # Per-node (CSV/JSON/RAW)
        # Per-node writes: if --split-by-node (all nodes) OR --split-for (selected nodes)
        want_pernode = False
        if self.args.split_by_node:
            want_pernode = True
        elif self.split_for:
            node_hex = normalize_node_id(row.get("sender") or row.get("from"))
            if node_hex in self.split_for:
                want_pernode = True

        if want_pernode:
            if self.csv_pernode:
                self.csv_pernode.get(row).write_csv_row(row)
            if self.json_pernode:
                self.json_pernode.get(row).write_jsonl_obj(enriched)
            if self.raw_pernode and raw_line is not None:
                self.raw_pernode.get(row).write_raw_line(raw_line)
            wrote_any = True

        # Combined (CSV/JSON/RAW) unless disabled with flags
        if (not self.args.split_by_node) or (self.args.split_by_node and not self.args.no_combined):
            self.csv_rot.write_csv_row(row)
            self.json_rot.write_jsonl_obj(enriched)
            if self.raw_rot and raw_line is not None and not self.args.raw_no_combined:
                self.raw_rot.write_raw_line(raw_line)
            wrote_any = True

        if not wrote_any:
            self.csv_rot.write_csv_row(row)
            self.json_rot.write_jsonl_obj(enriched)
            if self.raw_rot and raw_line is not None and not self.args.raw_no_combined:
                self.raw_rot.write_raw_line(raw_line)

    def on_message(self, client, userdata, msg):
        try:
            # Exact raw payload text (decoded to utf-8 with replacement, but no JSON reformat)
            raw_line = msg.payload.decode("utf-8", errors="replace").strip()
            if not raw_line:
                return

            try:
                obj = json.loads(raw_line)
            except json.JSONDecodeError:
                # If it isn't JSON, still store raw (if requested), but skip structured logging
                if self.raw_rot or self.raw_pernode:
                    # minimal synthetic row for routing fallback
                    dummy = {"sender": None, "from": None}
                    self._write_logs({}, {}, raw_line)
                return

            # Filter by --nodes if provided (accepts !hex or decimal)
            if self.targets:
                sender = (obj.get("sender") or "").lower().strip()
                frm = obj.get("from")
                frm_hex = normalize_node_id(frm) if frm is not None else None
                if sender not in self.targets and frm_hex not in self.targets:
                    return

            row = flatten_message(obj, topic=msg.topic)

            # Enrich JSONL with flattened & pretty
            enriched = dict(obj)
            enriched["_flat"] = ensure_keys(row, CSV_FIELDS)
            pretty = compute_derived_pretty(row)
            if pretty:
                enriched["_derived_pretty"] = pretty

            # Write logs (CSV/JSONL/RAW per flags)
            self._write_logs(row, enriched, raw_line if self.args.raw_jsonl else None)

            # Live display
            if self.args.live:
                try:
                    width = detect_width(self.args.live_width)
                    live_row = dict(row)
                    live_row.update(pretty)
                    if self.args.live_trend:
                        live_row = self._decorate_trend(live_row, eps=self.args.live_trend_epsilon)

                    colors_for = {k: None for k in (CSV_FIELDS + DERIVED_KEYS_PRETTY)}
                    if self.args.live_color == "env":
                        for k in ENV_FIELDS:
                            if k in colors_for:
                                colors_for[k] = FIELD_COLOR.get(k)
                        colors_for["relative_humidity_pretty"] = FIELD_COLOR.get("relative_humidity")
                        colors_for["barometric_pressure_pretty"] = FIELD_COLOR.get("barometric_pressure")

                    keys = self.args.live_keys if self.args.live_keys is not None else (CSV_FIELDS + DERIVED_KEYS_PRETTY)

                    line = format_live(
                        live_row,
                        fmt=self.args.live_format,
                        keys=keys,
                        width=width,
                        columns=self.args.live_columns,
                        colwidth=self.args.live_colwidth,
                        trunc=self.args.live_trunc,
                        color_mode=self.args.live_color,
                        colors_for=colors_for,
                    )
                    print(line)
                    if self.args.live_format == "grid":
                        print("-" * width)
                except Exception:
                    pass

        except Exception as e:
            print(f"[ERROR] on_message error: {e}", file=sys.stderr)

# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Stream Meshtastic MQTT to JSONL + CSV (+ optional raw) with optional node filtering.")

    # MQTT
    parser.add_argument("--host", default="mqtt.meshtastic.org", help="MQTT host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT port")
    parser.add_argument("--user", default="meshdev", help="MQTT username (public broker uses 'meshdev')")
    parser.add_argument("--password", default="large4cats", help="MQTT password for user (public broker default)")
    parser.add_argument("--topic", default="msh/US/#", help="MQTT topic filter (e.g., msh/US/#)")
    parser.add_argument("--client-id", default=None, help="MQTT client ID")

    # Filter
    parser.add_argument("--nodes", nargs="*", help="Filter to these node IDs (accepts '!xxxxxxxx' or decimal like 3825485809)")

    # Files (CSV/JSONL)
    parser.add_argument("--csv", default="meshtastic_stream.csv", help="CSV base filename (daily rotated)")
    parser.add_argument("--jsonl", default="meshtastic_stream.jsonl", help="JSONL base filename (daily rotated)")

    # Raw JSON capture
    parser.add_argument("--raw-jsonl", default=None, help="RAW JSONL base filename (daily rotated). Writes exact broker payload lines.")
    parser.add_argument("--raw-no-combined", action="store_true", help="When splitting, do NOT write combined RAW file")

    # Live
    parser.add_argument("--live", action="store_true", help="Print a live summary for each packet")
    parser.add_argument("--live-format", choices=["grid","kv","json"], default="grid", help="Live summary format")
    parser.add_argument("--live-keys", type=lambda s: [x.strip() for x in s.split(",")] if s else None,
                        help="Comma-separated list of fields to show in live (default: ALL CSV fields + pretty RH/baro)")
    parser.add_argument("--live-width", type=int, default=None, help="Override detected terminal width")
    parser.add_argument("--live-columns", type=int, default=None, help="Force number of columns in grid format")
    parser.add_argument("--live-colwidth", type=int, default=None, help="Max width of each grid cell")
    parser.add_argument("--live-trunc", action="store_true", help="Truncate values in grid cells instead of wrapping")
    parser.add_argument("--live-color", choices=["off","env"], default="env", help="Colorize env values in live output")
    parser.add_argument("--live-trend", action="store_true", help="Show trend arrows (↑/↓/→) on env + RF fields (arrow colored only)")
    parser.add_argument("--live-trend-epsilon", type=float, default=0.01, help="Ignore small changes below this delta")

    # Split options
    parser.add_argument("--split-by-node", action="store_true", help="Write separate CSV/JSONL per node")
    parser.add_argument("--no-combined", action="store_true", help="When splitting, do NOT write the combined CSV/JSONL files")
    parser.add_argument("--split-for", nargs="*",default=None, help="Only create per-node files for these node IDs; others go to combined only (requires --split-by-node).")
    args = parser.parse_args()

    logger = MeshtasticLogger(args)

    def handle_sigterm(signum, frame):
        logger.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    logger.start()

if __name__ == "__main__":
    main()
