#!/usr/bin/env python3
import argparse, json, csv, os, math, datetime
import folium

def to_float(v):
    try: return float(v)
    except: return None

def decode_latlon(rec):
    """Return (lat, lon) or (None, None). Handles *_i 1e-7 deg fallback."""
    lat = rec.get("latitude")
    lon = rec.get("longitude")
    if lat is not None and lon is not None:
        return to_float(lat), to_float(lon)
    # try payload
    p = rec.get("payload") or {}
    lat_i = rec.get("latitude_i", p.get("latitude_i"))
    lon_i = rec.get("longitude_i", p.get("longitude_i"))
    if lat_i is not None and lon_i is not None:
        return to_float(lat_i)/1e7, to_float(lon_i)/1e7
    return None, None

def get_alt(rec):
    p = rec.get("payload") or {}
    return rec.get("altitude", p.get("altitude"))

def get_time_iso(rec):
    # prefer our flattened times if present
    for k in ("local_time_iso","utc_time_iso"):
        if rec.get(k): return rec[k]
    # else try payload "time" as epoch
    p = rec.get("payload") or {}
    t = p.get("time") or rec.get("timestamp")
    try:
        t = int(float(t))
        return datetime.datetime.fromtimestamp(t).astimezone().isoformat(timespec="seconds")
    except:
        return None

def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2-lat1)
    dlmb = math.radians(lon2-lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlmb/2)**2
    return 2*R*math.asin(math.sqrt(a))

def load_records(path, sender_filter=None):
    out = []
    ext = os.path.splitext(path)[1].lower()
    if ext == ".jsonl":
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line=line.strip()
                if not line: continue
                try:
                    rec = json.loads(line)
                except:
                    continue
                if sender_filter and str(rec.get("sender","")).lower() not in sender_filter:
                    # also try 'from' (decimal)
                    frm = rec.get("from")
                    if frm is None or str(frm).lower() not in sender_filter:
                        continue
                out.append(rec)
    elif ext == ".csv":
        with open(path, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                # csv rows are strings; include only if sender matches (normalize)
                if sender_filter:
                    s = str(row.get("sender","")).lower()
                    frm = str(row.get("from","")).lower()
                    if s not in sender_filter and frm not in sender_filter:
                        continue
                out.append(row)
    else:
        raise SystemExit(f"Unsupported file type: {ext}")
    return out

def normalize_sender_list(ids):
    """Accept mix of decimal (e.g. 3825485809) and !hex (e.g. !db77dcd8)."""
    res=set()
    for s in ids or []:
        s=str(s).strip().lower()
        res.add(s)
    return res

def main():
    ap = argparse.ArgumentParser(description="Make an OSM map from Meshtastic CSV/JSONL")
    ap.add_argument("--input", required=True, help="Input file (.jsonl or .csv)")
    ap.add_argument("--output", default="track_map.html", help="Output HTML map")
    ap.add_argument("--sender", nargs="*", default=None, help="Only include these sender IDs (mix decimal or !hex)")
    ap.add_argument("--min-distance", type=float, default=5.0, help="Meters to skip near-duplicate points")
    ap.add_argument("--max-points", type=int, default=5000, help="Downsample if more points than this")
    args = ap.parse_args()

    sender_filter = normalize_sender_list(args.sender)
    recs = load_records(args.input, sender_filter=sender_filter if sender_filter else None)

    pts = []
    markers = []
    last = None
    for rec in recs:
        lat, lon = decode_latlon(rec)
        if lat is None or lon is None: 
            continue
        if last:
            if haversine_m(last[0], last[1], lat, lon) < args.min_distance:
                # keep fewer points in-place to reduce clutter
                pass
        t = get_time_iso(rec) or ""
        rssi = rec.get("rssi"); snr = rec.get("snr")
        alt = get_alt(rec)
        popup = []
        if t: popup.append(f"<b>time</b> {t}")
        popup.append(f"<b>lat</b> {lat:.6f}  <b>lon</b> {lon:.6f}")
        if alt not in (None,""): popup.append(f"<b>alt</b> {alt}")
        if rssi not in (None,""): popup.append(f"<b>rssi</b> {rssi}")
        if snr not in (None,""): popup.append(f"<b>snr</b> {snr}")
        markers.append((lat, lon, "<br>".join(popup)))
        pts.append((lat, lon))
        last = (lat, lon)

    if not pts:
        raise SystemExit("No GPS points found. Make sure your file contains latitude/longitude or *_i fields.")

    # light downsample if huge
    if len(pts) > args.max_points:
        step = max(1, len(pts)//args.max_points)
        pts = pts[::step]
        markers = markers[::step]

    # Center map on first point
    m = folium.Map(location=[pts[0][0], pts[0][1]], zoom_start=12, tiles="OpenStreetMap")

    # Route line
    folium.PolyLine(pts, weight=3, opacity=0.8).add_to(m)

    # Start/End markers
    folium.Marker(pts[0], tooltip="Start", icon=folium.Icon(color="green")).add_to(m)
    folium.Marker(pts[-1], tooltip="End", icon=folium.Icon(color="red")).add_to(m)

    # Waypoint markers (clickable)
    for lat, lon, html in markers:
        folium.CircleMarker([lat, lon], radius=3, fill=True).add_to(m)
        folium.Marker([lat, lon], popup=folium.Popup(html, max_width=300)).add_to(m)

    m.save(args.output)
    print(f"Wrote {args.output} with {len(pts)} points.")

if __name__ == "__main__":
    main()
