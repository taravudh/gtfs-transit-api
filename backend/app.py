# app.py
# ============================================================
# GTFS Transit API (FastAPI) - Single-file version (Updated)
# - CORS: supports ALLOWED_ORIGINS + ALLOWED_ORIGIN_REGEX
# - Better error visibility (global exception handler)
# - Safer stop_id compare (cast to text)
# - Plan1x: prefer rail/BTS with filters + ranking + dedup + debug
# ============================================================

import os
import logging
from typing import Optional, List, Dict, Any, Tuple, Set

from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import RealDictCursor

from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gtfs-api")

app = FastAPI(title="GTFS Transit API", version="1.4")

# -----------------------------
# CORS (RECOMMENDED)
# -----------------------------
def _split_env_csv(name: str) -> List[str]:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]

ALLOWED_ORIGINS_LIST = _split_env_csv("ALLOWED_ORIGINS")
ALLOWED_ORIGIN_REGEX = (os.getenv("ALLOWED_ORIGIN_REGEX", "") or "").strip() or None

if not ALLOWED_ORIGINS_LIST and not ALLOWED_ORIGIN_REGEX:
    ALLOWED_ORIGINS_LIST = ["*"]
    allow_credentials = False
else:
    allow_credentials = True

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS_LIST,
    allow_origin_regex=ALLOWED_ORIGIN_REGEX,
    allow_credentials=allow_credentials,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=86400,
)

@app.get("/api/debug/cors")
def debug_cors():
    return {
        "ALLOWED_ORIGINS": ALLOWED_ORIGINS_LIST,
        "ALLOWED_ORIGIN_REGEX": ALLOWED_ORIGIN_REGEX,
        "allow_credentials": allow_credentials,
    }

# -----------------------------
# Global exception handler
# -----------------------------
@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error on %s %s", request.method, request.url)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "detail": str(exc),
            "path": str(request.url),
        },
    )

# -----------------------------
# DB ENV (Render)
# -----------------------------
DBHOST = os.getenv("DBHOST")
DBPORT = int(os.getenv("DBPORT", "5432"))
DBNAME = os.getenv("DBNAME")
DBUSER = os.getenv("DBUSER")
DBPASS = os.getenv("DBPASS")
PGSSLMODE = os.getenv("PGSSLMODE", "require")

def _require_db_env():
    if not all([DBHOST, DBNAME, DBUSER, DBPASS]):
        raise HTTPException(
            status_code=500,
            detail="Database ENV not set. Please set DBHOST, DBNAME, DBUSER, DBPASS (and DBPORT/PGSSLMODE if needed).",
        )

def get_conn():
    _require_db_env()
    return psycopg2.connect(
        host=DBHOST,
        port=DBPORT,
        dbname=DBNAME,
        user=DBUSER,
        password=DBPASS,
        sslmode=PGSSLMODE,
    )

# -----------------------------
# Helpers
# -----------------------------
def gtfs_time_to_dt(service_date: date, hhmmss: str, tzinfo: ZoneInfo) -> datetime:
    if hhmmss is None:
        raise ValueError("GTFS time is None")

    s = str(hhmmss).strip()
    parts = s.split(":")
    if len(parts) != 3:
        raise ValueError(f"Invalid GTFS time: {hhmmss}")

    h = int(parts[0])
    m = int(parts[1])
    sec = int(parts[2])
    extra_days = h // 24
    hour = h % 24

    base = datetime(service_date.year, service_date.month, service_date.day, 0, 0, 0, tzinfo=tzinfo)
    return base + timedelta(days=extra_days, hours=hour, minutes=m, seconds=sec)

def _service_ids_for_date(conn, d: date) -> List[str]:
    ymd = d.strftime("%Y%m%d")
    dow_col = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"][d.weekday()]

    base_sql = f"""
    SELECT service_id
    FROM gtfs.calendar
    WHERE start_date <= %s AND end_date >= %s
      AND {dow_col} = 1
    """

    ex_sql = """
    SELECT service_id, exception_type
    FROM gtfs.calendar_dates
    WHERE date = %s
    """

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(base_sql, (ymd, ymd))
        base = {r["service_id"] for r in cur.fetchall()}

        try:
            cur.execute(ex_sql, (ymd,))
            ex = cur.fetchall()
        except psycopg2.errors.UndefinedTable:
            conn.rollback()
            ex = []

    for r in ex:
        sid = r["service_id"]
        et = int(r["exception_type"])
        if et == 1:
            base.add(sid)
        elif et == 2:
            base.discard(sid)

    return sorted(base)

def _parse_csv_param_list(raw: Optional[str]) -> Optional[List[str]]:
    if raw is None:
        return None
    raw = (raw or "").strip()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]

def _dedup_keep_order(items: List[Dict[str, Any]], key_fn) -> List[Dict[str, Any]]:
    seen: Set[Any] = set()
    out: List[Dict[str, Any]] = []
    for it in items:
        k = key_fn(it)
        if k in seen:
            continue
        seen.add(k)
        out.append(it)
    return out

# ============================================================
# Endpoints
# ============================================================
@app.get("/health")
def health():
    return {"ok": True}

# -----------------------------
# Stops Autocomplete
# -----------------------------
@app.get("/api/stops/autocomplete")
def autocomplete_stops(
    q: str = Query(..., min_length=1, description="Search text (Thai/English)"),
    lat: Optional[float] = Query(None, description="User latitude"),
    lon: Optional[float] = Query(None, description="User longitude"),
    radius_m: int = Query(30000, ge=500, le=200000, description="Search radius in meters (when lat/lon provided)"),
    limit: int = Query(10, ge=1, le=30, description="Max results"),
):
    q = q.strip()
    if not q:
        return {"query": q, "count": 0, "items": []}

    use_geo = (lat is not None and lon is not None)

    if use_geo:
        sql = """
        SELECT
          stop_id,
          stop_name AS name,
          lat,
          lon,
          ST_Distance(
            geom::geography,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
          ) AS dist_m
        FROM gtfs.stops_search
        WHERE stop_name ILIKE '%%' || %s || '%%'
          AND ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
            %s
          )
        ORDER BY dist_m ASC
        LIMIT %s;
        """
        params = (lon, lat, q, lon, lat, radius_m, limit)
    else:
        sql = """
        SELECT
          stop_id,
          stop_name AS name,
          lat,
          lon,
          NULL::double precision AS dist_m
        FROM gtfs.stops_search
        WHERE stop_name ILIKE '%%' || %s || '%%'
        ORDER BY similarity(stop_name, %s) DESC, stop_name
        LIMIT %s;
        """
        params = (q, q, limit)

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    items = [
        {
            "stop_id": r["stop_id"],
            "name": r["name"],
            "lat": float(r["lat"]) if r["lat"] is not None else None,
            "lon": float(r["lon"]) if r["lon"] is not None else None,
            "dist_m": float(r["dist_m"]) if r["dist_m"] is not None else None,
        }
        for r in rows
    ]
    return {"query": q, "count": len(items), "items": items}

# -----------------------------
# Stops Nearby
# -----------------------------
@app.get("/api/stops/nearby")
def nearby_stops(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    radius_m: int = Query(1000, ge=100, le=20000),
    limit: int = Query(20, ge=1, le=50),
):
    sql = """
    SELECT
      stop_id,
      stop_name AS name,
      lat,
      lon,
      ST_Distance(
        geom::geography,
        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
      ) AS dist_m
    FROM gtfs.stops_search
    WHERE ST_DWithin(
      geom::geography,
      ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
      %s
    )
    ORDER BY dist_m ASC
    LIMIT %s;
    """
    params = (lon, lat, lon, lat, radius_m, limit)

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    items = [
        {
            "stop_id": r["stop_id"],
            "name": r["name"],
            "lat": float(r["lat"]) if r["lat"] is not None else None,
            "lon": float(r["lon"]) if r["lon"] is not None else None,
            "dist_m": round(float(r["dist_m"]), 1) if r["dist_m"] is not None else None,
        }
        for r in rows
    ]

    return {"lat": lat, "lon": lon, "radius_m": radius_m, "count": len(items), "items": items}

# -----------------------------
# Routes for Stop
# -----------------------------
@app.get("/api/stops/{stop_id}/routes")
def routes_for_stop(
    stop_id: str,
    limit: int = Query(50, ge=1, le=200, description="Max routes returned"),
):
    sql = """
    SELECT
      route_id,
      route_short_name,
      route_long_name,
      route_type,
      agency_id
    FROM (
      SELECT DISTINCT
        r.route_id,
        r.route_short_name,
        r.route_long_name,
        r.route_type,
        r.agency_id
      FROM gtfs.stop_times st
      JOIN gtfs.trips t  ON t.trip_id = st.trip_id
      JOIN gtfs.routes r ON r.route_id = t.route_id
      WHERE st.stop_id::text = %s
    ) x
    ORDER BY
      x.route_type,
      COALESCE(x.route_short_name, x.route_id::text),
      x.route_id
    LIMIT %s;
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (stop_id, limit))
            rows = cur.fetchall()

    items = [
        {
            "route_id": r["route_id"],
            "route_short_name": r["route_short_name"],
            "route_long_name": r["route_long_name"],
            "route_type": r["route_type"],
            "agency_id": r["agency_id"],
        }
        for r in rows
    ]
    return {"stop_id": stop_id, "count": len(items), "items": items}

# -----------------------------
# Next Trips
# -----------------------------
@app.get("/api/trips/next")
def next_trips(
    stop_id: str = Query(..., description="GTFS stop_id"),
    route_id: Optional[str] = Query(None, description="Optional filter by route_id"),
    direction_id: Optional[int] = Query(None, ge=0, le=1, description="Optional filter 0/1"),
    limit: int = Query(5, ge=1, le=30, description="Max results"),
    days_ahead: int = Query(7, ge=1, le=30, description="How many days ahead to search if today has no trips"),
    tz: str = Query("Asia/Bangkok", description="Timezone for time comparison"),
):
    stop_id = stop_id.strip()
    if not stop_id:
        raise HTTPException(status_code=400, detail="stop_id is required")

    try:
        tzinfo = ZoneInfo(tz)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid tz: {tz}")

    now = datetime.now(tzinfo)
    today = now.date()
    now_time_str = now.strftime("%H:%M:%S")

    with get_conn() as conn:
        for i in range(days_ahead):
            d = today + timedelta(days=i)
            service_ids = _service_ids_for_date(conn, d)
            if not service_ids:
                continue

            time_filter_sql = ""
            time_params: List[Any] = []
            if i == 0:
                time_filter_sql = "AND st.departure_time >= %s"
                time_params.append(now_time_str)

            route_filter_sql = ""
            route_params: List[Any] = []
            if route_id:
                route_filter_sql = "AND r.route_id = %s"
                route_params.append(route_id)

            dir_filter_sql = ""
            dir_params: List[Any] = []
            if direction_id is not None:
                dir_filter_sql = "AND t.direction_id = %s"
                dir_params.append(int(direction_id))

            sql = f"""
            SELECT
              %s::text AS service_date,
              st.stop_id,
              st.trip_id,
              st.arrival_time,
              st.departure_time,
              st.stop_sequence,

              t.route_id,
              t.service_id,
              t.direction_id,
              t.trip_headsign,

              r.route_short_name,
              r.route_long_name,
              r.route_type,
              r.agency_id
            FROM gtfs.stop_times st
            JOIN gtfs.trips t ON t.trip_id = st.trip_id
            JOIN gtfs.routes r ON r.route_id = t.route_id
            WHERE st.stop_id = %s
              AND t.service_id = ANY(%s)
              {time_filter_sql}
              {route_filter_sql}
              {dir_filter_sql}
            ORDER BY st.departure_time ASC
            LIMIT %s;
            """

            params: List[Any] = []
            params.append(d.isoformat())
            params.append(stop_id)
            params.append(service_ids)
            params.extend(time_params)
            params.extend(route_params)
            params.extend(dir_params)
            params.append(limit)

            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()

            if rows:
                items: List[Dict[str, Any]] = []
                for r in rows:
                    service_d = date.fromisoformat(r["service_date"])
                    try:
                        dep_dt = gtfs_time_to_dt(service_d, r["departure_time"], tzinfo)
                    except Exception:
                        dep_dt = None
                    try:
                        arr_dt = gtfs_time_to_dt(service_d, r["arrival_time"], tzinfo)
                    except Exception:
                        arr_dt = None

                    items.append(
                        {
                            "service_date": r["service_date"],
                            "stop_id": r["stop_id"],
                            "trip_id": r["trip_id"],
                            "route_id": r["route_id"],
                            "service_id": r["service_id"],
                            "direction_id": r["direction_id"],
                            "trip_headsign": r["trip_headsign"],
                            "departure_time": r["departure_time"],
                            "arrival_time": r["arrival_time"],
                            "departure_dt": dep_dt.isoformat() if dep_dt else None,
                            "arrival_dt": arr_dt.isoformat() if arr_dt else None,
                            "stop_sequence": int(r["stop_sequence"]) if r["stop_sequence"] is not None else None,
                            "route_short_name": r["route_short_name"],
                            "route_long_name": r["route_long_name"],
                            "route_type": r["route_type"],
                            "agency_id": r.get("agency_id"),
                        }
                    )

                return {
                    "stop_id": stop_id,
                    "route_id": route_id,
                    "direction_id": direction_id,
                    "tz": tz,
                    "now": now.isoformat(),
                    "searched_days": i + 1,
                    "count": len(items),
                    "items": items,
                }

    return {"stop_id": stop_id, "route_id": route_id, "direction_id": direction_id, "tz": tz, "count": 0, "items": []}

# -----------------------------
# Route Shape (representative = most common shape_id)
# -----------------------------
@app.get("/api/routes/{route_id}/shape")
def route_shape(
    route_id: str,
    direction_id: Optional[int] = Query(None, description="0 or 1 (optional)"),
):
    pick_sql = """
    SELECT
      t.shape_id,
      COUNT(*) AS n_trips
    FROM gtfs.trips t
    WHERE t.route_id = %s
      AND t.shape_id IS NOT NULL
    """
    params: List[Any] = [route_id]
    if direction_id is not None:
        pick_sql += " AND t.direction_id = %s"
        params.append(int(direction_id))

    pick_sql += """
    GROUP BY t.shape_id
    ORDER BY n_trips DESC, t.shape_id
    LIMIT 1;
    """

    geom_sql = """
    SELECT
      sl.shape_id,
      ST_AsGeoJSON(sl.geom)::json AS geojson
    FROM gtfs.shape_lines sl
    WHERE sl.shape_id = %s
    LIMIT 1;
    """

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(pick_sql, params)
            pick = cur.fetchone()

            if not pick:
                return JSONResponse(
                    status_code=404,
                    content={"route_id": route_id, "direction_id": direction_id, "error": "no shape_id found in trips"},
                )

            shape_id = pick["shape_id"]
            cur.execute(geom_sql, (shape_id,))
            row = cur.fetchone()

    if not row or row["geojson"] is None:
        return JSONResponse(
            status_code=404,
            content={"route_id": route_id, "direction_id": direction_id, "shape_id": shape_id, "error": "shape geometry not found"},
        )

    return {
        "route_id": route_id,
        "direction_id": direction_id,
        "shape_id": shape_id,
        "shape_trip_count": int(pick["n_trips"]),
        "type": "Feature",
        "geometry": row["geojson"],
        "properties": {"route_id": route_id, "direction_id": direction_id},
    }

# -----------------------------
# Trip Stops (stop sequence)
# -----------------------------
@app.get("/api/trips/{trip_id}/stops")
def trip_stops(trip_id: str):
    sql = """
    SELECT
      st.stop_sequence,
      st.stop_id,
      s.stop_name AS stop_name,
      s.stop_lat::double precision AS lat,
      s.stop_lon::double precision AS lon,
      st.arrival_time,
      st.departure_time
    FROM gtfs.stop_times st
    JOIN gtfs.stops s ON s.stop_id = st.stop_id
    WHERE st.trip_id = %s
    ORDER BY st.stop_sequence ASC;
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (trip_id,))
            rows = cur.fetchall()

    return {"trip_id": trip_id, "count": len(rows), "items": rows}

# -----------------------------
# Route Stops (representative trip)
# -----------------------------
@app.get("/api/routes/{route_id}/stops")
def route_stops(
    route_id: str,
    direction_id: Optional[int] = Query(None, description="0 or 1 (optional)"),
):
    trip_sql = """
    SELECT t.trip_id
    FROM gtfs.trips t
    JOIN gtfs.stop_times st ON st.trip_id = t.trip_id
    WHERE t.route_id = %s
    """
    params: List[Any] = [route_id]
    if direction_id is not None:
        trip_sql += " AND t.direction_id = %s"
        params.append(int(direction_id))

    trip_sql += """
    GROUP BY t.trip_id
    ORDER BY COUNT(*) DESC
    LIMIT 1;
    """

    stops_sql = """
    SELECT
      st.stop_sequence,
      st.stop_id,
      s.stop_name AS stop_name,
      s.stop_lat::double precision AS lat,
      s.stop_lon::double precision AS lon
    FROM gtfs.stop_times st
    JOIN gtfs.stops s ON s.stop_id = st.stop_id
    WHERE st.trip_id = %s
    ORDER BY st.stop_sequence ASC;
    """

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(trip_sql, params)
            trip = cur.fetchone()

            if not trip:
                return JSONResponse(
                    status_code=404,
                    content={"route_id": route_id, "direction_id": direction_id, "error": "no trip found"},
                )

            rep_trip_id = trip["trip_id"]
            cur.execute(stops_sql, (rep_trip_id,))
            rows = cur.fetchall()

    return {
        "route_id": route_id,
        "direction_id": direction_id,
        "representative_trip_id": rep_trip_id,
        "count": len(rows),
        "items": rows,
    }

# -----------------------------
# Network Coverage (bbox)
# -----------------------------
@app.get("/api/network/coverage")
def network_coverage():
    sql = """
    SELECT
      COUNT(*)::bigint AS stop_count,
      MIN(stop_lon)::double precision AS min_lon,
      MIN(stop_lat)::double precision AS min_lat,
      MAX(stop_lon)::double precision AS max_lon,
      MAX(stop_lat)::double precision AS max_lat
    FROM gtfs.stops
    WHERE stop_lon IS NOT NULL AND stop_lat IS NOT NULL;
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            r = cur.fetchone()

    return {
        "stop_count": int(r["stop_count"] or 0),
        "bbox": [r["min_lon"], r["min_lat"], r["max_lon"], r["max_lat"]],
    }

# -----------------------------
# Route Plan (Direct-only, calendar_dates supported) - days_ahead auto-search
# -----------------------------
@app.get("/api/route/plan")
def route_plan(
    origin_stop_id: str = Query(...),
    dest_stop_id: str = Query(...),
    depart_dt: Optional[str] = Query(None, description="ISO datetime, e.g. 2026-01-29T08:30:00"),
    tz: str = Query("Asia/Bangkok"),
    limit: int = Query(3, ge=1, le=10),
    days_ahead: int = Query(30, ge=1, le=30, description="Auto-search next service day if no trips on selected day"),
):
    try:
        z = ZoneInfo(tz)
    except Exception:
        return JSONResponse(status_code=400, content={"error": f"Invalid tz: {tz}"})

    now = datetime.now(z)
    if depart_dt:
        try:
            depart = datetime.fromisoformat(depart_dt)
            if depart.tzinfo is None:
                depart = depart.replace(tzinfo=z)
            else:
                depart = depart.astimezone(z)
        except Exception:
            return JSONResponse(status_code=400, content={"error": "depart_dt must be ISO datetime"})
    else:
        depart = now

    base_service_day = depart.date()
    base_depart_time_str = depart.strftime("%H:%M:%S")

    sql_tpl = """
    WITH candidates AS (
      SELECT
        t.trip_id,
        t.route_id,
        t.service_id,
        t.direction_id,
        t.trip_headsign,
        st_o.departure_time AS depart_time,
        st_d.arrival_time   AS arrive_time,
        st_o.stop_sequence  AS o_seq,
        st_d.stop_sequence  AS d_seq
      FROM gtfs.trips t
      JOIN gtfs.stop_times st_o ON st_o.trip_id = t.trip_id AND st_o.stop_id = %s
      JOIN gtfs.stop_times st_d ON st_d.trip_id = t.trip_id AND st_d.stop_id = %s
      WHERE st_d.stop_sequence > st_o.stop_sequence
        AND t.service_id = ANY(%s)
        {time_filter_sql}
    )
    SELECT
      c.*,
      r.route_short_name,
      r.route_long_name,
      r.route_type,
      r.agency_id
    FROM candidates c
    JOIN gtfs.routes r ON r.route_id = c.route_id
    ORDER BY c.depart_time ASC
    LIMIT %s;
    """

    with get_conn() as conn:
        last_note = ""
        for i in range(days_ahead):
            service_day = base_service_day + timedelta(days=i)

            service_ids = _service_ids_for_date(conn, service_day)
            if not service_ids:
                last_note = "No active service_ids for this date (calendar/calendar_dates)."
                continue

            time_filter_sql = "AND st_o.departure_time >= %s"
            time_param = base_depart_time_str if i == 0 else "00:00:00"
            sql = sql_tpl.format(time_filter_sql=time_filter_sql)

            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, (origin_stop_id, dest_stop_id, service_ids, time_param, limit))
                rows = cur.fetchall()

            if not rows:
                last_note = "No matching trips found for this date/time window."
                continue

            items = []
            for r in rows:
                dep_dt = gtfs_time_to_dt(service_day, r["depart_time"], z)
                arr_dt = gtfs_time_to_dt(service_day, r["arrive_time"], z)
                items.append({**r, "depart_dt": dep_dt.isoformat(), "arrive_dt": arr_dt.isoformat()})

            note = "direct-only (no transfers yet), calendar_dates supported"
            if i > 0:
                note += f" | auto-shifted to next service day (+{i}d)"

            return {
                "origin_stop_id": origin_stop_id,
                "dest_stop_id": dest_stop_id,
                "tz": tz,
                "now": now.isoformat(),
                "depart_dt": depart.isoformat(),
                "service_date": service_day.isoformat(),
                "searched_days": i + 1,
                "count": len(items),
                "items": items,
                "note": note,
            }

    return {
        "origin_stop_id": origin_stop_id,
        "dest_stop_id": dest_stop_id,
        "tz": tz,
        "now": now.isoformat(),
        "depart_dt": depart.isoformat(),
        "service_date": base_service_day.isoformat(),
        "searched_days": days_ahead,
        "count": 0,
        "items": [],
        "note": f"No routes found within {days_ahead} day(s). Last: {last_note}",
    }

# -----------------------------
# MVP: Walk + 1 Transfer planner - UPDATED: prefer rail/BTS + ranking + dedup + debug + days_ahead
# -----------------------------
@app.get("/api/route/plan1x")
def route_plan_one_transfer(
    origin_lat: float = Query(...),
    origin_lon: float = Query(...),
    dest_lat: float = Query(...),
    dest_lon: float = Query(...),

    # Optional: if frontend already selected specific stops, pass them to avoid random nearby
    origin_stop_id: Optional[str] = Query(None, description="Optional: force origin stop_id (from UI selection)"),
    dest_stop_id: Optional[str] = Query(None, description="Optional: force destination stop_id (from UI selection)"),

    walk_radius_m: int = Query(800, ge=100, le=5000),
    max_nearby_stops: int = Query(15, ge=1, le=50, description="Use only top-N closest nearby stops"),
    transfer_wait_min: int = Query(5, ge=0, le=60),

    # Filters (defaults prefer BTS rail)
    route_type_whitelist: List[int] = Query([0], description="Prefer/limit route_type. Default [0]=rail"),
    agency_whitelist: Optional[str] = Query("BTSC", description="Comma list of agency_id to allow. Default BTSC. Use empty to disable."),
    enforce_filters: bool = Query(True, description="If true, filter strictly; if false, just rank preferences."),

    depart_dt: Optional[str] = Query(None, description="ISO datetime, e.g. 2026-01-29T08:30:00"),
    tz: str = Query("Asia/Bangkok"),
    limit: int = Query(3, ge=1, le=20),
    days_ahead: int = Query(30, ge=1, le=30, description="Auto-search next service day if no trips on selected day"),
):
    try:
        z = ZoneInfo(tz)
    except Exception:
        return JSONResponse(status_code=400, content={"error": f"Invalid tz: {tz}"})

    now = datetime.now(z)
    if depart_dt:
        try:
            depart = datetime.fromisoformat(depart_dt)
            if depart.tzinfo is None:
                depart = depart.replace(tzinfo=z)
            else:
                depart = depart.astimezone(z)
        except Exception:
            return JSONResponse(status_code=400, content={"error": "depart_dt must be ISO datetime"})
    else:
        depart = now

    base_service_day = depart.date()
    base_depart_time_str = depart.strftime("%H:%M:%S")

    agency_list = _parse_csv_param_list(agency_whitelist)
    if agency_list is None:
        agency_list = ["BTSC"]
    # if user sets empty string => disable agency filtering
    agency_filter_enabled = len(agency_list) > 0

    # Nearby (include name + dist for debug and scoring)
    nearby_sql = """
    SELECT
      stop_id,
      stop_name,
      ST_Distance(
        geom::geography,
        ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography
      ) AS dist_m
    FROM gtfs.stops_search
    WHERE ST_DWithin(
      geom::geography,
      ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography,
      %s
    )
    ORDER BY dist_m ASC
    LIMIT %s;
    """

    # Direct + scoring by walking distance (join dist from temp tables)
    direct_sql_tpl = """
    WITH o_near AS (
      SELECT * FROM (VALUES {o_values}) AS v(stop_id, o_dist)
    ),
    d_near AS (
      SELECT * FROM (VALUES {d_values}) AS v(stop_id, d_dist)
    ),
    o AS (
      SELECT st.trip_id, st.stop_id AS o_stop, st.stop_sequence AS o_seq, st.departure_time AS o_dep, o_near.o_dist
      FROM gtfs.stop_times st
      JOIN gtfs.trips t ON t.trip_id = st.trip_id
      JOIN o_near ON o_near.stop_id = st.stop_id
      WHERE t.service_id = ANY(%s)
        AND st.departure_time >= %s
    ),
    d AS (
      SELECT st.trip_id, st.stop_id AS d_stop, st.stop_sequence AS d_seq, st.arrival_time AS d_arr, d_near.d_dist
      FROM gtfs.stop_times st
      JOIN d_near ON d_near.stop_id = st.stop_id
    )
    SELECT
      o.trip_id, t.route_id, t.direction_id, t.trip_headsign, t.service_id,
      o.o_stop, d.d_stop, o.o_dep, d.d_arr, o.o_seq, d.d_seq,
      o.o_dist, d.d_dist,
      r.route_short_name, r.route_long_name, r.route_type, r.agency_id
    FROM o
    JOIN d ON d.trip_id = o.trip_id AND d.d_seq > o.o_seq
    JOIN gtfs.trips t ON t.trip_id = o.trip_id
    JOIN gtfs.routes r ON r.route_id = t.route_id
    {where_filters}
    ORDER BY
      r.route_type ASC,
      o.o_dep ASC,
      (o.o_dist + d.d_dist) ASC
    LIMIT %s;
    """

    one_xfer_sql_tpl = """
    WITH o_near AS (
      SELECT * FROM (VALUES {o_values}) AS v(stop_id, o_dist)
    ),
    d_near AS (
      SELECT * FROM (VALUES {d_values}) AS v(stop_id, d_dist)
    ),
    leg1 AS (
      SELECT
        st1.trip_id AS trip1,
        st1.stop_id AS o_stop,
        stx.stop_id AS x_stop,
        st1.stop_sequence AS o_seq,
        stx.stop_sequence AS x_seq,
        st1.departure_time AS o_dep,
        stx.arrival_time   AS x_arr,
        o_near.o_dist
      FROM gtfs.stop_times st1
      JOIN gtfs.trips t1 ON t1.trip_id = st1.trip_id
      JOIN gtfs.stop_times stx ON stx.trip_id = st1.trip_id
      JOIN o_near ON o_near.stop_id = st1.stop_id
      WHERE t1.service_id = ANY(%s)
        AND st1.departure_time >= %s
        AND stx.stop_sequence > st1.stop_sequence
    ),
    leg2 AS (
      SELECT
        stx2.trip_id AS trip2,
        stx2.stop_id AS x_stop,
        std.stop_id  AS d_stop,
        stx2.stop_sequence AS x2_seq,
        std.stop_sequence  AS d_seq,
        stx2.departure_time AS x_dep2,
        std.arrival_time    AS d_arr2,
        d_near.d_dist
      FROM gtfs.stop_times stx2
      JOIN gtfs.trips t2 ON t2.trip_id = stx2.trip_id
      JOIN gtfs.stop_times std ON std.trip_id = stx2.trip_id
      JOIN d_near ON d_near.stop_id = std.stop_id
      WHERE t2.service_id = ANY(%s)
        AND std.stop_sequence > stx2.stop_sequence
    )
    SELECT
      l1.trip1, t1.route_id AS route1, t1.direction_id AS dir1, t1.trip_headsign AS head1, t1.service_id AS sid1,
      l2.trip2, t2.route_id AS route2, t2.direction_id AS dir2, t2.trip_headsign AS head2, t2.service_id AS sid2,
      l1.o_stop, l1.x_stop, l2.d_stop,
      l1.o_dep, l1.x_arr, l2.x_dep2, l2.d_arr2,
      l1.o_dist, l2.d_dist,
      r1.route_short_name AS r1_short, r1.route_long_name AS r1_long, r1.route_type AS r1_type, r1.agency_id AS r1_agency,
      r2.route_short_name AS r2_short, r2.route_long_name AS r2_long, r2.route_type AS r2_type, r2.agency_id AS r2_agency
    FROM leg1 l1
    JOIN leg2 l2 ON l2.x_stop = l1.x_stop
    JOIN gtfs.trips t1 ON t1.trip_id = l1.trip1
    JOIN gtfs.trips t2 ON t2.trip_id = l2.trip2
    JOIN gtfs.routes r1 ON r1.route_id = t1.route_id
    JOIN gtfs.routes r2 ON r2.route_id = t2.route_id
    WHERE l2.x_dep2 >= l1.x_arr
    {where_filters}
    ORDER BY
      LEAST(r1.route_type, r2.route_type) ASC,
      l1.o_dep ASC,
      (l1.o_dist + l2.d_dist) ASC,
      l2.x_dep2 ASC
    LIMIT %s;
    """

    def _build_values_sql(pairs: List[Tuple[str, float]]) -> str:
        # Creates: ('22', 10.0),('12', 20.0)
        # Safe: stop_id comes from DB (gtfs), distance float.
        chunks = []
        for sid, dist in pairs:
            sid_str = str(sid).replace("'", "''")
            chunks.append(f"('{sid_str}', {float(dist)})")
        return ",".join(chunks) if chunks else "('0', 1e12)"

    with get_conn() as conn:
        # Build nearby lists once (independent from service day)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # origin nearby
            cur.execute(nearby_sql, (origin_lon, origin_lat, origin_lon, origin_lat, walk_radius_m, 50))
            o_rows = cur.fetchall()
            # dest nearby
            cur.execute(nearby_sql, (dest_lon, dest_lat, dest_lon, dest_lat, walk_radius_m, 50))
            d_rows = cur.fetchall()

        # If UI provides explicit stops, force them (distance=0 preferred)
        if origin_stop_id:
            origin_stop_id = origin_stop_id.strip()
        if dest_stop_id:
            dest_stop_id = dest_stop_id.strip()

        origin_pairs: List[Tuple[str, float]] = []
        dest_pairs: List[Tuple[str, float]] = []

        if origin_stop_id:
            origin_pairs = [(origin_stop_id, 0.0)]
        else:
            origin_pairs = [(r["stop_id"], float(r["dist_m"])) for r in o_rows[:max_nearby_stops]]

        if dest_stop_id:
            dest_pairs = [(dest_stop_id, 0.0)]
        else:
            dest_pairs = [(r["stop_id"], float(r["dist_m"])) for r in d_rows[:max_nearby_stops]]

        if not origin_pairs or not dest_pairs:
            return {
                "count": 0,
                "items": [],
                "note": "No nearby stops for origin or destination within walk radius.",
                "origin_nearby_count": len(origin_pairs),
                "dest_nearby_count": len(dest_pairs),
            }

        # Debug nearby objects
        origin_nearby_debug = []
        if origin_stop_id:
            origin_nearby_debug = [{"stop_id": origin_stop_id, "name": None, "dist_m": 0.0, "forced": True}]
        else:
            # keep names from query
            for r in o_rows[:max_nearby_stops]:
                origin_nearby_debug.append({"stop_id": r["stop_id"], "name": r["stop_name"], "dist_m": float(r["dist_m"]), "forced": False})

        dest_nearby_debug = []
        if dest_stop_id:
            dest_nearby_debug = [{"stop_id": dest_stop_id, "name": None, "dist_m": 0.0, "forced": True}]
        else:
            for r in d_rows[:max_nearby_stops]:
                dest_nearby_debug.append({"stop_id": r["stop_id"], "name": r["stop_name"], "dist_m": float(r["dist_m"]), "forced": False})

        # Build SQL VALUES
        o_values = _build_values_sql(origin_pairs)
        d_values = _build_values_sql(dest_pairs)

        # Filters
        where_filters = ""
        params_filters: List[Any] = []
        if enforce_filters:
            clauses = []
            if route_type_whitelist:
                clauses.append("r.route_type = ANY(%s)")
                params_filters.append(route_type_whitelist)
            if agency_filter_enabled:
                clauses.append("r.agency_id = ANY(%s)")
                params_filters.append(agency_list)
            if clauses:
                where_filters = "WHERE " + " AND ".join(clauses)

        last_note = ""
        for i in range(days_ahead):
            service_day = base_service_day + timedelta(days=i)
            service_ids = _service_ids_for_date(conn, service_day)
            if not service_ids:
                last_note = "No active service_ids for this date (calendar/calendar_dates)."
                continue

            time_str = base_depart_time_str if i == 0 else "00:00:00"

            items: List[Dict[str, Any]] = []

            # DIRECT
            direct_sql = direct_sql_tpl.format(o_values=o_values, d_values=d_values, where_filters=where_filters)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                params = [service_ids, time_str]
                params.extend(params_filters)
                params.append(limit * 10)  # pull more then dedup/rank in python
                cur.execute(direct_sql, params)
                direct_rows = cur.fetchall()

            for r in direct_rows:
                dep_dt = gtfs_time_to_dt(service_day, r["o_dep"], z)
                arr_dt = gtfs_time_to_dt(service_day, r["d_arr"], z)
                items.append(
                    {
                        "type": "direct",
                        "trip_id": r["trip_id"],
                        "route_id": r["route_id"],
                        "direction_id": r["direction_id"],
                        "trip_headsign": r["trip_headsign"],
                        "service_id": r["service_id"],
                        "origin_stop_id": r["o_stop"],
                        "dest_stop_id": r["d_stop"],
                        "depart_time": r["o_dep"],
                        "arrive_time": r["d_arr"],
                        "depart_dt": dep_dt.isoformat(),
                        "arrive_dt": arr_dt.isoformat(),
                        "route_short_name": r["route_short_name"],
                        "route_long_name": r["route_long_name"],
                        "route_type": r["route_type"],
                        "agency_id": r.get("agency_id"),
                        "walk_origin_m": float(r["o_dist"]) if r.get("o_dist") is not None else None,
                        "walk_dest_m": float(r["d_dist"]) if r.get("d_dist") is not None else None,
                        "walk_total_m": (float(r["o_dist"]) + float(r["d_dist"])) if (r.get("o_dist") is not None and r.get("d_dist") is not None) else None,
                    }
                )

            # 1-TRANSFER
            one_xfer_sql = one_xfer_sql_tpl.format(o_values=o_values, d_values=d_values, where_filters=where_filters.replace("r.", "r1.") if where_filters else "")
            # Note: for transfer case we applied filter only to route1 (r1) to keep SQL simple.
            # If you want strict filter on both legs, we can extend it later.
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                params = [service_ids, time_str, service_ids]
                # reuse same params_filters but mapped to r1 (so same lists ok)
                params.extend(params_filters)
                params.append(limit * 10)
                cur.execute(one_xfer_sql, params)
                x_rows = cur.fetchall()

            for r in x_rows:
                dep1 = gtfs_time_to_dt(service_day, r["o_dep"], z)
                arr1 = gtfs_time_to_dt(service_day, r["x_arr"], z)
                dep2 = gtfs_time_to_dt(service_day, r["x_dep2"], z)
                arr2 = gtfs_time_to_dt(service_day, r["d_arr2"], z)

                if dep2 < (arr1 + timedelta(minutes=transfer_wait_min)):
                    continue

                items.append(
                    {
                        "type": "1-transfer",
                        "origin_stop_id": r["o_stop"],
                        "transfer_stop_id": r["x_stop"],
                        "dest_stop_id": r["d_stop"],
                        "walk_origin_m": float(r["o_dist"]) if r.get("o_dist") is not None else None,
                        "walk_dest_m": float(r["d_dist"]) if r.get("d_dist") is not None else None,
                        "walk_total_m": (float(r["o_dist"]) + float(r["d_dist"])) if (r.get("o_dist") is not None and r.get("d_dist") is not None) else None,
                        "leg1": {
                            "trip_id": r["trip1"],
                            "route_id": r["route1"],
                            "direction_id": r["dir1"],
                            "trip_headsign": r["head1"],
                            "service_id": r["sid1"],
                            "depart_time": r["o_dep"],
                            "arrive_time": r["x_arr"],
                            "depart_dt": dep1.isoformat(),
                            "arrive_dt": arr1.isoformat(),
                            "route_short_name": r["r1_short"],
                            "route_long_name": r["r1_long"],
                            "route_type": r["r1_type"],
                            "agency_id": r["r1_agency"],
                        },
                        "leg2": {
                            "trip_id": r["trip2"],
                            "route_id": r["route2"],
                            "direction_id": r["dir2"],
                            "trip_headsign": r["head2"],
                            "service_id": r["sid2"],
                            "depart_time": r["x_dep2"],
                            "arrive_time": r["d_arr2"],
                            "depart_dt": dep2.isoformat(),
                            "arrive_dt": arr2.isoformat(),
                            "route_short_name": r["r2_short"],
                            "route_long_name": r["r2_long"],
                            "route_type": r["r2_type"],
                            "agency_id": r["r2_agency"],
                        },
                    }
                )

            if items:
                # Dedup
                items = _dedup_keep_order(
                    items,
                    key_fn=lambda it: (
                        it.get("type"),
                        it.get("trip_id") if it.get("type") == "direct" else (it.get("leg1", {}).get("trip_id"), it.get("leg2", {}).get("trip_id")),
                        it.get("origin_stop_id"),
                        it.get("dest_stop_id"),
                        it.get("transfer_stop_id") if it.get("type") == "1-transfer" else None,
                    ),
                )

                # Rank in python as final safety:
                def score(it: Dict[str, Any]) -> Tuple[int, str, float]:
                    # lower is better
                    if it["type"] == "direct":
                        rt = int(it.get("route_type") if it.get("route_type") is not None else 999)
                        dep = str(it.get("depart_time") or "99:99:99")
                    else:
                        rt1 = int(it["leg1"].get("route_type") if it["leg1"].get("route_type") is not None else 999)
                        rt2 = int(it["leg2"].get("route_type") if it["leg2"].get("route_type") is not None else 999)
                        rt = min(rt1, rt2)
                        dep = str(it["leg1"].get("depart_time") or "99:99:99")
                    walk = float(it.get("walk_total_m") or 1e12)
                    return (rt, dep, walk)

                items.sort(key=score)

                note = "MVP: direct + up to 1 transfer (prefer rail/BTS; ranking+dedup applied)"
                if i > 0:
                    note += f" | auto-shifted to next service day (+{i}d)"

                return {
                    "tz": tz,
                    "now": now.isoformat(),
                    "depart_dt": depart.isoformat(),
                    "service_date": service_day.isoformat(),
                    "searched_days": i + 1,
                    "origin": {"lat": origin_lat, "lon": origin_lon, "stop_id": origin_stop_id},
                    "destination": {"lat": dest_lat, "lon": dest_lon, "stop_id": dest_stop_id},
                    "walk_radius_m": walk_radius_m,
                    "max_nearby_stops": max_nearby_stops,
                    "transfer_wait_min": transfer_wait_min,
                    "filters": {
                        "route_type_whitelist": route_type_whitelist,
                        "agency_whitelist": agency_list if agency_filter_enabled else [],
                        "enforce_filters": enforce_filters,
                    },
                    "debug_nearby": {
                        "origin_nearby_count": len(o_rows),
                        "dest_nearby_count": len(d_rows),
                        "origin_used": origin_nearby_debug,
                        "dest_used": dest_nearby_debug,
                    },
                    "count": len(items[:limit]),
                    "items": items[:limit],
                    "note": note,
                }

            last_note = "No matching trips found for this date/time window."

    return {
        "tz": tz,
        "now": now.isoformat(),
        "depart_dt": depart.isoformat(),
        "service_date": base_service_day.isoformat(),
        "searched_days": days_ahead,
        "origin": {"lat": origin_lat, "lon": origin_lon, "stop_id": origin_stop_id},
        "destination": {"lat": dest_lat, "lon": dest_lon, "stop_id": dest_stop_id},
        "walk_radius_m": walk_radius_m,
        "max_nearby_stops": max_nearby_stops,
        "transfer_wait_min": transfer_wait_min,
        "filters": {
            "route_type_whitelist": route_type_whitelist,
            "agency_whitelist": agency_list if agency_filter_enabled else [],
            "enforce_filters": enforce_filters,
        },
        "debug_nearby": {
            "origin_nearby_count": len(o_rows) if 'o_rows' in locals() else None,
            "dest_nearby_count": len(d_rows) if 'd_rows' in locals() else None,
        },
        "count": 0,
        "items": [],
        "note": f"No routes found within {days_ahead} day(s). Last: {last_note}",
    }
