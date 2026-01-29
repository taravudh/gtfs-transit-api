# app.py
# ============================================================
# GTFS Transit API (FastAPI) - Single-file version (Updated)
# - CORS: supports ALLOWED_ORIGINS + ALLOWED_ORIGIN_REGEX
# - Better error visibility (global exception handler)
# - Safer stop_id compare (cast to text)
# ============================================================

import os
import logging
from typing import Optional, List, Dict, Any

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

app = FastAPI(title="GTFS Transit API", version="1.3")

# -----------------------------
# CORS (UPDATED)
# -----------------------------
def _split_env_csv(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default) or ""
    return [x.strip() for x in raw.split(",") if x.strip()]

ALLOWED_ORIGINS_LIST = _split_env_csv("ALLOWED_ORIGINS", "")  # e.g. "http://localhost:5173,https://xxx.netlify.app"
ALLOWED_ORIGIN_REGEX = (os.getenv("ALLOWED_ORIGIN_REGEX", "") or "").strip()  # e.g. r"^https://.*\.netlify\.app$"

# If nothing set, default to "*" (dev-friendly)
if not ALLOWED_ORIGINS_LIST and not ALLOWED_ORIGIN_REGEX:
    ALLOWED_ORIGINS_LIST = ["*"]

# CORS credentials note:
# If allow_origins is ["*"], browsers don't like allow_credentials=True. We set False in that case.
allow_credentials = False if (len(ALLOWED_ORIGINS_LIST) == 1 and ALLOWED_ORIGINS_LIST[0] == "*") else True

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS_LIST,
    allow_origin_regex=ALLOWED_ORIGIN_REGEX or None,
    allow_credentials=allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/debug/cors")
def debug_cors():
    return {
        "ALLOWED_ORIGINS": ALLOWED_ORIGINS_LIST,
        "ALLOWED_ORIGIN_REGEX": ALLOWED_ORIGIN_REGEX,
        "allow_credentials": allow_credentials,
    }

# -----------------------------
# Global exception handler (UPDATED)
# -----------------------------
@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error on %s %s", request.method, request.url)
    # Return JSON so you can see error quickly (and CORS middleware can attach headers)
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

    h = int(parts[0]); m = int(parts[1]); sec = int(parts[2])
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
# Routes for Stop (UPDATED: cast stop_id to text)
# -----------------------------
@app.get("/api/stops/{stop_id}/routes")
def routes_for_stop(
    stop_id: str,
    limit: int = Query(50, ge=1, le=200),
):
    sql = """
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
    ORDER BY
      r.route_type,
      COALESCE(r.route_short_name, r.route_id::text),
      r.route_id
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
# (ส่วนที่เหลือของคุณ: next_trips, route_shape, trip_stops, route_stops, coverage, plan, plan1x)
# ให้ “คงเดิม” ตามไฟล์เดิมได้เลย
# -----------------------------
