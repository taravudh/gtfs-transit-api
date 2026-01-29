import os
from typing import Optional, List, Dict, Any
from datetime import datetime, date, timedelta

from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="GTFS Transit API", version="1.1")

# --- CORS (ให้ Bolt.new / Netlify เรียกได้) ---
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in ALLOWED_ORIGINS if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- DB ENV (Render) ---
DBHOST = os.getenv("DBHOST")
DBPORT = int(os.getenv("DBPORT", "5432"))
DBNAME = os.getenv("DBNAME")
DBUSER = os.getenv("DBUSER")
DBPASS = os.getenv("DBPASS")
PGSSLMODE = os.getenv("PGSSLMODE", "require")

if not all([DBHOST, DBNAME, DBUSER, DBPASS]):
    # ไม่ raise ตอน import เพื่อให้ deploy ผ่านได้ แต่จะ error ตอนเรียก endpoint หากยังไม่ตั้ง env
    pass


def get_conn():
    return psycopg2.connect(
        host=DBHOST,
        port=DBPORT,
        dbname=DBNAME,
        user=DBUSER,
        password=DBPASS,
        sslmode=PGSSLMODE,
    )


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
    """
    Autocomplete stops from gtfs.stops_search.

    Requires table:
      gtfs.stops_search(stop_id, stop_name, lat, lon, geom)

    - If lat/lon provided -> filter by radius + order by distance
    - Else -> order by trigram similarity
    """
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
# Helper: service ids active on a given date (calendar + calendar_dates)
# -----------------------------
def _service_ids_for_date(conn, d: date) -> List[str]:
    """
    Return list of service_id active on date d.
    - calendar: weekday flags + start_date/end_date
    - calendar_dates: exception_type 1=add, 2=remove
    """
    ymd = d.strftime("%Y%m%d")
    # Python weekday: Monday=0 ... Sunday=6
    w = d.weekday()
    dow_col = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"][w]

    base_sql = f"""
    SELECT service_id
    FROM gtfs.calendar
    WHERE start_date <= %s AND end_date >= %s
      AND {dow_col} = 1
    """
    # exception adds/removes
    ex_sql = """
    SELECT service_id, exception_type
    FROM gtfs.calendar_dates
    WHERE date = %s
    """

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(base_sql, (ymd, ymd))
        base = {r["service_id"] for r in cur.fetchall()}

        # calendar_dates อาจไม่มีไฟล์ในบางชุด GTFS → ต้องกันพัง
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


# -----------------------------
# NEW: Next trips endpoint
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
    """
    Find next departures for a stop.
    - If no departures left today -> automatically search next service day (up to days_ahead).
    - Uses gtfs.calendar (+ gtfs.calendar_dates if exists).
    """
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
        # loop days to find the first day that has upcoming trips
        for i in range(days_ahead):
            d = today + timedelta(days=i)
            service_ids = _service_ids_for_date(conn, d)
            if not service_ids:
                continue

            # for today, only future times; for future days, take any time
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

            # NOTE: stop_times.departure_time เป็น text/HH:MM:SS ใน GTFS ส่วนใหญ่
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
            params.append(d.isoformat())            # service_date
            params.append(stop_id)                 # stop_id
            params.append(service_ids)             # ANY(array)
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
                    # build departure datetime in tz
                    dep_dt = datetime.fromisoformat(r["service_date"] + "T" + r["departure_time"]).replace(tzinfo=tzinfo)
                    arr_dt = datetime.fromisoformat(r["service_date"] + "T" + r["arrival_time"]).replace(tzinfo=tzinfo)

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
                            "departure_dt": dep_dt.isoformat(),
                            "arrival_dt": arr_dt.isoformat(),
                            "stop_sequence": int(r["stop_sequence"]) if r["stop_sequence"] is not None else None,
                            "route_short_name": r["route_short_name"],
                            "route_long_name": r["route_long_name"],
                            "route_type": r["route_type"],
                            "agency_id": r["agency_id"],
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

    # none found in days_ahead
    return {
        "stop_id": stop_id,
        "route_id": route_id,
        "direction_id": direction_id,
        "tz": tz,
        "count": 0,
        "items": [],
    }

from math import radians, cos

@app.get("/api/stops/nearby")
def nearby_stops(
    lat: float = Query(..., ge=-90, le=90, description="User latitude"),
    lon: float = Query(..., ge=-180, le=180, description="User longitude"),
    radius_m: int = Query(1000, ge=100, le=20000, description="Search radius (meters)"),
    limit: int = Query(20, ge=1, le=50, description="Max results"),
):
    """
    Find nearby stops within radius (meters), ordered by distance.

    Uses gtfs.stops_search.geom (EPSG:4326).
    """

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

    items = []
    for r in rows:
        items.append(
            {
                "stop_id": r["stop_id"],
                "name": r["name"],
                "lat": float(r["lat"]),
                "lon": float(r["lon"]),
                "dist_m": round(float(r["dist_m"]), 1),
            }
        )

    return {
        "lat": lat,
        "lon": lon,
        "radius_m": radius_m,
        "count": len(items),
        "items": items,
    }

