import os
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="GTFS Transit API", version="1.0")

# --- CORS (ให้ Bolt.new เรียกได้) ---
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


def get_conn():
    # ช่วยให้ error อ่านง่าย หากตั้ง env ยังไม่ครบ
    if not all([DBHOST, DBNAME, DBUSER, DBPASS]):
        raise HTTPException(
            status_code=500,
            detail="DB env not set. Please set DBHOST, DBNAME, DBUSER, DBPASS (and optional DBPORT, PGSSLMODE) in Render.",
        )

    return psycopg2.connect(
        host=DBHOST,
        port=DBPORT,
        dbname=DBNAME,
        user=DBUSER,
        password=DBPASS,
        sslmode=PGSSLMODE,  # Render Postgres มัก require
    )


@app.get("/health")
def health():
    return {"ok": True}


# --- (A) endpoint หลัก (ตามของเดิม) ---
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
    - Else -> order by trigram similarity (pg_trgm)
    """
    q = (q or "").strip()
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

    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
    except psycopg2.Error as e:
        # ให้ข้อความ error สั้น ๆ (จะเห็นรายละเอียดใน Render logs อยู่แล้ว)
        raise HTTPException(status_code=500, detail=f"DB query failed: {e.pgerror or str(e)}")

    items = []
    for r in rows:
        items.append(
            {
                "stop_id": r["stop_id"],
                "name": r["name"],
                "lat": float(r["lat"]) if r["lat"] is not None else None,
                "lon": float(r["lon"]) if r["lon"] is not None else None,
                "dist_m": float(r["dist_m"]) if r["dist_m"] is not None else None,
            }
        )

    return {"query": q, "count": len(items), "items": items}


# --- (B) endpoint alias: ให้ Bolt.new เรียกสั้น ๆ ได้ ---
@app.get("/stops/autocomplete")
def autocomplete_stops_alias(
    q: str = Query(..., min_length=1),
    lat: Optional[float] = Query(None),
    lon: Optional[float] = Query(None),
    radius_m: int = Query(30000, ge=500, le=200000),
    limit: int = Query(10, ge=1, le=30),
):
    return autocomplete_stops(q=q, lat=lat, lon=lon, radius_m=radius_m, limit=limit)

@app.get("/")
def root():
    return {"service": "GTFS Transit API", "status": "ok", "docs": "/docs"}

from fastapi import HTTPException

@app.get("/api/routes/for_stop")
def routes_for_stop(
    stop_id: str = Query(..., min_length=1, description="GTFS stop_id"),
    limit: int = Query(50, ge=1, le=500, description="Max routes returned"),
):
    """
    List routes that serve a given stop_id.

    Requires GTFS tables:
      - gtfs.stop_times(stop_id, trip_id, ...)
      - gtfs.trips(trip_id, route_id, ...)
      - gtfs.routes(route_id, route_short_name, route_long_name, route_type, agency_id, ...)
    """
    stop_id = stop_id.strip()
    if not stop_id:
        raise HTTPException(status_code=400, detail="stop_id is required")

    sql = """
    SELECT DISTINCT
      r.route_id,
      COALESCE(NULLIF(r.route_short_name,''), r.route_id) AS route_short_name,
      r.route_long_name,
      r.route_type,
      r.agency_id
    FROM gtfs.stop_times st
    JOIN gtfs.trips t  ON t.trip_id = st.trip_id
    JOIN gtfs.routes r ON r.route_id = t.route_id
    WHERE st.stop_id = %s
    ORDER BY route_short_name, r.route_long_name NULLS LAST
    LIMIT %s;
    """
    params = (stop_id, limit)

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    items = []
    for r in rows:
        items.append(
            {
                "route_id": r["route_id"],
                "route_short_name": r["route_short_name"],
                "route_long_name": r["route_long_name"],
                "route_type": r["route_type"],
                "agency_id": r["agency_id"],
            }
        )

    return {"stop_id": stop_id, "count": len(items), "items": items}

from datetime import datetime
from fastapi import HTTPException

@app.get("/api/trips/next")
def next_trips(
    stop_id: str = Query(..., description="GTFS stop_id"),
    route_id: Optional[str] = Query(None, description="Filter by route_id"),
    direction_id: Optional[int] = Query(None, ge=0, le=1, description="0/1"),
    limit: int = Query(5, ge=1, le=50, description="Max results"),
    tz: str = Query("Asia/Bangkok", description="IANA timezone name"),
):
    """
    Next scheduled departures at a stop (no realtime).
    Uses GTFS tables:
      - gtfs.stop_times(stop_id, trip_id, arrival_time, departure_time, stop_sequence)
      - gtfs.trips(trip_id, route_id, service_id, trip_headsign, direction_id)
      - gtfs.routes(route_id, route_short_name, route_long_name, route_type, agency_id)
      - gtfs.calendar(service_id, monday..sunday, start_date, end_date)
      - gtfs.calendar_dates(service_id, date, exception_type)

    Handles GTFS times beyond 24:00:00 (e.g., 25:10:00).
    """

    stop_id = stop_id.strip()
    if not stop_id:
        raise HTTPException(status_code=400, detail="stop_id is required")

    # ---- SQL pieces ----
    # Convert 'HH:MM:SS' (HH may be > 24) to seconds
    time_to_sec_sql = """
      (split_part(%s, ':', 1)::int * 3600) +
      (split_part(%s, ':', 2)::int * 60) +
      (split_part(%s, ':', 3)::int)
    """

    # NOTE: We use DB time (now()) at requested timezone.
    # Compute "service date" = local date at tz.
    sql = f"""
    WITH
    params AS (
      SELECT
        %s::text   AS stop_id,
        %s::text   AS route_id,
        %s::int    AS direction_id,
        %s::int    AS limit_n,
        %s::text   AS tz
    ),
    now_local AS (
      SELECT
        (now() AT TIME ZONE (SELECT tz FROM params)) AS now_ts,
        (now() AT TIME ZONE (SELECT tz FROM params))::date AS svc_date
    ),

    -- Active service_ids for today from calendar + calendar_dates
    base_services AS (
      SELECT c.service_id
      FROM gtfs.calendar c, now_local n
      WHERE n.svc_date BETWEEN to_date(c.start_date::text, 'YYYYMMDD') AND to_date(c.end_date::text, 'YYYYMMDD')
        AND (
          CASE extract(dow from n.svc_date)
            WHEN 0 THEN c.sunday
            WHEN 1 THEN c.monday
            WHEN 2 THEN c.tuesday
            WHEN 3 THEN c.wednesday
            WHEN 4 THEN c.thursday
            WHEN 5 THEN c.friday
            WHEN 6 THEN c.saturday
          END
        ) = 1
    ),
    added_services AS (
      SELECT cd.service_id
      FROM gtfs.calendar_dates cd, now_local n
      WHERE cd.exception_type = 1
        AND to_date(cd.date::text, 'YYYYMMDD') = n.svc_date
    ),
    removed_services AS (
      SELECT cd.service_id
      FROM gtfs.calendar_dates cd, now_local n
      WHERE cd.exception_type = 2
        AND to_date(cd.date::text, 'YYYYMMDD') = n.svc_date
    ),
    active_services AS (
      SELECT service_id FROM base_services
      UNION
      SELECT service_id FROM added_services
      EXCEPT
      SELECT service_id FROM removed_services
    ),

    candidates AS (
      SELECT
        st.stop_id,
        st.trip_id,
        st.stop_sequence,
        st.arrival_time,
        st.departure_time,
        t.route_id,
        t.service_id,
        t.trip_headsign,
        t.direction_id,
        r.route_short_name,
        r.route_long_name,
        r.route_type,
        r.agency_id,

        -- seconds since service-day start (can be > 86400)
        (
          (split_part(st.departure_time, ':', 1)::int * 3600) +
          (split_part(st.departure_time, ':', 2)::int * 60) +
          (split_part(st.departure_time, ':', 3)::int)
        ) AS dep_sec,

        (
          (split_part(st.arrival_time, ':', 1)::int * 3600) +
          (split_part(st.arrival_time, ':', 2)::int * 60) +
          (split_part(st.arrival_time, ':', 3)::int)
        ) AS arr_sec

      FROM gtfs.stop_times st
      JOIN gtfs.trips t  ON t.trip_id = st.trip_id
      JOIN gtfs.routes r ON r.route_id = t.route_id
      JOIN active_services a ON a.service_id = t.service_id
      JOIN params p ON true
      WHERE st.stop_id = p.stop_id
        AND (p.route_id IS NULL OR t.route_id = p.route_id)
        AND (p.direction_id IS NULL OR t.direction_id = p.direction_id)
    ),

    ranked AS (
      SELECT
        c.*,
        n.now_ts,
        n.svc_date,
        -- Convert dep_sec to a local timestamp (svc_date + dep_sec)
        (n.svc_date::timestamp + make_interval(secs => c.dep_sec)) AS departure_ts,
        (n.svc_date::timestamp + make_interval(secs => c.arr_sec)) AS arrival_ts
      FROM candidates c
      CROSS JOIN now_local n
      WHERE (n.svc_date::timestamp + make_interval(secs => c.dep_sec)) >= n.now_ts
      ORDER BY (n.svc_date::timestamp + make_interval(secs => c.dep_sec)) ASC
      LIMIT (SELECT limit_n FROM params)
    )
    SELECT
      stop_id, trip_id, route_id, direction_id, trip_headsign,
      route_short_name, route_long_name, route_type, agency_id,
      stop_sequence, arrival_time, departure_time,
      departure_ts, arrival_ts
    FROM ranked;
    """

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                sql,
                (stop_id, route_id, direction_id, limit, tz),
            )
            rows = cur.fetchall()

    items = []
    for r in rows:
        items.append(
            {
                "trip_id": r["trip_id"],
                "route_id": r["route_id"],
                "route_short_name": r["route_short_name"],
                "route_long_name": r["route_long_name"],
                "route_type": r["route_type"],
                "agency_id": r["agency_id"],
                "direction_id": r["direction_id"],
                "trip_headsign": r["trip_headsign"],
                "stop_sequence": r["stop_sequence"],
                "arrival_time": r["arrival_time"],
                "departure_time": r["departure_time"],
                # ISO strings
                "arrival_ts": r["arrival_ts"].isoformat() if r["arrival_ts"] else None,
                "departure_ts": r["departure_ts"].isoformat() if r["departure_ts"] else None,
            }
        )

    return {
        "stop_id": stop_id,
        "route_id": route_id,
        "direction_id": direction_id,
        "tz": tz,
        "count": len(items),
        "items": items,
    }

