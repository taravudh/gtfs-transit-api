import os
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="GTFS Transit API", version="1.0")

# --- CORS (ให้ Bolt.new เรียกได้) ---
# ตั้ง ALLOWED_ORIGINS เป็นโดเมน frontend ของคุณภายหลังจะปลอดภัยกว่า
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

if not all([DBHOST, DBNAME, DBUSER, DBPASS]):
    # ไม่ raise ตอน import เพื่อให้ deploy ผ่านได้ แต่จะ error ตอนเรียก endpoint หากยังไม่ตั้ง env
    pass


def get_conn():
    # Render Postgres มักต้อง SSL
    return psycopg2.connect(
        host=DBHOST,
        port=DBPORT,
        dbname=DBNAME,
        user=DBUSER,
        password=DBPASS,
        sslmode=os.getenv("PGSSLMODE", "require"),
    )


@app.get("/health")
def health():
    return {"ok": True}


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
        # ต้องมี pg_trgm และ index trigram จะเร็วมาก
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

    # normalize output
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
