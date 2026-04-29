"""Fetch Luxembourg cycling infrastructure from OpenStreetMap into PostGIS.

Source: Overpass API (https://overpass-api.de/api/interpreter). Pulls every
way tagged as a cycleway, on-road cycle lane/track, designated bike path, or
proposed cycle facility within Luxembourg's bounding box; classifies each one
into 'segregated' / 'shared' / 'planned'; loads them into the
`cycling_infrastructure` table in EPSG:3035.

The raw Overpass response is cached at ingest/data/osm_cycling.json with a
7-day freshness window so repeated runs don't hammer the public Overpass
endpoint.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import geopandas as gpd
import requests
from shapely.geometry import LineString

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import PROJECTED_CRS, get_engine, get_psycopg2_conn  # noqa: E402

TABLE = "cycling_infrastructure"
DATA_DIR = Path(__file__).resolve().parent / "data"
CACHE_FILE = DATA_DIR / "osm_cycling.json"
CACHE_MAX_AGE_S = 7 * 86400  # 7 days

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
LU_BBOX = "49.4,5.7,50.2,6.6"  # south,west,north,east
OVERPASS_QUERY = f"""[out:json][timeout:60];
(
  way["highway"="cycleway"]({LU_BBOX});
  way["cycleway"="lane"]({LU_BBOX});
  way["cycleway"="track"]({LU_BBOX});
  way["highway"="path"]["bicycle"="designated"]({LU_BBOX});
  way["highway"="cycleway"]["state"="proposed"]({LU_BBOX});
  way["proposed:highway"="cycleway"]({LU_BBOX});
);
out geom;
"""


def cache_is_fresh() -> bool:
    if not CACHE_FILE.exists():
        return False
    age_s = time.time() - CACHE_FILE.stat().st_mtime
    return age_s < CACHE_MAX_AGE_S


def fetch_overpass() -> dict:
    """Fetch from Overpass, with on-disk cache. Returns the parsed JSON."""
    if cache_is_fresh():
        print(f"Using cached Overpass response: {CACHE_FILE}")
        return json.loads(CACHE_FILE.read_text(encoding="utf-8"))

    print("Fetching cycling infrastructure from Overpass API ...")
    # Overpass returns 406 for the default python-requests UA; identify the
    # client clearly per their etiquette guide.
    headers = {
        "User-Agent": "LuxBusCatchmentTool/1.0 (https://github.com/nicolaswinklercnc/LuxBusCatchmentTool)",
        "Accept": "application/json",
    }
    resp = requests.post(
        OVERPASS_URL,
        data={"data": OVERPASS_QUERY},
        headers=headers,
        timeout=120,
    )
    resp.raise_for_status()
    payload = resp.json()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_FILE.write_text(json.dumps(payload), encoding="utf-8")
    print(f"Cached Overpass response: {CACHE_FILE} "
          f"({CACHE_FILE.stat().st_size / 1024:.0f} KB)")
    return payload


def classify(tags: dict[str, str]) -> str | None:
    """Map an OSM way's tags to one of segregated/shared/planned, or None
    if the way doesn't fit any of our cycling categories.

    Rule order matters: planned first so a proposed cycleway doesn't get
    swallowed by the segregated rule.
    """
    highway = tags.get("highway")
    cycleway = tags.get("cycleway")
    bicycle = tags.get("bicycle")
    state = tags.get("state")
    proposed_highway = tags.get("proposed:highway")

    if (highway == "cycleway" and state == "proposed") \
            or proposed_highway == "cycleway":
        return "planned"
    if highway == "cycleway":
        return "segregated"
    if highway in ("path", "footway") and bicycle == "designated":
        return "segregated"
    if cycleway in ("lane", "track"):
        return "shared"
    return None


def parse_overpass(payload: dict) -> gpd.GeoDataFrame:
    """Convert Overpass JSON into a WGS84 GeoDataFrame of cycling features."""
    rows = []
    skipped = 0
    for el in payload.get("elements", []):
        if el.get("type") != "way":
            continue
        geom_pts = el.get("geometry") or []
        if len(geom_pts) < 2:
            skipped += 1
            continue
        tags = el.get("tags") or {}
        category = classify(tags)
        if category is None:
            skipped += 1
            continue
        line = LineString([(p["lon"], p["lat"]) for p in geom_pts])
        rows.append({
            "osm_id": int(el["id"]),
            "category": category,
            "highway": tags.get("highway"),
            "name": tags.get("name"),
            "surface": tags.get("surface"),
            "geom": line,
        })
    if skipped:
        print(f"Skipped {skipped} elements (no geometry or unmatched tags).")
    if not rows:
        raise RuntimeError("Overpass returned no usable cycling features.")
    gdf = gpd.GeoDataFrame(rows, geometry="geom", crs="EPSG:4326")
    return gdf.to_crs(PROJECTED_CRS)


def load_to_postgis(gdf: gpd.GeoDataFrame) -> int:
    engine = get_engine()
    gdf.to_postgis(TABLE, engine, if_exists="replace", index=False)
    with get_psycopg2_conn() as conn, conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {TABLE} ADD PRIMARY KEY (osm_id);")
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS {TABLE}_geom_idx "
            f"ON {TABLE} USING GIST(geom);"
        )
        conn.commit()
    return len(gdf)


def print_summary() -> None:
    """Per-category counts and km, computed from the loaded table."""
    with get_psycopg2_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT category, COUNT(*), COALESCE(SUM(ST_Length(geom)) / 1000.0, 0) "
            f"FROM {TABLE} GROUP BY category;"
        )
        per_cat = {row[0]: (int(row[1]), float(row[2])) for row in cur.fetchall()}
        cur.execute(
            f"SELECT COUNT(*), COALESCE(SUM(ST_Length(geom)) / 1000.0, 0) "
            f"FROM {TABLE};"
        )
        total_count, total_km = cur.fetchone()
    for cat in ("segregated", "shared", "planned"):
        c, km = per_cat.get(cat, (0, 0.0))
        print(f"{cat:<11}: {c:>4} features ({km:.1f} km)")
    print(f"{'total':<11}: {int(total_count):>4} features ({float(total_km):.1f} km)")


def main() -> int:
    try:
        payload = fetch_overpass()
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        body = exc.response.text[:500] if exc.response is not None else ""
        print(f"ERROR fetching Overpass: HTTP {status}: {exc}\n{body}",
              file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR fetching Overpass: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    try:
        gdf = parse_overpass(payload)
    except Exception as exc:
        print(f"ERROR parsing Overpass response: {exc}", file=sys.stderr)
        return 1

    n = load_to_postgis(gdf)
    print(f"{TABLE}: {n:,} features loaded")
    print_summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
