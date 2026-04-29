"""Fetch cycling infrastructure from OSM + three official Luxembourg sources
into PostGIS.

Sources:
  - OpenStreetMap via Overpass API (segregated/shared/planned by tag)
  - data.public.lu national cycling paths (CKAN-discovered GeoJSON)
  - data.public.lu regional cycling itineraries (CKAN-discovered GeoJSON)
  - VdL ArcGIS FeatureServer for the City of Luxembourg network

OSM is the base layer. Each official feature is checked against the OSM
spatial index (10 m proximity) and dropped if a match exists; otherwise
it is appended with `source='official_lu'`. OSM rows get `source='osm'`.

Caching: each source is cached separately in `ingest/data/` with a 7-day
freshness window so repeated runs don't hammer the upstream services.

Schema (`cycling_infrastructure`):
  feature_id  SERIAL PRIMARY KEY        -- stable PK, since osm_id is nullable
  osm_id      BIGINT                    -- only set for source='osm'
  category    TEXT NOT NULL             -- segregated | shared | planned
  source      TEXT NOT NULL             -- osm | official_lu
  highway     TEXT
  name        TEXT
  surface     TEXT
  geom        GEOMETRY(LineString, 3035) NOT NULL
"""
from __future__ import annotations

import json
import sys
import tempfile
import time
from pathlib import Path
from typing import Iterable

import geopandas as gpd
import pandas as pd
import requests
from shapely import force_2d
from shapely.geometry import LineString
from shapely.strtree import STRtree

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import PROJECTED_CRS, get_engine, get_psycopg2_conn  # noqa: E402

TABLE = "cycling_infrastructure"
DATA_DIR = Path(__file__).resolve().parent / "data"
CACHE_MAX_AGE_S = 7 * 86400  # 7 days

# --- OSM / Overpass --------------------------------------------------------
OSM_CACHE = DATA_DIR / "osm_cycling.json"
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

# --- Official LU sources ---------------------------------------------------
NATIONAL_CKAN_API = (
    "https://data.public.lu/api/1/datasets/"
    "tourisme-geoportail-lu-pistes-cyclables-nationales/"
)
REGIONAL_CKAN_API = (
    "https://data.public.lu/api/1/datasets/"
    "sentiers-cyclables-reseau-ditineraires-cyclables-regionaux/"
)
VDL_URL = (
    "https://maps.vdl.lu/arcgis/rest/services/OPENDATA/GEOJSON/FeatureServer/"
    "23/query?where=1%3D1&outFields=*&f=geojson"
)
NATIONAL_CACHE = DATA_DIR / "national_cycling.geojson"
REGIONAL_CACHE = DATA_DIR / "regional_cycling.geojson"
VDL_CACHE = DATA_DIR / "vdl_cycling.geojson"

DEFAULT_HEADERS = {
    "User-Agent": (
        "LuxBusCatchmentTool/1.0 "
        "(https://github.com/nicolaswinklercnc/LuxBusCatchmentTool)"
    ),
    "Accept": "application/json",
}

DEDUP_DISTANCE_M = 10  # ST_DWithin equivalent


# --- Caching helpers -------------------------------------------------------

def cache_is_fresh(path: Path) -> bool:
    return path.exists() and (time.time() - path.stat().st_mtime) < CACHE_MAX_AGE_S


def fetch_to_cache(url: str, cache_path: Path, label: str,
                   headers: dict | None = None) -> bytes:
    """Cached HTTP GET — returns raw bytes."""
    if cache_is_fresh(cache_path):
        print(f"Using cached {label}: {cache_path}")
        return cache_path.read_bytes()
    print(f"Fetching {label} ...")
    resp = requests.get(url, headers=headers or DEFAULT_HEADERS, timeout=120)
    resp.raise_for_status()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(resp.content)
    print(f"Cached {label}: {cache_path} ({len(resp.content) / 1024:.0f} KB)")
    return resp.content


# --- OSM source ------------------------------------------------------------

def fetch_overpass() -> dict:
    """Fetch the Overpass response (cached). Returns parsed JSON."""
    if cache_is_fresh(OSM_CACHE):
        print(f"Using cached Overpass response: {OSM_CACHE}")
        return json.loads(OSM_CACHE.read_text(encoding="utf-8"))
    print("Fetching cycling infrastructure from Overpass API ...")
    resp = requests.post(
        OVERPASS_URL,
        data={"data": OVERPASS_QUERY},
        headers=DEFAULT_HEADERS,
        timeout=120,
    )
    resp.raise_for_status()
    payload = resp.json()
    OSM_CACHE.parent.mkdir(parents=True, exist_ok=True)
    OSM_CACHE.write_text(json.dumps(payload), encoding="utf-8")
    print(f"Cached Overpass response: {OSM_CACHE} "
          f"({OSM_CACHE.stat().st_size / 1024:.0f} KB)")
    return payload


def classify_osm(tags: dict[str, str]) -> str | None:
    """planned > segregated > shared. Returns None if no rule matches."""
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
        category = classify_osm(tags)
        if category is None:
            skipped += 1
            continue
        line = LineString([(p["lon"], p["lat"]) for p in geom_pts])
        rows.append({
            "osm_id": int(el["id"]),
            "category": category,
            "source": "osm",
            "highway": tags.get("highway"),
            "name": tags.get("name"),
            "surface": tags.get("surface"),
            "geom": line,
        })
    if skipped:
        print(f"Skipped {skipped} OSM elements (no geometry or unmatched tags).")
    if not rows:
        raise RuntimeError("Overpass returned no usable cycling features.")
    gdf = gpd.GeoDataFrame(rows, geometry="geom", crs="EPSG:4326")
    return gdf.to_crs(PROJECTED_CRS)


# --- Official-LU sources ---------------------------------------------------

def discover_ckan_geojson_url(api_url: str, label: str) -> str:
    """Same shape as download_communes.discover_communes_url — pick the
    most-recently-updated GeoJSON resource on a CKAN dataset."""
    resp = requests.get(api_url, headers=DEFAULT_HEADERS, timeout=30)
    resp.raise_for_status()
    resources = resp.json().get("resources", [])
    candidates = [
        r for r in resources
        if (r.get("format") or "").lower() == "geojson"
        or (r.get("url") or "").lower().endswith(".geojson")
    ]
    if not candidates:
        raise RuntimeError(f"No GeoJSON resource at {api_url}")
    candidates.sort(
        key=lambda r: r.get("last_modified") or r.get("modified") or "",
        reverse=True,
    )
    chosen = candidates[0]
    print(f"  {label} resource: '{chosen.get('title') or '(untitled)'}'")
    return (chosen.get("url") or "").strip()


def _read_geojson_bytes(body: bytes) -> gpd.GeoDataFrame:
    """Round-trip via a tempfile — geopandas/Fiona need a real path."""
    with tempfile.NamedTemporaryFile(
        mode="wb", suffix=".geojson", delete=False
    ) as fh:
        fh.write(body)
        tmp = fh.name
    try:
        return gpd.read_file(tmp)
    finally:
        Path(tmp).unlink(missing_ok=True)


def _pick(row: dict | pd.Series, *keys) -> str | None:
    """Return the first non-empty value among the given column names."""
    for k in keys:
        if k in row and row[k] not in (None, "", float("nan")):
            try:
                if pd.isna(row[k]):
                    continue
            except (TypeError, ValueError):
                pass
            return str(row[k])
    return None


def _to_lines(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Drop non-Line geometries; flatten MultiLineStrings to LineStrings;
    force 2D so geometries match the LineString,3035 column."""
    out_rows = []
    for _, row in gdf.iterrows():
        g = row.geometry
        if g is None or g.is_empty:
            continue
        if g.geom_type == "LineString":
            row["geometry"] = force_2d(g)
            out_rows.append(row)
        elif g.geom_type == "MultiLineString":
            for part in g.geoms:
                new_row = row.copy()
                new_row["geometry"] = force_2d(part)
                out_rows.append(new_row)
    return gpd.GeoDataFrame(out_rows, geometry="geometry", crs=gdf.crs)


def fetch_national_paths() -> gpd.GeoDataFrame:
    """National cycling paths — all classified as segregated."""
    if cache_is_fresh(NATIONAL_CACHE):
        body = NATIONAL_CACHE.read_bytes()
        print(f"Using cached national cycling: {NATIONAL_CACHE}")
    else:
        url = discover_ckan_geojson_url(NATIONAL_CKAN_API, "national")
        body = fetch_to_cache(url, NATIONAL_CACHE, "national cycling")
    raw = _read_geojson_bytes(body)
    raw = _to_lines(raw.to_crs(PROJECTED_CRS) if raw.crs else raw.set_crs("EPSG:4326").to_crs(PROJECTED_CRS))
    rows = []
    for _, r in raw.iterrows():
        rows.append({
            "osm_id": pd.NA,
            "category": "segregated",
            "source": "official_lu",
            "highway": None,
            "name": _pick(r, "name", "NAME", "nom", "NOM", "designation",
                          "DESIGNATION", "label", "LABEL"),
            "surface": _pick(r, "surface", "SURFACE", "revetement", "REVETEMENT"),
            "geom": r.geometry,
        })
    return gpd.GeoDataFrame(rows, geometry="geom", crs=raw.crs)


def fetch_regional_itineraries() -> gpd.GeoDataFrame:
    """Regional itineraries — mix of paths and roads, classified as shared."""
    if cache_is_fresh(REGIONAL_CACHE):
        body = REGIONAL_CACHE.read_bytes()
        print(f"Using cached regional cycling: {REGIONAL_CACHE}")
    else:
        url = discover_ckan_geojson_url(REGIONAL_CKAN_API, "regional")
        body = fetch_to_cache(url, REGIONAL_CACHE, "regional cycling")
    raw = _read_geojson_bytes(body)
    raw = _to_lines(raw.to_crs(PROJECTED_CRS) if raw.crs else raw.set_crs("EPSG:4326").to_crs(PROJECTED_CRS))
    rows = []
    for _, r in raw.iterrows():
        rows.append({
            "osm_id": pd.NA,
            "category": "shared",
            "source": "official_lu",
            "highway": None,
            "name": _pick(r, "name", "NAME", "nom", "NOM", "designation",
                          "DESIGNATION", "label", "LABEL"),
            "surface": None,
            "geom": r.geometry,
        })
    return gpd.GeoDataFrame(rows, geometry="geom", crs=raw.crs)


def _classify_vdl(row: pd.Series) -> str:
    """VdL features default to segregated; STATUS field tags planned routes."""
    status = str(row.get("STATUS") or "").lower()
    if any(tok in status for tok in ("geplant", "plan", "projekt", "propose")):
        return "planned"
    return "segregated"


def fetch_vdl_cycling() -> gpd.GeoDataFrame:
    """City of Luxembourg cycling network from the VdL ArcGIS server.

    The ArcGIS endpoint omits the GeoJSON `crs` member, but its source CRS
    is EPSG:2169 (Luxembourg national grid). We force-tag and reproject.
    """
    if cache_is_fresh(VDL_CACHE):
        body = VDL_CACHE.read_bytes()
        print(f"Using cached VdL cycling: {VDL_CACHE}")
    else:
        body = fetch_to_cache(VDL_URL, VDL_CACHE, "VdL cycling")
    raw = _read_geojson_bytes(body)
    if raw.crs is None:
        raw = raw.set_crs("EPSG:2169")
    raw = _to_lines(raw.to_crs(PROJECTED_CRS))
    rows = []
    for _, r in raw.iterrows():
        rows.append({
            "osm_id": pd.NA,
            "category": _classify_vdl(r),
            "source": "official_lu",
            "highway": None,
            "name": _pick(r, "BEMERKUNG", "VELOROUTE_NR", "name", "NAME"),
            "surface": None,
            "geom": r.geometry,
        })
    return gpd.GeoDataFrame(rows, geometry="geom", crs=raw.crs)


# --- Merge -----------------------------------------------------------------

def merge_with_osm(
    osm_gdf: gpd.GeoDataFrame,
    official_gdfs: Iterable[gpd.GeoDataFrame],
) -> tuple[gpd.GeoDataFrame, dict]:
    """Drop official features that are within DEDUP_DISTANCE_M of any OSM
    feature. Returns the merged frame and a stats dict."""
    osm_geoms = list(osm_gdf.geometry.values)
    tree = STRtree(osm_geoms) if osm_geoms else None

    new_rows: list[dict] = []
    skipped = 0
    official_total = 0
    for off in official_gdfs:
        official_total += len(off)
        for _, row in off.iterrows():
            geom = row["geom"]
            if geom is None or geom.is_empty:
                skipped += 1
                continue
            if tree is not None:
                hits = tree.query(geom.buffer(DEDUP_DISTANCE_M),
                                  predicate="intersects")
                if len(hits) > 0:
                    skipped += 1
                    continue
            new_rows.append(row.to_dict())

    if new_rows:
        new_gdf = gpd.GeoDataFrame(new_rows, geometry="geom", crs=osm_gdf.crs)
        merged = gpd.GeoDataFrame(
            pd.concat([osm_gdf, new_gdf], ignore_index=True),
            geometry="geom",
            crs=osm_gdf.crs,
        )
    else:
        merged = osm_gdf.copy()

    stats = {
        "osm": len(osm_gdf),
        "official_total": official_total,
        "official_new": len(new_rows),
        "duplicates": skipped,
        "final": len(merged),
    }
    return merged, stats


# --- Load + summary --------------------------------------------------------

def load_to_postgis(merged: gpd.GeoDataFrame) -> int:
    """Drop+recreate the table with an explicit schema, then append rows."""
    engine = get_engine()
    with get_psycopg2_conn() as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {TABLE};")
        cur.execute(
            f"""
            CREATE TABLE {TABLE} (
                feature_id SERIAL PRIMARY KEY,
                osm_id     BIGINT,
                category   TEXT NOT NULL,
                source     TEXT NOT NULL,
                highway    TEXT,
                name       TEXT,
                surface    TEXT,
                geom       GEOMETRY(LineString, 3035) NOT NULL
            );
            """
        )
        cur.execute(
            f"CREATE INDEX {TABLE}_geom_idx ON {TABLE} USING GIST(geom);"
        )
        conn.commit()

    cols = ["osm_id", "category", "source", "highway", "name", "surface", "geom"]
    out = merged[cols].copy()
    out["osm_id"] = pd.array(out["osm_id"].values, dtype="Int64")
    out.to_postgis(TABLE, engine, if_exists="append", index=False)
    return len(out)


def print_summary(stats: dict) -> None:
    with get_psycopg2_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT category, COUNT(*), COALESCE(SUM(ST_Length(geom)) / 1000.0, 0) "
            f"FROM {TABLE} GROUP BY category;"
        )
        per_cat = {row[0]: (int(row[1]), float(row[2])) for row in cur.fetchall()}
        cur.execute(
            f"SELECT COUNT(*), COALESCE(SUM(ST_Length(geom)) / 1000.0, 0) FROM {TABLE};"
        )
        total_count, total_km = cur.fetchone()

    print()
    print(f"OSM features:       {stats['osm']:>5,}")
    print(f"Official features:  {stats['official_total']:>5,}")
    print(f"New from official:  {stats['official_new']:>5,} (not in OSM)")
    print(f"Duplicates skipped: {stats['duplicates']:>5,}")
    print(f"Final total:        {stats['final']:>5,} features ({float(total_km):.1f} km)")
    print()
    for cat in ("segregated", "shared", "planned"):
        c, km = per_cat.get(cat, (0, 0.0))
        print(f"  {cat:<11}: {c:>5,} features ({km:.1f} km)")


# --- Entry point -----------------------------------------------------------

def main() -> int:
    try:
        osm_payload = fetch_overpass()
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        body = exc.response.text[:500] if exc.response is not None else ""
        print(f"ERROR fetching Overpass: HTTP {status}: {exc}\n{body}",
              file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR fetching Overpass: {type(exc).__name__}: {exc}",
              file=sys.stderr)
        return 1

    try:
        osm_gdf = parse_overpass(osm_payload)
    except Exception as exc:
        print(f"ERROR parsing Overpass response: {exc}", file=sys.stderr)
        return 1

    official_gdfs: list[gpd.GeoDataFrame] = []
    for fetcher, label in (
        (fetch_national_paths, "national"),
        (fetch_regional_itineraries, "regional"),
        (fetch_vdl_cycling, "VdL"),
    ):
        try:
            g = fetcher()
            print(f"  loaded {len(g):,} {label} features")
            official_gdfs.append(g)
        except Exception as exc:
            print(f"WARNING: {label} source failed: {type(exc).__name__}: {exc}",
                  file=sys.stderr)

    merged, stats = merge_with_osm(osm_gdf, official_gdfs)
    n = load_to_postgis(merged)
    print(f"{TABLE}: {n:,} features loaded")
    print_summary(stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
