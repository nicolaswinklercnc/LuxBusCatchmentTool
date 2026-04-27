"""Download Luxembourg bus stops from the public GTFS feed and load to PostGIS.

Source dataset:
    https://data.public.lu/fr/datasets/horaires-et-arrets-des-bus-et-trams-du-luxembourg-gtfs/

The GTFS .zip URL is discovered via the data.public.lu CKAN API (most-recently-
updated resource where format == 'GTFS' or url ends with .zip). The successful
URL is cached at ingest/data/discovered_urls.json so subsequent runs skip the
API call unless the cached URL stops responding (or INGEST_FORCE_REFRESH=1).

Loads into table `bus_stops`:
    stop_id (text PK), stop_name (text), commune (text NULL), geom (Point, 3035)
"""
from __future__ import annotations

import io
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Point

from db import PROJECTED_CRS, get_engine, get_psycopg2_conn  # noqa: E402
from url_cache import get_cached_url, remember_url  # noqa: E402

DATASET_SLUG = "horaires-et-arrets-des-bus-et-trams-du-luxembourg-gtfs"
DATASET_API = f"https://data.public.lu/api/1/datasets/{DATASET_SLUG}/"
DATASET_PAGE = f"https://data.public.lu/fr/datasets/{DATASET_SLUG}/"
TABLE = "bus_stops"
CACHE_KEY = "gtfs"


def _resource_modified(r: dict) -> str:
    return r.get("last_modified") or r.get("modified") or r.get("created_at") or ""


def discover_gtfs_url() -> str:
    """Hit the CKAN API and pick the most recently updated GTFS .zip resource."""
    resp = requests.get(DATASET_API, timeout=30)
    resp.raise_for_status()
    resources = resp.json().get("resources", [])
    candidates = [
        r for r in resources
        if (r.get("format") or "").lower() == "gtfs"
        or (r.get("url") or "").lower().endswith(".zip")
    ]
    if not candidates:
        raise RuntimeError(
            f"No GTFS .zip resource on {DATASET_PAGE} — check the page manually."
        )
    candidates.sort(key=_resource_modified, reverse=True)
    return (candidates[0].get("url") or "").strip()


def resolve_gtfs_url() -> str:
    cached = get_cached_url(CACHE_KEY)
    if cached:
        print(f"Using cached GTFS URL: {cached}")
        return cached
    url = discover_gtfs_url()
    print(f"Discovered GTFS URL: {url}")
    return url


def download_stops_txt(zip_url: str) -> pd.DataFrame:
    print(f"Downloading GTFS from {zip_url} ...")
    resp = requests.get(zip_url, timeout=120)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        with zf.open("stops.txt") as f:
            return pd.read_csv(f)


def to_geodataframe(stops: pd.DataFrame) -> gpd.GeoDataFrame:
    geom = [Point(xy) for xy in zip(stops["stop_lon"], stops["stop_lat"])]
    gdf = gpd.GeoDataFrame(
        {
            "stop_id": stops["stop_id"].astype(str),
            "stop_name": stops["stop_name"].astype(str),
            "commune": pd.Series([None] * len(stops), dtype="object"),
        },
        geometry=geom,
        crs="EPSG:4326",
    )
    return gdf.to_crs(PROJECTED_CRS).rename_geometry("geom")


def load_to_postgis(gdf: gpd.GeoDataFrame) -> int:
    engine = get_engine()
    gdf.to_postgis(TABLE, engine, if_exists="replace", index=False)
    with get_psycopg2_conn() as conn, conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {TABLE} ADD PRIMARY KEY (stop_id);")
        conn.commit()
    return len(gdf)


def main() -> int:
    try:
        zip_url = resolve_gtfs_url()
        stops = download_stops_txt(zip_url)
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            print(
                f"ERROR 404 downloading GTFS. Check the dataset page: {DATASET_PAGE}",
                file=sys.stderr,
            )
        else:
            print(f"ERROR downloading GTFS: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR fetching GTFS: {exc}", file=sys.stderr)
        return 1

    gdf = to_geodataframe(stops)
    n = load_to_postgis(gdf)
    remember_url(CACHE_KEY, zip_url)
    print(f"Loaded {n} rows into {TABLE} (CRS {PROJECTED_CRS}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
