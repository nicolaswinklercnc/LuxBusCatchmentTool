"""Download Luxembourg bus stops from the public GTFS feed and load to PostGIS.

Source dataset page:
    https://data.public.lu/fr/datasets/horaires-et-arrets-des-bus-et-trams-du-luxembourg-gtfs/

The actual GTFS .zip resource URL is discovered at runtime via the
data.public.lu API — this is more robust than hardcoding a resource ID
that the publisher may rotate.

Loads into table `bus_stops` with columns:
    stop_id (text PK), stop_name (text), commune (text NULL), geom (Point, 3035)
"""
from __future__ import annotations

import io
import sys
import zipfile
from pathlib import Path

# Make ingest/ importable when run as `python ingest/download_gtfs.py`
sys.path.insert(0, str(Path(__file__).resolve().parent))

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Point

from db import PROJECTED_CRS, get_engine, get_psycopg2_conn  # noqa: E402

DATASET_SLUG = "horaires-et-arrets-des-bus-et-trams-du-luxembourg-gtfs"
DATASET_API = f"https://data.public.lu/api/1/datasets/{DATASET_SLUG}/"
DATASET_PAGE = f"https://data.public.lu/fr/datasets/{DATASET_SLUG}/"
TABLE = "bus_stops"


def find_gtfs_resource_url() -> str:
    resp = requests.get(DATASET_API, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    for resource in payload.get("resources", []):
        url = (resource.get("url") or "").strip()
        title = (resource.get("title") or "").lower()
        fmt = (resource.get("format") or "").lower()
        if url.lower().endswith(".zip") or fmt == "zip" or "gtfs" in title:
            return url
    raise RuntimeError(
        f"Could not find a GTFS .zip resource on {DATASET_PAGE} — "
        f"open the page and check what's published."
    )


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
        zip_url = find_gtfs_resource_url()
        stops = download_stops_txt(zip_url)
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            print(
                f"ERROR 404 downloading GTFS. Check the dataset page manually: {DATASET_PAGE}",
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
    print(f"Loaded {n} rows into {TABLE} (CRS {PROJECTED_CRS}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
