"""Download Luxembourg bus stops from the public GTFS feed and load to PostGIS.

Source dataset:
    https://data.public.lu/fr/datasets/horaires-et-arrets-des-bus-et-trams-du-luxembourg-gtfs/

The GTFS .zip URL is discovered via the data.public.lu CKAN API (most-recently-
updated resource where format == 'GTFS' or url ends with .zip). The successful
URL is cached at ingest/data/discovered_urls.json so subsequent runs skip the
API call unless the cached URL stops responding (or INGEST_FORCE_REFRESH=1).

Loads into table `bus_stops`:
    stop_id (text PK), stop_name (text), commune (text NULL),
    vehicle_type (text — 'tram' | 'rail' | 'bus'), geom (Point, 3035)

`vehicle_type` is derived by joining routes.txt → trips.txt → stop_times.txt
and mapping route_type (0 = tram, 2 = rail, 3 = bus). If a stop is served by
several types we pick the highest-tier one (tram > rail > bus); in the
current Luxembourg feed there are no overlaps so the precedence is moot.
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

DATASET_SLUG = "horaires-et-arrets-des-transport-publics-gtfs"
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
    print(f"GTFS URL: {url}")
    return url


# GTFS route_type → our vehicle_type bucket. Anything not listed becomes 'bus'.
ROUTE_TYPE_TO_VEHICLE = {0: "tram", 2: "rail", 3: "bus"}
# Highest-tier wins when a stop is served by multiple route_types.
VEHICLE_PRECEDENCE = {"tram": 3, "rail": 2, "bus": 1}


def download_gtfs_tables(zip_url: str) -> dict[str, pd.DataFrame]:
    """Pull the four GTFS tables we need from the feed in one HTTP fetch."""
    print(f"Downloading GTFS from {zip_url} ...")
    resp = requests.get(zip_url, timeout=120)
    resp.raise_for_status()
    out: dict[str, pd.DataFrame] = {}
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        out["stops"] = pd.read_csv(zf.open("stops.txt"))
        out["routes"] = pd.read_csv(
            zf.open("routes.txt"), usecols=["route_id", "route_type"]
        )
        out["trips"] = pd.read_csv(
            zf.open("trips.txt"), usecols=["route_id", "trip_id"]
        )
        out["stop_times"] = pd.read_csv(
            zf.open("stop_times.txt"), usecols=["trip_id", "stop_id"]
        )
    return out


def derive_vehicle_type(tables: dict[str, pd.DataFrame]) -> pd.Series:
    """Return a Series indexed by stop_id with values in {'tram','rail','bus'}."""
    routes = tables["routes"][["route_id", "route_type"]]
    trip_to_type = tables["trips"].merge(routes, on="route_id")[["trip_id", "route_type"]]
    served = (
        tables["stop_times"][["trip_id", "stop_id"]]
        .merge(trip_to_type, on="trip_id")
        [["stop_id", "route_type"]]
        .drop_duplicates()
    )
    served["vehicle_type"] = served["route_type"].map(ROUTE_TYPE_TO_VEHICLE).fillna("bus")
    served["rank"] = served["vehicle_type"].map(VEHICLE_PRECEDENCE)
    chosen = (
        served.sort_values("rank", ascending=False)
        .drop_duplicates(subset=["stop_id"], keep="first")
        .set_index("stop_id")["vehicle_type"]
    )
    chosen.index = chosen.index.astype(str)
    return chosen


def to_geodataframe(
    stops: pd.DataFrame, vehicle_type: pd.Series
) -> gpd.GeoDataFrame:
    stop_ids = stops["stop_id"].astype(str)
    # Stops that never appear in stop_times default to 'bus' (no trips reference
    # them; treating them as bus matches the historical behaviour).
    vt = stop_ids.map(vehicle_type).fillna("bus")
    n_tram = (vt == "tram").sum()
    n_rail = (vt == "rail").sum()
    n_bus = (vt == "bus").sum()
    print(f"  vehicle_type: {n_tram:,} tram, {n_rail:,} rail, {n_bus:,} bus")

    geom = [Point(xy) for xy in zip(stops["stop_lon"], stops["stop_lat"])]
    gdf = gpd.GeoDataFrame(
        {
            "stop_id": stop_ids,
            "stop_name": stops["stop_name"].astype(str),
            "commune": pd.Series([None] * len(stops), dtype="object"),
            "vehicle_type": vt.values,
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
        tables = download_gtfs_tables(zip_url)
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

    vehicle_type = derive_vehicle_type(tables)
    gdf = to_geodataframe(tables["stops"], vehicle_type)
    n = load_to_postgis(gdf)
    remember_url(CACHE_KEY, zip_url)
    print(f"{TABLE}: {n:,} stops loaded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
