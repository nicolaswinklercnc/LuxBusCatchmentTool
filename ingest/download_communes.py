"""Download Luxembourg commune boundaries and load to PostGIS.

Source dataset:
    https://data.public.lu/fr/datasets/limites-administratives-du-grand-duche-de-luxembourg/

The commune GeoJSON URL is discovered via the data.public.lu CKAN API
(most-recently-updated resource where format == 'GeoJSON' or url ends in
.geojson). The selected resource's title is printed so you can sanity-check
that you got the commune layer rather than another administrative level.

The successful URL is cached at ingest/data/discovered_urls.json so subsequent
runs skip the API call (unless INGEST_FORCE_REFRESH=1).

Loads into table `communes`:
    commune_id (serial PK), name (text), geom (MultiPolygon, 3035)
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import MultiPolygon

from db import PROJECTED_CRS, get_engine, get_psycopg2_conn  # noqa: E402
from url_cache import get_cached_url, remember_url  # noqa: E402

DATASET_SLUG = "limites-administratives-du-grand-duche-de-luxembourg"
DATASET_API = f"https://data.public.lu/api/1/datasets/{DATASET_SLUG}/"
DATASET_PAGE = f"https://data.public.lu/fr/datasets/{DATASET_SLUG}/"
TABLE = "communes"
CACHE_KEY = "communes"
LAYER = "communes"


def _resource_modified(r: dict) -> str:
    return r.get("last_modified") or r.get("modified") or r.get("created_at") or ""


def discover_communes_url() -> str:
    resp = requests.get(DATASET_API, timeout=30)
    resp.raise_for_status()
    resources = resp.json().get("resources", [])
    candidates = [
        r for r in resources
        if (r.get("format") or "").lower() == "geojson"
        or (r.get("url") or "").lower().endswith(".geojson")
    ]
    if not candidates:
        raise RuntimeError(
            f"No GeoJSON resource on {DATASET_PAGE} — check the page manually."
        )
    candidates.sort(key=_resource_modified, reverse=True)
    chosen = candidates[0]
    title = chosen.get("title") or "(untitled)"
    print(f"  selected resource: '{title}'")
    return (chosen.get("url") or "").strip()


def resolve_communes_url() -> str:
    cached = get_cached_url(CACHE_KEY)
    if cached:
        print(f"Using cached communes URL: {cached}")
        return cached
    url = discover_communes_url()
    print(f"Discovered communes URL: {url}")
    return url


def fetch_geodataframe(url: str) -> gpd.GeoDataFrame:
    """Download the multi-layer GeoJSON and return only the communes layer.

    The GDAL drivers behind geopandas need a real filesystem path to address a
    named layer in a multi-layer GeoJSON, so the response is streamed to a
    tempfile rather than read from BytesIO.
    """
    print(f"Downloading commune boundaries from {url} ...")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    suffix = ".gpkg" if url.lower().endswith(".gpkg") else ".geojson"
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / f"limadmin{suffix}"
        path.write_bytes(resp.content)
        return gpd.read_file(path, layer=LAYER)


def _looks_like_names(series: pd.Series) -> bool:
    """True iff at least 80% of non-null values contain an alphabetic character."""
    s = series.dropna().astype(str)
    if s.empty:
        return False
    has_letter = s.str.contains(r"[A-Za-zÀ-ÿ]", regex=True, na=False).sum()
    return has_letter / len(s) >= 0.8


def pick_name_column(gdf: gpd.GeoDataFrame) -> str:
    if "LAU1" in gdf.columns and _looks_like_names(gdf["LAU1"]):
        print("  name column: LAU1 (looks like names)")
        return "LAU1"
    if "LAU1" in gdf.columns:
        print("  LAU1 looks like codes, falling back to DISTRICT")
    if "DISTRICT" in gdf.columns and _looks_like_names(gdf["DISTRICT"]):
        print("  name column: DISTRICT (looks like names)")
        return "DISTRICT"
    raise RuntimeError(
        f"Neither LAU1 nor DISTRICT looks like names. Columns: {list(gdf.columns)}"
    )


def normalise(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    name_field = pick_name_column(gdf)
    geom = gdf.geometry.apply(
        lambda g: g if g is None or g.geom_type == "MultiPolygon" else MultiPolygon([g])
    )
    out = gpd.GeoDataFrame(
        {"name": gdf[name_field].astype(str)},
        geometry=geom,
        crs=gdf.crs,
    )
    return out.to_crs(PROJECTED_CRS).rename_geometry("geom")


def load_to_postgis(gdf: gpd.GeoDataFrame) -> int:
    engine = get_engine()
    gdf.to_postgis(TABLE, engine, if_exists="replace", index=False)
    with get_psycopg2_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"ALTER TABLE {TABLE} ADD COLUMN commune_id SERIAL PRIMARY KEY;"
        )
        conn.commit()
    return len(gdf)


def main() -> int:
    try:
        url = resolve_communes_url()
        gdf = fetch_geodataframe(url)
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            print(
                f"ERROR 404 fetching commune boundaries. Check {DATASET_PAGE} manually.",
                file=sys.stderr,
            )
        else:
            print(f"ERROR fetching commune boundaries: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR fetching commune boundaries: {exc}", file=sys.stderr)
        return 1

    print(f"\nLoaded {len(gdf)} rows from layer '{LAYER}'.")
    print(f"  columns: {list(gdf.columns)}")
    display = gdf.head(5).copy()
    geom_col = gdf.geometry.name
    if geom_col in display.columns:
        display[geom_col] = display[geom_col].apply(
            lambda g: f"{g.geom_type}(...)" if g is not None else None
        )
    print("First 5 rows:")
    print(display.to_string())
    print()

    norm = normalise(gdf)
    n = load_to_postgis(norm)
    remember_url(CACHE_KEY, url)
    print(f"Loaded {n} rows into {TABLE} (CRS {PROJECTED_CRS}).")
    print(f"Total communes: {n} (expected 102).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
