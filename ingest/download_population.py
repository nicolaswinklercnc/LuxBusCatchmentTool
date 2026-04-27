"""Download Eurostat GEOSTAT 2021 1km² population grid (LU subset) and load to PostGIS.

URL discovery has three levels:
  1. Try a list of known download URLs with HEAD requests.
  2. If all fail, scrape the Eurostat GISCO landing page for .zip links
     containing 'geostat', '1k', or '1km'.
  3. If scraping also fails, fall back to a manually-placed file at
     ingest/data/GEOSTAT_manual.zip and tell the user how to provide one.

The successful URL is cached at ingest/data/discovered_urls.json so subsequent
runs skip discovery (unless INGEST_FORCE_REFRESH=1).

Loads into table `population_grid`:
    grid_id (text PK), pop_count (integer), geom (Polygon, 3035)
"""
from __future__ import annotations

import io
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

sys.path.insert(0, str(Path(__file__).resolve().parent))

import geopandas as gpd
import pandas as pd
import requests
from bs4 import BeautifulSoup

from db import PROJECTED_CRS, get_engine, get_psycopg2_conn  # noqa: E402
from url_cache import get_cached_url, head_ok, remember_url  # noqa: E402

KNOWN_URLS = [
    "https://ec.europa.eu/eurostat/cache/GISCO/geodatafiles/GEOSTAT_grid_POP_1K_2021_V2.zip",
    "https://gisco-services.ec.europa.eu/pub/census21/grid/GRD_1km_pop_2021_EU.zip",
    "https://gisco-services.ec.europa.eu/pub/census21/grid/GEOSTAT_grid_POP_1K_2021.zip",
]
GEOSTAT_PAGE = (
    "https://ec.europa.eu/eurostat/web/gisco/geodata/population-distribution/geostat"
)
MANUAL_FILE = Path(__file__).resolve().parent / "data" / "GEOSTAT_manual.zip"
TABLE = "population_grid"
CACHE_KEY = "geostat"


def discover_via_known_urls() -> Optional[str]:
    print("Trying known GEOSTAT URLs ...")
    for url in KNOWN_URLS:
        print(f"  HEAD {url}")
        if head_ok(url):
            return url
    return None


def discover_via_scrape() -> Optional[str]:
    print(f"Scraping {GEOSTAT_PAGE} for .zip candidates ...")
    try:
        resp = requests.get(GEOSTAT_PAGE, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"  scrape fetch failed: {exc}", file=sys.stderr)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    keywords = ("geostat", "1k", "1km")
    candidates: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href.lower().endswith(".zip"):
            continue
        if not any(kw in href.lower() for kw in keywords):
            continue
        candidates.append(urljoin(GEOSTAT_PAGE, href))

    if not candidates:
        return None
    print(f"  candidates: {candidates}")
    for c in candidates:
        if head_ok(c):
            return c
    return None


def resolve_geostat_source() -> tuple[Optional[str], Optional[bytes]]:
    """Return (url, zip_bytes). Exactly one of the two will be non-None."""
    cached = get_cached_url(CACHE_KEY)
    if cached:
        print(f"Using cached GEOSTAT URL: {cached}")
        return cached, None

    url = discover_via_known_urls()
    if url:
        print(f"GEOSTAT URL resolved (known list): {url}")
        return url, None

    print("All known GEOSTAT URLs returned non-200. Falling back to scrape ...")
    url = discover_via_scrape()
    if url:
        print(f"GEOSTAT URL resolved (scrape): {url}")
        return url, None

    if MANUAL_FILE.exists():
        print(f"Using manual fallback file: {MANUAL_FILE}")
        return None, MANUAL_FILE.read_bytes()

    raise RuntimeError(
        "Could not auto-discover GEOSTAT URL. Please visit:\n"
        f"  {GEOSTAT_PAGE}\n"
        "Download the 1km population grid zip manually and place it at:\n"
        f"  {MANUAL_FILE}\n"
        "Then re-run this script."
    )


def download_zip(url: str) -> bytes:
    print(f"Downloading GEOSTAT zip from {url} ...")
    resp = requests.get(url, timeout=600)
    resp.raise_for_status()
    return resp.content


def load_grid(zip_bytes: bytes) -> gpd.GeoDataFrame:
    """Open the GEOSTAT zip and return a GeoDataFrame of grid cells.

    The 2021 release usually ships a single GeoPackage with both geometry and
    TOT_P_2021. Some intermediate releases split them across a GPKG/SHP and a
    sidecar CSV keyed on GRD_ID — both layouts are handled below.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            zf.extractall(tmp)
        gpkgs = list(tmp.rglob("*.gpkg"))
        shps = list(tmp.rglob("*.shp"))
        csvs = list(tmp.rglob("*.csv"))
        if gpkgs:
            gdf = gpd.read_file(gpkgs[0])
        elif shps:
            gdf = gpd.read_file(shps[0])
        else:
            files = [p.name for p in tmp.rglob("*") if p.is_file()]
            raise RuntimeError(
                f"GEOSTAT zip did not contain a GPKG or SHP. Files present: {files}"
            )
        if "TOT_P_2021" not in gdf.columns and csvs:
            df = pd.read_csv(csvs[0])
            if "GRD_ID" in df.columns and "GRD_ID" in gdf.columns:
                gdf = gdf.merge(df, on="GRD_ID", how="left")
    return gdf


def filter_and_project(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if "CNTR_CODE" not in gdf.columns:
        raise RuntimeError(
            f"Expected CNTR_CODE column — got {list(gdf.columns)}"
        )
    lu = gdf[gdf["CNTR_CODE"] == "LU"].copy()
    if lu.empty:
        raise RuntimeError("No rows with CNTR_CODE='LU' in GEOSTAT data.")
    if "TOT_P_2021" not in lu.columns:
        raise RuntimeError(
            f"Expected TOT_P_2021 column — got {list(lu.columns)}"
        )
    out = gpd.GeoDataFrame(
        {
            "grid_id": lu["GRD_ID"].astype(str),
            "pop_count": lu["TOT_P_2021"].fillna(0).astype(int),
        },
        geometry=lu.geometry.values,
        crs=lu.crs,
    )
    return out.to_crs(PROJECTED_CRS).rename_geometry("geom")


def load_to_postgis(gdf: gpd.GeoDataFrame) -> int:
    engine = get_engine()
    gdf.to_postgis(TABLE, engine, if_exists="replace", index=False)
    with get_psycopg2_conn() as conn, conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {TABLE} ADD PRIMARY KEY (grid_id);")
        conn.commit()
    return len(gdf)


def main() -> int:
    try:
        url, zip_bytes = resolve_geostat_source()
    except Exception as exc:
        print(f"ERROR resolving GEOSTAT source: {exc}", file=sys.stderr)
        return 1

    try:
        if zip_bytes is None:
            zip_bytes = download_zip(url)
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            print(
                f"ERROR 404 at {url} (HEAD passed but GET failed) — check {GEOSTAT_PAGE}.",
                file=sys.stderr,
            )
        else:
            print(f"ERROR downloading GEOSTAT: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR downloading GEOSTAT: {exc}", file=sys.stderr)
        return 1

    try:
        raw = load_grid(zip_bytes)
        lu = filter_and_project(raw)
    except Exception as exc:
        print(f"ERROR processing GEOSTAT data: {exc}", file=sys.stderr)
        return 1

    n = load_to_postgis(lu)
    if url:
        remember_url(CACHE_KEY, url)
    total_pop = int(lu["pop_count"].sum())
    print(f"Loaded {n} rows into {TABLE} (CRS {PROJECTED_CRS}).")
    print(f"Total population (sum TOT_P_2021): {total_pop:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
