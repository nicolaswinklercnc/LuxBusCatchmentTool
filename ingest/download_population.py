"""Download Eurostat Census-GRID 2021 V2.2 1km² population grid for Luxembourg.

URL discovery has three levels:
  1. Try a list of known download URLs with HEAD requests.
  2. If all fail, scrape the Eurostat GISCO landing page for .zip links.
  3. If scraping also fails, fall back to a manually-placed file at
     ingest/data/GEOSTAT_manual.zip.

The successful URL is cached at ingest/data/discovered_urls.json so subsequent
runs skip discovery (unless INGEST_FORCE_REFRESH=1).

Schema (Census-GRID 2021 V2.2):
- The bundle ships a single GeoPackage covering all of Europe (~4.5M cells)
  in EPSG:3035, layer name `census2021`.
- There is no CNTR_CODE column — country filtering is done spatially by
  passing a Luxembourg bounding box (in 3035) to read_file's `bbox` param,
  which keeps memory bounded.
- Population columns: T (total), Y_LT15, Y_1564, Y_GE65.

Loads into table `population_grid`:
    grid_id (text PK), pop_count (int), pop_under15 (int),
    pop_working_age (int), pop_over65 (int), geom (Polygon, 3035)
"""
from __future__ import annotations

import io
import shutil
import sys
import zipfile
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

sys.path.insert(0, str(Path(__file__).resolve().parent))

import geopandas as gpd
import requests
from bs4 import BeautifulSoup

from db import PROJECTED_CRS, get_engine, get_psycopg2_conn  # noqa: E402
from url_cache import get_cached_url, head_ok, remember_url  # noqa: E402

KNOWN_URLS = [
    "https://gisco-services.ec.europa.eu/census/2021/Eurostat_Census-GRID_2021_V2.2.zip",
    "https://ec.europa.eu/eurostat/cache/GISCO/geodatafiles/GEOSTAT_grid_POP_1K_2021_V2.zip",
    "https://gisco-services.ec.europa.eu/pub/census21/grid/GRD_1km_pop_2021_EU.zip",
    "https://gisco-services.ec.europa.eu/pub/census21/grid/GEOSTAT_grid_POP_1K_2021.zip",
    "https://gisco-services.ec.europa.eu/pub/census21/grid/GEOSTAT_grid_POP_1K_2021_V2.zip",
    "https://gisco-services.ec.europa.eu/pub/census21/grid/GEOSTAT_2021_1km.zip",
]
GEOSTAT_PAGE = (
    "https://ec.europa.eu/eurostat/web/gisco/geodata/population-distribution/population-grids"
)
DATA_DIR = Path(__file__).resolve().parent / "data"
MANUAL_FILE = DATA_DIR / "GEOSTAT_manual.zip"
GPKG_CACHE = DATA_DIR / "ESTAT_Census_2021_V2.gpkg"
TABLE = "population_grid"
CACHE_KEY = "geostat"

GPKG_LAYER = "census2021"

# Luxembourg bounding box in EPSG:3035 — used at read_file time so we never
# load the full European grid into memory.
LU_BOUNDS = {
    "minx": 3_950_000,
    "miny": 3_050_000,
    "maxx": 4_050_000,
    "maxy": 3_200_000,
}
LU_BBOX = (LU_BOUNDS["minx"], LU_BOUNDS["miny"], LU_BOUNDS["maxx"], LU_BOUNDS["maxy"])

# source-column → PostGIS-column. Order is preserved when building the GDF.
POP_COLUMNS = {
    "T": "pop_count",
    "Y_LT15": "pop_under15",
    "Y_1564": "pop_working_age",
    "Y_GE65": "pop_over65",
}


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
    candidates: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        h = href.lower()
        if not h.endswith(".zip"):
            continue
        if "2021" not in h:
            continue
        if "1km" not in h and "1k" not in h:
            continue
        candidates.append(urljoin(GEOSTAT_PAGE, href))

    if not candidates:
        print("  no candidates matched (.zip + '2021' + '1km'/'1K').")
        return None
    print(f"  candidates ({len(candidates)}):")
    for c in candidates:
        print(f"    {c}")
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
        "Could not auto-discover population grid URL.\n"
        "Please visit:\n"
        f"  {GEOSTAT_PAGE}\n"
        "Download the 1km x 1km population grid for 2021.\n"
        f"Place the zip at: {MANUAL_FILE}\n"
        "Then re-run this script."
    )


def download_zip(url: str) -> bytes:
    print(f"Downloading GEOSTAT zip from {url} ...")
    resp = requests.get(url, timeout=600)
    resp.raise_for_status()
    return resp.content


def _list_layers(path: Path) -> list[str]:
    """List GPKG/Geopackage layer names, falling back across drivers."""
    try:
        from pyogrio import list_layers  # type: ignore
        return [row[0] for row in list_layers(str(path))]
    except Exception:
        pass
    try:
        import fiona  # type: ignore
        return list(fiona.listlayers(str(path)))
    except Exception as exc:
        return [f"(could not list layers: {exc})"]


def extract_gpkg_to_cache(zip_bytes: bytes) -> Path:
    """Stream the Census-GRID GeoPackage out of the zip into the persistent cache.

    The pan-European GPKG is large (~500 MB), so we keep it on disk after the
    first download and skip the zip entirely on subsequent runs.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        members = zf.namelist()
        print(f"Zip contents ({len(members)} files):")
        for m in members:
            print(f"  {m}")
        gpkg_members = [m for m in members if m.lower().endswith(".gpkg")]
        if not gpkg_members:
            raise RuntimeError(
                f"Census-GRID zip did not contain a GPKG. Files: {members}"
            )
        target = gpkg_members[0]
        print(f"Extracting {target} -> {GPKG_CACHE}")
        with zf.open(target) as src, open(GPKG_CACHE, "wb") as dst:
            shutil.copyfileobj(src, dst)
    return GPKG_CACHE


def read_lu_grid(gpkg: Path) -> gpd.GeoDataFrame:
    """Read only the LU bbox window from the cached GeoPackage."""
    print(f"GeoPackage layers in {gpkg.name}: {_list_layers(gpkg)}")
    print(f"Reading layer '{GPKG_LAYER}' with bbox={LU_BBOX} (EPSG:3035) ...")
    gdf = gpd.read_file(gpkg, layer=GPKG_LAYER, bbox=LU_BBOX)
    print(f"Loaded {len(gdf):,} rows for the LU bbox.")
    print(f"Columns: {list(gdf.columns)}")
    print(f"CRS: {gdf.crs}")
    return gdf


def load_lu_boundary() -> gpd.GeoDataFrame:
    """Load Luxembourg's outline as a single unified polygon from `communes`.

    The bbox-pushdown read in load_grid() captures slices of Belgium, France
    and Germany at the corners of the bounding box; this boundary is used to
    clip those out spatially.
    """
    engine = get_engine()
    try:
        with engine.connect() as conn:
            boundary = gpd.read_postgis(
                "SELECT ST_Union(geom) AS geom FROM communes",
                conn,
                geom_col="geom",
                crs="EPSG:3035",
            )
    except Exception as exc:
        raise RuntimeError(
            "Could not load LU boundary from `communes` table. "
            "Run download_communes.py first."
        ) from exc
    if boundary.empty or boundary.geometry.iloc[0] is None:
        raise RuntimeError(
            "`communes` table is empty — run download_communes.py first."
        )
    return boundary


def select_and_project(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if "GRD_ID" not in gdf.columns:
        raise RuntimeError(f"Expected GRD_ID column — got {list(gdf.columns)}")
    missing = [src for src in POP_COLUMNS if src not in gdf.columns]
    if missing:
        raise RuntimeError(
            f"Missing expected population columns {missing}. "
            f"Available: {list(gdf.columns)}"
        )

    out_data: dict = {"grid_id": gdf["GRD_ID"].astype(str)}
    for src, dst in POP_COLUMNS.items():
        out_data[dst] = gdf[src].fillna(0).astype(int)

    out = gpd.GeoDataFrame(
        out_data,
        geometry=gdf.geometry.values,
        crs=gdf.crs,
    )

    # Census-GRID V2.2 is published in EPSG:3035 — only reproject if not.
    current_epsg = out.crs.to_epsg() if out.crs else None
    if current_epsg == 3035:
        print("CRS already EPSG:3035; skipping reprojection.")
    else:
        print(f"Reprojecting from {out.crs} to EPSG:3035.")
        out = out.to_crs(epsg=3035)

    return out.rename_geometry("geom")


def load_to_postgis(gdf: gpd.GeoDataFrame) -> int:
    engine = get_engine()
    gdf.to_postgis(TABLE, engine, if_exists="replace", index=False)
    with get_psycopg2_conn() as conn, conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {TABLE} ADD PRIMARY KEY (grid_id);")
        conn.commit()
    return len(gdf)


def main() -> int:
    download_url: Optional[str] = None

    if GPKG_CACHE.exists():
        size_mb = GPKG_CACHE.stat().st_size / 1024 / 1024
        print(f"Using cached GeoPackage: {GPKG_CACHE}")
        print(f"Cached GeoPackage: {size_mb:.0f} MB")
    else:
        print("Downloading GEOSTAT zip (one-time, ~500MB) ...")
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
            extract_gpkg_to_cache(zip_bytes)
        except Exception as exc:
            print(f"ERROR extracting GeoPackage: {exc}", file=sys.stderr)
            return 1
        size_mb = GPKG_CACHE.stat().st_size / 1024 / 1024
        print(f"Cached GeoPackage: {size_mb:.0f} MB")
        download_url = url

    try:
        raw = read_lu_grid(GPKG_CACHE)
        lu = select_and_project(raw)
    except Exception as exc:
        print(f"ERROR processing GEOSTAT data: {exc}", file=sys.stderr)
        return 1

    try:
        boundary = load_lu_boundary()
    except Exception as exc:
        print(f"ERROR loading LU boundary: {exc}", file=sys.stderr)
        return 1

    before = len(lu)
    lu = lu[lu.intersects(boundary.geometry.iloc[0])].copy()
    print(f"\nClipped to LU boundary: {before:,} -> {len(lu):,} rows.")

    total_pop = int(lu["pop_count"].sum())
    zero_or_null = int((lu["pop_count"] == 0).sum())
    print("\nSummary:")
    print(f"  Luxembourg rows: {len(lu):,}")
    print(f"  Sum of pop_count: {total_pop:,}")
    print(f"  Cells with pop_count = 0 or null: {zero_or_null:,}")

    n = load_to_postgis(lu)
    if download_url:
        remember_url(CACHE_KEY, download_url)
    print(f"Loaded {n} rows into {TABLE} (CRS {PROJECTED_CRS}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
