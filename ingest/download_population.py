"""Download Eurostat GEOSTAT 2021 1km² population grid (LU subset) and load to PostGIS.

Source landing page:
    https://ec.europa.eu/eurostat/web/gisco/geodata/population-distribution/geostat

Loads into table `population_grid` with columns:
    grid_id (text PK), pop_count (integer), geom (Polygon, 3035)
"""
from __future__ import annotations

import io
import sys
import tempfile
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import geopandas as gpd
import pandas as pd
import requests

from db import PROJECTED_CRS, get_engine, get_psycopg2_conn  # noqa: E402

# Best-known location of the GEOSTAT 2021 1 km grid (geometry + population).
# If this 404s the URL has likely been rotated — check the landing page below
# and update GEOSTAT_URL.
GEOSTAT_URL = (
    "https://ec.europa.eu/eurostat/cache/GISCO/geodatafiles/"
    "GEOSTAT_grid_POP_1K_2021_V2.zip"
)
GEOSTAT_PAGE = (
    "https://ec.europa.eu/eurostat/web/gisco/geodata/population-distribution/geostat"
)
TABLE = "population_grid"


def download_zip(url: str) -> bytes:
    print(f"Downloading GEOSTAT grid from {url} ...")
    resp = requests.get(url, timeout=600)
    resp.raise_for_status()
    return resp.content


def load_grid(zip_bytes: bytes) -> gpd.GeoDataFrame:
    """Extract the GEOSTAT zip and return a GeoDataFrame of grid cells.

    The 2021 release usually ships a single GeoPackage that contains both
    geometry and the TOT_P_2021 population attribute. Some intermediate
    releases split them across a GPKG/SHP (geometry) plus a CSV
    (population) keyed on GRD_ID — both layouts are handled below.
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
            f"Expected CNTR_CODE column in GEOSTAT data — got {list(gdf.columns)}"
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
        zip_bytes = download_zip(GEOSTAT_URL)
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            print(
                f"ERROR 404 at {GEOSTAT_URL}. "
                f"The GEOSTAT URL may have changed — check {GEOSTAT_PAGE} "
                f"and update GEOSTAT_URL in this script.",
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
    total_pop = int(lu["pop_count"].sum())
    print(f"Loaded {n} rows into {TABLE} (CRS {PROJECTED_CRS}).")
    print(f"Total population (sum TOT_P_2021): {total_pop:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
