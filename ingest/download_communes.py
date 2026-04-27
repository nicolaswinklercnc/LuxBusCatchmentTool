"""Download Luxembourg commune boundaries and load to PostGIS.

Source dataset page:
    https://data.public.lu/fr/datasets/limites-administratives-du-grand-duche-de-luxembourg/

Loads into table `communes` with columns:
    commune_id (serial PK), name (text), geom (MultiPolygon, 3035)
"""
from __future__ import annotations

import io
import sys
import tempfile
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import geopandas as gpd
import requests
from shapely.geometry import MultiPolygon

from db import PROJECTED_CRS, get_engine, get_psycopg2_conn  # noqa: E402

DATASET_SLUG = "limites-administratives-du-grand-duche-de-luxembourg"
DATASET_API = f"https://data.public.lu/api/1/datasets/{DATASET_SLUG}/"
DATASET_PAGE = f"https://data.public.lu/fr/datasets/{DATASET_SLUG}/"
TABLE = "communes"

NAME_FIELD_CANDIDATES = [
    "COMMUNE", "commune", "NAME", "name", "LIBELLE", "libelle", "TEXTE", "NOM", "nom",
]


def find_communes_resource_url() -> str:
    """Pick the best resource: prefer GeoJSON titled with 'commune', else SHP zip."""
    resp = requests.get(DATASET_API, timeout=30)
    resp.raise_for_status()
    resources = resp.json().get("resources", [])

    geojson = []
    shp_zip = []
    for r in resources:
        url = (r.get("url") or "").strip()
        title = (r.get("title") or "").lower()
        fmt = (r.get("format") or "").lower()
        # Skip resources that are clearly not commune polygons
        if not any(kw in title for kw in ("commune", "communal", "limit", "admin")):
            continue
        if url.lower().endswith(".geojson") or fmt == "geojson":
            geojson.append(url)
        elif url.lower().endswith(".zip") or fmt in {"shp", "shapefile", "zip"}:
            shp_zip.append(url)
    if geojson:
        return geojson[0]
    if shp_zip:
        return shp_zip[0]
    raise RuntimeError(
        f"Could not find a commune-boundaries resource on {DATASET_PAGE} — "
        f"open the page and check available resources."
    )


def fetch_geodataframe(url: str) -> gpd.GeoDataFrame:
    print(f"Downloading commune boundaries from {url} ...")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    if url.lower().endswith(".geojson"):
        return gpd.read_file(io.BytesIO(resp.content))
    if url.lower().endswith(".zip"):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                zf.extractall(tmp)
            shp = next(tmp.rglob("*.shp"), None)
            if shp is None:
                raise RuntimeError("Zip did not contain a .shp file.")
            return gpd.read_file(shp)
    raise RuntimeError(f"Unsupported resource format at {url}")


def normalise(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    name_field = next((f for f in NAME_FIELD_CANDIDATES if f in gdf.columns), None)
    if name_field is None:
        raise RuntimeError(
            f"No commune-name field found. Columns: {list(gdf.columns)}"
        )
    # Promote single Polygons to MultiPolygon for a uniform geom column type
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
        url = find_communes_resource_url()
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

    norm = normalise(gdf)
    n = load_to_postgis(norm)
    print(f"Loaded {n} rows into {TABLE} (CRS {PROJECTED_CRS}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
