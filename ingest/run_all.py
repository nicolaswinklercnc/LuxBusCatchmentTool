"""Run all three Luxembourg data ingest scripts in order, then build spatial indexes.

Each sub-script's main() is called via importlib so a failure in one does not
stop the others. Final summary prints row counts queried back from the DB.
"""
from __future__ import annotations

import argparse
import importlib
import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import get_psycopg2_conn  # noqa: E402

SCRIPTS = [
    ("bus_stops", "download_gtfs"),
    ("communes", "download_communes"),
    ("population_grid", "download_population"),
    ("cycling_infrastructure", "download_cycling"),
]


def check_postgis() -> None:
    """Confirm the PostGIS extension is installed; abort with a clear error if not."""
    with get_psycopg2_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute("SELECT PostGIS_version();")
            (version,) = cur.fetchone()
            print(f"PostGIS available: {version}")
        except Exception as exc:
            raise RuntimeError(
                "PostGIS is not enabled on this database. "
                "Connect with psql and run: CREATE EXTENSION postgis;"
            ) from exc


def run_one(module_name: str) -> None:
    print(f"\n=== Running {module_name} ===")
    try:
        mod = importlib.import_module(module_name)
        mod.main()
    except Exception as exc:
        print(f"ERROR in {module_name}: {exc}", file=sys.stderr)


def row_count(table: str) -> int | None:
    try:
        with get_psycopg2_conn() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table};")
            (count,) = cur.fetchone()
            return int(count)
    except Exception:
        return None


def communes_ready() -> bool:
    """download_population needs communes loaded so it can clip to the LU boundary."""
    n = row_count("communes")
    return n is not None and n > 0


def create_indexes() -> None:
    with get_psycopg2_conn() as conn, conn.cursor() as cur:
        for table, _ in SCRIPTS:
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS {table}_geom_idx "
                f"ON {table} USING GIST(geom);"
            )
        conn.commit()
    print("Spatial indexes ensured on bus_stops, population_grid, communes.")


def backfill_commune_on_bus_stops() -> None:
    """Stamp bus_stops.commune via spatial join against communes polygons.

    Stops near the border can fall outside every commune polygon and stay
    NULL — that's expected, not an error.
    """
    with get_psycopg2_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE bus_stops
            SET commune = c.name
            FROM communes c
            WHERE ST_Within(bus_stops.geom, c.geom);
            """
        )
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM bus_stops WHERE commune IS NOT NULL;")
        (matched,) = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM bus_stops WHERE commune IS NULL;")
        (unmatched,) = cur.fetchone()
    print(
        f"Commune backfill: {matched} stops matched, "
        f"{unmatched} unmatched (expected for stops outside all polygons)."
    )


def print_summary() -> None:
    print("\nTable                  | Rows loaded")
    print("-----------------------|------------")
    for table, _ in SCRIPTS:
        n = row_count(table)
        n_str = "FAILED" if n is None else f"{n}"
        print(f"{table:<22} | {n_str}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run all Luxembourg data ingest scripts."
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Ignore cached download URLs and rediscover everything fresh.",
    )
    args = parser.parse_args()
    if args.force_refresh:
        os.environ["INGEST_FORCE_REFRESH"] = "1"
        print("--force-refresh: discovered URL cache will be ignored.")

    started = time.monotonic()
    print(f"Run started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    check_postgis()
    for _table, module_name in SCRIPTS:
        if module_name == "download_population" and not communes_ready():
            print(
                "\n=== Skipping download_population ===\n"
                "communes must be loaded before population_grid. "
                "Run download_communes first.",
                file=sys.stderr,
            )
            continue
        run_one(module_name)
    try:
        create_indexes()
    except Exception as exc:
        print(f"WARNING: could not create spatial indexes: {exc}", file=sys.stderr)
    try:
        backfill_commune_on_bus_stops()
    except Exception as exc:
        print(f"WARNING: commune backfill failed: {exc}", file=sys.stderr)
    print_summary()
    elapsed = int(time.monotonic() - started)
    print(f"Run completed in {elapsed}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
