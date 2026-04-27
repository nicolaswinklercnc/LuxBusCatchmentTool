"""Run all three Luxembourg data ingest scripts in order, then build spatial indexes.

Each sub-script's main() is called via importlib so a failure in one does not
stop the others. Final summary prints row counts queried back from the DB.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import get_psycopg2_conn  # noqa: E402

SCRIPTS = [
    ("bus_stops", "download_gtfs"),
    ("population_grid", "download_population"),
    ("communes", "download_communes"),
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


def create_indexes() -> None:
    with get_psycopg2_conn() as conn, conn.cursor() as cur:
        for table, _ in SCRIPTS:
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS {table}_geom_idx "
                f"ON {table} USING GIST(geom);"
            )
        conn.commit()
    print("Spatial indexes ensured on bus_stops, population_grid, communes.")


def print_summary() -> None:
    print("\nTable           | Rows loaded")
    print("----------------|------------")
    for table, _ in SCRIPTS:
        n = row_count(table)
        n_str = "FAILED" if n is None else f"{n}"
        print(f"{table:<15} | {n_str}")


def main() -> int:
    check_postgis()
    for _table, module_name in SCRIPTS:
        run_one(module_name)
    try:
        create_indexes()
    except Exception as exc:
        print(f"WARNING: could not create spatial indexes: {exc}", file=sys.stderr)
    print_summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
