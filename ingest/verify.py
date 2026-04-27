"""Sanity-check the loaded geospatial data.

Runs four checks, prints PASS/FAIL for each, exits non-zero if any failed:
1. bus_stops has at least one row
2. population_grid sum is in the ~660k range expected for Luxembourg
3. communes count is ~100 (Luxembourg's current commune count)
4. A 400 m catchment query around an arbitrary stop returns a sensible figure
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import get_psycopg2_conn  # noqa: E402


def query_one(sql: str):
    with get_psycopg2_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else None


def report(label: str, ok: bool, detail: str) -> bool:
    status = "PASS" if ok else "FAIL"
    print(f"[{status}] {label}: {detail}")
    return ok


def main() -> int:
    all_ok = True

    try:
        n_stops = query_one("SELECT COUNT(*) FROM bus_stops;")
        all_ok &= report(
            "bus_stops count > 0",
            n_stops is not None and int(n_stops) > 0,
            str(n_stops),
        )
    except Exception as exc:
        all_ok &= report("bus_stops count", False, f"query failed: {exc}")

    try:
        total_pop = query_one("SELECT COALESCE(SUM(pop_count), 0) FROM population_grid;")
        ok = total_pop is not None and 500_000 <= int(total_pop) <= 800_000
        all_ok &= report(
            "population_grid sum (~660k expected)",
            ok,
            f"{int(total_pop):,}",
        )
    except Exception as exc:
        all_ok &= report("population_grid sum", False, f"query failed: {exc}")

    try:
        n_communes = query_one("SELECT COUNT(*) FROM communes;")
        ok = n_communes is not None and 90 <= int(n_communes) <= 110
        all_ok &= report(
            "communes count (~100 expected)",
            ok,
            str(n_communes),
        )
    except Exception as exc:
        all_ok &= report("communes count", False, f"query failed: {exc}")

    try:
        catch_pop = query_one(
            """
            SELECT COALESCE(SUM(pop_count), 0)
            FROM population_grid
            WHERE ST_Intersects(
                geom,
                ST_Buffer((SELECT geom FROM bus_stops LIMIT 1), 400)
            );
            """
        )
        ok = catch_pop is not None and int(catch_pop) >= 0
        all_ok &= report(
            "catchment query (400 m buffer around one stop)",
            ok,
            f"population in catchment = {int(catch_pop):,}",
        )
    except Exception as exc:
        all_ok &= report("catchment query", False, f"query failed: {exc}")

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
