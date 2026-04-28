"""Test the core catchment SQL queries directly against PostGIS.

Runs five tests covering: nearest-stop lookup, multi-radius catchment for a
single stop, query timing, multi-stop commune aggregation, and edge cases.

No FastAPI, no frontend — just raw SQL to confirm the spatial logic is sound.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import get_psycopg2_conn  # noqa: E402

# Approximate EPSG:3035 coordinates for Luxembourg City train station.
LUX_STATION_X = 4_038_000
LUX_STATION_Y = 3_000_000

RADII_M = (250, 400, 800, 1200)
TIMING_BUDGET_MS = 200


def test_1_nearest_stops(cur) -> str | None:
    print("\n== Test 1: nearest stops to Luxembourg City station ==")
    cur.execute(
        """
        SELECT stop_id, stop_name, commune,
          ST_Distance(geom, ST_SetSRID(ST_MakePoint(%s, %s), 3035)) AS dist_m
        FROM bus_stops
        ORDER BY dist_m
        LIMIT 5;
        """,
        (LUX_STATION_X, LUX_STATION_Y),
    )
    rows = cur.fetchall()
    print(f"{'stop_id':<14} {'stop_name':<35} {'commune':<20} {'dist_m':>8}")
    for sid, name, commune, dist in rows:
        print(
            f"{sid:<14} {(name or '')[:34]:<35} "
            f"{(commune or '-')[:19]:<20} {dist:>8.0f}"
        )
    return rows[0][0] if rows else None


def _radius_query(cur, stop_id: str, radius: int) -> tuple[int, int]:
    cur.execute(
        """
        SELECT
          COUNT(pg.grid_id) AS cells_intersected,
          COALESCE(SUM(pg.pop_count), 0) AS residents
        FROM population_grid pg
        WHERE ST_Intersects(
          pg.geom,
          ST_Buffer(
            (SELECT geom FROM bus_stops WHERE stop_id = %s),
            %s
          )
        );
        """,
        (stop_id, radius),
    )
    return cur.fetchone()


def test_2_multi_radius(cur, stop_id: str) -> None:
    print(f"\n== Test 2: catchment at 4 radii for stop_id={stop_id} ==")
    print(f"{'Radius':<8} {'Cells':>7} {'Residents':>12}")
    print(f"{'-' * 6:<8} {'-' * 5:>7} {'-' * 9:>12}")
    for radius in RADII_M:
        cells, residents = _radius_query(cur, stop_id, radius)
        print(f"{str(radius) + 'm':<8} {cells:>7} {residents:>12,}")


def test_3_timing(cur, stop_id: str) -> None:
    print(f"\n== Test 3: timing 400m query × 5 ==")
    times_ms: list[float] = []
    for _ in range(5):
        t0 = time.perf_counter()
        _radius_query(cur, stop_id, 400)
        times_ms.append((time.perf_counter() - t0) * 1000)
    mn = min(times_ms)
    mx = max(times_ms)
    avg = sum(times_ms) / len(times_ms)
    print(f"min={mn:.1f}ms  avg={avg:.1f}ms  max={mx:.1f}ms")
    if avg > TIMING_BUDGET_MS:
        print(
            f"WARNING: avg > {TIMING_BUDGET_MS}ms. Try "
            "`VACUUM ANALYZE bus_stops, population_grid;`."
        )


def test_4_commune_coverage(cur) -> None:
    print(f"\n== Test 4: 400m catchment over Luxembourg City commune ==")
    cur.execute(
        """
        SELECT name FROM communes
        WHERE name ILIKE 'luxembourg%'
        ORDER BY length(name)
        LIMIT 1;
        """
    )
    row = cur.fetchone()
    if not row:
        print("No commune matching 'luxembourg%' — skipping.")
        return
    (commune,) = row

    cur.execute("SELECT COUNT(*) FROM bus_stops WHERE commune = %s;", (commune,))
    (n_stops,) = cur.fetchone()

    # Residents within 400m of ANY stop in the commune (ST_Union of buffers
    # avoids double-counting overlapping coverage).
    cur.execute(
        """
        SELECT COALESCE(SUM(pg.pop_count), 0)
        FROM population_grid pg
        WHERE ST_Intersects(
          pg.geom,
          (SELECT ST_Union(ST_Buffer(geom, 400))
             FROM bus_stops WHERE commune = %s)
        );
        """,
        (commune,),
    )
    (covered,) = cur.fetchone()

    # Total population intersecting the commune polygon (approximate — grid
    # cells straddling the border get counted whole).
    cur.execute(
        """
        SELECT COALESCE(SUM(pg.pop_count), 0)
        FROM population_grid pg, communes c
        WHERE c.name = %s AND ST_Intersects(pg.geom, c.geom);
        """,
        (commune,),
    )
    (commune_pop,) = cur.fetchone()

    pct = (covered / commune_pop * 100) if commune_pop else 0.0
    print(f"Commune:                              {commune}")
    print(f"Stops in commune:                     {n_stops}")
    print(f"Residents within 400m of any stop:    {covered:,}")
    print(f"Total commune population (approx):    {commune_pop:,}")
    print(f"Coverage:                             {pct:.1f}%")


def test_5_edge_cases(cur, stop_id: str) -> None:
    print(f"\n== Test 5: edge cases ==")
    try:
        cells, residents = _radius_query(cur, "NONEXISTENT_STOP_ID", 400)
        print(
            f"Nonexistent stop_id, r=400:    cells={cells}, "
            f"residents={residents} (no error — OK)"
        )
    except Exception as exc:
        print(f"Nonexistent stop_id, r=400:    ERROR: {exc}")

    cells, residents = _radius_query(cur, stop_id, 0)
    print(f"radius=0    (stop {stop_id}): cells={cells}, residents={residents}")

    cells, residents = _radius_query(cur, stop_id, 5000)
    print(f"radius=5000 (stop {stop_id}): cells={cells}, residents={residents:,}")


def main() -> int:
    with get_psycopg2_conn() as conn, conn.cursor() as cur:
        stop_id = test_1_nearest_stops(cur)
        if not stop_id:
            print("ERROR: bus_stops is empty — run ingest/run_all.py first.")
            return 1
        test_2_multi_radius(cur, stop_id)
        test_3_timing(cur, stop_id)
        test_4_commune_coverage(cur)
        test_5_edge_cases(cur, stop_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
