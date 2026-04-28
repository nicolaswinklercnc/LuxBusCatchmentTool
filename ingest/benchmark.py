"""Benchmark the canonical catchment query at the rate the API will hit it.

Picks 20 random stops, runs the 400m catchment for each, and reports
min/avg/p95/max. Prints PASS if p95 is under the 200ms budget.
"""
from __future__ import annotations

import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import get_psycopg2_conn  # noqa: E402

CATCHMENT_SQL_FILE = (
    Path(__file__).resolve().parent.parent / "api" / "sql" / "catchment.sql"
)
N_STOPS = 20
RADIUS_M = 400
P95_BUDGET_MS = 200


def _p95(samples_sorted: list[float]) -> float:
    if not samples_sorted:
        return 0.0
    if len(samples_sorted) == 1:
        return samples_sorted[0]
    return statistics.quantiles(samples_sorted, n=20)[-1]


def main() -> int:
    sql = CATCHMENT_SQL_FILE.read_text(encoding="utf-8")
    with get_psycopg2_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT stop_id FROM bus_stops ORDER BY random() LIMIT %s;",
            (N_STOPS,),
        )
        stop_ids = [r[0] for r in cur.fetchall()]
        if not stop_ids:
            print("ERROR: bus_stops is empty — run ingest/run_all.py first.")
            return 1
        if len(stop_ids) < N_STOPS:
            print(f"Only {len(stop_ids)} stops available — running anyway.")

        # Warm-up so the first measurement does not include planner cache
        # warmup or initial connection-level overhead.
        cur.execute(sql, {"stop_id": stop_ids[0], "radius_m": RADIUS_M})
        cur.fetchall()

        timings_ms: list[float] = []
        for sid in stop_ids:
            t0 = time.perf_counter()
            cur.execute(sql, {"stop_id": sid, "radius_m": RADIUS_M})
            cur.fetchall()
            timings_ms.append((time.perf_counter() - t0) * 1000)

    sorted_ms = sorted(timings_ms)
    mn = sorted_ms[0]
    mx = sorted_ms[-1]
    avg = statistics.mean(sorted_ms)
    p95 = _p95(sorted_ms)
    print(f"Ran {len(timings_ms)} queries at {RADIUS_M}m:")
    print(f"  min  {mn:6.1f} ms")
    print(f"  avg  {avg:6.1f} ms")
    print(f"  p95  {p95:6.1f} ms")
    print(f"  max  {mx:6.1f} ms")
    verdict = "PASS" if p95 < P95_BUDGET_MS else "FAIL"
    print(f"\n{verdict}: p95 {p95:.1f}ms vs budget {P95_BUDGET_MS}ms")
    return 0 if verdict == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
