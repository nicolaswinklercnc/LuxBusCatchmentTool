"""Run EXPLAIN ANALYZE on the canonical catchment query and report the plan.

Confirms that PostGIS spatial indexes are being used. If a `Seq Scan on
bus_stops` or `Seq Scan on population_grid` shows up, the GIST indexes are
not being consulted and the query will be slow at scale.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import get_psycopg2_conn  # noqa: E402

CATCHMENT_SQL_FILE = Path(__file__).resolve().parent / "sql" / "catchment.sql"
RADIUS_M = 400


def main() -> int:
    sql = CATCHMENT_SQL_FILE.read_text(encoding="utf-8")
    with get_psycopg2_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT stop_id FROM bus_stops LIMIT 1;")
        row = cur.fetchone()
        if not row:
            print("ERROR: bus_stops is empty — run ingest/run_all.py first.")
            return 1
        (stop_id,) = row
        cur.execute(
            f"EXPLAIN (ANALYZE, BUFFERS) {sql}",
            {"stop_id": stop_id, "radius_m": RADIUS_M},
        )
        plan_lines = [r[0] for r in cur.fetchall()]
    plan_text = "\n".join(plan_lines)
    print(plan_text)

    print("\n== Index check ==")
    has_index_scan = (
        "Index Scan" in plan_text or "Bitmap Index Scan" in plan_text
    )
    seq_scan_warn = False
    for table in ("bus_stops", "population_grid"):
        if f"Seq Scan on {table}" in plan_text:
            print(f"WARNING: Seq Scan on {table} — spatial index not used.")
            seq_scan_warn = True
    if has_index_scan and not seq_scan_warn:
        print("OK: index scans detected, no problematic seq scans.")
    elif not has_index_scan:
        print("WARNING: no Index Scan found in plan — check GIST indexes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
