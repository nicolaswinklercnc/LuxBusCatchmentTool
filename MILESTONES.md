# Milestones

## Milestone 1 — Data ingest into PostGIS
- **Status:** COMPLETE
- **Date:** 2026-04-28
- **Last commit:** `e60a544`
- Bus stops (GTFS), commune boundaries (data.public.lu), and Eurostat Census-GRID 2021 V2.2 1km² population grid are loaded into local PostGIS. Population grid is bbox-clipped from the LU communes boundary, then intersected against it; ~660k residents in ~700–900 cells. All spatial ops in EPSG:3035.

## Milestone 2 — Core catchment query in PostGIS
- **Status:** COMPLETE
- **Date:** 2026-04-28
- **Last commit:** `d46c60e`
- Canonical SQL at `api/sql/catchment.sql` (moved from `ingest/sql/` in `a5b64cb` so the Dockerfile can ship it with the API). Tested via `ingest/test_catchment.py`, `ingest/explain_query.py`, `ingest/benchmark.py`. p95 = 0.8 ms over 20 random stops at 400 m, well under the 200 ms budget. Query plan uses `bus_stops_pkey` and `population_grid_geom_idx` (no seq scans).

## Milestone 3 — FastAPI backend, live on Fly.io
- **Status:** COMPLETE
- **Date:** 2026-04-28
- **Live URL:** https://lux-bus-catchment-api.fly.dev
- Endpoints: `/health`, `/ping`, `/stops`, `/communes`, `/catchment`, `/commune/{name}/summary`. CORS scoped to GitHub Pages + localhost dev origins. Pooled SQLAlchemy (pool_size=5). Geometry returned in WGS84 for MapLibre. 11/11 pytest cases pass.
- **Backend:** Fly.io, region `cdg` (Paris). `fly.toml` healthcheck on `/ping` every 120 s (DB-independent so a transient Postgres issue can't take the machine out of rotation).
- **Database:** Supabase managed Postgres + PostGIS. Direct connection on port **5432** (not the transaction pooler on 6543 — the pooler rejected the plain `postgres` username).
- Live `/health` returns `db:connected` with the expected row counts.

## Milestone 4 — Frontend map on GitHub Pages
- **Status:** COMPLETE
- **Date:** 2026-04-28
- **Live URL:** https://nicolaswinklercnc.github.io/LuxBusCatchmentTool
- Single-file `frontend/index.html` (no build step). MapLibre 4.7.1 + OpenFreeMap Liberty basemap, locked to Luxembourg via `maxBounds`. Fetches `/stops` and `/communes` from the live API, renders stops as red dots (with labels at zoom ≥ 13) over faint blue commune outlines. Click a stop → popup with name/commune/residents and a translucent red 400 m catchment circle drawn from `/catchment`'s WGS84 polygon. Auto-deploys via `.github/workflows/deploy.yml` on push to `main`.

## Milestone 5 — Deployment verification + observability
- **Status:** NOT STARTED
