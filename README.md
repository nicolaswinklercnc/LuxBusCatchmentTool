# Luxembourg Bus Catchment Tool

Geospatial web tool that shows how many residents fall within walking distance of each bus stop in Luxembourg.

## Stack

- **PostGIS** — spatial database (PostgreSQL 15 + PostGIS 3.3)
- **FastAPI** — Python backend serving catchment queries
- **MapLibre GL JS** — frontend map rendering
- **Python ingest** — GTFS + population grid loaders (`gtfs-kit`, `geopandas`)

## Coordinate reference system

All spatial operations use **EPSG:3035 (ETRS89 / LAEA Europe)**. This is a metre-based projected CRS, so distances, buffers, and areas are computed in metres rather than degrees. WGS84 (EPSG:4326) is used only at the I/O boundary (map display, GeoJSON exchange).

## Project layout

```
ingest/              Python data download / loading scripts
api/                 FastAPI backend (api/main.py exposes /health)
frontend/            Static HTML/JS map (deployed to GitHub Pages)
.github/workflows/   CI: deploy.yml publishes frontend/ to Pages on push to main
docker-compose.yml   postgis + api services
requirements.txt     Python dependencies
.env.example         Template for local DB credentials
```

## Quickstart

```bash
cp .env.example .env.local
docker compose up -d
curl http://localhost:8000/health   # -> {"status":"ok","db":"connected", ...}
python frontend/serve.py            # opens http://localhost:3000
```

## Environments

The repo uses three env files, all gitignored:

| File | When loaded | What it points at |
|---|---|---|
| `.env.local` | default — `ENVIRONMENT` unset or `development` | local Docker postgis (`localhost:5432`) |
| `.env.staging` | `ENVIRONMENT=staging` | Supabase staging project |
| `.env.production` | `ENVIRONMENT=production` | Supabase production project |

`ingest/db.py` and `api/db.py` pick the right file at startup based on the `ENVIRONMENT` shell variable. The selected file's name is printed to stderr (e.g. `[db] ENVIRONMENT=development`) so you always see which database the process is about to talk to.

```bash
# Local development (default — never touches a remote DB):
python ingest/run_all.py

# Staging (deliberate opt-in):
ENVIRONMENT=staging python ingest/run_all.py

# Production (deliberate opt-in):
ENVIRONMENT=production python ingest/run_all.py
```

To set up for the first time:
```bash
cp .env.example .env.local         # already configured for local Docker
cp .env.example .env.staging       # then uncomment the staging block, paste the Supabase URL
cp .env.example .env.production    # then uncomment the production block, paste the Supabase URL
```

The Fly.io APIs and the GitHub Pages frontend never load these files — they get configuration from Fly secrets and from the static `frontend/.env.development` injection respectively.

## Deployment

The repo runs a three-branch pipeline. Pushes are tested on every branch; promotion is by branch:

| Branch | Trigger | Workflow | What ships |
|---|---|---|---|
| `feature/*` | push | `.github/workflows/test.yml` | tests only — no deploy |
| `develop` | push | `.github/workflows/deploy-staging.yml` | tests, then API to staging Fly app |
| `main` | push | `.github/workflows/deploy-production.yml` | tests, then API to prod Fly app + frontend to GitHub Pages |

| Component | Target | How |
|---|---|---|
| Frontend (production) | GitHub Pages — <https://nicolaswinklercnc.github.io/LuxBusCatchmentTool> | `deploy-production.yml` on push to `main` |
| Backend (staging) | Fly.io app `lux-bus-catchment-api-staging` — <https://lux-bus-catchment-api-staging.fly.dev> | `deploy-staging.yml` on push to `develop` |
| Backend (production) | Fly.io app `lux-bus-catchment-api` (region `cdg`, Paris) — <https://lux-bus-catchment-api.fly.dev> | `deploy-production.yml` on push to `main` |
| Database | Supabase (separate projects per environment) | Connection string set via Fly secrets |

Tests run against a tiny PostGIS fixture committed at `api/test_fixtures/init.sql` (3 stops / 2 communes / 4 cells). Locally, the same `pytest api/test_main.py` runs against the full ingested dataset — env vars in `test.yml` swap the expected counts to fixture values for CI.

### One-time setup: GitHub Secrets

The deploy workflows expect these repo-level secrets:

| Secret | Source | Used by |
|---|---|---|
| `FLY_API_TOKEN` | `flyctl auth token` | `deploy-staging.yml`, `deploy-production.yml` |

Set with `gh`:
```bash
gh secret set FLY_API_TOKEN --body "$(flyctl auth token)"
```

Each Fly app reads its own DATABASE_URL from Fly secrets:
```bash
flyctl secrets set DATABASE_URL="postgresql://user:pass@host:5432/db" --app lux-bus-catchment-api
flyctl secrets set DATABASE_URL="postgresql://user:pass@host:5432/db" --app lux-bus-catchment-api-staging
```

Never commit credentials. `.env`, `.env.local`, `.env.staging`, and `.env.production` are all gitignored; `.env.example` is the committed template.

### Keep-warm

`fly.toml` is configured with `auto_stop_machines = "off"` and `min_machines_running = 1` so the API stays warm. As an extra safeguard, an external cron at [cron-job.org](https://cron-job.org) pings `https://lux-bus-catchment-api.fly.dev/ping` every 5 minutes.

## Milestones

- **M0 — scaffold** ✅ repo, compose stack, frontend placeholder, Pages CI
- M1 — ingest GTFS bus stops + Luxembourg population grid into PostGIS
- M2 — compute walking-distance catchments per stop
- M3 — API endpoint returning catchments + population counts
- M4 — frontend visualisation with stop selection and choropleth
