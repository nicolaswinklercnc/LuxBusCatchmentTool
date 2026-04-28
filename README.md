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

The repo uses two env files, both gitignored:

| File | When loaded | What it points at |
|---|---|---|
| `.env.local` | default — `ENVIRONMENT` unset or anything other than `production` | local Docker postgis (`localhost:5432`) |
| `.env.production` | `ENVIRONMENT=production` | Supabase (the live database) |

`ingest/db.py` and `api/db.py` pick the right file at startup based on the `ENVIRONMENT` shell variable. The selected file's name is printed to stderr (e.g. `[db] ENVIRONMENT=development`) so you always see which database the process is about to talk to.

```bash
# Local development (default — never touches production):
python ingest/run_all.py

# Production data load (deliberate opt-in):
ENVIRONMENT=production python ingest/run_all.py
```

To set up for the first time:
```bash
cp .env.example .env.local         # already configured for local Docker
cp .env.example .env.production    # then uncomment the production block, paste the Supabase URL
```

The Fly.io API and the GitHub Pages frontend never load these files — they get configuration from Fly secrets and from the static `frontend/.env.development` injection respectively.

## Deployment

| Component | Target | How |
|---|---|---|
| Frontend | GitHub Pages — <https://nicolaswinklercnc.github.io/LuxBusCatchmentTool> | Automatic via `.github/workflows/deploy.yml` on push to `main` |
| Backend  | Fly.io app `lux-bus-catchment-api` (region `cdg`, Paris) | Manual: `flyctl deploy` from terminal |
| Database | Fly.io managed Postgres + PostGIS extension | See `fly-postgres-notes.md` |

### Backend secrets

Set the connection string (and any other secret) on Fly:

```bash
flyctl secrets set DATABASE_URL="postgresql://user:pass@host:5432/db"
```

Never commit credentials. `.env`, `.env.local`, and `.env.production` are all gitignored; `.env.example` is the committed template.

### Keep-warm

`fly.toml` is configured with `auto_stop_machines = "off"` and `min_machines_running = 1` so the API stays warm. As an extra safeguard, an external cron at [cron-job.org](https://cron-job.org) pings `https://lux-bus-catchment-api.fly.dev/ping` every 5 minutes.

## Milestones

- **M0 — scaffold** ✅ repo, compose stack, frontend placeholder, Pages CI
- M1 — ingest GTFS bus stops + Luxembourg population grid into PostGIS
- M2 — compute walking-distance catchments per stop
- M3 — API endpoint returning catchments + population counts
- M4 — frontend visualisation with stop selection and choropleth
