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
cp .env.example .env
docker compose up -d
curl http://localhost:8000/health   # -> {"status":"ok","crs":"EPSG:3035"}
```

Open `frontend/index.html` in a browser (or visit the GitHub Pages URL once deployed) to see the map.

## Milestones

- **M0 — scaffold** ✅ repo, compose stack, frontend placeholder, Pages CI
- M1 — ingest GTFS bus stops + Luxembourg population grid into PostGIS
- M2 — compute walking-distance catchments per stop
- M3 — API endpoint returning catchments + population counts
- M4 — frontend visualisation with stop selection and choropleth
