-- Tiny PostGIS fixture for CI.
--
-- Why hand-rolled and not a pg_dump of production?
--   The real DB has ~660k population cells and ~2800 stops; not worth shipping
--   in CI. This fixture has 3 stops (one of each vehicle_type), 2 communes,
--   and 4 population cells, engineered so api/test_main.py's catchment and
--   commune-summary assertions resolve to predictable, hand-checkable totals.
--
-- Coordinates are in EPSG:3035 (metres) inside Luxembourg's extent.
--
-- Counts the tests pin via env (see api/test_main.py):
--   EXPECTED_STOPS=3   EXPECTED_COMMUNES=2   EXPECTED_POPULATION_CELLS=4
--   TEST_STOP_ID=BUS001   TEST_STOP_RESIDENTS_400M=100
--   TEST_COMMUNE_NAME=Luxembourg-Test

CREATE EXTENSION IF NOT EXISTS postgis;

DROP TABLE IF EXISTS bus_stops;
DROP TABLE IF EXISTS communes;
DROP TABLE IF EXISTS population_grid;

CREATE TABLE bus_stops (
  stop_id      TEXT PRIMARY KEY,
  stop_name    TEXT NOT NULL,
  commune      TEXT,
  vehicle_type TEXT NOT NULL,
  geom         GEOMETRY(Point, 3035) NOT NULL
);

CREATE TABLE communes (
  commune_id TEXT PRIMARY KEY,
  name       TEXT NOT NULL,
  canton     TEXT,
  lau2       TEXT,
  geom       GEOMETRY(MultiPolygon, 3035) NOT NULL
);

CREATE TABLE population_grid (
  grid_id          TEXT PRIMARY KEY,
  pop_count        INTEGER NOT NULL,
  pop_under15      INTEGER NOT NULL,
  pop_working_age  INTEGER NOT NULL,
  pop_over65       INTEGER NOT NULL,
  geom             GEOMETRY(Polygon, 3035) NOT NULL
);

CREATE INDEX bus_stops_geom_idx       ON bus_stops       USING GIST(geom);
CREATE INDEX communes_geom_idx        ON communes        USING GIST(geom);
CREATE INDEX population_grid_geom_idx ON population_grid USING GIST(geom);

-- Stops. BUS001 sits in the centre of cell C1 so a 400 m buffer is fully
-- inside C1 and intersects no other cell — keeps the catchment count exact.
INSERT INTO bus_stops (stop_id, stop_name, commune, vehicle_type, geom) VALUES
  ('BUS001',  'Test Bus Stop',  'Luxembourg-Test', 'bus',
     ST_SetSRID(ST_MakePoint(4080500, 2980500), 3035)),
  ('TRAM001', 'Test Tram Stop', 'Luxembourg-Test', 'tram',
     ST_SetSRID(ST_MakePoint(4082500, 2980500), 3035)),
  ('RAIL001', 'Test Rail Stop', 'Diekirch-Test',   'rail',
     ST_SetSRID(ST_MakePoint(4080500, 2985500), 3035));

-- Communes. Rectangles wide enough to fully contain their stops and the
-- relevant cells. Stored as MultiPolygon to match the production schema.
INSERT INTO communes (commune_id, name, canton, lau2, geom) VALUES
  ('LUX-T', 'Luxembourg-Test', 'Luxembourg', 'LU0001',
     ST_Multi(ST_GeomFromText(
       'POLYGON((4079000 2979000, 4084000 2979000, 4084000 2982000, 4079000 2982000, 4079000 2979000))',
       3035))),
  ('DIE-T', 'Diekirch-Test',   'Diekirch',   'LU0002',
     ST_Multi(ST_GeomFromText(
       'POLYGON((4079000 2984000, 4082000 2984000, 4082000 2987000, 4079000 2987000, 4079000 2984000))',
       3035)));

-- Population cells (1 km × 1 km).
--   C1 contains BUS001 (and its 400 m buffer) — pop 100 → catchment(BUS001, 400) = 100.
--   C2 contains TRAM001 — pop 50.
--   C3 contains RAIL001 — pop 30.
--   C4 sits far from every stop and outside every commune — sanity row.
INSERT INTO population_grid (grid_id, pop_count, pop_under15, pop_working_age, pop_over65, geom) VALUES
  ('C1', 100, 20, 60, 20,
     ST_GeomFromText('POLYGON((4080000 2980000, 4081000 2980000, 4081000 2981000, 4080000 2981000, 4080000 2980000))', 3035)),
  ('C2',  50, 10, 30, 10,
     ST_GeomFromText('POLYGON((4082000 2980000, 4083000 2980000, 4083000 2981000, 4082000 2981000, 4082000 2980000))', 3035)),
  ('C3',  30,  5, 20,  5,
     ST_GeomFromText('POLYGON((4080000 2985000, 4081000 2985000, 4081000 2986000, 4080000 2986000, 4080000 2985000))', 3035)),
  ('C4', 200, 40, 120, 40,
     ST_GeomFromText('POLYGON((4070000 2970000, 4071000 2970000, 4071000 2971000, 4070000 2971000, 4070000 2970000))', 3035));
