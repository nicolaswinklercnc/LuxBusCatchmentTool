-- Catchment query: residents within radius of a stop
-- Parameters: %(stop_id)s, %(radius_m)s
SELECT
  bs.stop_id,
  bs.stop_name,
  bs.commune,
  %(radius_m)s::integer AS radius_m,
  COUNT(pg.grid_id) AS cells_intersected,
  COALESCE(SUM(pg.pop_count), 0) AS residents,
  COALESCE(SUM(pg.pop_under15), 0) AS residents_under15,
  COALESCE(SUM(pg.pop_working_age), 0) AS residents_working_age,
  COALESCE(SUM(pg.pop_over65), 0) AS residents_over65,
  ST_AsGeoJSON(ST_Transform(ST_Buffer(bs.geom, %(radius_m)s), 4326)) AS catchment_geojson
FROM bus_stops bs
LEFT JOIN population_grid pg
  ON ST_Intersects(pg.geom, ST_Buffer(bs.geom, %(radius_m)s))
WHERE bs.stop_id = %(stop_id)s
GROUP BY bs.stop_id, bs.stop_name, bs.commune, bs.geom;
