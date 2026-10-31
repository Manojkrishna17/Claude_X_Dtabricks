CREATE SCHEMA IF NOT EXISTS claudecatalog.cur
COMMENT 'Curated layer: aggregated views for consumption';

CREATE OR REPLACE VIEW claudecatalog.cur.bookings_per_city
COMMENT 'Number of bookings per airport city'
AS
SELECT
  airport_city AS city,
  COUNT(booking_id) AS booking_count
FROM claudecatalog.enr.bookings_enriched
GROUP BY airport_city;
