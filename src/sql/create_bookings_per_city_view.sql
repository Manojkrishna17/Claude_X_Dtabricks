CREATE SCHEMA IF NOT EXISTS {catalog}.{cur_schema}
COMMENT 'Curated layer: aggregated views for consumption';

CREATE OR REPLACE VIEW {catalog}.{cur_schema}.bookings_per_city
COMMENT 'Number of bookings per airport city'
AS
SELECT
  airport_city AS city,
  COUNT(booking_id) AS booking_count
FROM {catalog}.{enr_schema}.bookings_enriched
GROUP BY airport_city;
