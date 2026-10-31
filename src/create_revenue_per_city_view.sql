CREATE SCHEMA IF NOT EXISTS claudecatalog.cur
COMMENT 'Curated layer: aggregated views for consumption';

CREATE OR REPLACE VIEW claudecatalog.cur.revenue_per_city
COMMENT 'Total booking revenue per airport city'
AS
SELECT
  airport_city AS city,
  SUM(amount) AS total_revenue,
  COUNT(booking_id) AS booking_count
FROM claudecatalog.enr.bookings_enriched
GROUP BY airport_city;
