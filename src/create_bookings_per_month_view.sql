CREATE SCHEMA IF NOT EXISTS claudecatalog.cur
COMMENT 'Curated layer: aggregated views for consumption';

CREATE OR REPLACE VIEW claudecatalog.cur.bookings_per_month
COMMENT 'Number of bookings and revenue per month'
AS
SELECT
  DATE_TRUNC('month', booking_date) AS booking_month,
  COUNT(booking_id) AS booking_count,
  SUM(amount) AS total_revenue
FROM claudecatalog.enr.bookings_enriched
GROUP BY DATE_TRUNC('month', booking_date);
