CREATE SCHEMA IF NOT EXISTS {catalog}.{cur_schema}
COMMENT 'Curated layer: aggregated views for consumption';

CREATE OR REPLACE VIEW {catalog}.{cur_schema}.bookings_per_month
COMMENT 'Number of bookings and revenue per month'
AS
SELECT
  DATE_TRUNC('month', booking_date) AS booking_month,
  COUNT(booking_id) AS booking_count,
  SUM(amount) AS total_revenue
FROM {catalog}.{enr_schema}.bookings_enriched
GROUP BY DATE_TRUNC('month', booking_date);
