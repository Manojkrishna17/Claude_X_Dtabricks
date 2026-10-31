-- Each assert_true raises an error (failing the task and the job) if its condition is false.

SELECT assert_true(
  (SELECT COUNT(*) FROM {catalog}.{enr_schema}.bookings_enriched)
    = (SELECT COUNT(*) FROM {catalog}.{raw_schema}.bookings),
  'Row count mismatch: bookings_enriched vs raw.bookings (join dropped or duplicated rows)'
);

SELECT assert_true(
  (SELECT COUNT(*) FROM {catalog}.{enr_schema}.bookings_enriched WHERE passenger_name IS NULL) = 0,
  'Found bookings with no matching passenger'
);

SELECT assert_true(
  (SELECT COUNT(*) FROM {catalog}.{enr_schema}.bookings_enriched WHERE airport_name IS NULL) = 0,
  'Found bookings with no matching airport'
);
