"""Incrementally load enr.bookings_enriched from raw.bookings using Change Data Feed.

First run (or if the target is missing): full load.
Later runs: MERGE only the bookings changed since the last processed raw.bookings version.

Optional args (used for testing against scratch schemas): <raw_schema> <enr_schema>
"""
import sys

from pyspark.sql import SparkSession

CATALOG = "claudecatalog"
raw = sys.argv[1] if len(sys.argv) > 1 else "raw"
enr = sys.argv[2] if len(sys.argv) > 2 else "enr"

BOOKINGS = f"{CATALOG}.{raw}.bookings"
PASSENGERS = f"{CATALOG}.{raw}.passengers"
AIRPORTS = f"{CATALOG}.{raw}.airports"
TARGET = f"{CATALOG}.{enr}.bookings_enriched"
STATE = f"{CATALOG}.{enr}._load_state"

COLUMNS = [
    "booking_id", "flight_id", "amount", "booking_date", "passenger_id",
    "passenger_name", "gender", "nationality",
    "airport_id", "airport_name", "airport_city", "airport_country",
]


def enriched_select(bookings_ref: str) -> str:
    return f"""
        SELECT
          b.booking_id, b.flight_id, b.amount, b.booking_date,
          b.passenger_id, p.name AS passenger_name, p.gender, p.nationality,
          b.airport_id, a.airport_name, a.city AS airport_city, a.country AS airport_country
        FROM {bookings_ref} b
        LEFT JOIN {PASSENGERS} p ON b.passenger_id = p.passenger_id
        LEFT JOIN {AIRPORTS} a ON b.airport_id = a.airport_id
    """


spark = SparkSession.builder.getOrCreate()

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {CATALOG}.{enr}
    COMMENT 'Enriched layer: denormalized, joined tables'
""")

props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {BOOKINGS}").collect()}
if props.get("delta.enableChangeDataFeed") != "true":
    raise RuntimeError(f"Change Data Feed is not enabled on {BOOKINGS}")

# Read the version first: anything committed during the load is picked up next run (MERGE is idempotent).
current_version = spark.sql(f"DESCRIBE HISTORY {BOOKINGS} LIMIT 1").first()["version"]

spark.sql(f"CREATE TABLE IF NOT EXISTS {STATE} (table_name STRING, source_version BIGINT)")
row = spark.sql(f"SELECT source_version FROM {STATE} WHERE table_name = '{TARGET}'").first()
last_version = row["source_version"] if row else None

if last_version is None or not spark.catalog.tableExists(TARGET):
    print(f"Full load of {TARGET} at {BOOKINGS} version {current_version}")
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TARGET}
        COMMENT 'One big table: bookings joined with passengers and airports'
        AS {enriched_select(BOOKINGS)}
    """)
elif last_version >= current_version:
    # Don't sys.exit() here: Databricks reports SystemExit as a task failure.
    print(f"No new changes (processed version {last_version}, source at {current_version})")
else:
    start = last_version + 1
    print(f"Incremental load: {BOOKINGS} versions {start}..{current_version}")
    # Latest change per booking; a delete loses to an insert/update in the same commit (key was replaced).
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW bookings_changes AS
        SELECT booking_id, flight_id, amount, booking_date, passenger_id, airport_id,
               (_change_type = 'delete') AS is_delete
        FROM (
          SELECT *, ROW_NUMBER() OVER (
                   PARTITION BY booking_id
                   ORDER BY _commit_version DESC,
                            CASE WHEN _change_type = 'delete' THEN 0 ELSE 1 END DESC) AS rn
          FROM table_changes('{BOOKINGS}', {start}, {current_version})
          WHERE _change_type <> 'update_preimage'
        )
        WHERE rn = 1
    """)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW bookings_staged AS
        SELECT e.*, c.is_delete
        FROM ({enriched_select('bookings_changes')}) e
        JOIN bookings_changes c ON e.booking_id = c.booking_id
    """)
    set_clause = ", ".join(f"t.{c} = s.{c}" for c in COLUMNS if c != "booking_id")
    spark.sql(f"""
        MERGE INTO {TARGET} t
        USING bookings_staged s ON t.booking_id = s.booking_id
        WHEN MATCHED AND s.is_delete THEN DELETE
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED AND NOT s.is_delete THEN
          INSERT ({", ".join(COLUMNS)}) VALUES ({", ".join(f"s.{c}" for c in COLUMNS)})
    """)

if last_version is None or last_version < current_version:
    spark.sql(f"""
        MERGE INTO {STATE} t
        USING (SELECT '{TARGET}' AS table_name, CAST({current_version} AS BIGINT) AS source_version) s
        ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET t.source_version = s.source_version
        WHEN NOT MATCHED THEN INSERT (table_name, source_version) VALUES (s.table_name, s.source_version)
    """)
    print(f"State updated: {TARGET} processed through {BOOKINGS} version {current_version}")
