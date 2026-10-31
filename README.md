# Claude_X_Dtabricks
An end-to-end AI-assisted data engineering project using Databricks, Claude Code, AI Dev Kit, and MCP Server. Covers data ingestion, transformation, multi-table joins, automated workflows, and analytical reporting to convert raw data into clean, analytics-ready datasets.

## Architecture (medallion)

Everything lives in the Unity Catalog catalog `claudecatalog` and is deployed as a Declarative Automation Bundle (`databricks.yml`) with one job, `bookings_enriched_job`.

| Layer | Schema | Contents |
|---|---|---|
| Bronze (raw) | `raw` | `bookings`, `passengers`, `airports` (loaded manually) |
| Silver (enriched) | `enr` | `bookings_enriched`: bookings joined with passengers and airports, loaded incrementally via Change Data Feed |
| Gold (curated) | `cur` | `bookings_per_city`, `revenue_per_city`, `bookings_per_month` views |

### Job flow (daily, 06:00 UTC, email on failure)

```
incremental_load_bookings_enriched   (Python, serverless; CDF MERGE into enr)
        |
check_bookings_enriched_quality      (row count and null-join checks; fails the job)
        |
   +----+--------------------+------------------------+
   |                         |                        |
create_bookings_per_city_view  create_revenue_per_city_view  create_bookings_per_month_view
```

### Deploy

```bash
databricks bundle validate --profile <PROFILE>
databricks bundle deploy -t dev --profile <PROFILE>
databricks bundle run bookings_enriched_job -t dev --profile <PROFILE>
```

Prerequisite: Change Data Feed enabled on the source table
(`ALTER TABLE claudecatalog.raw.bookings SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`).
