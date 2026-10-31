# Claude_X_Dtabricks
An end-to-end AI-assisted data engineering project using Databricks, Claude Code, AI Dev Kit, and MCP Server. Covers data ingestion, transformation, multi-table joins, automated workflows, and analytical reporting to convert raw data into clean, analytics-ready datasets.

## Architecture (medallion)

Everything lives in the Unity Catalog catalog `claudecatalog` and is deployed as a Declarative Automation Bundle (`databricks.yml`) with one job, `bookings_enriched_job`, and one AI/BI dashboard, `Bookings Overview`.

| Layer | Schema (prod / dev) | Contents |
|---|---|---|
| Bronze (raw) | `raw` (shared) | `bookings`, `passengers`, `airports` (loaded manually) |
| Silver (enriched) | `enr` / `dev_enr` | `bookings_enriched`: bookings joined with passengers and airports, loaded incrementally via Change Data Feed |
| Gold (curated) | `cur` / `dev_cur` | `bookings_per_city`, `revenue_per_city`, `bookings_per_month` views |
| Reporting | n/a | `Bookings Overview` dashboard on the `cur` views |

### Job flow

```
incremental_load_bookings_enriched   (src/incremental_load_bookings_enriched.py; CDF MERGE into enr)
        |
check_bookings_enriched_quality      (src/sql/..., row count and null-join checks; fails the job)
        |
   +----+--------------------+------------------------+
   |                         |                        |
create_bookings_per_city_view  create_revenue_per_city_view  create_bookings_per_month_view
```

All tasks run on serverless compute. SQL lives in `src/sql/` as templates (`{catalog}`, `{enr_schema}`, ...) and is executed by `src/run_sql.py`.

### Targets

| Target | Mode | Schemas | Schedule / alerts |
|---|---|---|---|
| `dev` (default) | development | `dev_enr`, `dev_cur` | none |
| `prod` | production | `enr`, `cur` | daily 06:00 UTC, email on failure |

### Deploy

```bash
databricks bundle validate --strict -t dev --profile <PROFILE>
databricks bundle deploy -t dev --profile <PROFILE>
databricks bundle run bookings_enriched_job -t dev --profile <PROFILE>
# production
databricks bundle deploy -t prod --profile <PROFILE>
```

Prerequisite: Change Data Feed enabled on the source table
(`ALTER TABLE claudecatalog.raw.bookings SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`).
