"""Run a SQL file statement by statement, filling in catalog/schema placeholders.

Args: <sql_file> <catalog> <raw_schema> <enr_schema> <cur_schema>
Placeholders in the file: {catalog} {raw_schema} {enr_schema} {cur_schema}
A failing statement (e.g. assert_true in the quality check) fails the task.
"""
import sys

from pyspark.sql import SparkSession

sql_file, catalog, raw_schema, enr_schema, cur_schema = sys.argv[1:6]

with open(sql_file) as f:
    text = f.read()
for placeholder, value in {
    "{catalog}": catalog,
    "{raw_schema}": raw_schema,
    "{enr_schema}": enr_schema,
    "{cur_schema}": cur_schema,
}.items():
    text = text.replace(placeholder, value)

# Drop comment lines, then split into statements (the files contain no ';' inside strings).
text = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--"))

spark = SparkSession.builder.getOrCreate()
for statement in (s.strip() for s in text.split(";")):
    if statement:
        print(f"Running: {statement.splitlines()[0]}")
        spark.sql(statement).collect()  # collect() forces execution so assert_true actually fires
print(f"Done: {sql_file}")
