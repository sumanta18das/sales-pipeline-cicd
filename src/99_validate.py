# Databricks notebook source
# MAGIC %md
# MAGIC # 99 Validate: data quality checks
# MAGIC
# MAGIC Runs after gold. If **any** check fails, this notebook raises an error, so:
# MAGIC - the job run fails
# MAGIC - `databricks bundle run` in GitHub Actions fails
# MAGIC - the CD pipeline stops and the change **never reaches the next environment**
# MAGIC
# MAGIC This is the "test" part of CD: we test the data, not only the code.

# COMMAND ----------

from transformations import run_quality_checks, table_name

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")
dbutils.widgets.text("schema", "sales")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

silver_df = spark.table(table_name(catalog, schema, "silver_orders"))
gold_df = spark.table(table_name(catalog, schema, "gold_daily_sales"))

# COMMAND ----------

errors = run_quality_checks(silver_df, gold_df)

if errors:
    print("DATA QUALITY CHECKS FAILED:")
    for e in errors:
        print(f"  - {e}")
    raise Exception(f"{len(errors)} data quality check(s) failed in {catalog}.{schema}")

print(f"All data quality checks passed for {catalog}.{schema}")
