# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze: load raw orders
# MAGIC
# MAGIC Reads the raw `orders.csv` file **as is** (all columns as text) and saves it to the
# MAGIC `bronze_orders` table with two audit columns: `_ingested_at` and `_source_file`.
# MAGIC
# MAGIC The **catalog** and **schema** come from job parameters, so the same notebook writes to
# MAGIC `dev_catalog`, `uat_catalog` or `prod_catalog` depending on where it is deployed.

# COMMAND ----------

import pandas as pd

from transformations import add_ingestion_metadata, table_name

# COMMAND ----------

# Widgets = notebook parameters. The job passes real values; defaults are for manual runs.
dbutils.widgets.text("catalog", "dev_catalog")
dbutils.widgets.text("schema", "sales")
dbutils.widgets.text("source_path", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_path = dbutils.widgets.get("source_path")

if not source_path:
    raise ValueError("source_path is empty. Run this notebook through the job.")

# Bundle paths may come without the /Workspace prefix. Python needs the full path.
if not source_path.startswith("/Workspace"):
    source_path = "/Workspace" + source_path

target_table = table_name(catalog, schema, "bronze_orders")
print(f"Environment catalog : {catalog}")
print(f"Source file         : {source_path}")
print(f"Target table        : {target_table}")

# COMMAND ----------

# Make sure the schema exists (first run in a new environment creates it).
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# Read every column as text. Bronze keeps raw data; cleaning happens in silver.
# The CSV file is deployed with the bundle, so we read it with pandas from the workspace.
pdf = pd.read_csv(source_path, dtype=str, keep_default_na=False)
raw_df = spark.createDataFrame(pdf)

bronze_df = add_ingestion_metadata(raw_df, source_path)

bronze_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

print(f"Wrote {bronze_df.count()} rows to {target_table}")

# COMMAND ----------

display(spark.table(target_table))
