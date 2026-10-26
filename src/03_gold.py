# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold: daily sales by category
# MAGIC
# MAGIC Reads `silver_orders` and builds `gold_daily_sales`
# MAGIC (orders, quantity and revenue per day per category, cancelled orders excluded).

# COMMAND ----------

from transformations import build_daily_sales, table_name

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")
dbutils.widgets.text("schema", "sales")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

source_table = table_name(catalog, schema, "silver_orders")
target_table = table_name(catalog, schema, "gold_daily_sales")
print(f"{source_table}  ->  {target_table}")

# COMMAND ----------

gold_df = build_daily_sales(spark.table(source_table))

gold_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

print(f"Wrote {spark.table(target_table).count()} rows to {target_table}")

# COMMAND ----------

display(spark.table(target_table))
