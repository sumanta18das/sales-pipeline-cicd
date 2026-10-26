# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver: clean orders
# MAGIC
# MAGIC Reads `bronze_orders`, applies the cleaning rules from `transformations.clean_orders`
# MAGIC and saves the result to `silver_orders`.
# MAGIC
# MAGIC Rules: trim text, safe type casting, drop bad rows, remove duplicates, add `amount`.

# COMMAND ----------

from transformations import clean_orders, table_name

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")
dbutils.widgets.text("schema", "sales")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

source_table = table_name(catalog, schema, "bronze_orders")
target_table = table_name(catalog, schema, "silver_orders")
print(f"{source_table}  ->  {target_table}")

# COMMAND ----------

bronze_df = spark.table(source_table)
silver_df = clean_orders(bronze_df)

silver_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

bronze_count = bronze_df.count()
silver_count = spark.table(target_table).count()
print(f"Bronze rows : {bronze_count}")
print(f"Silver rows : {silver_count}")
print(f"Dropped     : {bronze_count - silver_count} bad or duplicate rows")

# COMMAND ----------

display(spark.table(target_table))
