# Databricks notebook source

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# my_rules = {
#     "rule1: product_id  is not null",
#     "rule2: product_name is not null"
# }

# COMMAND ----------

# @dlt.table()

# def DimProducts_stage():
#     df = spark.readStream.table("databricks_cat.silver.products")
#      return df
    


# COMMAND ----------

# @dlt.view()

# def DimProducts_view():
#     df = spark.readStream.table("DimProducts_stage")
#     return df

# COMMAND ----------

# @dlt.create_streaming_table("DimProducts")

# COMMAND ----------

# dlt.apply_changes(
#     target = "DimProducts",
#     source =  "Live.DimProducts_view",
#     sequence_by = "product_id",
#     keys = ["product_id"],
#     stored_as_scd_type = 2
# )

# spark.sql("select * from databricks_cat.silver.products").display()
# spark.sql("select * from databricks_cat.silver.products limit 10").show()

# COMMAND ----------

# from pyspark.sql.functions import *
# from delta.tables import DeltaTable

# # Source table (latest snapshot from silver layer)
# df_src = spark.table("databricks_cat.silver.products")

# # Add load timestamp
# df_src = df_src.withColumn("load_ts", current_timestamp())

# # Target table (dimension)
# target_table = "databricks_cat.gold.DimProducts"

# # Check if dim table exists
# if spark.catalog.tableExists(target_table):
    
#     dim_tbl = DeltaTable.forName(spark, target_table)

#     # Existing active records (where end_date is null)
#     df_dim = spark.table(target_table).filter("end_date IS NULL")

#     # Join source with existing active dimension rows
#     df_join = df_src.alias("src") \
#         .join(df_dim.alias("dim"), "product_id", "left")

#     # Detect records where something has changed
#     df_changes = df_join.filter("""
#         dim.product_name <> src.product_name OR
#         dim.category <> src.category OR
#         dim.price <> src.price OR
#         dim.description <> src.description
#     """)

#     # 1️⃣ EXPIRE OLD RECORDS (set end_date)
#     dim_tbl.alias("dim").merge(
#         df_changes.alias("src"),
#         "dim.product_id = src.product_id AND dim.end_date IS NULL"
#     ).whenMatchedUpdate(set={"end_date": "current_timestamp()"}).execute()

#     # 2️⃣ INSERT NEW VERSION RECORDS
#     df_new_versions = df_changes.select(
#         "src.*",
#         lit(current_timestamp()).alias("start_date"),
#         lit(None).cast("timestamp").alias("end_date")
#     )

#     df_new_versions.write.format("delta").mode("append").saveAsTable(target_table)

# else:
#     # FIRST LOAD → INSERT ALL RECORDS AS ACTIVE VERSION
#     df_initial = df_src.withColumn("start_date", current_timestamp()) \
#                        .withColumn("end_date", lit(None).cast("timestamp"))
    
#     df_initial.write.format("delta").mode("overwrite").saveAsTable(target_table)


# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import DeltaTable

# Source table (from silver layer)
df_src = spark.table("databricks_cat.silver.products")
df_src = df_src.withColumn("load_ts", current_timestamp())

# Target dimension table
target_table = "databricks_cat.gold.DimProducts"

# Check if DimProducts exists
if spark.catalog.tableExists(target_table):

    dim_tbl = DeltaTable.forName(spark, target_table)

    # Active dimension records (end_date IS NULL)
    df_dim = spark.table(target_table).filter("end_date IS NULL")

    # Join source with dimension
    df_join = df_src.alias("src") \
        .join(df_dim.alias("dim"), "product_id", "left")

    # Detect changed values (SCD2 change detection)
    df_changes = df_join.filter(
        (col("dim.product_name") != col("src.product_name")) |
        (col("dim.category") != col("src.category")) |
        (col("dim.brand") != col("src.brand")) |
        (col("dim.price") != col("src.price")) |
        (col("dim.discount_price") != col("src.discount_price"))
    )

    # 1️⃣ EXPIRE OLD RECORDS
    dim_tbl.alias("dim").merge(
        df_changes.alias("src"),
        "dim.product_id = src.product_id AND dim.end_date IS NULL"
    ).whenMatchedUpdate(
        set={"end_date": current_timestamp()}
    ).execute()

    # 2️⃣ INSERT NEW VERSION RECORDS
    df_new_versions = df_changes.select(
        "src.*",
        current_timestamp().alias("start_date"),
        lit(None).cast("timestamp").alias("end_date")
    )

    df_new_versions.write.format("delta").mode("append").saveAsTable(target_table)

else:
    # First load → insert all as active versions
    df_initial = (
        df_src.withColumn("start_date", current_timestamp())
              .withColumn("end_date", lit(None).cast("timestamp"))
    )

    df_initial.write.format("delta").mode("overwrite").saveAsTable(target_table)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cat.gold.dimproducts
# MAGIC

# COMMAND ----------

