# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Reading

# COMMAND ----------

df = spark.read.format('parquet').load('abfss://bronze@shubhamlake01.dfs.core.windows.net/products')

# COMMAND ----------

df.limit(4).display()

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df.createOrReplaceTempView('products')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function databricks_cat.bronze.discount_func(p_price double)
# MAGIC returns double
# MAGIC language sql
# MAGIC return p_price * 0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id,price, databricks_cat.bronze.discount_func(price) as discount_price
# MAGIC from products

# COMMAND ----------

df = df.withColumn('discount_price',expr('databricks_cat.bronze.discount_func(price)'))

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function databricks_cat.bronze.upper_func(p_brand string)
# MAGIC returns string
# MAGIC language python
# MAGIC as
# MAGIC $$
# MAGIC return p_brand.upper()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id ,brand,databricks_cat.bronze.upper_func(brand) as brand_upper
# MAGIC from products

# COMMAND ----------

df.write.format('delta').mode('overwrite')\
    .save("abfss://silver@shubhamlake01.dfs.core.windows.net/products")
        

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cat.silver.products
# MAGIC using delta
# MAGIC location "abfss://silver@shubhamlake01.dfs.core.windows.net/products"
# MAGIC
# MAGIC