# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df =spark.read.table('databricks_cat.default.regions')
   

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()


# COMMAND ----------

df.write.format('delta').mode('overwrite')\
    .save("abfss://silver@shubhamlake01.dfs.core.windows.net/regions")

# COMMAND ----------

df = spark.read.format('delta')\
    .load("abfss://silver@shubhamlake01.dfs.core.windows.net/products")


display(df)    

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cat.silver.regions
# MAGIC using delta
# MAGIC location "abfss://silver@shubhamlake01.dfs.core.windows.net/regions"
# MAGIC

# COMMAND ----------

