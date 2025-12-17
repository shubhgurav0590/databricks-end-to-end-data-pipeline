# Databricks notebook source
df = spark.read.format('parquet').load("abfss://source@shubhamlake01.dfs.core.windows.net/orders")
df.display()

# COMMAND ----------

dbutils.widgets.text("file_name",'')

# COMMAND ----------

p_file_name = dbutils.widgets.get('file_name')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data reading

# COMMAND ----------

# reading a data through autoloader
df = spark.readStream.format('CloudFiles')\
    .option('CloudFiles.format','parquet')\
        .option('CloudFiles.schemaLocation',f"abfss://bronze@shubhamlake01.dfs.core.windows.net/checkpoint_{p_file_name}")\
            .load(f"abfss://source@shubhamlake01.dfs.core.windows.net/{p_file_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Writing

# COMMAND ----------

df.writeStream.format('parquet')\
    .outputMode('append')\
        .option('checkpointLocation',f"abfss://bronze@shubhamlake01.dfs.core.windows.net/checkpoint_{p_file_name}")\
            .option('path',f'abfss://bronze@shubhamlake01.dfs.core.windows.net/{p_file_name}')\
                .trigger(once=True)\
                    .start()

# COMMAND ----------

df = spark.read.format('parquet')\
    .load(f'abfss://bronze@shubhamlake01.dfs.core.windows.net/{p_file_name}')


display(df)   


# COMMAND ----------

