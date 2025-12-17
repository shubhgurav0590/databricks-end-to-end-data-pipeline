# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Reading

# COMMAND ----------

df = spark.read.format('parquet')\
    .load('abfss://bronze@shubhamlake01.dfs.core.windows.net/customers')


# COMMAND ----------

display(df)

# COMMAND ----------

df = df.drop('_rescued_data')
df.limit(4).display()

# COMMAND ----------

df = df.withColumn('domains',split(col('email'),'@')[1])

# COMMAND ----------

df.groupBy('domains').agg(count('customer_id').alias('total_customers')).sort('total_customers',ascending = False).display()

# COMMAND ----------

df_gmail = df.filter(col('domains') == 'gmail.com')
# df_gmail.display()


df_yahoo = df.filter(col('domains') == 'yahoo.com')
# df_yahoo.display()


# COMMAND ----------

# full name for the customer
df = df.withColumn('full_name',concat(col('first_name'),lit(' '),col('last_name')))
df = df.drop('first_name','last_name')

df.display()

# COMMAND ----------

df.write.format('delta').mode('overwrite').save('abfss://silver@shubhamlake01.dfs.core.windows.net/customers')

# COMMAND ----------

