# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Reading

# COMMAND ----------

df = spark.read.format('parquet')\
    .load('abfss://bronze@shubhamlake01.dfs.core.windows.net/orders')

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumnRenamed('_rescued_data','rescued_data')

# COMMAND ----------

df = df.drop('rescued_data')
display(df)

# COMMAND ----------

# to convert into timestamp
from pyspark.sql.functions import to_timestamp, col

df =df.withColumn('order_date',to_timestamp(col('order_date')))

# COMMAND ----------

# to get a new column
from pyspark.sql.functions import year, col

df = df.withColumn('year',year(col('order_date')))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, desc, col

df1 = df.withColumn('flag',dense_rank().over(Window.partitionBy('year').orderBy(desc('total_amount'))))
df1.display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank,rank,row_number, desc, col
df1 = df1.withColumn('rank_flag',rank().over(Window.partitionBy('year').orderBy(desc('total_amount'))))
df1.display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank,rank,row_number, desc, col
df1 = df1.withColumn('row_num',row_number().over(Window.partitionBy('year').orderBy(desc('total_amount'))))
df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classes OOP concept

# COMMAND ----------

class windows:
    def dense_rank(self,df):
        df_dense_rank = df.withColumn('flag',dense_rank().over(Window.partitionBy('year').orderBy(desc('total_amount'))))
        return df_dense_rank

    def rank(self,df):
        df_rank = df.withColumn('rank_flag',rank().over(Window.partitionBy('year').orderBy(desc('total_amount'))))
        return df_rank

    def row_number(self,df):
        df_row_num = df.withColumn('row_num',row_number().over(Window.partitionBy('year').orderBy(desc('total_amount'))))
        return df_row_num

       

# COMMAND ----------

df_new = df

# COMMAND ----------

obj = windows()

# COMMAND ----------

df_result = obj.dense_rank(df_new) 
df_result .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Writing

# COMMAND ----------

df.write.format('delta').mode('overwrite').save("abfss://silver@shubhamlake01.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cat.silver.orders
# MAGIC using delta
# MAGIC location "abfss://silver@shubhamlake01.dfs.core.windows.net/orders"
# MAGIC

# COMMAND ----------

