# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

try:
    init_load_flag = int(dbutils.widgets.get("init_load_flag"))
except:
    # Default value when running in Jobs/Pipeline (no widget)
    init_load_flag = 0

print("init_load_flag =", init_load_flag)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data reading from source

# COMMAND ----------

df = spark.sql("select * from databricks_cat.silver.customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ## Removing duplicates

# COMMAND ----------

df = df.dropDuplicates(subset = ["customer_id"])

# COMMAND ----------

if init_load_flag == 0:
    df_old = spark.sql('''select DimCustomerKey,customer_id,create_date,update_date 
                       from databricks_cat.gold.Dimcustomer
                       ''')
else:
     df_old = spark.sql('''select  0 DimCustomerKey, 0 customer_id, 0 create_date, 0 update_date 
                       from databricks_cat.silver.customer where 1= 0''')    

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Renaming with old records**

# COMMAND ----------

df_old = df_old.withColumnRenamed('DimCustomerKey','old_DimCustomerKey')\
    .withColumnRenamed('customer_id','old_customer_id')\
    .withColumnRenamed('create_date','old_create_date')\
    .withColumnRenamed('update_date','old_update_date')\
   

# COMMAND ----------

# MAGIC %md
# MAGIC **Applying join with old records**

# COMMAND ----------

df_join = df.join(df_old,df['customer_id'] == df_old['old_customer_id'] ,'left')
# df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Seperating old vs new records**

# COMMAND ----------

df_new = df_join.filter(df_join['old_DimCustomerKey'].isNull())

# df_join.filter(df_join['old_DimCustomerKey'].isNull()).display()'])

# COMMAND ----------

df_old = df_join.filter(df_join['old_DimCustomerKey'].isNotNull())

# COMMAND ----------

# Preparing old table
# dropping all the columns which are not required
df_old = df_old.drop('old_customer_id','old_update_date')
# renaming old dimcsustomerkey column to DimCustomerKey
df_old = df_old.withColumnRenamed('old_DimCustomerKey','DimCustomerKey')
# renaming old_create date column to create_date
df_old = df_old.withColumnRenamed('old_create_date','create_date')
df_old = df_old.withColumn('create_date',to_timestamp(col("create_date")))

# renaming old_create date column to create_date

df_old = df_old.withColumn('update_date',current_timestamp())







# COMMAND ----------

df_old.display()

# COMMAND ----------

# preparing new _df
#
# dropping all the columns which are not required
df_new = df_new.drop('old_DimCustomerKey','old_customer_id','old_update_date','old_create_date')



# renaming update date,create_date column with current timestamp

df_new = df_new.withColumn('update_date',current_timestamp())
df_new = df_new.withColumn('create_date',current_timestamp())

# COMMAND ----------

# df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Surrogate `key-- all the values`

# COMMAND ----------

df_new = df_new.withColumn('DimCustomerKey',monotonically_increasing_id() + lit(1))
# df_new.limit(10).display()


# COMMAND ----------

# adding max surrogate key
if init_load_flag == 1:
    max_surrogate_key = 0
else:
    df_maxsur = spark.sql("select  max(DimCustomerKey) as max_surrogate_key from databricks_cat.gold.DimCustomer")
    # converting df_maxsur to max_surrogate key variable
    max_surrogate_key = df_maxsur.collect()[0]['max_surrogate_key']

# COMMAND ----------

df_new = df_new.withColumn('DimCustomerKey',lit(max_surrogate_key)+  col('DimCustomerKey'))

# COMMAND ----------

# union of df_old and df_new
df_final = df_new.unionByName(df_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# scd- type-1

if spark.catalog.tableExists("databricks_cat.gold.DimCustomer"):
     dlt_obj = DeltaTable.forPath(spark,"abfss://gold@shubhamlake01.dfs.core.windows.net/DimCustomer")

     dlt_obj.alias('trg').merge(df_final.alias('src'),'trg.DimCustomerKey = src.DimCustomerKey')\
        .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
                .execute()      
else:
    df_final.write.mode('overwrite')\
        .option('path','abfss://gold@shubhamlake01.dfs.core.windows.net/DimCustomer')\
            .saveAsTable("databricks_cat.gold.DimCustomer")    


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cat.gold.dimcustomer
# MAGIC

# COMMAND ----------

