# Databricks notebook source
# MAGIC %md
# MAGIC Fact table
# MAGIC
# MAGIC data reading

# COMMAND ----------

df = spark.sql('select * from databricks_cat.silver.orders')
df.display()

# COMMAND ----------

df_dimcus = spark.sql('select DimCustomerKey,customer_id as dim_customer_id from databricks_cat.gold.dimcustomer')
df_dimpro = spark.sql('select product_id as DimProductKey , product_id as dim_product_id from databricks_cat.gold.dimproducts')
# df_dimcus.display()

# COMMAND ----------

df_fact = df.join(df_dimcus, df['customer_id'] == df_dimcus['dim_customer_id'], "left") \
            .join(df_dimpro, df['product_id'] == df_dimpro['dim_product_id'], "left")



df_fact_new = df_fact.drop("dim_customer_id", "dim_product_id", "customer_id", "product_id")
display(df_fact_new)


# COMMAND ----------

# upsert on the fact tables
from delta.tables import DeltaTable

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists("databricks_cat.gold.factorders"):
    
    dlt_obj = DeltaTable.forName(spark, "databricks_cat.gold.factorders")

    merge_condition = """
        trg.order_id = src.order_id AND
        trg.DimCustomerKey = src.DimCustomerKey AND
        trg.DimProductKey = src.DimProductKey
    """

    dlt_obj.alias('trg') \
        .merge(df_fact_new.alias('src'), merge_condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

else:
    df_fact_new.write.format("delta") \
        .option('path','abfss://gold@shubhamlake01.dfs.core.windows.net/factorders')\
            .saveAsTable("databricks_cat.gold.factorders")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cat.gold.factorders
# MAGIC -- limit 10

# COMMAND ----------

