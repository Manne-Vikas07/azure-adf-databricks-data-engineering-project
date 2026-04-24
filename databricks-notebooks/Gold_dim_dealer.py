# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #CREATE FLAG PARAMETER

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATING DIMENSIONAL MODEL

# COMMAND ----------

# MAGIC %md
# MAGIC ##Fetch Relative Columns

# COMMAND ----------

df_src = spark.sql('''
select distinct(Dealer_ID) as Dealer_ID, DealerName from parquet.`abfss://silver@vikasproject2storage.dfs.core.windows.net/carsales`
''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##dim_model sink - Initial and Incremental

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):

    df_sink = spark.sql("""
        SELECT dim_dealer_key, Dealer_ID, DealerName
        FROM cars_catalog.gold.dim_dealer
    """)

else:

    df_sink = spark.sql("""
        SELECT 1 as dim_dealer_key, Dealer_ID, DealerName
        FROM parquet.`abfss://silver@vikasproject2storage.dfs.core.windows.net/carsales`
        WHERE 1 = 0
    """)

# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering new records and old records

# COMMAND ----------

df_filter=df_src.join(df_sink,df_src['Dealer_ID']==df_sink['Dealer_ID'],'left').select(df_src['Dealer_ID'],df_src['DealerName'],df_sink['dim_dealer_key'])


# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_old**

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_dealer_key').isNotNull())
df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_new**

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_dealer_key').isNull()).select(df_src['Dealer_ID'],df_src['DealerName'])

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Surrogate Key

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch the max Surrogate Key from existing table

# COMMAND ----------

# Fetch the max surrogate key from existing table

if incremental_flag == '0':
    max_value = 1
else:
    max_value_df = spark.sql("""
        SELECT MAX(dim_dealer_key) 
        FROM cars_catalog.gold.dim_dealer
    """)
    
    max_value = max_value_df.collect()[0][0]+1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Surrogate Key column and ADD the max Surrogate key

# COMMAND ----------


df_filter_new = df_filter_new.withColumn(
    "dim_dealer_key",
    max_value + monotonically_increasing_id()
)

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Final DF - df_filter_old + df_filter_new

# COMMAND ----------

df_final=df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD TYPE - 1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# Incremental Run
if spark.catalog.tableExists("cars_catalog.gold.dim_dealer"):

    delta_tbl = DeltaTable.forPath(
        spark,
        "abfss://gold@vikasproject2storage.dfs.core.windows.net/dim_dealer"
    )

    delta_tbl.alias("trg").merge(
        df_final.alias("src"),
        "trg.dim_dealer_key = src.dim_dealer_key"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# Initial Run
else:
    df_final.write.format("delta") \
        .mode("overwrite") \
        .option("path", "abfss://gold@vikasproject2storage.dfs.core.windows.net/dim_dealer") \
        .saveAsTable("cars_catalog.gold.dim_dealer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_dealer

# COMMAND ----------

