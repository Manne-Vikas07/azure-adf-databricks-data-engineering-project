# Databricks notebook source
df=spark.read.format('parquet')\
    .option('inferSchema', True)\
    .load('abfss://bronze@vikasproject2storage.dfs.core.windows.net/rawdata')
    


# COMMAND ----------

df.count()

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df=df.withColumn("Model_Category",split(col("Model_ID"),"-")[0])

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('Units_sold',col("Units_Sold").cast(StringType())).display()

# COMMAND ----------

df=df.withColumn("RevPerUnit",col("Revenue")/col("Units_Sold"))
df.display()

# COMMAND ----------

df.groupBy("Year","BranchName").agg(sum("Units_Sold").alias("Total_Units")).sort("year","Total_Units",ascending=[1,0]).display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.format("parquet")\
        .mode('overwrite')\
        .option('path','abfss://silver@vikasproject2storage.dfs.core.windows.net/carsales')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #Querying Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@vikasproject2storage.dfs.core.windows.net/carsales`

# COMMAND ----------

 spark.read.parquet("abfss://silver@vikasproject2storage.dfs.core.windows.net/carsales").printSchema()

# COMMAND ----------

