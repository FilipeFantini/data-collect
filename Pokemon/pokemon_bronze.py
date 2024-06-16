# Databricks notebook source
(spark.read
      .json("/mnt/datalake/pokemon/pokemon")
      .createOrReplaceTempView("pokemon"))

# COMMAND ----------

query = '''
SELECT 
ingestion_date
,poke.*
FROM pokemon
LATERAL VIEW explode(results) as poke
QUALIFY row_number() OVER (PARTITION BY poke.name ORDER BY ingestion_date desc) = 1
'''

df = spark.sql(query).coalesce(1)

(df.write.format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .saveAsTable("bronze.pokemon.pokemon"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE bronze.pokemon

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.pokemon.pokemon limit 10
