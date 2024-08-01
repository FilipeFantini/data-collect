# Databricks notebook source
def write_to_delta(catalog, schema, tables):
    for table in all_tables:
        path = f"/Volumes/raw/formula1/{table}/"
        df = spark.read.json(path).dropDuplicates()
        df.writeTo(f"{catalog}.{schema}.{table}").createOrReplace() #delta is the default for writeTo in DB

# COMMAND ----------

catalog = "silver"
schema = "formula1"
all_tables = ["car_data","drivers","meetings","sessions"]

write_to_delta(catalog, schema, all_tables)
