# Databricks notebook source
from pyspark.sql.functions import explode
from pyspark.sql import functions as F

df = spark.read.option("multiline", "true").json("/Volumes/raw/weather/history")

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import arrays_zip, col, explode_outer
from pyspark.sql import functions as F


# Explode array columns
df2 = df.withColumn("forecast_exp", explode_outer(col("forecast.forecastday"))) \
        .withColumn("hour_exp", explode_outer(col("forecast_exp.hour"))) 


df2 = df2.select(
     "forecast_exp.astro.*"
     ,"forecast_exp.date"
     ,"forecast_exp.date_epoch"
     ,"forecast_exp.day.*"
     ,"hour_exp.*"
     ,"location.*"

)

df2.display()

# COMMAND ----------

df2 = df2.drop("condition")

df_columns = df2.columns
duplicate_col_index = [idx for idx, val in enumerate(df_columns) if val in df_columns[:idx]]
duplicate_col_index

for i in duplicate_col_index:
    df_columns[i] = f"{df_columns[i]}_duplicated"

df2 = df2.toDF(*df_columns)


df2.selectExpr("uv_duplicated as uv_hour")
df2.printSchema()

# COMMAND ----------

df2.write.mode("overwrite").saveAsTable("silver.weather.weather_history")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from silver.weather.weather_history limit 50
