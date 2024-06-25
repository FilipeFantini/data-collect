# Databricks notebook source
from pyspark.sql.functions import explode
from pyspark.sql import functions as F

df = spark.read.option("multiline", "true").json("/Volumes/dev/raw/weather/")

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
