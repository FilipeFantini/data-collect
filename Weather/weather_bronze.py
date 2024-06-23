# Databricks notebook source
from pyspark.sql.functions import explode
from pyspark.sql import functions as F

df = spark.read.json("/FileStore/datalake/weather")

df.printSchema()
