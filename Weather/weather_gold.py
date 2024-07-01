# Databricks notebook source
# MAGIC %sql 
# MAGIC
# MAGIC SHOW COLUMNS IN dev.silver.weather_history

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.silver.weather_history LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dev.gold.dim_weather_country_region 
# MAGIC (
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY
# MAGIC   ,country STRING
# MAGIC   ,city_name STRING
# MAGIC   ,region STRING
# MAGIC   ,macro_region STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO dev.gold.dim_weather_country_region 
# MAGIC (country, city_name, region, macro_region)
# MAGIC (    
# MAGIC     SELECT DISTINCT
# MAGIC     country
# MAGIC     ,(name) as city_name
# MAGIC     ,region
# MAGIC     ,tz_id as macro_region
# MAGIC     FROM dev.silver.weather_history
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dev.gold.dim_weather_moon_details
# MAGIC (
# MAGIC   id_moon              BIGINT GENERATED ALWAYS AS IDENTITY
# MAGIC   ,moon_illumination   INT
# MAGIC   ,moon_phase          STRING
# MAGIC   ,moonrise            STRING
# MAGIC   ,moonset             STRING
# MAGIC   ,sunrise             STRING
# MAGIC   ,sunset              STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO dev.gold.dim_weather_moon_details
# MAGIC (
# MAGIC    moon_illumination
# MAGIC   ,moon_phase       
# MAGIC   ,moonrise         
# MAGIC   ,moonset          
# MAGIC   ,sunrise          
# MAGIC   ,sunset           
# MAGIC )
# MAGIC (    
# MAGIC   SELECT DISTINCT
# MAGIC   moon_illumination
# MAGIC   ,moon_phase       
# MAGIC   ,moonrise         
# MAGIC   ,moonset          
# MAGIC   ,sunrise          
# MAGIC   ,sunset 
# MAGIC   FROM dev.silver.weather_history
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE dev.gold.fact_weather_history
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dev.gold.fact_weather_history
# MAGIC AS 
# MAGIC (
# MAGIC  SELECT DISTINCT wh.*
# MAGIC   EXCEPT
# MAGIC   (
# MAGIC      wh.moon_illumination
# MAGIC     ,wh.moon_phase       
# MAGIC     ,wh.moonrise         
# MAGIC     ,wh.moonset          
# MAGIC     ,wh.sunrise          
# MAGIC     ,wh.sunset
# MAGIC     ,wh.country
# MAGIC     ,wh.name
# MAGIC     ,wh.region
# MAGIC     ,wh.tz_id 
# MAGIC   )
# MAGIC   ,cr.id as id_country_region
# MAGIC   ,md.id_moon
# MAGIC FROM dev.silver.weather_history wh
# MAGIC LEFT JOIN dev.gold.dim_weather_country_region cr ON cr.country = wh.country AND cr.city_name = wh.name
# MAGIC LEFT JOIN dev.gold.dim_weather_moon_details md ON md.moon_phase = wh.moon_phase AND md.moonrise = wh.moonrise AND md.moonset = wh.moonset AND md.sunrise = wh.sunrise AND md.sunset = wh.sunset
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.gold.fact_weather_history
