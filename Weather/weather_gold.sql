-- Databricks notebook source
DROP TABLE IF EXISTS gold.weather.fact_weather_history; 
DROP TABLE IF EXISTS gold.weather.dim_weather_moon_details; 
DROP TABLE IF EXISTS gold.weather.dim_weather_country_region; 


-- COMMAND ----------


SHOW COLUMNS IN silver.weather.weather_history

-- COMMAND ----------


SELECT * FROM silver.weather.weather_history LIMIT 10

-- COMMAND ----------


CREATE TABLE gold.weather.dim_weather_country_region 
(
  id BIGINT GENERATED ALWAYS AS IDENTITY
  ,country STRING
  ,city_name STRING
  ,region STRING
  ,macro_region STRING
)

-- COMMAND ----------


INSERT INTO gold.weather.dim_weather_country_region
(country, city_name, region, macro_region)
(    
    SELECT DISTINCT
    country
    ,(name) as city_name
    ,region
    ,tz_id as macro_region
    FROM silver.weather.weather_history
)

-- COMMAND ----------


CREATE TABLE gold.weather.dim_weather_moon_details
(
  id_moon              BIGINT GENERATED ALWAYS AS IDENTITY
  ,moon_illumination   INT
  ,moon_phase          STRING
  ,moonrise            STRING
  ,moonset             STRING
  ,sunrise             STRING
  ,sunset              STRING
)

-- COMMAND ----------



INSERT INTO gold.weather.dim_weather_moon_details
(
   moon_illumination
  ,moon_phase       
  ,moonrise         
  ,moonset          
  ,sunrise          
  ,sunset           
)
(    
  SELECT DISTINCT
  moon_illumination
  ,moon_phase       
  ,moonrise         
  ,moonset          
  ,sunrise          
  ,sunset 
  FROM silver.weather.weather_history
)

-- COMMAND ----------


DROP TABLE IF EXISTS gold.weather.fact_weather_history


-- COMMAND ----------


CREATE TABLE gold.weather.fact_weather_history
AS 
(
 SELECT DISTINCT wh.*
  EXCEPT
  (
     wh.moon_illumination
    ,wh.moon_phase       
    ,wh.moonrise         
    ,wh.moonset          
    ,wh.sunrise          
    ,wh.sunset
    ,wh.country
    ,wh.name
    ,wh.region
    ,wh.tz_id 
  )
  ,cr.id as id_country_region
  ,md.id_moon
FROM silver.weather.weather_history wh
LEFT JOIN gold.weather.dim_weather_country_region cr ON cr.country = wh.country AND cr.city_name = wh.name
LEFT JOIN gold.weather.dim_weather_moon_details md ON md.moon_phase = wh.moon_phase AND md.moonrise = wh.moonrise AND md.moonset = wh.moonset AND md.sunrise = wh.sunrise AND md.sunset = wh.sunset
)

-- COMMAND ----------


select * from gold.weather.fact_weather_history limit 10
