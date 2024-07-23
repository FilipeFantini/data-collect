# Databricks notebook source
# MAGIC %md
# MAGIC ## Get F1 Data
# MAGIC And save it in raw layer for further data cleaning and transformation using Medalion Architecture.
# MAGIC More details on the source data at: https://openf1.org/#introduction

# COMMAND ----------

import requests
import json
import datetime

base = "https://api.openf1.org/v1/"

# COMMAND ----------

def get_and_save_parquet(url, dimension,  extra_info=""):
    response = requests.get(url)
    data = response.json()
    if response.status_code == 200:
        now = datetime.datetime.now().strftime("%Y-%m")
        filename = f"{dimension}_{now}"
        path = f"/Volumes/raw/formula1/{dimension}/{filename}.parquet"
        df = spark.createDataFrame(data)
        df.display()
        df.coalesce(1).write.mode("overwrite").parquet(path)
    else: print(response.json())

def get_and_save_json(url, dimension, extra_info=""):
    response = requests.get(url)
    data = response.json()
    if response.status_code == 200:
        now = datetime.datetime.now().strftime("%Y-%m")
        filename = f"{dimension}{extra_info}_{now}"
        path = f"/Volumes/raw/formula1/{dimension}/{filename}.json"
        with open(path, "w") as open_file:
            json.dump(data, open_file)
    else: print(response.json())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get Meetings 
# MAGIC Meeting = Grand Prix, or testing event

# COMMAND ----------

#get meetings
get_and_save_json(f"{base}meetings", "meetings")


#get_data(f"{base}sessions", "sessions")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get Sessions 
# MAGIC Based on current Meetings' already available data

# COMMAND ----------

#get sessions
path = "/Volumes/raw/formula1/meetings/"
df = spark.read.json(path)
list_meetings = list(df.select(df.meeting_key).toPandas()["meeting_key"])

for i in list_meetings:
    get_and_save_json(f"{base}sessions?meeting_key={i}", "sessions", f"mkey{i}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get Car Details 
# MAGIC Based on current Meetings and Sessions already available

# COMMAND ----------

#get car details
sessions = "/Volumes/raw/formula1/sessions/"
list_sessions = list(spark.read.json(sessions).toPandas()["session_key"])

#breaking because it's timing out from source, I need to paginate it by driver and session key, too much data
for i in list_sessions:
    get_and_save_json(f"{base}car_data?session_key={i}", "car_data", f"skey{i}")

# COMMAND ----------

path = "/Volumes/raw/formula1/sessions/"
df = spark.read.json(path)
df.display()
