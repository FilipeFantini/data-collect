# Databricks notebook source
# MAGIC %md
# MAGIC ## Get F1 Data
# MAGIC And save it in raw layer for further data cleaning and transformation using Medalion Architecture.
# MAGIC More details on the source data at: https://openf1.org/#introduction

# COMMAND ----------

import requests
import json
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import current_timestamp, to_timestamp


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

def get_and_save_json(url, dimension, *args):
    try:
        response = requests.get(url)
    except:
        print(f"Did not work for url: {url}")
    extra_path = args[0]
    if response.status_code == 200:
        data = response.json()
        now = datetime.now().strftime("%Y-%m")
        filename = f"{dimension}{extra_path}_{now}"
        path = f"/Volumes/raw/formula1/{dimension}/{filename}.json"
        with open(path, "w") as open_file:
            json.dump(data, open_file)
    else: print(response.json())

    checkpoint = {
        "project": "F1",
        "dimension": f"{dimension}",
        "url": f"{url}",
        "path": f"{path}",
        "created": f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "total_rows": f"{len(data)}"
        #datetime.strptime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
    }

    path_log = f"/Volumes/raw/metadata/load_logs/logs_{filename}.json"
    with open(path_log, "w") as open_file_logs:
        json.dump(checkpoint, open_file_logs)


# COMMAND ----------

df2 = spark.read.json("/Volumes/raw/metadata/load_logs/").display()

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
# MAGIC ## Get Drivers
# MAGIC Based on current Meetings already available

# COMMAND ----------

#get drivers
path = "/Volumes/raw/formula1/meetings/"
df = spark.read.json(path)
list_meetings = list(df.select(df.meeting_key).toPandas()["meeting_key"])

for i in list_meetings:
    get_and_save_json(f"{base}drivers?meeting_key={i}", "drivers", f"_mkey{i}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get Car Details 
# MAGIC Based on current Meetings and Sessions already available

# COMMAND ----------

#get car details
sessions = "/Volumes/raw/formula1/sessions/"
list_sessions = list(spark.read.json(sessions).toPandas()["session_key"])
drivers = "/Volumes/raw/formula1/drivers/"
list_drivers = list(spark.read.json(drivers).toPandas()["driver_number"].drop_duplicates())

#breaking because it's timing out from source, I need to paginate it by driver and session key, too much data

for i in list_drivers:
    for j in list_sessions:
        for n_gear in range(0,9):
            try:
                get_and_save_json(f"{base}car_data?driver_number={i}&session_key={j}&n_gear={n_gear}", "car_data", f"_skey{j}_dn{i}_ng{n_gear}")
            except: continue

# COMMAND ----------

path = "/Volumes/raw/formula1/sessions/"
df = spark.read.json(path)
df.display()
