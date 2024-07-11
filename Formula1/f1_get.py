# Databricks notebook source
import requests
import json
import datetime

base = "https://api.openf1.org/v1/"

# COMMAND ----------

def get_and_save_parquet(url, dimension):
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

def get_and_save_json(url, dimension):
    response = requests.get(url)
    data = response.json()
    if response.status_code == 200:
        now = datetime.datetime.now().strftime("%Y-%m")
        filename = f"{dimension}_{now}"
        path = f"/Volumes/raw/formula1/{dimension}/{filename}.json"
        with open(path, "w") as open_file:
            json.dump(data, open_file)
    else: print(response.json())

# COMMAND ----------

#get meetings
get_and_save_json(f"{base}meetings", "meetings")


#get_data(f"{base}sessions", "sessions")

# COMMAND ----------

#get sessions
path = "/Volumes/raw/formula1/meetings/"
df = spark.read.json(path)
df.select(df.meeting_key).show()
