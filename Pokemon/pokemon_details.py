# Databricks notebook source
import requests
import datetime
import json
from multiprocessing import Pool

# COMMAND ----------

def save_pokemon(data):
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")
    data['date_ingestion'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    filename = f"/dbfs/mnt/datalake/pokemon/pokemon_details/{data['id']}_{now}.json"
    with open(filename, "w") as open_file:
        json.dump(data, open_file)

def get_and_save(url):
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.json()
        save_pokemon(data)
    else:
        print(f"Cannot load following url file: {url}")


urls = (spark.table("bronze.pokemon.pokemon")
             .select("url")
             .distinct()
             .toPandas()["url"]
             .tolist())


with Pool(5) as p:
    p.map(get_and_save, urls)


# COMMAND ----------

df = spark.read.json("/mnt/datalake/pokemon/pokemon_details/")
df.count()
