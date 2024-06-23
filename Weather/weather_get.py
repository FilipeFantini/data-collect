# Databricks notebook source
import configparser
     
config = configparser.ConfigParser()
config.read('config.ini')

api_key = config.get('API', 'weather_api_key')

# COMMAND ----------

import requests 
from datetime import datetime, timedelta
import json
import os

directory = "/dbfs/FileStore/datalake/weather/"
os.makedirs(directory, exist_ok=True)
base = "https://api.weatherapi.com/v1/history.json?"
date = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')
cities = ["Lisbon", "London", "Paris", "Sao Paulo", "Rio de Janeiro", "Tokyo", "California"]

for city in cities:
    url = f"{base}q={city}&dt={date}"
    params = {
        "key": api_key
    } 

    resp = requests.get(url, params=params)
    print(f"{city}, {resp.status_code}")

    data = resp.json()
    now = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    #data['ingestion_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    filename = f"{directory}{city}_{now}.json"
    with open(filename, "w") as open_file:
        json.dump(data, open_file)
