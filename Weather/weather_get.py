# Databricks notebook source
import configparser
     
config = configparser.ConfigParser()
config.read('config.ini')

api_key = config.get('API', 'weather_api_key')

# COMMAND ----------

import requests 
from datetime import datetime, timedelta
import json
import time

directory = "/Volumes/raw/weather/history"

base = "https://api.weatherapi.com/v1/history.json?"
date = (datetime.today() - timedelta(days=14)).strftime('%Y-%m-%d')
cities = ["New York City", "London", "Tokyo", "Paris", "Hong Kong", "Singapore", "Shanghai", "Beijing", "Los Angeles", "Dubai", "Sydney", "San Francisco", "Chicago", "Berlin", "Moscow", "Toronto", "Seoul", "Mumbai", "São Paulo", "Mexico City", "Buenos Aires", "Bangkok", "Istanbul", "Delhi", "Jakarta", "Cairo", "Kuala Lumpur", "Johannesburg", "Frankfurt", "Madrid", "Amsterdam", "Zurich", "Brussels", "Vienna", "Dublin", "Milan", "Stockholm", "Copenhagen", "Oslo", "Helsinki", "Warsaw", "Lisbon", "Santiago", "Manila", "Lima", "Bogota", "Rio de Janeiro", "Caracas", "Cape Town", "Melbourne", "Barcelona", "Vancouver", "Montreal", "Washington", "Rome","California", "Porto", "Salvador","Campinas", "Malmo","Sao Paulo"]


for city in cities:
    url = f"{base}q={city}&dt={date}"
    params = {
        "key": api_key
    } 

    resp = requests.get(url, params=params)
    print(f"{city}, {resp.status_code}")
    time.sleep(1)

    data = resp.json()
    now = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    #data['ingestion_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    filename = f"{directory}/{city}_{now}.json"
    with open(filename, "w") as open_file:
        json.dump(data, open_file)
