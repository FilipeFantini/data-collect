# Databricks notebook source
import requests
import json

# COMMAND ----------

url = "https://api.openf1.org/v1/meetings"
response = requests.get(url)

response.json()
