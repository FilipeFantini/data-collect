# Databricks notebook source
client_id = "your_id"
client_secret = "your_id"
redirect_uri = "http://localhost:8888/callback"
scope = "user-library-read"

# COMMAND ----------

import requests
import json
import base64
import datetime

client_creds = f"{client_id}:{client_secret}"
client_creds_base64 = base64.b64encode(client_creds.encode())
client_creds_base64

token_url = "https://accounts.spotify.com/api/token"
method = "POST"
token_data = {
"grant_type": "client_credentials"
}
token_headers ={
"Authorization": f"Basic {client_creds_base64.decode()}"
}

resp = requests.post(token_url,data=token_data, headers=token_headers)
token_response_data = resp.json()
print(resp.status_code)

access_token = token_response_data['access_token']

# COMMAND ----------

url_episodes = "https://api.spotify.com/v1/artists/0TnOYISbd1XYRBk9myaseg"

headers = {
    'Authorization': f'Bearer {access_token}'
    }

resp = requests.get(url_episodes, headers=headers)
resp.json()
