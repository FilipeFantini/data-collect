#%%
import requests
import pandas as pd
import datetime
import json
import os

#%%
def get_response(**kwargs):
    url = 'https://www.tabnews.com.br/api/v1/contents'
    resp = requests.get(url, params=kwargs)
    return resp

def save_data(data, option='json'):

    now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")

    if option == 'json':
        with open(f"data/contents/json/{now}.json", 'w') as open_file:
            json.dump(data, open_file, indent=4)

    elif option == 'parquet':
        df = pd.DataFrame(data)
        df.to_parquet(f"data/contents/parquet/{now}.parquet", index=False)

#%%

resp = get_response(page=1, per_page=100, strategy="new")
if resp.status_code == 200:
    print("sucesso!")

#%%
data = resp.json()
data

#%%
save_data(data)