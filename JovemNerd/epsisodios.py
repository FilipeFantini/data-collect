#%%
import requests
import datetime
import json
import pandas as pd

#%%

class Collector:
    
    def __init__(self, url, instance_name):
        self.url = url
        self.instance_name = instance_name

    def get_content(self, **kwargs):
        resp =  requests.get(url, params=kwargs)
        return resp

    def save_parquet(self, data):
        df = pd.DataFrame(data)
        df.to_parquet(f"data/{self.instance_name}/parquet/{now}.parquet", index=False)

    def save_json(self, data):
        now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")
        with open(f"data/{self.instance_name}/json/{now}.json", 'w') as open_file:
            json.dump(data, open_file)

    def save_data(self, data, format='json'):
        if format == 'json':
            self.save_json(data)

        elif format == 'parquet':
            self.save_parquet(data)

    def get_and_save(self, save_format='json', **kwargs):
        resp = self.get_content(**kwargs)
        if resp.status_code == 200:
            self.save_data(resp.json(), save_format)
        else:
            print(f"Failed request: {resp.status_code}", resp.json())


        

#%%

url =  "https://api.jovemnerd.com.br/wp-json/jovemnerd/v1/nerdcasts/"
collect = Collector(url, 'episodios')

#%%

collect.get_and_save()