import pandas as pd
from DataManager import DataManager
import requests
import json
import os
from tqdm import tqdm


"""
TRY TO OPTIMIZE WITH PARALLELISM
"""


def get_data(path: str = 'data.csv', manager = None):
    if os.path.exists(path):
        return pd.read_csv(path, index_col=0)
    manager = manager if manager else DataManager()
    data = pd.DataFrame(manager.get_artworks(), columns=['name'])
    data.to_csv('data.csv')
    return data


if __name__ == '__main__':
    tqdm.pandas()
    key = json.load(open('key.json'))['key']
    API_URL = "https://api-inference.huggingface.co/models/beyond/genius-large-k2t"
    headers = {"Authorization": f"Bearer {key}"}

    def query(payload):
        response = requests.post(API_URL, headers=headers, json={"inputs": payload})
        return response.json()

    manager = DataManager()
    data = get_data('data.csv', manager)
    data['caption'] = data.progress_apply(lambda x: query(manager.get_prompt_by_artwork(x['name'])), axis = 1)
    data.to_csv('data.csv')