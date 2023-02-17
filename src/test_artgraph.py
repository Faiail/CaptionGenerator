import pandas as pd
from DataManager import DataManager
import requests
import json
import os
from tqdm import tqdm


def get_data(path: str = 'data.csv', manager = None):
    if os.path.exists(path):
        return pd.read_csv(path, index_col=0)
    manager = manager if manager else DataManager()
    data = pd.DataFrame(manager.get_artworks(), columns=['name'])
    data.to_csv('data.csv')
    return data


def query(payload, url, headers):
    response = requests.post(url, headers=headers, json={"inputs": payload})
    return response.json()

if __name__ == '__main__':
    tqdm.pandas()
    key = json.load(open('key.json'))['key']
    API_URL = "https://api-inference.huggingface.co/models/beyond/genius-large-k2t"
    headers = {"Authorization": f"Bearer {key}"}

    manager = DataManager()
    data = get_data('data.csv', manager)

    if 'prompt' not in data.columns:
        data['prompt'] = data.progress_apply(lambda x: manager.get_prompt_by_artwork(x['name']), axis=1)

    if 'caption' not in data.columns:
        data['caption'] = data.progress_apply(lambda x: query(x['prompt'], API_URL, headers), axis=1)

    data.to_csv('data.csv')
