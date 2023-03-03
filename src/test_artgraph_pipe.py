import pandas as pd
from DataManager import DataManager
import os
from tqdm import tqdm
from transformers import pipeline
from datasets import load_dataset
from transformers.pipelines.pt_utils import KeyDataset


def get_data(path: str = 'data.csv', manager: DataManager = None):
    if os.path.exists(path):
        return pd.read_csv(path, index_col=0)
    manager = manager if manager else DataManager()
    data = pd.DataFrame(manager.get_artworks(), columns=['name'])
    data.to_csv('data.csv')
    return data


if __name__ == '__main__':
    data = pd.read_csv('data.csv', index_col=0)
    dataset = load_dataset('csv', data_files='data.csv')['train']  # load the dataset

    # compose the pipeline
    genius = pipeline('text2text-generation', model='beyond/genius-large-k2t', device=0)

    for i, out in enumerate(tqdm(genius(KeyDataset(dataset, 'prompt'), batch_size=8))):
        data.loc[i, 'caption'] = out[0]['generated_text']  # generate the captions

    # save
    data.to_csv('data_captions.csv')
