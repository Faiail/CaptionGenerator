import pandas as pd
from DataManager import DataManager
import os
from tqdm import tqdm
from transformers import pipeline
from torch.utils.data import DataLoader
from datasets import load_dataset


def get_data(path: str = 'data.csv', manager = None):
    if os.path.exists(path):
        return pd.read_csv(path, index_col=0)
    manager = manager if manager else DataManager()
    data = pd.DataFrame(manager.get_artworks(), columns=['name'])
    data.to_csv('data.csv')
    return data


if __name__ == '__main__':
    manager = DataManager()
    data = get_data('data.csv', manager)

    if 'prompt' not in data.columns:
        data['prompt'] = data.progress_apply(lambda x: manager.get_prompt_by_artwork(x['name']), axis=1)
        data.to_csv('data.csv')

    dataset = load_dataset('csv', data_files='data.csv')['train']
    loader = DataLoader(dataset=dataset, batch_size=16)

    genius = pipeline('text2text-generation', model='beyond/genius-large-k2t', device=0)

    data['caption'] = ''

    for batch in tqdm(loader):
        ix, prompts = batch['Unnamed: 0'].tolist(), batch['prompt']
        out = genius(prompts)
        out = list(map(lambda x: x['generated_text'], out))
        data.loc[ix, 'caption'] = out

    data.to_csv('data.csv')
