# %% Imports
import logging
import torch
import os
import pandas as pd
from torch.utils.data import Dataset
from transformers import RobertaTokenizer, RobertaModel, AutoTokenizer, AutoConfig, AutoModelForSequenceClassification

logging.basicConfig(format = '\n[%(asctime)s]\n %(message)s\n---', level = logging.INFO)

# %% Pretrained
tokenizer = AutoTokenizer.from_pretrained('siebert/sentiment-roberta-large-english')
model = AutoModelForSequenceClassification.from_pretrained('siebert/sentiment-roberta-large-english')
config = AutoConfig.from_pretrained('siebert/sentiment-roberta-large-english')

#encoded_input = tokenizer(['Dog', 'Butt'], return_tensors='pt')
pt_batch = tokenizer(
    ["We are very happy to show you the 🤗 Transformers library.", "We hope you don't hate it."],
    padding=True,
    truncation=True,
    max_length=512,
    return_tensors="pt"
)
# %% 
import numpy as np
print(pt_batch)
predictions = model(**pt_batch)
scores = torch.nn.Softmax(dim = 0)(predictions.logits[0])

pd.concat([*map(
    lambda x: pd.DataFrame({'labels': config.id2label.values(), 'score': x}),
    model(**pt_batch).logits.cpu().detach().numpy()
)])
# pd.DataFrame({'labels': config.id2label.values(), 'x': scores.cpu().detach().numpy()})

# %% Test
from transformers import AutoTokenizer
tokenizer = AutoTokenizer.from_pretrained('siebert/sentiment-roberta-large-english')
tokenized_datasets = dataset.map(tokenize_function, batched=True)

def tokenize_function(examples):
    return tokenizer(examples["text"], padding="max_length", truncation=True)

from transformers import AutoModelForSequenceClassification
model = AutoModelForSequenceClassification.from_pretrained(
    'siebert/sentiment-roberta-large-english',
    num_labels=3
    )

model

# %% Import Data
tokenizer = RobertaTokenizer.from_pretrained('roberta-base')

raw_data = pd.read_csv('test.csv').to_dict('records')
tokenized_data = []
for x in raw_data:
    # Leave off 2 tokens, since we need to append one to start & end
    cut_text = x['text'][:512-2]
    tokens = tokenizer.tokenize(cut_text)
    indexed_tokens = tokenizer.encode(cut_text)
    tokenized_data.append(tokens)
    
# %% Run Model
model = RobertaModel

model.register_classification_head('new_task', num_classes=3)
logprobs = roberta.predict('new_task', tokens)


# %% Import Dataset

# Load One at a Time
class TextDataset(Dataset):
    def __init__(self, filepath, transform=None):
        self.data = pd.read_csv(filepath)
        self.max_len = 512
        self.transform = transform

    def __len__(self):
        return len(self.filepath)

    def __getitem__(self, idx):
        if torch.is_tensor(idx):
            idx = idx.tolist()
        
        sample = self.data.iloc[idx].to_dict().text
        
        if self.transform:
            sample = self.transform(sample)

        return sample

text_dataset = TextDataset('test.csv')


# %% Import Tokenizer
tokenizer = RobertaTokenizer.from_pretrained('roberta-base')
logging.info(f'Vocab Size\n {tokenizer.vocab_size}')
logging.info(f'Max Token Sizes\n {tokenizer.max_model_input_sizes}')

tokens = tokenizer.tokenize('Hello WORLD how ARE yoU?')
logging.info(tokens)

indexes = tokenizer.convert_tokens_to_ids(tokens)
logging.info(indexes)

def tokenize_and_cut(sentence):
    tokens = tokenizer.tokenize(sentence) 
    tokens = tokens[:512-2] # Leave off 2 tokens, since we need to append one to start & end
    return tokens


# %% 
model = RobertaModel.from_pretrained("roberta-base")

inputs = tokenizer("Hello, I hate my dog", return_tensors="pt")
outputs = model(**inputs)

# %%
torch.utils.data
# %%
