# %% Imports
os.environ['EF_DIR'] = 'D:/OneDrive/__Projects/econforecasting'
sys.path.append(os.getenv('EF_DIR') + '/model-inputs')
from constants import CONST
import logging
import torch
import pandas as pd
import polars as pl
import os
import sys
from sqlalchemy import create_engine
from siuba import group_by, summarize, count, filter, _
from transformers import AutoTokenizer, AutoConfig, AutoModelForSequenceClassification

logging.basicConfig(format = '\n[%(asctime)s]\n %(message)s\n---', level = logging.INFO)

# %% Connections
db = create_engine(f"postgresql://{CONST['DB_USERNAME']}:{CONST['DB_PASSWORD']}@{CONST['DB_SERVER']}:5432/{CONST['DB_DATABASE']}")
pd_df = pd.DataFrame(db.execute(
    "SELECT scrape_id, text_part, score_model, score, score_conf FROM sentiment_analysis_reddit_score LIMIT 10"
    ).fetchall())

# %% Polars Test
df = pl.from_dicts(pd_df.to_dict('records'))


# %% Siuba Test
(df >>
    filter(_.score_model == 'ROBERTA') >>
    group_by(_.score) >>
    summarize(count = _.size)
)

# %% Load Pretrained
pretrained_model = 'siebert/sentiment-roberta-large-english'
tokenizer = AutoTokenizer.from_pretrained(pretrained_model)
model = AutoModelForSequenceClassification.from_pretrained(pretrained_model)
config = AutoConfig.from_pretrained(pretrained_model)

# %% Tokenize & Predict
encoded_tokens = tokenizer(
    ["Test Str1.", "Test string 2"],
    padding = True,
    truncation = True,
    max_length = 512,
    return_tensors = "pt"
)
predictions = model(**encoded_tokens).logits
predictions_normalized = torch.nn.Softmax(dim=-1)(predictions).cpu().detach().numpy()
pd.concat([pd.DataFrame({'labels': config.id2label.values(), 'score': x, 'idx': i}) for i, x in enumerate(predictions_normalized)])


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
    


# # %% Import Dataset

# # Load One at a Time
# class TextDataset(Dataset):
#     def __init__(self, filepath, transform=None):
#         self.data = pd.read_csv(filepath)
#         self.max_len = 512
#         self.transform = transform

#     def __len__(self):
#         return len(self.filepath)

#     def __getitem__(self, idx):
#         if torch.is_tensor(idx):
#             idx = idx.tolist()
        
#         sample = self.data.iloc[idx].to_dict().text
        
#         if self.transform:
#             sample = self.transform(sample)

#         return sample

# text_dataset = TextDataset('test.csv')


# # %% Import Tokenizer
# tokenizer = RobertaTokenizer.from_pretrained('roberta-base')
# logging.info(f'Vocab Size\n {tokenizer.vocab_size}')
# logging.info(f'Max Token Sizes\n {tokenizer.max_model_input_sizes}')

# tokens = tokenizer.tokenize('Hello WORLD how ARE yoU?')
# logging.info(tokens)

# indexes = tokenizer.convert_tokens_to_ids(tokens)
# logging.info(indexes)

# def tokenize_and_cut(sentence):
#     tokens = tokenizer.tokenize(sentence) 
#     tokens = tokens[:512-2] # Leave off 2 tokens, since we need to append one to start & end
#     return tokens


# # %% 
# model = RobertaModel.from_pretrained("roberta-base")

# inputs = tokenizer("Hello, I hate my dog", return_tensors="pt")
# outputs = model(**inputs)

# # %%
# torch.utils.data
# # %%
