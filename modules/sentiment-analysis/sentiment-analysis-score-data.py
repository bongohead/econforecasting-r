# Initialize ----------------------------------------------------------
# %% Imports
import os
import logging
import yaml
import torch
import pandas as pd
import polars as pl
import numpy as np
from datetime import datetime
import pytz
from sqlalchemy import create_engine
from transformers import AutoTokenizer, AutoConfig, AutoModelForSequenceClassification

# %% Constants  
JOB_NAME = 'sentiment-analysis-pull-data'
EF_DIR = os.environ['EF_DIR']
RESET_SQL = False
MAX_ROWS = 50000

# %% Logger
logging.basicConfig(
    filename = os.path.join(EF_DIR, 'logs', JOB_NAME + '.log'),
    format = '\n[%(asctime)s]\n %(message)s\n---',
    level = logging.INFO
    )
logging.getLogger().addHandler(logging.StreamHandler())


# %% Connections
with open(EF_DIR + "/model-inputs/constants.yml","r") as f: CONST = (yaml.full_load(f.read()))
db = create_engine(
    f"postgresql://{CONST['DB_USERNAME']}:{CONST['DB_PASSWORD']}@{CONST['DB_SERVER']}:5432/{CONST['DB_DATABASE']}"
    )


# Data Import ----------------------------------------------------------

# %% Create Tables
# if RESET_SQL:



# %% Boards
def get_boards():
    score_boards = pl.from_pandas(pd.read_sql(
        """
        SELECT subreddit, scrape_ups_floor FROM sentiment_analysis_reddit_boards
        WHERE score_active = TRUE
        """,
        db
    ))
    return (score_boards)

reddit = {}
(reddit['scrape_boards']) = get_boards()


# %%

# %% Pull Unscored Data 
def get_unscored_text():

    score_models = ['DICT', 'DISTILBERT', 'ROBERTA']

    def get_raw_df(x):
        raw_df = pl.from_pandas(pd.read_sql(
            f"""
			(
				SELECT
					'reddit' AS source, DATE(r1.created_dttm AT TIME ZONE 'US/Eastern') as created_dt,
					r1.id,
					CONCAT(r1.title, COALESCE(CONCAT('. ', r1.selftext), '')) AS text_part_all_text
				FROM sentiment_analysis_reddit_scrape r1
				LEFT JOIN
					(
					SELECT scrape_id--, DATE(scraped_dttm) AS scrape_date
					FROM sentiment_analysis_reddit_score
					WHERE score_model = '{x}'
					) r2
					ON r1.id = r2.scrape_id
				-- Only keep subreddits in scrape_boards directory and above score_ups_floor
				INNER JOIN sentiment_analysis_reddit_boards b
					ON r1.subreddit = b.subreddit AND b.scrape_active = TRUE AND r1.ups >= b.score_ups_floor 
				WHERE r2.scrape_id IS NULL 
					-- Rescore comments scored before 5/10/22
                    -- Uncomment  below for testing
                    -- OR DATE(r1.scraped_dttm) <= '2022-05-01'
			)
			UNION ALL
			(
				SELECT 
					'media' AS source, m1.created_dt,
					m1.id, 
					CONCAT(m1.title, COALESCE(CONCAT('. ', m1.description), '')) AS text_part_all_text
				FROM sentiment_analysis_media_scrape m1
				LEFT JOIN 
					(
					SELECT scrape_id--, DATE(scraped_dttm) AS scrape_date
					FROM sentiment_analysis_media_score
					WHERE score_model = '{x}'
					) m2
					ON m1.id = m2.scrape_id
				WHERE m2.scrape_id IS NULL
					-- Rescore comments scored before 5/10/22
					-- OR DATE(r2.scrape_date) <= '2022-05-01'
			)
            """,
            db
        ))

        return raw_df

    return {k: v for k, v in zip(score_models, [get_raw_df(x) for x in score_models])}

unscored_text = get_unscored_text()




# Analysis ----------------------------------------------------------

# %% Dictionary Analysis
def run_dict_analysis():
    BATCH_SIZE = 10000

# %% DISTILBERT
def run_distilbert():
    BATCH_SIZE = 10 #1000
    print(f'UNSCORED DISTILBERT: {len(unscored_text["DISTILBERT"])}')

    # Load pretrained
    pretrained_model = 'distilbert-base-uncased-finetuned-sst-2-english'
    tokenizer = AutoTokenizer.from_pretrained(pretrained_model)
    model = AutoModelForSequenceClassification.from_pretrained(pretrained_model)
    config = AutoConfig.from_pretrained(pretrained_model)

    batches =\
        unscored_text["DISTILBERT"]\
        .head(MAX_ROWS)\
        .sort(['source', 'created_dt'], reverse = [True, True])\
        .pipe(lambda x: x.with_column(pl.Series(list(range(1, len(x) + 1))).alias('split')))\
        .with_column((pl.col('split')/BATCH_SIZE).ceil())\
        .partition_by(['source', 'split'])

    def get_scores_and_send_to_sql(batch):
        encoded_tokens = tokenizer(
            batch['text_part_all_text'].to_list(),
            padding = True,
            truncation = True,
            max_length = 512,
            return_tensors = "pt"
        )
        predictions = model(**encoded_tokens).logits
        predictions_normalized = torch.nn.Softmax(dim=-1)(predictions).cpu().detach().numpy()
        sql_data =\
            pl.concat([
                pl.from_dict({'score': list(config.id2label.values()), 'score_conf': x.tolist(), 'idx': [i] * 2})
                for i, x in enumerate(predictions_normalized)
            ])\
            .with_columns([
                pl.when(pl.col('score') == 'POSITIVE').then('p').otherwise('n').alias('score'),
                pl.Series(list(np.repeat(batch['id'].to_list(), 2))).alias('scrape_id'),
                pl.lit('all_text').alias('text_part'),
                pl.lit('DISTILBERT').alias('score_model'),
                pl.lit(datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S %z')).alias('scored_dttm'),
                ])\
            [['scrape_id', 'text_part', 'score_model', 'score', 'score_conf', 'scored_dttm']]

        ## Create insert_sql query            

    get_scores_and_send_to_sql(batches[1])
    [get_scores_and_send_to_sql(batch) for batch in batches]

run_distilbert()


# %% Pull Unscored Data
pd_df = pd.DataFrame(db.execute(
    "SELECT scrape_id, text_part, score_model, score, score_conf FROM sentiment_analysis_reddit_score LIMIT 10"
    ).fetchall())


# %% Polars Test
df = pl.from_dicts(pd_df.to_dict('records'))
(df
    .filter(pl.col('score_model') == 'ROBERTA')
    .groupby('score')
    .agg([
        pl.count().alias('count')
    ])
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
pl.concat(
    [
        pl.from_dict({'labels': list(config.id2label.values()), 'score': x.tolist(), 'idx': [i] * 2})
        for i, x in enumerate(predictions_normalized)
    ]
)

# pd.concat([pd.DataFrame({'labels': config.id2label.values(), 'score': x, 'idx': i}) for i, x in enumerate(predictions_normalized)])

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
