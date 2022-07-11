#' Sentiment Analysis with Dictionary Analysis & BERT

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'sentiment-analysis-rereddit-score'
EF_DIR = Sys.getenv('EF_DIR')
RESET_SQL = FALSE

## Cron Log ----------------------------------------------------------
if (interactive() == FALSE && rstudioapi::isAvailable(child_ok = T) == F) {
	sink_path = file.path(EF_DIR, 'logs', paste0(JOB_NAME, '.log'))
	sink_conn = file(sink_path, open = 'at')
	system(paste0('echo "$(tail -50 ', sink_path, ')" > ', sink_path,''))
	lapply(c('output', 'message'), function(x) sink(sink_conn, append = T, type = x))
	message(paste0('\n\n----------- START ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
}

## Load Libs ----------------------------------------------------------'
library(econforecasting)
library(tidyverse)
library(data.table)
library(DBI)
library(RPostgres)
library(lubridate)
library(jsonlite)
library(tidytext)
library(reticulate)
use_virtualenv(file.path(EF_DIR, '.virtualenvs', 'econforecasting'))

## Load Connection Info ----------------------------------------------------------
source(file.path(EF_DIR, 'model-inputs', 'constants.r'))
db = dbConnect(
	RPostgres::Postgres(),
	dbname = CONST$DB_DATABASE,
	host = CONST$DB_SERVER,
	port = 5432,
	user = CONST$DB_USERNAME,
	password = CONST$DB_PASSWORD
)


# Batch ----------------------------------------------------------

torch = import('torch')
transformers = import('transformers')
pd = import('pandas')

pretrained_model = 'siebert/sentiment-roberta-large-english'
tokenizer = transformers$AutoTokenizer$from_pretrained(pretrained_model)
model = transformers$AutoModelForSequenceClassification$from_pretrained(pretrained_model)
config = transformers$AutoConfig$from_pretrained(pretrained_model)

encoded_tokens = tokenizer(
	list("We are very happy to show you the 🤗 Transformers library.", "We hope you don't hate it."),
	padding = T,
	truncation = T,
	max_length = 512L,
	return_tensors = 'pt'
)


# Pass spread all key value pairs
predictions = model(input_ids = encoded_tokens$get('input_ids'))$logits
predictions_normalized = torch$nn$Softmax(dim=-1L)(predictions)$cpu()$detach()$tolist()
results = lapply(predictions_normalized, function(x, i)
	data.table(
		index = i,
		label = unlist(config$id2label),
		score = x
		)
	)




batches =
	unscored_bert %>%
	as.data.table(.) %>%
	.[, text_part_title_text := str_sub(text_part_title_text, 1, 512)] %>%
	.[, split := ceiling((1:nrow(.))/BATCH_SIZE)] %>%
	split(., by = c('split'), .keep = FALSE)

