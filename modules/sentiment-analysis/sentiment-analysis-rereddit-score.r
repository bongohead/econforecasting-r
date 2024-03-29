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


# Data Prep --------------------------------------------------------

## Create Tables --------------------------------------------------------
local({
	if (RESET_SQL) {
		
		dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_score_rereddit CASCADE')

		dbExecute(
			db,
			'CREATE TABLE sentiment_analysis_score_rereddit (
			scrape_id INT NOT NULL,
			text_part VARCHAR(255) NOT NULL,
			score_model VARCHAR(255) NOT NULL, 
			score INT NOT NULL,
			score_conf DECIMAL(20, 4) NULL,
			scored_dttm TIMESTAMP WITH TIME ZONE NOT NULL,
			PRIMARY KEY (scrape_id, text_part, score_model),
			CONSTRAINT sentiment_analysis_scrape_rereddit_fk FOREIGN KEY (scrape_id)
					REFERENCES sentiment_analysis_scrape_rereddit (id) ON DELETE CASCADE ON UPDATE CASCADE
			)'
		)

	}
})

## Pull Unscored Data --------------------------------------------------------
local({
	
	# Pull data NULL
	unscored_bert = dbGetQuery(db,
	 "(
			SELECT
				rereddit1.id, rereddit1.title AS text_part_title_text
			FROM sentiment_analysis_scrape_rereddit rereddit1
			LEFT JOIN 
				(
					SELECT scrape_id FROM sentiment_analysis_score_rereddit WHERE score_model = 'DISTILBERT'
				) rereddit2
				ON rereddit1.id = rereddit2.scrape_id
			WHERE rereddit2.scrape_id IS NULL
		)") %>%
		as_tibble(.)
	
	unscored_dict = dbGetQuery(db,
		"(
			SELECT
				rereddit1.id, rereddit1.title AS text_part_title_text
			FROM sentiment_analysis_scrape_rereddit rereddit1
			LEFT JOIN 
				(
					SELECT scrape_id FROM sentiment_analysis_score_rereddit WHERE score_model = 'DICT'
				) rereddit2
				ON rereddit1.id = rereddit2.scrape_id
			WHERE rereddit2.scrape_id IS NULL
		)") %>%
		as_tibble(.)

	unscored_bert <<- unscored_bert
	unscored_dict <<- unscored_dict
})


# Analysis --------------------------------------------------------

## Dictionary Analysis --------------------------------------------------------
local({
	
	BATCH_SIZE = 10000
	
	dict =
		list(
			get_sentiments('nrc') %>%
				as.data.table(.) %>%
				.[, sentiment := fcase(
					sentiment %chin% c('trust', 'positive', 'joy', 'anticipation', 'surprise'), 1,
					sentiment %chin% c('disgust', 'sadness', 'fear', 'negative', 'anger'), -1
				)],
			get_sentiments('bing') %>%
				as.data.table(.) %>%
				.[, sentiment := ifelse(sentiment == 'positive', 1, -1)]
		) %>%
		rbindlist(.) %>%
		.[, list(sentiment = head(sentiment, 1)), by = 'word']
	
	batches =
		unscored_dict %>%
		as.data.table(.) %>%
		.[, split := ceiling((1:nrow(.))/BATCH_SIZE)] %>%
		split(., by = c('split'), .keep = FALSE)
	
	iwalk(batches, function(x, i) {
		
		message('***** Dictionary Scoring Batch ', i, ' of ', length(batches))
		
		result =
			x %>%
			.[, c('id', 'text_part_title_text')] %>% # Name is the unique post identifier
			tidytext::unnest_tokens(., word, text_part_title_text) %>%
			# .[!word %chin% stop_words$word]  %>%
			merge(., dict, by = 'word', all = F, allow.cartesian = T) %>%
			.[, list(score = sum(sentiment)/.N), by = 'id'] %>%
			.[, score := ifelse(score >= 0, 1, -1)]
		
		# print(result)
		if (nrow(result) == 0) {
			message('***** No dictionary matches, skipping')
			return()
		}
		
		sql_data =
			result %>%
			transmute(
				.,
				scrape_id = id,
				text_part = 'title_text',
				score_model = 'DICT',
				score,
				score_conf = NULL,
				scored_dttm = now()
			) %>%
			mutate(., across(where(is.POSIXt), function(x) format(x, '%Y-%m-%d %H:%M:%S %Z')))
		
		sql_data %>%
			create_insert_query(
				.,
				paste0('sentiment_analysis_score_rereddit'),
				'ON CONFLICT (scrape_id, text_part, score_model) DO UPDATE SET
					score=EXCLUDED.score,
					score_conf=EXCLUDED.score_conf,
					scored_dttm=EXCLUDED.scored_dttm'
			) %>%
			dbExecute(db, .)
	})
	
})

## BERT  --------------------------------------------------------
local({
	
	BATCH_SIZE = 500
	
	happytransformer = import('happytransformer')
	happy_tc = happytransformer$HappyTextClassification(
		'DISTILBERT', # 'ROBERTA',
		'distilbert-base-uncased-finetuned-sst-2-english', # 'siebert/sentiment-roberta-large-english',
		num_labels = 2
	)
	
	batches =
		unscored_bert %>%
		as.data.table(.) %>%
		.[, text_part_title_text := str_sub(text_part_title_text, 1, 512)] %>%
		.[, split := ceiling((1:nrow(.))/BATCH_SIZE)] %>%
		split(., by = c('split'), .keep = FALSE)
	
	
	iwalk(batches, function(x, i) {
		
		message('***** BERT Scoring Batch ', i, ' of ', length(batches))
		
		fwrite(set_names(x[, 'text_part_title_text'], 'text'), file.path(tempdir(), 'text.csv'))
		classified_text = happy_tc$test(file.path(tempdir(), 'text.csv'))
		
		result = data.table(
			scrape_id = x$id,
			score = map_chr(classified_text, ~ .$label),
			score_conf = map_chr(classified_text, ~ .$score)
		)[, score := ifelse(score == 'POSITIVE', 1, -1)]
		
		# print(result)
		
		sql_data =
			result %>%
			transmute(
				.,
				scrape_id,
				text_part = 'text_part_title_text',
				score_model = 'DISTILBERT',
				score,
				score_conf,
				scored_dttm = now()
			) %>%
			mutate(., across(where(is.POSIXt), function(x) format(x, '%Y-%m-%d %H:%M:%S %Z')))
		
		sql_data %>%
			create_insert_query(
				.,
				paste0('sentiment_analysis_score_rereddit'),
				'ON CONFLICT (scrape_id, text_part, score_model) DO UPDATE SET
					score=EXCLUDED.score,
					score_conf=EXCLUDED.score_conf,
					scored_dttm=EXCLUDED.scored_dttm'
			) %>%
			dbExecute(db, .)
	})
	
})

# Finalize --------------------------------------------------------

## Close Connections --------------------------------------------------------
dbDisconnect(db)
message(paste0('\n\n----------- FINISHED ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
