#' Sentiment Analysis with Dictionary Analysis & BERT

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'sentiment-analysis-score-data'
EF_DIR = Sys.getenv('EF_DIR')
RESET_SQL = FALSE

## Cron Log ----------------------------------------------------------
if (interactive() == FALSE) {
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
	
	dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_score_reddit CASCADE')
	dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_score_reuters CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE sentiment_analysis_score_reddit (
		scrape_id INT NOT NULL,
		text_part VARCHAR(255) NOT NULL,
		score_model VARCHAR(255) NOT NULL, 
		score INT NOT NULL,
		score_conf DECIMAL(20, 4) NULL,
		scored_dttm TIMESTAMP WITH TIME ZONE NOT NULL,
		PRIMARY KEY (scrape_id, text_part, score_model),
		CONSTRAINT sentiment_analysis_scrape_reddit_fk FOREIGN KEY (scrape_id)
				REFERENCES sentiment_analysis_scrape_reddit (id) ON DELETE CASCADE ON UPDATE CASCADE
		)'
	)
	
	dbExecute(
		db,
		'CREATE TABLE sentiment_analysis_score_reuters (
		scrape_id INT NOT NULL,
		text_part VARCHAR(255) NOT NULL,
		score_model VARCHAR(255) NOT NULL, 
		score INT NOT NULL,
		score_conf DECIMAL(20, 4) NULL,
		scored_dttm TIMESTAMP WITH TIME ZONE NOT NULL,
		PRIMARY KEY (scrape_id, text_part, score_model),
		CONSTRAINT sentiment_analysis_score_reuters_fk FOREIGN KEY (scrape_id)
				REFERENCES sentiment_analysis_scrape_reuters (id) ON DELETE CASCADE ON UPDATE CASCADE
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
				'reddit' AS source, reddit1.id, reddit1.title AS text_part_title, reddit1.selftext as text_part_content
			FROM sentiment_analysis_scrape_reddit reddit1
			LEFT JOIN 
				(
					SELECT scrape_id FROM sentiment_analysis_score_reddit WHERE score_model = 'DISTILBERT'
				) reddit2
				ON reddit1.id = reddit2.scrape_id
			WHERE reddit2.scrape_id IS NULL
		)
		UNION ALL
		(
			SELECT 
				'reuters' AS source, reuters1.id, reuters1.title AS text_part_title, reuters1.description AS text_part_content
			FROM sentiment_analysis_scrape_reuters reuters1
			LEFT JOIN 
				(
					SELECT scrape_id FROM sentiment_analysis_score_reuters WHERE score_model = 'DISTILBERT'
				) reuters2
				ON reuters1.id = reuters2.scrape_id
			WHERE reuters2.scrape_id IS NULL
		)") %>%
		as_tibble(.) %>%
		mutate(., text_part_all_text = paste0(text_part_title, ' ', text_part_content))
	
	
	unscored_dict = dbGetQuery(db,
		"(
			SELECT
				'reddit' AS source, reddit1.id, reddit1.title AS text_part_title, reddit1.selftext as text_part_content
			FROM sentiment_analysis_scrape_reddit reddit1
			LEFT JOIN 
				(
					SELECT scrape_id FROM sentiment_analysis_score_reddit WHERE score_model = 'DICT'
				) reddit2
				ON reddit1.id = reddit2.scrape_id
			WHERE reddit2.scrape_id IS NULL
		)
		UNION ALL
		(
			SELECT
				'reuters' AS source, reuters1.id, reuters1.title AS text_part_title, reuters1.description AS text_part_content
			FROM sentiment_analysis_scrape_reuters reuters1
			LEFT JOIN 
				(
					SELECT scrape_id FROM sentiment_analysis_score_reuters WHERE score_model = 'DICT'
				) reuters2
				ON reuters1.id = reuters2.scrape_id
			WHERE reuters2.scrape_id IS NULL
		)") %>%
		as_tibble(.) %>%
		mutate(., text_part_all_text = paste0(text_part_title, ' ', text_part_content))
	
	unscored_bert <<- unscored_bert
	unscored_dict <<- unscored_dict
})


# Analysis --------------------------------------------------------

## Dictionary Analysis --------------------------------------------------------
local({
	
	BATCH_SIZE = 5000
	
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
		split(., by = c('source', 'split'), .keep = FALSE)
	
	iwalk(batches, function(x, i) {
		
		message('***** Dictionary Scoring Batch ', i, ' of ', length(batches))
		
		result =
			x %>%
			.[, c('id', 'text_part_all_text')] %>% # Name is the unique post identifier
			tidytext::unnest_tokens(., word, text_part_all_text) %>%
			.[!word %chin% stop_words$word]  %>%
			merge(., dict, by = 'word', all = F, allow.cartesian = T) %>%
			.[, list(score = sum(sentiment)/.N), by = 'id'] %>%
			.[, score := ifelse(score >= 0, 1, -1)]
	
		# print(result)
			
		sql_data =
			result %>%
			transmute(
				.,
				scrape_id = id,
				text_part = 'all_text',
				score_model = 'DICT',
				score,
				score_conf = NULL,
				scored_dttm = now()
				) %>%
			mutate(., across(where(is.POSIXt), function(x) format(x, '%Y-%m-%d %H:%M:%S %Z')))

		sql_data %>%
			create_insert_query(
				.,
				paste0('sentiment_analysis_score_', x$source[[1]]),
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
		.[, text_part_all_text := str_sub(text_part_all_text, 1, 2000)] %>%
		.[, split := ceiling((1:nrow(.))/BATCH_SIZE)] %>%
		split(., by = c('source', 'split'), .keep = FALSE)
	
	
	iwalk(batches, function(x, i) {
		
		message('***** BERT Scoring Batch ', i, ' of ', length(batches))
		
		fwrite(set_names(x[, 'text_part_all_text'], 'text'), file.path(tempdir(), 'text.csv'))
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
				text_part = 'all_text',
				score_model = 'DISTILBERT',
				score,
				score_conf,
				scored_dttm = now()
				) %>%
			mutate(., across(where(is.POSIXt), function(x) format(x, '%Y-%m-%d %H:%M:%S %Z')))
		
		sql_data %>%
			create_insert_query(
				.,
				paste0('sentiment_analysis_score_', x$source[[1]]),
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


## Score Analysis --------------------------------------------------------
# scored_data =
# 	pulled_data %>%
# 	merge(., dict_result, all.x = T) %>%
# 	bind_cols(., bert_result[, c('bert_label', 'bert_score')])
# 
# 
# scored_data %>%
# 	.[, list(score = mean(bert_label, na.rm = T)), by = c('created_date', 'subreddit')] %>%
# 	.[order(subreddit, created_date)] %>%
# 	.[, score := frollmean(score, n = 30, fill = NA), by = 'subreddit'] %>%
# 	ggplot(.) +
# 	geom_line(aes(x = created_date, y = score, color = subreddit))
