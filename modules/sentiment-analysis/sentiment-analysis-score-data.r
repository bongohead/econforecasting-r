#' Sentiment Analysis with Dictionary Analysis & BERT

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'sentiment-analysis-score-data'
EF_DIR = Sys.getenv('EF_DIR')
RESET_SQL = FALSE
MAX_ROWS = 50000 # Max numbers of rows to process with jobs

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
library(lubridate)
library(tidytext)
library(reticulate)
use_virtualenv(file.path(EF_DIR, '.virtualenvs', 'econforecasting'))

## Load Connection Info ----------------------------------------------------------
db = connect_db(secrets_path = file.path(EF_DIR, 'model-inputs', 'constants.yaml'))
run_id = log_start_in_db(db, JOB_NAME, 'sentiment-analysis')


# Data Import --------------------------------------------------------

## Create Tables --------------------------------------------------------
local({
if (RESET_SQL) {

	dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_reddit_score CASCADE')
	dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_media_score CASCADE')

	dbExecute(
		db,
		'CREATE TABLE sentiment_analysis_reddit_score (
		scrape_id INT NOT NULL,
		text_part VARCHAR(255) NOT NULL,
		score_model VARCHAR(255) NOT NULL,
		score VARCHAR(100) NOT NULL,
		score_conf DECIMAL(20, 4) NULL,
		scored_dttm TIMESTAMP WITH TIME ZONE NOT NULL,
		PRIMARY KEY (scrape_id, text_part, score_model),
		CONSTRAINT sentiment_analysis_reddit_score_fk FOREIGN KEY (scrape_id)
				REFERENCES sentiment_analysis_reddit_scrape (id) ON DELETE CASCADE ON UPDATE CASCADE
		)'
	)

	dbExecute(
		db,
		'CREATE TABLE sentiment_analysis_media_score (
		scrape_id INT NOT NULL,
		text_part VARCHAR(255) NOT NULL,
		score_model VARCHAR(255) NOT NULL,
		score VARCHAR(100) NOT NULL,
		score_conf DECIMAL(20, 4) NULL,
		scored_dttm TIMESTAMP WITH TIME ZONE NOT NULL,
		PRIMARY KEY (scrape_id, text_part, score_model),
		CONSTRAINT sentiment_analysis_media_score_fk FOREIGN KEY (scrape_id)
				REFERENCES sentiment_analysis_media_scrape (id) ON DELETE CASCADE ON UPDATE CASCADE
		)'
	)

}
})

## Boards --------------------------------------------------------
local({

	score_boards = collect(tbl(db, sql(
		"SELECT subreddit, scrape_ups_floor FROM sentiment_analysis_reddit_boards
		WHERE score_active = TRUE"
	)))

	score_boards <<- score_boards
})

## Pull Unscored Data --------------------------------------------------------
local({

	score_models = c('DICT', 'DISTILBERT', 'ROBERTA')

	unscored_text = lapply(score_models %>% setNames(., .), function(x)
		dbGetQuery(db, str_glue(
			"(
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
					--OR DATE(r2.scrape_date) <= '2022-05-01'
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
			)"
			)) %>%
			as_tibble(.) %>%
			mutate(
				.,
				text_part_all_text =
					text_part_all_text %>%
					str_replace_all(., '[^\x01-\x7F]', '') %>%
					str_squish(.) %>%
					str_sub(., 1, 512)
				)
	)

	unscored_text %>%
		bind_rows(.) %>%
		sample_n(., size = 20) %>%
		.$text_part_all_text %>%
		print(.)

	unscored_text <<- unscored_text
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
		unscored_text$DICT %>%
		as.data.table(.) %>%
		.[, split := ceiling((1:nrow(.))/BATCH_SIZE)] %>%
		split(., by = c('source', 'split'), keep.by = T, flatten = T) %>%
		unname(.)

	iwalk(batches, function(x, i) {

		message('***** Dictionary Scoring Batch ', i, ' of ', length(batches))

		result =
			x %>%
			.[, c('id', 'text_part_all_text')] %>% # Name is the unique post identifier
			tidytext::unnest_tokens(., word, text_part_all_text) %>%
			# .[!word %chin% stop_words$word]  %>%
			merge(., dict, by = 'word', all = F, allow.cartesian = T) %>%
			.[, list(score = sum(sentiment)/.N), by = 'id'] %>%
			.[, score := ifelse(score >= 0, 'p', 'n')]

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
				paste0('sentiment_analysis_', x$source[[1]], '_score'),
				'ON CONFLICT (scrape_id, text_part, score_model) DO UPDATE SET
					score=EXCLUDED.score,
					score_conf=EXCLUDED.score_conf,
					scored_dttm=EXCLUDED.scored_dttm'
				) %>%
			dbExecute(db, .)
	})

})

## DISTILBERT  --------------------------------------------------------
local({

	BATCH_SIZE = 1000
	message('UNSCORED DISTILBERT: ', nrow(unscored_text$DISTILBERT))

	happytransformer = import('happytransformer')
	happy_tc = happytransformer$HappyTextClassification(
		'DISTILBERT', # 'ROBERTA',
		'distilbert-base-uncased-finetuned-sst-2-english', # 'siebert/sentiment-roberta-large-english',
		num_labels = 2
	)

	batches =
		unscored_text$DISTILBERT %>%
			# Score first 10k only to prevent overlap
			head(., MAX_ROWS) %>%
			as.data.table(.) %>%
			.[order(-source, -created_dt)] %>%
			.[, split := ceiling((1:nrow(.))/BATCH_SIZE)] %>%
			split(., by = c('source', 'split'), keep.by = T, flatten = T) %>%
			unname(.)

	iwalk(batches, function(x, i) {

		pull_counts =
			x %>%
			.[, list(count = .N), by = 'created_dt'] %>%
			.[, x:= paste0(format(created_dt, "%m/%d/%y"), " (", count, ")")] %>%
			.$x %>%
			paste0(., collapse = "\n")

		message(str_glue('***** DISTILBERT Scoring {i} of {length(batches)} | Source: {x$source[[1]]} | Counts: \n{pull_counts}'))

		fwrite(set_names(x[, 'text_part_all_text'], 'text'), file.path(tempdir(), 'text.csv'))
		classified_text = happy_tc$test(file.path(tempdir(), 'text.csv'))

		result = data.table(
			scrape_id = x$id,
			score = map_chr(classified_text, ~ .$label),
			score_conf = map_chr(classified_text, ~ .$score)
			)[, score := ifelse(score == 'POSITIVE', 'p', 'n')]

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
				paste0('sentiment_analysis_', x$source[[1]], '_score'),
				'ON CONFLICT (scrape_id, text_part, score_model) DO UPDATE SET
					score=EXCLUDED.score,
					score_conf=EXCLUDED.score_conf,
					scored_dttm=EXCLUDED.scored_dttm'
			) %>%
			dbExecute(db, .)
	})

	# Parallelization
	# USE_CORES = 2
	#
	# batches =
	# 	unscored_bert %>%
	# 	# Score first X rows only to prevent overlap
	# 	head(., MAX_ROWS) %>%
	# 	as.data.table(.) %>%
	# 	.[, text_part_all_text := str_sub(text_part_all_text, 1, 512)] %>%
	# 	.[order(-source, -created_dt)] %>%
	# 	.[, split_core := ceiling((1:nrow(.))/(MAX_ROWS/USE_CORES))] %>%
	# 	split(., by = 'split_core', keep.by = F) %>%
	# 	lapply(., function(x) {
	# 		x %>%
	# 			.[, split := ceiling((1:nrow(.))/BATCH_SIZE)] %>%
	# 			split(., by = c('source', 'split'), keep.by = T, flatten = T) %>%
	# 			unname(.)
	# 	})
	#
	# ## Setup empty progress-bar file
	# progress_file = tempfile()
	# cat('', file = progress_file, append = F)
	#
	# library(future)
	# plan('multisession', workers = USE_CORES)
	#
	# ## Detect non-exportable objects and give an error asap
	# options(future.globals.onReference = "error")
	# promises = lapply(1:USE_CORES, function(i)
	# 	future({
	# 		pid = Sys.getpid()
	# 		# cat(str_glue('\nCore {i}/{pid} initialized'), file = progress_file, append = T)
	# 		message(pid)
	# 		library(reticulate)
	# 		# cat(str_glue('\nImporting Python Modules: {i}/{pid}'), file = progress_file, append = T)
	# 		import_path = file.path(EF_DIR, '.virtualenvs', 'econforecasting')
	# 		use_virtualenv(import_path)
	# 		happytransformer = reticulate::import_from_path(
	# 			'happytransformer',
	# 			path = file.path(EF_DIR, '.virtualenvs', 'econforecasting', 'lib64/python3.8/site-packages/happytransformer')
	# 			)
	# 		# happytransformer = reticulate::import('happytransformer')
	# 		# happy_tc = happytransformer$HappyTextClassification(
	# 		# 	'DISTILBERT', # 'ROBERTA',
	# 		# 	'distilbert-base-uncased-finetuned-sst-2-english', # 'siebert/sentiment-roberta-large-english',
	# 		# 	num_labels = 2
	# 		# )
	#
	# 		Sys.sleep(10)
	# 	}, globals = TRUE)
	# )
	#
	# ## Poll progress every 0.1 s until done
	# while (all(resolved(promises)) == FALSE) {
	# 	## Report on progress
	# 	print('test')
	# 	# cat(read_file(progress_file))
	# 	readLines(progress_file)
	# 	Sys.sleep(1)
	# }
	# value(promises[[1]])
})


## ROBERTA  --------------------------------------------------------
local({

	BATCH_SIZE = 1000
	message('UNSCORED ROBERTA: ', nrow(unscored_text$ROBERTA))

	happytransformer = import('happytransformer')
	happy_tc = 	happytransformer$HappyTextClassification(
		'DistilRoBERTa-base', # 'ROBERTA',
		'j-hartmann/emotion-english-distilroberta-base',
		num_labels = 7
	)

	batches =
		unscored_text$ROBERTA %>%
		head(., MAX_ROWS) %>%
		as.data.table(.) %>%
		.[order(-source, -created_dt)] %>%
		.[, split := ceiling((1:nrow(.))/BATCH_SIZE)] %>%
		split(., by = c('source', 'split'), keep.by = T, flatten = T) %>%
		unname(.)

	iwalk(batches, function(x, i) {

		pull_counts =
			x %>%
			.[, list(count = .N), by = 'created_dt'] %>%
			.[, x:= paste0(format(created_dt, "%m/%d/%y"), " (", count, ")")] %>%
			.$x %>%
			paste0(., collapse = "\n")

		message(str_glue('***** ROBERTA Scoring {i} of {length(batches)} | Source: {x$source[[1]]} | Counts: \n{pull_counts}'))

		fwrite(set_names(x[, 'text_part_all_text'], 'text'), file.path(tempdir(), 'text.csv'))
		classified_text = happy_tc$test(file.path(tempdir(), 'text.csv'))

		result = data.table(
			scrape_id = x$id,
			score = map_chr(classified_text, ~ .$label),
			score_conf = map_chr(classified_text, ~ .$score)
		)

		sql_data =
			result %>%
			transmute(
				.,
				scrape_id,
				text_part = 'all_text',
				score_model = 'ROBERTA',
				score,
				score_conf,
				scored_dttm = now()
			) %>%
			mutate(., across(where(is.POSIXt), function(x) format(x, '%Y-%m-%d %H:%M:%S %Z')))

		sql_data %>%
			create_insert_query(
				.,
				paste0('sentiment_analysis_', x$source[[1]], '_score'),
				'ON CONFLICT (scrape_id, text_part, score_model) DO UPDATE SET
			score=EXCLUDED.score,
			score_conf=EXCLUDED.score_conf,
			scored_dttm=EXCLUDED.scored_dttm'
			) %>%
			dbExecute(db, .)
	})

})

# Finalize --------------------------------------------------------

## Log --------------------------------------------------------
local({

	log_data = list(
		scored_dict = nrow(unscored_text$DICT),
		scored_roberta = nrow(unscored_text$ROBERTA),
		scored_distilbert = nrow(unscored_text$DISTILBERT),
		stdout = paste0(tail(read_lines(file.path(EF_DIR, 'logs', paste0(JOB_NAME, '.log'))), 100), collapse = '\n')
	)
	log_finish_in_db(db, run_id, JOB_NAME, 'sentiment-analysis', log_data)
})

## Close Connections --------------------------------------------------------
dbDisconnect(db)
message(paste0('\n\n----------- FINISHED ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
