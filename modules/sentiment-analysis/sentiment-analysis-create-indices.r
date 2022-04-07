#' Sentiment Analysis with Dictionary Analysis & BERT

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'sentiment-analysis-create-indices'
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
# if (RESET_SQL) {
# 	
# 	dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_score_reddit CASCADE')
# 	dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_score_reuters CASCADE')
# 	
# 	dbExecute(
# 		db,
# 		'CREATE TABLE sentiment_analysis_score_reddit (
# 		scrape_id INT NOT NULL,
# 		score_model VARCHAR(255) NOT NULL, 
# 		score INT NOT NULL,
# 		score_conf DECIMAL(20, 4) NULL,
# 		scored_dttm TIMESTAMP WITH TIME ZONE NOT NULL,
# 		PRIMARY KEY (scrape_id, score_model),
# 		CONSTRAINT sentiment_analysis_scrape_reddit_fk FOREIGN KEY (scrape_id)
# 				REFERENCES sentiment_analysis_scrape_reddit (id) ON DELETE CASCADE ON UPDATE CASCADE
# 		)'
# 	)
# 	
# 	dbExecute(
# 		db,
# 		'CREATE TABLE sentiment_analysis_score_reuters (
# 		scrape_id INT NOT NULL,
# 		score_model VARCHAR(255) NOT NULL, 
# 		score INT NOT NULL,
# 		score_conf DECIMAL(20, 4) NULL,
# 		scored_dttm TIMESTAMP WITH TIME ZONE NOT NULL,
# 		PRIMARY KEY (scrape_id, score_model),
# 		CONSTRAINT sentiment_analysis_score_reuters_fk FOREIGN KEY (scrape_id)
# 				REFERENCES sentiment_analysis_scrape_reuters (id) ON DELETE CASCADE ON UPDATE CASCADE
# 		)'
# 	)
# 	
# }
})	

## Pull Unscored Data --------------------------------------------------------
local({
	
	# Pull data NULL
	reddit_scored = dbGetQuery(db,
		"SELECT
			'reddit' AS source, reddit1.*, reddit2.*
		FROM sentiment_analysis_scrape_reddit reddit1
		INNER JOIN sentiment_analysis_score_reddit reddit2
			ON reddit1.id = reddit2.scrape_id"
		) %>%
		as_tibble(.) %>%
		mutate(., created_dt = as_date(created_dttm))
		
	reuters_scored = dbGetQuery(db, 
		"SELECT
			'reuters' AS source, reuters1.*, reuters2.*
		FROM sentiment_analysis_scrape_reuters reuters1
		INNER JOIN sentiment_analysis_score_reuters reuters2
			ON reuters1.id = reuters2.scrape_id"
		) %>%
		as_tibble(.) 

	reddit_scored <<- reddit_scored
	reuters_scored <<- reuters_scored
})


## Plot By Board -----------------------------------------------------------
local({
	
	plot =
		reddit_scored %>%
		group_by(., subreddit) %>%
		mutate(., subreddit_count = n()) %>%
		filter(., subreddit_count >= 1000 & created_dt >= as_date('2022-02-01')) %>%
		ungroup(.) %>%
		group_by(., created_dt, subreddit) %>%
		summarize(., mean_score = mean(score), .groups = 'drop') %>% 
		group_split(., subreddit) %>%
		map_dfr(., function(x)
			left_join(
				tibble(created_dt = seq(min(x$created_dt), to = max(x$created_dt), by = '1 day')),
				x,
				by = 'created_dt'
				) %>%
				mutate(., subreddit = x$subreddit[[1]], mean_score = coalesce(mean_score, 0)) %>%
				mutate(., mean_score_7dma = zoo::rollmean(mean_score, 7, fill = NA, na.pad = TRUE, align = 'right'))
			) %>%
		ggplot(.) + 
		geom_line(aes(x = created_dt, y = mean_score_7dma, color = subreddit))
	
	print(plot)
	subreddit_plot <<- plot
})



## Plot By Category --------------------------------------------------------
local({
	
	board_mapping = tribble(
		~ subreddit, ~ category,
		'news', 'News',
		'worldnews', 'News',
		'politics', 'News',
		'jobs', 'Labor Market',
		'careerguidance', 'Labor Market',
		# 'personalfinance', 'Labor Market',
		'Economics', 'Financial Markets',
		'investing', 'Financial Markets',
		'wallstreetbets', 'Financial Markets',
		'StockMarket', 'Financial Markets',
		'stocks', 'Financial Markets',
		'AskReddit', 'General',
		'pics', 'General',
		'videos', 'General',
		'funny', 'General',
		'dogs', 'Dog'
		)
	
	input_data = 
		reddit_scored %>%
		filter(
			.,
			method %in% c('top_200_today_by_board', 'top_1000_month_by_board'),
			score_model == 'DISTILBERT',
			score_conf > .7,
			created_dt >= as_date('2021-02-01'),
			subreddit %in% board_mapping$subreddit
			)

	# Weight each post by the relative amount of votes is got compared to the median value that day
	input_data %>%
		group_by(., subreddit, created_dt) %>%
		summarize(., subreddit_mean_score = mean(ups), .groups = 'drop') %>%
		mutate(., subreddit_mean_scored_7dlma = zoo::rollmean(subreddit_mean_score, 7, fill =  NA, na.paid, align = 'right')) %>%
		print(., n = 100)
		
		
	index_data =
		input_data %>%
		group_by(., subreddit) %>%
		mutate(., subreddit_mean_score = mean(score)) %>%
		ungroup(.) %>%
		inner_join(., board_mapping, by = 'subreddit') %>%
		group_by(., created_dt, category) %>%
		summarize(., mean_score = mean(score), count_posts = n(), .groups = 'drop') %>%
		filter(., count_posts >= 10) %>%
		group_split(., category) %>%
		map_dfr(., function(x)
			left_join(
				tibble(created_dt = seq(min(x$created_dt), to = max(x$created_dt), by = '1 day')),
				x,
				by = 'created_dt'
				) %>%
				mutate(., category = x$category[[1]], mean_score = zoo::na.locf(mean_score)) %>%
				mutate(., mean_score_7dma = zoo::rollmean(mean_score, 7, fill = NA, na.pad = TRUE, align = 'right'))
		)
	
	index_data %>%
		ggplot(.) + 
		geom_line(aes(x = created_dt, y = mean_score_7dma, color = category)) +
		geom_point(aes(x = created_dt, y = mean_score_7dma, color = category))
	
})

# Finalize --------------------------------------------------------

## Close Connections --------------------------------------------------------
dbDisconnect(db)
message(paste0('\n\n----------- FINISHED ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))

