#' Store list of boards of interest to scrape from Reddit

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
EF_DIR = Sys.getenv('EF_DIR')

## Load Libs ----------------------------------------------------------'
library(econforecasting)
library(tidyverse)
library(DBI)
library(RPostgres)

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


# Script ----------------------------------------------------------

## List of Boards to Scrape ----------------------------------------------------------
scrape_boards = tribble(
	~ board, ~ category,
	'news', 'News',
	'worldnews', 'News',
	'politics', 'News',
	'jobs', 'Labor Market',
	'careerguidance', 'Labor Market',
	'personalfinance', 'Labor Market',
	'Economics', 'Financial Markets',
	'investing', 'Financial Markets',
	'wallstreetbets', 'Financial Markets',
	'StockMarket', 'Financial Markets',
	'stocks', 'Financial Markets',
	'AskReddit', 'General',
	'pics', 'General',
	'videos', 'General',
	'funny', 'General',
	'dogs', 'Other'
)

## Send to SQL ----------------------------------------------------------
dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_reddit_boards CASCADE')

dbExecute(
	db,
	'CREATE TABLE sentiment_analysis_reddit_boards (
		board VARCHAR(255) PRIMARY KEY,
		category VARCHAR(255)
		)'
)

create_insert_query(
	scrape_boards,
	'sentiment_analysis_reddit_boards',
	'ON CONFLICT (board) DO UPDATE SET
				category=EXCLUDED.category'
	) %>%
	dbExecute(db, .)

## Close Connections ----------------------------------------------------------
dbDisconnect(db)
