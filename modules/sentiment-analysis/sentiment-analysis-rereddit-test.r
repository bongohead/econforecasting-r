#' ReReddit Scrape History

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'sentiment-analysis-score-rereddit'
EF_DIR = Sys.getenv('EF_DIR')
RESET_SQL = TRUE

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
library(rvest)
library(httr)

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


# Analysis --------------------------------------------------------

## Create Table --------------------------------------------------------
if (RESET_SQL) {
	dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_scrape_rereddit CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE sentiment_analysis_scrape_rereddit (
			id SERIAL PRIMARY KEY,
			created_dt DATE NOT NULL,
			page_rank INTEGER NOT NULL,
			page_number INTEGER NOT NULL,
			subreddit VARCHAR(255) NOT NULL,
			title TEXT NOT NULL,
			ups INTEGER NOT NULL,
			scraped_dttm TIMESTAMP WITH TIME ZONE NOT NULL,
			UNIQUE (created_dt, page_rank, page_number)
			)'
	)
}

## Pull Data ----------------------------------------------------------
scrape_boards = collect(tbl(db, sql('SELECT * FROM sentiment_analysis_reddit_boards')))

## Scrape Data ----------------------------------------------------------
dates =
	seq(as_date('2021-10-11'), as_date('2021-01-01'), '-1 day') #%>%
	# keep(., ~ !. %in% dbGetQuery(db, 'SELECT DISTINCT created_dt FROM sentiment_analysis_scrape_rereddit')[[1]])

iwalk(dates, function(scrape_date, i) {
	
	message(str_glue('Scraping {i} of {length(dates)}: {scrape_date}'))
	
	page_links =
		str_glue('https://www.reddit.com/posts/{format(scrape_date, "%Y/%B-%d-1")}') %>%
		GET(.) %>%
		content(.) %>%
		html_nodes(., 'nav.Pagination > a') %>%
		{tibble(
			page = as.integer(html_text(.)),
			href = html_attr(., 'href')
		)} %>%
		purrr::transpose(.)
	
	page_results = map_dfr(page_links, function(page_link) {
		
		# message(page_link$page)
		
		page_content = 
			str_glue('https://www.reddit.com{page_link$href}') %>%
			GET(.) %>%
			content(.) %>%
			html_nodes(., '#main div.DirectoryPost__Details')
		
		tibble(
			created_dt = scrape_date,
			page_number = page_link$page,
			subreddit = page_content %>% html_nodes(., '.DirectoryPost__Subreddit') %>% html_text(.),
			title = page_content %>% html_nodes(., '.DirectoryPost__Title') %>% html_text(.),
			stats = page_content %>% html_nodes(., '.DirectoryPost__Stats') %>% html_text(.),
			page_rank = 1:length(page_content)
			)
		}) %>%
		transmute(
			.,
			created_dt,
			page_rank,
			page_number,
			subreddit,
			title,
			ups =
				stats %>%
				str_extract(., '[^ upvotes]+') %>%
				{ifelse(str_detect(., 'k'), as.character(as.numeric(str_replace(., 'k', '')) * 1000), .)} %>%
				as.integer(.),
			scraped_dttm = now('America/New_York')
		)
	
	
	page_results %>%
		mutate(., across(where(is.POSIXt), function(x) format(x, '%Y-%m-%d %H:%M:%S %Z'))) %>%
		mutate(., split = ceiling((1:nrow(.))/10000)) %>%
		group_split(., split, .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'sentiment_analysis_scrape_rereddit',
				'ON CONFLICT (created_dt, page_rank, page_number) DO UPDATE SET
					subreddit=EXCLUDED.subreddit,
					title=EXCLUDED.title,			
					ups=EXCLUDED.ups,
					scraped_dttm=EXCLUDED.scraped_dttm'
				) %>%
				dbExecute(db, .)
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}
	
	})

# Finalize --------------------------------------------------------

## Close Connections --------------------------------------------------------
dbDisconnect(db)
message(paste0('\n\n----------- FINISHED ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))