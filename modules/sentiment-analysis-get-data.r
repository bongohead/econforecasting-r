#' Reddit scraper (sentiment analysis)
#' 
#' Chek curl example for correct headers to send: https://github.com/reddit-archive/reddit/wiki/OAuth2-Quick-Start-Example
#' After token is fetched: https://www.reddit.com/dev/api
#' Page scraping query arguments: https://www.reddit.com/dev/api#listings

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'sentiment-analysis-get-data'
EF_DIR = Sys.getenv('EF_DIR')
RESET_SQL = TRUE
BACKFILL = TRUE

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
library(httr)
library(RCurl)
library(DBI)
library(RPostgres)
library(lubridate)
library(jsonlite)

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


# Reddit ----------------------------------------------------------------

## Reset SQL  ----------------------------------------------------------------
local({
if (RESET_SQL) {
		
	dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_scrape_reddit CASCADE')

	dbExecute(
		db,
		'CREATE TABLE sentiment_analysis_scrape_reddit (
		method VARCHAR(255) NOT NULL,
		name VARCHAR(255) NOT NULL,
		subreddit VARCHAR(255) NOT NULL,
		title TEXT NOT NULL,
		created_dttm TIMESTAMP NOT NULL,
		scraped_dttm TIMESTAMP NOT NULL,
		selftext TEXT,
		upvote_ratio NUMERIC(4, 2),
		ups NUMERIC(20, 0),
		is_self BOOLEAN,
		domain TEXT,
		url_overridden_by_dest TEXT,
		PRIMARY KEY (method, name)
		)'
	)
		
}
})

## Token --------------------------------------------------------
local({
	token =
		POST(
			'https://www.reddit.com/api/v1/access_token',
			add_headers(c(
				'User-Agent' = 'windows:SentimentAnalysis:v0.0.1 (by /u/dongobread)',
				'Authorization' = paste0(
					'Basic ', base64(txt = paste0(CONST$REDDIT_ID, ':', CONST$REDDIT_SECRET), mode = 'character')
					)
				)),
			body = list(
				grant_type = 'client_credentials',
				username = CONST$REDDIT_USERNAME,
				password = CONST$REDDIT_PASSWORD
				),
			encoding = 'json'
			) %>%
		httr::content(., 'parsed') %>%
		.$access_token
	
	reddit <<- list()
	reddit$token <<- token
})

## Top (All) --------------------------------------------------------
local({
	
	top_1000_today_all = reduce(1:9, function(accum, i) {
		
		query =
			list(t = 'day', limit = 100, show = 'all', after = {if (i == 1) NULL else tail(accum, 1)$after}) %>%
			compact(.) %>%
			paste0(names(.), '=', .) %>%
			paste0(collapse = '&')
	
		http_result = GET(
			paste0('https://oauth.reddit.com/top?', query),
			add_headers(c(
				'User-Agent' = 'windows:SentimentAnalysis:v0.0.1 (by /u/dongobread)',
				'Authorization' = paste0('bearer ', reddit$token)
				))
			)
		
		calls_remaining = as.integer(headers(http_result)$`x-ratelimit-remaining`)
		reset_seconds = as.integer(headers(http_result)$`x-ratelimit-reset`)
		if (calls_remaining == 0) Sys.sleep(reset_seconds)
		result = content(http_result, 'parsed')
	
		parsed =
			lapply(result$data$children, function(y) 
				y[[2]] %>% keep(., ~ !is.null(.) && !is.list(.)) %>% as_tibble(.)
			) %>%
			bind_rows(.) %>%
			select(., any_of(c(
				'name', 'subreddit', 'title', 'created',
				'selftext', 'upvote_ratio', 'ups', 'is_self', 'domain', 'url_overridden_by_dest'
			))) %>%
			bind_cols(i = i, after = result$data$after %||% NA, .)
		
		if (is.null(result$data$after)) {
			message('----- Break, missing AFTER')
			return(done(bind_rows(accum, parsed)))
		} else {
			return(bind_rows(accum, parsed))
		}
		
		}, .init = tibble()) %>%
		mutate(., created = as_datetime(created)) %>%
		transmute(
			.,
			method = 'top_1000_today_all', name,
			subreddit, title, 
			created_dttm = created, scraped_dttm = now(),
			selftext, upvote_ratio, ups, is_self, domain, url_overridden_by_dest
			)
	
	reddit$data$top_1000_today_all <<- top_1000_today_all
})

## Top (By Board) --------------------------------------------------------
local({
	
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
		'AskReddit', 'General',
		'pics', 'General',
		'videos', 'General',
		'funny', 'General'
		)
	
	top_200_today_by_board = lapply(scrape_boards$board, function(board) {
		
		message('*** Pull for: ', board)
		
		# Only top possible for top
		reduce(1:2, function(accum, i) {
			
			message('***** Pull ', i)
			query =
				list(t = 'day', limit = 100, show = 'all', after = {if (i == 1) NULL else tail(accum, 1)$after}) %>%
				compact(.) %>%
				paste0(names(.), '=', .) %>%
				paste0(collapse = '&')
			
			# message(query)
			http_result = GET(
				paste0('https://oauth.reddit.com/r/', board, '/top?', query),
				add_headers(c(
					'User-Agent' = 'windows:SentimentAnalysis:v0.0.1 (by /u/dongobread)',
					'Authorization' = paste0('bearer ', reddit$token)
					))
				)
			
			calls_remaining = as.integer(headers(http_result)$`x-ratelimit-remaining`)
			reset_seconds = as.integer(headers(http_result)$`x-ratelimit-reset`)
			if (calls_remaining == 0) Sys.sleep(reset_seconds)
			result = content(http_result, 'parsed')
	
			parsed =
				lapply(result$data$children, function(y) 
					y[[2]] %>% keep(., ~ !is.null(.) && !is.list(.)) %>% as_tibble(.)
				) %>%
				rbindlist(., fill = T) %>%
				select(., any_of(c(
					'name', 'subreddit', 'title', 'created',
					'selftext', 'upvote_ratio', 'ups', 'is_self', 'domain', 'url_overridden_by_dest'
				))) %>%
				as.data.table(.) %>%
				.[, i := i] %>%
				.[, after := result$data$after %||% NA]
			
			if (is.null(result$data$after)) {
				message('----- Break, missing AFTER')
				return(done(rbindlist(list(accum, parsed), fill = TRUE)))
			} else {
				return(rbindlist(list(accum, parsed), fill = TRUE))
			}
			
			}, .init = data.table()) %>%
			.[, created := as_datetime(created)] %>%
			return(.)
		}) %>%
		rbindlist(., fill = TRUE) %>%
		transmute(
			.,
			method = 'top_200_today_by_board', name,
			subreddit, title, 
			created_dttm = created, scraped_dttm = now(),
			selftext, upvote_ratio, ups, is_self, domain, url_overridden_by_dest
		)
	
	reddit$scrape_boards <<- scrape_boards
	reddit$data$top_200_today_by_board <<- top_200_today_by_board
})
	
## Top (By Board, Year) --------------------------------------------------------
local({
if (BACKFILL == TRUE) {
	
	top_1000_old_by_board = lapply(reddit$scrape_boards$board, function(board) {
		
		message('*** Pull for: ', board)
		
		# Only top possible for top
		reduce(1:9, function(accum, i) {
			
			message('***** Pull ', i)
			query =
				list(t = 'year', limit = 100, show = 'all', after = {if (i == 1) NULL else tail(accum, 1)$after}) %>%
				compact(.) %>%
				paste0(names(.), '=', .) %>%
				paste0(collapse = '&')
			
			http_result = GET(
				paste0('https://oauth.reddit.com/r/', board, '/top?', query),
				add_headers(c(
					'User-Agent' = 'windows:SentimentAnalysis:v0.0.1 (by /u/dongobread)',
					'Authorization' = paste0('bearer ', reddit$token)
				))
			)
			
			calls_remaining = as.integer(headers(http_result)$`x-ratelimit-remaining`)
			reset_seconds = as.integer(headers(http_result)$`x-ratelimit-reset`)
			if (calls_remaining == 0) Sys.sleep(reset_seconds)
			result = content(http_result, 'parsed')
			
			parsed =
				lapply(result$data$children, function(y) 
					y[[2]] %>% keep(., ~ !is.null(.) && !is.list(.)) %>% as_tibble(.)
				) %>%
				rbindlist(., fill = T) %>%
				select(., any_of(c(
					'name', 'subreddit', 'title', 'created',
					'selftext', 'upvote_ratio', 'ups', 'is_self', 'domain', 'url_overridden_by_dest'
				))) %>%
				as.data.table(.) %>%
				.[, i := i] %>%
				.[, after := result$data$after %||% NA]
			
			if (is.null(result$data$after)) {
				message('----- Break, missing AFTER')
				return(done(rbindlist(list(accum, parsed), fill = TRUE)))
			} else {
				return(rbindlist(list(accum, parsed), fill = TRUE))
			}
			
			}, .init = data.table()) %>%
			.[, created := as_datetime(created)] %>%
			return(.)
		}) %>%
		rbindlist(., fill = TRUE) %>%
		transmute(
			.,
			method = 'top_1000_old_by_board', name,
			subreddit, title, 
			created_dttm = created, scraped_dttm = now(),
			selftext, upvote_ratio, ups, is_self, domain, url_overridden_by_dest
		)
	
	# Verify no duplicated unique posts (name should be unique)
	top_1000_old_by_board %>%
		as_tibble(.) %>%
		group_by(., name) %>%
		summarize(., n = n()) %>%
		arrange(., desc(n)) %>%
		print(.)
	
	reddit$data$top_1000_old_by_board <<- top_1000_old_by_board
}
})


## SQL Data --------------------------------------------------------
local({
	
	message(str_glue('*** Sending Reddit Data to SQL: {format(now(), "%H:%M")}'))
	
	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM sentiment_analysis_scrape_reddit')$count)
	message('***** Initial Count: ', initial_count)
	
	sql_result =
		reddit$data %>%
		bind_rows(.) %>%
		as_tibble(.) %>%
		mutate(., across(where(is.POSIXt), as.character)) %>%
		mutate(., split = ceiling((1:nrow(.))/10000)) %>%
		group_split(., split, .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'sentiment_analysis_scrape_reddit',
				'ON CONFLICT (method, name) DO UPDATE SET
				subreddit=EXCLUDED.subreddit,
				title=EXCLUDED.title,
				created_dttm=EXCLUDED.created_dttm,
				scraped_dttm=EXCLUDED.scraped_dttm,
				selftext=EXCLUDED.selftext,
				upvote_ratio=EXCLUDED.upvote_ratio,
				ups=EXCLUDED.ups,
				is_self=EXCLUDED.is_self,
				domain=EXCLUDED.domain,
				url_overridden_by_dest=EXCLUDED.url_overridden_by_dest'
				) %>%
				dbExecute(db, .)
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}
	
	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM sentiment_analysis_scrape_reddit')$count)
	message('***** Rows Added: ', final_count - initial_count)
	
	create_insert_query(
		tribble(
			~ logname, ~ module, ~ log_date, ~ log_group, ~ log_info,
			JOB_NAME, 'composite-model', today(), 'job-success',
			toJSON(list(rows_added = final_count - initial_count))
		),
		'job_logs',
		'ON CONFLICT ON CONSTRAINT job_logs_pk DO UPDATE SET log_info=EXCLUDED.log_info,log_dttm=CURRENT_TIMESTAMP'
		) %>%
		dbExecute(db, .)
	
})

# News --------------------------------------------------------

## Reuters --------------------------------------------------------
local({
	
	
	reuters_data =
		# Iterate through pages
		# Go up to 3000
		purrr::reduce(1:3000, function(accum, page) {
			#if (page %% 20 == 1)
			message('Downloading data for page ', page)
			pageContent =
				httr::GET(paste0('https://www.reuters.com/news/archive/businessnews?view=page&page=',
												 page, '&pageSize=10')) %>%
				httr::content(.) %>%
				rvest::html_node(., 'div.column1')
			
			res =
				tibble(
					page = page,
					article1 = html_text(html_nodes(pageContent, 'h3.story-title'), trim = TRUE),
					article2 = html_text(html_nodes(pageContent, 'div.story-content > p'), trim = TRUE),
					date = html_text(html_nodes(pageContent, 'span.timestamp'), trim = TRUE)
				) %>%
				mutate(
					.,
					date = ifelse(str_detect(date, coll('EDT')) == TRUE, format(Sys.Date(), '%b %d %Y'), date),
					date = as.Date(parse_date_time2(date, '%b %d %Y'))
				) %>%
				bind_rows(accum, .)
			
			if (any(as.Date(res$date) %in% sentMeta$date)) return(done(res))
			return(res)
		}, .init = tibble()) %>%
		dplyr::filter(., date < as.Date(Sys.Date()) & !date %in% sentMeta$date)
	
})





# Finalize --------------------------------------------------------

## Send to SQL --------------------------------------------------------
local({
	
	if (RESET_SQL == TRUE) {
		dbExecute(
			db,
			'CREATE TABLE sentiment_analysis_raw_reddit (
			vdate DATE NOT NULL,
			form VARCHAR(50) NOT NULL,
			freq CHAR(1) NOT NULL,
			varname VARCHAR(50) NOT NULL,
			date DATE NOT NULL,
			value NUMERIC(20, 4) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (vdate, form, freq, varname, date),
			CONSTRAINT forecast_hist_values_fk FOREIGN KEY (varname)
				REFERENCES forecast_variables (varname) ON DELETE CASCADE ON UPDATE CASCADE
			)'
		)
	}
	
})

